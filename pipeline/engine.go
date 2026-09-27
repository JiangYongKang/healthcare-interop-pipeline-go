package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/formats"
)

// Result is the external result of one ingestion attempt.
type Result struct {
	EventID  string
	Outcome  Outcome
	Reason   string
	Error    *IngestionError
	Decision *Decision
	// ConflictID is set when the delivery was parked for review.
	ConflictID string
}

// Engine orchestrates parse -> normalize -> de-identify -> persist and
// is safe for concurrent use by many upstream writers.
type Engine struct {
	cfg   Config
	regs  *formats.Registry
	norm  *Normalizer
	deid  *Deidentifier
	store Store
	sem   chan struct{}
}

// NewEngine builds an engine and opens (or replays) its output store.
func NewEngine(cfg Config) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	st, err := newFileStore(cfg)
	if err != nil {
		return nil, err
	}
	return &Engine{
		cfg:   cfg,
		regs:  formats.DefaultRegistry(),
		norm:  NewNormalizer(),
		deid:  NewDeidentifier(cfg.DeIDSalt),
		store: st,
		sem:   make(chan struct{}, cfg.MaxInFlight),
	}, nil
}

// Ingest processes one raw message end to end. msgID is the upstream
// delivery identifier used for logs when the payload carries no event
// id; every outcome (including rejection) produces a traceable decision.
func (e *Engine) Ingest(ctx context.Context, msgID string, payload []byte) *Result {
	// Bound concurrent processing: bounded semaphore, acquired before
	// any allocation of parsing state, so memory use is capped.
	select {
	case e.sem <- struct{}{}:
	case <-ctx.Done():
		return &Result{EventID: msgID, Error: &IngestionError{Type: ErrUnavailable, Message: "ingestion cancelled", Reason: "CANCELLED"}}
	}
	defer func() { <-e.sem }()

	out := e.preparePayload(ctx, msgID, payload)
	if out.res != nil {
		return out.res
	}
	return e.commitPrepared(ctx, &preparedIngest{rec: out.rec})
}

// preparePayload runs detect -> parse -> normalize -> deidentify.
func (e *Engine) preparePayload(ctx context.Context, msgID string, payload []byte) prepared {
	parser, err := e.regs.Detect(stripBOM(payload))
	if err != nil {
		return prepared{res: e.journalReject(ctx, msgID, "", "", err)}
	}
	parsed, perr := parser.Parse(payload)
	if perr != nil {
		eventID := msgID
		if parsed != nil && parsed.EventID != "" {
			eventID = parsed.EventID
		}
		version := ""
		if parsed != nil {
			version = parsed.FormatVersion
		}
		return prepared{res: e.journalReject(ctx, eventID, parser.Format(), version, perr)}
	}

	rec, nerr := e.norm.Normalize(parsed)
	if nerr != nil {
		return prepared{res: e.journalReject(ctx, parsed.EventID, parsed.Format, parsed.FormatVersion, nerr)}
	}
	if derr := e.deid.DeidentifyRecord(rec); derr != nil {
		return prepared{res: e.journalReject(ctx, parsed.EventID, parsed.Format, parsed.FormatVersion, derr)}
	}
	return prepared{rec: rec}
}

// journalReject maps parser/normalizer failures onto the three external
// error classes and records a REJECTED decision. Internal parser text is
// kept in Detail only (logs), never returned as the external message.
func (e *Engine) journalReject(ctx context.Context, eventID, format, version string, cause error) *Result {
	et, reason := classifyError(cause)
	var detail string
	var ie *IngestionError
	if errors.As(cause, &ie) {
		detail = ie.Detail
		if ie.Reason != "" {
			reason = ie.Reason
		}
	} else if re, ok := cause.(*formats.RejectError); ok {
		detail = re.Detail
		if re.EventID != "" {
			eventID = re.EventID
		}
	} else {
		detail = cause.Error()
	}
	d := e.store.Reject(ctx, eventID, et, reason, detail, format, version)
	return &Result{
		EventID:  eventID,
		Outcome:  OutcomeRejected,
		Reason:   reason,
		Decision: d,
		Error:    externalError(et, externalMessage(et), reason, eventID, detail),
	}
}

// classifyError separates upstream format defects from business
// validation failures. Parser reasons are FORMAT_ERROR except those that
// describe semantically incomplete-but-parseable messages, which are
// VALIDATION_ERROR.
func classifyError(err error) (ErrorType, string) {
	var re *formats.RejectError
	if errors.As(err, &re) {
		switch re.Reason {
		case formats.ReasonMissingEventID,
			formats.ReasonMissingPatientID,
			formats.ReasonMissingCode,
			formats.ReasonMissingEventTime,
			formats.ReasonInvalidStatus,
			formats.ReasonUnsupportedEventType:
			return ErrValidationError, string(re.Reason)
		default:
			return ErrFormatError, string(re.Reason)
		}
	}
	var ie *IngestionError
	if errors.As(err, &ie) {
		return ie.Type, ie.Reason
	}
	return ErrFormatError, "UNCLASSIFIED"
}

func externalMessage(t ErrorType) string {
	switch t {
	case ErrFormatError:
		return "message rejected: malformed upstream payload"
	case ErrValidationError:
		return "message rejected: failed clinical validation"
	case ErrStorageConflict:
		return "message not stored: duplicate, stale or over capacity"
	default:
		return "message rejected"
	}
}

// IngestFile reads a single sample message file and processes it. The
// file base name is used as the upstream delivery identifier.
func (e *Engine) IngestFile(ctx context.Context, path string) *Result {
	msgID := filepath.Base(path)
	payload, err := os.ReadFile(path)
	if err != nil {
		d := e.store.Reject(ctx, msgID, ErrFormatError, "UNREADABLE_FILE", err.Error(), "", "")
		return &Result{EventID: msgID, Outcome: OutcomeRejected, Reason: "UNREADABLE_FILE", Decision: d,
			Error: externalError(ErrFormatError, externalMessage(ErrFormatError), "UNREADABLE_FILE", msgID, err.Error())}
	}
	return e.Ingest(ctx, msgID, payload)
}

// IngestPaths processes message file paths concurrently. Parsing and
// de-identification run in parallel, but store commits are applied in
// the order paths are given, so revision/out-of-order decisions for a
// fixed ordered input are reproducible despite concurrency.
func (e *Engine) IngestPaths(ctx context.Context, paths []string) []*Result {
	pre := make(chan *preparedIngest, len(paths))

	workers := e.cfg.MaxInFlight
	if workers > len(paths) {
		workers = len(paths)
	}
	var wg sync.WaitGroup
	jobs := make(chan int)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				pre <- e.prepareFile(ctx, paths[i], i)
			}
		}()
	}
	go func() {
		for i := range paths {
			jobs <- i
		}
		close(jobs)
	}()
	go func() { wg.Wait(); close(pre) }()

	byIdx := make(map[int]*preparedIngest, len(paths))
	next := 0
	results := make([]*Result, len(paths))
	for item := range pre {
		byIdx[item.idx] = item
		for {
			cur, ok := byIdx[next]
			if !ok {
				break
			}
			results[next] = e.commitPrepared(ctx, cur)
			delete(byIdx, next)
			next++
		}
	}
	return results
}

// preparedIngest carries one file through the ordered pipeline.
type preparedIngest struct {
	idx  int
	path string
	rec  *Record
	res  *Result // non-nil when parsing/normalization/de-identification rejected
}

// prepareFile runs read+parse+normalize+deidentify and returns either a
// ready record or a terminal rejection result.
func (e *Engine) prepareFile(ctx context.Context, path string, idx int) *preparedIngest {
	msgID := filepath.Base(path)
	payload, err := os.ReadFile(path)
	if err != nil {
		d := e.store.Reject(ctx, msgID, ErrFormatError, "UNREADABLE_FILE", err.Error(), "", "")
		return &preparedIngest{idx: idx, path: path, res: &Result{
			EventID: msgID, Outcome: OutcomeRejected, Reason: "UNREADABLE_FILE", Decision: d,
			Error: externalError(ErrFormatError, externalMessage(ErrFormatError), "UNREADABLE_FILE", msgID, err.Error()),
		}}
	}
	out := e.preparePayload(ctx, msgID, payload)
	return &preparedIngest{idx: idx, path: path, rec: out.rec, res: out.res}
}

type prepared struct {
	rec *Record
	res *Result
}

func (e *Engine) commitPrepared(ctx context.Context, item *preparedIngest) *Result {
	if item.res != nil {
		return item.res
	}
	d, serr := e.store.Submit(ctx, item.rec)
	r := &Result{EventID: item.rec.EventID, Decision: d, ConflictID: d.ConflictID}
	if serr != nil {
		var ie *IngestionError
		if errors.As(serr, &ie) {
			r.Error = ie
			r.Outcome = Outcome(d.Outcome)
			r.Reason = d.Reason
			return r
		}
		r.Error = &IngestionError{Type: ErrUnavailable, Message: "persistence failure", Reason: "IO_ERROR", EventID: item.rec.EventID, Detail: serr.Error()}
		return r
	}
	r.Outcome = d.Outcome
	r.Reason = d.Reason
	return r
}

// Snapshot returns the current stable, ordered accepted records.
func (e *Engine) Snapshot(ctx context.Context) ([]*Record, error) {
	return e.store.Snapshot(ctx)
}

// Decisions returns the full traceable decision journal.
func (e *Engine) Decisions(ctx context.Context) ([]*Decision, error) {
	return e.store.Decisions(ctx)
}

// Chains returns the event consistency view across all chains.
func (e *Engine) Chains(ctx context.Context) ([]*ChainView, error) {
	return e.store.Chains(ctx)
}

// PendingConflicts lists conflicts awaiting review.
func (e *Engine) PendingConflicts(ctx context.Context) ([]*Conflict, error) {
	return e.store.PendingConflicts(ctx)
}

// ResolveConflict applies a deterministic review conclusion. Re-applying
// the same conclusion is idempotent.
func (e *Engine) ResolveConflict(ctx context.Context, conflictID string, choice ResolutionChoice, reviewer string) (*ReviewResult, error) {
	return e.store.Resolve(ctx, conflictID, choice, reviewer)
}

// Replay rebuilds chain state from the journals and verifies it equals
// the live state.
func (e *Engine) Replay(ctx context.Context) (*ReplayReport, error) {
	return e.store.Replay(ctx)
}

// Close flushes all journals and releases resources.
func (e *Engine) Close() error { return e.store.Close() }

func stripBOM(b []byte) []byte {
	return []byte(strings.TrimPrefix(string(b), "\uFEFF"))
}
