package pipeline

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Outcome is the journal classification for an ingestion decision.
type Outcome string

const (
	OutcomeAccepted   Outcome = "ACCEPTED"
	OutcomeRejected   Outcome = "REJECTED"
	OutcomeDuplicate  Outcome = "DUPLICATE"
	OutcomeOutOfOrder Outcome = "OUT_OF_ORDER"
	OutcomeDegraded   Outcome = "DEGRADED"
)

// Decision is one traceable entry in the pipeline journals.
type Decision struct {
	Seq       int64   `json:"seq"`
	EventID   string  `json:"eventId"`
	Outcome   Outcome `json:"outcome"`
	Reason    string  `json:"reason,omitempty"`
	ErrorType string  `json:"errorType,omitempty"`
	Detail    string  `json:"detail,omitempty"`
	Format    string  `json:"format,omitempty"`
	Version   string  `json:"version,omitempty"`
}

// Store is the concurrency-safe persistence boundary.
type Store interface {
	// Submit applies one fully de-identified record under the event key.
	// Duplicates and stale out-of-order versions return an
	// IngestionError of type STORAGE_CONFLICT; the decision is still
	// journaled. Overflow follows the configured policy.
	Submit(ctx context.Context, rec *Record) (*Decision, error)
	// Reject journals a rejected message.
	Reject(ctx context.Context, eventID string, et ErrorType, reason, detail, format, version string) *Decision
	// Snapshot returns records ordered by (eventTime, eventId).
	Snapshot(ctx context.Context) ([]*Record, error)
	// Decisions returns the append-only decision journal.
	Decisions(ctx context.Context) ([]*Decision, error)
	// Close fsyncs and releases all file handles.
	Close() error
}

const (
	revisionsFile = "revisions.jsonl"
	decisionsFile = "decisions.jsonl"
	degradedFile  = "degraded.jsonl"
)

// fileStore persists to JSONL files under a directory. All mutating
// decisions are serialized by mu, so de-identified data, idempotence
// and persistence cannot observe half-applied state.
type fileStore struct {
	dir string
	cfg Config
	mu  sync.Mutex

	revFile *os.File
	decFile *os.File
	degFile *os.File

	seq      int64 // monotonic arrival sequence (journal only)
	byEvent  map[string]*Record
	revision map[string]string          // eventID -> effective time of current winner
	digests  map[string]map[string]bool // eventID -> all persisted content digests
	accepted int                        // distinct events currently won
	closed   bool
}

func newFileStore(cfg Config) (Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return nil, &IngestionError{Type: ErrUnavailable, Message: "cannot create output directory", Reason: "IO_ERROR", Detail: err.Error()}
	}
	s := &fileStore{
		dir:     cfg.OutputDir,
		cfg:     cfg,
		byEvent: map[string]*Record{},
		digests: map[string]map[string]bool{},
	}
	if cfg.ResetOutput {
		for _, f := range []string{revisionsFile, decisionsFile, degradedFile} {
			if err := os.Remove(filepath.Join(cfg.OutputDir, f)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
		}
	}
	if err := s.openAndReplay(); err != nil {
		_ = s.closeHandles()
		return nil, err
	}
	return s, nil
}

func (s *fileStore) openAndReplay() error {
	var err error
	// Open all files first; if any open fails no file was truncated.
	if s.revFile, err = os.OpenFile(filepath.Join(s.dir, revisionsFile), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644); err != nil {
		return ioErr(err)
	}
	if s.decFile, err = os.OpenFile(filepath.Join(s.dir, decisionsFile), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644); err != nil {
		return ioErr(err)
	}
	if s.degFile, err = os.OpenFile(filepath.Join(s.dir, degradedFile), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644); err != nil {
		return ioErr(err)
	}
	if err := s.replay(); err != nil {
		return err
	}
	return nil
}

func ioErr(err error) error {
	return &IngestionError{Type: ErrUnavailable, Message: "persistence failure", Reason: "IO_ERROR", Detail: err.Error()}
}

type revEntry struct {
	rec    *Record
	raw    string
	ok     bool
	endOff int64
}

// replay rebuilds in-memory indexes from the append-only journals and
// heals any torn trailing line left by a killed process. Revision and
// decision journals are healed independently; in-memory state reflects
// only prefixes that are complete on both journals.
func (s *fileStore) replay() error {
	var revs []revEntry
	var revOff int64
	if err := readJSONLLines(s.revFile, func(line string) {
		var r Record
		revOff += int64(len(line) + 1)
		if json.Unmarshal([]byte(line), &r) == nil && r.EventID != "" {
			revs = append(revs, revEntry{&r, line, true, revOff})
		} else {
			revs = append(revs, revEntry{nil, line, false, revOff})
		}
	}); err != nil {
		return err
	}
	var decisions []*Decision
	if err := readJSONLLines(s.decFile, func(line string) {
		var d Decision
		if json.Unmarshal([]byte(line), &d) == nil && d.EventID != "" {
			decisions = append(decisions, &d)
		} else {
			decisions = append(decisions, nil)
		}
	}); err != nil {
		return err
	}

	// First torn/invalid line terminates replay for that journal.
	goodRevs := 0
	for _, rl := range revs {
		if !rl.ok {
			break
		}
		goodRevs++
	}
	goodDecs := len(decisions)
	for i, d := range decisions {
		if d == nil {
			goodDecs = i
			break
		}
	}

	// Decisions mark which revisions were committed. A revision appended
	// just before a crash (with no decision) is an orphan and is removed
	// by truncation, so no partial state can survive a failed write.
	committed := map[string]bool{}
	for _, d := range decisions[:goodDecs] {
		if s.seq < d.Seq {
			s.seq = d.Seq
		}
		if d.Outcome == OutcomeAccepted || d.Outcome == OutcomeOutOfOrder {
			committed[d.EventID] = true
		}
	}
	var lastCommittedOff int64
	for i := 0; i < goodRevs; i++ {
		r := revs[i].rec
		if !committed[r.EventID] {
			continue
		}
		if cur, exists := s.byEvent[r.EventID]; !exists || recNewer(r, cur) {
			s.byEvent[r.EventID] = r
		}
		if s.digests[r.EventID] == nil {
			s.digests[r.EventID] = map[string]bool{}
		}
		s.digests[r.EventID][businessDigest(r)] = true
		lastCommittedOff = revs[i].endOff
	}
	s.accepted = len(s.byEvent)

	// Heal torn trailing lines and crash-orphan revisions by truncating
	// both journals back to their last committed prefixes.
	if goodRevs < len(revs) || lastCommittedOff < revOff {
		if err := s.truncateToValid(lastCommittedOff); err != nil {
			return err
		}
	}
	if goodDecs < len(decisions) {
		if err := s.truncateDecToValid(decisions, goodDecs); err != nil {
			return err
		}
	}
	return nil
}

func (s *fileStore) truncateToValid(length int64) error {
	if err := s.revFile.Truncate(length); err != nil {
		return ioErr(err)
	}
	if _, err := s.revFile.Seek(0, io.SeekEnd); err != nil {
		return ioErr(err)
	}
	return nil
}

func (s *fileStore) truncateDecToValid(decs []*Decision, good int) error {
	var n int64
	for i := 0; i < good; i++ {
		b, _ := json.Marshal(decs[i])
		n += int64(len(b)) + 1
	}
	if err := s.decFile.Truncate(n); err != nil {
		return ioErr(err)
	}
	if _, err := s.decFile.Seek(0, io.SeekEnd); err != nil {
		return ioErr(err)
	}
	return nil
}

// recNewer reports whether incoming revision r supersedes current c.
// Ordering rule (fully deterministic, independent of arrival order):
// version ordinal, then effective/event time, then content digest.
// A never-newer record must not silently overwrite newer data.
func recNewer(r, c *Record) bool {
	rv, cv := versionOrdinal(r.SourceVersion), versionOrdinal(c.SourceVersion)
	if rv != cv {
		return rv > cv
	}
	rt, ct := effectiveOf(r), effectiveOf(c)
	if rt != ct {
		return rt > ct
	}
	return businessDigest(r) > businessDigest(c)
}

func effectiveOf(r *Record) string {
	if r.EffectiveTime != "" {
		return r.EffectiveTime
	}
	return r.EventTime
}

// versionOrdinal assigns a total order across supported versions.
// HL7 2.3 < 2.5.1 < FHIR STU3 < R4. Cross-format ordering only serves
// the deterministic revision rule; same events usually share a format.
func versionOrdinal(v string) int {
	switch v {
	case "2.3":
		return 1
	case "2.5.1":
		return 2
	case "STU3":
		return 3
	case "R4":
		return 4
	}
	return 0
}

// businessDigest hashes all business fields except arrival metadata, so
// exact re-delivery of the same event is recognizable regardless of
// re-receipt sequence.
func businessDigest(r *Record) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s",
		r.EventID, r.EventType, r.PatientRef, r.SourceFormat, r.SourceVersion,
		r.Status, r.EventTime, r.Code, r.CodeSystem, r.CodeDisplay, r.Value, r.Unit)
	for _, k := range sortedKeys(r.UnknownFields) {
		fmt.Fprintf(h, "\x00%s=%s", k, r.UnknownFields[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func sortedKeys(m map[string]string) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func (s *fileStore) Submit(_ context.Context, rec *Record) (*Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, &IngestionError{Type: ErrUnavailable, Message: "store closed", Reason: "CLOSED"}
	}
	s.seq++
	_, exists := s.byEvent[rec.EventID]
	d := &Decision{Seq: s.seq, EventID: rec.EventID, Format: rec.SourceFormat, Version: rec.SourceVersion}
	digest := businessDigest(rec)

	// Exact redelivery of any previously persisted revision is a
	// duplicate regardless of which revision currently wins.
	if s.digests[rec.EventID][digest] {
		d.Outcome = OutcomeDuplicate
		d.Reason = "identical redelivery; no new record created"
		if err := s.appendDecision(d); err != nil {
			return nil, err
		}
		return d, conflictErr(d, "duplicate event suppressed")
	}

	// Capacity applies only when this is the first revision of a new
	// event; repeated/out-of-order revisions of known events never
	// displace the configured limit.
	if !exists && s.cfg.MaxRecords > 0 && s.accepted >= s.cfg.MaxRecords {
		switch s.cfg.OverflowPolicy {
		case OverflowReject:
			d.Outcome = OutcomeRejected
			d.Reason = fmt.Sprintf("capacity limit %d reached; reject policy", s.cfg.MaxRecords)
			d.ErrorType = string(ErrStorageConflict)
			if err := s.appendDecision(d); err != nil {
				return nil, err
			}
			return d, conflictErr(d, "capacity limit reached")
		case OverflowDegrade:
			d.Outcome = OutcomeDegraded
			d.Reason = fmt.Sprintf("capacity limit %d reached; degraded journal", s.cfg.MaxRecords)
			if err := s.appendJSON(s.degFile, rec); err != nil {
				return nil, err
			}
			if err := s.appendDecision(d); err != nil {
				return nil, err
			}
			return d, nil
		}
	}

	// Every non-duplicate revision is durably retained (append-only) so
	// the decision is fully traceable. Outcome vs. the *currently winning*
	// revision follows the deterministic rule and is independent of
	// arrival order: late old data is marked, never silently applied.
	winner := rec
	outcome := OutcomeAccepted
	if cur, curExists := s.byEvent[rec.EventID]; curExists {
		if recNewer(rec, cur) {
			d.Reason = fmt.Sprintf("newer revision supersedes prior (ordinal=%d time=%s)",
				versionOrdinal(cur.SourceVersion), effectiveOf(cur))
		} else {
			outcome = OutcomeOutOfOrder
			winner = cur
			d.Reason = fmt.Sprintf("stale revision (winner ordinal=%d time=%s vs incoming ordinal=%d time=%s); newer data retained",
				versionOrdinal(cur.SourceVersion), effectiveOf(cur),
				versionOrdinal(rec.SourceVersion), effectiveOf(rec))
		}
	}
	if err := s.appendJSON(s.revFile, rec); err != nil {
		return nil, err
	}
	if s.digests[rec.EventID] == nil {
		s.digests[rec.EventID] = map[string]bool{}
	}
	s.digests[rec.EventID][digest] = true
	s.byEvent[rec.EventID] = winner
	if !exists {
		s.accepted++
	}
	d.Outcome = outcome
	if err := s.appendDecision(d); err != nil {
		return nil, err
	}
	if outcome == OutcomeOutOfOrder {
		return d, conflictErr(d, "out-of-order message retained as stale revision")
	}
	return d, nil
}

func conflictErr(d *Decision, msg string) error {
	return &IngestionError{
		Type: ErrStorageConflict, Message: msg, Reason: string(d.Outcome),
		EventID: d.EventID, Detail: d.Reason,
	}
}

func (s *fileStore) Reject(_ context.Context, eventID string, et ErrorType, reason, detail, format, version string) *Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	d := &Decision{
		Seq: s.seq, EventID: eventID, Outcome: OutcomeRejected,
		Reason: reason, ErrorType: string(et), Detail: detail,
		Format: format, Version: version,
	}
	_ = s.appendDecision(d)
	return d
}

func (s *fileStore) appendJSON(f *os.File, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return &IngestionError{Type: ErrUnavailable, Message: "encode failure", Reason: "ENCODE_ERROR", Detail: err.Error()}
	}
	b = append(b, '\n')
	if _, err := f.Write(b); err != nil {
		return ioErr(err)
	}
	if err := f.Sync(); err != nil {
		return ioErr(err)
	}
	return nil
}

func (s *fileStore) appendDecision(d *Decision) error {
	return s.appendJSON(s.decFile, d)
}

func (s *fileStore) Snapshot(_ context.Context) ([]*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Record, 0, len(s.byEvent))
	for _, r := range s.byEvent {
		out = append(out, cloneRecord(r))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EventTime != out[j].EventTime {
			return out[i].EventTime < out[j].EventTime
		}
		return out[i].EventID < out[j].EventID
	})
	return out, nil
}

func cloneRecord(r *Record) *Record {
	c := *r
	if r.UnknownFields != nil {
		c.UnknownFields = make(map[string]string, len(r.UnknownFields))
		for k, v := range r.UnknownFields {
			c.UnknownFields[k] = v
		}
	}
	return &c
}

func (s *fileStore) Decisions(_ context.Context) ([]*Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Decision, 0)
	// Read current file content (includes everything appended so far).
	if _, err := s.decFile.Seek(0, io.SeekStart); err != nil {
		return nil, ioErr(err)
	}
	sc := bufio.NewScanner(s.decFile)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var d Decision
		if err := json.Unmarshal([]byte(line), &d); err != nil {
			return nil, ioErr(err)
		}
		out = append(out, &d)
	}
	_, _ = s.decFile.Seek(0, io.SeekEnd)
	return out, sc.Err()
}

func (s *fileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.closeHandles()
}

func (s *fileStore) closeHandles() error {
	var errs []string
	for _, f := range []*os.File{s.revFile, s.decFile, s.degFile} {
		if f == nil {
			continue
		}
		if err := f.Sync(); err != nil {
			errs = append(errs, err.Error())
		}
		if err := f.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return ioErr(errors.New(strings.Join(errs, "; ")))
	}
	return nil
}

// readJSONLLines reads whole lines from an O_APPEND file from offset 0,
// restoring the append offset afterwards.
func readJSONLLines(f *os.File, emit func(string)) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return ioErr(err)
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			emit(line)
		}
	}
	_, _ = f.Seek(0, io.SeekEnd)
	return sc.Err()
}
