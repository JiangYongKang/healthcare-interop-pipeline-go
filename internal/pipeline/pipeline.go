// Package pipeline wires parser -> normalize -> de-identify -> store behind
// a concurrency-safe Ingest entry point. It enforces configurable ceilings
// (in-flight concurrency and retained record count) and guarantees:
//   - a real value never receives two pseudonyms concurrently,
//   - duplicate redelivery never creates duplicate records,
//   - failures leave no partial write and leak no resource.
package pipeline

import (
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/apperr"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/audit"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/deid"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/model"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/normalize"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/parser"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/store"
)

// Config is the pipeline capacity/behavior configuration.
type Config struct {
	HMACKey     []byte
	MaxInFlight int    // simultaneous ingest calls admitted (0 = unlimited)
	MaxRecords  int    // retained current-record ceiling (0 = unlimited)
	StoreDir    string // "" = memory-only sink
	StoreFile   string
	Policy      string // "reject" (default) or "degrade" when over capacity
	AuditDir    string // "" = audit stays in memory
}

// Pipeline is the ingestion facade.
type Pipeline struct {
	cfg   Config
	deid  *deid.Engine
	store *store.Store
	audit *audit.Log
	sem   chan struct{}
}

// New constructs a pipeline.
func New(cfg Config) (*Pipeline, error) {
	eng := deid.NewEngine(cfg.HMACKey)
	st, err := store.Open(store.Config{
		Dir:        cfg.StoreDir,
		FileName:   cfg.StoreFile,
		MaxCurrent: cfg.MaxRecords,
		Policy:     cfg.Policy,
	})
	if err != nil {
		return nil, err
	}
	al, err := audit.NewLog(cfg.AuditDir)
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	p := &Pipeline{cfg: cfg, deid: eng, store: st, audit: al}
	if cfg.MaxInFlight > 0 {
		p.sem = make(chan struct{}, cfg.MaxInFlight)
	}
	return p, nil
}

// Result is the caller-facing outcome of one ingest attempt.
type Result struct {
	Accepted  bool
	Degraded  bool // accepted under the degrade policy (spooled, evicted from index)
	Status    model.RecordStatus
	Record    model.OutputRecord
	Code      apperr.Code // set when not accepted
	MessageID string
}

// Ingest processes one raw upstream message end to end. It returns an
// *apperr.Error (never a raw parser error) for every rejection; Result is
// nil only in that case. The internal rationale is recorded in the audit
// trail rather than exposed to the caller.
func (p *Pipeline) Ingest(raw []byte) (*Result, *apperr.Error) {
	// Bounded concurrency. Token acquisition is part of the critical path;
	// release happens on every return.
	if p.sem != nil {
		select {
		case p.sem <- struct{}{}:
		default:
			ae := apperr.New(apperr.CodeCapacityExceeded, "").
				WithInternal("in-flight concurrency ceiling reached")
			p.record(ae, model.Format(""), "", audit.DecisionRejected)
			return nil, ae
		}
	}
	defer func() {
		if p.sem != nil {
			<-p.sem
		}
	}()

	rv, ae := parser.Parse(raw)
	if ae != nil {
		if id := parser.BestEffortID(raw); id != "" {
			ae.MessageID = id
		}
		p.record(ae, model.Format(""), "", audit.DecisionRejected)
		return nil, ae
	}

	c, ae := normalize.Normalize(rv)
	if ae != nil {
		p.record(ae, rv.Format, rv.Version, audit.DecisionRejected)
		return nil, ae
	}

	out := p.deid.Deidentify(c)
	out.OrderingKey = c.OrderingKey
	out.Defaults = c.Defaults
	out.Fingerprint = normalize.Fingerprint(c)

	outcome, ce := p.store.Commit(out)
	rec := outcome.Record
	switch {
	case ce == nil && outcome.Degraded:
		p.record(nil, c.Format, c.Version, audit.DecisionDegraded, rec)
		return &Result{Accepted: true, Degraded: true, Status: rec.Status, Record: rec, MessageID: c.EventID}, nil
	case ce == nil:
		// Accepted: new event or newer version superseding prior current.
		p.record(nil, c.Format, c.Version, audit.DecisionAccepted, rec)
		return &Result{Accepted: true, Status: rec.Status, Record: rec, MessageID: c.EventID}, nil
	case ce.Code == apperr.CodeDuplicateEvent:
		p.record(ce, c.Format, c.Version, audit.DecisionDuplicate, rec)
		return &Result{Accepted: false, Status: rec.Status, Record: rec,
			Code: ce.Code, MessageID: c.EventID}, ce
	case ce.Code == apperr.CodeStaleVersion:
		p.record(ce, c.Format, c.Version, audit.DecisionStale, rec)
		return &Result{Accepted: false, Status: model.StatusOutOfOrder, Record: rec,
			Code: ce.Code, MessageID: c.EventID}, ce
	default:
		p.record(ce, c.Format, c.Version, audit.DecisionRejected, rec)
		return nil, ce
	}
}

// record appends one audit entry; err carries the internal detail which is
// retained for tests but never exported. When even a message identifier
// cannot be recovered (e.g. undecodable bytes), a stable "unknown" token is
// used so every attempt is still traceable.
func (p *Pipeline) record(err *apperr.Error, f model.Format, version string, d audit.Decision, recs ...model.OutputRecord) {
	e := audit.Entry{Format: f, Version: version, Decision: d}
	if err != nil {
		e.Code = err.Code
		e.Reason = apperr.SafeMessage(err)
		e.Internal = err.InternalDetail
		e.MessageID = err.MessageID
	}
	if len(recs) > 0 {
		if e.MessageID == "" {
			e.MessageID = recs[0].EventID
		}
		if d == audit.DecisionAccepted {
			e.Reason = "accepted status=" + string(recs[0].Status)
		}
	}
	if e.MessageID == "" {
		e.MessageID = "unknown"
	}
	if d == audit.DecisionDegraded {
		e.Reason = "accepted under degrade policy (spooled, evicted from index)"
	}
	p.audit.Append(e)
}

// Audit returns the decision trail.
func (p *Pipeline) Audit() []audit.Entry { return p.audit.Entries() }

// Store returns the backing store (tests/CLI read access).
func (p *Pipeline) Store() *store.Store { return p.store }

// Engine exposes the de-identification engine for mapping tests.
func (p *Pipeline) Engine() *deid.Engine { return p.deid }

// Close releases every owned resource exactly once; failure to close is the
// caller's only possible resource leak.
func (p *Pipeline) Close() error {
	if err := p.store.Close(); err != nil {
		_ = p.audit.Close()
		return err
	}
	return p.audit.Close()
}
