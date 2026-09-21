// Package store is the durable, concurrency-safe sink. It owns:
//   - idempotent acceptance keyed by EventID,
//   - out-of-order detection keyed by a deterministic ordering value,
//   - append-only JSONL journaling with fsync-before-index-update (no
//     partial writes, no half-updated state, replayable on restart).
//
// Records handed to the store are already de-identified; the store never sees
// raw PHI.
package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/apperr"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/model"
)

// Policy names for over-capacity behavior.
const (
	PolicyReject  = "reject"
	PolicyDegrade = "degrade"
)

// Config configures the backing store.
type Config struct {
	Dir        string // directory for the append-only JSONL file; "" = memory only
	FileName   string // JSONL file name within Dir (default: records.jsonl)
	MaxCurrent int    // ceiling on in-memory current records (0 = unlimited)
	Policy     string // reject (default) | degrade
}

// journalLine is one append-only journal entry. "supersede" lines mutate a
// previously stored event line during replay.
type journalLine struct {
	Op           string             `json:"op"` // event | supersede
	Record       model.OutputRecord `json:"record"`
	Fingerprint  string             `json:"fp,omitempty"` // internal duplicate/clash key
	RefLine      int64              `json:"refLine,omitempty"`
	Status       model.RecordStatus `json:"status,omitempty"`
	SupersededBy string             `json:"supersededBy,omitempty"`
}

// eventState is the in-memory state for one EventID.
type eventState struct {
	current model.OutputRecord
	lineNo  int64
}

// Store is the ingestion sink.
type Store struct {
	mu      sync.Mutex
	cfg     Config
	current map[string]*eventState
	history []*model.OutputRecord // indexed by lineNo-1; nil for marker lines
	spooled map[string]bool       // degrade overflow, only on disk journal
	lineNo  int64
	f       *os.File
	enc     *json.Encoder
	closed  bool
}

// Open opens/creates the store and replays any existing journal so state is
// identical across restarts.
func Open(cfg Config) (*Store, error) {
	if cfg.FileName == "" {
		cfg.FileName = "records.jsonl"
	}
	if cfg.Policy == "" {
		cfg.Policy = PolicyReject
	}
	s := &Store{
		cfg:     cfg,
		current: map[string]*eventState{},
		spooled: map[string]bool{},
	}
	if cfg.Dir != "" {
		if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
			return nil, err
		}
		path := filepath.Join(cfg.Dir, cfg.FileName)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			return nil, err
		}
		s.f = f
		s.enc = json.NewEncoder(f)
		if err := s.replay(path); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	return s, nil
}

func (s *Store) replay(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		var jl journalLine
		if err := json.Unmarshal(line, &jl); err != nil {
			return fmt.Errorf("corrupt journal line: %w", err)
		}
		s.lineNo++
		switch jl.Op {
		case "event":
			jl.Record.Fingerprint = jl.Fingerprint
			s.applyEventLine(jl.Record)
		case "supersede":
			if jl.RefLine >= 1 && int(jl.RefLine-1) < len(s.history) && s.history[jl.RefLine-1] != nil {
				s.history[jl.RefLine-1].Status = jl.Status
				s.history[jl.RefLine-1].SupersededBy = jl.SupersededBy
			}
		}
	}
	return sc.Err()
}

// applyEventLine indexes a journaled event during replay. The in-memory
// current ceiling is not re-enforced during replay; spooled ids are tracked
// when the ceiling is already reached in degrade mode.
func (s *Store) applyEventLine(rec model.OutputRecord) {
	idx := int(s.lineNo - 1)
	for len(s.history) <= idx {
		s.history = append(s.history, nil)
	}
	cp := rec
	s.history[idx] = &cp

	existing := s.current[rec.EventID]
	newer := existing == nil || rec.OrderingKey > existing.current.OrderingKey
	if newer {
		if existing == nil {
			if s.cfg.MaxCurrent > 0 && len(s.current) >= s.cfg.MaxCurrent {
				// restored overflow entry from a degraded run
				s.spooled[rec.EventID] = true
				return
			}
		}
		s.current[rec.EventID] = &eventState{current: rec, lineNo: s.lineNo}
	}
}

// Outcome describes a successful commit: the stored record and whether it
// was accepted under the degrade policy (durable spool, evicted from index).
type Outcome struct {
	Record   model.OutputRecord
	Degraded bool
}

// Commit attempts to atomically accept one record.
//   - identical redelivery          -> apperr DUPLICATE_EVENT
//   - older version arriving later  -> apperr STALE_VERSION (retained as
//     out-of-order history; current untouched)
//   - same key, different body       -> apperr STORAGE_CLASH
//
// On any conflict the current state is never overwritten.
func (s *Store) Commit(rec model.OutputRecord) (Outcome, *apperr.Error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if st := s.current[rec.EventID]; st != nil {
		cur := st.current
		switch {
		case rec.Fingerprint == cur.Fingerprint:
			return Outcome{Record: cur}, apperr.New(apperr.CodeDuplicateEvent, "").
				WithMessage(rec.EventID).
				WithInternal("identical payload already stored at line " +
					fmt.Sprint(st.lineNo))
		case rec.OrderingKey == cur.OrderingKey:
			return Outcome{Record: cur}, apperr.New(apperr.CodeStorageClash, "").
				WithMessage(rec.EventID).
				WithInternal("same ordering key but different payload")
		case rec.OrderingKey < cur.OrderingKey:
			// Late arrival of an older version: retain as marked history.
			rec.Status = model.StatusOutOfOrder
			rec.SupersededBy = lineRef(cur.EventID, st.lineNo)
			rec.IngestedAt = nowTS()
			if ae := s.appendEvent(rec, false); ae != nil {
				return Outcome{}, ae
			}
			return Outcome{Record: rec}, apperr.New(apperr.CodeStaleVersion, "").
				WithMessage(rec.EventID).
				WithInternal(fmt.Sprintf("incoming key %d < current %d",
					rec.OrderingKey, cur.OrderingKey))
		default:
			// Newer version supersedes current: journal new event + marker.
			rec.Status = model.StatusCurrent
			rec.IngestedAt = nowTS()
			if ae := s.appendEvent(rec, true); ae != nil {
				return Outcome{}, ae
			}
			return Outcome{Record: rec}, nil
		}
	}

	// Brand-new event id: capacity gate is atomic with the write.
	if s.cfg.MaxCurrent > 0 && len(s.current) >= s.cfg.MaxCurrent {
		if s.cfg.Policy == PolicyDegrade && s.f != nil {
			// Degrade: spool durably but evict from the in-memory index.
			rec.Status = model.StatusCurrent
			rec.IngestedAt = nowTS()
			if ae := s.appendEvent(rec, false); ae != nil {
				return Outcome{}, ae
			}
			s.spooled[rec.EventID] = true
			return Outcome{Record: rec, Degraded: true}, nil
		}
		return Outcome{}, apperr.New(apperr.CodeCapacityExceeded, "").
			WithMessage(rec.EventID).
			WithInternal(fmt.Sprintf("current-record ceiling %d reached", s.cfg.MaxCurrent))
	}

	rec.Status = model.StatusCurrent
	rec.IngestedAt = nowTS()
	if ae := s.appendEvent(rec, true); ae != nil {
		return Outcome{}, ae
	}
	return Outcome{Record: rec}, nil
}

// appendEvent journalizes the record and, when index=true and an older
// current exists, a supersede marker; only then is the in-memory index
// updated. The journal is fsynced first, so a failure leaves no partial
// state and no untracked index entry.
func (s *Store) appendEvent(rec model.OutputRecord, index bool) *apperr.Error {
	s.lineNo++
	line := s.lineNo
	if s.enc != nil {
		if err := s.enc.Encode(journalLine{Op: "event", Record: rec, Fingerprint: rec.Fingerprint}); err != nil {
			s.lineNo--
			return apperr.New(apperr.CodeStorageClash, "").
				WithMessage(rec.EventID).WithInternal("journal write: " + err.Error())
		}
		if err := s.f.Sync(); err != nil {
			s.lineNo--
			return apperr.New(apperr.CodeStorageClash, "").
				WithMessage(rec.EventID).WithInternal("journal sync: " + err.Error())
		}
	}
	for len(s.history) < int(line) {
		s.history = append(s.history, nil)
	}
	cp := rec
	s.history[line-1] = &cp

	if index {
		if old := s.current[rec.EventID]; old != nil {
			if s.enc != nil {
				if err := s.enc.Encode(journalLine{
					Op:           "supersede",
					RefLine:      old.lineNo,
					Status:       model.StatusStale,
					SupersededBy: lineRef(rec.EventID, line),
				}); err != nil {
					// New event line is already durable; surface clash, keep
					// index pointing at the previous current.
					return apperr.New(apperr.CodeStorageClash, "").
						WithMessage(rec.EventID).
						WithInternal("supersede marker write: " + err.Error())
				}
				if err := s.f.Sync(); err != nil {
					return apperr.New(apperr.CodeStorageClash, "").
						WithMessage(rec.EventID).
						WithInternal("supersede marker sync: " + err.Error())
				}
			}
			if int(old.lineNo-1) < len(s.history) && s.history[old.lineNo-1] != nil {
				s.history[old.lineNo-1].Status = model.StatusStale
				s.history[old.lineNo-1].SupersededBy = lineRef(rec.EventID, line)
			}
		}
		delete(s.spooled, rec.EventID)
		s.current[rec.EventID] = &eventState{current: rec, lineNo: line}
	}
	return nil
}

func lineRef(eventID string, lineNo int64) string {
	return fmt.Sprintf("%s#%d", eventID, lineNo)
}

func nowTS() string { return time.Now().UTC().Format("2006-01-02T15:04:05.000000Z") }

// Get returns the current record for an event id. In degrade mode an
// overflow event spooled only to disk is recovered by journal scan.
func (s *Store) Get(eventID string) (model.OutputRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.current[eventID]; ok {
		return st.current, true
	}
	if !s.spooled[eventID] || s.f == nil {
		return model.OutputRecord{}, false
	}
	if rec, ok := s.scanSpooled(eventID); ok {
		return rec, true
	}
	return model.OutputRecord{}, false
}

// scanSpooled finds the newest journaled event line for an evicted id.
func (s *Store) scanSpooled(eventID string) (model.OutputRecord, bool) {
	path := filepath.Join(s.cfg.Dir, s.cfg.FileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return model.OutputRecord{}, false
	}
	var found model.OutputRecord
	hit := false
	for _, l := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(l) == "" {
			continue
		}
		var jl journalLine
		if json.Unmarshal([]byte(l), &jl) != nil {
			continue
		}
		if jl.Op == "event" && jl.Record.EventID == eventID {
			jl.Record.Fingerprint = jl.Fingerprint
			found, hit = jl.Record, true
		}
	}
	return found, hit
}

// List returns a snapshot of all current records.
func (s *Store) List() []model.OutputRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.OutputRecord, 0, len(s.current))
	for _, st := range s.current {
		out = append(out, st.current)
	}
	return out
}

// History returns every retained event line in journal order (including
// stale and out-of-order), omitting marker lines.
func (s *Store) History() []model.OutputRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.OutputRecord, 0, len(s.history))
	for _, p := range s.history {
		if p != nil {
			out = append(out, *p)
		}
	}
	return out
}

// Close releases resources; safe to call once.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.f == nil {
		s.closed = true
		return nil
	}
	s.closed = true
	return s.f.Close()
}
