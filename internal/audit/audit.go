// Package audit records an append-only, reproducible decision trail for
// every ingest attempt: accepted, deduplicated, rejected or degraded.
// Internal parsing details live here and are never exposed on the API.
package audit

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/apperr"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/model"
)

// Decision is the outcome of one ingest attempt.
type Decision string

const (
	DecisionAccepted  Decision = "ACCEPTED"
	DecisionDuplicate Decision = "DUPLICATE"
	DecisionStale     Decision = "STALE"
	DecisionRejected  Decision = "REJECTED"
	DecisionDegraded  Decision = "DEGRADED"
)

// Entry is one audit record.
type Entry struct {
	Seq       int64        `json:"seq"`
	MessageID string       `json:"messageId"`
	Format    model.Format `json:"format"`
	Version   string       `json:"version,omitempty"`
	Decision  Decision     `json:"decision"`
	Code      apperr.Code  `json:"code,omitempty"`
	Reason    string       `json:"reason"`
	Internal  string       `json:"-"` // parser internals; never exported to API or file
	At        string       `json:"at"`
}

// Log is a concurrency-safe append-only audit trail, optionally spooled to
// an append-only file for reproducibility.
type Log struct {
	mu      sync.Mutex
	entries []Entry
	seq     int64
	f       *os.File
	w       *bufio.Writer
	enc     *json.Encoder
	closed  bool
}

// NewLog creates an in-memory audit log. When dir is non-empty the trail is
// additionally spooled to dir/audit.jsonl (append-only, best-effort sync on
// Close). The caller owns Close.
func NewLog(dir string) (*Log, error) {
	l := &Log{}
	if dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(filepath.Join(dir, "audit.jsonl"),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, err
		}
		l.f = f
		l.w = bufio.NewWriter(f)
		l.enc = json.NewEncoder(l.w)
	}
	return l, nil
}

// Append records a decision and assigns its monotonically increasing Seq.
func (l *Log) Append(e Entry) Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	e.Seq = l.seq
	if e.At == "" {
		e.At = time.Now().UTC().Format("2006-01-02T15:04:05.000000Z")
	}
	// Keep the trail bounded per-record: Internal detail is held in memory
	// only and deliberately not serialized.
	l.entries = append(l.entries, e)
	if l.enc != nil {
		_ = l.enc.Encode(e) // Internal is json:"-"
	}
	return e
}

// Entries returns a snapshot copy of the trail.
func (l *Log) Entries() []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Entry, len(l.entries))
	copy(out, l.entries)
	return out
}

// Close flushes and closes any file backing; safe to call once.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.f == nil {
		l.closed = true
		return nil
	}
	l.closed = true
	if err := l.w.Flush(); err != nil {
		_ = l.f.Close()
		return err
	}
	if err := l.f.Sync(); err != nil {
		_ = l.f.Close()
		return err
	}
	return l.f.Close()
}
