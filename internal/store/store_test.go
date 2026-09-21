package store

import (
	"path/filepath"
	"testing"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/apperr"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/model"
)

func rec(id string, key int64, fp, notes string) model.OutputRecord {
	return model.OutputRecord{
		EventID: id, EventType: "OBS_RESULT", Format: model.FormatHL7v2,
		Version: "2.5.1", PatientPseudo: "PT-x", OrderingKey: key,
		Fingerprint: fp, Notes: notes,
	}
}

func TestDuplicateDoesNotDuplicate(t *testing.T) {
	s, _ := Open(Config{})
	defer s.Close()
	r := rec("E-1", 100, "fp-1", "")
	if _, ae := s.Commit(r); ae != nil {
		t.Fatal(ae)
	}
	_, ae := s.Commit(r)
	if ae == nil || ae.Code != apperr.CodeDuplicateEvent {
		t.Fatalf("identical redelivery: got %v", ae)
	}
	t.Logf("message=E-1 decision=DUPLICATE code=%s basis=%q", ae.Code, ae.InternalDetail)
	if len(s.List()) != 1 {
		t.Fatal("duplicate must not create a second record")
	}
}

func TestClashSameKeyDifferentBody(t *testing.T) {
	s, _ := Open(Config{})
	defer s.Close()
	if _, ae := s.Commit(rec("E-2", 100, "fp-a", "a")); ae != nil {
		t.Fatal(ae)
	}
	_, ae := s.Commit(rec("E-2", 100, "fp-b", "b"))
	if ae == nil || ae.Code != apperr.CodeStorageClash {
		t.Fatalf("same key different body: got %v", ae)
	}
	t.Logf("message=E-2 decision=REJECT code=%s basis=%q", ae.Code, ae.InternalDetail)
	cur, _ := s.Get("E-2")
	if cur.Fingerprint != "fp-a" {
		t.Fatal("clash must not silently overwrite the current record")
	}
}

func TestOutOfOrderMarkedAndNewerSupersedes(t *testing.T) {
	s, _ := Open(Config{})
	defer s.Close()
	// Newer arrives first (simulate reordering), older later.
	if _, ae := s.Commit(rec("E-3", 200, "fp-new", "newer")); ae != nil {
		t.Fatal(ae)
	}
	out, ae := s.Commit(rec("E-3", 100, "fp-old", "older"))
	if ae == nil || ae.Code != apperr.CodeStaleVersion {
		t.Fatalf("late older version: got %v", ae)
	}
	stale := out.Record
	t.Logf("message=E-3 decision=STALE code=%s status=%s basis=%q",
		ae.Code, stale.Status, ae.InternalDetail)
	if stale.Status != model.StatusOutOfOrder {
		t.Fatalf("stale record status=%s", stale.Status)
	}
	cur, _ := s.Get("E-3")
	if cur.OrderingKey != 200 || cur.Status != model.StatusCurrent {
		t.Fatalf("current must remain newer: key=%d status=%s", cur.OrderingKey, cur.Status)
	}
	h := s.History()
	if len(h) != 2 {
		t.Fatalf("history should retain both lines, got %d", len(h))
	}

	// An even newer version supersedes and marks the prior current stale.
	if _, ae := s.Commit(rec("E-3", 300, "fp-newest", "newest")); ae != nil {
		t.Fatal(ae)
	}
	cur, _ = s.Get("E-3")
	if cur.OrderingKey != 300 {
		t.Fatal("newer version must become current")
	}
	if h := s.History(); h[0].Status != model.StatusStale {
		t.Fatalf("superseded prior current must be marked stale, got %s", h[0].Status)
	}
	t.Logf("message=E-3 newer key=300 accepted; prior current status=%s", s.History()[0].Status)
}

func TestCapacityReject(t *testing.T) {
	s, _ := Open(Config{MaxCurrent: 1})
	defer s.Close()
	if _, ae := s.Commit(rec("E-a", 1, "f1", "")); ae != nil {
		t.Fatal(ae)
	}
	_, ae := s.Commit(rec("E-b", 1, "f2", ""))
	if ae == nil || ae.Code != apperr.CodeCapacityExceeded {
		t.Fatalf("over ceiling reject: got %v", ae)
	}
	t.Logf("message=E-b decision=REJECT code=%s basis=%q", ae.Code, ae.InternalDetail)
}

func TestCapacityDegradeSpooled(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(Config{Dir: dir, MaxCurrent: 1, Policy: PolicyDegrade})
	defer s.Close()
	if _, ae := s.Commit(rec("E-a", 1, "f1", "")); ae != nil {
		t.Fatal(ae)
	}
	o2, ae := s.Commit(rec("E-b", 2, "f2", ""))
	if ae != nil || !o2.Degraded {
		t.Fatalf("over ceiling degrade: out=%+v err=%v", o2, ae)
	}
	t.Logf("message=E-b decision=DEGRADED spooledOnly=true")
	// Index holds only one current record (memory bounded)...
	if len(s.List()) != 1 {
		t.Fatal("degrade must keep in-memory index at the ceiling")
	}
	// ...but the spooled record remains recoverable from durable journal.
	if r, ok := s.Get("E-b"); !ok || r.Fingerprint != "f2" {
		t.Fatalf("degraded record must remain recoverable: %+v ok=%v", r, ok)
	}
}

func TestJournalReplayReproducesState(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Dir: dir, FileName: "r.jsonl"}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, ae := s.Commit(rec("E-1", 100, "fp-1", "")); ae != nil {
		t.Fatal(ae)
	}
	if _, ae := s.Commit(rec("E-1", 200, "fp-2", "")); ae != nil {
		t.Fatal(ae)
	}
	if _, ae := s.Commit(rec("E-2", 50, "fp-x", "")); ae != nil {
		t.Fatal(ae)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	cur, ok := s2.Get("E-1")
	if !ok || cur.OrderingKey != 200 {
		t.Fatalf("replay current wrong: %+v ok=%v", cur, ok)
	}
	h := s2.History()
	if len(h) != 3 || h[0].Status != model.StatusStale {
		t.Fatalf("replay history/lifecycle wrong: %+v", h)
	}
	t.Logf("message=E-1 replay: current key=%d, stale line status=%s, history=%d",
		cur.OrderingKey, h[0].Status, len(h))
	// Identical redelivery after restart stays a duplicate.
	if _, ae := s2.Commit(rec("E-2", 50, "fp-x", "")); ae == nil ||
		ae.Code != apperr.CodeDuplicateEvent {
		t.Fatalf("post-restart duplicate: %v", ae)
	}
}

func TestReplayCorruptJournalFailsClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "records.jsonl")
	if err := writeLineForTest(path, "{not json}\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{Dir: dir}); err == nil {
		t.Fatal("corrupt journal must surface a startup error, not silent loss")
	}
}
