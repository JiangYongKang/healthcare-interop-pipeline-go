package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestTornJournalHealing simulates a process killed mid-write: the final
// revision line is partial. On reopen the store truncates to the last
// complete prefix and works normally.
func TestTornJournalHealing(t *testing.T) {
	cfg := testCfg(t, OverflowReject, 100)
	st, _ := newFileStore(cfg)
	ctx := context.Background()
	_, _ = st.Submit(ctx, recFor("A", "R4", "2026-09-01T00:00:00Z", "1"))
	_, _ = st.Submit(ctx, recFor("B", "R4", "2026-09-02T00:00:00Z", "1"))
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt by appending a torn revision line with no decision behind
	// it (mimics crash between revision append and decision append).
	revPath := filepath.Join(cfg.OutputDir, revisionsFile)
	f, err := os.OpenFile(revPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"eventId":"ORPHAN","eventType":"Obse`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	cfg.ResetOutput = false
	st2, err := newFileStore(cfg)
	if err != nil {
		t.Fatalf("reopen must heal torn journal, got: %v", err)
	}
	defer st2.Close()
	recs, _ := st2.Snapshot(ctx)
	if len(recs) != 2 {
		t.Fatalf("torn/orphan revision must not survive; records=%d", len(recs))
	}
	// New writes still land on a healthy file and replay cleanly again.
	_, err = st2.Submit(ctx, recFor("C", "R4", "2026-09-03T00:00:00Z", "1"))
	if err != nil {
		t.Fatalf("write after heal: %v", err)
	}
	if err := st2.Close(); err != nil {
		t.Fatal(err)
	}
	st3, err := newFileStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st3.Close()
	recs, _ = st3.Snapshot(ctx)
	if len(recs) != 3 {
		t.Fatalf("post-heal write must persist across restarts; records=%d", len(recs))
	}
	t.Log("basis=torn revision line truncated at reopen; no partial state; subsequent writes durable")
}
