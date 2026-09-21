package pipeline

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func testCfg(t *testing.T, policy OverflowPolicy, max int) Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.OutputDir = t.TempDir()
	cfg.OverflowPolicy = policy
	cfg.MaxRecords = max
	cfg.ResetOutput = true
	cfg.MaxInFlight = 4
	return cfg
}

func recFor(id, version, eff, value string) *Record {
	return &Record{
		EventID: id, EventType: EventObservation, PatientRef: "MRN-1",
		SourceFormat: "HL7v2", SourceVersion: version, Status: StatusFinal,
		EventTime: eff, EffectiveTime: eff, Code: "C", Value: value,
	}
}

func TestStoreDuplicateSuppressed(t *testing.T) {
	st, err := newFileStore(testCfg(t, OverflowReject, 100))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	r := recFor("E1", "2.5.1", "2026-09-01T00:00:00Z", "1")
	d1, e1 := st.Submit(ctx, r)
	_, e2 := st.Submit(ctx, recFor("E1", "2.5.1", "2026-09-01T00:00:00Z", "1"))
	if d1.Outcome != OutcomeAccepted || e1 != nil {
		t.Fatalf("first submit: %+v err=%v", d1, e1)
	}
	d2, _ := st.Submit(ctx, recFor("E1", "2.5.1", "2026-09-01T00:00:00Z", "1"))
	if d2.Outcome != OutcomeDuplicate {
		t.Fatalf("third submit outcome=%s want DUPLICATE", d2.Outcome)
	}
	if e2 == nil {
		t.Fatal("duplicate must surface STORAGE_CONFLICT")
	}
	var ie *IngestionError
	if !errors.As(e2, &ie) || ie.Type != ErrStorageConflict {
		t.Fatalf("err type=%v", e2)
	}
	recs, _ := st.Snapshot(ctx)
	if len(recs) != 1 {
		t.Fatalf("duplicate created a second record: %d", len(recs))
	}
	t.Logf("messageId=E1 basis=identical business digest -> %s, snapshot records=%d", d2.Outcome, len(recs))
}

func TestStoreOutOfOrderWinnerDeterministic(t *testing.T) {
	// Deliver stale-first, then fresh: winner must be fresh in both runs.
	for _, reverse := range []bool{true, false} {
		name := "stale-first"
		order := []string{"2.3", "2.5.1"}
		if !reverse {
			name = "fresh-first"
			order[0], order[1] = order[1], order[0]
		}
		t.Run(name, func(t *testing.T) {
			st, _ := newFileStore(testCfg(t, OverflowReject, 100))
			defer st.Close()
			ctx := context.Background()
			effs := map[string]string{"2.3": "2026-08-01T00:00:00Z", "2.5.1": "2026-09-01T00:00:00Z"}
			vals := map[string]string{"2.3": "old", "2.5.1": "new"}
			var outcomes []Outcome
			for _, v := range order {
				d, err := st.Submit(ctx, recFor("E9", v, effs[v], vals[v]))
				if err != nil && d == nil {
					t.Fatal(err)
				}
				outcomes = append(outcomes, d.Outcome)
			}
			recs, _ := st.Snapshot(ctx)
			if len(recs) != 1 || recs[0].Value != "new" || recs[0].SourceVersion != "2.5.1" {
				t.Fatalf("winner wrong: %+v", recs)
			}
			switch name {
			case "fresh-first":
				// Older data arriving late must be explicitly marked.
				if outcomes[1] != OutcomeOutOfOrder {
					t.Fatalf("late stale revision must be OUT_OF_ORDER, got %v", outcomes)
				}
			case "stale-first":
				// Newer data arriving late supersedes; it must still be
				// accepted and end up the winner.
				if outcomes[0] != OutcomeAccepted || outcomes[1] != OutcomeAccepted {
					t.Fatalf("superseding arrival outcomes=%v", outcomes)
				}
			}
			t.Logf("messageId=E9 basis=%s arrival outcomes=%v winner=2.5.1/new (older data never overwrites)", name, outcomes)
		})
	}
}

func TestStoreCapacityRejectAndDegrade(t *testing.T) {
	ctx := context.Background()

	t.Run("reject", func(t *testing.T) {
		st, _ := newFileStore(testCfg(t, OverflowReject, 2))
		defer st.Close()
		if _, err := st.Submit(ctx, recFor("A", "R4", "2026-09-01T00:00:00Z", "1")); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Submit(ctx, recFor("B", "R4", "2026-09-02T00:00:00Z", "1")); err != nil {
			t.Fatal(err)
		}
		d, err := st.Submit(ctx, recFor("C", "R4", "2026-09-03T00:00:00Z", "1"))
		if err == nil || d.Outcome != OutcomeRejected {
			t.Fatalf("over-capacity must reject: d=%v err=%v", d, err)
		}
		// duplicates of known events still recognized after capacity hit
		d2, _ := st.Submit(ctx, recFor("A", "R4", "2026-09-01T00:00:00Z", "1"))
		if d2.Outcome != OutcomeDuplicate {
			t.Fatalf("duplicate handling after limit: %s", d2.Outcome)
		}
		t.Logf("basis=maxRecords=2 reject policy -> C REJECTED, A redelivery still %s", d2.Outcome)
	})

	t.Run("degrade", func(t *testing.T) {
		st, _ := newFileStore(testCfg(t, OverflowDegrade, 1))
		defer st.Close()
		if _, err := st.Submit(ctx, recFor("A", "R4", "2026-09-01T00:00:00Z", "1")); err != nil {
			t.Fatal(err)
		}
		d, err := st.Submit(ctx, recFor("B", "R4", "2026-09-02T00:00:00Z", "1"))
		if err != nil || d.Outcome != OutcomeDegraded {
			t.Fatalf("degrade outcome=%s err=%v", d.Outcome, err)
		}
		recs, _ := st.Snapshot(ctx)
		if len(recs) != 1 {
			t.Fatalf("primary store must stay at cap, got %d", len(recs))
		}
		t.Logf("messageId=B basis=maxRecords=1 degrade policy -> DEGRADED journal, primary records=%d", len(recs))
	})
}

func TestStoreReplayIdempotent(t *testing.T) {
	cfg := testCfg(t, OverflowReject, 100)
	st, _ := newFileStore(cfg)
	ctx := context.Background()
	_, _ = st.Submit(ctx, recFor("A", "R4", "2026-09-01T00:00:00Z", "1"))
	_, _ = st.Submit(ctx, recFor("B", "R4", "2026-09-02T00:00:00Z", "1"))
	_, _ = st.Submit(ctx, recFor("A", "R4", "2026-09-01T00:00:00Z", "1")) // duplicate
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	cfg.ResetOutput = false
	st2, err := newFileStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	recs, _ := st2.Snapshot(ctx)
	if len(recs) != 2 {
		t.Fatalf("replay restored %d records want 2", len(recs))
	}
	// Redelivery after restart still recognized.
	d, _ := st2.Submit(ctx, recFor("A", "R4", "2026-09-01T00:00:00Z", "1"))
	if d.Outcome != OutcomeDuplicate {
		t.Fatalf("post-restart redelivery outcome=%s", d.Outcome)
	}
	decCount := 0
	decs, _ := st2.Decisions(ctx)
	decCount = len(decs)
	t.Logf("basis=journal replay: restored %d distinct events, prior dup remembered, decision rows=%d", len(recs), decCount)
}

func TestStoreSnapshotSorted(t *testing.T) {
	st, _ := newFileStore(testCfg(t, OverflowReject, 100))
	defer st.Close()
	ctx := context.Background()
	inOrder := map[string]string{
		"C": "2026-09-03T00:00:00Z",
		"A": "2026-09-01T00:00:00Z",
		"B": "2026-09-02T00:00:00Z",
	}
	for _, id := range []string{"C", "A", "B"} {
		_, _ = st.Submit(ctx, recFor(id, "R4", inOrder[id], "1"))
	}
	recs, _ := st.Snapshot(ctx)
	if recs[0].EventID != "A" || recs[1].EventID != "B" || recs[2].EventID != "C" {
		t.Fatalf("snapshot not sorted by eventTime: %s %s %s", recs[0].EventID, recs[1].EventID, recs[2].EventID)
	}
}

var _ = filepath.Join // keep filepath import stable across edits
