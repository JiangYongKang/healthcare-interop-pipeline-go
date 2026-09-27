package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Reprocessing the journal history must yield the same effective result
// as incremental processing, across a mix of accepted, duplicate,
// out-of-order, conflicted and resolved deliveries.
func TestReplayConsistentAfterMixedHistory(t *testing.T) {
	cfg := testCfg(t, OverflowReject, 100)
	st, _ := newFileStore(cfg)
	ctx := context.Background()

	st.Submit(ctx, recFor("RA", "2.5.1", "2026-09-01T00:00:00Z", "1"))
	st.Submit(ctx, recFor("RA", "2.5.1", "2026-09-01T00:00:00Z", "1"))      // duplicate
	st.Submit(ctx, recFor("RA", "2.3", "2026-08-01T00:00:00Z", "0"))        // out of order
	st.Submit(ctx, recFor("RB", "R4", "2026-09-02T00:00:00Z", "x"))         // new event
	d, _ := st.Submit(ctx, recFor("RB", "R4", "2026-09-02T00:00:00Z", "y")) // conflict
	if _, err := st.Resolve(ctx, d.ConflictID, AcceptIncoming, "reviewer"); err != nil {
		t.Fatal(err)
	}
	st.Submit(ctx, recFor("RC", "STU3", "2026-09-03T00:00:00Z", "s")) // accepted after review

	live, _ := st.Snapshot(ctx)
	rep, err := st.Replay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Consistent {
		t.Fatalf("replay must match live state: %+v", rep)
	}
	if rep.Chains != 3 || rep.Conflicts != 1 || rep.PendingReviews != 0 {
		t.Fatalf("report counts: %+v", rep)
	}
	st.Close()

	// Reopening replays the journals: identical effective state.
	cfg.ResetOutput = false
	st2, err := newFileStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	replayed, _ := st2.Snapshot(ctx)
	if len(replayed) != len(live) {
		t.Fatalf("replayed %d records, live had %d", len(replayed), len(live))
	}
	for i := range live {
		if businessDigest(replayed[i]) != businessDigest(live[i]) {
			t.Fatalf("record %d differs: live=%+v replayed=%+v", i, live[i], replayed[i])
		}
	}
	rep2, _ := st2.Replay(ctx)
	if !rep2.Consistent {
		t.Fatalf("replay after reopen must be consistent: %+v", rep2)
	}
	t.Logf("basis=mixed history (accept/dup/out-of-order/conflict/resolve) -> replay consistent, chains=%d deliveries=%d conflicts=%d; reopen equals live",
		rep.Chains, rep.Deliveries, rep.Conflicts)
}

// Repeated replays are idempotent: state unchanged, every run journaled
// and queryable.
func TestReplayRepeatedRunsStable(t *testing.T) {
	cfg := testCfg(t, OverflowReject, 100)
	st, _ := newFileStore(cfg)
	defer st.Close()
	ctx := context.Background()
	st.Submit(ctx, recFor("RR", "R4", "2026-09-01T00:00:00Z", "1"))

	before, _ := st.Snapshot(ctx)
	r1, err1 := st.Replay(ctx)
	r2, err2 := st.Replay(ctx)
	if err1 != nil || err2 != nil || !r1.Consistent || !r2.Consistent {
		t.Fatalf("replays: %+v / %+v err=%v/%v", r1, r2, err1, err2)
	}
	after, _ := st.Snapshot(ctx)
	if len(before) != len(after) || businessDigest(before[0]) != businessDigest(after[0]) {
		t.Fatal("replay mutated live state")
	}
	// Both runs are auditable in replay.jsonl.
	f, err := os.ReadFile(filepath.Join(cfg.OutputDir, replayFile))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Count(strings.TrimSpace(string(f)), "\n") + 1
	if lines != 2 {
		t.Fatalf("replay journal must hold 2 reports, got %d", lines)
	}
	t.Logf("messageId=RR basis=two replay runs -> both consistent, state unchanged, replay.jsonl rows=%d", lines)
}

// The replay backlog is bounded; excess requests are refused with a
// stable reason and the refusal stays queryable.
func TestReplayConcurrencyLimit(t *testing.T) {
	cfg := testCfg(t, OverflowReject, 100)
	cfg.MaxReplayTasks = 1
	st, _ := newFileStore(cfg)
	defer st.Close()
	ctx := context.Background()
	st.Submit(ctx, recFor("RL", "R4", "2026-09-01T00:00:00Z", "1"))

	// Occupy the single replay slot, then a concurrent replay is refused.
	fs := st.(*fileStore)
	fs.replaySem <- struct{}{}
	rep, err := st.Replay(ctx)
	<-fs.replaySem

	var ie *IngestionError
	if !errors.As(err, &ie) || ie.Reason != "REPLAY_LIMIT" || ie.Type != ErrUnavailable {
		t.Fatalf("over-limit replay must be refused with REPLAY_LIMIT: %v", err)
	}
	if rep == nil || rep.Consistent {
		t.Fatalf("refusal report: %+v", rep)
	}
	// Refusal is journaled and queryable.
	f, _ := os.ReadFile(filepath.Join(cfg.OutputDir, replayFile))
	if !strings.Contains(string(f), "replay refused") {
		t.Fatalf("refusal must be journaled: %s", f)
	}
	// A slot frees up and the next replay works.
	rep2, err2 := st.Replay(ctx)
	if err2 != nil || !rep2.Consistent {
		t.Fatalf("replay after slot freed: %+v err=%v", rep2, err2)
	}
	t.Logf("messageId=RL basis=maxReplayTasks=1 -> concurrent replay refused (REPLAY_LIMIT, journaled); freed slot replays consistently")
}

// Re-ingesting the same history after a restart is a no-op: every
// redelivery is a duplicate and the effective result is unchanged.
func TestDuplicateRedeliveryAfterRestart(t *testing.T) {
	cfg := testCfg(t, OverflowReject, 100)
	st, _ := newFileStore(cfg)
	ctx := context.Background()
	st.Submit(ctx, recFor("RD", "2.5.1", "2026-09-01T00:00:00Z", "1"))
	st.Submit(ctx, recFor("RD", "2.3", "2026-08-01T00:00:00Z", "0"))
	before, _ := st.Snapshot(ctx)
	st.Close()

	cfg.ResetOutput = false
	st2, _ := newFileStore(cfg)
	defer st2.Close()
	d1, _ := st2.Submit(ctx, recFor("RD", "2.5.1", "2026-09-01T00:00:00Z", "1"))
	d2, _ := st2.Submit(ctx, recFor("RD", "2.3", "2026-08-01T00:00:00Z", "0"))
	if d1.Outcome != OutcomeDuplicate || d2.Outcome != OutcomeDuplicate {
		t.Fatalf("redeliveries after restart: %s / %s", d1.Outcome, d2.Outcome)
	}
	after, _ := st2.Snapshot(ctx)
	if len(after) != len(before) || businessDigest(after[0]) != businessDigest(before[0]) {
		t.Fatal("duplicate redelivery changed effective state")
	}
	t.Logf("messageId=RD basis=restart + full redelivery -> DUPLICATE/DUPLICATE, effective unchanged (digest=%s)",
		shortDigest(businessDigest(after[0])))
}
