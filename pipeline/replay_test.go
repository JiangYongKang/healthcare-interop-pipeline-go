package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// ingestConsistencyCorpus feeds the whole consistency sample directory
// and resolves EVT-5002 via review, returning the engine.
func ingestConsistencyCorpus(t *testing.T, eng *Engine) {
	t.Helper()
	ctx := context.Background()
	dir := consistencyDir(t)
	paths, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("consistency samples missing: %v", err)
	}
	paths = filterFiles(paths)
	results := eng.IngestPaths(ctx, paths)
	counts := map[Outcome]int{}
	for _, r := range results {
		counts[r.Outcome]++
	}
	// Deterministic labels for the sorted corpus.
	if counts[OutcomeAccepted] != 5 || counts[OutcomeOutOfOrder] != 2 || counts[OutcomeConflict] != 2 {
		t.Fatalf("unexpected corpus outcomes: %v", counts)
	}
	// Resolve the EVT-5002 value conflict by pinning the HL7 delivery.
	view, err := eng.Chain(ctx, "EVT-5002")
	if err != nil {
		t.Fatal(err)
	}
	var target int64
	for _, d := range view.Deliveries {
		if d.Relation == RelationConflictPending {
			target = d.Seq
		}
	}
	if _, _, err := eng.Review(ctx, ReviewRequest{
		ReviewID: "RVW-5002", EventID: "EVT-5002",
		Action: ReviewUseDelivery, TargetSeq: target, Reviewer: "dr.smith",
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("basis=sorted consistency corpus: %v; EVT-5002 resolved by RVW-5002", counts)
}

// snapshotDigests maps eventID -> business digest of the effective
// version, the comparable essence of the external result.
func snapshotDigests(t *testing.T, eng *Engine) map[string]string {
	t.Helper()
	recs, err := eng.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, r := range recs {
		out[r.EventID] = businessDigest(r)
	}
	return out
}

// TestReplayEqualsIncremental: rebuilding from history must produce
// exactly the effective state that incremental processing reached.
func TestReplayEqualsIncremental(t *testing.T) {
	eng := newTestEngine(t, 8)
	ingestConsistencyCorpus(t, eng)
	before := snapshotDigests(t, eng)

	rep, err := eng.SubmitReplay(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Mismatches != 0 {
		t.Fatalf("replay diverged from incremental state: %+v", rep)
	}
	if !rep.Replaced || rep.Events != 4 {
		t.Fatalf("report=%+v", rep)
	}
	after := snapshotDigests(t, eng)
	if len(before) != len(after) {
		t.Fatalf("snapshot size changed across replay: %d -> %d", len(before), len(after))
	}
	for id, d := range before {
		if after[id] != d {
			t.Fatalf("event %s effective version changed across replay", id)
		}
	}
	// Pending set must survive the rebuild: EVT-5004 still unreviewed.
	pending, _ := eng.PendingReviews(context.Background())
	if len(pending) != 1 || pending[0] != "EVT-5004" {
		t.Fatalf("pending after replay=%v", pending)
	}
	t.Logf("basis=replay report=%+v: incremental == historical reprocessing, pending preserved=%v", rep, pending)
}

// TestRestartReplayRestoresEverything: close + reopen replays journals;
// chains, reviews, pending state and idempotence memory all survive.
func TestRestartReplayRestoresEverything(t *testing.T) {
	cfg := DefaultConfig()
	cfg.OutputDir = t.TempDir()
	cfg.ResetOutput = true
	cfg.MaxRecords = 100
	cfg.MaxInFlight = 4

	eng, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ingestConsistencyCorpus(t, eng)
	before := snapshotDigests(t, eng)
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	cfg.ResetOutput = false
	eng2, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng2.Close()
	after := snapshotDigests(t, eng2)
	if len(before) != len(after) {
		t.Fatalf("restart changed snapshot size: %d -> %d", len(before), len(after))
	}
	for id, d := range before {
		if after[id] != d {
			t.Fatalf("event %s effective version changed across restart", id)
		}
	}
	ctx := context.Background()
	// Review conclusions are remembered: re-applying is a no-op duplicate.
	rec, dec, err := eng2.Review(ctx, ReviewRequest{
		ReviewID: "RVW-5002", EventID: "EVT-5002",
		Action: ReviewUseDelivery, TargetSeq: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Duplicate || dec != nil {
		t.Fatalf("review idempotence must survive restart: %+v", rec)
	}
	// Pending state survives.
	pending, _ := eng2.PendingReviews(ctx)
	if len(pending) != 1 || pending[0] != "EVT-5004" {
		t.Fatalf("pending after restart=%v", pending)
	}
	// Chain view is fully rebuilt with deliveries and reviews.
	view, err := eng2.Chain(ctx, "EVT-5002")
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Deliveries) != 2 || len(view.Reviews) != 1 || view.Current.Value != "95.0" {
		t.Fatalf("chain view after restart: deliveries=%d reviews=%d value=%s",
			len(view.Deliveries), len(view.Reviews), view.Current.Value)
	}
	t.Logf("basis=journal replay across restart: %d events, reviews remembered (duplicate=%v), pending=%v, EVT-5002 value=95.0",
		len(after), rec.Duplicate, pending)
}

// TestArrivalOrderIndependence: any interleaving of the same revisions
// yields the same effective clinical content.
func TestArrivalOrderIndependence(t *testing.T) {
	dir := consistencyDir(t)
	files := []string{
		"c05_hl7_evt5003.hl7", "c06_hl7_evt5003_late_old.hl7", "c07_fhir_evt5003_amended.json",
	}
	orders := [][]int{{0, 1, 2}, {2, 1, 0}, {1, 2, 0}, {2, 0, 1}}
	var wantVal, wantStatus string
	for i, order := range orders {
		eng := newTestEngine(t, 4)
		ctx := context.Background()
		for _, idx := range order {
			r := eng.IngestFile(ctx, filepath.Join(dir, files[idx]))
			if r.Outcome == OutcomeRejected {
				t.Fatalf("order %v: unexpected rejection: %+v", order, r.Error)
			}
		}
		view, err := eng.Chain(ctx, "EVT-5003")
		if err != nil {
			t.Fatal(err)
		}
		if view.Status != ChainEffective {
			t.Fatalf("order %v: chain must be EFFECTIVE, got %s", order, view.Status)
		}
		if i == 0 {
			wantVal, wantStatus = view.Current.Value, view.Current.Status
		} else if view.Current.Value != wantVal || view.Current.Status != wantStatus {
			t.Fatalf("order %v: effective=(%s,%s) want (%s,%s)",
				order, view.Current.Value, view.Current.Status, wantVal, wantStatus)
		}
		if len(view.Deliveries) != 3 {
			t.Fatalf("order %v: all 3 deliveries must be retained, got %d", order, len(view.Deliveries))
		}
		t.Logf("order=%v basis=deterministic adjudication -> effective value=%s status=%s (arrival order irrelevant)",
			order, view.Current.Value, view.Current.Status)
		eng.Close()
	}
	if wantVal != "122" || wantStatus != StatusAmended {
		t.Fatalf("the amended revision must always win, got %s/%s", wantVal, wantStatus)
	}
}

// blockingStore blocks ReplayConsistency until the gate opens and
// signals when a replay was entered, so tests can deterministically
// park the replay worker and fill the queue.
type blockingStore struct {
	Store
	entered chan struct{}
	gate    chan struct{}
}

func (b *blockingStore) ReplayConsistency(ctx context.Context) (*ReplayReport, error) {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.gate
	return b.Store.ReplayConsistency(ctx)
}

// TestReplayBacklogLimit: the replay task queue is bounded; overflow
// follows the configured policy and stays queryable.
func TestReplayBacklogLimit(t *testing.T) {
	newBlockingEngine := func(t *testing.T, policy OverflowPolicy) (*Engine, *blockingStore) {
		cfg := DefaultConfig()
		cfg.OutputDir = t.TempDir()
		cfg.ResetOutput = true
		cfg.MaxRecords = 100
		cfg.MaxInFlight = 2
		cfg.MaxReplayTasks = 1
		cfg.OverflowPolicy = policy
		eng, err := NewEngine(cfg)
		if err != nil {
			t.Fatal(err)
		}
		bs := &blockingStore{Store: eng.store, entered: make(chan struct{}, 1), gate: make(chan struct{})}
		eng.store = bs
		t.Cleanup(func() {
			select {
			case <-bs.gate:
			default:
				close(bs.gate)
			}
			eng.Close()
		})
		return eng, bs
	}
	// parkWorker blocks the replay worker with one in-flight task and
	// fills the bounded queue with a second, deterministically.
	parkWorker := func(t *testing.T, eng *Engine, bs *blockingStore) chan error {
		t.Helper()
		done := make(chan error, 1)
		go func() { _, err := eng.SubmitReplay(context.Background()); done <- err }()
		<-bs.entered                                                // worker is now provably blocked inside the replay
		eng.replayQ <- &replayTask{res: make(chan replayResult, 1)} // queue now full
		return done
	}

	t.Run("reject", func(t *testing.T) {
		eng, bs := newBlockingEngine(t, OverflowReject)
		ctx := context.Background()
		done := parkWorker(t, eng, bs)
		_, err := eng.SubmitReplay(ctx) // queue full -> rejected
		var ie *IngestionError
		if !errors.As(err, &ie) || ie.Reason != "REPLAY_BACKLOG_FULL" {
			t.Fatalf("full replay queue must reject: %v", err)
		}
		decs, _ := eng.Decisions(ctx)
		var noted bool
		for _, d := range decs {
			if d.EventID == "_replay" && d.Outcome == OutcomeRejected {
				noted = true
			}
		}
		if !noted {
			t.Fatal("replay overflow must stay queryable in the decision journal")
		}
		close(bs.gate) // release the worker; queued tasks run to completion
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		t.Log("basis=MaxReplayTasks=1 reject policy: overflow replay -> REPLAY_BACKLOG_FULL, journaled, queued tasks complete")
	})

	t.Run("degrade", func(t *testing.T) {
		eng, bs := newBlockingEngine(t, OverflowDegrade)
		ctx := context.Background()
		done := parkWorker(t, eng, bs)
		rep, err := eng.SubmitReplay(ctx) // full -> degraded skip
		if err != nil || rep != nil {
			t.Fatalf("degrade must skip without error: rep=%v err=%v", rep, err)
		}
		decs, _ := eng.Decisions(ctx)
		var noted bool
		for _, d := range decs {
			if d.EventID == "_replay" && d.Outcome == OutcomeDegraded {
				noted = true
			}
		}
		if !noted {
			t.Fatal("degraded replay skip must stay queryable")
		}
		close(bs.gate)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		t.Log("basis=MaxReplayTasks=1 degrade policy: overflow replay skipped + journaled DEGRADED")
	})
}

// TestDerivedOutputAtomicallyCurrent: records.jsonl always reflects the
// effective set right after each mutating commit (ingest supersede,
// review), never a half-written intermediate.
func TestDerivedOutputAtomicallyCurrent(t *testing.T) {
	cfg := DefaultConfig()
	cfg.OutputDir = t.TempDir()
	cfg.ResetOutput = true
	cfg.MaxRecords = 100
	cfg.MaxInFlight = 4
	eng, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	ingestConsistencyCorpus(t, eng)

	readDerived := func() map[string]string {
		b, err := os.ReadFile(filepath.Join(cfg.OutputDir, recordsFile))
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]string{}
		for _, line := range splitLines(string(b)) {
			if line == "" {
				continue
			}
			var r Record
			if err := json.Unmarshal([]byte(line), &r); err != nil {
				t.Fatalf("derived file must never be torn: %v", err)
			}
			out[r.EventID] = r.Value
		}
		return out
	}
	derived := readDerived()
	if derived["EVT-5002"] != "95.0" || derived["EVT-5003"] != "122" {
		t.Fatalf("derived output must reflect review + supersede atomically: %v", derived)
	}
	if len(derived) != 4 {
		t.Fatalf("derived must hold all 4 effective events: %v", derived)
	}
	t.Logf("basis=records.jsonl rewritten tmp+rename per commit: EVT-5002=%s EVT-5003=%s events=%d",
		derived["EVT-5002"], derived["EVT-5003"], len(derived))
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}
