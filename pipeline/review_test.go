package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// Same revision coordinates with different content must be parked for
// review with a stable, distinguishable reason; the effective version
// is retained.
func TestConflictDetectedAndParked(t *testing.T) {
	st, err := newFileStore(testCfg(t, OverflowReject, 100))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()

	d1, e1 := st.Submit(ctx, recFor("EC1", "2.5.1", "2026-09-01T00:00:00Z", "36.8"))
	if e1 != nil || d1.Outcome != OutcomeAccepted {
		t.Fatalf("first submit: %v %v", d1, e1)
	}
	d2, e2 := st.Submit(ctx, recFor("EC1", "2.5.1", "2026-09-01T00:00:00Z", "39.9"))
	if d2.Outcome != OutcomeConflict {
		t.Fatalf("conflicting content outcome=%s want CONFLICT", d2.Outcome)
	}
	var ie *IngestionError
	if !errors.As(e2, &ie) || ie.Type != ErrStorageConflict {
		t.Fatalf("conflict must surface STORAGE_CONFLICT, got %v", e2)
	}
	if d2.ConflictID == "" {
		t.Fatal("conflict decision must carry a conflict id")
	}

	// Effective version retained.
	recs, _ := st.Snapshot(ctx)
	if len(recs) != 1 || recs[0].Value != "36.8" {
		t.Fatalf("effective version must be retained: %+v", recs)
	}
	// Stable, distinguishable reason; pending and queryable.
	pend, _ := st.PendingConflicts(ctx)
	if len(pend) != 1 || pend[0].Reason != ReasonContentConflict || pend[0].Status != ConflictPending {
		t.Fatalf("pending conflicts: %+v", pend)
	}
	// Chain view shows PENDING_REVIEW.
	chains, _ := st.Chains(ctx)
	if chains[0].Status != ChainPendingReview || chains[0].PendingConflicts != 1 {
		t.Fatalf("chain status=%s pending=%d", chains[0].Status, chains[0].PendingConflicts)
	}
	t.Logf("messageId=EC1 basis=same coords different content -> %s conflictId=%s effective retained=36.8",
		d2.Outcome, d2.ConflictID)
}

// Review conclusions drive subsequent processing atomically; applying
// the same conclusion repeatedly creates no duplicate state or records.
func TestResolveAcceptIncomingThenIdempotent(t *testing.T) {
	cfg := testCfg(t, OverflowReject, 100)
	st, _ := newFileStore(cfg)
	ctx := context.Background()
	st.Submit(ctx, recFor("EC2", "R4", "2026-09-01T00:00:00Z", "old"))
	d, _ := st.Submit(ctx, recFor("EC2", "R4", "2026-09-01T00:00:00Z", "new"))
	cid := d.ConflictID

	res, err := st.Resolve(ctx, cid, AcceptIncoming, "reviewer-1")
	if err != nil || !res.Applied {
		t.Fatalf("resolve: %+v err=%v", res, err)
	}
	recs, _ := st.Snapshot(ctx)
	if len(recs) != 1 || recs[0].Value != "new" {
		t.Fatalf("accept-incoming must flip effective atomically: %+v", recs)
	}
	if pend, _ := st.PendingConflicts(ctx); len(pend) != 0 {
		t.Fatalf("no pending conflict may remain: %+v", pend)
	}

	// Idempotent re-application: no new state, no new records.
	decsBefore, _ := st.Decisions(ctx)
	res2, err := st.Resolve(ctx, cid, AcceptIncoming, "reviewer-1")
	if err != nil || res2.Applied {
		t.Fatalf("re-applying same conclusion must be idempotent: %+v err=%v", res2, err)
	}
	decsAfter, _ := st.Decisions(ctx)
	if len(decsAfter) != len(decsBefore) {
		t.Fatalf("idempotent resolve wrote new decisions: %d -> %d", len(decsBefore), len(decsAfter))
	}
	recs2, _ := st.Snapshot(ctx)
	if len(recs2) != 1 || recs2[0].Value != "new" {
		t.Fatalf("idempotent resolve changed state: %+v", recs2)
	}

	// A different conclusion on a resolved conflict is rejected.
	_, err = st.Resolve(ctx, cid, KeepCurrent, "reviewer-2")
	var ie *IngestionError
	if !errors.As(err, &ie) || ie.Reason != "CONFLICT_ALREADY_RESOLVED" {
		t.Fatalf("re-resolve must be rejected, got %v", err)
	}
	st.Close()

	// Resolution survives restart (reviews journal is the commit record).
	cfg.ResetOutput = false
	st2, err := newFileStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	recs3, _ := st2.Snapshot(ctx)
	if len(recs3) != 1 || recs3[0].Value != "new" {
		t.Fatalf("resolution must survive restart: %+v", recs3)
	}
	if pend, _ := st2.PendingConflicts(ctx); len(pend) != 0 {
		t.Fatalf("resolved conflict must not reappear as pending: %+v", pend)
	}
	t.Logf("messageId=EC2 basis=ACCEPT_INCOMING applied once (seq=%d); re-apply idempotent; restart keeps winner=new",
		res.Conflict.Resolution.Seq)
}

func TestResolveKeepCurrentAndUnknown(t *testing.T) {
	st, _ := newFileStore(testCfg(t, OverflowReject, 100))
	defer st.Close()
	ctx := context.Background()
	st.Submit(ctx, recFor("EC3", "R4", "2026-09-01T00:00:00Z", "keep"))
	d, _ := st.Submit(ctx, recFor("EC3", "R4", "2026-09-01T00:00:00Z", "drop"))

	res, err := st.Resolve(ctx, d.ConflictID, KeepCurrent, "reviewer-1")
	if err != nil || !res.Applied {
		t.Fatalf("resolve keep-current: %+v %v", res, err)
	}
	recs, _ := st.Snapshot(ctx)
	if len(recs) != 1 || recs[0].Value != "keep" {
		t.Fatalf("keep-current must retain effective: %+v", recs)
	}

	if _, err := st.Resolve(ctx, "CON-doesnotexist", KeepCurrent, "r"); err == nil {
		t.Fatal("unknown conflict must error")
	} else {
		var ie *IngestionError
		if !errors.As(err, &ie) || ie.Reason != "UNKNOWN_CONFLICT" {
			t.Fatalf("unknown conflict reason: %v", err)
		}
	}
	if _, err := st.Resolve(ctx, d.ConflictID, "SIDE_WITH_NOBODY", "r"); err == nil {
		t.Fatal("invalid choice must error")
	}
	t.Logf("messageId=EC3 basis=KEEP_CURRENT retains effective=keep; unknown/invalid resolutions rejected with stable reasons")
}

// The pending-review backlog is bounded; overflow follows the
// configured policy and stays queryable without half-applied state.
func TestPendingReviewBacklogLimit(t *testing.T) {
	ctx := context.Background()

	t.Run("reject", func(t *testing.T) {
		cfg := testCfg(t, OverflowReject, 100)
		cfg.MaxPendingReviews = 1
		st, _ := newFileStore(cfg)
		defer st.Close()
		st.Submit(ctx, recFor("EB1", "R4", "2026-09-01T00:00:00Z", "a"))
		st.Submit(ctx, recFor("EB1", "R4", "2026-09-01T00:00:00Z", "b")) // conflict 1 (pending=1)
		st.Submit(ctx, recFor("EB2", "R4", "2026-09-02T00:00:00Z", "a"))
		d, err := st.Submit(ctx, recFor("EB2", "R4", "2026-09-02T00:00:00Z", "b"))
		if d.Outcome != OutcomeRejected || err == nil {
			t.Fatalf("backlog overflow must reject: %s err=%v", d.Outcome, err)
		}
		var ie *IngestionError
		if !errors.As(err, &ie) || ie.Type != ErrStorageConflict {
			t.Fatalf("overflow error type: %v", err)
		}
		// No half state: EB2 keeps its first revision, one pending left.
		recs, _ := st.Snapshot(ctx)
		if len(recs) != 2 {
			t.Fatalf("records after overflow: %+v", recs)
		}
		pend, _ := st.PendingConflicts(ctx)
		if len(pend) != 1 {
			t.Fatalf("pending must stay at cap: %+v", pend)
		}
		t.Logf("messageId=EB2 basis=maxPendingReviews=1 reject policy -> %s, pending=%d (queryable, no partial write)",
			d.Outcome, len(pend))
	})

	t.Run("degrade", func(t *testing.T) {
		cfg := testCfg(t, OverflowDegrade, 100)
		cfg.MaxPendingReviews = 1
		st, _ := newFileStore(cfg)
		defer st.Close()
		st.Submit(ctx, recFor("EB1", "R4", "2026-09-01T00:00:00Z", "a"))
		st.Submit(ctx, recFor("EB1", "R4", "2026-09-01T00:00:00Z", "b"))
		st.Submit(ctx, recFor("EB2", "R4", "2026-09-02T00:00:00Z", "a"))
		d, err := st.Submit(ctx, recFor("EB2", "R4", "2026-09-02T00:00:00Z", "b"))
		if d.Outcome != OutcomeDegraded || err != nil {
			t.Fatalf("backlog overflow must degrade: %s err=%v", d.Outcome, err)
		}
		recs, _ := st.Snapshot(ctx)
		if len(recs) != 2 {
			t.Fatalf("primary store unaffected by degrade: %+v", recs)
		}
		t.Logf("messageId=EB2 basis=maxPendingReviews=1 degrade policy -> %s to degraded journal", d.Outcome)
	})
}

// Concurrent ingestion and review must serialize deterministically:
// exactly one conflict, one resolution, and a replay-consistent state.
func TestConcurrentIngestAndReview(t *testing.T) {
	eng := newTestEngine(t, 8)
	ctx := context.Background()

	const writers = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	outcomes := map[Outcome]int{}
	conflictIDs := map[string]int{}

	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			// Every writer sends the same event at the same coordinates
			// but with writer-specific content: exactly one wins, the
			// rest conflict (or duplicate).
			r := eng.Ingest(ctx, fmt.Sprintf("w%d", g),
				hl7MsgValue("EVT-CONC", "MRN-C", "2.5.1", "20260901101500", fmt.Sprintf("v%d", g)))
			mu.Lock()
			outcomes[r.Outcome]++
			if r.ConflictID != "" {
				conflictIDs[r.ConflictID]++
			}
			mu.Unlock()
		}(g)
	}
	wg.Wait()

	if outcomes[OutcomeAccepted] != 1 {
		t.Fatalf("exactly one delivery may be accepted, outcomes=%v", outcomes)
	}
	if outcomes[OutcomeConflict] != writers-1 {
		t.Fatalf("all other deliveries must conflict, outcomes=%v", outcomes)
	}

	// Resolve every pending conflict concurrently; each resolves once.
	pend, _ := eng.PendingConflicts(ctx)
	var rwg sync.WaitGroup
	for _, c := range pend {
		for _, choice := range []ResolutionChoice{KeepCurrent, AcceptIncoming} {
			rwg.Add(1)
			go func(cid string, ch ResolutionChoice) {
				defer rwg.Done()
				_, _ = eng.ResolveConflict(ctx, cid, ch, "racer")
			}(c.ConflictID, choice)
		}
	}
	rwg.Wait()

	// Deterministic end state: no pending conflicts, exactly one
	// effective record, and replay agrees with the live state.
	if pend2, _ := eng.PendingConflicts(ctx); len(pend2) != 0 {
		t.Fatalf("pending after concurrent review: %+v", pend2)
	}
	recs, _ := eng.Snapshot(ctx)
	if len(recs) != 1 {
		t.Fatalf("one effective record expected, got %+v", recs)
	}
	rep, err := eng.Replay(ctx)
	if err != nil || !rep.Consistent {
		t.Fatalf("replay after concurrent review: %+v err=%v", rep, err)
	}
	t.Logf("messageId=EVT-CONC basis=%d concurrent writers -> accepted=1 conflict=%d; concurrent review -> pending=0 replay consistent (chains=%d deliveries=%d)",
		writers, outcomes[OutcomeConflict], rep.Chains, rep.Deliveries)
}

// hl7MsgValue is hl7Msg with an explicit observation value.
func hl7MsgValue(eid, mrn, ver, tstamp, value string) []byte {
	return []byte(fmt.Sprintf(
		"MSH|^~\\&|LAB|HOSP|EHR|HOSP|%s||ORU^R01|%s|P|%s\rPID|1||%s^^^HOSP^MR||DOE^JOHN\rOBX|1||8310-5^Temp^LN||%s|cel|||||F|||%s\r",
		tstamp, eid, ver, mrn, value, tstamp))
}
