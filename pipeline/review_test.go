package pipeline

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// setupConflictChain ingests c03+c04 so EVT-5002 is PENDING_REVIEW and
// returns the engine plus the seq of the conflicting delivery.
func setupConflictChain(t *testing.T) (*Engine, *ChainView) {
	t.Helper()
	eng := newTestEngine(t, 4)
	ctx := context.Background()
	dir := consistencyDir(t)
	eng.IngestFile(ctx, filepath.Join(dir, "c03_fhir_evt5002.json"))
	eng.IngestFile(ctx, filepath.Join(dir, "c04_hl7_evt5002_conflict.hl7"))
	view, err := eng.Chain(ctx, "EVT-5002")
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != ChainPendingReview {
		t.Fatalf("precondition: chain must be PENDING_REVIEW, got %s", view.Status)
	}
	return eng, view
}

// TestReviewResolveUseDelivery: a review conclusion pins the conflicting
// delivery as effective; chain, snapshot and derived output move
// atomically; re-applying the same conclusion is a no-op.
func TestReviewResolveUseDelivery(t *testing.T) {
	eng, view := setupConflictChain(t)
	ctx := context.Background()
	var conflictSeq int64
	for _, d := range view.Deliveries {
		if d.Relation == RelationConflictPending {
			conflictSeq = d.Seq
		}
	}
	req := ReviewRequest{
		ReviewID: "RVW-1", EventID: "EVT-5002", Action: ReviewUseDelivery,
		TargetSeq: conflictSeq, Reason: "HL7 lab value confirmed by reviewer", Reviewer: "dr.smith",
	}
	rec, dec, err := eng.Review(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Outcome != OutcomeReviewApplied || dec.ReviewID != "RVW-1" {
		t.Fatalf("decision=%+v", dec)
	}
	view2, _ := eng.Chain(ctx, "EVT-5002")
	if view2.Status != ChainEffective {
		t.Fatalf("chain must leave PENDING_REVIEW, got %s", view2.Status)
	}
	if view2.Current.Value != "95.0" {
		t.Fatalf("reviewed value must become effective, got %s", view2.Current.Value)
	}
	rel := map[string]int{}
	for _, d := range view2.Deliveries {
		rel[d.Relation]++
	}
	if rel[RelationReviewPinned] != 1 || rel[RelationSuperseded] != 1 {
		t.Fatalf("relations after review=%v", rel)
	}
	if len(view2.Reviews) != 1 || view2.Reviews[0].ReviewID != "RVW-1" {
		t.Fatalf("review must be visible in chain view: %+v", view2.Reviews)
	}
	recs, _ := eng.Snapshot(ctx)
	if len(recs) != 1 || recs[0].Value != "95.0" {
		t.Fatalf("snapshot must follow review: %+v", recs)
	}

	// Idempotence: same conclusion again -> duplicate flag, no new
	// decision, no state change.
	decsBefore, _ := eng.Decisions(ctx)
	rec2, dec2, err := eng.Review(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if !rec2.Duplicate || dec2 != nil || rec2.AppliedSeq != rec.AppliedSeq {
		t.Fatalf("re-apply must be a reported no-op: %+v dec=%v", rec2, dec2)
	}
	decsAfter, _ := eng.Decisions(ctx)
	if len(decsAfter) != len(decsBefore) {
		t.Fatalf("duplicate review must not append decisions: %d -> %d", len(decsBefore), len(decsAfter))
	}
	view3, _ := eng.Chain(ctx, "EVT-5002")
	if len(view3.Reviews) != 1 || view3.Current.Value != "95.0" {
		t.Fatalf("duplicate review must not duplicate state: %+v", view3.Reviews)
	}

	// Same review id with a different conclusion is rejected.
	_, _, err = eng.Review(ctx, ReviewRequest{
		ReviewID: "RVW-1", EventID: "EVT-5002", Action: ReviewKeepCurrent,
	})
	var ie *IngestionError
	if !errors.As(err, &ie) || ie.Reason != "REVIEW_ID_CONFLICT" {
		t.Fatalf("conflicting review id must be rejected: %v", err)
	}
	t.Logf("messageId=EVT-5002 basis=review RVW-1 USE_DELIVERY seq=%d -> value 95.0 effective; re-apply idempotent (duplicate=%v); reviewId conflict rejected",
		conflictSeq, rec2.Duplicate)
}

// TestReviewKeepCurrent: the reviewer confirms the provisional version.
func TestReviewKeepCurrent(t *testing.T) {
	eng, _ := setupConflictChain(t)
	ctx := context.Background()
	_, _, err := eng.Review(ctx, ReviewRequest{
		ReviewID: "RVW-K", EventID: "EVT-5002", Action: ReviewKeepCurrent,
		Reason: "FHIR value is the system of record", Reviewer: "dr.jones",
	})
	if err != nil {
		t.Fatal(err)
	}
	view, _ := eng.Chain(ctx, "EVT-5002")
	if view.Status != ChainEffective || view.Current.Value != "71.2" {
		t.Fatalf("KEEP_CURRENT must retain provisional: status=%s value=%s", view.Status, view.Current.Value)
	}
	pending, _ := eng.PendingReviews(ctx)
	if len(pending) != 0 {
		t.Fatalf("pending must be empty after review: %v", pending)
	}
	t.Logf("messageId=EVT-5002 basis=review RVW-K KEEP_CURRENT -> provisional 71.2 confirmed, pending cleared")
}

// TestReviewValidationAndUnknownTargets covers the review contract.
func TestReviewValidationAndUnknownTargets(t *testing.T) {
	eng, _ := setupConflictChain(t)
	ctx := context.Background()
	cases := []ReviewRequest{
		{ReviewID: "", EventID: "EVT-5002", Action: ReviewKeepCurrent},
		{ReviewID: "R1", EventID: "", Action: ReviewKeepCurrent},
		{ReviewID: "R2", EventID: "EVT-5002", Action: "BOGUS"},
		{ReviewID: "R3", EventID: "EVT-5002", Action: ReviewUseDelivery},                  // no target
		{ReviewID: "R4", EventID: "EVT-5002", Action: ReviewUseDelivery, TargetSeq: 9999}, // unknown delivery
		{ReviewID: "R5", EventID: "NOPE", Action: ReviewKeepCurrent},                      // unknown chain
	}
	for i, req := range cases {
		if _, _, err := eng.Review(ctx, req); err == nil {
			t.Fatalf("case %d must fail: %+v", i, req)
		} else {
			t.Logf("case %d basis=contract violation -> %v", i, err)
		}
	}
}

// TestPendingReviewBacklogLimit: the pending-review backlog is bounded;
// overflow follows the configured policy and stays queryable.
func TestPendingReviewBacklogLimit(t *testing.T) {
	mkCfg := func(policy OverflowPolicy) Config {
		cfg := DefaultConfig()
		cfg.OutputDir = t.TempDir()
		cfg.ResetOutput = true
		cfg.MaxRecords = 100
		cfg.MaxInFlight = 4
		cfg.MaxPendingReviews = 1
		cfg.OverflowPolicy = policy
		return cfg
	}
	// Two distinct events, each getting a same-time conflicting value.
	conflictPair := func(id, v1, v2 string) ([]byte, []byte) {
		a := []byte(fmt.Sprintf(`{"resourceType":"Observation","id":"%s","status":"final","subject":{"reference":"Patient/M"},"code":{"coding":[{"system":"LN","code":"8310-5"}]},"valueQuantity":{"value":%s,"unit":"cel"},"effectiveDateTime":"2026-09-10T09:00:00Z"}`, id, v1))
		b := []byte(fmt.Sprintf(`{"resourceType":"Observation","id":"%s","status":"final","subject":{"reference":"Patient/M"},"code":{"coding":[{"system":"LN","code":"8310-5"}]},"valueQuantity":{"value":%s,"unit":"cel"},"effectiveDateTime":"2026-09-10T09:00:00Z"}`, id, v2))
		return a, b
	}

	t.Run("reject", func(t *testing.T) {
		cfg := mkCfg(OverflowReject)
		eng, err := NewEngine(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer eng.Close()
		ctx := context.Background()
		a1, b1 := conflictPair("PC-1", "1", "2")
		a2, b2 := conflictPair("PC-2", "3", "4")
		eng.Ingest(ctx, "a1", a1)
		if r := eng.Ingest(ctx, "b1", b1); r.Outcome != OutcomeConflict {
			t.Fatalf("first conflict must pend: %s", r.Outcome)
		}
		eng.Ingest(ctx, "a2", a2)
		r := eng.Ingest(ctx, "b2", b2) // backlog full (1 pending)
		if r.Outcome != OutcomeRejected || r.Error == nil || r.Error.Type != ErrStorageConflict {
			t.Fatalf("overflow must reject: %+v err=%v", r, r.Error)
		}
		decs, _ := eng.Decisions(ctx)
		var found bool
		for _, d := range decs {
			if d.EventID == "PC-2" && d.Outcome == OutcomeRejected {
				found = true
			}
		}
		if !found {
			t.Fatal("overflow rejection must stay queryable in the decision journal")
		}
		pending, _ := eng.PendingReviews(ctx)
		if len(pending) != 1 {
			t.Fatalf("pending must stay at cap: %v", pending)
		}
		t.Logf("messageId=PC-2 basis=MaxPendingReviews=1 reject policy -> REJECTED, queryable, pending=%v", pending)
	})

	t.Run("degrade", func(t *testing.T) {
		cfg := mkCfg(OverflowDegrade)
		eng, err := NewEngine(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer eng.Close()
		ctx := context.Background()
		a1, b1 := conflictPair("PD-1", "1", "2")
		a2, b2 := conflictPair("PD-2", "3", "4")
		eng.Ingest(ctx, "a1", a1)
		eng.Ingest(ctx, "b1", b1)
		eng.Ingest(ctx, "a2", a2)
		r := eng.Ingest(ctx, "b2", b2)
		if r.Outcome != OutcomeDegraded {
			t.Fatalf("overflow must degrade: %s", r.Outcome)
		}
		pending, _ := eng.PendingReviews(ctx)
		if len(pending) != 1 {
			t.Fatalf("degraded conflict must not enter backlog: %v", pending)
		}
		t.Logf("messageId=PD-2 basis=MaxPendingReviews=1 degrade policy -> DEGRADED journal, pending=%v", pending)
	})
}

// TestConcurrentIngestAndReview hammers one chain with concurrent
// ingestions and review applications; final state must be consistent.
func TestConcurrentIngestAndReview(t *testing.T) {
	eng := newTestEngine(t, 8)
	ctx := context.Background()
	mk := func(id, val string) []byte {
		return []byte(fmt.Sprintf(`{"resourceType":"Observation","id":"%s","status":"final","subject":{"reference":"Patient/CR"},"code":{"coding":[{"system":"LN","code":"8310-5"}]},"valueQuantity":{"value":%s,"unit":"cel"},"effectiveDateTime":"2026-09-10T09:00:00Z"}`, id, val))
	}
	eng.Ingest(ctx, "seed", mk("CR-1", "36.5"))

	var wg sync.WaitGroup
	// Concurrent conflicting deliveries of the same event.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eng.Ingest(ctx, fmt.Sprintf("c%d", i), mk("CR-1", fmt.Sprintf("4%d.0", i)))
		}(i)
	}
	wg.Wait() // all ingestions settled before picking a review target
	// Concurrent reviews (same id, same conclusion -> idempotent).
	view, _ := eng.Chain(ctx, "CR-1")
	var target int64
	for _, d := range view.Deliveries {
		if d.Relation == RelationConflictPending {
			target = d.Seq
			break
		}
	}
	if target == 0 {
		t.Fatal("expected at least one pending conflict delivery")
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = eng.Review(ctx, ReviewRequest{
				ReviewID: "RVW-CR", EventID: "CR-1",
				Action: ReviewUseDelivery, TargetSeq: target,
			})
		}()
	}
	wg.Wait()

	view, _ = eng.Chain(ctx, "CR-1")
	if view.Status != ChainEffective {
		t.Fatalf("chain must be resolved exactly once, status=%s", view.Status)
	}
	if len(view.Reviews) != 1 {
		t.Fatalf("concurrent identical reviews must collapse to one record: %+v", view.Reviews)
	}
	decs, _ := eng.Decisions(ctx)
	applied := 0
	for _, d := range decs {
		if d.Outcome == OutcomeReviewApplied {
			applied++
		}
	}
	if applied != 1 {
		t.Fatalf("exactly one REVIEW_APPLIED decision expected, got %d", applied)
	}
	recs, _ := eng.Snapshot(ctx)
	if len(recs) != 1 {
		t.Fatalf("one effective record expected, got %d", len(recs))
	}
	t.Logf("messageId=CR-1 basis=8 concurrent conflicts + 4 concurrent identical reviews -> 1 review record, 1 REVIEW_APPLIED, chain EFFECTIVE")
}
