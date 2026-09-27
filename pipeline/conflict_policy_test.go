package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

// recWith builds a de-identified observation record with explicit
// coordinates, so conflict policy tests do not depend on a parser.
func recWith(id, patient, version, eff, status, value string) *Record {
	r := &Record{
		EventID: id, EventType: EventObservation, PatientRef: patient,
		SourceFormat: "HL7v2", SourceVersion: version, Status: status,
		EventTime: eff, EffectiveTime: eff, Code: "8310-5", Value: value, Unit: "Cel",
	}
	_ = NewDeidentifier("local-dev-salt-v1").DeidentifyRecord(r)
	return r
}

// Regression for the reported defect: a higher-version delivery at the
// same clinical effective time with different content used to silently
// overwrite the effective result. It must now park for review and retain
// the original effective version.
func TestHigherVersionDoesNotSilentlyOverwrite(t *testing.T) {
	st, _ := newFileStore(testCfg(t, OverflowReject, 100))
	defer st.Close()
	ctx := context.Background()

	d1, e1 := st.Submit(ctx, recWith("EVT-HV", "MRN-1", "2.3", "2026-09-01T10:00:00Z", StatusFinal, "36.8"))
	if e1 != nil || d1.Outcome != OutcomeAccepted {
		t.Fatalf("first submit: %+v %v", d1, e1)
	}
	// Same event, same clinical time, same patient, but a "higher" version
	// (R4) carrying a different result value.
	d2, e2 := st.Submit(ctx, recWith("EVT-HV", "MRN-1", "R4", "2026-09-01T10:00:00Z", StatusFinal, "40.2"))
	if d2.Outcome != OutcomeConflict {
		t.Fatalf("higher-version overwrite outcome=%s want CONFLICT", d2.Outcome)
	}
	var ie *IngestionError
	if !errors.As(e2, &ie) || ie.Type != ErrStorageConflict {
		t.Fatalf("conflict must surface STORAGE_CONFLICT, got %v", e2)
	}
	if !strings.HasPrefix(d2.Reason, string(ReasonContentConflict)) {
		t.Fatalf("decision must carry the stable content-conflict reason: %q", d2.Reason)
	}
	recs, _ := st.Snapshot(ctx)
	if len(recs) != 1 || recs[0].Value != "36.8" || recs[0].SourceVersion != "2.3" {
		t.Fatalf("original effective version must be retained: %+v", recs)
	}
	pend, _ := st.PendingConflicts(ctx)
	if len(pend) != 1 || pend[0].Reason != ReasonContentConflict {
		t.Fatalf("content conflict must be pending with its stable reason: %+v", pend)
	}
	chains, _ := st.Chains(ctx)
	if chains[0].Status != ChainPendingReview {
		t.Fatalf("chain must be pending review: %s", chains[0].Status)
	}
	t.Logf("messageId=EVT-HV basis=higher version R4 at same time with value 40.2 vs 36.8 -> %s %s; effective retained=2.3/36.8",
		d2.Outcome, d2.ConflictID)
}

// Cross-version content conflict must be detected regardless of which
// version arrives first; the conflict id (symmetric digest pair) is the
// same in both orders and the first-arriving content stays effective
// until a review conclusion.
func TestCrossVersionContentConflictOrderIndependent(t *testing.T) {
	for _, hiFirst := range []bool{true, false} {
		name := "high-first"
		if !hiFirst {
			name = "low-first"
		}
		t.Run(name, func(t *testing.T) {
			st, _ := newFileStore(testCfg(t, OverflowReject, 100))
			defer st.Close()
			ctx := context.Background()
			lo := recWith("EVT-XV", "MRN-1", "2.3", "2026-09-01T10:00:00Z", StatusFinal, "low")
			hi := recWith("EVT-XV", "MRN-1", "R4", "2026-09-01T10:00:00Z", StatusFinal, "high")
			var da, db *Decision
			if hiFirst {
				da, _ = st.Submit(ctx, hi)
				db, _ = st.Submit(ctx, lo)
			} else {
				da, _ = st.Submit(ctx, lo)
				db, _ = st.Submit(ctx, hi)
			}
			if da.Outcome != OutcomeAccepted || db.Outcome != OutcomeConflict {
				t.Fatalf("%s outcomes: %s/%s", name, da.Outcome, db.Outcome)
			}
			if db.Reason == "" || !strings.HasPrefix(db.Reason, string(ReasonContentConflict)) {
				t.Fatalf("%s stable reason missing: %q", name, db.Reason)
			}
			recs, _ := st.Snapshot(ctx)
			want := "high"
			if !hiFirst {
				want = "low"
			}
			if len(recs) != 1 || recs[0].Value != want {
				t.Fatalf("%s effective must stay first-arriving %q: %+v", name, want, recs)
			}
			pend, _ := st.PendingConflicts(ctx)
			if len(pend) != 1 {
				t.Fatalf("%s pending=%+v", name, pend)
			}
			// Resolve KEEP_CURRENT twice in a row: second is idempotent.
			r1, err := st.Resolve(ctx, pend[0].ConflictID, KeepCurrent, "rev")
			if err != nil || !r1.Applied {
				t.Fatalf("%s resolve: %+v %v", name, r1, err)
			}
			r2, err := st.Resolve(ctx, pend[0].ConflictID, KeepCurrent, "rev")
			if err != nil || r2.Applied {
				t.Fatalf("%s re-resolve must be an idempotent no-op: %+v %v", name, r2, err)
			}
			recs2, _ := st.Snapshot(ctx)
			if recs2[0].Value != want {
				t.Fatalf("%s keep-current changed effective: %+v", name, recs2)
			}
			t.Logf("messageId=EVT-XV basis=%s cross-version content conflict id=%s; KEEP_CURRENT applied once, effective=%s",
				name, pend[0].ConflictID, want)
		})
	}
}

// A patient mismatch conflict, once adjudicated ACCEPT_INCOMING, flips
// the effective version atomically; the same conclusion repeated creates
// no duplicate state; after restart the resolved state and effective
// record are reproduced exactly from the journals.
func TestPatientMismatchReviewAtomicAndRestart(t *testing.T) {
	cfg := testCfg(t, OverflowReject, 100)
	st, _ := newFileStore(cfg)
	ctx := context.Background()

	st.Submit(ctx, recWith("EVT-PMR", "MRN-A", "R4", "2026-09-01T10:00:00Z", StatusFinal, "a"))
	d, _ := st.Submit(ctx, recWith("EVT-PMR", "MRN-B", "R4", "2026-09-01T10:00:00Z", StatusFinal, "b"))
	if d.Outcome != OutcomeConflict {
		t.Fatalf("patient mismatch outcome=%s", d.Outcome)
	}
	pend, _ := st.PendingConflicts(ctx)
	if len(pend) != 1 || pend[0].Reason != ReasonPatientMismatch {
		t.Fatalf("want one patient-mismatch conflict, got %+v", pend)
	}
	cid := pend[0].ConflictID

	decsBefore, _ := st.Decisions(ctx)
	res, err := st.Resolve(ctx, cid, AcceptIncoming, "dr-who")
	if err != nil || !res.Applied {
		t.Fatalf("accept incoming: %+v %v", res, err)
	}
	recs, _ := st.Snapshot(ctx)
	if len(recs) != 1 || recs[0].Value != "b" {
		t.Fatalf("effective must atomically flip to incoming: %+v", recs)
	}
	if pend2, _ := st.PendingConflicts(ctx); len(pend2) != 0 {
		t.Fatalf("no pending conflict may remain: %+v", pend2)
	}
	// Idempotent re-application writes nothing.
	res2, err := st.Resolve(ctx, cid, AcceptIncoming, "dr-who")
	if err != nil || res2.Applied {
		t.Fatalf("repeat conclusion must be idempotent no-op: %+v %v", res2, err)
	}
	decsAfter, _ := st.Decisions(ctx)
	if len(decsAfter) != len(decsBefore)+1 {
		t.Fatalf("exactly one review decision expected: %d -> %d", len(decsBefore), len(decsAfter))
	}
	// A contradictory conclusion is rejected with the stable reason.
	if _, err := st.Resolve(ctx, cid, KeepCurrent, "dr-other"); err == nil {
		t.Fatal("different conclusion on resolved conflict must be rejected")
	} else {
		var ie *IngestionError
		if !errors.As(err, &ie) || ie.Reason != "CONFLICT_ALREADY_RESOLVED" {
			t.Fatalf("re-resolve reason: %v", err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: journals rebuild the resolved state exactly, replay agrees.
	cfg.ResetOutput = false
	st2, err := newFileStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	recs2, _ := st2.Snapshot(ctx)
	if len(recs2) != 1 || recs2[0].Value != "b" {
		t.Fatalf("post-restart effective must be adjudicated incoming: %+v", recs2)
	}
	if p, _ := st2.PendingConflicts(ctx); len(p) != 0 {
		t.Fatalf("post-restart pending must be empty: %+v", p)
	}
	conflicts, _ := st2.Chains(ctx)
	var got *ChainView
	for _, c := range conflicts {
		if c.EventID == "EVT-PMR" {
			got = c
		}
	}
	if got == nil || got.Status != ChainEffective {
		t.Fatalf("post-restart chain must be effective: %+v", got)
	}
	rep, err := st2.Replay(ctx)
	if err != nil || !rep.Consistent {
		t.Fatalf("post-restart replay must be consistent: %+v err=%v", rep, err)
	}
	t.Logf("messageId=EVT-PMR basis=patient mismatch + ACCEPT_INCOMING -> effective=b; re-apply no-op; restart consistent (chains=%d conflicts=%d)",
		rep.Chains, rep.Conflicts)
}

// A content conflict parked because of version ordering must still be
// detected when the lower version arrives later at the same time: the
// "out of order" path must never swallow a genuine content disagreement.
func TestLateLowerVersionSameTimeStillConflicts(t *testing.T) {
	st, _ := newFileStore(testCfg(t, OverflowReject, 100))
	defer st.Close()
	ctx := context.Background()
	st.Submit(ctx, recWith("EVT-LC", "MRN-1", "R4", "2026-09-01T10:00:00Z", StatusFinal, "new"))
	d, _ := st.Submit(ctx, recWith("EVT-LC", "MRN-1", "2.3", "2026-09-01T10:00:00Z", StatusFinal, "old-different"))
	if d.Outcome != OutcomeConflict || !strings.HasPrefix(d.Reason, string(ReasonContentConflict)) {
		t.Fatalf("late lower-version same-time disagreement must be CONTENT conflict, got %s %q", d.Outcome, d.Reason)
	}
	recs, _ := st.Snapshot(ctx)
	if recs[0].Value != "new" {
		t.Fatalf("effective must be retained: %+v", recs)
	}
	t.Logf("messageId=EVT-LC basis=late 2.3 at same effective time with different content -> %s (not OUT_OF_ORDER)", d.Outcome)
}

// Different status at the same clinical time is a content conflict even
// though value and code agree; a different effective time remains a
// normal revision/out-of-order decision (no conflict).
func TestStatusDisagreementIsContentConflict(t *testing.T) {
	st, _ := newFileStore(testCfg(t, OverflowReject, 100))
	defer st.Close()
	ctx := context.Background()
	st.Submit(ctx, recWith("EVT-SD", "MRN-1", "R4", "2026-09-01T10:00:00Z", StatusFinal, "1"))
	d, _ := st.Submit(ctx, recWith("EVT-SD", "MRN-1", "R4", "2026-09-01T10:00:00Z", StatusEnteredInError, "1"))
	if d.Outcome != OutcomeConflict || !strings.HasPrefix(d.Reason, string(ReasonContentConflict)) {
		t.Fatalf("status disagreement at same time must be content conflict: %s %q", d.Outcome, d.Reason)
	}
	// Same facts at a later time: normal supersede, no conflict.
	d2, err := st.Submit(ctx, recWith("EVT-SD2", "MRN-1", "R4", "2026-09-02T10:00:00Z", StatusFinal, "1"))
	if err != nil || d2.Outcome != OutcomeAccepted {
		t.Fatalf("first EVT-SD2 must accept: %s %v", d2.Outcome, err)
	}
	d3, err := st.Submit(ctx, recWith("EVT-SD2", "MRN-1", "R4", "2026-09-03T10:00:00Z", StatusFinal, "2"))
	if err != nil || d3.Outcome != OutcomeAccepted {
		t.Fatalf("later-time revision must supersede: %s %v", d3.Outcome, err)
	}
	t.Logf("messageId=EVT-SD basis=status final vs entered-in-error same time -> %s; later-time revision -> %s",
		d.Outcome, d3.Outcome)
}

// Concurrent deliveries of the same event at the same clinical time from
// mixed formats/versions with genuinely different content must end with
// exactly one effective record, one pending conflict per disagreeing
// digest pair, and a replay-consistent state; patient-mismatch deliveries
// get their own distinguishable reason.
func TestConcurrentMixedFormatsAndPatients(t *testing.T) {
	eng := newTestEngine(t, 12)
	ctx := context.Background()

	const writers = 12
	var wg sync.WaitGroup
	var mu sync.Mutex
	outcomes := map[Outcome]int{}
	reasons := map[string]int{}

	// Every writer carries a distinct clinical value at the same
	// effective time, split across two patients and two formats, so no
	// delivery can collapse to DUPLICATE: exactly one is accepted and
	// every other one conflicts (content or patient mismatch).
	payload := func(g int) ([]byte, string) {
		val := "v" + string(rune('a'+g))
		patient := "MRN-MIX"
		if g%2 == 1 {
			patient = "MRN-OTHER"
		}
		if g%4 < 2 {
			return hl7MsgValue("EVT-MIX", patient, "2.5.1", "20260901101500", val), "HL7-" + patient
		}
		return fhirObs("EVT-MIX", patient, "4.0", "final", "39."+string(rune('0'+(g%10))), "2026-09-01T10:15:00Z"), "FHIR-" + patient
	}
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			b, who := payload(g)
			r := eng.Ingest(ctx, who, b)
			mu.Lock()
			outcomes[r.Outcome]++
			if r.Outcome == OutcomeConflict {
				chains, _ := eng.Chains(ctx)
				c := findChain(chains, "EVT-MIX")
				if c != nil {
					for _, p := range c.Deliveries {
						if p.Outcome == OutcomeConflict {
							if strings.HasPrefix(p.Reason, string(ReasonPatientMismatch)) {
								reasons[string(ReasonPatientMismatch)]++
							} else if strings.HasPrefix(p.Reason, string(ReasonContentConflict)) {
								reasons[string(ReasonContentConflict)]++
							}
						}
					}
				}
			}
			mu.Unlock()
		}(g)
	}
	wg.Wait()

	if outcomes[OutcomeAccepted] != 1 {
		t.Fatalf("exactly one delivery accepted, outcomes=%v", outcomes)
	}
	if outcomes[OutcomeConflict] != writers-1 {
		t.Fatalf("all other deliveries must conflict (content or patient), outcomes=%v", outcomes)
	}
	if reasons[string(ReasonContentConflict)] == 0 || reasons[string(ReasonPatientMismatch)] == 0 {
		t.Fatalf("both conflict reasons must occur across the chain: %v", reasons)
	}

	// Every pending conflict gets adjudicated concurrently with both
	// choices; exactly one choice wins per conflict.
	pend, _ := eng.PendingConflicts(ctx)
	if len(pend) < 2 {
		t.Fatalf("at least one content and one patient conflict pending: %+v", pend)
	}
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

	if p2, _ := eng.PendingConflicts(ctx); len(p2) != 0 {
		t.Fatalf("all conflicts resolved: %+v", p2)
	}
	recs, _ := eng.Snapshot(ctx)
	if len(recs) != 1 {
		t.Fatalf("one effective record after concurrent review: %+v", recs)
	}
	rep, err := eng.Replay(ctx)
	if err != nil || !rep.Consistent {
		t.Fatalf("replay after concurrent mixed review: %+v err=%v", rep, err)
	}
	t.Logf("messageId=EVT-MIX basis=%d mixed-format/patient writers -> accepted=1 conflicts=%d reasons=%v; concurrent review -> pending=0 replay consistent",
		writers, outcomes[OutcomeConflict], reasons)
}

// Patient-mismatch deliveries are bounded by the pending-review backlog
// exactly like content conflicts: reject policy refuses the delivery and
// degrade policy routes it to the degraded journal; the chain linkage is
// never rewritten and the backlog stays queryable.
func TestPatientMismatchBacklogPolicy(t *testing.T) {
	ctx := context.Background()

	t.Run("reject", func(t *testing.T) {
		cfg := testCfg(t, OverflowReject, 100)
		cfg.MaxPendingReviews = 1
		st, _ := newFileStore(cfg)
		defer st.Close()
		st.Submit(ctx, recWith("PM1", "MRN-A", "R4", "2026-09-01T10:00:00Z", StatusFinal, "a"))
		d1, _ := st.Submit(ctx, recWith("PM1", "MRN-B", "R4", "2026-09-01T10:00:00Z", StatusFinal, "b"))
		if d1.Outcome != OutcomeConflict {
			t.Fatalf("first mismatch parked: %s", d1.Outcome)
		}
		st.Submit(ctx, recWith("PM2", "MRN-A", "R4", "2026-09-02T10:00:00Z", StatusFinal, "a"))
		d2, err := st.Submit(ctx, recWith("PM2", "MRN-C", "R4", "2026-09-02T10:00:00Z", StatusFinal, "c"))
		if d2.Outcome != OutcomeRejected || err == nil {
			t.Fatalf("backlog cap must reject the second mismatch: %s err=%v", d2.Outcome, err)
		}
		pend, _ := st.PendingConflicts(ctx)
		if len(pend) != 1 || pend[0].Reason != ReasonPatientMismatch {
			t.Fatalf("backlog stays at cap with the patient reason: %+v", pend)
		}
		chains, _ := st.Chains(ctx)
		c := findChain(chains, "PM2")
		if c.PatientRef == "" || strings.Contains(c.PatientRef, "MRN-C") {
			t.Fatalf("rejected mismatch must not touch PM2 linkage: %+v", c)
		}
		t.Logf("messageId=PM2 basis=patient mismatch over backlog cap -> %s; linkage retained, pending queryable", d2.Outcome)
	})

	t.Run("degrade", func(t *testing.T) {
		cfg := testCfg(t, OverflowDegrade, 100)
		cfg.MaxPendingReviews = 1
		st, _ := newFileStore(cfg)
		defer st.Close()
		st.Submit(ctx, recWith("PM3", "MRN-A", "R4", "2026-09-01T10:00:00Z", StatusFinal, "a"))
		st.Submit(ctx, recWith("PM3", "MRN-B", "R4", "2026-09-01T10:00:00Z", StatusFinal, "b"))
		st.Submit(ctx, recWith("PM4", "MRN-A", "R4", "2026-09-02T10:00:00Z", StatusFinal, "a"))
		d, err := st.Submit(ctx, recWith("PM4", "MRN-D", "R4", "2026-09-02T10:00:00Z", StatusFinal, "d"))
		if d.Outcome != OutcomeDegraded || err != nil {
			t.Fatalf("over-cap mismatch must degrade: %s err=%v", d.Outcome, err)
		}
		pend, _ := st.PendingConflicts(ctx)
		if len(pend) != 1 {
			t.Fatalf("degrade must not grow the backlog: %+v", pend)
		}
		chains, _ := st.Chains(ctx)
		c := findChain(chains, "PM4")
		if c.PendingConflicts != 0 || c.Status != ChainEffective {
			t.Fatalf("degraded mismatch creates no pending state: %+v", c)
		}
		t.Logf("messageId=PM4 basis=patient mismatch over backlog cap -> %s journal; primary chain untouched", d.Outcome)
	})
}

// Resolving a conflict by accepting the incoming revision must not
// consume an additional capacity slot: one chain stays one accepted
// record, and a store at MaxRecords can still recognize duplicates and
// reject only genuinely new events after the review.
func TestAcceptIncomingDoesNotConsumeCapacityAgain(t *testing.T) {
	cfg := testCfg(t, OverflowReject, 1)
	cfg.MaxPendingReviews = 10
	st, _ := newFileStore(cfg)
	defer st.Close()
	ctx := context.Background()
	st.Submit(ctx, recWith("CAP-E", "MRN-1", "R4", "2026-09-01T10:00:00Z", StatusFinal, "one"))
	d, _ := st.Submit(ctx, recWith("CAP-E", "MRN-1", "R4", "2026-09-01T10:00:00Z", StatusFinal, "two"))
	if d.Outcome != OutcomeConflict {
		t.Fatalf("setup conflict: %s", d.Outcome)
	}
	pend, _ := st.PendingConflicts(ctx)
	if _, err := st.Resolve(ctx, pend[0].ConflictID, AcceptIncoming, "rev"); err != nil {
		t.Fatal(err)
	}
	recs, _ := st.Snapshot(ctx)
	if len(recs) != 1 || recs[0].Value != "two" {
		t.Fatalf("accept-incoming keeps one chain/record: %+v", recs)
	}
	// A brand new event must still be rejected by the single-record cap.
	dNew, err := st.Submit(ctx, recWith("CAP-OTHER", "MRN-1", "R4", "2026-09-10T10:00:00Z", StatusFinal, "x"))
	if err == nil || dNew.Outcome != OutcomeRejected {
		t.Fatalf("new event over cap must reject after review: %s %v", dNew.Outcome, err)
	}
	// The exact winning revision redelivered is still a duplicate.
	dDup, _ := st.Submit(ctx, recWith("CAP-E", "MRN-1", "R4", "2026-09-01T10:00:00Z", StatusFinal, "two"))
	if dDup.Outcome != OutcomeDuplicate {
		t.Fatalf("winning revision redelivery must be duplicate: %s", dDup.Outcome)
	}
	t.Logf("messageId=CAP-E basis=accept-incoming after conflict -> 1 record, cap still 1 (new event %s, redelivery %s)",
		dNew.Outcome, dDup.Outcome)
}
