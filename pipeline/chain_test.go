package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func fhirObs(id, mrn, ver, status, value, eff string) []byte {
	return []byte(fmt.Sprintf(
		`{"resourceType":"Observation","id":"%s","meta":{"fhirVersion":"%s"},"status":"%s",`+
			`"subject":{"reference":"Patient/%s"},"code":{"coding":[{"code":"8310-5"}]},`+
			`"valueQuantity":{"value":%s,"unit":"Cel"},"effectiveDateTime":"%s"}`,
		id, ver, status, mrn, value, eff))
}

func findChain(chains []*ChainView, eventID string) *ChainView {
	for _, c := range chains {
		if c.EventID == eventID {
			return c
		}
	}
	return nil
}

// The same clinical event delivered in different formats and versions
// must land in one event chain with a deterministic effective version.
// The cross-format re-delivery is allowed to supersede only because its
// clinical facts (time/value/code/status) are equal; a disagreeing
// re-delivery is covered by TestCrossFormatContentConflictBothOrders.
func TestChainCrossFormatSameEvent(t *testing.T) {
	eng := newTestEngine(t, 4)
	ctx := context.Background()

	// HL7 2.5.1 first, then the same event (same event id) as FHIR R4;
	// both carry the same clinical fact: temperature 36.8 at the same
	// effective time (unit casing "cel"/"Cel" and code-system spelling
	// LN/URL are normalized as equivalent).
	r1 := eng.Ingest(ctx, "h1", hl7Msg("EVT-XFMT", "MRN-X", "2.5.1", "20260901101500"))
	r2 := eng.Ingest(ctx, "f1", fhirObs("EVT-XFMT", "MRN-X", "4.0", "final", "36.8", "2026-09-01T10:15:00Z"))
	if r1.Outcome != OutcomeAccepted || r2.Outcome != OutcomeAccepted {
		t.Fatalf("outcomes: %s / %s (%s)", r1.Outcome, r2.Outcome, r2.Reason)
	}

	chains, err := eng.Chains(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c := findChain(chains, "EVT-XFMT")
	if c == nil {
		t.Fatalf("no chain for EVT-XFMT: %+v", chains)
	}
	if len(c.Deliveries) != 2 {
		t.Fatalf("cross-format deliveries must share one chain, got %d deliveries", len(c.Deliveries))
	}
	// Deterministic winner: R4 ordinal > 2.5.1 ordinal, equal clinical
	// facts, so the R4 re-delivery supersedes.
	if c.Effective == nil || c.Effective.SourceFormat != "FHIR-JSON" || c.Effective.Value != "36.8" {
		t.Fatalf("effective must be the R4 delivery: %+v", c.Effective)
	}
	if c.Status != ChainEffective || c.PendingConflicts != 0 {
		t.Fatalf("chain status=%s pending=%d", c.Status, c.PendingConflicts)
	}
	// Source and time relation of each delivery are queryable.
	var sawHL7, sawFHIR bool
	for _, d := range c.Deliveries {
		if d.Format == "HL7v2" && d.Version == "2.5.1" && d.Outcome == OutcomeAccepted {
			sawHL7 = true
		}
		if d.Format == "FHIR-JSON" && d.Version == "R4" && d.Outcome == OutcomeAccepted &&
			d.EffectiveTime == "2026-09-01T10:15:00Z" {
			sawFHIR = true
		}
	}
	if !sawHL7 || !sawFHIR {
		t.Fatalf("delivery views missing source/outcome relation: %+v", c.Deliveries)
	}
	// De-identified view: pseudonym, never the raw MRN.
	if !strings.HasPrefix(c.PatientRef, "PAT-") || strings.Contains(c.PatientRef, "MRN-X") {
		t.Fatalf("chain view leaks patient identifier: %q", c.PatientRef)
	}
	t.Logf("messageId=EVT-XFMT basis=cross-format same eventId -> chainId=%s deliveries=2 effective=%s/%s status=%s",
		c.ChainID, c.Effective.SourceFormat, c.Effective.SourceVersion, c.Status)
}

// A late, lower-priority (older version) delivery must not change the
// effective result, in either arrival order.
func TestChainLateArrivalKeepsEffective(t *testing.T) {
	for _, freshFirst := range []bool{true, false} {
		name := "fresh-first"
		if !freshFirst {
			name = "stale-first"
		}
		t.Run(name, func(t *testing.T) {
			eng := newTestEngine(t, 4)
			ctx := context.Background()
			fresh := hl7Msg("EVT-LATE", "MRN-L", "2.5.1", "20260901101500")
			stale := hl7Msg("EVT-LATE", "MRN-L", "2.3", "20260901101500")
			var outcomes []Outcome
			if freshFirst {
				outcomes = append(outcomes, eng.Ingest(ctx, "m1", fresh).Outcome)
				outcomes = append(outcomes, eng.Ingest(ctx, "m2", stale).Outcome)
			} else {
				outcomes = append(outcomes, eng.Ingest(ctx, "m1", stale).Outcome)
				outcomes = append(outcomes, eng.Ingest(ctx, "m2", fresh).Outcome)
			}
			chains, _ := eng.Chains(ctx)
			c := findChain(chains, "EVT-LATE")
			if c.Effective == nil || c.Effective.SourceVersion != "2.5.1" {
				t.Fatalf("%s: effective must stay 2.5.1, got %+v", name, c.Effective)
			}
			t.Logf("messageId=EVT-LATE basis=%s outcomes=%v effective=2.5.1 (late lower-priority never displaces)",
				name, outcomes)
		})
	}
}

// Cross-version content conflict: same event, same clinical time, but a
// genuinely different value arriving in a HIGHER version must not
// silently overwrite the effective result; it is parked for review with
// a stable cross-revision reason. This is the exact defect from the
// bug report (a later "higher version" delivery quietly won).
func TestCrossVersionContentConflictHigherDoesNotOverwrite(t *testing.T) {
	eng := newTestEngine(t, 4)
	ctx := context.Background()

	older := eng.Ingest(ctx, "m1", hl7MsgValue("EVT-XVER", "MRN-X", "2.3", "20260901101500", "36.8"))
	newer := eng.Ingest(ctx, "m2", hl7MsgValue("EVT-XVER", "MRN-X", "2.5.1", "20260901101500", "39.9"))
	if older.Outcome != OutcomeAccepted {
		t.Fatalf("first delivery: %s", older.Outcome)
	}
	if newer.Outcome != OutcomeConflict {
		t.Fatalf("higher-version disagreeing delivery must CONFLICT, got %s (%s)", newer.Outcome, newer.Reason)
	}
	if newer.ConflictID == "" {
		t.Fatal("conflict decision must carry a conflict id")
	}

	chains, _ := eng.Chains(ctx)
	c := findChain(chains, "EVT-XVER")
	if c == nil || c.Status != ChainPendingReview || c.PendingConflicts != 1 {
		t.Fatalf("chain must be pending review: %+v", c)
	}
	// The original effective version is retained, not overwritten.
	if c.Effective == nil || c.Effective.SourceVersion != "2.3" || c.Effective.Value != "36.8" {
		t.Fatalf("original effective version must be retained: %+v", c.Effective)
	}
	pend, _ := eng.PendingConflicts(ctx)
	if len(pend) != 1 || pend[0].Reason != ReasonContentConflictCrossRevision {
		t.Fatalf("pending must carry the stable cross-revision reason: %+v", pend)
	}
	// The decision journal keeps the verdict basis (versions + fields).
	decs, _ := eng.Decisions(ctx)
	found := false
	for _, d := range decs {
		if d.Outcome == OutcomeConflict && strings.Contains(d.Reason, string(ReasonContentConflictCrossRevision)) &&
			strings.Contains(d.Reason, "value") {
			found = true
		}
	}
	if !found {
		t.Fatalf("decision journal must retain basis and differing fields: %+v", decs)
	}
	t.Logf("messageId=EVT-XVER basis=2.3/36.8 then 2.5.1/39.9 same time -> %s %s, effective retained=2.3/36.8",
		newer.Outcome, pend[0].Reason)
}

// Cross-format content conflict in BOTH arrival orders: regardless of
// which message arrives first, the effective result and the conflict
// identity/reason are identical; the lower-priority late delivery is
// not allowed to overwrite, nor is the disagreement skipped as
// out-of-order.
func TestCrossFormatContentConflictBothOrders(t *testing.T) {
	for _, fhirFirst := range []bool{true, false} {
		name := "fhir-r4-first"
		if !fhirFirst {
			name = "hl7-2.3-first"
		}
		t.Run(name, func(t *testing.T) {
			eng := newTestEngine(t, 4)
			ctx := context.Background()
			hl7 := hl7MsgValue("EVT-XFC", "MRN-F", "2.3", "20260901101500", "36.8")
			fhir := fhirObs("EVT-XFC", "MRN-F", "4.0", "final", "40.2", "2026-09-01T10:15:00Z")

			var first, second *Result
			if fhirFirst {
				first = eng.Ingest(ctx, "m1", fhir)
				second = eng.Ingest(ctx, "m2", hl7)
			} else {
				first = eng.Ingest(ctx, "m1", hl7)
				second = eng.Ingest(ctx, "m2", fhir)
			}
			if first.Outcome != OutcomeAccepted {
				t.Fatalf("%s: first must accept, got %s", name, first.Outcome)
			}
			if second.Outcome != OutcomeConflict || second.ConflictID == "" {
				t.Fatalf("%s: disagreeing cross-format delivery must CONFLICT regardless of order, got %s (%s)",
					name, second.Outcome, second.Reason)
			}

			chains, _ := eng.Chains(ctx)
			c := findChain(chains, "EVT-XFC")
			if c == nil || c.Status != ChainPendingReview || c.PendingConflicts != 1 {
				t.Fatalf("%s: chain pending review expected: %+v", name, c)
			}
			pend, _ := eng.PendingConflicts(ctx)
			if len(pend) != 1 {
				t.Fatalf("%s: %d pending conflicts", name, len(pend))
			}
			if pend[0].Reason != ReasonContentConflictCrossRevision {
				t.Fatalf("%s: reason=%s want %s", name, pend[0].Reason, ReasonContentConflictCrossRevision)
			}

			// Effective is always the FIRST-arriving delivery (the one
			// accepted); both orders must retain the original effective
			// version rather than letting the newer/older later message
			// win or being skipped as out-of-order.
			if c.Effective == nil {
				t.Fatalf("%s: missing effective", name)
			}
			if fhirFirst {
				if c.Effective.SourceFormat != "FHIR-JSON" || c.Effective.Value != "40.2" {
					t.Fatalf("%s: effective must stay first-arrival R4/40.2: %+v", name, c.Effective)
				}
				if second.Reason != "" && !strings.Contains(second.Reason, "ordinal=1") {
					t.Fatalf("%s: basis must record incoming stale ordinal: %s", name, second.Reason)
				}
			} else {
				if c.Effective.SourceFormat != "HL7v2" || c.Effective.Value != "36.8" {
					t.Fatalf("%s: effective must stay first-arrival HL7/36.8: %+v", name, c.Effective)
				}
			}
			// Both deliveries remain visible on the chain.
			if len(c.Deliveries) != 2 {
				t.Fatalf("%s: deliveries=%d want 2", name, len(c.Deliveries))
			}
			// Replay must agree with the live state in either order.
			rep, err := eng.Replay(ctx)
			if err != nil || !rep.Consistent {
				t.Fatalf("%s: replay inconsistent: %+v err=%v", name, rep, err)
			}
			t.Logf("messageId=EVT-XFC basis=%s cross-format value disagreement -> %s cid=%s effective=first-arrival retained",
				name, second.Outcome, second.ConflictID)
		})
	}
}

// Same revision coordinates with genuinely different content keep the
// original same-revision reason (distinct from cross-revision and from
// patient mismatch).
func TestSameRevisionContentConflictReasonStable(t *testing.T) {
	eng := newTestEngine(t, 4)
	ctx := context.Background()
	r1 := eng.Ingest(ctx, "m1", hl7MsgValue("EVT-SAME", "MRN-S", "2.5.1", "20260901101500", "1"))
	r2 := eng.Ingest(ctx, "m2", hl7MsgValue("EVT-SAME", "MRN-S", "2.5.1", "20260901101500", "2"))
	if r1.Outcome != OutcomeAccepted || r2.Outcome != OutcomeConflict {
		t.Fatalf("outcomes %s/%s", r1.Outcome, r2.Outcome)
	}
	pend, _ := eng.PendingConflicts(ctx)
	if len(pend) != 1 || pend[0].Reason != ReasonContentConflictSameRevision {
		t.Fatalf("reason must stay CONTENT_CONFLICT_SAME_REVISION: %+v", pend)
	}
	t.Logf("messageId=EVT-SAME basis=identical coords different value -> %s (stable reason)", pend[0].Reason)
}

// A delivery that names a different patient for an existing event chain
// must NOT rewrite the chain's patient association; it is parked with
// the distinct PATIENT_MISMATCH reason and the effective version is
// retained. Holds even when the incoming delivery is a higher version.
func TestPatientMismatchDoesNotRewriteChain(t *testing.T) {
	eng := newTestEngine(t, 4)
	ctx := context.Background()
	r1 := eng.Ingest(ctx, "m1", hl7Msg("EVT-PM", "MRN-A", "2.3", "20260901101500"))
	r2 := eng.Ingest(ctx, "m2", fhirObs("EVT-PM", "MRN-B", "4.0", "final", "36.8", "2026-09-01T10:15:00Z"))
	if r1.Outcome != OutcomeAccepted || r2.Outcome != OutcomeConflict {
		t.Fatalf("outcomes %s/%s (%s)", r1.Outcome, r2.Outcome, r2.Reason)
	}
	pend, _ := eng.PendingConflicts(ctx)
	if len(pend) != 1 || pend[0].Reason != ReasonPatientMismatch {
		t.Fatalf("patient mismatch must carry PATIENT_MISMATCH: %+v", pend)
	}
	chains, _ := eng.Chains(ctx)
	c := findChain(chains, "EVT-PM")
	if c == nil || c.Status != ChainPendingReview {
		t.Fatalf("chain state: %+v", c)
	}
	// The chain patient link is still the original patient's pseudonym.
	recs, _ := eng.Snapshot(ctx)
	var eff *Record
	for _, r := range recs {
		if r.EventID == "EVT-PM" {
			eff = r
		}
	}
	if eff == nil || eff.SourceVersion != "2.3" {
		t.Fatalf("effective must remain the original 2.3 delivery: %+v", eff)
	}
	d := eng.deid
	pseudoA, _ := d.PatientPlaceholder("MRN-A")
	if c.PatientRef != pseudoA || eff.PatientRef != pseudoA {
		t.Fatalf("chain patient association was rewritten: chain=%s eff=%s want %s", c.PatientRef, eff.PatientRef, pseudoA)
	}
	// Both deliveries are retained on the chain for audit/review.
	if len(c.Deliveries) != 2 {
		t.Fatalf("deliveries=%d want 2", len(c.Deliveries))
	}
	t.Logf("messageId=EVT-PM basis=patient swapped MRN-A->MRN-B in higher version -> %s %s, chain patient retained",
		r2.Outcome, pend[0].Reason)
}

// Patient mismatch must be caught even when the foreign-patient
// delivery carries a different (newer) clinical effective time: the
// coordinate rule must never supersede into a different patient's data.
func TestPatientMismatchDifferentTimeNotSuperseded(t *testing.T) {
	eng := newTestEngine(t, 4)
	ctx := context.Background()
	r1 := eng.Ingest(ctx, "m1", hl7Msg("EVT-PMT", "MRN-A", "2.3", "20260901101500"))
	r2 := eng.Ingest(ctx, "m2", fhirObs("EVT-PMT", "MRN-B", "4.0", "final", "36.8", "2026-09-05T10:15:00Z"))
	if r1.Outcome != OutcomeAccepted || r2.Outcome != OutcomeConflict {
		t.Fatalf("outcomes %s/%s (%s)", r1.Outcome, r2.Outcome, r2.Reason)
	}
	pend, _ := eng.PendingConflicts(ctx)
	if len(pend) != 1 || pend[0].Reason != ReasonPatientMismatch {
		t.Fatalf("must be PATIENT_MISMATCH regardless of time: %+v", pend)
	}
	chains, _ := eng.Chains(ctx)
	c := findChain(chains, "EVT-PMT")
	if c == nil || c.Effective == nil || c.Effective.SourceVersion != "2.3" {
		t.Fatalf("newer-time foreign-patient delivery must not supersede: %+v", c)
	}
	t.Logf("messageId=EVT-PMT basis=different patient at newer effective time -> %s retained 2.3 original", pend[0].Reason)
}

// Accepting a patient-mismatch conflict is an explicit human override:
// only then does the effective version (and chain patient link) change,
// atomically; idempotent re-application changes nothing.
func TestPatientMismatchReviewAcceptIncoming(t *testing.T) {
	eng := newTestEngine(t, 4)
	ctx := context.Background()
	eng.Ingest(ctx, "m1", hl7Msg("EVT-PMR", "MRN-A", "2.3", "20260901101500"))
	d := eng.Ingest(ctx, "m2", fhirObs("EVT-PMR", "MRN-B", "4.0", "final", "36.8", "2026-09-01T10:15:00Z"))
	if d.Outcome != OutcomeConflict {
		t.Fatalf("want conflict got %s", d.Outcome)
	}
	res, err := eng.ResolveConflict(ctx, d.ConflictID, AcceptIncoming, "reviewer-1")
	if err != nil || !res.Applied {
		t.Fatalf("resolve: %+v %v", res, err)
	}
	chains, _ := eng.Chains(ctx)
	c := findChain(chains, "EVT-PMR")
	if c.Status != ChainEffective || c.Effective == nil || c.Effective.SourceFormat != "FHIR-JSON" {
		t.Fatalf("after accept-incoming effective must be the R4 delivery: %+v", c)
	}
	pseudoB, _ := eng.deid.PatientPlaceholder("MRN-B")
	if c.PatientRef != pseudoB {
		t.Fatalf("chain patient link must follow reviewed winner: %s want %s", c.PatientRef, pseudoB)
	}
	// Idempotent re-application: no new decision rows.
	before, _ := eng.Decisions(ctx)
	res2, err := eng.ResolveConflict(ctx, d.ConflictID, AcceptIncoming, "reviewer-1")
	if err != nil || res2.Applied {
		t.Fatalf("re-apply must be idempotent: %+v %v", res2, err)
	}
	after, _ := eng.Decisions(ctx)
	if len(after) != len(before) {
		t.Fatalf("idempotent review wrote new decisions: %d -> %d", len(before), len(after))
	}
	t.Logf("messageId=EVT-PMR basis=reviewed ACCEPT_INCOMING flips patient link atomically to %s; re-apply no-op", pseudoB)
}

// Under concurrent load mixing formats/versions, genuinely
// disagreeing same-event deliveries must never silently overwrite: each
// event ends with one effective (the first commit) and one parked
// cross-revision conflict, with no duplicates. Concurrent review then
// resolves each exactly once, and replay agrees.
func TestConcurrentCrossFormatConflictsAndReview(t *testing.T) {
	eng := newTestEngine(t, 8)
	ctx := context.Background()

	const events = 12
	var wg sync.WaitGroup
	var mu sync.Mutex
	outcomes := map[Outcome]int{}
	conflictIDs := map[string]int{}

	// For each event, hammer one HL7 2.3 and one FHIR R4 delivery with
	// genuinely different values at the same clinical time, repeatedly,
	// from independent goroutines.
	deliver := func(g int) {
		defer wg.Done()
		for i := 0; i < events; i++ {
			eid := fmt.Sprintf("EVT-XCC-%02d", i)
			var payload []byte
			if (g+i)%2 == 0 {
				payload = hl7MsgValue(eid, "MRN-XCC", "2.3", "20260901101500", "36.8")
			} else {
				payload = fhirObs(eid, "MRN-XCC", "4.0", "final", "40.1", "2026-09-01T10:15:00Z")
			}
			r := eng.Ingest(ctx, fmt.Sprintf("g%d-e%d", g, i), payload)
			mu.Lock()
			outcomes[r.Outcome]++
			if r.ConflictID != "" {
				conflictIDs[r.ConflictID]++
			}
			mu.Unlock()
		}
	}
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go deliver(g)
	}
	wg.Wait()

	chains, _ := eng.Chains(ctx)
	if len(chains) != events {
		t.Fatalf("chains=%d want %d (outcomes=%v)", len(chains), events, outcomes)
	}
	for _, c := range chains {
		if c.Status != ChainPendingReview || c.PendingConflicts != 1 {
			t.Fatalf("chain %s must have exactly one pending cross-format conflict: status=%s pending=%d",
				c.EventID, c.Status, c.PendingConflicts)
		}
		if c.Effective == nil {
			t.Fatalf("chain %s lost its effective version under concurrency", c.EventID)
		}
		for _, p := range c.Deliveries {
			if p.Outcome == OutcomeConflict && p.Reason != "" &&
				!strings.Contains(p.Reason, string(ReasonContentConflictCrossRevision)) &&
				!strings.Contains(p.Reason, string(ReasonContentConflictSameRevision)) {
				t.Fatalf("chain %s conflict delivery has wrong reason: %s", c.EventID, p.Reason)
			}
		}
	}
	pend, _ := eng.PendingConflicts(ctx)
	if len(pend) != events {
		t.Fatalf("pending conflicts=%d want %d", len(pend), events)
	}

	// Resolve concurrently with competing choices: each conflict ends
	// resolved exactly once.
	var rwg sync.WaitGroup
	for _, c := range pend {
		for _, choice := range []ResolutionChoice{KeepCurrent, AcceptIncoming, AcceptIncoming} {
			rwg.Add(1)
			go func(cid string, ch ResolutionChoice) {
				defer rwg.Done()
				_, _ = eng.ResolveConflict(ctx, cid, ch, "racer")
			}(c.ConflictID, choice)
		}
	}
	rwg.Wait()
	if pend2, _ := eng.PendingConflicts(ctx); len(pend2) != 0 {
		t.Fatalf("pending after concurrent review: %d", len(pend2))
	}
	recs, _ := eng.Snapshot(ctx)
	if len(recs) != events {
		t.Fatalf("effective records=%d want %d", len(recs), events)
	}
	rep, err := eng.Replay(ctx)
	if err != nil || !rep.Consistent {
		t.Fatalf("replay inconsistent after concurrent review: %+v err=%v", rep, err)
	}
	t.Logf("basis=8 goroutines x %d events cross-format disagreeing deliveries -> outcomes=%v distinctConflicts=%d; concurrent review -> pending=0, replay consistent",
		events, outcomes, len(conflictIDs))
}

// Cross-version deliveries of one event keep patient longitudinal
// linkage: the chain pseudonym matches the patient pseudonym of other
// events of the same patient.
func TestChainPatientLinkageAcrossVersions(t *testing.T) {
	eng := newTestEngine(t, 4)
	ctx := context.Background()
	eng.Ingest(ctx, "a", hl7Msg("EVT-P1", "MRN-P", "2.3", "20260901101500"))
	eng.Ingest(ctx, "b", hl7Msg("EVT-P1", "MRN-P", "2.5.1", "20260902101500"))
	eng.Ingest(ctx, "c", hl7Msg("EVT-P2", "MRN-P", "2.5.1", "20260903101500"))

	chains, _ := eng.Chains(ctx)
	c1, c2 := findChain(chains, "EVT-P1"), findChain(chains, "EVT-P2")
	if c1 == nil || c2 == nil {
		t.Fatal("chains missing")
	}
	if c1.PatientRef != c2.PatientRef {
		t.Fatalf("same patient across chains must share pseudonym: %q vs %q", c1.PatientRef, c2.PatientRef)
	}
	if len(c1.Deliveries) != 2 {
		t.Fatalf("cross-version deliveries of EVT-P1 must group, got %d", len(c1.Deliveries))
	}
	t.Logf("basis=MRN-P across versions/chains -> pseudonym %s (longitudinal linkage preserved)", c1.PatientRef)
}
