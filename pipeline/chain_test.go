package pipeline

import (
	"context"
	"fmt"
	"strings"
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
// must land in one event chain. When the clinical facts agree at the
// same effective time, the higher version deterministically supersedes;
// when the facts disagree the delivery is parked for review instead of
// silently overwriting the effective result (in either arrival order).
func TestChainCrossFormatSameEvent(t *testing.T) {
	eng := newTestEngine(t, 4)
	ctx := context.Background()

	// HL7 2.5.1 first, then the same event (same event id, same value at
	// the same clinical effective time) as FHIR R4.
	r1 := eng.Ingest(ctx, "h1", hl7Msg("EVT-XFMT", "MRN-X", "2.5.1", "20260901101500"))
	r2 := eng.Ingest(ctx, "f1", fhirObs("EVT-XFMT", "MRN-X", "4.0", "final", "36.8", "2026-09-01T10:15:00Z"))
	if r1.Outcome != OutcomeAccepted || r2.Outcome != OutcomeAccepted {
		t.Fatalf("outcomes: %s / %s", r1.Outcome, r2.Outcome)
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
	// Deterministic winner: R4 ordinal > 2.5.1 ordinal.
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
	t.Logf("messageId=EVT-XFMT basis=cross-format same eventId same facts -> chainId=%s deliveries=2 effective=%s/%s status=%s",
		c.ChainID, c.Effective.SourceFormat, c.Effective.SourceVersion, c.Status)
}

// Cross-format deliveries at the same clinical effective time but with
// genuinely different clinical content must never silently overwrite the
// effective result just because the new delivery has a higher version
// ordinal. The result must be identical in both arrival orders: the
// first delivery stays effective and one content conflict is parked with
// the stable CONTENT_CONFLICT_SAME_REVISION reason.
func TestChainCrossFormatContentConflictBothOrders(t *testing.T) {
	for _, fhirFirst := range []bool{false, true} {
		name := "hl7-first"
		if fhirFirst {
			name = "fhir-first"
		}
		t.Run(name, func(t *testing.T) {
			eng := newTestEngine(t, 4)
			ctx := context.Background()
			hl7 := hl7MsgValue("EVT-XCONF", "MRN-XC", "2.5.1", "20260901101500", "36.8")
			fhir := fhirObs("EVT-XCONF", "MRN-XC", "4.0", "final", "39.9", "2026-09-01T10:15:00Z")
			var first, second *Result
			if fhirFirst {
				first = eng.Ingest(ctx, "f1", fhir)
				second = eng.Ingest(ctx, "h1", hl7)
			} else {
				first = eng.Ingest(ctx, "h1", hl7)
				second = eng.Ingest(ctx, "f1", fhir)
			}
			if first.Outcome != OutcomeAccepted {
				t.Fatalf("%s: first delivery must be accepted: %s", name, first.Outcome)
			}
			if second.Outcome != OutcomeConflict || second.ConflictID == "" {
				t.Fatalf("%s: disagreeing delivery must conflict: %s", name, second.Outcome)
			}
			if !strings.HasPrefix(second.Reason, string(ReasonContentConflict)) {
				t.Fatalf("%s: stable reason mismatch: %q", name, second.Reason)
			}
			pend, _ := eng.PendingConflicts(ctx)
			if len(pend) != 1 || pend[0].Reason != ReasonContentConflict {
				t.Fatalf("%s: pending content conflict: %+v", name, pend)
			}
			// The higher version (R4) must not win merely by arriving:
			// effective value stays whatever arrived first.
			recs, _ := eng.Snapshot(ctx)
			wantVal := "36.8"
			if fhirFirst {
				wantVal = "39.9"
			}
			if len(recs) != 1 || recs[0].Value != wantVal {
				t.Fatalf("%s: effective value must stay %s: %+v", name, wantVal, recs)
			}
			// Conflict id is symmetric over the digest pair, so the same
			// disagreeing pair yields the same id in both orders.
			t.Logf("messageId=EVT-XCONF basis=%s cross-format content conflict -> %s conflictId=%s effective retained=%s",
				name, second.Outcome, second.ConflictID, wantVal)
		})
	}
}

// Same event id delivered for a different patient must not rewrite the
// chain's patient linkage or effective result, in either arrival order,
// and must be distinguishable from a plain content conflict.
func TestChainPatientMismatchBothOrders(t *testing.T) {
	for _, aFirst := range []bool{true, false} {
		name := "patientA-first"
		if !aFirst {
			name = "patientB-first"
		}
		t.Run(name, func(t *testing.T) {
			eng := newTestEngine(t, 4)
			ctx := context.Background()
			recA := recFor("EVT-PM", "R4", "2026-09-01T10:00:00Z", "a")
			recA.PatientRef = "MRN-A"
			recB := recFor("EVT-PM", "R4", "2026-09-01T10:00:00Z", "b")
			recB.PatientRef = "MRN-B"
			// Re-run de-identification so pseudonyms are stable.
			deid := NewDeidentifier(eng.cfg.DeIDSalt)
			_ = deid.DeidentifyRecord(recA)
			_ = deid.DeidentifyRecord(recB)

			var d1, d2 *Decision
			if aFirst {
				d1, _ = eng.store.Submit(ctx, recA)
				d2, _ = eng.store.Submit(ctx, recB)
			} else {
				d1, _ = eng.store.Submit(ctx, recB)
				d2, _ = eng.store.Submit(ctx, recA)
			}
			if d1.Outcome != OutcomeAccepted || d2.Outcome != OutcomeConflict {
				t.Fatalf("%s: outcomes %s/%s", name, d1.Outcome, d2.Outcome)
			}
			pend, _ := eng.PendingConflicts(ctx)
			if len(pend) != 1 || pend[0].Reason != ReasonPatientMismatch {
				t.Fatalf("%s: pending must be a patient mismatch: %+v", name, pend)
			}
			c0 := pend[0]
			if c0.CurrentPatient == "" || c0.IncomingPatient == "" ||
				c0.CurrentPatient == c0.IncomingPatient {
				t.Fatalf("%s: both patient pseudonyms must be recorded distinctly: %+v", name, c0)
			}
			if strings.HasPrefix(c0.CurrentPatient, "MRN-") || strings.HasPrefix(c0.IncomingPatient, "MRN-") {
				t.Fatalf("patient mismatch record leaks raw MRN: %+v", c0)
			}
			chains, _ := eng.Chains(ctx)
			c := findChain(chains, "EVT-PM")
			if c.Status != ChainPendingReview || c.PendingConflicts != 1 {
				t.Fatalf("%s: chain status: %s pending=%d", name, c.Status, c.PendingConflicts)
			}
			// Chain linkage stays on the first patient; both deliveries are
			// retained and individually visible in the de-identified view.
			wantPseudo := c0.CurrentPatient
			if c.PatientRef != wantPseudo {
				t.Fatalf("%s: chain linkage rewritten: chain=%s want=%s", name, c.PatientRef, wantPseudo)
			}
			if len(c.Deliveries) != 2 || c.Deliveries[0].PatientRef == c.Deliveries[1].PatientRef {
				t.Fatalf("%s: deliveries must retain their own patient refs: %+v", name, c.Deliveries)
			}
			if c.Effective == nil || c.Effective.PatientRef != wantPseudo {
				t.Fatalf("%s: effective result must keep the first patient", name)
			}
			t.Logf("messageId=EVT-PM basis=%s patient mismatch -> %s conflictId=%s chain patient retained=%s",
				name, ReasonPatientMismatch, c0.ConflictID, wantPseudo)
		})
	}
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
