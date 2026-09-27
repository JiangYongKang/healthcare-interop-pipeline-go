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
// must land in one event chain with a deterministic effective version.
func TestChainCrossFormatSameEvent(t *testing.T) {
	eng := newTestEngine(t, 4)
	ctx := context.Background()

	// HL7 2.5.1 first, then the same event (same event id) as FHIR R4.
	r1 := eng.Ingest(ctx, "h1", hl7Msg("EVT-XFMT", "MRN-X", "2.5.1", "20260901101500"))
	r2 := eng.Ingest(ctx, "f1", fhirObs("EVT-XFMT", "MRN-X", "4.0", "final", "37.1", "2026-09-01T10:15:00Z"))
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
	if c.Effective == nil || c.Effective.SourceFormat != "FHIR-JSON" || c.Effective.Value != "37.1" {
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
