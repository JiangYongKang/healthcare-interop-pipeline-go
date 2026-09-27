package pipeline

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

// TestAdjudicateDeterministic pins the content-only adjudication rule:
// no verdict may depend on arrival order or source format/version.
func TestAdjudicateDeterministic(t *testing.T) {
	base := func() *Record {
		return &Record{
			EventID: "E", EventType: EventObservation, PatientRef: "PAT-x",
			SourceFormat: "HL7v2", SourceVersion: "2.5.1", Status: StatusFinal,
			EventTime: "2026-09-10T09:00:00Z", EffectiveTime: "2026-09-10T09:00:00Z",
			Code: "8310-5", CodeSystem: "LN", Value: "36.8", Unit: "cel",
		}
	}
	cases := []struct {
		name   string
		mutate func(r *Record)
		want   verdict
		reason string
	}{
		{"newer time supersedes", func(r *Record) { r.EffectiveTime = "2026-09-11T09:00:00Z" }, verdictSupersede, ""},
		{"older time is stale", func(r *Record) { r.EffectiveTime = "2026-09-09T09:00:00Z" }, verdictStale, ""},
		{"amended supersedes at equal time", func(r *Record) { r.Status = StatusAmended }, verdictSupersede, ""},
		{"preliminary stale against final", func(r *Record) { r.Status = StatusPreliminary }, verdictStale, ""},
		{"format version alone does not supersede", func(r *Record) {
			r.SourceFormat = "FHIR-JSON"
			r.SourceVersion = "R4"
		}, verdictStale, ""},
		{"same time different value conflicts", func(r *Record) { r.Value = "39.1" }, verdictConflict, ReasonConflictValue},
		{"same time different unit conflicts", func(r *Record) { r.Unit = "degF" }, verdictConflict, ReasonConflictValue},
		{"same rank different status conflicts", func(r *Record) { r.Status = StatusActive }, verdictConflict, ReasonConflictStatus},
		{"retraction ranks below final", func(r *Record) { r.Status = StatusEnteredInError }, verdictStale, ""},
		{"different patient conflicts", func(r *Record) { r.PatientRef = "PAT-y" }, verdictConflict, ReasonConflictPatient},
		{"different code conflicts", func(r *Record) { r.Code = "9999-9" }, verdictConflict, ReasonConflictCode},
		{"different event type conflicts", func(r *Record) { r.EventType = EventCondition }, verdictConflict, ReasonConflictEventType},
		{"display-only difference is not a conflict", func(r *Record) { r.CodeDisplay = "Body temperature (typos)" }, verdictStale, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cur := base()
			inc := base()
			tc.mutate(inc)
			v, reason := adjudicate(inc, cur)
			if v != tc.want || reason != tc.reason {
				t.Fatalf("adjudicate=%v/%q want %v/%q", v, reason, tc.want, tc.reason)
			}
			// Symmetry check for the order-independent cases: swapping
			// the operands must not turn a conflict into a silent win.
			if tc.want == verdictConflict {
				v2, reason2 := adjudicate(cur, inc)
				if v2 != verdictConflict || reason2 != tc.reason {
					t.Fatalf("conflict must be symmetric, got %v/%q", v2, reason2)
				}
			}
			t.Logf("messageId=E basis=%s -> verdict=%v reason=%q", tc.name, v, reason)
		})
	}
}

// consistencyDir locates the cross-format consistency sample corpus.
func consistencyDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(findRepoRoot(t), "testdata", "consistency")
}

// TestEngineCrossFormatSameEventChain: the same clinical event arriving
// via HL7v2 and FHIR-JSON lands in ONE event chain; the view exposes
// both deliveries with source/time relations and no raw PHI.
func TestEngineCrossFormatSameEventChain(t *testing.T) {
	eng := newTestEngine(t, 4)
	ctx := context.Background()
	dir := consistencyDir(t)
	r1 := eng.IngestFile(ctx, filepath.Join(dir, "c01_hl7_evt5001.hl7"))
	r2 := eng.IngestFile(ctx, filepath.Join(dir, "c02_fhir_evt5001.json"))
	if r1.Outcome != OutcomeAccepted {
		t.Fatalf("first delivery: %s (%v)", r1.Outcome, r1.Error)
	}
	if r2.Outcome != OutcomeOutOfOrder {
		t.Fatalf("equivalent cross-format redelivery must be retained-not-applied, got %s", r2.Outcome)
	}
	view, err := eng.Chain(ctx, "EVT-5001")
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != ChainEffective {
		t.Fatalf("chain status=%s want EFFECTIVE", view.Status)
	}
	if len(view.Deliveries) != 2 {
		t.Fatalf("chain must hold both deliveries, got %d", len(view.Deliveries))
	}
	formatsSeen := map[string]bool{}
	relations := map[string]int{}
	for _, d := range view.Deliveries {
		formatsSeen[d.Format] = true
		relations[d.Relation]++
	}
	if len(formatsSeen) != 2 {
		t.Fatalf("chain must span both source formats: %v", formatsSeen)
	}
	if relations[RelationCurrent] != 1 || relations[RelationStale] != 1 {
		t.Fatalf("relations=%v want one CURRENT + one STALE", relations)
	}
	if view.Current == nil || view.Current.Value != "36.8" {
		t.Fatalf("current effective version wrong: %+v", view.Current)
	}
	// Patient longitudinal linkage stays available via the pseudonym,
	// and no raw identifier may appear anywhere in the view.
	if !strings.HasPrefix(view.PatientRef, "PAT-") {
		t.Fatalf("patient ref must be a pseudonym: %q", view.PatientRef)
	}
	raw, _ := json.Marshal(view)
	for _, phi := range []string{"MRN-5001", "LEE", "AMY"} {
		if strings.Contains(string(raw), phi) {
			t.Fatalf("sensitive value %q leaked into chain view", phi)
		}
	}
	t.Logf("messageId=EVT-5001 basis=cross-format grouping: deliveries=%d relations=%v patient=%s (no PHI in view)",
		len(view.Deliveries), relations, view.PatientRef)
}

// TestEngineConflictPendingReviewView: a genuinely inconsistent
// same-time delivery is not silently applied; the chain goes
// PENDING_REVIEW, keeps the current effective version, and the
// conflict reason is stable and distinguishable.
func TestEngineConflictPendingReviewView(t *testing.T) {
	eng := newTestEngine(t, 4)
	ctx := context.Background()
	dir := consistencyDir(t)
	r1 := eng.IngestFile(ctx, filepath.Join(dir, "c03_fhir_evt5002.json"))
	r2 := eng.IngestFile(ctx, filepath.Join(dir, "c04_hl7_evt5002_conflict.hl7"))
	if r1.Outcome != OutcomeAccepted || r2.Outcome != OutcomeConflict {
		t.Fatalf("outcomes=%s/%s", r1.Outcome, r2.Outcome)
	}
	if r2.Error == nil || r2.Error.Type != ErrStorageConflict {
		t.Fatalf("conflict must surface STORAGE_CONFLICT: %+v", r2.Error)
	}
	view, err := eng.Chain(ctx, "EVT-5002")
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != ChainPendingReview {
		t.Fatalf("status=%s want PENDING_REVIEW", view.Status)
	}
	if view.Current.Value != "71.2" {
		t.Fatalf("current effective version must be preserved, got value=%s", view.Current.Value)
	}
	var conflictDelivery *Delivery
	for _, d := range view.Deliveries {
		if d.Relation == RelationConflictPending {
			conflictDelivery = d
		}
	}
	if conflictDelivery == nil || !strings.Contains(conflictDelivery.Reason, ReasonConflictValue) {
		t.Fatalf("conflict delivery must carry stable reason %s: %+v", ReasonConflictValue, conflictDelivery)
	}
	pending, err := eng.PendingReviews(ctx)
	if err != nil || len(pending) != 1 || pending[0] != "EVT-5002" {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
	// Snapshot (external result) must still show the preserved version.
	recs, _ := eng.Snapshot(ctx)
	if len(recs) != 1 || recs[0].Value != "71.2" {
		t.Fatalf("snapshot must keep preserved version: %+v", recs)
	}
	t.Logf("messageId=EVT-5002 basis=%s: chain PENDING_REVIEW, current value=71.2 preserved, conflicting delivery retained seq=%d",
		ReasonConflictValue, conflictDelivery.Seq)
}
