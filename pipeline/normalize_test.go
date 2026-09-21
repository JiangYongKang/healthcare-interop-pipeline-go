package pipeline

import (
	"testing"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/formats"
)

func TestNormalizeVersionDefaults(t *testing.T) {
	cases := []struct {
		name, format, version, rawStatus, eventType, wantStatus, wantSystem, wantUnit string
	}{
		{"hl7 2.3 empty status", "HL7v2", "2.3", "", "Observation", StatusPreliminary, "L", "unspecified"},
		{"hl7 2.5.1 empty status", "HL7v2", "2.5.1", "", "Observation", StatusFinal, "LN", "unspecified"},
		{"hl7 2.3 P", "HL7v2", "2.3", "P", "Observation", StatusPreliminary, "L", "unspecified"},
		{"hl7 2.5.1 X", "HL7v2", "2.5.1", "X", "Observation", StatusEnteredInError, "LN", "unspecified"},
		{"fhir R4 obs empty", "FHIR-JSON", "R4", "", "Observation", StatusPreliminary, "http://loinc.org", "unspecified"},
		{"fhir STU3 obs corrected", "FHIR-JSON", "STU3", "corrected", "Observation", StatusFinal, "http://loinc.org", "1"},
		{"fhir STU3 cond inactive", "FHIR-JSON", "STU3", "inactive", "Condition", StatusResolved, "http://loinc.org", "1"},
		{"fhir R4 cond recurrence", "FHIR-JSON", "R4", "recurrence", "Condition", StatusActive, "http://loinc.org", "unspecified"},
		{"fhir R4 enc empty", "FHIR-JSON", "R4", "", "Encounter", StatusFinal, "http://loinc.org", "unspecified"},
		{"fhir R4 enc cancelled", "FHIR-JSON", "R4", "cancelled", "Encounter", StatusEnteredInError, "http://loinc.org", "unspecified"},
	}
	n := NewNormalizer()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &formats.ParsedMessage{
				Format: tc.format, FormatVersion: tc.version, EventID: "E",
				PatientMRN: "M", EventType: tc.eventType, Status: tc.rawStatus,
				EventTime: "2026-09-01T00:00:00Z", Code: "C",
			}
			r, err := n.Normalize(p)
			if err != nil {
				t.Fatalf("normalize rejected: %v", err)
			}
			if r.Status != tc.wantStatus || r.CodeSystem != tc.wantSystem || r.Unit != tc.wantUnit {
				t.Fatalf("got status=%s system=%s unit=%s want %s/%s/%s",
					r.Status, r.CodeSystem, r.Unit, tc.wantStatus, tc.wantSystem, tc.wantUnit)
			}
			if r.EffectiveTime != r.EventTime {
				t.Fatal("missing effective time must default to event time")
			}
			t.Logf("messageId=E basis=%s default/mapping -> status=%s system=%s unit=%s",
				tc.name, r.Status, r.CodeSystem, r.Unit)
		})
	}
}

func TestNormalizeUnmappableStatus(t *testing.T) {
	n := NewNormalizer()
	p := &formats.ParsedMessage{
		Format: "HL7v2", FormatVersion: "2.5.1", EventID: "E",
		EventType: "Observation", Status: "Z", EventTime: "2026-09-01T00:00:00Z",
	}
	_, err := n.Normalize(p)
	if err == nil {
		t.Fatal("enum Z must fail normalization")
	}
	ie := err.(*IngestionError)
	if ie.Type != ErrValidationError || ie.Reason != string(formats.ReasonInvalidStatus) {
		t.Fatalf("error type=%s reason=%s", ie.Type, ie.Reason)
	}
	t.Logf("messageId=E basis=enum Z not in canonical map -> VALIDATION_ERROR/INVALID_STATUS (external detail hidden: %t)", ie.Detail != "")
}
