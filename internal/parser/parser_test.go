package parser

import (
	"os"
	"strings"
	"testing"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/apperr"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/model"
)

// readSample loads one local sample file.
func readSample(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("../../testdata/samples/" + name)
	if err != nil {
		t.Fatalf("read sample %s: %v", name, err)
	}
	return data
}

// TestMalformedReasons verifies that structurally different failures map to
// distinct, reproducible reason codes rather than being silently skipped.
func TestMalformedReasons(t *testing.T) {
	cases := []struct {
		file string
		want apperr.Code
	}{
		{"00_empty.dat", apperr.CodeTruncated},
		{"07_hl7_truncated.hl7", apperr.CodeTruncated},
		{"08_fhir_truncated.json", apperr.CodeTruncated},
		{"09_fhir_malformed.json", apperr.CodeMalformedJSON},
		{"10_hl7_illegal_encoding.hl7", apperr.CodeIllegalEncoding},
		{"11_hl7_unsupported_version.hl7", apperr.CodeUnsupportedVersion},
		{"12_fhir_unsupported_version.json", apperr.CodeUnsupportedVersion},
		{"13_unknown_format.dat", apperr.CodeUnknownFormat},
	}
	for _, tc := range cases {
		t.Run(tc.file, func(t *testing.T) {
			_, ae := Parse(readSample(t, tc.file))
			if ae == nil {
				t.Fatalf("expected rejection %s, got accept", tc.want)
			}
			t.Logf("message-file=%-32s decision=REJECT code=%s kind=%s basis=%q",
				tc.file, ae.Code, ae.Kind, ae.InternalDetail)
			if ae.Code != tc.want {
				t.Fatalf("code=%s want=%s", ae.Code, tc.want)
			}
			if ae.InternalDetail == "" {
				t.Fatal("internal basis must be recorded for traceability")
			}
		})
	}
}

func TestIllegalControlByte(t *testing.T) {
	raw := []byte("MSH|^~\\&|x|y|z|z|20260101||ADT^A01|E-X|P|2.3\rPID\x01\r")
	if !hasIllegalControlByte(raw) {
		t.Fatal("0x01 must be treated as illegal encoding")
	}
}

// TestTrailingJSONRejected ensures ambiguous concatenated JSON is a format
// error instead of silently taking the first document.
func TestTrailingJSONRejected(t *testing.T) {
	doc := []byte(`{"fhirVersion":"R4"}{"fhirVersion":"R4"}`)
	_, ae := Parse(doc)
	if ae == nil || ae.Code != apperr.CodeMalformedJSON {
		t.Fatalf("got %v, want MALFORMED_JSON (trailing data)", ae)
	}
	t.Logf("message=- decision=REJECT code=%s basis=%q", ae.Code, ae.InternalDetail)
}

// TestHL7AcceptedFields checks successful decoding and unknown-segment
// capture (unknown content must be retained, never dropped silently).
func TestHL7AcceptedFields(t *testing.T) {
	ev, ae := Parse(readSample(t, "19_hl7_unknown_segments.hl7"))
	if ae != nil {
		t.Fatalf("unexpected error: %+v", ae)
	}
	t.Logf("message=%s format=%s version=%s unknowns=%d",
		ev.EventID, ev.Format, ev.Version, len(ev.Unknowns))
	if ev.Format != model.FormatHL7v2 || ev.Version != model.HL7v251 {
		t.Fatalf("format/version = %s/%s", ev.Format, ev.Version)
	}
	if ev.EventID != "E-005" || ev.PatientMRN != "MRN-5005" || ev.PatientMRNSys != "HOSP" {
		t.Fatalf("identity fields wrong: %+v", ev)
	}
	if ev.EffectiveTime != "20260901080500" || ev.Sequence != 40 || !ev.HasSequence {
		t.Fatalf("time/sequence wrong: eff=%s seq=%d has=%v", ev.EffectiveTime, ev.Sequence, ev.HasSequence)
	}
	if len(ev.Unknowns) != 2 {
		t.Fatalf("expected 2 captured unknown segments, got %d", len(ev.Unknowns))
	}
	loc := ev.Unknowns[0].Location
	if !strings.HasPrefix(loc, "ZPD") && !strings.HasPrefix(loc, "ZZZ") {
		t.Fatalf("unknown segment location unexpected: %s", loc)
	}
}

// TestFHIRUnknownFieldsRetained verifies R5 forward-compat: new/unknown
// properties are tolerated and captured rather than fatal.
func TestFHIRUnknownFieldsRetained(t *testing.T) {
	ev, ae := Parse(readSample(t, "05_fhir_r5_unknown_fields.json"))
	if ae != nil {
		t.Fatalf("unexpected error: %+v", ae)
	}
	t.Logf("message=%s version=%s unknowns=%d", ev.EventID, ev.Version, len(ev.Unknowns))
	if ev.Version != model.FHIRR5 {
		t.Fatalf("version=%s", ev.Version)
	}
	want := map[string]bool{
		"event.futureScore":     false,
		"subject.novelLink":     false,
		"experimentalExtension": false,
	}
	for _, u := range ev.Unknowns {
		if _, ok := want[u.Location]; ok {
			want[u.Location] = true
		}
	}
	for k, found := range want {
		if !found {
			t.Fatalf("unknown field %q not retained; captured=%+v", k, ev.Unknowns)
		}
	}
}

// TestDSTU2FamilyArray verifies old-version HumanName family array handling.
func TestDSTU2FamilyArray(t *testing.T) {
	ev, ae := Parse(readSample(t, "06_fhir_dstu2_old_missing.json"))
	if ae != nil {
		t.Fatalf("unexpected error: %+v", ae)
	}
	t.Logf("message=%s name=%q", ev.EventID, ev.PatientName)
	if ev.PatientName != "Nguyen" {
		t.Fatalf("DSTU2 family array name=%q", ev.PatientName)
	}
}
