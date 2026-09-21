package formats

import (
	"strings"
	"testing"
)

// buildHL7 assembles segments with precise field positions.
func buildHL7(segments ...string) []byte {
	return []byte(strings.Join(segments, "\r") + "\r")
}

func hl7MSH(evt, eid, ver, t string) string {
	return "MSH|^~\\&|LAB|HOSP|EHR|HOSP|" + t + "||" + evt + "|" + eid + "|P|" + ver
}
func hl7PID(mrn string) string { return "PID|1||" + mrn + "^^^HOSP^MR||DOE^JOHN" }
func hl7OBX(code, value, unit, status, eff string) string {
	// splitFields indices: OBX-1=1, OBX-2="", OBX-3=code, OBX-4="",
	// OBX-5=value, OBX-6=unit, OBX-7..10="", OBX-11=status,
	// OBX-12/13="", OBX-14=eff
	return "OBX|1||" + code + "||" + value + "|" + unit + "|||||" + status + "|||" + eff
}

func TestHL7ValidAcrossVersions(t *testing.T) {
	cases := []struct {
		name, ver, statusIn, wantStatus string
	}{
		{"v2.3 final", "2.3", "F", "F"},
		{"v2.5.1 final", "2.5.1", "F", "F"},
		{"v2.3 empty status defaults (normalizer)", "2.3", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := buildHL7(
				hl7MSH("ORU^R01", "EVT-T1", tc.ver, "20260901101500"),
				hl7PID("MRN-1"),
				hl7OBX("8310-5^Temp^LN", "36.8", "cel", tc.statusIn, "20260901101500"),
			)
			p, err := NewHL7v2Parser().Parse(data)
			if err != nil {
				t.Fatalf("unexpected reject: %v", err)
			}
			if p.FormatVersion != tc.ver || p.EventID != "EVT-T1" || p.PatientMRN != "MRN-1" {
				t.Fatalf("unexpected parsed header: %+v", p)
			}
			if p.Status != tc.wantStatus {
				t.Fatalf("status=%q want %q", p.Status, tc.wantStatus)
			}
			if p.EventTime != "2026-09-01T10:15:00Z" || p.EffectiveTime != "2026-09-01T10:15:00Z" {
				t.Fatalf("time normalization failed: event=%q eff=%q", p.EventTime, p.EffectiveTime)
			}
			t.Logf("messageId=EVT-T1 basis=valid HL7 %s observation accepted status=%q", tc.ver, p.Status)
		})
	}
}

func TestHL7MalformedRejectReasons(t *testing.T) {
	goodOBX := hl7OBX("8310-5", "36.8", "cel", "F", "20260901101500")
	cases := []struct {
		name   string
		data   []byte
		reason RejectReason
	}{
		{"empty", nil, ReasonEmptyPayload},
		{"bad encoding chars", buildHL7("MSH|^X&|LAB||||||ORU^R01|E|P|2.3", hl7PID("M"), goodOBX), ReasonEncodingInvalid},
		{"not utf8", append([]byte("MSH|^~\\&|LAB||\xff|||ORU^R01|E|P|2.3\r"), goodOBX...), ReasonEncodingInvalid},
		{"truncated missing PID/OBX", buildHL7(hl7MSH("ORU^R01", "E", "2.3", "20260901101500")), ReasonStructureTruncated},
		{"first segment not MSH", buildHL7(hl7PID("M"), hl7MSH("ORU^R01", "E", "2.3", "20260901101500")), ReasonStructureTruncated},
		{"bad escape", buildHL7(hl7MSH("ORU^R01", "E", "2.3", "20260901101500"), "PID|1||M||DOE^JO\\QHN", goodOBX), ReasonEncodingInvalid},
		{"unsupported version", buildHL7(hl7MSH("ORU^R01", "E", "2.1", "20260901101500"), hl7PID("M"), goodOBX), ReasonUnsupportedVersion},
		{"unsupported event", buildHL7(hl7MSH("XXX^YYY", "E", "2.3", "20260901101500"), hl7PID("M"), goodOBX), ReasonUnsupportedEventType},
		{"missing event id", buildHL7("MSH|^~\\&|LAB|HOSP|EHR|HOSP|20260901101500||ORU^R01||P|2.3", hl7PID("M"), goodOBX), ReasonMissingEventID},
		{"missing mrn", buildHL7(hl7MSH("ORU^R01", "E", "2.3", "20260901101500"), "PID|1||", goodOBX), ReasonMissingPatientID},
		{"missing code", buildHL7(hl7MSH("ORU^R01", "E", "2.3", "20260901101500"), hl7PID("M"), hl7OBX("", "36.8", "cel", "F", "20260901101500")), ReasonMissingCode},
		{"invalid time", buildHL7(hl7MSH("ORU^R01", "E", "2.3", "bad-time"), hl7PID("M"), goodOBX), ReasonInvalidTime},
		{"illegal component in status", buildHL7(hl7MSH("ORU^R01", "E", "2.3", "20260901101500"), hl7PID("M"), hl7OBX("8310-5", "36.8", "cel", "F^X", "20260901101500")), ReasonInvalidStatus},
		{"illegal segment name", buildHL7(hl7MSH("ORU^R01", "E", "2.3", "20260901101500"), hl7PID("M"), goodOBX, "B4|x"), ReasonStructureTruncated},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewHL7v2Parser().Parse(tc.data)
			if err == nil {
				t.Fatalf("expected reject %s, got acceptance", tc.reason)
			}
			re, ok := err.(*RejectError)
			if !ok || re.Reason != tc.reason {
				t.Fatalf("reason=%v want %s (err=%v)", re, tc.reason, err)
			}
			t.Logf("messageId=E basis=malformed HL7 (%s) rejected reason=%s detail=%q", tc.name, re.Reason, re.Detail)
		})
	}
}

func TestHL7UnknownSegmentAccepted(t *testing.T) {
	data := buildHL7(
		hl7MSH("ORU^R01", "EVT-Z", "2.5.1", "20260901101500"),
		hl7PID("MRN-1"),
		hl7OBX("8310-5", "36.8", "cel", "F", "20260901101500"),
		"ZZZ|1|extra-data",
	)
	p, err := NewHL7v2Parser().Parse(data)
	if err != nil {
		t.Fatalf("unknown segment must not fail parsing: %v", err)
	}
	got := map[string]string{}
	for _, u := range p.Unknown {
		got[u.Path] = u.Value
	}
	if got["ZZZ-2"] != "extra-data" || got["ZZZ-1"] != "1" {
		t.Fatalf("unknown segment not captured: %+v", p.Unknown)
	}
	t.Logf("messageId=EVT-Z basis=unknown ZZZ segment captured as raw fields, parsing succeeds")
}

func TestRegistryDetect(t *testing.T) {
	r := DefaultRegistry()
	if p, err := r.Detect([]byte("MSH|^~\\&|")); err != nil || p.Format() != HL7v2Name {
		t.Fatalf("HL7 detect failed: %v", err)
	}
	if p, err := r.Detect([]byte(`{"resourceType":"Observation"}`)); err != nil || p.Format() != FHIRJSONName {
		t.Fatalf("FHIR detect failed: %v", err)
	}
	if _, err := r.Detect(nil); err == nil {
		t.Fatal("empty payload must reject")
	}
	if _, err := r.Detect([]byte("!!binary!!")); err == nil {
		t.Fatal("unknown format must reject")
	}
}
