package formats

import (
	"testing"
)

func fhirObs(id, ver, status, extra string) string {
	v := ""
	if ver != "" {
		v = `"fhirVersion":"` + ver + `",`
	}
	return `{` + v + `"resourceType":"Observation","id":"` + id + `","status":"` + status +
		`","subject":{"reference":"Patient/MRN-1"},"code":{"coding":[{"code":"8310-5"}]},` +
		`"effectiveDateTime":"2026-09-01T10:00:00Z"` + extra + `}`
}

func TestFHIRValidVersionsAndEnvelope(t *testing.T) {
	cases := []struct{ name, payload, wantVer string }{
		{"bare defaults to R4", fhirObs("E1", "", "final", ""), FHIRR4},
		{"envelope STU3", `{"fhirVersion":"3.0","resource":` +
			fhirObs("E2", "", "final", "") + `}`, FHIRSTU3},
		{"R4 explicit", fhirObs("E3", "4.0", "final", ""), FHIRR4},
		{"date-only effective", `{"resourceType":"Observation","id":"E4","status":"final",` +
			`"subject":{"reference":"Patient/MRN-1"},"code":{"coding":[{"code":"8310-5"}]},"effectiveDateTime":"2026-09-01"}`, FHIRR4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := NewFHIRJSONParser().Parse([]byte(tc.payload))
			if err != nil {
				t.Fatalf("unexpected reject: %v", err)
			}
			if p.FormatVersion != tc.wantVer || p.EventTime != "2026-09-01T10:00:00Z" &&
				tc.name != "date-only effective" {
				t.Fatalf("version=%s time=%s want %s", p.FormatVersion, p.EventTime, tc.wantVer)
			}
			if tc.name == "date-only effective" && p.EventTime != "2026-09-01T00:00:00Z" {
				t.Fatalf("date normalization=%s", p.EventTime)
			}
			t.Logf("messageId=%s basis=valid FHIR %s accepted eventTime=%s", p.EventID, p.FormatVersion, p.EventTime)
		})
	}
}

func TestFHIRMalformedRejectReasons(t *testing.T) {
	cases := []struct {
		name, payload string
		reason        RejectReason
	}{
		{"truncated", `{"resourceType":"Observation","id":"X"`, ReasonStructureTruncated},
		{"invalid json", `{"resourceType":}`, ReasonJSONInvalid},
		{"duplicate key", `{"resourceType":"Observation","resourceType":"Condition"}`, ReasonJSONInvalid},
		{"trailing data", `{"resourceType":"Observation"}xxx`, ReasonJSONInvalid},
		{"top level array", `[1,2,3]`, ReasonEmptyEnvelope},
		{"unsupported version", fhirObs("E", "DSTU2", "final", ""), ReasonUnsupportedVersion},
		{"missing resourceType", `{"id":"E"}`, ReasonEmptyEnvelope},
		{"unsupported resource", `{"resourceType":"Patient","id":"E"}`, ReasonUnsupportedEventType},
		{"missing id", `{"resourceType":"Observation"}`, ReasonMissingEventID},
		{"missing subject", `{"resourceType":"Observation","id":"E","status":"final","code":{"coding":[{"code":"x"}]},"effectiveDateTime":"2026-09-01T00:00:00Z"}`, ReasonMissingPatientID},
		{"bad subject ref", `{"resourceType":"Observation","id":"E","subject":{"reference":"Organization/1"},"code":{"coding":[{"code":"x"}]},"effectiveDateTime":"2026-09-01T00:00:00Z"}`, ReasonInvalidComponent},
		{"invalid status", fhirObs("E", "", "purple", ""), ReasonInvalidStatus},
		{"missing code", `{"resourceType":"Observation","id":"E","status":"final","subject":{"reference":"Patient/MRN-1"},"effectiveDateTime":"2026-09-01T00:00:00Z"}`, ReasonMissingCode},
		{"missing time", `{"resourceType":"Observation","id":"E","status":"final","subject":{"reference":"Patient/MRN-1"},"code":{"coding":[{"code":"x"}]}}`, ReasonMissingEventTime},
		{"invalid time", `{"resourceType":"Observation","id":"E","status":"final",` +
			`"subject":{"reference":"Patient/MRN-1"},"code":{"coding":[{"code":"x"}]},` +
			`"effectiveDateTime":"nope"}`, ReasonInvalidTime},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewFHIRJSONParser().Parse([]byte(tc.payload))
			if err == nil {
				t.Fatalf("expected reject %s", tc.reason)
			}
			re := err.(*RejectError)
			if re.Reason != tc.reason {
				t.Fatalf("reason=%s want %s detail=%q", re.Reason, tc.reason, re.Detail)
			}
			t.Logf("basis=malformed FHIR (%s) rejected reason=%s detail=%q", tc.name, re.Reason, re.Detail)
		})
	}
}

func TestFHIRUnknownFieldsCapturedNotFatal(t *testing.T) {
	payload := fhirObs("E-UK", "4.0", "final",
		`,"extension":[{"url":"http://x/y","valueString":"secret John"}],"customTop":"42"`)
	p, err := NewFHIRJSONParser().Parse([]byte(payload))
	if err != nil {
		t.Fatalf("unknown fields must not fail parsing: %v", err)
	}
	paths := map[string]bool{}
	for _, u := range p.Unknown {
		paths[u.Path] = true
	}
	if !paths["extension[0].url"] || !paths["extension[0].valueString"] || !paths["customTop"] {
		t.Fatalf("unknown fields not captured: %+v", p.Unknown)
	}
	t.Logf("messageId=E-UK basis=unknown/forward-compatible fields captured=%v", paths)
}

func TestFHIRBOMTolerated(t *testing.T) {
	data := append([]byte("\xef\xbb\xbf"), []byte(fhirObs("E-BOM", "", "final", ""))...)
	p, err := NewFHIRJSONParser().Parse(data)
	if err != nil {
		t.Fatalf("BOM should be tolerated: %v", err)
	}
	if p.EventID != "E-BOM" {
		t.Fatalf("event id=%s", p.EventID)
	}
}
