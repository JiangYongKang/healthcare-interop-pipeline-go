package normalize

import (
	"testing"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/apperr"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/parser"
)

func mustParse(t *testing.T, file string) *parser.RawEvent {
	t.Helper()
	rv, ae := parser.ParseFile("../../testdata/samples/" + file)
	if ae != nil {
		t.Fatalf("parse %s: %s (%s)", file, ae.Code, ae.InternalDetail)
	}
	return rv
}

func TestDefaultsForOldVersion(t *testing.T) {
	c, ae := Normalize(mustParse(t, "06_fhir_dstu2_old_missing.json"))
	if ae != nil {
		t.Fatalf("unexpected: %s %s", ae.Code, ae.InternalDetail)
	}
	t.Logf("message=%s version=%s defaults=%v", c.EventID, c.Version, c.Defaults)
	if c.PatientGender != DefaultGender {
		t.Fatalf("gender default=%q", c.PatientGender)
	}
	if c.PatientMRNSys != DefaultMRNSystem {
		t.Fatalf("mrn-system default=%q", c.PatientMRNSys)
	}
	if c.PatientContact != "" || c.PatientBirthDate != "" {
		t.Fatalf("omitted fields must default to empty: contact=%q dob=%q",
			c.PatientContact, c.PatientBirthDate)
	}
	want := map[string]bool{
		"gender:UNK": false, "mrn-system:LOCAL-MPI": false,
		"contact:empty": false, "birth-date:empty": false,
		"sequence:derived-from-effective-time": false,
	}
	for _, d := range c.Defaults {
		want[d] = true
	}
	for d, ok := range want {
		if !ok {
			t.Fatalf("stable default %q not recorded: %v", d, c.Defaults)
		}
	}
}

func TestMissingRequired(t *testing.T) {
	rv, pae := parser.ParseFile("../../testdata/samples/14_hl7_missing_mrn.hl7")
	if pae != nil {
		t.Fatalf("parser should not reject: %s", pae.Code)
	}
	_, ae := Normalize(rv)
	if ae == nil || ae.Code != apperr.CodeMissingRequired {
		t.Fatalf("got %v, want MISSING_REQUIRED_FIELD", ae)
	}
	t.Logf("message=%s decision=REJECT code=%s basis=%q", ae.MessageID, ae.Code, ae.InternalDetail)
}

func TestInvalidValue(t *testing.T) {
	rv, _ := parser.ParseFile("../../testdata/samples/15_hl7_invalid_time.hl7")
	_, ae := Normalize(rv)
	if ae == nil || ae.Code != apperr.CodeInvalidValue {
		t.Fatalf("got %v, want INVALID_VALUE", ae)
	}
	t.Logf("message=%s decision=REJECT code=%s basis=%q", ae.MessageID, ae.Code, ae.InternalDetail)
}

// TestCrossFormatFingerprintEqual is the core idempotency guarantee: the same
// business event over HL7v2 and FHIR JSON normalizes to one fingerprint.
func TestCrossFormatFingerprintEqual(t *testing.T) {
	a, ae := Normalize(mustParse(t, "01_hl7v23_admit.hl7"))
	if ae != nil {
		t.Fatal(ae)
	}
	b, ae := Normalize(mustParse(t, "02_fhir_r4_dup_of_hl7.json"))
	if ae != nil {
		t.Fatal(ae)
	}
	fa, fb := Fingerprint(a), Fingerprint(b)
	t.Logf("message=E-001 hl7-fp=%s fhir-fp=%s", fa, fb)
	if fa != fb {
		t.Fatalf("cross-format fingerprint mismatch:\nHL7:  %+v\nFHIR: %+v", a, b)
	}
}

func TestGenderCanonicalization(t *testing.T) {
	cases := map[string]string{"F": "F", "f": "F", "female": "F", "M": "M", "male": "M", "": "UNK"}
	for in, want := range cases {
		got, gae := canonicalGender(in)
		if gae != nil || got != want {
			t.Fatalf("gender %q => %q err=%v, want %q", in, got, gae, want)
		}
	}
	if _, err := canonicalGender("X"); err == nil {
		t.Fatal("unknown gender code must be INVALID_VALUE")
	}
}

func TestHL7TimePrecision(t *testing.T) {
	s, key, ae := hl7Time("20260901080000")
	if ae != nil {
		t.Fatal(ae)
	}
	if s != "2026-09-01T08:00:00" {
		t.Fatalf("canonical=%s", s)
	}
	t.Logf("message=- time 20260901080000 => %s key=%d", s, key)
	if _, _, ae = hl7Time("2026-09"); ae == nil || ae.Code != apperr.CodeInvalidValue {
		t.Fatalf("bad precision got %v", ae)
	}
}
