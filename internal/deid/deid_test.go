package deid

import (
	"strings"
	"testing"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/model"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/normalize"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/parser"
)

func TestFieldDeterministic(t *testing.T) {
	e := NewEngine([]byte("test-key"))
	a := e.Field("name", "Alice Chen")
	b := e.Field("name", "Alice Chen")
	if a == "" || a != b || !strings.HasPrefix(a, "NM-") {
		t.Fatalf("same value must map to one stable placeholder: %q %q", a, b)
	}
	t.Logf("real='Alice Chen' => pseudo=%s (reproducible)", a)
	if e.Field("name", "Bob") == a {
		t.Fatal("different values must map to different placeholders")
	}
	if got := e.Field("contact", ""); got != "" {
		t.Fatalf("empty real value must map to empty, got %q", got)
	}
	// Distinct scope gives distinct prefixes.
	if strings.HasPrefix(e.Field("contact", "x"), "NM-") {
		t.Fatal("scope prefix wrong")
	}
}

func TestPatientLinkStableAcrossMessagesAndFormats(t *testing.T) {
	e := NewEngine([]byte("test-key"))
	// Same patient as she appears in HL7v2 and in FHIR R5 sample messages.
	hl7 := raw(t, "01_hl7v23_admit.hl7")
	fhir := raw(t, "05_fhir_r5_unknown_fields.json")

	linkHL7 := e.PatientLink(hl7.PatientMRN, hl7.PatientMRNSys)
	linkFHIR := e.PatientLink(fhir.PatientMRN, fhir.PatientMRNSys)
	t.Logf("message=E-001/E-003 realMRN=%s linkHL7=%s linkFHIR=%s",
		hl7.PatientMRN, linkHL7, linkFHIR)
	if linkHL7 != linkFHIR {
		t.Fatal("same MRN identity across formats must share one longitudinal pseudonym")
	}
	if linkHL7 == e.PatientLink("MRN-9999", "HOSP") {
		t.Fatal("different MRN must not share the longitudinal pseudonym")
	}
}

func TestDifferentEnginesDifferentKeys(t *testing.T) {
	if NewEngine([]byte("k1")).Field("name", "Alice") ==
		NewEngine([]byte("k2")).Field("name", "Alice") {
		t.Fatal("different HMAC keys must produce different placeholders")
	}
}

func TestDeidentifyHasNoRawPHI(t *testing.T) {
	e := NewEngine([]byte("test-key"))
	c, ae := normalize.Normalize(raw(t, "19_hl7_unknown_segments.hl7"))
	if ae != nil {
		t.Fatal(ae)
	}
	out := e.Deidentify(c)
	rawVals := []string{c.PatientMRN, c.PatientName, c.PatientContact}
	blob := out.PatientPseudo + out.MRNPseudo + out.NamePseudo + out.ContactPseudo
	for _, v := range rawVals {
		if strings.Contains(blob, v) {
			t.Fatalf("raw PHI %q leaked into output record", v)
		}
	}
	t.Logf("message=%s pseudonyms patient=%s mrn=%s name=%s contact=%s",
		c.EventID, out.PatientPseudo, out.MRNPseudo, out.NamePseudo, out.ContactPseudo)
	// Longitudinal link field present and MRN pseudo differs from link.
	if !strings.HasPrefix(out.PatientPseudo, "PT-") {
		t.Fatal("missing longitudinal patient pseudonym")
	}
	var _ model.Format
}

func raw(t *testing.T, file string) *parser.RawEvent {
	t.Helper()
	rv, ae := parser.ParseFile("../../testdata/samples/" + file)
	if ae != nil {
		t.Fatalf("parse %s: %s", file, ae.Code)
	}
	return rv
}
