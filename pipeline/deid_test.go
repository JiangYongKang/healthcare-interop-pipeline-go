package pipeline

import "testing"

func TestDeidDeterministicAndDistinct(t *testing.T) {
	a := NewDeidentifier("salt-x")
	b := NewDeidentifier("salt-x")
	c := NewDeidentifier("salt-y")

	p1, err := a.PatientPlaceholder("MRN-1001")
	if err != nil {
		t.Fatal(err)
	}
	p2, _ := b.PatientPlaceholder("MRN-1001")
	pOther, _ := a.PatientPlaceholder("MRN-1002")
	pDiffSalt, _ := c.PatientPlaceholder("MRN-1001")

	if p1 != p2 {
		t.Fatalf("same MRN must map to same placeholder across instances: %s vs %s", p1, p2)
	}
	if p1 == pOther {
		t.Fatalf("different MRNs must map to different placeholders: %s", p1)
	}
	if p1 == pDiffSalt {
		t.Fatalf("different salt must yield different placeholder")
	}
	if len(p1) != len(patientPrefix)+hashLen {
		t.Fatalf("placeholder shape wrong: %s", p1)
	}
	t.Logf("basis=HMAC-SHA256 deterministic MRN-1001 -> %s (stable across instances, differs per MRN/salt)", p1)

	tok1 := a.Token("field:PID-5", "John Doe")
	tok2 := b.Token("field:PID-5", "John Doe")
	tokOther := a.Token("field:PID-5", "Jane Roe")
	if tok1 != tok2 || tok1 == tokOther {
		t.Fatalf("token determinism/distinctness failed: %s %s %s", tok1, tok2, tokOther)
	}
	if _, err := a.PatientPlaceholder(""); err == nil {
		t.Fatal("empty MRN must error")
	}
}

func TestDeidRecordNoRawLeak(t *testing.T) {
	d := NewDeidentifier("s")
	r := &Record{
		EventID: "E", EventType: EventObservation, PatientRef: "MRN-1001",
		Code: "8310-5", Value: "36.8", CodeDisplay: "Temp",
		UnknownFields: map[string]string{"PID-5": "John Doe", "ZZZ-2": "secret"},
	}
	if err := d.DeidentifyRecord(r); err != nil {
		t.Fatal(err)
	}
	if r.PatientRef == "MRN-1001" {
		t.Fatal("real MRN leaked")
	}
	for k, v := range r.UnknownFields {
		if v == "John Doe" || v == "secret" {
			t.Fatalf("raw value leaked at %s", k)
		}
	}
	if r.Code != "8310-5" || r.Value != "36.8" {
		t.Fatalf("clinical facts must be preserved: code=%s value=%s", r.Code, r.Value)
	}
	t.Logf("messageId=E basis=record de-identified patientRef=%s unknownTokens=%v, clinical facts retained", r.PatientRef, r.UnknownFields)
}
