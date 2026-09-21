package pipeline

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/apperr"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/audit"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/model"
)

func sample(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "samples", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newPipe(t *testing.T, cfg Config) *Pipeline {
	t.Helper()
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// TestSampleMatrix runs the full sample set and asserts one reproducible
// decision per message identity, logging id + basis for each.
func TestSampleMatrix(t *testing.T) {
	p := newPipe(t, Config{})
	want := map[string]struct {
		code apperr.Code // empty => accepted
	}{
		"01_hl7v23_admit.hl7":              {},
		"02_fhir_r4_dup_of_hl7.json":       {apperr.CodeDuplicateEvent},
		"03_hl7v23_dup.hl7":                {apperr.CodeDuplicateEvent},
		"04_fhir_r4_event2.json":           {},
		"05_fhir_r5_unknown_fields.json":   {},
		"06_fhir_dstu2_old_missing.json":   {},
		"07_hl7_truncated.hl7":             {apperr.CodeTruncated},
		"08_fhir_truncated.json":           {apperr.CodeTruncated},
		"09_fhir_malformed.json":           {apperr.CodeMalformedJSON},
		"10_hl7_illegal_encoding.hl7":      {apperr.CodeIllegalEncoding},
		"11_hl7_unsupported_version.hl7":   {apperr.CodeUnsupportedVersion},
		"12_fhir_unsupported_version.json": {apperr.CodeUnsupportedVersion},
		"13_unknown_format.dat":            {apperr.CodeUnknownFormat},
		"14_hl7_missing_mrn.hl7":           {apperr.CodeMissingRequired},
		"15_hl7_invalid_time.hl7":          {apperr.CodeInvalidValue},
		"16_fhir_clash_event2.json":        {apperr.CodeStorageClash},
		"17_fhir_stale_event2.json":        {apperr.CodeStaleVersion},
		"18_hl7_newer_e1.hl7":              {},
		"19_hl7_unknown_segments.hl7":      {},
		"00_empty.dat":                     {apperr.CodeTruncated},
	}
	names := make([]string, 0, len(want))
	for n := range want {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		w := want[name]
		res, ae := p.Ingest(sample(t, name))
		gotCode := apperr.Code("")
		if res != nil && !res.Accepted {
			gotCode = res.Code
		}
		if ae != nil {
			gotCode = ae.Code
		}
		id := "-"
		basis := ""
		if ae != nil {
			id, basis = ae.MessageID, ae.InternalDetail
		} else if res != nil {
			id = res.MessageID
		}
		t.Logf("file=%-32s message=%-6s code=%-22s basis=%q", name, id, gotCode, basis)
		if gotCode != w.code {
			t.Fatalf("%s: code=%s want=%s", name, gotCode, w.code)
		}
	}

	// Final current set and lifecycle expectations.
	if cur, ok := p.store.Get("E-001"); !ok || cur.Version != "2.5.1" {
		t.Fatalf("E-001 current must be newer 2.5.1 version: %+v", cur)
	}
	hist := p.store.History()
	var e1stale int
	for _, h := range hist {
		if h.EventID == "E-001" && h.Status == model.StatusStale {
			e1stale++
		}
	}
	if e1stale != 1 {
		t.Fatalf("expected one stale E-001 line, got %d", e1stale)
	}
	if len(p.store.List()) != 5 {
		t.Fatalf("expected 5 current distinct events, got %d", len(p.store.List()))
	}
}

// TestErrorKindClassification verifies upstream, business and storage
// failures map to distinct external kinds, and no internal parser detail is
// present in the caller-safe message.
func TestErrorKindClassification(t *testing.T) {
	p := newPipe(t, Config{})
	cases := []struct {
		file string
		kind apperr.Kind
	}{
		{"09_fhir_malformed.json", apperr.KindFormat},
		{"14_hl7_missing_mrn.hl7", apperr.KindValidation},
		{"04_fhir_r4_event2.json", apperr.KindFormat}, // accepted below, skip via flag
	}
	// seed E-002 to create a storage conflict then check kinds.
	if _, ae := p.Ingest(sample(t, "04_fhir_r4_event2.json")); ae != nil {
		t.Fatal(ae)
	}
	_, clash := p.Ingest(sample(t, "16_fhir_clash_event2.json"))
	_, malformed := p.Ingest(sample(t, cases[0].file))
	_, missing := p.Ingest(sample(t, cases[1].file))
	t.Logf("message=E-002 kind=%s code=%s safe=%q", clash.Kind, clash.Code, apperr.SafeMessage(clash))
	if clash.Kind != apperr.KindConflict || malformed.Kind != apperr.KindFormat ||
		missing.Kind != apperr.KindValidation {
		t.Fatalf("kinds clash=%s fmt=%s val=%s", clash.Kind, malformed.Kind, missing.Kind)
	}
	for _, ae := range []*apperr.Error{clash, malformed, missing} {
		if ae.Detail == "" || strings.Contains(ae.Detail, "json") ||
			strings.Contains(ae.Detail, "MSH") {
			t.Fatalf("safe detail leaked internals: %q", ae.Detail)
		}
	}
}

// TestConcurrentConsistency hammers one business event (same MRN/event) and
// distinct events concurrently. Requirements verified:
//   - one real value -> one pseudonym across goroutines,
//   - identical redelivery never duplicates the stored record,
//   - no half-updated current state.
func TestConcurrentConsistency(t *testing.T) {
	p := newPipe(t, Config{MaxInFlight: 64})
	const workers = 64
	const perWorker = 10

	dup := sample(t, "01_hl7v23_admit.hl7")
	var wg sync.WaitGroup
	var mu sync.Mutex
	codes := map[apperr.Code]int{}
	var acceptedPseudos []string
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				res, ae := p.Ingest(dup)
				mu.Lock()
				switch {
				case ae != nil:
					codes[ae.Code]++
				case res.Accepted:
					codes["ACCEPTED"]++
					acceptedPseudos = append(acceptedPseudos, res.Record.PatientPseudo)
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	total := workers * perWorker
	t.Logf("message=E-001 concurrent attempts=%d outcomes=%v pseudos=%d", total, codes, len(acceptedPseudos))
	if codes["ACCEPTED"] != 1 {
		t.Fatalf("exactly one accept required, got %d", codes["ACCEPTED"])
	}
	if codes[apperr.CodeDuplicateEvent] != total-1 {
		t.Fatalf("remaining attempts must be duplicates: %v", codes)
	}
	if len(p.store.List()) != 1 {
		t.Fatalf("duplicates created records: current=%d", len(p.store.List()))
	}
	wantPseudo := p.store.List()[0].PatientPseudo
	if wantPseudo == "" || !strings.HasPrefix(wantPseudo, "PT-") {
		t.Fatal("missing stable patient pseudonym")
	}
	for _, ps := range acceptedPseudos {
		if ps != wantPseudo {
			t.Fatal("one real MRN assigned multiple pseudonyms under concurrency")
		}
	}
}

// TestConcurrentDistinctEventsNoClash checks many different events at once:
// each unique event accepted exactly once, store count is deterministic.
func TestConcurrentDistinctEventsNoClash(t *testing.T) {
	p := newPipe(t, Config{MaxInFlight: 64})
	tmpl := string(sample(t, "01_hl7v23_admit.hl7"))
	const n = 80
	var wg sync.WaitGroup
	var accepts, rejects int64
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msg := strings.Replace(tmpl, "E-001", fmt.Sprintf("E-%03d", i), 1)
			res, ae := p.Ingest([]byte(msg))
			mu.Lock()
			defer mu.Unlock()
			if ae == nil && res.Accepted {
				accepts++
			} else {
				rejects++
			}
		}(i)
	}
	wg.Wait()
	t.Logf("distinct events n=%d accepted=%d rejected=%d current=%d",
		n, accepts, rejects, len(p.store.List()))
	if accepts != n || rejects != 0 || len(p.store.List()) != n {
		t.Fatalf("accepts=%d rejects=%d current=%d", accepts, rejects, len(p.store.List()))
	}
}

// TestOutOfOrderArrival delivers an event's versions newest-first: the late
// older copy must be marked out-of-order without overwriting current.
func TestOutOfOrderArrival(t *testing.T) {
	p := newPipe(t, Config{})
	if _, ae := p.Ingest(sample(t, "04_fhir_r4_event2.json")); ae != nil {
		t.Fatal(ae)
	}
	res, ae := p.Ingest(sample(t, "17_fhir_stale_event2.json"))
	if ae == nil || ae.Code != apperr.CodeStaleVersion {
		t.Fatalf("want STALE_VERSION, got %v", ae)
	}
	t.Logf("message=E-002 late-older decision=STALE status=%s", res.Status)
	if res.Status != model.StatusOutOfOrder {
		t.Fatalf("late copy status=%s", res.Status)
	}
	cur, _ := p.store.Get("E-002")
	if cur.OrderingKey <= res.Record.OrderingKey {
		t.Fatal("current must stay the newer version")
	}
}

// TestAuditTrailCompleteness requires one entry per attempt, with the
// message id and a PHI-free safe reason present.
func TestAuditTrailCompleteness(t *testing.T) {
	p := newPipe(t, Config{})
	files := []string{"01_hl7v23_admit.hl7", "09_fhir_malformed.json", "03_hl7v23_dup.hl7"}
	for _, f := range files {
		p.Ingest(sample(t, f))
	}
	entries := p.Audit()
	if len(entries) != len(files) {
		t.Fatalf("audit entries=%d want=%d", len(entries), len(files))
	}
	for _, e := range entries {
		t.Logf("audit seq=%d message=%-6s decision=%-9s reason=%q",
			e.Seq, e.MessageID, e.Decision, e.Reason)
		if e.MessageID == "" {
			t.Fatal("audit entry must carry the message id")
		}
		if e.Reason == "" {
			t.Fatal("audit entry must carry a safe basis")
		}
		if strings.Contains(strings.ToUpper(e.Reason), "MRN-1001") ||
			strings.Contains(e.Reason, "Chen") {
			t.Fatal("safe audit reason must not contain raw PHI")
		}
	}
	decisions := map[audit.Decision]int{}
	for _, e := range entries {
		decisions[e.Decision]++
	}
	if decisions[audit.DecisionAccepted] != 1 ||
		decisions[audit.DecisionDuplicate] != 1 ||
		decisions[audit.DecisionRejected] != 1 {
		t.Fatalf("decision distribution wrong: %v", decisions)
	}
}

// TestMaxInFlightRejection ensures the concurrency ceiling returns a typed
// limit error (when semaphore is full), never a blocked silent hang.
func TestMaxInFlightRejection(t *testing.T) {
	p := newPipe(t, Config{MaxInFlight: 1})
	p.sem <- struct{}{} // saturate
	_, ae := p.Ingest(sample(t, "01_hl7v23_admit.hl7"))
	if ae == nil || ae.Kind != apperr.KindLimit || ae.Code != apperr.CodeCapacityExceeded {
		t.Fatalf("got %v, want LIMIT/CAPACITY_EXCEEDED", ae)
	}
	t.Logf("message=- saturated decision=REJECT kind=%s code=%s", ae.Kind, ae.Code)
	<-p.sem
}

// TestOutputContainsNoRawPHI scans every field of accepted output records
// for the real values known from the samples.
func TestOutputContainsNoRawPHI(t *testing.T) {
	p := newPipe(t, Config{})
	if _, ae := p.Ingest(sample(t, "01_hl7v23_admit.hl7")); ae != nil {
		t.Fatal(ae)
	}
	for _, r := range p.store.List() {
		blob := fmt.Sprintf("%+v", r)
		for _, secret := range []string{"MRN-1001", "Alice", "Chen", "555-1234"} {
			if strings.Contains(blob, secret) {
				t.Fatalf("raw value %q leaked into output %+v", secret, r)
			}
		}
		t.Logf("message=%s output verified PHI-free (patient=%s)", r.EventID, r.PatientPseudo)
	}
}
