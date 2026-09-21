package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/formats"
)

func newTestEngine(t *testing.T, maxInFlight int) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.OutputDir = t.TempDir()
	cfg.ResetOutput = true
	cfg.MaxRecords = 1000
	cfg.MaxInFlight = maxInFlight
	eng, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

func hl7Msg(eid, mrn, ver, tstamp string) []byte {
	return []byte(fmt.Sprintf(
		"MSH|^~\\&|LAB|HOSP|EHR|HOSP|%s||ORU^R01|%s|P|%s\rPID|1||%s^^^HOSP^MR||DOE^JOHN\rOBX|1||8310-5^Temp^LN||36.8|cel|||||F|||%s\r",
		tstamp, eid, ver, mrn, tstamp))
}

func TestEngineExternalErrorClassification(t *testing.T) {
	eng := newTestEngine(t, 4)
	ctx := context.Background()
	cases := []struct {
		name    string
		path    string // optional: read sample file
		payload []byte
		want    ErrorType
		reason  string
	}{
		{"truncated structure", "", []byte("MSH|^~\\&|LAB|||20260901||ORU^R01|E|P|2.3\r"), ErrFormatError, string(formats.ReasonStructureTruncated)},
		{"unsupported version", "", []byte("MSH|^~\\&|LAB|||20260901||ORU^R01|E|P|9.9\rPID|1||M\rOBX|1||c||||||F|||20260901\r"), ErrFormatError, string(formats.ReasonUnsupportedVersion)},
		{"missing patient", "", []byte("MSH|^~\\&|LAB|HOSP|EHR|HOSP|20260901101500||ORU^R01|E|P|2.3\rPID|1||\rOBX|1||c||36.8|cel|||||F|||20260901101500\r"), ErrValidationError, string(formats.ReasonMissingPatientID)},
		{"bad json", "", []byte("{oops"), ErrFormatError, string(formats.ReasonJSONInvalid)},
		{"truncated json", "", []byte(`{"resourceType":"Observation","id":"x"`), ErrFormatError, string(formats.ReasonStructureTruncated)},
		{"fhir invalid status", "", []byte(`{"resourceType":"Observation","id":"E","status":"weird","subject":{"reference":"Patient/M"},"code":{"coding":[{"code":"c"}]},"effectiveDateTime":"2026-09-01T00:00:00Z"}`), ErrValidationError, string(formats.ReasonInvalidStatus)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := tc.payload
			if tc.path != "" {
				b, err := os.ReadFile(tc.path)
				if err != nil {
					t.Fatal(err)
				}
				payload = b
			}
			r := eng.Ingest(ctx, "test-msg", payload)
			if r.Error == nil || r.Error.Type != tc.want || r.Error.Reason != tc.reason {
				t.Fatalf("got %+v want %s/%s", r.Error, tc.want, tc.reason)
			}
			// Internal parser detail must not be the external message.
			if r.Error.Detail == r.Error.Message || r.Error.Message == "" {
				t.Fatalf("external message leaked internal detail: %+v", r.Error)
			}
			t.Logf("basis=%s -> external=%s reason=%s (internal detail kept server-side)", tc.name, r.Error.Type, r.Error.Reason)
		})
	}
}

func TestEngineCrossFormatPatientLinkage(t *testing.T) {
	eng := newTestEngine(t, 4)
	ctx := context.Background()
	r1 := eng.Ingest(ctx, "h1", hl7Msg("HE1", "MRN-LINK", "2.5.1", "20260901101500"))
	fhir := []byte(`{"resourceType":"Observation","id":"FE1","status":"final","subject":{"reference":"Patient/MRN-LINK"},"code":{"coding":[{"code":"8310-5"}]},"effectiveDateTime":"2026-09-02T00:00:00Z"}`)
	r2 := eng.Ingest(ctx, "f1", fhir)
	if r1.Error != nil || r2.Error != nil {
		t.Fatalf("ingest failed: %+v %+v", r1.Error, r2.Error)
	}
	recs, _ := eng.Snapshot(ctx)
	var refs []string
	for _, r := range recs {
		if r.EventID == "HE1" || r.EventID == "FE1" {
			refs = append(refs, r.PatientRef)
		}
	}
	if len(refs) != 2 || refs[0] != refs[1] {
		t.Fatalf("same real patient across formats must share pseudonym: %v", refs)
	}
	t.Logf("basis=MRN-LINK in HL7v2 and FHIR-JSON both -> %s (longitudinal linkage preserved)", refs[0])
}

func TestEngineConcurrentConsistency(t *testing.T) {
	eng := newTestEngine(t, 8)
	ctx := context.Background()

	const goroutines = 16
	const repeats = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	outcomes := map[Outcome]int{}

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < repeats; i++ {
				// Each event id is hammered by every goroutine: only one
				// content per id may ever become a record.
				id := fmt.Sprintf("EVT-%03d", i%10)
				r := eng.Ingest(ctx, fmt.Sprintf("g%d-i%d", g, i), hl7Msg(id, "MRN-CONC", "2.5.1", "20260901101500"))
				mu.Lock()
				outcomes[r.Outcome]++
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()

	recs, err := eng.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 10 {
		t.Fatalf("exactly 10 distinct records expected under concurrent load, got %d", len(recs))
	}
	pseudos := map[string]bool{}
	for _, r := range recs {
		if r.PatientRef == "MRN-CONC" {
			t.Fatal("raw MRN leaked under concurrency")
		}
		pseudos[r.PatientRef] = true
	}
	if len(pseudos) != 1 {
		t.Fatalf("one real MRN got %d placeholders under concurrency: %v", len(pseudos), pseudos)
	}
	if outcomes[OutcomeAccepted] != 10 {
		t.Fatalf("accepted count=%d want 10 (outcomes=%v)", outcomes[OutcomeAccepted], outcomes)
	}
	if outcomes[OutcomeDuplicate] != goroutines*repeats-10 {
		t.Fatalf("duplicate count=%d want %d (outcomes=%v)",
			outcomes[OutcomeDuplicate], goroutines*repeats-10, outcomes)
	}
	t.Logf("basis=%d goroutines x %d msgs: accepted=%d duplicate=%d records=%d pseudonyms=%d (no half-state, no double placeholder)",
		goroutines, repeats, outcomes[OutcomeAccepted], outcomes[OutcomeDuplicate], len(recs), len(pseudos))
}

func TestEngineMixedSampleDirectory(t *testing.T) {
	eng := newTestEngine(t, 16)
	ctx := context.Background()
	root := findRepoRoot(t)
	paths, err := filepath.Glob(filepath.Join(root, "testdata", "samples", "*"))
	if err != nil || len(paths) < 20 {
		t.Fatalf("samples not found: %d (%v)", len(paths), err)
	}
	paths = filterFiles(paths)
	results := eng.IngestPaths(ctx, paths)
	counts := map[Outcome]int{}
	for _, r := range results {
		counts[r.Outcome]++
	}
	// Sorted input + ordered commits makes every label reproducible.
	if counts[OutcomeAccepted] != 9 || counts[OutcomeDuplicate] != 2 ||
		counts[OutcomeOutOfOrder] != 1 || counts[OutcomeRejected] != 13 {
		t.Fatalf("unexpected distribution over samples: %v", counts)
	}
	recs, _ := eng.Snapshot(ctx)
	if len(recs) != 9 {
		t.Fatalf("final distinct records must be 9, got %d", len(recs))
	}
	t.Logf("basis=concurrent parse with ordered commits over samples (n=%d): %v; final records=%d",
		len(results), counts, len(recs))
}

func TestEngineOutOfOrderMarkedSequentially(t *testing.T) {
	eng := newTestEngine(t, 4)
	ctx := context.Background()
	root := findRepoRoot(t)
	// Sorted order places fresh EVT-0002 (2.5.1) before the stale 2.3
	// redelivery, so the stale one must be deterministically marked.
	fresh := filepath.Join(root, "testdata", "samples", "hl7_obs_v251_ok.hl7")
	stale := filepath.Join(root, "testdata", "samples", "hl7_stale_0002.hl7")
	r1 := eng.IngestFile(ctx, fresh)
	r2 := eng.IngestFile(ctx, stale)
	if r1.Outcome != OutcomeAccepted || r2.Outcome != OutcomeOutOfOrder {
		t.Fatalf("fresh=%s stale=%s", r1.Outcome, r2.Outcome)
	}
	t.Logf("messageId=EVT-0002 basis=fresh revision present first; later stale 2.3 marked %s: %s",
		r2.Outcome, r2.Error.Reason)
}

func filterFiles(paths []string) []string {
	out := paths[:0]
	for _, p := range paths {
		if fi, err := os.Stat(p); err == nil && !fi.IsDir() {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
}
