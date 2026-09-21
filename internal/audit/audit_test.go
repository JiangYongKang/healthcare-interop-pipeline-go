package audit

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/apperr"
)

func TestConcurrentAppendSequential(t *testing.T) {
	l, err := NewLog("")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Append(Entry{MessageID: "E", Decision: DecisionAccepted})
		}(i)
	}
	wg.Wait()
	got := l.Entries()
	if len(got) != n {
		t.Fatalf("entries=%d want=%d", len(got), n)
	}
	for i, e := range got {
		if e.Seq != int64(i+1) {
			t.Fatalf("non-contiguous seq at %d: %d", i, e.Seq)
		}
	}
	t.Logf("message=E concurrent appends=%d contiguous seq 1..%d OK", n, n)
}

func TestAuditSpooledToFileAndPHIFree(t *testing.T) {
	dir := t.TempDir()
	l, err := NewLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	l.Append(Entry{
		MessageID: "E-001", Decision: DecisionRejected,
		Code: apperr.CodeMalformedJSON, Reason: "safe text",
		Internal: "raw PHI MRN-SECRET must never be written",
	})
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "audit.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"decision":"REJECTED"`) {
		t.Fatalf("spool content wrong: %s", data)
	}
	if strings.Contains(string(data), "MRN-SECRET") {
		t.Fatal("internal/PHI detail must not be spooled to the audit file")
	}
	// Exactly one JSON line.
	lines := countLines(data)
	if lines != 1 {
		t.Fatalf("audit lines=%d", lines)
	}
	t.Logf("message=E-001 spooled=1 line; internal detail absent from file")
}

func countLines(b []byte) int {
	s := bufio.NewScanner(strings.NewReader(string(b)))
	n := 0
	for s.Scan() {
		if strings.TrimSpace(s.Text()) != "" {
			n++
		}
	}
	return n
}
