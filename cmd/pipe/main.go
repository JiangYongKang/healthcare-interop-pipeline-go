// Command pipe runs the local interop pipeline over sample message files:
// every file under -in is ingested and the de-identified, idempotent result
// plus the per-message decision trail is printed and spooled locally.
//
// Usage:
//
//	go run ./cmd/pipe -in testdata/samples -out .local-run
//	go run ./cmd/pipe -in testdata/samples -concurrent 8 -max-records 20 -policy degrade
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/apperr"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/pipeline"
)

func main() {
	in := flag.String("in", "testdata/samples", "directory of sample message files (or a single file)")
	out := flag.String("out", ".local-run", "directory for records.jsonl / audit.jsonl (empty = memory only)")
	concurrent := flag.Int("concurrent", 1, "number of concurrent ingesters")
	maxInFlight := flag.Int("max-inflight", 0, "in-flight concurrency ceiling (0 = unlimited)")
	maxRecords := flag.Int("max-records", 0, "current-record ceiling (0 = unlimited)")
	policy := flag.String("policy", "reject", "over-capacity policy: reject|degrade")
	flag.Parse()

	files, err := listMessages(*in)
	if err != nil {
		fatal(err)
	}

	p, err := pipeline.New(pipeline.Config{
		MaxInFlight: *maxInFlight,
		MaxRecords:  *maxRecords,
		StoreDir:    *out,
		Policy:      *policy,
		AuditDir:    *out,
	})
	if err != nil {
		fatal(err)
	}
	defer p.Close()

	ingestAll(p, files, *concurrent)

	printTrail(p)
	fmt.Println("---- current records ----")
	for _, r := range p.Store().List() {
		fmt.Printf("event=%-12s status=%-11s patient=%s mrn=%s contact=%s\n",
			r.EventID, r.Status, r.PatientPseudo, r.MRNPseudo, r.ContactPseudo)
	}
	if *out != "" {
		fmt.Println("---- artifacts ----")
		fmt.Println(filepath.Join(*out, "records.jsonl"))
		fmt.Println(filepath.Join(*out, "audit.jsonl"))
	}
}

func listMessages(path string) ([]string, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() {
		return []string{path}, nil
	}
	var files []string
	err = filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

type fileResult struct {
	name    string
	message string
	ok      bool
	code    apperr.Code
	detail  string
}

func ingestAll(p *pipeline.Pipeline, files []string, workers int) {
	var mu sync.Mutex
	var results []fileResult
	var wg sync.WaitGroup
	if workers < 1 {
		workers = 1
	}
	ch := make(chan string)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range ch {
				data, err := os.ReadFile(f)
				if err != nil {
					mu.Lock()
					results = append(results, fileResult{name: f, detail: err.Error()})
					mu.Unlock()
					continue
				}
				res, ae := p.Ingest(data)
				r := fileResult{name: f}
				if res != nil {
					r.message = res.MessageID
					r.ok = res.Accepted
					if !res.Accepted {
						r.code = res.Code
					}
				}
				if ae != nil {
					r.code = ae.Code
					r.detail = ae.Detail
					if r.message == "" {
						r.message = ae.MessageID
					}
				}
				mu.Lock()
				results = append(results, r)
				mu.Unlock()
			}
		}()
	}
	for _, f := range files {
		ch <- f
	}
	close(ch)
	wg.Wait()
	sort.Slice(results, func(i, j int) bool { return results[i].name < results[j].name })

	fmt.Println("---- per-file decisions ----")
	for _, r := range results {
		state := "ACCEPTED"
		if !r.ok {
			state = "REJECTED/" + string(r.code)
		}
		id := r.message
		if id == "" {
			id = "-"
		}
		fmt.Printf("%-40s message=%-12s %s %s\n", filepath.Base(r.name), id, state, r.detail)
	}
}

func printTrail(p *pipeline.Pipeline) {
	fmt.Println("---- audit trail ----")
	for _, e := range p.Audit() {
		id := e.MessageID
		if id == "" {
			id = "-"
		}
		fmt.Printf("#%03d message=%-12s %-9s %-22s %s\n",
			e.Seq, id, e.Decision, string(e.Code), strings.TrimSpace(e.Reason))
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "pipe:", err)
	os.Exit(1)
}
