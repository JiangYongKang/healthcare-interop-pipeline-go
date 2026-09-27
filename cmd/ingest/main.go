// Command ingest runs the local clinical interoperability pipeline over
// sample message files: ./ingest -dir testdata/samples -out output
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/pipeline"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ingest:", err)
		os.Exit(1)
	}
}

func run() error {
	dir := flag.String("dir", "testdata/samples", "directory containing sample messages")
	out := flag.String("out", "output", "output directory")
	policy := flag.String("policy", string(pipeline.OverflowReject), "overflow policy: reject|degrade")
	maxRecords := flag.Int("max-records", 10000, "maximum accepted distinct events (0=unlimited)")
	maxInFlight := flag.Int("max-inflight", 32, "maximum concurrently processed messages")
	reset := flag.Bool("reset", false, "reset output directory before ingestion")
	flag.Parse()

	cfg := pipeline.DefaultConfig()
	cfg.OutputDir = *out
	cfg.OverflowPolicy = pipeline.OverflowPolicy(*policy)
	cfg.MaxRecords = *maxRecords
	cfg.MaxInFlight = *maxInFlight
	cfg.ResetOutput = *reset

	eng, err := pipeline.NewEngine(cfg)
	if err != nil {
		return err
	}
	defer eng.Close()

	ctx := context.Background()
	paths, err := listMessageFiles(*dir)
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return fmt.Errorf("no message files found in %s", *dir)
	}

	results := eng.IngestPaths(ctx, paths)

	counts := map[pipeline.Outcome]int{}
	for i, r := range results {
		counts[r.Outcome]++
		switch r.Outcome {
		case pipeline.OutcomeAccepted, pipeline.OutcomeDegraded:
			fmt.Printf("[%s] %s eventId=%s reason=%q\n", r.Outcome, filepath.Base(paths[i]), r.EventID, r.Reason)
		default:
			et := "-"
			if r.Error != nil {
				et = string(r.Error.Type)
			}
			fmt.Printf("[%s] %s eventId=%s errorType=%s reason=%s basis=%q\n",
				r.Outcome, filepath.Base(paths[i]), r.EventID, et, r.Reason, r.Error)
		}
	}

	records, err := eng.Snapshot(ctx)
	if err != nil {
		return err
	}
	if err := writeRecordsJSONL(filepath.Join(*out, "records.jsonl"), records); err != nil {
		return err
	}

	fmt.Printf("\nsummary: files=%d accepted=%d duplicate=%d outOfOrder=%d conflict=%d degraded=%d rejected=%d -> %s/records.jsonl\n",
		len(results),
		counts[pipeline.OutcomeAccepted], counts[pipeline.OutcomeDuplicate],
		counts[pipeline.OutcomeOutOfOrder], counts[pipeline.OutcomeConflict],
		counts[pipeline.OutcomeDegraded],
		counts[pipeline.OutcomeRejected], *out)

	// Event consistency view: one chain per clinical event.
	chains, err := eng.Chains(ctx)
	if err != nil {
		return err
	}
	for _, c := range chains {
		eff := "-"
		if c.Effective != nil {
			eff = fmt.Sprintf("%s/%s value=%q", c.Effective.SourceFormat, c.Effective.SourceVersion, c.Effective.Value)
		}
		fmt.Printf("[chain] %s eventId=%s patient=%s status=%s deliveries=%d pending=%d effective=%s\n",
			c.ChainID, c.EventID, c.PatientRef, c.Status, len(c.Deliveries), c.PendingConflicts, eff)
	}
	pending, err := eng.PendingConflicts(ctx)
	if err != nil {
		return err
	}
	for _, c := range pending {
		extra := ""
		if c.Reason == pipeline.ReasonPatientMismatch {
			extra = fmt.Sprintf(" currentPatient=%s incomingPatient=%s", c.CurrentPatient, c.IncomingPatient)
		}
		fmt.Printf("[pending-review] %s eventId=%s reason=%s current=%s incoming=%s%s\n",
			c.ConflictID, c.EventID, c.Reason, short(c.CurrentDigest), short(c.IncomingDigest), extra)
	}
	return nil
}

func short(d string) string {
	if len(d) > 12 {
		return d[:12]
	}
	return d
}

func listMessageFiles(dir string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		ext := filepath.Ext(path)
		if ext == ".hl7" || ext == ".json" || ext == ".msg" || ext == ".txt" {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	return paths, nil
}

func writeRecordsJSONL(path string, records []*pipeline.Record) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	for _, r := range records {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	return f.Sync()
}
