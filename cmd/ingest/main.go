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
	maxPending := flag.Int("max-pending-reviews", 0, "maximum chains pending conflict review (0=unlimited)")
	maxReplay := flag.Int("max-replay-tasks", 1, "maximum queued consistency-replay tasks")
	replay := flag.Bool("replay", false, "run a consistency replay after ingestion and print the report")
	reset := flag.Bool("reset", false, "reset output directory before ingestion")
	flag.Parse()

	cfg := pipeline.DefaultConfig()
	cfg.OutputDir = *out
	cfg.OverflowPolicy = pipeline.OverflowPolicy(*policy)
	cfg.MaxRecords = *maxRecords
	cfg.MaxInFlight = *maxInFlight
	cfg.MaxPendingReviews = *maxPending
	cfg.MaxReplayTasks = *maxReplay
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

	// Event-consistency view: chains waiting for a conflict review.
	pending, err := eng.PendingReviews(ctx)
	if err != nil {
		return err
	}
	for _, id := range pending {
		view, err := eng.Chain(ctx, id)
		if err != nil {
			return err
		}
		fmt.Printf("[PENDING_REVIEW] eventId=%s deliveries=%d currentValue=%s basis=%s\n",
			id, len(view.Deliveries), view.Current.Value, view.Deliveries[len(view.Deliveries)-1].Reason)
	}
	if len(pending) > 0 {
		fmt.Printf("pending reviews: %d chain(s) await a review conclusion\n", len(pending))
	}

	if *replay {
		rep, err := eng.SubmitReplay(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("replay: events=%d deliveries=%d pending=%d reviews=%d mismatches=%d replaced=%v\n",
			rep.Events, rep.Deliveries, rep.Pending, rep.Reviews, rep.Mismatches, rep.Replaced)
	}
	return nil
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
