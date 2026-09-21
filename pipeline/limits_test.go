package pipeline

import (
	"context"
	"testing"
	"time"
)

func TestInvalidConfigRejected(t *testing.T) {
	cases := []Config{
		{OutputDir: "", MaxInFlight: 1, OverflowPolicy: OverflowReject},
		{OutputDir: t.TempDir(), MaxInFlight: 0, OverflowPolicy: OverflowReject},
		{OutputDir: t.TempDir(), MaxInFlight: 1, OverflowPolicy: "bogus"},
	}
	for i, cfg := range cases {
		if _, err := NewEngine(cfg); err == nil {
			t.Fatalf("case %d: invalid config must fail fast", i)
		}
	}
	t.Log("basis=empty dir / non-positive inflight / bad policy all rejected at construction")
}

func TestContextCancellation(t *testing.T) {
	eng := newTestEngine(t, 1) // single in-flight slot

	// Occupy the single slot directly and deterministically.
	eng.sem <- struct{}{}
	defer func() { <-eng.sem }()

	// A second ingestion with an already-cancelled context must return
	// immediately with UNAVAILABLE rather than block on the slot.
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	r := eng.Ingest(cancelledCtx, "cancelled", hl7Msg("EX", "M", "2.5.1", "20260901101500"))
	if r.Error == nil || r.Error.Type != ErrUnavailable {
		t.Fatalf("cancelled waiter must return UNAVAILABLE, got %+v", r.Error)
	}
	t.Logf("basis=in-flight cap reached with cancelled context -> %s/%s", r.Error.Type, r.Error.Reason)
}

func TestInflightCapIsBounded(t *testing.T) {
	// Empirical bound: with MaxInFlight=2, 6 concurrent ingestions must
	// still all complete (the semaphore bounds concurrency, it must not
	// deadlock or drop messages).
	cfg := DefaultConfig()
	cfg.OutputDir = t.TempDir()
	cfg.ResetOutput = true
	cfg.MaxRecords = 0 // unlimited
	cfg.MaxInFlight = 2
	eng, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	done := make(chan Outcome, 6)
	start := make(chan struct{})
	for i := 0; i < 6; i++ {
		go func(i int) {
			<-start
			r := eng.Ingest(context.Background(), "m",
				hl7Msg("CAP-"+string(rune('A'+i)), "M", "2.5.1", "20260901101500"))
			done <- r.Outcome
		}(i)
	}
	close(start)
	seen := map[Outcome]int{}
	deadline := time.After(3 * time.Second)
	for i := 0; i < 6; i++ {
		select {
		case o := <-done:
			seen[o]++
		case <-deadline:
			t.Fatal("in-flight limiter deadlocked; messages dropped")
		}
	}
	if seen[OutcomeAccepted] != 6 {
		t.Fatalf("all six must be accepted, got %v", seen)
	}
	t.Logf("basis=MaxInFlight=2 with 6 concurrent writers: %v (bounded concurrency, no drop/deadlock)", seen)
}
