package pipeline

import "errors"

// OverflowPolicy defines behavior once a configured limit is reached.
type OverflowPolicy string

const (
	// OverflowReject rejects further ingestion with STORAGE_CONFLICT.
	OverflowReject OverflowPolicy = "reject"
	// OverflowDegrade keeps accepting but moves accepted records to the
	// degraded journal instead of the primary store.
	OverflowDegrade OverflowPolicy = "degrade"
)

// Config bounds resource usage of a pipeline instance.
type Config struct {
	// OutputDir is the directory for records.jsonl and side journals.
	OutputDir string
	// MaxRecords caps accepted canonical records. 0 means unlimited.
	MaxRecords int
	// MaxInFlight caps concurrently processed messages. Must be > 0.
	MaxInFlight int
	// OverflowPolicy selects reject/degrade when MaxRecords is reached.
	OverflowPolicy OverflowPolicy
	// DeIDSalt is the HMAC salt (fixed value => reproducible pseudonyms).
	DeIDSalt string
	// ResetOutput clears prior output at Open. When false, the store
	// replays its journals, guaranteeing idempotence across restarts.
	ResetOutput bool
	// MaxPendingReviews caps conflicts awaiting review. 0 means
	// unlimited. Overflow follows OverflowPolicy (reject the delivery
	// or degrade it to the degraded journal); either way the backlog
	// stays queryable and no half-applied state is left behind.
	MaxPendingReviews int
	// MaxReplayTasks caps concurrently running consistency replays.
	// 0 means unlimited; excess requests are refused with UNAVAILABLE.
	MaxReplayTasks int
}

// DefaultConfig returns conservative local defaults.
func DefaultConfig() Config {
	return Config{
		OutputDir:         "output",
		MaxRecords:        10000,
		MaxInFlight:       32,
		OverflowPolicy:    OverflowReject,
		DeIDSalt:          "local-dev-salt-v1",
		ResetOutput:       false,
		MaxPendingReviews: 1000,
		MaxReplayTasks:    2,
	}
}

func (c Config) Validate() error {
	if c.OutputDir == "" {
		return errors.New("output dir is required")
	}
	if c.MaxInFlight <= 0 {
		return errors.New("max in-flight must be positive")
	}
	switch c.OverflowPolicy {
	case OverflowReject, OverflowDegrade:
	default:
		return errors.New("invalid overflow policy")
	}
	if c.MaxPendingReviews < 0 {
		return errors.New("max pending reviews must be >= 0")
	}
	if c.MaxReplayTasks < 0 {
		return errors.New("max replay tasks must be >= 0")
	}
	return nil
}
