package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// ConflictReason is a stable, machine-distinguishable conflict cause.
type ConflictReason string

const (
	// ReasonContentConflictSameRevision: two deliveries carry the same
	// revision coordinates (format-version ordinal + clinical effective
	// time) but different clinical content. Never resolved silently.
	ReasonContentConflictSameRevision ConflictReason = "CONTENT_CONFLICT_SAME_REVISION"
	// ReasonContentConflictCrossRevision: two deliveries of one event at
	// the same clinical effective time but different format/version
	// coordinates carry genuinely different clinical content (value,
	// status, code, ...). The newer/higher-priority delivery must NOT
	// silently overwrite the effective version; both orders park for
	// review. Distinct from ReasonContentConflictSameRevision so reviewers
	// can tell same-revision disagreements from cross-version ones.
	ReasonContentConflictCrossRevision ConflictReason = "CONTENT_CONFLICT_CROSS_REVISION"
	// ReasonPatientMismatch: a delivery in an existing event chain names
	// a different patient than the chain's current effective version
	// (patient identifier swapped for another person). The chain's
	// patient association must never be rewritten by such a delivery; it
	// is parked for review and the effective version (with its patient
	// link) is retained. Distinct from the content-conflict reasons.
	ReasonPatientMismatch ConflictReason = "PATIENT_MISMATCH"
)

// ReasonContentConflict is retained as an alias for the same-revision
// content-conflict reason (callers and older journals use the short
// name).
const ReasonContentConflict = ReasonContentConflictSameRevision

// ConflictStatus is the lifecycle state of a detected conflict.
type ConflictStatus string

const (
	ConflictPending  ConflictStatus = "PENDING"
	ConflictResolved ConflictStatus = "RESOLVED"
)

// ResolutionChoice is the deterministic adjudication outcome.
type ResolutionChoice string

const (
	// KeepCurrent retains the current effective version.
	KeepCurrent ResolutionChoice = "KEEP_CURRENT"
	// AcceptIncoming promotes the conflicting delivery to effective.
	AcceptIncoming ResolutionChoice = "ACCEPT_INCOMING"
)

// Resolution records one review conclusion. Re-applying the same
// conclusion is idempotent: no duplicate state, no duplicate records.
type Resolution struct {
	Choice       ResolutionChoice `json:"choice"`
	WinnerDigest string           `json:"winnerDigest"`
	Reviewer     string           `json:"reviewer,omitempty"`
	Seq          int64            `json:"seq"`
}

// Conflict is one detected inconsistency inside an event chain.
// Digests (not raw content) identify the disagreeing revisions, so the
// review flow never exposes sensitive values beyond the de-identified
// record view.
type Conflict struct {
	ConflictID     string         `json:"conflictId"`
	ChainID        string         `json:"chainId"`
	EventID        string         `json:"eventId"`
	Reason         ConflictReason `json:"reason"`
	CurrentDigest  string         `json:"currentDigest"`
	IncomingDigest string         `json:"incomingDigest"`
	// DifferingFields lists the disagreeing clinical fact fields for
	// content conflicts (value/status/code/...); empty for patient
	// mismatch (the disagreement is the patient itself).
	DifferingFields []string `json:"differingFields,omitempty"`
	// CurrentPatient / IncomingPatient carry de-identified pseudonyms
	// for PATIENT_MISMATCH conflicts, empty otherwise.
	CurrentPatient  string         `json:"currentPatient,omitempty"`
	IncomingPatient string         `json:"incomingPatient,omitempty"`
	Status          ConflictStatus `json:"status"`
	Resolution      *Resolution    `json:"resolution,omitempty"`
	DetectedSeq     int64          `json:"detectedSeq"`
}

// conflictIDForPair builds a deterministic, arrival-order-independent
// conflict identifier from the unordered pair of the two disagreeing
// revisions' business digests: the same two deliveries always yield the
// same id no matter which one arrived first (and is currently
// effective). Repeat detection therefore never creates duplicate
// conflict records.
func conflictIDForPair(eventID string, a, b *Record) string {
	digestA, digestB := businessDigest(a), businessDigest(b)
	if digestA > digestB {
		digestA, digestB = digestB, digestA
	}
	h := sha256.New()
	h.Write([]byte("conflict\x00" + eventID + "\x00" + digestA + "\x00" + digestB))
	return "CON-" + hex.EncodeToString(h.Sum(nil))[:hashLen]
}

// reviewEntry is one journaled review conclusion (reviews.jsonl). The
// review journal is the commit record for resolutions: every complete
// line is applied on replay.
type reviewEntry struct {
	ConflictID   string           `json:"conflictId"`
	ChainID      string           `json:"chainId"`
	EventID      string           `json:"eventId"`
	Choice       ResolutionChoice `json:"choice"`
	WinnerDigest string           `json:"winnerDigest"`
	Reviewer     string           `json:"reviewer,omitempty"`
	Seq          int64            `json:"seq"`
}

// ReviewResult is the outcome of applying (or idempotently re-applying)
// a review conclusion.
type ReviewResult struct {
	Conflict *Conflict
	Decision *Decision
	// Applied is false when the same conclusion already existed and was
	// simply returned without new state or journal records.
	Applied bool
}

// Resolve applies a review conclusion to a pending conflict. The
// reviews journal is the commit record: the conclusion is appended and
// fsynced before the in-memory chain is updated, and the informational
// decision follows, so a crash can never leave a half-applied review.
//
// Idempotence: re-applying the same conclusion to an already resolved
// conflict returns the existing resolution with Applied=false and
// writes nothing. A different conclusion for a resolved conflict is
// rejected with CONFLICT_ALREADY_RESOLVED.
func (s *fileStore) Resolve(_ context.Context, conflictID string, choice ResolutionChoice, reviewer string) (*ReviewResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, &IngestionError{Type: ErrUnavailable, Message: "store closed", Reason: "CLOSED"}
	}
	if choice != KeepCurrent && choice != AcceptIncoming {
		return nil, &IngestionError{
			Type: ErrValidationError, Message: "unknown resolution choice",
			Reason: "INVALID_RESOLUTION", EventID: conflictID,
			Detail: fmt.Sprintf("choice %q is not KEEP_CURRENT or ACCEPT_INCOMING", choice),
		}
	}
	c := s.model.conflicts[conflictID]
	if c == nil {
		return nil, &IngestionError{
			Type: ErrValidationError, Message: "unknown conflict",
			Reason: "UNKNOWN_CONFLICT", EventID: conflictID,
			Detail: fmt.Sprintf("no conflict with id %s", conflictID),
		}
	}
	if c.Status == ConflictResolved {
		if c.Resolution.Choice == choice {
			// Same conclusion re-applied: idempotent no-op.
			return &ReviewResult{Conflict: c, Applied: false}, nil
		}
		return nil, &IngestionError{
			Type: ErrStorageConflict, Message: "conflict already resolved",
			Reason:  "CONFLICT_ALREADY_RESOLVED",
			EventID: c.EventID,
			Detail: fmt.Sprintf("conflict %s resolved as %s at seq %d; cannot re-resolve as %s",
				conflictID, c.Resolution.Choice, c.Resolution.Seq, choice),
		}
	}

	winner := c.CurrentDigest
	if choice == AcceptIncoming {
		winner = c.IncomingDigest
	}
	s.seq++
	entry := &reviewEntry{
		ConflictID:   c.ConflictID,
		ChainID:      c.ChainID,
		EventID:      c.EventID,
		Choice:       choice,
		WinnerDigest: winner,
		Reviewer:     reviewer,
		Seq:          s.seq,
	}
	// Commit: review journal first (it is the source of truth for
	// resolutions), then the chain state flips atomically under mu.
	if err := s.appendJSON(s.rvwFile, entry); err != nil {
		return nil, err
	}
	s.model.applyReview(entry)

	d := &Decision{
		Seq: s.seq, EventID: c.EventID, Outcome: OutcomeReviewApplied,
		Reason:  fmt.Sprintf("review applied: %s -> winner digest %s", choice, shortDigest(winner)),
		ChainID: c.ChainID, ConflictID: c.ConflictID,
	}
	if err := s.appendDecision(d); err != nil {
		return nil, err
	}
	return &ReviewResult{Conflict: c, Decision: d, Applied: true}, nil
}
