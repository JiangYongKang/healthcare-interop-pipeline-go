package pipeline

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Outcome is the journal classification for an ingestion decision.
type Outcome string

const (
	OutcomeAccepted   Outcome = "ACCEPTED"
	OutcomeRejected   Outcome = "REJECTED"
	OutcomeDuplicate  Outcome = "DUPLICATE"
	OutcomeOutOfOrder Outcome = "OUT_OF_ORDER"
	OutcomeDegraded   Outcome = "DEGRADED"
	// OutcomeConflict: content genuinely inconsistent with the current
	// effective version; retained and parked in PENDING_REVIEW instead
	// of being silently applied.
	OutcomeConflict Outcome = "CONFLICT"
	// OutcomeReviewApplied: a review conclusion was applied to a chain.
	OutcomeReviewApplied Outcome = "REVIEW_APPLIED"
	// OutcomeReplayed: a consistency replay rebuilt state from journals.
	OutcomeReplayed Outcome = "REPLAYED"
)

// Decision is one traceable entry in the pipeline journals.
type Decision struct {
	Seq       int64   `json:"seq"`
	EventID   string  `json:"eventId"`
	Outcome   Outcome `json:"outcome"`
	Reason    string  `json:"reason,omitempty"`
	ErrorType string  `json:"errorType,omitempty"`
	Detail    string  `json:"detail,omitempty"`
	Format    string  `json:"format,omitempty"`
	Version   string  `json:"version,omitempty"`
	ReviewID  string  `json:"reviewId,omitempty"`
}

// Store is the concurrency-safe persistence boundary.
type Store interface {
	// Submit applies one fully de-identified record under the event key.
	// Duplicates, stale out-of-order versions and over-capacity records
	// return an IngestionError of type STORAGE_CONFLICT; the decision is
	// still journaled. Genuinely conflicting content is retained and
	// parked in PENDING_REVIEW, never silently applied.
	Submit(ctx context.Context, rec *Record) (*Decision, error)
	// Reject journals a rejected message.
	Reject(ctx context.Context, eventID string, et ErrorType, reason, detail, format, version string) *Decision
	// ApplyReview records and applies one review conclusion. Application
	// is idempotent by ReviewID.
	ApplyReview(ctx context.Context, req ReviewRequest) (*ReviewRecord, *Decision, error)
	// Chain returns the consistency view of one event chain.
	Chain(ctx context.Context, eventID string) (*ChainView, error)
	// Chains returns consistency views of all known event chains.
	Chains(ctx context.Context) ([]*ChainView, error)
	// ReplayConsistency rebuilds all in-memory state from the journals,
	// verifies it against the incrementally maintained state, atomically
	// replaces it and republishes derived output.
	ReplayConsistency(ctx context.Context) (*ReplayReport, error)
	// Note journals a system decision (e.g. replay backlog overflow).
	Note(ctx context.Context, eventID string, outcome Outcome, reason string) *Decision
	// Snapshot returns records ordered by (eventTime, eventId).
	Snapshot(ctx context.Context) ([]*Record, error)
	// Decisions returns the append-only decision journal.
	Decisions(ctx context.Context) ([]*Decision, error)
	// Close fsyncs and releases all file handles.
	Close() error
}

const (
	recordsFile   = "records.jsonl"
	revisionsFile = "revisions.jsonl"
	decisionsFile = "decisions.jsonl"
	degradedFile  = "degraded.jsonl"
	reviewsFile   = "reviews.jsonl"
)

// chainState is the in-memory consistency state of one event chain.
type chainState struct {
	deliveries []*Delivery
	records    map[int64]*Record // delivery seq -> retained record
}

// fileStore persists to JSONL files under a directory. All mutating
// decisions are serialized by mu, so de-identified data, idempotence
// and persistence cannot observe half-applied state.
type fileStore struct {
	dir string
	cfg Config
	mu  sync.Mutex

	revFile *os.File
	decFile *os.File
	degFile *os.File
	rvwFile *os.File

	seq        int64 // monotonic arrival sequence (journal only)
	rebuildSeq int64 // synthetic seq base for journal-rebuild deliveries
	byEvent    map[string]*Record
	chains     map[string]*chainState
	pending    map[string]bool          // eventID currently PENDING_REVIEW
	reviews    map[string]*ReviewRecord // reviewID -> applied conclusion
	digests    map[string]map[string]bool
	accepted   int
	closed     bool
}

func newFileStore(cfg Config) (Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return nil, &IngestionError{Type: ErrUnavailable, Message: "cannot create output directory", Reason: "IO_ERROR", Detail: err.Error()}
	}
	s := &fileStore{
		dir:     cfg.OutputDir,
		cfg:     cfg,
		byEvent: map[string]*Record{},
		chains:  map[string]*chainState{},
		pending: map[string]bool{},
		reviews: map[string]*ReviewRecord{},
		digests: map[string]map[string]bool{},
	}
	if cfg.ResetOutput {
		for _, f := range []string{recordsFile, revisionsFile, decisionsFile, degradedFile, reviewsFile} {
			if err := os.Remove(filepath.Join(cfg.OutputDir, f)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
		}
	}
	if err := s.openAndReplay(); err != nil {
		_ = s.closeHandles()
		return nil, err
	}
	return s, nil
}

func (s *fileStore) openAndReplay() error {
	var err error
	// Open all files first; if any open fails no file was truncated.
	if s.revFile, err = os.OpenFile(filepath.Join(s.dir, revisionsFile), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644); err != nil {
		return ioErr(err)
	}
	if s.decFile, err = os.OpenFile(filepath.Join(s.dir, decisionsFile), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644); err != nil {
		return ioErr(err)
	}
	if s.degFile, err = os.OpenFile(filepath.Join(s.dir, degradedFile), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644); err != nil {
		return ioErr(err)
	}
	if s.rvwFile, err = os.OpenFile(filepath.Join(s.dir, reviewsFile), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644); err != nil {
		return ioErr(err)
	}
	if err := s.replay(); err != nil {
		return err
	}
	// Derived output is (re)published from replayed state, so a crash
	// between journal commit and derived rewrite cannot leave a stale
	// records.jsonl behind.
	return s.publishDerivedLocked()
}

func ioErr(err error) error {
	return &IngestionError{Type: ErrUnavailable, Message: "persistence failure", Reason: "IO_ERROR", Detail: err.Error()}
}

type revEntry struct {
	rec    *Record
	ok     bool
	endOff int64
}

// replay rebuilds in-memory indexes from the append-only journals and
// heals any torn trailing line left by a killed process. Journals are
// healed independently; in-memory state reflects only prefixes that are
// complete on every journal.
func (s *fileStore) replay() error {
	var revs []revEntry
	var revOff int64
	if err := readJSONLLines(s.revFile, func(line string) {
		var r Record
		revOff += int64(len(line) + 1)
		if json.Unmarshal([]byte(line), &r) == nil && r.EventID != "" {
			revs = append(revs, revEntry{&r, true, revOff})
		} else {
			revs = append(revs, revEntry{nil, false, revOff})
		}
	}); err != nil {
		return err
	}
	var decisions []*Decision
	if err := readJSONLLines(s.decFile, func(line string) {
		var d Decision
		if json.Unmarshal([]byte(line), &d) == nil && d.EventID != "" {
			decisions = append(decisions, &d)
		} else {
			decisions = append(decisions, nil)
		}
	}); err != nil {
		return err
	}
	var reviews []*ReviewRecord
	var rvwOff int64
	if err := readJSONLLines(s.rvwFile, func(line string) {
		var r ReviewRecord
		rvwOff += int64(len(line) + 1)
		if json.Unmarshal([]byte(line), &r) == nil && r.ReviewID != "" {
			reviews = append(reviews, &r)
		} else {
			reviews = append(reviews, nil)
		}
	}); err != nil {
		return err
	}

	// First torn/invalid line terminates replay for that journal.
	goodRevs := 0
	for _, rl := range revs {
		if !rl.ok {
			break
		}
		goodRevs++
	}
	goodDecs := len(decisions)
	for i, d := range decisions {
		if d == nil {
			goodDecs = i
			break
		}
	}
	goodRvws := len(reviews)
	for i, r := range reviews {
		if r == nil {
			goodRvws = i
			break
		}
	}

	// Decisions mark which revisions were committed. A revision appended
	// just before a crash (with no decision) is an orphan and is removed
	// by truncation, so no partial state can survive a failed write.
	// Committed decisions are queued per event so each retained revision
	// can be paired with the arrival seq of its commit decision; this
	// keeps delivery seqs (and thus review targets) stable across
	// rebuilds.
	committed := map[string]bool{}
	committedReviews := map[string]bool{}
	decQueue := map[string][]int64{}
	for _, d := range decisions[:goodDecs] {
		if s.seq < d.Seq {
			s.seq = d.Seq
		}
		switch d.Outcome {
		case OutcomeAccepted, OutcomeOutOfOrder, OutcomeConflict:
			committed[d.EventID] = true
			decQueue[d.EventID] = append(decQueue[d.EventID], d.Seq)
		case OutcomeReviewApplied:
			committedReviews[d.ReviewID] = true
		}
	}
	var lastCommittedOff int64
	var committedRevs []*Record
	var committedSeqs []int64
	for i := 0; i < goodRevs; i++ {
		r := revs[i].rec
		if !committed[r.EventID] {
			continue
		}
		committedRevs = append(committedRevs, r)
		q := decQueue[r.EventID]
		if len(q) > 0 {
			committedSeqs = append(committedSeqs, q[0])
			decQueue[r.EventID] = q[1:]
		} else {
			committedSeqs = append(committedSeqs, 0)
		}
		lastCommittedOff = revs[i].endOff
	}

	// Rebuild chain state by re-applying the deterministic adjudication
	// in arrival order (capacity checks are not re-run: a retained
	// revision stays retained). Incremental processing and this rebuild
	// observe the same rules, so both reach the same effective state.
	for i, r := range committedRevs {
		s.applyRetainedLocked(r, committedSeqs[i])
	}
	// Committed review conclusions replay on top, in journal order.
	var lastCommittedRvwOff int64
	{
		off := int64(0)
		for i := 0; i < goodRvws; i++ {
			b, _ := json.Marshal(reviews[i])
			off += int64(len(b)) + 1
			r := reviews[i]
			if !committedReviews[r.ReviewID] {
				continue
			}
			s.applyReviewStateLocked(r)
			s.reviews[r.ReviewID] = r
			lastCommittedRvwOff = off
		}
	}
	s.accepted = len(s.byEvent)

	// Heal torn trailing lines and crash-orphan entries by truncating
	// all journals back to their last committed prefixes.
	if goodRevs < len(revs) || lastCommittedOff < revOff {
		if err := truncateFile(s.revFile, lastCommittedOff); err != nil {
			return err
		}
	}
	if goodDecs < len(decisions) {
		var n int64
		for i := 0; i < goodDecs; i++ {
			b, _ := json.Marshal(decisions[i])
			n += int64(len(b)) + 1
		}
		if err := truncateFile(s.decFile, n); err != nil {
			return err
		}
	}
	if goodRvws < len(reviews) || lastCommittedRvwOff < rvwOff {
		if err := truncateFile(s.rvwFile, lastCommittedRvwOff); err != nil {
			return err
		}
	}
	return nil
}

func truncateFile(f *os.File, length int64) error {
	if err := f.Truncate(length); err != nil {
		return ioErr(err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return ioErr(err)
	}
	return nil
}

// applyRetainedLocked folds one retained (committed) revision into
// chain state using the same deterministic adjudication as live
// ingestion. seq is the arrival seq of its commit decision (0 when
// unknown, in which case a synthetic placeholder is assigned). Used by
// journal replay; no capacity checks, no journaling.
func (s *fileStore) applyRetainedLocked(rec *Record, seq int64) *chainState {
	cs := s.chains[rec.EventID]
	if cs == nil {
		cs = &chainState{records: map[int64]*Record{}}
		s.chains[rec.EventID] = cs
	}
	digest := businessDigest(rec)
	if s.digests[rec.EventID] == nil {
		s.digests[rec.EventID] = map[string]bool{}
	}
	s.digests[rec.EventID][digest] = true
	if seq <= 0 {
		seq = s.nextRebuildSeq()
	}

	dlv := &Delivery{
		Seq: seq, EventID: rec.EventID, Digest: digest,
		Format: rec.SourceFormat, Version: rec.SourceVersion,
		EffectiveTime: effectiveOf(rec),
	}
	cur := s.byEvent[rec.EventID]
	if cur == nil {
		dlv.Outcome, dlv.Relation = OutcomeAccepted, RelationCurrent
		s.byEvent[rec.EventID] = rec
	} else {
		switch v, _ := adjudicate(rec, cur); v {
		case verdictSupersede:
			dlv.Outcome, dlv.Relation = OutcomeAccepted, RelationCurrent
			s.markCurrentLocked(rec.EventID, RelationSuperseded)
			s.byEvent[rec.EventID] = rec
		case verdictStale:
			dlv.Outcome, dlv.Relation = OutcomeOutOfOrder, RelationStale
		default:
			dlv.Outcome, dlv.Relation = OutcomeConflict, RelationConflictPending
			s.pending[rec.EventID] = true
		}
	}
	cs.deliveries = append(cs.deliveries, dlv)
	cs.records[dlv.Seq] = rec
	return cs
}

// nextRebuildSeq assigns synthetic seq numbers during rebuild. Live seq
// numbers come from the decision journal; rebuild only needs stable,
// increasing placeholders in a range that cannot collide with them.
func (s *fileStore) nextRebuildSeq() int64 {
	if s.rebuildSeq == 0 {
		s.rebuildSeq = 1 << 40
	}
	s.rebuildSeq++
	return s.rebuildSeq
}

// markCurrentLocked re-labels the relation of the delivery currently
// marked CURRENT/REVIEW_PINNED.
func (s *fileStore) markCurrentLocked(eventID, relation string) {
	cs := s.chains[eventID]
	if cs == nil {
		return
	}
	for _, d := range cs.deliveries {
		if d.Relation == RelationCurrent || d.Relation == RelationReviewPinned {
			d.Relation = relation
		}
	}
}

// effectiveOf prefers the clinical effective time.
func effectiveOf(r *Record) string {
	if r.EffectiveTime != "" {
		return r.EffectiveTime
	}
	return r.EventTime
}

// businessDigest hashes all business fields except arrival metadata, so
// exact re-delivery of the same event is recognizable regardless of
// re-receipt sequence.
func businessDigest(r *Record) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s",
		r.EventID, r.EventType, r.PatientRef, r.SourceFormat, r.SourceVersion,
		r.Status, r.EventTime, r.Code, r.CodeSystem, r.CodeDisplay, r.Value, r.Unit)
	for _, k := range sortedKeys(r.UnknownFields) {
		fmt.Fprintf(h, "\x00%s=%s", k, r.UnknownFields[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func sortedKeys(m map[string]string) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func (s *fileStore) Submit(_ context.Context, rec *Record) (*Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, &IngestionError{Type: ErrUnavailable, Message: "store closed", Reason: "CLOSED"}
	}
	s.seq++
	cur, exists := s.byEvent[rec.EventID]
	d := &Decision{Seq: s.seq, EventID: rec.EventID, Format: rec.SourceFormat, Version: rec.SourceVersion}
	digest := businessDigest(rec)

	// Exact redelivery of any previously persisted revision is a
	// duplicate regardless of which revision currently wins.
	if s.digests[rec.EventID][digest] {
		d.Outcome = OutcomeDuplicate
		d.Reason = "identical redelivery; no new record created"
		if err := s.appendDecision(d); err != nil {
			return nil, err
		}
		return d, conflictErr(d, "duplicate event suppressed")
	}

	// Capacity applies only when this is the first revision of a new
	// event; repeated/out-of-order revisions of known events never
	// displace the configured limit.
	if !exists && s.cfg.MaxRecords > 0 && s.accepted >= s.cfg.MaxRecords {
		switch s.cfg.OverflowPolicy {
		case OverflowReject:
			d.Outcome = OutcomeRejected
			d.Reason = fmt.Sprintf("capacity limit %d reached; reject policy", s.cfg.MaxRecords)
			d.ErrorType = string(ErrStorageConflict)
			if err := s.appendDecision(d); err != nil {
				return nil, err
			}
			return d, conflictErr(d, "capacity limit reached")
		case OverflowDegrade:
			d.Outcome = OutcomeDegraded
			d.Reason = fmt.Sprintf("capacity limit %d reached; degraded journal", s.cfg.MaxRecords)
			if err := s.appendJSON(s.degFile, rec); err != nil {
				return nil, err
			}
			if err := s.appendDecision(d); err != nil {
				return nil, err
			}
			return d, nil
		}
	}

	// Deterministic adjudication against the current effective version.
	// The verdict depends only on record content, never on arrival
	// order or source format.
	outcome := OutcomeAccepted
	winner := rec
	relation := RelationCurrent
	if exists {
		switch v, reason := adjudicate(rec, cur); v {
		case verdictSupersede:
			d.Reason = fmt.Sprintf("newer revision supersedes prior (time=%s status=%s)",
				effectiveOf(cur), cur.Status)
		case verdictStale:
			outcome = OutcomeOutOfOrder
			winner = cur
			relation = RelationStale
			d.Reason = fmt.Sprintf("stale revision (winner time=%s status=%s vs incoming time=%s status=%s); newer data retained",
				effectiveOf(cur), cur.Status, effectiveOf(rec), rec.Status)
		default: // verdictConflict
			// Pending-review backlog is bounded; overflow follows the
			// configured policy and stays queryable via the journal.
			if !s.pending[rec.EventID] && s.cfg.MaxPendingReviews > 0 && len(s.pending) >= s.cfg.MaxPendingReviews {
				return s.overflowPendingLocked(rec, d)
			}
			outcome = OutcomeConflict
			winner = cur
			relation = RelationConflictPending
			s.pending[rec.EventID] = true
			d.Reason = fmt.Sprintf("%s; retained for review, current effective version preserved", reason)
		}
	}

	if err := s.appendJSON(s.revFile, rec); err != nil {
		return nil, err
	}
	cs := s.chains[rec.EventID]
	if cs == nil {
		cs = &chainState{records: map[int64]*Record{}}
		s.chains[rec.EventID] = cs
	}
	if s.digests[rec.EventID] == nil {
		s.digests[rec.EventID] = map[string]bool{}
	}
	s.digests[rec.EventID][digest] = true
	if outcome == OutcomeAccepted && exists {
		s.markCurrentLocked(rec.EventID, RelationSuperseded)
	}
	cs.deliveries = append(cs.deliveries, &Delivery{
		Seq: s.seq, EventID: rec.EventID, Digest: digest,
		Format: rec.SourceFormat, Version: rec.SourceVersion,
		EffectiveTime: effectiveOf(rec), Outcome: outcome,
		Relation: relation, Reason: d.Reason,
	})
	cs.records[s.seq] = rec
	s.byEvent[rec.EventID] = winner
	if !exists {
		s.accepted++
	}
	d.Outcome = outcome
	if err := s.appendDecision(d); err != nil {
		return nil, err
	}
	if winner == rec {
		// Effective version changed: republish derived output atomically
		// in the same critical section as the journal commit.
		if err := s.publishDerivedLocked(); err != nil {
			return nil, err
		}
	}
	switch outcome {
	case OutcomeOutOfOrder:
		return d, conflictErr(d, "out-of-order message retained as stale revision")
	case OutcomeConflict:
		return d, conflictErr(d, "conflicting content retained for review")
	}
	return d, nil
}

// overflowPendingLocked handles a conflicting delivery when the
// pending-review backlog is full, per the configured overflow policy.
func (s *fileStore) overflowPendingLocked(rec *Record, d *Decision) (*Decision, error) {
	switch s.cfg.OverflowPolicy {
	case OverflowDegrade:
		d.Outcome = OutcomeDegraded
		d.Reason = fmt.Sprintf("pending-review backlog %d reached; conflicting delivery degraded", s.cfg.MaxPendingReviews)
		if err := s.appendJSON(s.degFile, rec); err != nil {
			return nil, err
		}
		if err := s.appendDecision(d); err != nil {
			return nil, err
		}
		return d, nil
	default:
		d.Outcome = OutcomeRejected
		d.Reason = fmt.Sprintf("pending-review backlog %d reached; reject policy", s.cfg.MaxPendingReviews)
		d.ErrorType = string(ErrStorageConflict)
		if err := s.appendDecision(d); err != nil {
			return nil, err
		}
		return d, conflictErr(d, "pending-review backlog full")
	}
}

func conflictErr(d *Decision, msg string) error {
	return &IngestionError{
		Type: ErrStorageConflict, Message: msg, Reason: string(d.Outcome),
		EventID: d.EventID, Detail: d.Reason,
	}
}

// ApplyReview journals and applies one review conclusion atomically:
// review entry first, decision as commit marker, state change and
// derived republish in the same critical section. Re-applying the same
// ReviewID with the same conclusion is a no-op that reports the
// original record; the same ReviewID with a different conclusion is
// rejected as a conflict.
func (s *fileStore) ApplyReview(_ context.Context, req ReviewRequest) (*ReviewRecord, *Decision, error) {
	if err := req.Validate(); err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil, &IngestionError{Type: ErrUnavailable, Message: "store closed", Reason: "CLOSED"}
	}
	if prev, ok := s.reviews[req.ReviewID]; ok {
		if prev.EventID == req.EventID && prev.Action == req.Action && prev.TargetSeq == req.TargetSeq {
			dup := *prev
			dup.Duplicate = true
			return &dup, nil, nil
		}
		return nil, nil, &IngestionError{
			Type: ErrStorageConflict, Message: "review id already used with a different conclusion",
			Reason: "REVIEW_ID_CONFLICT", EventID: req.EventID,
		}
	}
	cs := s.chains[req.EventID]
	if cs == nil {
		return nil, nil, &IngestionError{
			Type: ErrValidationError, Message: "no such event chain",
			Reason: "UNKNOWN_EVENT", EventID: req.EventID,
		}
	}
	s.seq++
	rec := &ReviewRecord{
		ReviewID: req.ReviewID, EventID: req.EventID, Action: req.Action,
		TargetSeq: req.TargetSeq, Reason: req.Reason, Reviewer: req.Reviewer,
		AppliedSeq: s.seq,
	}
	var target *Record
	if req.Action == ReviewUseDelivery {
		target = cs.records[req.TargetSeq]
		if target == nil {
			return nil, nil, &IngestionError{
				Type: ErrValidationError, Message: "target delivery not retained in chain",
				Reason: "UNKNOWN_DELIVERY", EventID: req.EventID,
			}
		}
	} else {
		target = s.byEvent[req.EventID]
	}
	if err := s.appendJSON(s.rvwFile, rec); err != nil {
		return nil, nil, err
	}
	s.applyReviewStateLocked(rec)
	s.reviews[rec.ReviewID] = rec
	s.byEvent[req.EventID] = target
	delete(s.pending, req.EventID)
	d := &Decision{
		Seq: s.seq, EventID: req.EventID, Outcome: OutcomeReviewApplied,
		Reason:   fmt.Sprintf("review %s applied: %s", req.ReviewID, req.Action),
		ReviewID: req.ReviewID,
	}
	if err := s.appendDecision(d); err != nil {
		return nil, nil, err
	}
	if err := s.publishDerivedLocked(); err != nil {
		return nil, nil, err
	}
	return rec, d, nil
}

// applyReviewStateLocked updates chain relations for a committed
// review. Shared by live application and journal replay.
func (s *fileStore) applyReviewStateLocked(r *ReviewRecord) {
	cs := s.chains[r.EventID]
	if cs == nil {
		return
	}
	s.markCurrentLocked(r.EventID, RelationSuperseded)
	for _, dlv := range cs.deliveries {
		switch r.Action {
		case ReviewUseDelivery:
			if dlv.Seq == r.TargetSeq {
				dlv.Relation = RelationReviewPinned
			}
		case ReviewKeepCurrent:
			if cur := s.byEvent[r.EventID]; cur != nil && dlv.Digest == businessDigest(cur) {
				dlv.Relation = RelationReviewPinned
			}
		}
	}
	if r.Action == ReviewUseDelivery {
		if target := cs.records[r.TargetSeq]; target != nil {
			s.byEvent[r.EventID] = target
		}
	}
	delete(s.pending, r.EventID)
}

// Chain returns the de-identified consistency view of one event chain.
func (s *fileStore) Chain(_ context.Context, eventID string) (*ChainView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cs := s.chains[eventID]
	if cs == nil {
		return nil, &IngestionError{Type: ErrValidationError, Message: "no such event chain", Reason: "UNKNOWN_EVENT", EventID: eventID}
	}
	return s.chainViewLocked(eventID, cs), nil
}

// Chains returns consistency views of all chains, ordered by event id.
func (s *fileStore) Chains(_ context.Context) ([]*ChainView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.chains))
	for id := range s.chains {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*ChainView, 0, len(ids))
	for _, id := range ids {
		out = append(out, s.chainViewLocked(id, s.chains[id]))
	}
	return out, nil
}

func (s *fileStore) chainViewLocked(eventID string, cs *chainState) *ChainView {
	v := &ChainView{EventID: eventID, Status: ChainEffective}
	if s.pending[eventID] {
		v.Status = ChainPendingReview
	}
	if cur := s.byEvent[eventID]; cur != nil {
		v.Current = cloneRecord(cur)
		v.CurrentDigest = businessDigest(cur)
		v.PatientRef = cur.PatientRef
	}
	v.Deliveries = make([]*Delivery, len(cs.deliveries))
	for i, d := range cs.deliveries {
		c := *d
		v.Deliveries[i] = &c
	}
	for _, r := range s.reviews {
		if r.EventID == eventID {
			c := *r
			v.Reviews = append(v.Reviews, &c)
		}
	}
	sort.Slice(v.Reviews, func(i, j int) bool { return v.Reviews[i].AppliedSeq < v.Reviews[j].AppliedSeq })
	return v
}

// ReplayConsistency rebuilds all chain state from the journals and
// verifies it equals the incrementally maintained state. The rebuilt
// state atomically replaces the live one and derived output is
// republished, so a replay can never leave half-applied state behind.
func (s *fileStore) ReplayConsistency(_ context.Context) (*ReplayReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, &IngestionError{Type: ErrUnavailable, Message: "store closed", Reason: "CLOSED"}
	}
	fresh := &fileStore{
		dir: s.dir, cfg: s.cfg,
		revFile: s.revFile, decFile: s.decFile, degFile: s.degFile, rvwFile: s.rvwFile,
		byEvent: map[string]*Record{}, chains: map[string]*chainState{},
		pending: map[string]bool{}, reviews: map[string]*ReviewRecord{},
		digests: map[string]map[string]bool{},
	}
	if err := fresh.replay(); err != nil {
		return nil, err
	}
	rep := &ReplayReport{
		Events: len(fresh.byEvent), Reviews: len(fresh.reviews),
		Pending: len(fresh.pending),
	}
	for _, cs := range fresh.chains {
		rep.Deliveries += len(cs.deliveries)
	}
	rep.Mismatches = s.diffStateLocked(fresh)
	// Atomic replacement of in-memory state under the same lock.
	s.byEvent, s.chains, s.pending, s.reviews, s.digests =
		fresh.byEvent, fresh.chains, fresh.pending, fresh.reviews, fresh.digests
	s.accepted = len(s.byEvent)
	if err := s.publishDerivedLocked(); err != nil {
		return nil, err
	}
	rep.Replaced = true
	s.seq++
	d := &Decision{
		Seq: s.seq, EventID: "_replay", Outcome: OutcomeReplayed,
		Reason: fmt.Sprintf("consistency replay: events=%d deliveries=%d pending=%d reviews=%d mismatches=%d",
			rep.Events, rep.Deliveries, rep.Pending, rep.Reviews, rep.Mismatches),
	}
	if err := s.appendDecision(d); err != nil {
		return nil, err
	}
	return rep, nil
}

// diffStateLocked counts divergences between live and rebuilt state.
func (s *fileStore) diffStateLocked(fresh *fileStore) int {
	mism := 0
	for id, rec := range fresh.byEvent {
		cur, ok := s.byEvent[id]
		if !ok || businessDigest(cur) != businessDigest(rec) {
			mism++
		}
	}
	for id := range s.byEvent {
		if _, ok := fresh.byEvent[id]; !ok {
			mism++
		}
	}
	for id := range fresh.pending {
		if !s.pending[id] {
			mism++
		}
	}
	for id := range s.pending {
		if !fresh.pending[id] {
			mism++
		}
	}
	return mism
}

// publishDerivedLocked atomically rewrites records.jsonl (tmp+rename)
// from the current effective versions. Called only with mu held, right
// after the journal commit that changed the effective set.
func (s *fileStore) publishDerivedLocked() error {
	recs := make([]*Record, 0, len(s.byEvent))
	for _, r := range s.byEvent {
		recs = append(recs, r)
	}
	sort.Slice(recs, func(i, j int) bool {
		if recs[i].EventTime != recs[j].EventTime {
			return recs[i].EventTime < recs[j].EventTime
		}
		return recs[i].EventID < recs[j].EventID
	})
	tmp := filepath.Join(s.dir, recordsFile+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return ioErr(err)
	}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	for _, r := range recs {
		if err := enc.Encode(r); err != nil {
			f.Close()
			return ioErr(err)
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return ioErr(err)
	}
	if err := f.Close(); err != nil {
		return ioErr(err)
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, recordsFile)); err != nil {
		return ioErr(err)
	}
	return nil
}

func (s *fileStore) Reject(_ context.Context, eventID string, et ErrorType, reason, detail, format, version string) *Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	d := &Decision{
		Seq: s.seq, EventID: eventID, Outcome: OutcomeRejected,
		Reason: reason, ErrorType: string(et), Detail: detail,
		Format: format, Version: version,
	}
	_ = s.appendDecision(d)
	return d
}

// Note journals a system decision that is not tied to one message
// (e.g. replay backlog overflow), keeping such events queryable.
func (s *fileStore) Note(_ context.Context, eventID string, outcome Outcome, reason string) *Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	d := &Decision{Seq: s.seq, EventID: eventID, Outcome: outcome, Reason: reason}
	_ = s.appendDecision(d)
	return d
}

func (s *fileStore) appendJSON(f *os.File, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return &IngestionError{Type: ErrUnavailable, Message: "encode failure", Reason: "ENCODE_ERROR", Detail: err.Error()}
	}
	b = append(b, '\n')
	if _, err := f.Write(b); err != nil {
		return ioErr(err)
	}
	if err := f.Sync(); err != nil {
		return ioErr(err)
	}
	return nil
}

func (s *fileStore) appendDecision(d *Decision) error {
	return s.appendJSON(s.decFile, d)
}

func (s *fileStore) Snapshot(_ context.Context) ([]*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Record, 0, len(s.byEvent))
	for _, r := range s.byEvent {
		out = append(out, cloneRecord(r))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EventTime != out[j].EventTime {
			return out[i].EventTime < out[j].EventTime
		}
		return out[i].EventID < out[j].EventID
	})
	return out, nil
}

func cloneRecord(r *Record) *Record {
	c := *r
	if r.UnknownFields != nil {
		c.UnknownFields = make(map[string]string, len(r.UnknownFields))
		for k, v := range r.UnknownFields {
			c.UnknownFields[k] = v
		}
	}
	return &c
}

func (s *fileStore) Decisions(_ context.Context) ([]*Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Decision, 0)
	// Read current file content (includes everything appended so far).
	if _, err := s.decFile.Seek(0, io.SeekStart); err != nil {
		return nil, ioErr(err)
	}
	sc := bufio.NewScanner(s.decFile)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var d Decision
		if err := json.Unmarshal([]byte(line), &d); err != nil {
			return nil, ioErr(err)
		}
		out = append(out, &d)
	}
	_, _ = s.decFile.Seek(0, io.SeekEnd)
	return out, sc.Err()
}

func (s *fileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.closeHandles()
}

func (s *fileStore) closeHandles() error {
	var errs []string
	for _, f := range []*os.File{s.revFile, s.decFile, s.degFile, s.rvwFile} {
		if f == nil {
			continue
		}
		if err := f.Sync(); err != nil {
			errs = append(errs, err.Error())
		}
		if err := f.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return ioErr(errors.New(strings.Join(errs, "; ")))
	}
	return nil
}

// readJSONLLines reads whole lines from an O_APPEND file from offset 0,
// restoring the append offset afterwards.
func readJSONLLines(f *os.File, emit func(string)) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return ioErr(err)
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			emit(line)
		}
	}
	_, _ = f.Seek(0, io.SeekEnd)
	return sc.Err()
}
