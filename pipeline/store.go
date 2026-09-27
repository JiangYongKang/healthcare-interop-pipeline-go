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
	// OutcomeConflict: the delivery conflicts with the current effective
	// version on identical revision coordinates; it is retained for
	// audit and parked for review. The effective version is unchanged.
	OutcomeConflict Outcome = "CONFLICT"
	// OutcomeReviewApplied: a review conclusion was applied to a chain.
	OutcomeReviewApplied Outcome = "REVIEW_APPLIED"
)

// Decision is one traceable entry in the pipeline journals.
type Decision struct {
	Seq        int64   `json:"seq"`
	EventID    string  `json:"eventId"`
	Outcome    Outcome `json:"outcome"`
	Reason     string  `json:"reason,omitempty"`
	ErrorType  string  `json:"errorType,omitempty"`
	Detail     string  `json:"detail,omitempty"`
	Format     string  `json:"format,omitempty"`
	Version    string  `json:"version,omitempty"`
	ChainID    string  `json:"chainId,omitempty"`
	ConflictID string  `json:"conflictId,omitempty"`
}

// Store is the concurrency-safe persistence boundary.
type Store interface {
	// Submit applies one fully de-identified record under the event key.
	// Duplicates and stale out-of-order versions return an
	// IngestionError of type STORAGE_CONFLICT; the decision is still
	// journaled. Overflow follows the configured policy.
	Submit(ctx context.Context, rec *Record) (*Decision, error)
	// Reject journals a rejected message.
	Reject(ctx context.Context, eventID string, et ErrorType, reason, detail, format, version string) *Decision
	// Snapshot returns records ordered by (eventTime, eventId).
	Snapshot(ctx context.Context) ([]*Record, error)
	// Decisions returns the append-only decision journal.
	Decisions(ctx context.Context) ([]*Decision, error)
	// Chains returns the event consistency view: one chain per clinical
	// event with all deliveries and the current effective version.
	Chains(ctx context.Context) ([]*ChainView, error)
	// PendingConflicts lists conflicts awaiting review.
	PendingConflicts(ctx context.Context) ([]*Conflict, error)
	// Resolve applies a review conclusion to a conflict. Applying the
	// same conclusion repeatedly is idempotent.
	Resolve(ctx context.Context, conflictID string, choice ResolutionChoice, reviewer string) (*ReviewResult, error)
	// Replay rebuilds chain state from the journals and verifies it
	// matches the live state, proving history reprocessing equals
	// incremental processing.
	Replay(ctx context.Context) (*ReplayReport, error)
	// Close fsyncs and releases all file handles.
	Close() error
}

const (
	revisionsFile = "revisions.jsonl"
	decisionsFile = "decisions.jsonl"
	degradedFile  = "degraded.jsonl"
	conflictsFile = "conflicts.jsonl"
	reviewsFile   = "reviews.jsonl"
	replayFile    = "replay.jsonl"
)

// journalFiles lists every append-only journal, for reset and close.
var journalFiles = []string{revisionsFile, decisionsFile, degradedFile, conflictsFile, reviewsFile, replayFile}

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
	conFile *os.File
	rvwFile *os.File
	rplFile *os.File

	seq       int64 // monotonic arrival sequence (journals only)
	model     *chainModel
	replaySem chan struct{}
	closed    bool
}

func newFileStore(cfg Config) (Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return nil, &IngestionError{Type: ErrUnavailable, Message: "cannot create output directory", Reason: "IO_ERROR", Detail: err.Error()}
	}
	s := &fileStore{
		dir:   cfg.OutputDir,
		cfg:   cfg,
		model: newChainModel(),
	}
	if cfg.MaxReplayTasks > 0 {
		s.replaySem = make(chan struct{}, cfg.MaxReplayTasks)
	}
	if cfg.ResetOutput {
		for _, f := range journalFiles {
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
	handles := []struct {
		name string
		dst  **os.File
	}{
		{revisionsFile, &s.revFile},
		{decisionsFile, &s.decFile},
		{degradedFile, &s.degFile},
		{conflictsFile, &s.conFile},
		{reviewsFile, &s.rvwFile},
		{replayFile, &s.rplFile},
	}
	for _, h := range handles {
		if *h.dst, err = os.OpenFile(filepath.Join(s.dir, h.name), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644); err != nil {
			return ioErr(err)
		}
	}
	if err := s.replay(); err != nil {
		return err
	}
	return nil
}

func ioErr(err error) error {
	return &IngestionError{Type: ErrUnavailable, Message: "persistence failure", Reason: "IO_ERROR", Detail: err.Error()}
}

// committingOutcome reports whether a decision outcome commits one
// appended revision (as opposed to duplicate/reject/degrade decisions,
// which append no revision, and review decisions, which are
// informational: the reviews journal is their commit record).
func committingOutcome(o Outcome) bool {
	return o == OutcomeAccepted || o == OutcomeOutOfOrder || o == OutcomeConflict
}

// replayAction is one ordered history event: a committed revision or a
// committed review conclusion.
type replayAction struct {
	seq int64
	rev *Record
	dec *Decision // paired committing decision, when rev != nil
	rw  *reviewEntry
}

// replay rebuilds the in-memory chain model from the append-only
// journals and heals torn or crash-orphan tails. Revisions are paired
// with their committing decisions per event in order; reviews are
// merged by sequence so the reprocessed history interleaves exactly as
// it happened live, yielding the same effective state.
func (s *fileStore) replay() error {
	j, err := s.readJournals()
	if err != nil {
		return err
	}

	// Rebuild the model from ordered history: committed revisions paired
	// with their decisions, reviews merged by sequence.
	for _, d := range j.decisions {
		if d.Seq > s.seq {
			s.seq = d.Seq
		}
	}
	for _, rw := range j.reviews {
		if rw.Seq > s.seq {
			s.seq = rw.Seq
		}
	}
	actions, lastCommittedRevOff := buildReplayActions(j)
	applyReplayActions(s.model, actions)

	// Heal torn tails and crash orphans so no half-written state
	// survives a restart.
	if err := truncateFile(s.revFile, lastCommittedRevOff, j.revSize); err != nil {
		return err
	}
	if err := truncateFile(s.decFile, j.decGood, j.decSize); err != nil {
		return err
	}
	// Conflict lines are journaled in detection order, one per CONFLICT
	// decision; lines beyond the committed count are crash orphans.
	committedConflicts := int64(0)
	for _, d := range j.decisions {
		if d.Outcome == OutcomeConflict {
			committedConflicts++
		}
	}
	if err := truncateFile(s.conFile, prefixOffset(j.conOffsets, committedConflicts), j.conSize); err != nil {
		return err
	}
	if err := truncateFile(s.rvwFile, j.rvwGood, j.rvwSize); err != nil {
		return err
	}
	return truncateFile(s.rplFile, j.rplGood, j.rplSize)
}

// truncateFile shortens f to length when it is longer (torn/orphan
// tail) and restores the append offset.
func truncateFile(f *os.File, length, size int64) error {
	if length < size {
		if err := f.Truncate(length); err != nil {
			return ioErr(err)
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return ioErr(err)
	}
	return nil
}

// prefixOffset returns the end offset of the first n lines; fewer lines
// available means the file is already shorter (torn), keep its good end.
func prefixOffset(offsets []int64, n int64) int64 {
	if n <= 0 {
		return 0
	}
	if int(n) > len(offsets) {
		return offsets[len(offsets)-1]
	}
	return offsets[n-1]
}

// journalData holds the good prefixes of every journal plus the offsets
// needed for healing.
type journalData struct {
	revisions  []*Record
	revOffsets []int64 // end offset of each good revision line
	revSize    int64
	decisions  []*Decision
	decGood    int64 // end offset of good decision prefix
	decSize    int64
	reviews    []*reviewEntry
	rvwGood    int64
	rvwSize    int64
	conOffsets []int64 // end offsets of parseable conflict lines
	conSize    int64
	rplGood    int64
	rplSize    int64
}

// readJournals scans all journals and returns their valid prefixes.
// The first torn/invalid line terminates the scan of that journal.
func (s *fileStore) readJournals() (*journalData, error) {
	j := &journalData{}
	var off int64

	// Revisions: valid = parses and carries an event id.
	err := scanLines(s.revFile, func(line string, end int64) bool {
		var r Record
		if json.Unmarshal([]byte(line), &r) != nil || r.EventID == "" {
			return false
		}
		j.revisions = append(j.revisions, &r)
		j.revOffsets = append(j.revOffsets, end)
		return true
	})
	if err != nil {
		return nil, err
	}
	if j.revSize, err = fileSize(s.revFile); err != nil {
		return nil, err
	}

	// Decisions.
	err = scanLines(s.decFile, func(line string, end int64) bool {
		var d Decision
		if json.Unmarshal([]byte(line), &d) != nil || d.EventID == "" {
			return false
		}
		j.decisions = append(j.decisions, &d)
		j.decGood = end
		return true
	})
	if err != nil {
		return nil, err
	}
	if j.decSize, err = fileSize(s.decFile); err != nil {
		return nil, err
	}

	// Reviews: every complete line is a committed conclusion.
	err = scanLines(s.rvwFile, func(line string, end int64) bool {
		var rw reviewEntry
		if json.Unmarshal([]byte(line), &rw) != nil || rw.ConflictID == "" {
			return false
		}
		j.reviews = append(j.reviews, &rw)
		j.rvwGood = end
		return true
	})
	if err != nil {
		return nil, err
	}
	if j.rvwSize, err = fileSize(s.rvwFile); err != nil {
		return nil, err
	}

	// Conflicts (audit log; state is re-derived from revisions +
	// decisions, offsets only serve orphan truncation).
	err = scanLines(s.conFile, func(line string, end int64) bool {
		var c Conflict
		if json.Unmarshal([]byte(line), &c) != nil || c.ConflictID == "" {
			return false
		}
		j.conOffsets = append(j.conOffsets, end)
		return true
	})
	if err != nil {
		return nil, err
	}
	if j.conSize, err = fileSize(s.conFile); err != nil {
		return nil, err
	}

	// Replay audit log: heal torn tail only.
	off = 0
	err = scanLines(s.rplFile, func(line string, end int64) bool {
		var rp ReplayReport
		if json.Unmarshal([]byte(line), &rp) != nil {
			return false
		}
		j.rplGood = end
		return true
	})
	if err != nil {
		return nil, err
	}
	if j.rplSize, err = fileSize(s.rplFile); err != nil {
		return nil, err
	}
	_ = off
	return j, nil
}

func fileSize(f *os.File) (int64, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, ioErr(err)
	}
	return fi.Size(), nil
}

// scanLines emits each non-empty line with its end offset until emit
// returns false (invalid line: terminates the valid prefix).
func scanLines(f *os.File, emit func(line string, end int64) bool) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return ioErr(err)
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var off int64
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		off += int64(len(sc.Bytes())) + 1
		if line == "" {
			continue
		}
		if !emit(line, off) {
			break
		}
	}
	if err := sc.Err(); err != nil {
		return ioErr(err)
	}
	_, err := f.Seek(0, io.SeekEnd)
	return err
}

func effectiveOf(r *Record) string {
	if r.EffectiveTime != "" {
		return r.EffectiveTime
	}
	return r.EventTime
}

// versionOrdinal assigns a total order across supported versions.
// HL7 2.3 < 2.5.1 < FHIR STU3 < R4. Cross-format ordering only serves
// the deterministic revision rule; same events usually share a format.
func versionOrdinal(v string) int {
	switch v {
	case "2.3":
		return 1
	case "2.5.1":
		return 2
	case "STU3":
		return 3
	case "R4":
		return 4
	}
	return 0
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

// clinicalDigest hashes only the clinical facts that must agree across
// format/version re-deliveries of the same event: event type, status,
// code, result value and unit. Source format/version, code system and
// display spelling (format-specific defaults/casing), unknown/extension
// leaves and timestamps are deliberately excluded, so semantically equal
// re-deliveries in HL7 2.3 and FHIR R4 compare equal. Units are compared
// case-insensitively because HL7 ("cel") and FHIR ("Cel") use different
// capitalization for the same UCUM unit. The digest is compared only
// within one event chain, so eventId is not part of it.
func clinicalDigest(r *Record) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%s",
		r.EventType, r.Status, r.Code, r.Value, strings.ToLower(r.Unit))
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
	d := &Decision{
		Seq: s.seq, EventID: rec.EventID, Format: rec.SourceFormat,
		Version: rec.SourceVersion, ChainID: chainIDFor(rec.EventID),
	}
	digest := businessDigest(rec)

	// Exact redelivery of any previously persisted revision is a
	// duplicate regardless of which revision currently wins.
	if s.model.digests[rec.EventID][digest] {
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
	_, known := s.model.chains[rec.EventID]
	if !known && s.cfg.MaxRecords > 0 && s.model.accepted >= s.cfg.MaxRecords {
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

	chain := s.model.chainFor(rec.EventID)
	cur := chain.effective
	outcome := OutcomeAccepted
	var conflict *Conflict
	if cur != nil {
		var reason ConflictReason
		var differs bool
		switch {
		case rec.PatientRef != cur.PatientRef:
			// Different patient on the same event id: the chain linkage
			// must never be rewritten. Distinguished from a content
			// conflict by its own stable reason.
			reason, differs = ReasonPatientMismatch, true
		case effectiveOf(rec) == effectiveOf(cur) && clinicalDigest(rec) != clinicalDigest(cur):
			// Same clinical time, same patient, but different clinical
			// facts. Version ordinal is irrelevant: a higher version
			// never silently overwrites the effective result.
			reason, differs = ReasonContentConflict, true
		}
		if differs {
			curDigest, inDigest := businessDigest(cur), digest
			cid := conflictIDFor(rec.EventID, curDigest, inDigest)
			// Backlog cap applies to both conflict kinds; the overflow
			// decision is the same whichever reason was detected.
			if s.cfg.MaxPendingReviews > 0 && s.model.pending >= s.cfg.MaxPendingReviews {
				if _, exists := s.model.conflicts[cid]; !exists {
					switch s.cfg.OverflowPolicy {
					case OverflowReject:
						d.Outcome = OutcomeRejected
						d.Reason = fmt.Sprintf("pending review backlog limit %d reached; reject policy", s.cfg.MaxPendingReviews)
						d.ErrorType = string(ErrStorageConflict)
						if err := s.appendDecision(d); err != nil {
							return nil, err
						}
						return d, conflictErr(d, "pending review backlog limit reached")
					case OverflowDegrade:
						d.Outcome = OutcomeDegraded
						d.Reason = fmt.Sprintf("pending review backlog limit %d reached; degraded journal", s.cfg.MaxPendingReviews)
						if err := s.appendJSON(s.degFile, rec); err != nil {
							return nil, err
						}
						if err := s.appendDecision(d); err != nil {
							return nil, err
						}
						return d, nil
					}
				}
			}
			outcome = OutcomeConflict
			d.ConflictID = cid
			d.Reason = s.conflictReasonText(reason, cur, rec, cid)
			if _, exists := s.model.conflicts[cid]; !exists {
				conflict = &Conflict{
					ConflictID:      cid,
					ChainID:         chain.id,
					EventID:         rec.EventID,
					Reason:          reason,
					CurrentDigest:   curDigest,
					IncomingDigest:  inDigest,
					CurrentPatient:  cur.PatientRef,
					IncomingPatient: rec.PatientRef,
					Status:          ConflictPending,
					DetectedSeq:     d.Seq,
				}
			}
		} else {
			// Same patient, clinical facts agree at this effective time.
			// Apply the deterministic revision rule (version ordinal,
			// then effective time); it never decides disagreeing content.
			switch cmp := compareCoords(rec, cur); {
			case cmp > 0:
				d.Reason = fmt.Sprintf("newer revision supersedes prior (ordinal=%d time=%s)",
					versionOrdinal(cur.SourceVersion), effectiveOf(cur))
			case cmp == 0:
				// Same ordinal and same effective time with equal
				// clinical facts: representation differs (e.g. another
				// format's code-system default), so it is not an exact
				// duplicate; it replaces no clinical fact. The incoming
				// revision becomes effective but yields the same view.
				d.Reason = fmt.Sprintf("equivalent revision at same coordinates (ordinal=%d time=%s); no clinical fact changed",
					versionOrdinal(rec.SourceVersion), effectiveOf(rec))
			default:
				outcome = OutcomeOutOfOrder
				d.Reason = fmt.Sprintf("stale revision (winner ordinal=%d time=%s vs incoming ordinal=%d time=%s); newer data retained",
					versionOrdinal(cur.SourceVersion), effectiveOf(cur),
					versionOrdinal(rec.SourceVersion), effectiveOf(rec))
			}
		}
	}

	// Commit order: revision, then conflict detection, then decision.
	// The decision is the commit record; a crash before it leaves an
	// orphan that replay truncates, so no half-applied state survives.
	if err := s.appendJSON(s.revFile, rec); err != nil {
		return nil, err
	}
	if conflict != nil {
		if err := s.appendJSON(s.conFile, conflict); err != nil {
			return nil, err
		}
	}
	s.model.applyCommitted(rec, d.Seq, outcome, d.Reason)
	d.Outcome = outcome
	if err := s.appendDecision(d); err != nil {
		return nil, err
	}
	switch outcome {
	case OutcomeOutOfOrder:
		return d, conflictErr(d, "out-of-order message retained as stale revision")
	case OutcomeConflict:
		return d, conflictErr(d, "content conflict parked for review; effective version retained")
	}
	return d, nil
}

func conflictErr(d *Decision, msg string) error {
	return &IngestionError{
		Type: ErrStorageConflict, Message: msg, Reason: string(d.Outcome),
		EventID: d.EventID, Detail: d.Reason,
	}
}

// conflictReasonText renders the stable, machine-distinguishable reason
// plus the human-auditable judgment basis (arrival-independent facts)
// written into the decision journal.
func (s *fileStore) conflictReasonText(reason ConflictReason, cur, rec *Record, cid string) string {
	switch reason {
	case ReasonPatientMismatch:
		return fmt.Sprintf("%s: event id %s delivered for patient %s while the chain belongs to %s; chain patient linkage and effective version retained; parked for review as %s",
			reason, rec.EventID, rec.PatientRef, cur.PatientRef, cid)
	default:
		return fmt.Sprintf("%s: same clinical effective time %s (incoming ordinal=%d %s/%s vs current ordinal=%d %s/%s) but different clinical content (digests incoming=%s current=%s); parked for review as %s; effective version retained",
			reason, effectiveOf(rec),
			versionOrdinal(rec.SourceVersion), rec.SourceFormat, rec.SourceVersion,
			versionOrdinal(cur.SourceVersion), cur.SourceFormat, cur.SourceVersion,
			shortDigest(businessDigest(rec)), shortDigest(businessDigest(cur)), cid)
	}
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
	return s.model.snapshot(), nil
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

func (s *fileStore) Chains(_ context.Context) ([]*ChainView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.model.chainViews(), nil
}

func (s *fileStore) PendingConflicts(_ context.Context) ([]*Conflict, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.model.pendingConflicts(), nil
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
	for _, f := range []*os.File{s.revFile, s.decFile, s.degFile, s.conFile, s.rvwFile, s.rplFile} {
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
