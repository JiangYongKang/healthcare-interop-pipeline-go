package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

// ChainStatus is the review state of one event chain.
type ChainStatus string

const (
	// ChainEffective: the chain has a current effective version and no
	// unresolved conflict.
	ChainEffective ChainStatus = "EFFECTIVE"
	// ChainPendingReview: at least one content conflict is awaiting a
	// review decision; the last effective version is retained.
	ChainPendingReview ChainStatus = "PENDING_REVIEW"
)

// DeliveryView describes one delivery (revision) inside an event chain:
// where it came from, how it relates in time, and its final state.
// It carries de-identified data only.
type DeliveryView struct {
	Seq           int64   `json:"seq"`
	EventID       string  `json:"eventId"`
	PatientRef    string  `json:"patientRef"` // pseudonym of this delivery (audit for patient mismatches)
	Format        string  `json:"format"`
	Version       string  `json:"version"`
	Outcome       Outcome `json:"outcome"`
	Reason        string  `json:"reason,omitempty"`
	EffectiveTime string  `json:"effectiveTime,omitempty"`
	Digest        string  `json:"digest"` // short business-content digest
}

// ChainView is the consistency view of one clinical event: every
// delivery regardless of source format/version, the current effective
// version, and the review state. Contains no raw sensitive values.
type ChainView struct {
	ChainID          string          `json:"chainId"`
	EventID          string          `json:"eventId"`
	EventType        string          `json:"eventType"`
	PatientRef       string          `json:"patientRef"` // pseudonym
	Status           ChainStatus     `json:"status"`
	Effective        *Record         `json:"effective,omitempty"`
	Deliveries       []*DeliveryView `json:"deliveries"`
	PendingConflicts int             `json:"pendingConflicts"`
}

// chainIDFor derives the stable chain identifier for a clinical event.
// The chain key is the clinical event id (HL7 MSH-10 / FHIR id), which
// upstream systems keep stable across formats and versions of the same
// event; the hash keeps the view namespace uniform and free of any
// delimiter collisions.
func chainIDFor(eventID string) string {
	sum := sha256.Sum256([]byte("chain\x00" + eventID))
	return "CHAIN-" + hex.EncodeToString(sum[:])[:hashLen]
}

// revisionEntry is one retained delivery inside a chain.
type revisionEntry struct {
	rec     *Record
	seq     int64
	outcome Outcome
	reason  string
	digest  string
}

// chainState is the mutable per-event chain state.
type chainState struct {
	id        string
	eventID   string
	revisions []*revisionEntry
	effective *Record // current effective version (points into revisions)
}

// chainModel is the pure consistency state derived from the journals.
// The live store holds one instance; a consistency replay builds a
// second instance from history and compares. All transitions are
// deterministic functions of (revision, recorded outcome) or (review),
// so reprocessing history yields the same effective result as
// incremental processing.
type chainModel struct {
	chains    map[string]*chainState
	digests   map[string]map[string]bool // eventID -> persisted content digests
	conflicts map[string]*Conflict       // conflictID -> conflict
	pending   int                        // unresolved conflicts
	accepted  int                        // chains with an effective record
}

func newChainModel() *chainModel {
	return &chainModel{
		chains:    map[string]*chainState{},
		digests:   map[string]map[string]bool{},
		conflicts: map[string]*Conflict{},
	}
}

func (m *chainModel) chainFor(eventID string) *chainState {
	c := m.chains[eventID]
	if c == nil {
		c = &chainState{id: chainIDFor(eventID), eventID: eventID}
		m.chains[eventID] = c
	}
	return c
}

// compareCoords orders two revisions deterministically, independent of
// arrival order and of content: format-version ordinal, then effective
// time. Equal coordinates with different content is a conflict and is
// never resolved silently, so content is deliberately not a tiebreak.
func compareCoords(r, c *Record) int {
	rv, cv := versionOrdinal(r.SourceVersion), versionOrdinal(c.SourceVersion)
	if rv != cv {
		return rv - cv
	}
	rt, ct := effectiveOf(r), effectiveOf(c)
	switch {
	case rt > ct:
		return 1
	case rt < ct:
		return -1
	}
	return 0
}

// applyCommitted folds one committed (revision, decision) pair into the
// model. Used both by journal replay on open and by consistency replay.
func (m *chainModel) applyCommitted(rec *Record, seq int64, outcome Outcome, reason string) {
	chain := m.chainFor(rec.EventID)
	digest := businessDigest(rec)
	if m.digests[rec.EventID] == nil {
		m.digests[rec.EventID] = map[string]bool{}
	}
	m.digests[rec.EventID][digest] = true
	chain.revisions = append(chain.revisions, &revisionEntry{
		rec: rec, seq: seq, outcome: outcome, reason: reason, digest: digest,
	})
	switch outcome {
	case OutcomeAccepted:
		if chain.effective == nil {
			m.accepted++
		}
		chain.effective = rec
	case OutcomeOutOfOrder:
		// Retained for audit; never displaces the effective version.
	case OutcomeConflict:
		cur := chain.effective
		curDigest := ""
		if cur != nil {
			curDigest = businessDigest(cur)
		}
		// Derive the stable conflict reason from the disagreeing pair
		// itself, so replay reaches the same classification regardless
		// of arrival order: a different patient on one chain is a
		// patient mismatch; otherwise it is a content conflict.
		reason := ReasonContentConflict
		curPatient, inPatient := "", rec.PatientRef
		if cur != nil {
			curPatient = cur.PatientRef
			if cur.PatientRef != rec.PatientRef {
				reason = ReasonPatientMismatch
			}
		}
		cid := conflictIDFor(rec.EventID, curDigest, digest)
		if _, ok := m.conflicts[cid]; !ok {
			m.conflicts[cid] = &Conflict{
				ConflictID:      cid,
				ChainID:         chain.id,
				EventID:         rec.EventID,
				Reason:          reason,
				CurrentDigest:   curDigest,
				IncomingDigest:  digest,
				CurrentPatient:  curPatient,
				IncomingPatient: inPatient,
				Status:          ConflictPending,
				DetectedSeq:     seq,
			}
			m.pending++
		}
	}
}

// applyReview folds one committed review conclusion into the model.
// Re-applying an already-applied conclusion is a no-op.
func (m *chainModel) applyReview(e *reviewEntry) {
	c := m.conflicts[e.ConflictID]
	if c == nil || c.Status == ConflictResolved {
		return
	}
	c.Status = ConflictResolved
	c.Resolution = &Resolution{
		Choice:       e.Choice,
		WinnerDigest: e.WinnerDigest,
		Reviewer:     e.Reviewer,
		Seq:          e.Seq,
	}
	m.pending--
	if e.Choice == AcceptIncoming {
		chain := m.chains[c.EventID]
		if chain == nil {
			return
		}
		if rec := m.revisionByDigest(chain, c.IncomingDigest); rec != nil {
			chain.effective = rec
		}
	}
}

func (m *chainModel) revisionByDigest(chain *chainState, digest string) *Record {
	for _, rev := range chain.revisions {
		if rev.digest == digest {
			return rev.rec
		}
	}
	return nil
}

// chainViews renders the de-identified consistency view, sorted by
// event id for stable output.
func (m *chainModel) chainViews() []*ChainView {
	ids := make([]string, 0, len(m.chains))
	for id := range m.chains {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*ChainView, 0, len(ids))
	for _, id := range ids {
		chain := m.chains[id]
		view := &ChainView{
			ChainID: chain.id,
			EventID: chain.eventID,
			Status:  ChainEffective,
		}
		if chain.effective != nil {
			view.Effective = cloneRecord(chain.effective)
			view.EventType = chain.effective.EventType
			view.PatientRef = chain.effective.PatientRef
		} else if len(chain.revisions) > 0 {
			view.EventType = chain.revisions[0].rec.EventType
			view.PatientRef = chain.revisions[0].rec.PatientRef
		}
		for _, rev := range chain.revisions {
			view.Deliveries = append(view.Deliveries, &DeliveryView{
				Seq:           rev.seq,
				EventID:       rev.rec.EventID,
				PatientRef:    rev.rec.PatientRef,
				Format:        rev.rec.SourceFormat,
				Version:       rev.rec.SourceVersion,
				Outcome:       rev.outcome,
				Reason:        rev.reason,
				EffectiveTime: effectiveOf(rev.rec),
				Digest:        shortDigest(rev.digest),
			})
		}
		for _, c := range m.conflicts {
			if c.ChainID == chain.id && c.Status == ConflictPending {
				view.PendingConflicts++
			}
		}
		if view.PendingConflicts > 0 {
			view.Status = ChainPendingReview
		}
		out = append(out, view)
	}
	return out
}

// pendingConflicts lists unresolved conflicts in detection order.
func (m *chainModel) pendingConflicts() []*Conflict {
	out := make([]*Conflict, 0, len(m.conflicts))
	for _, c := range m.conflicts {
		if c.Status == ConflictPending {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DetectedSeq < out[j].DetectedSeq })
	return out
}

// snapshot returns the effective record of every chain, ordered by
// (eventTime, eventId).
func (m *chainModel) snapshot() []*Record {
	out := make([]*Record, 0, len(m.chains))
	for _, chain := range m.chains {
		if chain.effective != nil {
			out = append(out, cloneRecord(chain.effective))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EventTime != out[j].EventTime {
			return out[i].EventTime < out[j].EventTime
		}
		return out[i].EventID < out[j].EventID
	})
	return out
}

func shortDigest(d string) string {
	if len(d) > hashLen {
		return d[:hashLen]
	}
	return d
}
