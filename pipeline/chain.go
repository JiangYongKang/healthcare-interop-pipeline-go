package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
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
	digests   map[string]map[string]bool // eventID -> persisted business digests
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

// canonicalCodeSystem folds format-specific coding-system spellings for
// the same code system into one token, so cross-format re-deliveries of
// the same clinical fact compare equal (HL7 LN == LOINC URL). Local
// codes ("L") and unknown systems are left untouched.
func canonicalCodeSystem(sys string) string {
	switch sys {
	case "LN", "http://loinc.org":
		return "LOINC"
	default:
		return sys
	}
}

// canonicalUnit folds case/notation-only unit differences: UCUM unit
// codes are case-sensitive in general, but the local sample dialect
// spells the same Celsius unit as "cel"/"Cel"/"CEL".
func canonicalUnit(u string) string {
	return strings.ToUpper(strings.TrimSpace(u))
}

// canonicalValue normalizes numeric result values (37 == 37.0 == "37")
// while leaving textual findings untouched, so number-formatting
// differences across formats are not clinical disagreements.
func canonicalValue(v string) string {
	if n, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
		return "num:" + strconv.FormatFloat(n, 'g', -1, 64)
	}
	return "txt:" + strings.TrimSpace(v)
}

// clinicalDigest hashes only the clinical facts of a delivery, ignoring
// source format/version, arrival metadata, display text (a format-
// specific label for the same code), unknown fields and the patient
// reference (patient disagreements have their own reason). Two
// deliveries with equal clinical facts at the same effective time are
// the same clinical statement regardless of wire format, which is what
// lets a newer-format re-delivery supersede an older one, and what lets
// genuine disagreements be detected across formats/versions.
func clinicalDigest(r *Record) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%s\x00%s",
		r.EventType,
		r.Status,
		effectiveOf(r),
		canonicalCodeSystem(r.CodeSystem)+":"+strings.TrimSpace(r.Code),
		canonicalValue(r.Value),
		canonicalUnit(r.Unit))
	return hex.EncodeToString(h.Sum(nil))
}

// clinicalDiffFields lists the clinical fact fields that differ between
// two records, in a stable order, for conflict decision logs. Patient is
// intentionally absent (patient mismatch has its own reason) and the
// code display is absent (format-specific label, not a clinical fact).
func clinicalDiffFields(r, c *Record) []string {
	var diffs []string
	add := func(name string, same bool) {
		if !same {
			diffs = append(diffs, name)
		}
	}
	add("eventType", r.EventType == c.EventType)
	add("status", r.Status == c.Status)
	add("codeSystem", canonicalCodeSystem(r.CodeSystem) == canonicalCodeSystem(c.CodeSystem))
	add("code", strings.TrimSpace(r.Code) == strings.TrimSpace(c.Code))
	add("value", canonicalValue(r.Value) == canonicalValue(c.Value))
	add("unit", canonicalUnit(r.Unit) == canonicalUnit(c.Unit))
	return diffs
}

// revisionClass is the deterministic adjudication of an incoming
// delivery against the chain's current effective version. It is a pure
// function of the two records and therefore yields the same verdict in
// either arrival order and during journal replay.
type revisionClass struct {
	outcome Outcome
	reason  ConflictReason // set when outcome == OutcomeConflict
	fields  []string       // clinical fields that disagree (content conflicts)
	basis   string         // human-readable decision basis for the journal
}

// classifyRevision adjudicates incoming rec against the currently
// effective cur:
//
//   - Different patient reference (a different real person) ->
//     PATIENT_MISMATCH conflict, regardless of coordinates; the chain's
//     patient association is never rewritten.
//   - Same clinical effective time with genuinely different clinical
//     content -> content conflict, regardless of format/version
//     ordering. A newer/higher-priority format never silently overwrites
//     a disagreeing statement, and a stale late delivery is not merely
//     labelled out-of-order when the facts disagree. Same-revision
//     coordinates yield CONTENT_CONFLICT_SAME_REVISION; different
//     version coordinates yield CONTENT_CONFLICT_CROSS_REVISION.
//   - Same clinical effective time with equal clinical facts -> the
//     deterministic coordinate rule applies: a higher ordinal re-
//     delivery supersedes, a lower ordinal late delivery is
//     OUT_OF_ORDER, equal ordinals are an identical redelivery
//     (DUPLICATE).
//   - Different effective time -> the deterministic coordinate rule
//     applies (newer data supersedes; stale late delivery is
//     OUT_OF_ORDER and retained).
func classifyRevision(rec, cur *Record) revisionClass {
	if rec.PatientRef != cur.PatientRef {
		return revisionClass{
			outcome: OutcomeConflict,
			reason:  ReasonPatientMismatch,
			basis: fmt.Sprintf("%s: delivery patient=%s differs from effective patient=%s; chain patient association retained, parked for review",
				ReasonPatientMismatch, shortDigest(rec.PatientRef), shortDigest(cur.PatientRef)),
		}
	}

	cmp := compareCoords(rec, cur)
	sameClinicalTime := effectiveOf(rec) == effectiveOf(cur)
	sameClinical := clinicalDigest(rec) == clinicalDigest(cur)

	if sameClinicalTime && !sameClinical {
		reason := ReasonContentConflictCrossRevision
		if versionOrdinal(rec.SourceVersion) == versionOrdinal(cur.SourceVersion) {
			reason = ReasonContentConflictSameRevision
		}
		return revisionClass{
			outcome: OutcomeConflict,
			reason:  reason,
			fields:  clinicalDiffFields(rec, cur),
			basis: fmt.Sprintf("%s: same clinical time %s but clinical content differs (incoming ordinal=%d %s/%s vs effective ordinal=%d %s/%s); effective version retained, parked for review",
				reason, effectiveOf(rec),
				versionOrdinal(rec.SourceVersion), rec.SourceFormat, rec.SourceVersion,
				versionOrdinal(cur.SourceVersion), cur.SourceFormat, cur.SourceVersion),
		}
	}

	switch {
	case cmp > 0:
		return revisionClass{
			outcome: OutcomeAccepted,
			basis: fmt.Sprintf("newer revision supersedes prior (ordinal=%d time=%s) with equal clinical facts at the replaced coordinate",
				versionOrdinal(cur.SourceVersion), effectiveOf(cur)),
		}
	case cmp == 0:
		// Equal coordinates and equal clinical facts: a semantically
		// identical redelivery in another encoding. No new effective
		// content; mark DUPLICATE so it never silently "rewrites" the
		// effective record.
		return revisionClass{
			outcome: OutcomeDuplicate,
			basis: fmt.Sprintf("clinically identical redelivery at equal coordinates (ordinal=%d time=%s); effective version retained",
				versionOrdinal(cur.SourceVersion), effectiveOf(cur)),
		}
	default:
		return revisionClass{
			outcome: OutcomeOutOfOrder,
			basis: fmt.Sprintf("stale revision (winner ordinal=%d time=%s vs incoming ordinal=%d time=%s); newer data retained",
				versionOrdinal(cur.SourceVersion), effectiveOf(cur),
				versionOrdinal(rec.SourceVersion), effectiveOf(rec)),
		}
	}
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
		cid := conflictIDForPair(rec.EventID, cur, rec)
		if _, ok := m.conflicts[cid]; !ok {
			// Re-derive the stable reason (and evidence fields) from the
			// records themselves so replay needs no reason journal: the
			// verdict is a pure function of the two revisions and is
			// therefore identical live and replayed, in either original
			// arrival order.
			reason := ReasonContentConflictSameRevision
			var fields []string
			curPatient, inPatient := "", ""
			if cur != nil {
				if cls := classifyRevision(rec, cur); cls.reason != "" {
					reason = cls.reason
					fields = cls.fields
				}
				curPatient, inPatient = cur.PatientRef, rec.PatientRef
			}
			m.conflicts[cid] = &Conflict{
				ConflictID:      cid,
				ChainID:         chain.id,
				EventID:         rec.EventID,
				Reason:          reason,
				CurrentDigest:   curDigest,
				IncomingDigest:  digest,
				DifferingFields: fields,
				Status:          ConflictPending,
				DetectedSeq:     seq,
			}
			if reason == ReasonPatientMismatch {
				m.conflicts[cid].CurrentPatient = curPatient
				m.conflicts[cid].IncomingPatient = inPatient
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
