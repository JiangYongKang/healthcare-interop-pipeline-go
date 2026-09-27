package pipeline

// ChainStatus is the externally visible state of one event chain.
type ChainStatus string

const (
	// ChainEffective: the chain has a current effective version and no
	// unresolved conflict.
	ChainEffective ChainStatus = "EFFECTIVE"
	// ChainPendingReview: deliveries with genuinely inconsistent content
	// were observed; the provisional current version is kept but the
	// chain awaits a review conclusion.
	ChainPendingReview ChainStatus = "PENDING_REVIEW"
)

// Stable, machine-distinguishable conflict reason codes. A conflicting
// delivery is never silently applied by arrival order or source format.
const (
	ReasonConflictPatient   = "CONFLICT_PATIENT"
	ReasonConflictEventType = "CONFLICT_EVENT_TYPE"
	ReasonConflictCode      = "CONFLICT_CODE"
	ReasonConflictValue     = "CONFLICT_VALUE"
	ReasonConflictStatus    = "CONFLICT_STATUS"
)

// Delivery relations of one retained delivery to the current effective
// version of its chain.
const (
	RelationCurrent         = "CURRENT"
	RelationSuperseded      = "SUPERSEDED"
	RelationStale           = "STALE"
	RelationConflictPending = "CONFLICT_PENDING"
	RelationReviewPinned    = "REVIEW_PINNED"
)

// Delivery describes one non-duplicate, retained delivery inside an
// event chain: where it came from, when it arrived, and how it ended up
// relative to the current effective version.
type Delivery struct {
	Seq           int64   `json:"seq"`
	EventID       string  `json:"eventId"`
	Digest        string  `json:"digest"`
	Format        string  `json:"format"`
	Version       string  `json:"version"`
	EffectiveTime string  `json:"effectiveTime"`
	Outcome       Outcome `json:"outcome"`
	Relation      string  `json:"relation"`
	Reason        string  `json:"reason,omitempty"`
}

// ChainView is the event-consistency view of one clinical event across
// all formats, versions and arrival orders. All records inside are
// already de-identified; no raw sensitive value appears here.
type ChainView struct {
	EventID       string          `json:"eventId"`
	Status        ChainStatus     `json:"status"`
	PatientRef    string          `json:"patientRef"` // pseudonym only
	Current       *Record         `json:"current,omitempty"`
	CurrentDigest string          `json:"currentDigest,omitempty"`
	Deliveries    []*Delivery     `json:"deliveries"`
	Reviews       []*ReviewRecord `json:"reviews,omitempty"`
}

// verdict is the deterministic adjudication result of one incoming
// record against the current effective version of its chain.
type verdict int

const (
	verdictSupersede verdict = iota // incoming wins; supersedes current
	verdictStale                    // incoming is older/lower; retained, not applied
	verdictConflict                 // genuinely inconsistent; pending review
)

// adjudicate compares an incoming record with the current effective
// version using only record content. The result never depends on
// arrival order or on which source format delivered the data:
//
//  1. identity fields (patient pseudonym, event type, code/system)
//     disagree            -> CONFLICT with a stable identity reason;
//  2. revision order (effective time, then status rank) is strict
//     -> the strictly newer side wins, the older side is STALE;
//  3. same revision rank but different clinical content
//     -> CONFLICT with a stable content reason (never a silent
//     format- or arrival-order-based overwrite).
func adjudicate(incoming, current *Record) (verdict, string) {
	if reason := identityConflict(incoming, current); reason != "" {
		return verdictConflict, reason
	}
	switch compareRevision(incoming, current) {
	case 1:
		return verdictSupersede, ""
	case -1:
		return verdictStale, ""
	}
	if businessDigest(incoming) == businessDigest(current) {
		// Same content at the same rank: a redelivery variant (e.g.
		// different unknown fields already tokenized). Not a conflict;
		// the retained current version stands.
		return verdictStale, ""
	}
	if reason := contentConflict(incoming, current); reason != "" {
		return verdictConflict, reason
	}
	// Representation-only difference (display text, arrival metadata,
	// extension fields): clinical facts agree, so this is not a
	// conflict; the current version simply stands.
	return verdictStale, ""
}

// compareRevision orders two records of one event by revision rank:
// effective time first (a correction is newer when its clinical time is
// newer), then status rank (amended > final/active/resolved >
// preliminary > entered-in-error). Source format/version is deliberately
// NOT part of the order, so no format silently outranks another.
func compareRevision(a, b *Record) int {
	ta, tb := effectiveOf(a), effectiveOf(b)
	if ta != tb {
		if ta > tb {
			return 1
		}
		return -1
	}
	sa, sb := statusRank(a.Status), statusRank(b.Status)
	if sa != sb {
		if sa > sb {
			return 1
		}
		return -1
	}
	return 0
}

// identityConflict reports a stable reason when two records of the same
// event id disagree on identity-defining clinical fields. PatientRef is
// already a pseudonym here, so the check (and its logged reason) never
// touches a raw identifier.
func identityConflict(a, b *Record) string {
	if a.PatientRef != b.PatientRef {
		return ReasonConflictPatient
	}
	if a.EventType != b.EventType {
		return ReasonConflictEventType
	}
	if a.Code != b.Code || a.CodeSystem != b.CodeSystem {
		return ReasonConflictCode
	}
	return ""
}

// contentConflict reports a stable reason when two records of the same
// event disagree on clinical facts (result value/unit or status).
// Representation-only differences (display text, extension fields) are
// not clinical conflicts.
func contentConflict(a, b *Record) string {
	if a.Value != b.Value || a.Unit != b.Unit {
		return ReasonConflictValue
	}
	if a.Status != b.Status {
		return ReasonConflictStatus
	}
	return ""
}

// statusRank orders revision-bearing statuses; higher supersedes at
// equal effective time. "amended" marks an explicit correction and
// outranks a plain "final"; "entered-in-error" ranks lowest as a
// retraction marker.
func statusRank(s string) int {
	switch s {
	case StatusAmended:
		return 3
	case StatusFinal, StatusActive, StatusResolved:
		return 2
	case StatusPreliminary:
		return 1
	case StatusEnteredInError:
		return 0
	}
	return 1
}
