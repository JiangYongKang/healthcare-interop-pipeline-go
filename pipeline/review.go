package pipeline

// ReviewAction selects how a review conclusion resolves a conflict.
type ReviewAction string

const (
	// ReviewUseDelivery pins one retained delivery (by arrival seq) as
	// the effective version of the chain.
	ReviewUseDelivery ReviewAction = "USE_DELIVERY"
	// ReviewKeepCurrent confirms the provisional current version and
	// closes the pending-review state.
	ReviewKeepCurrent ReviewAction = "KEEP_CURRENT"
)

// ReviewRequest is one review conclusion submitted for an event chain.
// ReviewID makes application idempotent: re-submitting the same
// conclusion never creates duplicate state or duplicate records.
type ReviewRequest struct {
	ReviewID  string       `json:"reviewId"`
	EventID   string       `json:"eventId"`
	Action    ReviewAction `json:"action"`
	TargetSeq int64        `json:"targetSeq,omitempty"`
	Reason    string       `json:"reason,omitempty"`
	Reviewer  string       `json:"reviewer,omitempty"`
}

// Validate enforces the review request contract.
func (r *ReviewRequest) Validate() error {
	if r.ReviewID == "" {
		return externalError(ErrValidationError, "review id is required", "MISSING_REVIEW_ID", r.EventID, "empty reviewId")
	}
	if r.EventID == "" {
		return externalError(ErrValidationError, "event id is required", "MISSING_EVENT_ID", "", "empty eventId in review")
	}
	switch r.Action {
	case ReviewKeepCurrent:
		// no target needed
	case ReviewUseDelivery:
		if r.TargetSeq <= 0 {
			return externalError(ErrValidationError, "target delivery seq is required", "MISSING_REVIEW_TARGET", r.EventID, "USE_DELIVERY without targetSeq")
		}
	default:
		return externalError(ErrValidationError, "unknown review action", "INVALID_REVIEW_ACTION", r.EventID, "action="+string(r.Action))
	}
	return nil
}

// ReviewRecord is a locally journaled, applied review conclusion.
type ReviewRecord struct {
	ReviewID   string       `json:"reviewId"`
	EventID    string       `json:"eventId"`
	Action     ReviewAction `json:"action"`
	TargetSeq  int64        `json:"targetSeq,omitempty"`
	Reason     string       `json:"reason,omitempty"`
	Reviewer   string       `json:"reviewer,omitempty"`
	AppliedSeq int64        `json:"appliedSeq"`
	Duplicate  bool         `json:"duplicate,omitempty"`
}
