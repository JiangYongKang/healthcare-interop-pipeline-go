package pipeline

import "fmt"

// ErrorType is the externally visible, stable classification of an
// ingestion failure. Parser internals are never exposed directly.
type ErrorType string

const (
	// ErrFormatError: upstream payload could not be parsed (malformed,
	// truncated, illegal encoding, unsupported format/version).
	ErrFormatError ErrorType = "FORMAT_ERROR"
	// ErrValidationError: structurally parsed but failed business rules
	// (required field missing, illegal enum, etc.).
	ErrValidationError ErrorType = "VALIDATION_ERROR"
	// ErrStorageConflict: persistence rejected the record (duplicate /
	// stale out-of-order data, or resource limit policy rejection).
	ErrStorageConflict ErrorType = "STORAGE_CONFLICT"
	// ErrUnavailable: the pipeline itself is unavailable (shutting down,
	// or a degraded persistence write failed).
	ErrUnavailable ErrorType = "UNAVAILABLE"
)

// IngestionError is the only error shape presented to external callers.
// Internal parser details stay in Detail, which is safe to log but is not
// part of the stable interface contract.
type IngestionError struct {
	Type    ErrorType
	Message string
	Reason  string
	EventID string
	Detail  string
}

func (e *IngestionError) Error() string {
	if e.EventID != "" {
		return fmt.Sprintf("%s: %s (reason=%s, eventId=%s)", e.Type, e.Message, e.Reason, e.EventID)
	}
	return fmt.Sprintf("%s: %s (reason=%s)", e.Type, e.Message, e.Reason)
}

func externalError(t ErrorType, msg, reason, eventID, detail string) *IngestionError {
	return &IngestionError{Type: t, Message: msg, Reason: reason, EventID: eventID, Detail: detail}
}
