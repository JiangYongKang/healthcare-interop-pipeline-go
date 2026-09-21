// Package apperr defines the small, stable set of error types that the
// pipeline exposes to callers. Low-level parser/decoder details must never
// cross this boundary; they are retained only in the internal audit trail.
package apperr

import "errors"

// Kind is the externally visible error category.
type Kind string

const (
	KindFormat     Kind = "FORMAT_ERROR"     // upstream message malformed / illegal encoding / truncated
	KindValidation Kind = "VALIDATION_ERROR" // structurally parseable but violates business rules
	KindConflict   Kind = "CONFLICT"         // idempotency / storage conflict (stale, duplicate, clash)
	KindLimit      Kind = "LIMIT_EXCEEDED"   // configured capacity / memory ceiling hit
)

// Code is the stable, distinguishable machine-readable reason code.
type Code string

const (
	CodeTruncated          Code = "MSG_TRUNCATED"
	CodeIllegalEncoding    Code = "ILLEGAL_ENCODING"
	CodeUnsupportedVersion Code = "UNSUPPORTED_VERSION"
	CodeUnknownFormat      Code = "UNKNOWN_FORMAT"
	CodeMalformedJSON      Code = "MALFORMED_JSON"
	CodeMissingRequired    Code = "MISSING_REQUIRED_FIELD"
	CodeInvalidValue       Code = "INVALID_VALUE"
	CodeDuplicateEvent     Code = "DUPLICATE_EVENT"
	CodeStaleVersion       Code = "STALE_VERSION"
	CodeStorageClash       Code = "STORAGE_CLASH"
	CodeCapacityExceeded   Code = "CAPACITY_EXCEEDED"
)

// codeKind maps each stable code to its external category.
var codeKind = map[Code]Kind{
	CodeTruncated:          KindFormat,
	CodeIllegalEncoding:    KindFormat,
	CodeUnsupportedVersion: KindFormat,
	CodeUnknownFormat:      KindFormat,
	CodeMalformedJSON:      KindFormat,
	CodeMissingRequired:    KindValidation,
	CodeInvalidValue:       KindValidation,
	CodeDuplicateEvent:     KindConflict,
	CodeStaleVersion:       KindConflict,
	CodeStorageClash:       KindConflict,
	CodeCapacityExceeded:   KindLimit,
}

// safeDetail is the fixed, PHI-free, internals-free text shown per code.
var safeDetail = map[Code]string{
	CodeTruncated:          "message structure is incomplete (truncated)",
	CodeIllegalEncoding:    "message contains illegal character encoding",
	CodeUnsupportedVersion: "message format version is not supported",
	CodeUnknownFormat:      "message format could not be recognized",
	CodeMalformedJSON:      "message is not well-formed JSON",
	CodeMissingRequired:    "a required business field is missing",
	CodeInvalidValue:       "a field has an invalid value",
	CodeDuplicateEvent:     "event already ingested (duplicate redelivery)",
	CodeStaleVersion:       "event version is older than the current one",
	CodeStorageClash:       "stored event conflicts with the incoming payload",
	CodeCapacityExceeded:   "configured ingestion capacity ceiling reached",
}

// Error is the external error type. Detail carries a safe (non-PHI,
// non-internal) message; InternalDetail is never serialized to callers.
type Error struct {
	Kind           Kind
	Code           Code
	Detail         string
	MessageID      string
	InternalDetail string
}

func (e *Error) Error() string {
	id := e.MessageID
	if id == "" {
		id = "-"
	}
	return string(e.Kind) + "/" + string(e.Code) + " [message=" + id + "]: " + e.Detail
}

// New builds an external error for the given code. The public Detail is the
// fixed safe text; callers attach internal specifics via WithInternal.
func New(code Code, detail string) *Error {
	d := detail
	if d == "" {
		d = safeDetail[code]
	}
	return &Error{Kind: codeKind[code], Code: code, Detail: d}
}

// WithMessage tags the error with the upstream message identifier.
func (e *Error) WithMessage(id string) *Error {
	e.MessageID = id
	return e
}

// WithInternal attaches low-level parsing detail that must remain internal.
func (e *Error) WithInternal(internal string) *Error {
	e.InternalDetail = internal
	return e
}

// Classify maps an arbitrary pipeline error to an *Error. Unknown errors are
// deliberately reported as a generic validation failure so internal details
// never leak; typed *Error values pass through unchanged.
func Classify(err error) *Error {
	if err == nil {
		return nil
	}
	var ae *Error
	if errors.As(err, &ae) {
		return ae
	}
	return New(CodeInvalidValue, "message rejected by pipeline")
}

// SafeMessage returns a caller-safe message with no parser internals or PHI.
func SafeMessage(e *Error) string {
	if e == nil {
		return ""
	}
	id := e.MessageID
	if id == "" {
		id = "-"
	}
	return string(e.Kind) + " " + string(e.Code) + " [message=" + id + "]: " + e.Detail
}
