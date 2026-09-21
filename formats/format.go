// Package formats contains parsers for the supported upstream clinical
// message formats and versions. Parsers never log or skip: every problem
// is returned as a RejectError with a distinguishable reason.
package formats

import (
	"bytes"
	"fmt"
)

// RejectReason is a stable, machine-readable rejection classification.
type RejectReason string

const (
	ReasonEmptyPayload         RejectReason = "EMPTY_PAYLOAD"
	ReasonEncodingInvalid      RejectReason = "ENCODING_INVALID"
	ReasonStructureTruncated   RejectReason = "STRUCTURE_TRUNCATED"
	ReasonUnsupportedFormat    RejectReason = "UNSUPPORTED_FORMAT"
	ReasonUnsupportedVersion   RejectReason = "UNSUPPORTED_VERSION"
	ReasonUnsupportedEventType RejectReason = "UNSUPPORTED_EVENT_TYPE"
	ReasonMissingEventID       RejectReason = "MISSING_EVENT_ID"
	ReasonMissingPatientID     RejectReason = "MISSING_PATIENT_ID"
	ReasonMissingEventType     RejectReason = "MISSING_EVENT_TYPE"
	ReasonMissingCode          RejectReason = "MISSING_CODE"
	ReasonMissingEventTime     RejectReason = "MISSING_EVENT_TIME"
	ReasonInvalidTime          RejectReason = "INVALID_TIME"
	ReasonInvalidStatus        RejectReason = "INVALID_STATUS"
	ReasonInvalidComponent     RejectReason = "INVALID_COMPONENT"
	ReasonSegmentUnknown       RejectReason = "SEGMENT_UNKNOWN"
	ReasonJSONInvalid          RejectReason = "JSON_INVALID"
	ReasonEmptyEnvelope        RejectReason = "EMPTY_ENVELOPE"
)

// RejectError carries a stable reason plus an internal detail. Detail is
// intended for local logs and must never be shown verbatim externally.
type RejectError struct {
	Reason  RejectReason
	Detail  string
	EventID string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("%s: %s", e.Reason, e.Detail)
}

// NewReject constructs a RejectError.
func NewReject(reason RejectReason, detail string, args ...any) *RejectError {
	return &RejectError{Reason: reason, Detail: fmt.Sprintf(detail, args...)}
}

// RawField is a raw (possibly unknown) field captured from a message.
type RawField struct {
	Path  string
	Value string
}

// ParsedMessage is the format-neutral output of a parser.
type ParsedMessage struct {
	Format        string
	FormatVersion string
	EventID       string
	PatientMRN    string
	EventType     string
	Status        string
	EventTime     string
	EffectiveTime string
	Code          string
	CodeSystem    string
	CodeDisplay   string
	Value         string
	Unit          string
	// Unknown captures unrecognized segments / JSON fields. Values are
	// retained only until de-identification; the de-identified pipeline
	// never emits their raw values.
	Unknown []RawField
}

// Parser parses one raw message in a single format family.
type Parser interface {
	// Format returns the canonical format name, e.g. "HL7v2" / "FHIR-JSON".
	Format() string
	// Detect reports whether data looks like this parser's format.
	Detect(data []byte) bool
	// Parse validates and parses data, returning a RejectError on any
	// malformed, truncated or illegal input.
	Parse(data []byte) (*ParsedMessage, error)
}

// Registry holds the ordered set of available parsers.
type Registry struct {
	parsers []Parser
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{}
}

// Register adds a parser to the registry.
func (r *Registry) Register(p Parser) {
	r.parsers = append(r.parsers, p)
}

// Detect finds a parser for data; returns an UNSUPPORTED_FORMAT reject
// when nothing matches.
func (r *Registry) Detect(data []byte) (Parser, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, NewReject(ReasonEmptyPayload, "payload is empty")
	}
	for _, p := range r.parsers {
		if p.Detect(data) {
			return p, nil
		}
	}
	return nil, NewReject(ReasonUnsupportedFormat, "no parser matched payload")
}

// DefaultRegistry returns parsers for all built-in format families.
func DefaultRegistry() *Registry {
	r := NewRegistry()
	r.Register(NewHL7v2Parser())
	r.Register(NewFHIRJSONParser())
	return r
}

// Raw event type names produced by the parsers. Their string values are
// identical to the pipeline canonical types; pipeline mapping validates
// and normalizes them.
const (
	RawEventObservation = "Observation"
	RawEventCondition   = "Condition"
	RawEventEncounter   = "Encounter"
)
