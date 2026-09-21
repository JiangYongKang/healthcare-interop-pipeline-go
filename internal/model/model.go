// Package model defines the canonical (wire-format-independent) clinical
// event model produced by the normalization stage, plus the de-identified
// output record that is allowed to leave the pipeline.
package model

// Format identifies the upstream wire format family.
type Format string

const (
	FormatHL7v2    Format = "HL7v2"
	FormatFHIRJSON Format = "FHIR-JSON"
)

// Supported format versions.
//
//	HL7v2:     2.3, 2.5.1
//	FHIR JSON: DSTU2 (back-compat), R4 (primary), R5 (forward-compat:
//	           unknown properties are retained, never fatal)
const (
	HL7v23    = "2.3"
	HL7v251   = "2.5.1"
	FHIRDSTU2 = "DSTU2"
	FHIRR4    = "R4"
	FHIRR5    = "R5"
)

// SupportedFormatVersion returns true when the given format/version pair is
// accepted by the parser. Versions not listed here must be rejected with a
// distinguishable reason rather than parsed with guessed semantics.
func SupportedFormatVersion(f Format, v string) bool {
	switch f {
	case FormatHL7v2:
		return v == HL7v23 || v == HL7v251
	case FormatFHIRJSON:
		return v == FHIRDSTU2 || v == FHIRR4 || v == FHIRR5
	default:
		return false
	}
}

// CanonicalEvent is the normalized, format-independent representation of an
// upstream message. Fields here still contain REAL values; de-identification
// is a later, explicit stage.
type CanonicalEvent struct {
	Format           Format
	Version          string
	EventID          string // idempotency key: HL7 MSH-10 / FHIR identifier
	MessageControlID string // HL7 MSH-10 equivalent trace id (== EventID for HL7)
	EventType        string
	Sequence         int64 // per-event source sequence (HL7 ZSN-1 / FHIR meta.seq); 0 when absent

	PatientMRN       string
	PatientMRNSys    string
	PatientName      string
	PatientGender    string
	PatientBirthDate string // YYYY-MM-DD, "" when not provided
	PatientContact   string // phone/email, "" when not provided

	EffectiveTime string // YYYY-MM-DDTHH:MM:SS
	Notes         string

	// OrderingKey is the deterministic ordering value for out-of-order
	// detection: source sequence when present, otherwise effective-time
	// epoch seconds.
	OrderingKey int64

	// Defaults lists the stable default semantics applied because an older
	// version omitted a field (e.g. "gender:UNK").
	Defaults []string

	// Unknowns captures unknown/new segments or JSON fields so forward
	// compatibility is traceable without failing parsing or misaligning columns.
	Unknowns []UnknownField
}

// UnknownField records an unrecognized segment or JSON property that was
// tolerated (and retained) rather than causing a parse failure.
type UnknownField struct {
	Location string `json:"location"` // e.g. "ZXY[0]" or "subject.foo"
	Kind     string `json:"kind"`     // "segment" | "json-property"
	SizeHint int    `json:"sizeHint"` // length of raw value (value never exported)
}

// RecordStatus is the lifecycle state of an accepted event record.
type RecordStatus string

const (
	StatusCurrent    RecordStatus = "current"
	StatusStale      RecordStatus = "stale"        // superseded by a newer version of the same event
	StatusOutOfOrder RecordStatus = "out-of-order" // arrived late; retained for traceability
)

// OutputRecord is the ONLY structure that may be persisted to the external
// sink. Every sensitive real value has been replaced by a stable pseudonym;
// the longitudinal linkage pseudonym keeps cross-record joins possible.
type OutputRecord struct {
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	Format    Format `json:"format"`
	Version   string `json:"version"`

	PatientPseudo string `json:"patientPseudo"` // per-patient stable longitudinal key
	MRNPseudo     string `json:"mrnPseudo"`
	NamePseudo    string `json:"namePseudo"`
	ContactPseudo string `json:"contactPseudo"`

	Gender        string       `json:"gender"`
	BirthDate     string       `json:"birthDate"`
	EffectiveTime string       `json:"effectiveTime"`
	Sequence      int64        `json:"sequence"`
	OrderingKey   int64        `json:"orderingKey"` // canonical ordering (effective-time key)
	Status        RecordStatus `json:"status"`
	SupersededBy  string       `json:"supersededBy,omitempty"`

	Notes    string         `json:"notes,omitempty"`
	Unknowns []UnknownField `json:"unknowns,omitempty"`
	Defaults []string       `json:"defaults,omitempty"` // stable defaults for old-version omissions
	// Fingerprint is internal (duplicate/clash discrimination) and never
	// part of the external contract.
	Fingerprint string `json:"-"`
	IngestedAt  string `json:"ingestedAt"`
}
