// Package pipeline implements a local clinical data interoperability
// ingestion pipeline: parse -> normalize -> de-identify -> persist.
package pipeline

// Canonical event types shared across all upstream formats and versions.
const (
	EventObservation = "Observation"
	EventCondition   = "Condition"
	EventEncounter   = "Encounter"
)

// Canonical status values produced by format-specific normalization.
const (
	StatusFinal          = "final"
	StatusPreliminary    = "preliminary"
	StatusAmended        = "amended"
	StatusActive         = "active"
	StatusResolved       = "resolved"
	StatusEnteredInError = "entered-in-error"
)

// SchemaVersion is bumped whenever the canonical record layout changes.
const SchemaVersion = "1.0"

// Record is the version-independent, de-identified canonical representation
// of one clinical event accepted by the pipeline.
type Record struct {
	EventID       string            `json:"eventId"`
	EventType     string            `json:"eventType"`
	PatientRef    string            `json:"patientRef"`
	SourceFormat  string            `json:"sourceFormat"`
	SourceVersion string            `json:"sourceVersion"`
	Status        string            `json:"status"`
	EventTime     string            `json:"eventTime"`
	Code          string            `json:"code"`
	CodeSystem    string            `json:"codeSystem"`
	CodeDisplay   string            `json:"codeDisplay,omitempty"`
	Value         string            `json:"value,omitempty"`
	Unit          string            `json:"unit,omitempty"`
	EffectiveTime string            `json:"effectiveTime,omitempty"`
	UnknownFields map[string]string `json:"unknownFields,omitempty"`
}
