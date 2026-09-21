package pipeline

import (
	"fmt"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/formats"
)

// Normalizer converts parsed format-specific messages into canonical
// records, applying documented version-specific default semantics.
type Normalizer struct{}

func NewNormalizer() *Normalizer { return &Normalizer{} }

// Normalize validates and maps a parsed message. Parser rejects are
// structural/format problems; failures here are business validation
// problems (unknown enum values that cannot be normalized, etc.).
func (n *Normalizer) Normalize(p *formats.ParsedMessage) (*Record, error) {
	if p == nil {
		return nil, externalError(ErrValidationError, "empty parsed message", string(formats.ReasonMissingEventType), "", "nil parsed message")
	}
	eventType, err := canonicalEventType(p.EventType)
	if err != nil {
		return nil, externalError(ErrValidationError, "unknown event type", string(formats.ReasonUnsupportedEventType), p.EventID, err.Error())
	}
	status := n.normalizeStatus(p)
	if status == "" {
		return nil, externalError(ErrValidationError, "unmappable status", string(formats.ReasonInvalidStatus), p.EventID,
			fmt.Sprintf("eventType=%s status=%q version=%s", eventType, p.Status, p.FormatVersion))
	}
	system := n.normalizeCodeSystem(p)
	unit := p.Unit
	if unit == "" {
		unit = defaultUnit(p)
	}
	effective := p.EffectiveTime
	if effective == "" {
		// Stable default semantics: effective time falls back to the
		// event time when the source version omitted it.
		effective = p.EventTime
	}
	return &Record{
		EventID:       p.EventID,
		EventType:     eventType,
		PatientRef:    p.PatientMRN, // real MRN; replaced by de-identify
		SourceFormat:  p.Format,
		SourceVersion: p.FormatVersion,
		Status:        status,
		EventTime:     p.EventTime,
		EffectiveTime: effective,
		Code:          p.Code,
		CodeSystem:    system,
		CodeDisplay:   p.CodeDisplay,
		Value:         p.Value,
		Unit:          unit,
		UnknownFields: rawFieldsToMap(p.Unknown),
	}, nil
}

func rawFieldsToMap(fs []formats.RawField) map[string]string {
	if len(fs) == 0 {
		return nil
	}
	m := make(map[string]string, len(fs))
	for _, f := range fs {
		if existing, ok := m[f.Path]; ok {
			m[f.Path] = existing + "|" + f.Value
		} else {
			m[f.Path] = f.Value
		}
	}
	return m
}

func canonicalEventType(raw string) (string, error) {
	switch raw {
	case pipelineEventObservation:
		return EventObservation, nil
	case pipelineEventCondition:
		return EventCondition, nil
	case pipelineEventEncounter:
		return EventEncounter, nil
	default:
		return "", fmt.Errorf("unknown event type %q", raw)
	}
}

const (
	pipelineEventObservation = "Observation"
	pipelineEventCondition   = "Condition"
	pipelineEventEncounter   = "Encounter"
)

// normalizeStatus maps raw format statuses to canonical values and
// applies version-specific defaults when the source omitted status.
func (n *Normalizer) normalizeStatus(p *formats.ParsedMessage) string {
	raw := p.Status
	switch p.Format {
	case formats.HL7v2Name:
		switch raw {
		case "F", "":
			// Version default: 2.3 treated an empty OBX-11 as
			// "preliminary"; 2.5.1 treats it as "final".
			if raw == "" && p.FormatVersion == formats.HL7v23 {
				return StatusPreliminary
			}
			return StatusFinal
		case "P":
			return StatusPreliminary
		case "C":
			return StatusAmended
		case "X":
			return StatusEnteredInError
		}
		return ""
	case formats.FHIRJSONName:
		switch p.EventType {
		case pipelineEventObservation:
			switch raw {
			case "final", "corrected":
				return StatusFinal
			case "amended":
				return StatusAmended
			case "cancelled":
				return StatusEnteredInError
			case "preliminary", "registered", "unknown", "":
				return StatusPreliminary
			}
			return ""
		case pipelineEventCondition:
			switch raw {
			case "active", "recurrence", "":
				return StatusActive
			case "remission", "inactive":
				// Default semantics for older STU3 messages that used
				// "inactive" where R4 uses "resolved".
				if p.FormatVersion == formats.FHIRSTU3 {
					return StatusResolved
				}
				return StatusResolved
			case "resolved":
				return StatusResolved
			case "entered-in-error":
				return StatusEnteredInError
			}
			return ""
		case pipelineEventEncounter:
			switch raw {
			case "finished", "":
				return StatusFinal
			case "triaged", "arrived", "in-progress", "onleave", "planned":
				return StatusActive
			case "cancelled":
				return StatusEnteredInError
			}
			return ""
		}
	}
	return ""
}

func (n *Normalizer) normalizeCodeSystem(p *formats.ParsedMessage) string {
	if p.CodeSystem != "" {
		return p.CodeSystem
	}
	// Stable default coding system per format/version.
	switch p.Format {
	case formats.HL7v2Name:
		if p.FormatVersion == formats.HL7v23 {
			return "L" // 2.3 local codes
		}
		return "LN" // 2.5.1: LOINC preferred for OBX
	case formats.FHIRJSONName:
		return "http://loinc.org"
	}
	return "unknown"
}

func defaultUnit(p *formats.ParsedMessage) string {
	switch p.Format {
	case formats.HL7v2Name:
		return "unspecified"
	case formats.FHIRJSONName:
		// R4 clarified Quantity.unit; STU3 allowed omission, which we
		// interpret as a unitless quantity.
		if p.FormatVersion == formats.FHIRSTU3 {
			return "1"
		}
		return "unspecified"
	}
	return "unspecified"
}
