package formats

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Supported JSON FHIR dialect versions.
const (
	FHIRJSONName = "FHIR-JSON"
	FHIRR4       = "R4"
	FHIRSTU3     = "STU3"
)

// FHIRJSONParser handles the simplified JSON FHIR dialect used by the
// local sample upstream. Supported versions: STU3, R4.
//
// Accepted payloads are either a bare resource object
// {"resourceType": ...} or an envelope
// {"fhirVersion": "4.0", "resource": {...}}. Unknown JSON fields never
// fail parsing; they are captured as RawField leaves.
type FHIRJSONParser struct{}

func NewFHIRJSONParser() *FHIRJSONParser { return &FHIRJSONParser{} }

func (p *FHIRJSONParser) Format() string { return FHIRJSONName }

func (p *FHIRJSONParser) Detect(data []byte) bool {
	trimmed := bytes.TrimSpace(bytes.TrimPrefix(data, []byte("\xef\xbb\xbf")))
	return len(trimmed) > 0 && trimmed[0] == '{'
}

// decodeStrict parses JSON while rejecting duplicate object keys,
// truncation and trailing data. Numbers use json.Number to avoid loss.
func decodeStrict(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	v, err := decodeValue(dec)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF ||
			strings.Contains(err.Error(), "EOF") ||
			strings.Contains(err.Error(), "unexpected end of JSON input") {
			return nil, NewReject(ReasonStructureTruncated, "JSON structure is truncated: %v", err)
		}
		return nil, NewReject(ReasonJSONInvalid, "invalid JSON: %v", err)
	}
	if dec.More() {
		return nil, NewReject(ReasonJSONInvalid, "trailing data after JSON document")
	}
	return v, nil
}

func decodeValue(dec *json.Decoder) (any, error) {
	t, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch tv := t.(type) {
	case json.Delim:
		switch tv {
		case '{':
			m := map[string]any{}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key := kt.(string)
				if _, exists := m[key]; exists {
					return nil, NewReject(ReasonJSONInvalid, "duplicate JSON key %q", key)
				}
				v, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				m[key] = v
			}
			if _, err := dec.Token(); err != nil { // closing brace
				return nil, err
			}
			return m, nil
		case '[':
			arr := []any{}
			for dec.More() {
				v, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				arr = append(arr, v)
			}
			if _, err := dec.Token(); err != nil { // closing bracket
				return nil, err
			}
			return arr, nil
		}
	}
	return t, nil
}

func asMap(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

func mapField(m map[string]any, key string) (map[string]any, bool) {
	if v, ok := m[key]; ok {
		return asMap(v)
	}
	return nil, false
}

func strField(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// parseFHIRTime accepts RFC3339 date-times and plain YYYY-MM-DD dates,
// returning RFC3339 UTC.
func parseFHIRTime(raw string) (string, bool) {
	if raw == "" {
		return "", false
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC().Format(time.RFC3339), true
	}
	if t, err := time.Parse("2006-01-02", raw); err == nil {
		return t.UTC().Format(time.RFC3339), true
	}
	return "", false
}

func parseFHIRVersion(raw string) (string, error) {
	switch strings.TrimSpace(raw) {
	case "", "4.0", "4.0.1", "R4":
		return FHIRR4, nil
	case "3.0", "3.0.1", "3.0.2", "STU3":
		return FHIRSTU3, nil
	default:
		return "", NewReject(ReasonUnsupportedVersion, "unsupported FHIR version %q", raw)
	}
}

var fhirStatuses = map[string]map[string]bool{
	RawEventObservation: mapSet("preliminary", "final", "amended", "corrected", "cancelled", "entered-in-error", "registered", "unknown"),
	RawEventCondition:   mapSet("active", "recurrence", "inactive", "remission", "resolved", "entered-in-error"),
	RawEventEncounter:   mapSet("planned", "arrived", "triaged", "in-progress", "onleave", "finished", "cancelled", "entered-in-error"),
}

func mapSet(vs ...string) map[string]bool {
	m := make(map[string]bool, len(vs))
	for _, v := range vs {
		m[v] = true
	}
	return m
}

func collectUnknown(prefix string, v any, out *[]RawField) {
	switch tv := v.(type) {
	case map[string]any:
		for k, sub := range tv {
			collectUnknown(joinPath(prefix, k), sub, out)
		}
	case []any:
		for i, sub := range tv {
			collectUnknown(prefix+"["+strconv.Itoa(i)+"]", sub, out)
		}
	case string:
		if tv != "" {
			*out = append(*out, RawField{Path: prefix, Value: tv})
		}
	case json.Number:
		*out = append(*out, RawField{Path: prefix, Value: tv.String()})
	case bool:
		*out = append(*out, RawField{Path: prefix, Value: strconv.FormatBool(tv)})
	}
}

func joinPath(a, b string) string {
	if a == "" {
		return b
	}
	return a + "." + b
}

func (p *FHIRJSONParser) Parse(data []byte) (*ParsedMessage, error) {
	// Strip an optional UTF-8 BOM.
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))
	if !utf8.Valid(data) {
		return nil, NewReject(ReasonEncodingInvalid, "payload is not valid UTF-8")
	}
	doc, err := decodeStrict(data)
	if err != nil {
		return nil, err
	}
	top, ok := asMap(doc)
	if !ok {
		return nil, NewReject(ReasonEmptyEnvelope, "top-level JSON value must be an object")
	}

	version := ""
	if envV := strField(top, "fhirVersion"); envV != "" {
		version, err = parseFHIRVersion(envV)
		if err != nil {
			return nil, err
		}
	}
	res := top
	if _, hasResource := top["resource"]; hasResource {
		rm, ok := mapField(top, "resource")
		if !ok {
			return nil, NewReject(ReasonEmptyEnvelope, "envelope \"resource\" must be an object")
		}
		res = rm
	} else if rt := strField(top, "resourceType"); rt == "" {
		return nil, NewReject(ReasonEmptyEnvelope, "neither resourceType nor envelope resource found")
	}
	if meta, ok := mapField(res, "meta"); ok {
		if mv := strField(meta, "fhirVersion"); mv != "" && version == "" {
			version, err = parseFHIRVersion(mv)
			if err != nil {
				return nil, err
			}
		}
	}
	if version == "" {
		version = FHIRR4 // documented stable default
	}

	out := &ParsedMessage{Format: FHIRJSONName, FormatVersion: version}

	eventType := strField(res, "resourceType")
	switch eventType {
	case RawEventObservation, RawEventCondition, RawEventEncounter:
		out.EventType = eventType
	case "":
		return nil, NewReject(ReasonMissingEventType, "resource.resourceType is required")
	default:
		return nil, NewReject(ReasonUnsupportedEventType, "unsupported resourceType %q", eventType)
	}

	out.EventID = strField(res, "id")
	if out.EventID == "" {
		return nil, NewReject(ReasonMissingEventID, "%s.id is empty", eventType)
	}

	if subject, ok := mapField(res, "subject"); ok {
		ref := strField(subject, "reference")
		switch {
		case ref == "":
			return nil, NewReject(ReasonMissingPatientID, "subject.reference is empty (eventId=%s)", out.EventID)
		case !strings.HasPrefix(ref, "Patient/"):
			return nil, NewReject(ReasonInvalidComponent, "subject.reference %q must be Patient/<mrn> (eventId=%s)", ref, out.EventID)
		}
		out.PatientMRN = strings.TrimPrefix(ref, "Patient/")
	} else {
		return nil, NewReject(ReasonMissingPatientID, "subject is missing (eventId=%s)", out.EventID)
	}

	if status := strField(res, "status"); status != "" {
		if !fhirStatuses[out.EventType][status] {
			return nil, NewReject(ReasonInvalidStatus, "%s.status %q is not a valid %s status (eventId=%s)", eventType, status, version, out.EventID)
		}
		out.Status = status
	}

	switch out.EventType {
	case RawEventObservation:
		if err := p.parseObservation(res, out); err != nil {
			return nil, err
		}
	case RawEventCondition:
		if err := p.parseCondition(res, out); err != nil {
			return nil, err
		}
	case RawEventEncounter:
		if err := p.parseEncounter(res, out); err != nil {
			return nil, err
		}
	}

	// Capture unknown top-level fields (and unknown envelope fields) as
	// hashed-later leaves; known subtrees are consumed explicitly above.
	for k, v := range res {
		if !knownResourceFields[k] {
			collectUnknown(k, v, &out.Unknown)
		}
	}
	return out, nil
}

func firstCodingCode(m map[string]any) (code, system, display string, hasCoding bool) {
	codeable, ok := mapField(m, "code")
	if !ok {
		return "", "", "", false
	}
	codings, ok := codeable["coding"].([]any)
	if !ok || len(codings) == 0 {
		return "", "", "", true
	}
	first, ok := asMap(codings[0])
	if !ok {
		return "", "", "", true
	}
	return strField(first, "code"), strField(first, "system"), strField(first, "display"), true
}

func (p *FHIRJSONParser) parseObservation(res map[string]any, out *ParsedMessage) error {
	code, system, display, hasCoding := firstCodingCode(res)
	if !hasCoding {
		return NewReject(ReasonMissingCode, "code.coding is missing (eventId=%s)", out.EventID)
	}
	if code == "" {
		return NewReject(ReasonMissingCode, "code.coding[0].code is empty (eventId=%s)", out.EventID)
	}
	out.Code, out.CodeSystem, out.CodeDisplay = code, system, display
	if q, ok := mapField(res, "valueQuantity"); ok {
		if n, ok := q["value"].(json.Number); ok {
			out.Value = n.String()
		}
		out.Unit = strField(q, "unit")
	}
	if eff := strField(res, "effectiveDateTime"); eff != "" {
		ts, ok := parseFHIRTime(eff)
		if !ok {
			return NewReject(ReasonInvalidTime, "effectiveDateTime %q is not valid RFC3339/date (eventId=%s)", eff, out.EventID)
		}
		out.EffectiveTime = ts
	}
	out.EventTime = out.EffectiveTime
	if issued := strField(res, "issued"); issued != "" {
		if ts, ok := parseFHIRTime(issued); ok && out.EventTime == "" {
			out.EventTime = ts
		}
	}
	if out.EventTime == "" {
		return NewReject(ReasonMissingEventTime, "effectiveDateTime or issued is required (eventId=%s)", out.EventID)
	}
	return nil
}

func (p *FHIRJSONParser) parseCondition(res map[string]any, out *ParsedMessage) error {
	code, system, display, hasCoding := firstCodingCode(res)
	if !hasCoding {
		return NewReject(ReasonMissingCode, "code.coding is missing (eventId=%s)", out.EventID)
	}
	if code == "" {
		return NewReject(ReasonMissingCode, "code.coding[0].code is empty (eventId=%s)", out.EventID)
	}
	out.Code, out.CodeSystem, out.CodeDisplay = code, system, display
	if onset := strField(res, "onsetDateTime"); onset != "" {
		ts, ok := parseFHIRTime(onset)
		if !ok {
			return NewReject(ReasonInvalidTime, "onsetDateTime %q is not valid (eventId=%s)", onset, out.EventID)
		}
		out.EffectiveTime = ts
		out.EventTime = ts
	}
	if recorded := strField(res, "recordedDate"); recorded != "" {
		if ts, ok := parseFHIRTime(recorded); ok {
			out.EventTime = ts
			if out.EffectiveTime == "" {
				out.EffectiveTime = ts
			}
		} else if out.EventTime == "" {
			return NewReject(ReasonInvalidTime, "recordedDate %q is not valid (eventId=%s)", recorded, out.EventID)
		}
	}
	if out.EventTime == "" {
		return NewReject(ReasonMissingEventTime, "onsetDateTime or recordedDate is required (eventId=%s)", out.EventID)
	}
	return nil
}

func (p *FHIRJSONParser) parseEncounter(res map[string]any, out *ParsedMessage) error {
	class, ok := mapField(res, "class")
	if !ok {
		return NewReject(ReasonMissingCode, "class is missing (eventId=%s)", out.EventID)
	}
	code := strField(class, "code")
	if code == "" {
		return NewReject(ReasonMissingCode, "class.code is empty (eventId=%s)", out.EventID)
	}
	out.Code = code
	if system := strField(class, "system"); system != "" {
		out.CodeSystem = system
	} else if out.CodeSystem == "" {
		out.CodeSystem = "http://terminology.hl7.org/CodeSystem/v3-ActCode"
	}
	out.CodeDisplay = strField(class, "display")
	if period, ok := mapField(res, "period"); ok {
		if start := strField(period, "start"); start != "" {
			ts, ok := parseFHIRTime(start)
			if !ok {
				return NewReject(ReasonInvalidTime, "period.start %q is not valid (eventId=%s)", start, out.EventID)
			}
			out.EffectiveTime = ts
			out.EventTime = ts
		}
	}
	if out.EventTime == "" {
		return NewReject(ReasonMissingEventTime, "period.start is required (eventId=%s)", out.EventID)
	}
	return nil
}

// knownResourceFields lists resource top-level fields consumed by the
// parser; any sibling is retained (then de-identified) as unknown.
var knownResourceFields = map[string]bool{
	"resourceType": true, "id": true, "meta": true, "status": true,
	"subject": true, "code": true, "valueQuantity": true,
	"effectiveDateTime": true, "issued": true,
	"onsetDateTime": true, "recordedDate": true,
	"class": true, "period": true,
}
