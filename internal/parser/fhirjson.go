package parser

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/apperr"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/model"
)

// fhirObject is a decoded JSON object keyed by raw property values. Unknown
// properties are kept as raw bytes and reported rather than silently ignored.
type fhirObject map[string]json.RawMessage

var knownTopLevel = map[string]bool{
	"resourceType": true, "fhirVersion": true, "id": true,
	"identifier": true, "event": true, "subject": true,
	"effectiveDateTime": true, "notes": true, "meta": true,
}

var knownEvent = map[string]bool{"type": true, "sequence": true}
var knownSubject = map[string]bool{
	"identifier": true, "name": true, "gender": true,
	"birthDate": true, "telecom": true,
}

func parseFHIRJSON(raw []byte) (*RawEvent, *apperr.Error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var top fhirObject
	if err := d.Decode(&top); err != nil {
		if isTruncatedJSON(err) {
			return nil, apperr.New(apperr.CodeTruncated, "").
				WithInternal("JSON document is truncated")
		}
		return nil, apperr.New(apperr.CodeMalformedJSON, "").
			WithInternal("JSON syntax error: " + err.Error())
	}
	// Reject concatenated / trailing documents (ambiguous input).
	var extra json.RawMessage
	if err := d.Decode(&extra); err != io.EOF {
		return nil, apperr.New(apperr.CodeMalformedJSON, "").
			WithInternal("trailing data after JSON document")
	}

	version, ae := fhirVersion(top)
	if ae != nil {
		return nil, ae
	}
	ev := &RawEvent{Format: model.FormatFHIRJSON, Version: version}

	for k, rv := range top {
		if !knownTopLevel[k] {
			ev.Unknowns = append(ev.Unknowns, model.UnknownField{
				Location: k, Kind: "json-property", SizeHint: len(rv)})
		}
	}

	var resourceType string
	if rt, ok := top["resourceType"]; ok {
		s, ok := jsonString(rt)
		if !ok {
			return nil, apperr.New(apperr.CodeInvalidValue, "").
				WithInternal("resourceType is not a string")
		}
		resourceType = s
	}

	// Event id: identifier[use=event-id] | first identifier | id.
	if id, _, ok, ae := pickEventID(top["identifier"]); ae != nil {
		return nil, ae
	} else if ok {
		ev.EventID, ev.MessageControlID = id, id
	}
	if ev.EventID == "" {
		if id, ok := jsonString(top["id"]); ok {
			ev.EventID, ev.MessageControlID = id, id
		}
	}

	// event: {type, sequence}
	if rv, ok := top["event"]; ok {
		var eo fhirObject
		if err := json.Unmarshal(rv, &eo); err != nil {
			return nil, apperr.New(apperr.CodeInvalidValue, "").
				WithInternal("event is not a JSON object")
		}
		for k, rawK := range eo {
			if !knownEvent[k] {
				ev.Unknowns = append(ev.Unknowns, model.UnknownField{
					Location: "event." + k, Kind: "json-property", SizeHint: len(rawK)})
			}
		}
		if t, ok := jsonString(eo["type"]); ok {
			ev.EventType = t
		}
		if rvSeq, ok := eo["sequence"]; ok {
			n, err := decodeInt(rvSeq)
			if err != nil {
				return nil, apperr.New(apperr.CodeInvalidValue, "").
					WithInternal("event.sequence: " + err.Error())
			}
			ev.Sequence, ev.HasSequence = n, true
		}
	}
	// Business event type falls back to the FHIR resourceType so every
	// accepted record has a stable, non-empty type.
	if ev.EventType == "" {
		ev.EventType = resourceType
	}

	if rv, ok := top["subject"]; ok {
		if ae := applySubject(ev, rv); ae != nil {
			return nil, ae
		}
	}

	if t, ok := jsonString(top["effectiveDateTime"]); ok {
		ev.EffectiveTime = t
	}
	if t, ok := jsonString(top["notes"]); ok {
		ev.Notes = t
	}
	return ev, nil
}

func isTruncatedJSON(err error) bool {
	if err == io.ErrUnexpectedEOF {
		return true
	}
	if se, ok := err.(*json.SyntaxError); ok {
		return strings.Contains(se.Error(), "unexpected end of JSON input")
	}
	return strings.Contains(err.Error(), "unexpected end of JSON input")
}

func fhirVersion(top fhirObject) (string, *apperr.Error) {
	v, ok := jsonString(top["fhirVersion"])
	if !ok || strings.TrimSpace(v) == "" {
		return "", apperr.New(apperr.CodeTruncated, "").
			WithInternal("fhirVersion is missing")
	}
	switch {
	case v == "DSTU2" || strings.HasPrefix(v, "1."):
		v = model.FHIRDSTU2
	case v == "R4" || strings.HasPrefix(v, "4."):
		v = model.FHIRR4
	case v == "R5" || strings.HasPrefix(v, "5."):
		v = model.FHIRR5
	default:
		return "", apperr.New(apperr.CodeUnsupportedVersion, "").
			WithInternal("FHIR version not supported: " + v)
	}
	return v, nil
}

// pickEventID accepts {"use":"event-id","value":"..."} (or array form),
// plain {"value":"..."}, or a JSON string identifier.
func pickEventID(rv json.RawMessage) (id string, system string, ok bool, ae *apperr.Error) {
	if len(rv) == 0 {
		return "", "", false, nil
	}
	var single fhirObject
	if err := json.Unmarshal(rv, &single); err == nil {
		val, _ := jsonString(single["value"])
		if val == "" {
			return "", "", false, nil
		}
		use, _ := jsonString(single["use"])
		if use != "" && use != "event-id" {
			return "", "", false, nil
		}
		sys, _ := jsonString(single["system"])
		return val, sys, true, nil
	}
	var arr []fhirObject
	if err := json.Unmarshal(rv, &arr); err != nil {
		return "", "", false, apperr.New(apperr.CodeInvalidValue, "").
			WithInternal("identifier is neither object nor array")
	}
	var fallback string
	var fallbackSys string
	for _, o := range arr {
		val, _ := jsonString(o["value"])
		if val == "" {
			continue
		}
		if use, _ := jsonString(o["use"]); use == "event-id" {
			sys, _ := jsonString(o["system"])
			return val, sys, true, nil
		}
		if fallback == "" {
			fallback = val
			fallbackSys, _ = jsonString(o["system"])
		}
	}
	if fallback != "" {
		return fallback, fallbackSys, true, nil
	}
	return "", "", false, nil
}

func applySubject(ev *RawEvent, rv json.RawMessage) *apperr.Error {
	var sub fhirObject
	if err := json.Unmarshal(rv, &sub); err != nil {
		return apperr.New(apperr.CodeInvalidValue, "").
			WithInternal("subject is not a JSON object")
	}
	for k, rawK := range sub {
		if !knownSubject[k] {
			ev.Unknowns = append(ev.Unknowns, model.UnknownField{
				Location: "subject." + k, Kind: "json-property", SizeHint: len(rawK)})
		}
	}
	if ido, ok := sub["identifier"]; ok {
		var o fhirObject
		if err := json.Unmarshal(ido, &o); err != nil {
			return apperr.New(apperr.CodeInvalidValue, "").
				WithInternal("subject.identifier is not an object")
		}
		ev.PatientMRN, _ = jsonString(o["value"])
		ev.PatientMRNSys, _ = jsonString(o["system"])
	}
	if nr, ok := sub["name"]; ok {
		ev.PatientName = fhirName(nr)
	}
	if g, ok := jsonString(sub["gender"]); ok {
		ev.PatientGender = g
	}
	if b, ok := jsonString(sub["birthDate"]); ok {
		ev.PatientBirthDate = b
	}
	if tc, ok := sub["telecom"]; ok {
		var arr []fhirObject
		if err := json.Unmarshal(tc, &arr); err != nil {
			return apperr.New(apperr.CodeInvalidValue, "").
				WithInternal("subject.telecom is not an array")
		}
		for _, o := range arr {
			if v, ok := jsonString(o["value"]); ok && v != "" {
				ev.PatientContact = v
				break
			}
		}
	}
	return nil
}

// fhirName renders HumanName as "given... family" (repeats use first entry).
func fhirName(rv json.RawMessage) string {
	var single fhirObject
	if err := json.Unmarshal(rv, &single); err == nil {
		return joinHumanName(single)
	}
	var arr []fhirObject
	if err := json.Unmarshal(rv, &arr); err != nil || len(arr) == 0 {
		return ""
	}
	return joinHumanName(arr[0])
}

func joinHumanName(o fhirObject) string {
	var given []string
	if g, ok := o["given"]; ok {
		_ = json.Unmarshal(g, &given)
	}
	// FHIR R4/R5 use family:string; DSTU2 uses family:[string].
	var family string
	if f, ok := o["family"]; ok {
		if s, ok := jsonString(f); ok {
			family = s
		} else {
			var parts []string
			if err := json.Unmarshal(f, &parts); err == nil && len(parts) > 0 {
				family = parts[0]
			}
		}
	}
	name := strings.Join(given, " ")
	if family != "" {
		if name != "" {
			name += " "
		}
		name += family
	}
	return name
}

func jsonString(rv json.RawMessage) (string, bool) {
	var s string
	if err := json.Unmarshal(rv, &s); err != nil {
		return "", false
	}
	return s, true
}

func decodeInt(rv json.RawMessage) (int64, error) {
	var n json.Number
	if err := json.Unmarshal(rv, &n); err != nil {
		return 0, err
	}
	return n.Int64()
}
