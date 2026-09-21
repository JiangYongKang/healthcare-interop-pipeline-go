// Package parser decodes upstream wire messages (HL7v2 pipe-encoded and
// FHIR JSON) into a raw field set. Parsing covers ONLY structural/encoding
// concerns; business validation and default semantics live in normalize.
//
// Nothing is silently skipped: missing fields, illegal encodings and
// truncated structures each yield a distinct apperr code.
package parser

import (
	"bytes"
	"encoding/json"
	"os"
	"unicode/utf8"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/apperr"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/model"
)

// ParseFile reads and parses a local message file (sample upstream source).
func ParseFile(path string) (*RawEvent, *apperr.Error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, apperr.New(apperr.CodeTruncated, "").
			WithInternal("cannot read message file: " + err.Error())
	}
	return Parse(raw)
}

// RawEvent is the decoded-but-not-yet-normalized content of one message.
type RawEvent struct {
	Format  model.Format
	Version string

	EventID          string
	MessageControlID string
	EventType        string
	Sequence         int64
	HasSequence      bool

	PatientMRN       string
	PatientMRNSys    string
	PatientName      string
	PatientGender    string
	PatientBirthDate string
	PatientContact   string

	EffectiveTime string
	Notes         string

	Unknowns []model.UnknownField
}

// BestEffortID extracts a likely message identifier from a rejected payload
// purely for traceability: HL7 MSH-10 or a JSON "id"/control-id property.
// It never errors and never validates; it returns "" when nothing is found.
func BestEffortID(raw []byte) string {
	t := bytes.TrimSpace(raw)
	if bytes.HasPrefix(t, []byte("MSH")) {
		line := t
		if i := bytes.IndexAny(t, "\r\n"); i >= 0 {
			line = t[:i]
		}
		f := bytes.Split(line, []byte("|"))
		if len(f) > 9 {
			return string(bytes.TrimSpace(f[9]))
		}
		return ""
	}
	if len(t) > 0 && t[0] == '{' {
		var probe struct {
			ID      string `json:"id"`
			Message struct {
				ID string `json:"id"`
			} `json:"message"`
			Identifier []struct {
				Use   string `json:"use"`
				Value string `json:"value"`
			} `json:"identifier"`
		}
		if json.Unmarshal(t, &probe) == nil {
			if probe.ID != "" {
				return probe.ID
			}
			if probe.Message.ID != "" {
				return probe.Message.ID
			}
			for _, idf := range probe.Identifier {
				if idf.Use == "event-id" && idf.Value != "" {
					return idf.Value
				}
			}
		}
	}
	return ""
}

// Parse detects the wire format and decodes one message. It performs only
// structural decoding; business rules are enforced later.
func Parse(raw []byte) (*RawEvent, *apperr.Error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, apperr.New(apperr.CodeTruncated, "").
			WithInternal("empty or whitespace-only payload")
	}
	if !utf8.Valid(trimmed) {
		return nil, apperr.New(apperr.CodeIllegalEncoding, "").
			WithInternal("payload is not valid UTF-8")
	}
	if hasIllegalControlByte(trimmed) {
		return nil, apperr.New(apperr.CodeIllegalEncoding, "").
			WithInternal("payload contains an illegal control byte")
	}

	f, ok := detectFormat(trimmed)
	if !ok {
		return nil, apperr.New(apperr.CodeUnknownFormat, "").
			WithInternal("neither MSH-prefixed HL7v2 nor JSON object")
	}
	switch f {
	case model.FormatHL7v2:
		return parseHL7(trimmed)
	case model.FormatFHIRJSON:
		return parseFHIRJSON(trimmed)
	default:
		return nil, apperr.New(apperr.CodeUnknownFormat, "")
	}
}

// hasIllegalControlByte rejects C0 controls other than tab/newline/CR.
// HL7 segment terminators (CR) and JSON whitespace are permitted.
func hasIllegalControlByte(b []byte) bool {
	for _, c := range b {
		switch c {
		case '\t', '\n', '\r':
			continue
		}
		if c < 0x20 {
			return true
		}
	}
	return false
}

// detectFormat distinguishes the supported wire families.
// HL7v2 begins with the MSH segment; FHIR JSON begins with '{'.
func detectFormat(raw []byte) (model.Format, bool) {
	if bytes.HasPrefix(raw, []byte("MSH")) {
		return model.FormatHL7v2, true
	}
	if raw[0] == '{' {
		return model.FormatFHIRJSON, true
	}
	return "", false
}
