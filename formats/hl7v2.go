package formats

import (
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// Supported HL7 v2 dialect versions.
const (
	HL7v2Name    = "HL7v2"
	HL7v23       = "2.3"
	HL7v251      = "2.5.1"
	hl7Encodings = "^~\\&"
)

// HL7v2Parser handles the simplified pipe-encoded HL7 v2.x dialect used
// by the local sample upstream. Supported versions: 2.3, 2.5.1.
//
// Required segments: MSH (first), PID, OBX. Supported message types
// (MSH-9): ORU^R01 (Observation), PPR^PC1 (Condition), ADT^A04
// (Encounter). Unknown segments are captured, never fatal.
type HL7v2Parser struct{}

func NewHL7v2Parser() *HL7v2Parser { return &HL7v2Parser{} }

func (p *HL7v2Parser) Format() string { return HL7v2Name }

func (p *HL7v2Parser) Detect(data []byte) bool {
	s := strings.TrimSpace(string(data))
	if len(s) < 3 {
		return false
	}
	return strings.HasPrefix(s, "MSH")
}

// hl7Timestamp matches YYYYMMDD[HH[MM[SS]]][.frac][±HHMM].
var hl7Timestamp = regexp.MustCompile(`^\d{8}(\d{2})?(\d{2})?(\d{2})?(\.\d+)?([+-]\d{4})?$`)

// parseHL7Time validates an HL7 timestamp and returns RFC3339 UTC.
func parseHL7Time(raw string) (string, bool) {
	if raw == "" {
		return "", false
	}
	if !hl7Timestamp.MatchString(raw) {
		return "", false
	}
	// Strip fraction and timezone for the simple, deterministic local
	// normalization; dates are emitted at second precision in UTC.
	core := raw
	if i := strings.IndexAny(core, ".+-"); i >= 0 {
		core = core[:i]
	}
	layout := "20060102"
	switch len(core) {
	case 8:
	case 10:
		layout = "2006010215"
	case 12:
		layout = "200601021504"
	case 14:
		layout = "20060102150405"
	default:
		return "", false
	}
	t, err := time.Parse(layout, core)
	if err != nil {
		return "", false
	}
	return t.UTC().Format(time.RFC3339), true
}

// decodeEscapes resolves the five standard HL7 escape sequences. Any
// unknown or unterminated escape is an encoding error.
func decodeEscapes(s string) (string, error) {
	if !strings.ContainsRune(s, '\\') {
		return s, nil
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		c := s[i]
		if c != '\\' {
			b.WriteByte(c)
			i++
			continue
		}
		j := strings.IndexByte(s[i+1:], '\\')
		if j < 0 {
			return "", NewReject(ReasonEncodingInvalid, "unterminated escape sequence in %q", s)
		}
		seq := s[i+1 : i+1+j]
		switch seq {
		case "F":
			b.WriteByte('|')
		case "S":
			b.WriteByte('^')
		case "T":
			b.WriteByte('&')
		case "R":
			b.WriteByte('~')
		case "E":
			b.WriteByte('\\')
		default:
			return "", NewReject(ReasonEncodingInvalid, "unknown escape sequence \\%s\\", seq)
		}
		i = i + 1 + j + 1
	}
	return b.String(), nil
}

// splitSegments splits on CR, tolerating CRLF and bare LF.
func splitSegments(data []byte) []string {
	s := string(data)
	var segs []string
	for _, line := range strings.Split(s, "\r") {
		line = strings.TrimSuffix(line, "\n")
		if line != "" {
			segs = append(segs, line)
		}
	}
	return segs
}

// splitFields returns fields with their HL7 1-based index, including the
// special MSH-1 separator and MSH-2 encoding character fields.
func splitFields(seg string) []string {
	if strings.HasPrefix(seg, "MSH") {
		body := seg[3:] // begins with "|"
		parts := strings.Split(body, "|")
		fields := make([]string, 0, len(parts)+1)
		fields = append(fields, "MSH", "|")
		fields = append(fields, parts[1:]...)
		return fields
	}
	return strings.Split(seg, "|")
}

func component(fields []string, idx, comp int) string {
	if idx >= len(fields) {
		return ""
	}
	cs := strings.Split(fields[idx], "^")
	if comp-1 >= len(cs) {
		return ""
	}
	return cs[comp-1]
}

func (p *HL7v2Parser) Parse(data []byte) (*ParsedMessage, error) {
	if !utf8.Valid(data) {
		return nil, NewReject(ReasonEncodingInvalid, "payload is not valid UTF-8")
	}
	segments := splitSegments(data)
	if len(segments) == 0 {
		return nil, NewReject(ReasonEmptyPayload, "no segments found")
	}
	if !strings.HasPrefix(segments[0], "MSH|") && segments[0] != "MSH" {
		return nil, NewReject(ReasonStructureTruncated, "first segment must be MSH, got %q", segmentName(segments[0]))
	}

	var (
		msh, pid, obx []string
		unknown       []RawField
		sawMSH        bool
	)
	for _, seg := range segments {
		name := segmentName(seg)
		switch name {
		case "MSH":
			if sawMSH {
				return nil, NewReject(ReasonStructureTruncated, "multiple MSH segments")
			}
			sawMSH = true
			msh = splitFields(seg)
		case "PID":
			pid = splitFields(seg)
		case "OBX":
			if obx != nil {
				return nil, NewReject(ReasonStructureTruncated, "multiple OBX segments")
			}
			obx = splitFields(seg)
		default:
			if !validSegmentName(name) {
				return nil, NewReject(ReasonStructureTruncated, "invalid segment identifier %q", name)
			}
			for i, v := range splitFields(seg)[1:] {
				if v == "" {
					continue
				}
				dec, err := decodeEscapes(v)
				if err != nil {
					return nil, err
				}
				unknown = append(unknown, RawField{Path: name + "-" + itoa(i+1), Value: dec})
			}
		}
	}
	if pid == nil {
		return nil, NewReject(ReasonStructureTruncated, "missing required PID segment")
	}
	if obx == nil {
		return nil, NewReject(ReasonStructureTruncated, "missing required OBX segment")
	}

	out := &ParsedMessage{Format: HL7v2Name, Unknown: unknown}

	// --- MSH ---
	if len(msh) < 3 || msh[2] == "" {
		return nil, NewReject(ReasonStructureTruncated, "MSH truncated before encoding characters")
	}
	if msh[2] != hl7Encodings {
		return nil, NewReject(ReasonEncodingInvalid, "unsupported encoding characters %q, want %q", msh[2], hl7Encodings)
	}
	version := component(msh, 12, 1)
	switch version {
	case HL7v23, HL7v251:
		out.FormatVersion = version
	case "":
		return nil, NewReject(ReasonUnsupportedVersion, "MSH-12 version id is required")
	default:
		return nil, NewReject(ReasonUnsupportedVersion, "unsupported HL7 version %q", version)
	}
	if strings.Contains(field(msh, 9), "~") || len(strings.Split(field(msh, 9), "^")) > 2 {
		return nil, NewReject(ReasonInvalidComponent, "MSH-9 message type has illegal component/repeat structure")
	}
	if len(strings.Split(field(msh, 12), "^")) > 3 {
		return nil, NewReject(ReasonInvalidComponent, "MSH-12 version identifier has too many components")
	}
	msgType := component(msh, 9, 1)
	trigger := component(msh, 9, 2)
	switch msgType + "^" + trigger {
	case "ORU^R01":
		out.EventType = RawEventObservation
	case "PPR^PC1":
		out.EventType = RawEventCondition
	case "ADT^A04":
		out.EventType = RawEventEncounter
	default:
		return nil, NewReject(ReasonUnsupportedEventType, "unsupported message type %q", msgType+"^"+trigger)
	}
	out.EventID = field(msh, 10)
	if out.EventID == "" {
		return nil, NewReject(ReasonMissingEventID, "MSH-10 message control id is empty")
	}
	rawTime := field(msh, 7)
	if rawTime == "" {
		return nil, NewReject(ReasonMissingEventTime, "MSH-7 message date/time is empty (eventId=%s)", out.EventID)
	}
	ts, ok := parseHL7Time(rawTime)
	if !ok {
		return nil, NewReject(ReasonInvalidTime, "MSH-7 %q is not a valid HL7 timestamp (eventId=%s)", rawTime, out.EventID)
	}
	out.EventTime = ts

	// --- PID ---
	mrn := ""
	if raw := field(pid, 3); raw != "" {
		first := strings.Split(raw, "~")[0]
		mrn = strings.Split(first, "^")[0]
	}
	if mrn == "" {
		return nil, NewReject(ReasonMissingPatientID, "PID-3 patient identifier is empty (eventId=%s)", out.EventID)
	}
	out.PatientMRN = mrn
	if name := field(pid, 5); name != "" {
		dec, err := decodeEscapes(name)
		if err != nil {
			return nil, err
		}
		out.Unknown = append(out.Unknown, RawField{Path: "PID-5", Value: dec})
	}

	// --- OBX ---
	if code := component(obx, 3, 1); code == "" {
		return nil, NewReject(ReasonMissingCode, "OBX-3.1 observation code is empty (eventId=%s)", out.EventID)
	}
	code, err := decodeEscapes(component(obx, 3, 1))
	if err != nil {
		return nil, err
	}
	out.Code = code
	if display := component(obx, 3, 2); display != "" {
		dec, err := decodeEscapes(display)
		if err != nil {
			return nil, err
		}
		out.CodeDisplay = dec
	}
	out.CodeSystem = component(obx, 3, 3)
	if v := field(obx, 5); v != "" {
		dec, err := decodeEscapes(strings.Split(v, "~")[0])
		if err != nil {
			return nil, err
		}
		out.Value = dec
	}
	out.Unit = component(obx, 6, 1)
	status := field(obx, 11)
	if strings.ContainsAny(status, "^~") {
		return nil, NewReject(ReasonInvalidStatus, "OBX-11 status must be a single token (eventId=%s)", out.EventID)
	}
	out.Status = status
	if eff := field(obx, 14); eff != "" {
		ts, ok := parseHL7Time(eff)
		if !ok {
			return nil, NewReject(ReasonInvalidTime, "OBX-14 %q is not a valid HL7 timestamp (eventId=%s)", eff, out.EventID)
		}
		out.EffectiveTime = ts
	}
	return out, nil
}

func field(fields []string, idx int) string {
	if idx < len(fields) {
		return fields[idx]
	}
	return ""
}

func segmentName(seg string) string {
	if i := strings.IndexByte(seg, '|'); i >= 0 {
		return seg[:i]
	}
	return seg
}

func validSegmentName(name string) bool {
	if len(name) != 3 {
		return false
	}
	for _, c := range name {
		if !((c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
			return false
		}
	}
	return true
}

func itoa(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = digits[i%10]
		i /= 10
	}
	return string(b[pos:])
}
