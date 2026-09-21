package parser

import (
	"strconv"
	"strings"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/apperr"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/model"
)

// parseHL7 decodes an HL7v2 pipe-encoded message. Only the structural subset
// required by the pipeline (MSH/PID/EVN plus a custom ZSN) is interpreted;
// every other segment is retained as an UnknownField rather than dropped.
func parseHL7(raw []byte) (*RawEvent, *apperr.Error) {
	text := strings.ReplaceAll(string(raw), "\r\n", "\n")
	segLines := strings.FieldsFunc(text, func(r rune) bool { return r == '\r' || r == '\n' })
	if len(segLines) == 0 || !strings.HasPrefix(segLines[0], "MSH") {
		return nil, apperr.New(apperr.CodeTruncated, "").
			WithInternal("MSH segment missing")
	}

	mshFields := strings.Split(segLines[0], "|")
	// indices: [0]=MSH [1]=MSH-2 enc chars ... [9]=MSH-10 control id
	//          [11]=MSH-12 version id
	if len(mshFields) < 12 {
		return nil, apperr.New(apperr.CodeTruncated, "").
			WithInternal("MSH segment ends before field MSH-12 (version id)")
	}
	enc := mshFields[1]
	if len(enc) < 1 {
		return nil, apperr.New(apperr.CodeTruncated, "").
			WithInternal("MSH-2 encoding characters missing")
	}
	compSep := string(enc[0]) // '^'

	version := strings.TrimSpace(mshFields[11])
	if version == "" {
		return nil, apperr.New(apperr.CodeTruncated, "").
			WithInternal("MSH-12 version id is empty")
	}
	if !model.SupportedFormatVersion(model.FormatHL7v2, version) {
		return nil, apperr.New(apperr.CodeUnsupportedVersion, "").
			WithInternal("HL7v2 version not supported: " + version)
	}

	ev := &RawEvent{
		Format:           model.FormatHL7v2,
		Version:          version,
		MessageControlID: strings.TrimSpace(mshFields[9]),
		EventID:          strings.TrimSpace(mshFields[9]),
		EventType:        hl7EventType(mshFields[8], compSep),
	}

	seen := map[string]bool{"MSH": true}
	pidSeen := false
	for _, line := range segLines[1:] {
		name := hl7SegmentName(line)
		switch name {
		case "PID":
			if seen["PID"] {
				ev.Unknowns = append(ev.Unknowns, model.UnknownField{
					Location: "PID[repeat]", Kind: "segment", SizeHint: len(line)})
				continue
			}
			if !strings.Contains(line, "|") {
				return nil, apperr.New(apperr.CodeTruncated, "").
					WithInternal("PID segment contains no field delimiter")
			}
			seen["PID"] = true
			applyPID(ev, line, compSep)
			pidSeen = true
		case "EVN":
			if seen["EVN"] {
				ev.Unknowns = append(ev.Unknowns, model.UnknownField{
					Location: "EVN[repeat]", Kind: "segment", SizeHint: len(line)})
				continue
			}
			seen["EVN"] = true
			applyEVN(ev, line)
		case "ZSN":
			if seen["ZSN"] {
				ev.Unknowns = append(ev.Unknowns, model.UnknownField{
					Location: "ZSN[repeat]", Kind: "segment", SizeHint: len(line)})
				continue
			}
			seen["ZSN"] = true
			if ae := applyZSN(ev, line); ae != nil {
				return nil, ae
			}
		default:
			ev.Unknowns = append(ev.Unknowns, model.UnknownField{
				Location: name + "[0]", Kind: "segment", SizeHint: len(line)})
		}
	}

	if !pidSeen {
		return nil, apperr.New(apperr.CodeTruncated, "").
			WithInternal("PID segment missing")
	}
	return ev, nil
}

func hl7SegmentName(line string) string {
	if i := strings.IndexByte(line, '|'); i >= 0 {
		return line[:i]
	}
	return line
}

func hl7EventType(field, compSep string) string {
	parts := strings.Split(field, compSep)
	if len(parts) >= 2 && parts[1] != "" {
		return parts[0] + "_" + parts[1]
	}
	return parts[0]
}

// component returns component idx (0-based) of a pipe field, first repeat only.
func component(field, compSep string, idx int) string {
	rep := strings.SplitN(field, "~", 2)[0]
	parts := strings.Split(rep, compSep)
	if idx < len(parts) {
		return hl7Unescape(parts[idx])
	}
	return ""
}

func hl7Unescape(s string) string {
	r := strings.NewReplacer(
		`\F\`, "|",
		`\S\`, "^",
		`\T\`, "&",
		`\R\`, "~",
		`\E\`, `\`,
	)
	return r.Replace(s)
}

func applyPID(ev *RawEvent, line, compSep string) {
	f := strings.Split(line, "|")
	// f[0]="PID" and field PID-n is at f[n] (the name occupies slot 0).
	// PID-2 external ID, PID-3 internal ID, PID-5 name, PID-7 DOB,
	// PID-8 sex, PID-13 phone.
	get := func(n int) string {
		if n < len(f) {
			return f[n]
		}
		return ""
	}
	if mrn := component(get(3), compSep, 0); mrn != "" {
		ev.PatientMRN = mrn
		ev.PatientMRNSys = component(get(3), compSep, 3)
	} else if ext := component(get(2), compSep, 0); ext != "" {
		ev.PatientMRN = ext
		ev.PatientMRNSys = component(get(2), compSep, 3)
	}

	given := component(get(5), compSep, 1)
	family := component(get(5), compSep, 0)
	switch {
	case given != "" && family != "":
		ev.PatientName = given + " " + family
	case family != "":
		ev.PatientName = family
	case given != "":
		ev.PatientName = given
	}

	ev.PatientBirthDate = strings.TrimSpace(get(7))
	ev.PatientGender = strings.TrimSpace(get(8))
	// Patient contact phone: PID-13 home phone or PID-14 work phone (XTN);
	// accept whichever is populated, first component (unformatted number).
	for _, idx := range []int{13, 14} {
		if c := component(get(idx), compSep, 0); c != "" {
			ev.PatientContact = c
			break
		}
	}
}

func applyEVN(ev *RawEvent, line string) {
	f := strings.Split(line, "|")
	if len(f) >= 3 {
		ev.EffectiveTime = strings.TrimSpace(f[2])
	}
}

func applyZSN(ev *RawEvent, line string) *apperr.Error {
	f := strings.Split(line, "|")
	if len(f) < 2 || strings.TrimSpace(f[1]) == "" {
		return apperr.New(apperr.CodeTruncated, "").
			WithInternal("ZSN-1 source sequence missing")
	}
	seq, err := strconv.ParseInt(strings.TrimSpace(f[1]), 10, 64)
	if err != nil {
		return apperr.New(apperr.CodeInvalidValue, "").
			WithInternal("ZSN-1 sequence is not an integer: " + f[1])
	}
	ev.Sequence = seq
	ev.HasSequence = true
	return nil
}
