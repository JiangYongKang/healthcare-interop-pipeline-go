// Package normalize converts parsed raw events into canonical events while
// applying:
//   - business validation (distinguishable rejection reasons),
//   - deterministic version-specific default semantics for fields absent in
//     older versions,
//   - retention (not failure) of unknown/new fields.
package normalize

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/apperr"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/model"
	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/parser"
)

// Defaults applied when older versions omit a field.
const (
	DefaultGender    = "UNK"
	DefaultMRNSystem = "LOCAL-MPI"
	CanonicalLayout  = "2006-01-02T15:04:05"
	DateLayout       = "2006-01-02"
)

// genderMap maps format-native codes (compared case-insensitively) to the
// canonical set M/F/O/U/UNK.
var genderMap = map[string]string{
	"m": "M", "f": "F", "o": "O", "u": "U",
	"male": "M", "female": "F", "other": "O", "unknown": "U",
}

// Normalize validates and maps a raw event onto the canonical model.
// It returns *apperr.Error with Kind VALIDATION for business failures.
func Normalize(r *parser.RawEvent) (*model.CanonicalEvent, *apperr.Error) {
	missing := func(field string) *apperr.Error {
		return apperr.New(apperr.CodeMissingRequired, "").
			WithInternal("required field missing: " + field)
	}

	id := strings.TrimSpace(r.EventID)
	if id == "" {
		return nil, missing("event-id")
	}
	mrn := strings.TrimSpace(r.PatientMRN)
	if mrn == "" {
		return nil, missing("patient-mrn").WithMessage(id)
	}
	etype := strings.TrimSpace(r.EventType)
	if etype == "" {
		return nil, missing("event-type").WithMessage(id)
	}
	if strings.TrimSpace(r.EffectiveTime) == "" {
		return nil, missing("effective-time").WithMessage(id)
	}
	eff, key, ae := canonicalTime(r.EffectiveTime, r.Format)
	if ae != nil {
		return nil, ae.WithMessage(id)
	}

	c := &model.CanonicalEvent{
		Format:           r.Format,
		Version:          r.Version,
		EventID:          id,
		MessageControlID: strings.TrimSpace(r.MessageControlID),
		EventType:        etype,
		Sequence:         r.Sequence,
		PatientMRN:       mrn,
		PatientMRNSys:    strings.TrimSpace(r.PatientMRNSys),
		PatientName:      strings.TrimSpace(r.PatientName),
		PatientBirthDate: strings.TrimSpace(r.PatientBirthDate),
		PatientContact:   strings.TrimSpace(r.PatientContact),
		EffectiveTime:    eff,
		OrderingKey:      key,
		Notes:            r.Notes,
		Unknowns:         r.Unknowns,
	}

	if strings.TrimSpace(r.PatientBirthDate) != "" {
		canon, ok := parseDate(r.PatientBirthDate, r.Format)
		if !ok {
			return nil, apperr.New(apperr.CodeInvalidValue, "").
				WithMessage(id).
				WithInternal("patient birth date is not a valid date: " + r.PatientBirthDate)
		}
		c.PatientBirthDate = canon
	}

	// Deterministic default semantics for fields omitted by older versions.
	g, gae := canonicalGender(r.PatientGender)
	if gae != nil {
		return nil, gae.WithMessage(id)
	}
	c.PatientGender = g
	if strings.TrimSpace(r.PatientGender) == "" {
		c.Defaults = append(c.Defaults, "gender:"+DefaultGender)
	}
	if c.PatientMRNSys == "" {
		c.PatientMRNSys = DefaultMRNSystem
		c.Defaults = append(c.Defaults, "mrn-system:"+DefaultMRNSystem)
	}
	if c.PatientName == "" {
		c.Defaults = append(c.Defaults, "name:empty")
	}
	if c.PatientContact == "" {
		c.Defaults = append(c.Defaults, "contact:empty")
	}
	if c.PatientBirthDate == "" {
		c.Defaults = append(c.Defaults, "birth-date:empty")
	}
	// OrderingKey is always derived from the normalized effective time so it
	// is comparable across wire formats; the source Sequence stays available
	// as metadata. Two versions of the same event with an identical effective
	// time but different payloads are therefore a storage clash, not a
	// silent overwrite.
	c.OrderingKey = key
	if !r.HasSequence {
		c.Defaults = append(c.Defaults, "sequence:derived-from-effective-time")
	}
	return c, nil
}

// canonicalGender maps native codes case-insensitively and applies the
// stable UNK default when the field is absent (e.g. old versions).
func canonicalGender(raw string) (string, *apperr.Error) {
	code := strings.ToLower(strings.TrimSpace(raw))
	if code == "" {
		return DefaultGender, nil
	}
	if g, ok := genderMap[code]; ok {
		return g, nil
	}
	return "", apperr.New(apperr.CodeInvalidValue, "").
		WithInternal("patient gender code is not recognized: " + raw)
}

// canonicalTime parses format-native timestamps and returns both the
// canonical string and an ordering key (unix seconds).
func canonicalTime(v string, f model.Format) (string, int64, *apperr.Error) {
	if f == model.FormatHL7v2 {
		return hl7Time(v)
	}
	return isoTime(v)
}

// hl7Time accepts HL7 DTM precision YYYY[MM[DD[HH[MM[SS]]]]]. Local samples
// without a timezone offset are interpreted deterministically as UTC.
func hl7Time(v string) (string, int64, *apperr.Error) {
	s := strings.TrimSpace(v)
	if i := strings.IndexAny(s, "+-"); i >= 14 {
		s = s[:i] // strip an offset suffix; the local sample set is UTC-based
	}
	layouts := []string{"20060102150405", "200601021504", "2006010215", "20060102", "2006"}
	for _, l := range layouts {
		if len(s) == len(l) {
			t, err := time.Parse(l, s)
			if err != nil {
				return "", 0, apperr.New(apperr.CodeInvalidValue, "").
					WithInternal("HL7 timestamp invalid: " + v)
			}
			return t.Format(CanonicalLayout), t.Unix(), nil
		}
	}
	return "", 0, apperr.New(apperr.CodeInvalidValue, "").
		WithInternal("HL7 timestamp has unsupported precision: " + v)
}

func isoTime(v string) (string, int64, *apperr.Error) {
	s := strings.TrimSpace(v)
	for _, l := range []string{time.RFC3339, CanonicalLayout} {
		if t, err := time.Parse(l, s); err == nil {
			return t.Format(CanonicalLayout), t.Unix(), nil
		}
	}
	return "", 0, apperr.New(apperr.CodeInvalidValue, "").
		WithInternal("FHIR effectiveDateTime is not RFC3339: " + v)
}

func parseDate(v string, f model.Format) (string, bool) {
	s := strings.TrimSpace(v)
	if f == model.FormatHL7v2 {
		if len(s) == 8 {
			t, err := time.Parse("20060102", s)
			if err == nil {
				return t.Format(DateLayout), true
			}
		}
		return "", false
	}
	if _, err := time.Parse(DateLayout, s); err == nil {
		return s, true
	}
	return "", false
}

// Fingerprint builds a stable business-payload signature from a canonical
// event. It deliberately excludes format/version, lifecycle fields and the
// transport-level source Sequence so a genuinely identical event redelivered
// through a different wire format (or a re-sequenced transport copy) is still
// recognized as a duplicate.
func Fingerprint(c *model.CanonicalEvent) string {
	var b strings.Builder
	fields := []string{
		c.EventID, c.EventType,
		c.PatientMRNSys, c.PatientMRN, c.PatientName,
		c.PatientGender, c.PatientBirthDate, c.PatientContact,
		c.EffectiveTime, strconv.FormatInt(c.OrderingKey, 10), c.Notes,
	}
	for i, f := range fields {
		if i > 0 {
			b.WriteByte(0)
		}
		b.WriteString(f)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}
