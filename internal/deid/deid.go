// Package deid provides deterministic pseudonymization: the same real value
// always maps to the same placeholder, across messages and wire formats.
// Mapping is derived from a keyed HMAC so real values can never be recovered
// from output and no raw value is ever exported.
package deid

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"

	"github.com/highcumontoa/healthcare-interop-pipeline-go/internal/model"
)

// DefaultKey is used when no explicit key is configured. It is fixed so that
// local sample runs and tests produce reproducible placeholders.
var DefaultKey = []byte("healthcare-interop-pipeline/local-v1")

// placeholder length (hex chars of truncated HMAC).
const pseudoLen = 16

// Engine resolves stable pseudonyms. It is safe for concurrent use: HMAC
// computation is stateless and deterministic, so a single real value can
// never be assigned two placeholders even under concurrent ingestion.
type Engine struct {
	mac []byte // HMAC key; stateless use via hmac.New per call
}

// NewEngine builds an Engine with the given HMAC key. An empty key falls
// back to the fixed DefaultKey for reproducible local runs.
func NewEngine(hmacKey []byte) *Engine {
	if len(hmacKey) == 0 {
		hmacKey = DefaultKey
	}
	cp := make([]byte, len(hmacKey))
	copy(cp, hmacKey)
	return &Engine{mac: cp}
}

func (e *Engine) sum(scope, realValue string) string {
	m := hmac.New(sha256.New, e.mac)
	m.Write([]byte(scope))
	m.Write([]byte{0})
	m.Write([]byte(realValue))
	return hex.EncodeToString(m.Sum(nil))[:pseudoLen]
}

// PatientLink returns the stable longitudinal patient linkage pseudonym
// derived from the MRN identity (system + value). It intentionally ignores
// name/contact changes so the longitudinal association survives updates.
func (e *Engine) PatientLink(mrn, mrnSystem string) string {
	return "PT-" + e.sum("patient-link", mrnSystem+"|"+mrn)
}

// Field returns a stable, scope-namespaced placeholder for a sensitive value.
// Empty real values map deterministically to "" (meaning "no value"), never to
// a random token, so old-version defaults stay distinguishable from data.
func (e *Engine) Field(scope, realValue string) string {
	if realValue == "" {
		return ""
	}
	prefix := "PHI-"
	switch scope {
	case "mrn":
		prefix = "MRN-"
	case "name":
		prefix = "NM-"
	case "contact":
		prefix = "CT-"
	default:
		prefix = "PHI-"
	}
	return prefix + e.sum("field:"+scope, realValue)
}

// Deidentify maps every sensitive field of a canonical event onto stable
// pseudonyms and copies through only non-identifying fields. The returned
// record contains no raw PHI. Lifecycle fields (Status/IngestedAt/...) are
// left for the pipeline/store to assign.
func (e *Engine) Deidentify(c *model.CanonicalEvent) model.OutputRecord {
	return model.OutputRecord{
		EventID:       c.EventID,
		EventType:     c.EventType,
		Format:        c.Format,
		Version:       c.Version,
		PatientPseudo: e.PatientLink(c.PatientMRN, c.PatientMRNSys),
		MRNPseudo:     e.Field("mrn", c.PatientMRN),
		NamePseudo:    e.Field("name", c.PatientName),
		ContactPseudo: e.Field("contact", c.PatientContact),
		Gender:        c.PatientGender,
		BirthDate:     c.PatientBirthDate,
		EffectiveTime: c.EffectiveTime,
		Sequence:      c.Sequence,
		Notes:         c.Notes,
		Unknowns:      c.Unknowns,
	}
}
