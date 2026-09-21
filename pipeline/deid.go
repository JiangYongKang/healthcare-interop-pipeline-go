package pipeline

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// Placeholder prefixes make de-identified values recognizable and keep
// patient linkage in a dedicated namespace.
const (
	patientPrefix = "PAT-"
	tokenPrefix   = "TOK-"
	hashLen       = 12 // 48-bit hex digest: stable, non-reversible locally
)

// Deidentifier maps sensitive real values to stable placeholders.
// Mapping is deterministic HMAC-SHA256 over a configured salt, so the
// same real value always maps to the same placeholder across messages,
// formats and goroutines, with no mutable shared mapping state (which
// also removes any race window where one real value could receive two
// placeholders).
type Deidentifier struct {
	salt []byte
}

// NewDeidentifier constructs a Deidentifier with the given salt.
func NewDeidentifier(salt string) *Deidentifier {
	return &Deidentifier{salt: []byte(salt)}
}

func (d *Deidentifier) digest(kind, value string) string {
	mac := hmac.New(sha256.New, d.salt)
	mac.Write([]byte(kind))
	mac.Write([]byte{0})
	mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil))[:hashLen]
}

// PatientPlaceholder maps a real MRN to a stable patient pseudonym.
// Different real MRNs map to different pseudonyms with overwhelming
// probability; the same MRN maps to the same pseudonym everywhere.
func (d *Deidentifier) PatientPlaceholder(mrn string) (string, error) {
	if mrn == "" {
		return "", fmt.Errorf("cannot pseudonymize empty patient identifier")
	}
	return patientPrefix + d.digest("mrn", mrn), nil
}

// Token maps any other sensitive value to a stable non-patient token.
func (d *Deidentifier) Token(kind, value string) string {
	if value == "" {
		return ""
	}
	return tokenPrefix + d.digest(kind, value)
}

// DeidentifyRecord rewrites a Record in place: the patient reference is
// pseudonymized, and every captured unknown field value (which may be a
// name, address or other free-text PHI) is replaced by a field-scoped
// token. Clinical facts (codes, results, units, times) are preserved.
// After this call the record contains no raw sensitive value.
func (d *Deidentifier) DeidentifyRecord(r *Record) error {
	pseudo, err := d.PatientPlaceholder(r.PatientRef)
	if err != nil {
		return err
	}
	r.PatientRef = pseudo
	if len(r.UnknownFields) > 0 {
		tokens := make(map[string]string, len(r.UnknownFields))
		for path, v := range r.UnknownFields {
			tokens[path] = d.Token("field:"+path, v)
		}
		r.UnknownFields = tokens
	}
	return nil
}
