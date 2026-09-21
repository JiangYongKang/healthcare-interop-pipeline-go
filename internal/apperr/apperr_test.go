package apperr

import (
	"errors"
	"strings"
	"testing"
)

func TestCodeKindMapping(t *testing.T) {
	cases := map[Code]Kind{
		CodeTruncated:        KindFormat,
		CodeMalformedJSON:    KindFormat,
		CodeIllegalEncoding:  KindFormat,
		CodeMissingRequired:  KindValidation,
		CodeInvalidValue:     KindValidation,
		CodeDuplicateEvent:   KindConflict,
		CodeStaleVersion:     KindConflict,
		CodeStorageClash:     KindConflict,
		CodeCapacityExceeded: KindLimit,
	}
	for c, want := range cases {
		e := New(c, "")
		if e.Kind != want {
			t.Fatalf("code=%s kind=%s want=%s", c, e.Kind, want)
		}
		if e.Detail == "" {
			t.Fatalf("code=%s must carry a safe default detail", c)
		}
		t.Logf("code=%-22s -> kind=%s safe=%q", c, e.Kind, e.Detail)
	}
}

func TestClassifyTypedAndUnknown(t *testing.T) {
	ae := New(CodeStorageClash, "").WithMessage("E-9")
	if got := Classify(ae); got.Code != CodeStorageClash {
		t.Fatal("typed error must pass through")
	}
	if Classify(nil) != nil {
		t.Fatal("nil classifies to nil")
	}
	// Arbitrary internal errors must be collapsed to a generic external
	// error so parser/runtime internals never leak.
	raw := errors.New("json: cannot unmarshal number at offset 42 / MSH splat")
	got := Classify(raw)
	if got.Kind != KindValidation {
		t.Fatalf("unknown error kind=%s", got.Kind)
	}
	if strings.Contains(got.Detail, "offset 42") || strings.Contains(got.Detail, "MSH") {
		t.Fatalf("internal detail leaked: %q", got.Detail)
	}
	t.Logf("internal error collapsed to safe=%q", SafeMessage(got))
}

func TestWithInternalNotInSafeMessage(t *testing.T) {
	e := New(CodeMalformedJSON, "").WithInternal("secret stack")
	if strings.Contains(SafeMessage(e), "secret stack") {
		t.Fatal("internal detail must not appear in safe message")
	}
	if e.InternalDetail != "secret stack" {
		t.Fatal("internal detail should be retained for audit")
	}
}
