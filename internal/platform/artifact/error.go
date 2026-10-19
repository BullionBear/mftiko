package artifact

import (
	"errors"
	"fmt"
)

// Reasons a pull fails with (D928). The caller decides whether to retry and when the failure becomes
// INIT_FAILED; this package does not retry.
//
// Permanent reasons name what is wrong with the artifact or the caller's right to read it. ReasonUnavailable
// is temporary: the registry could not be reached, or it returned 5xx (or 429). ReasonNotImage, ReasonWrongMediaType
// and ReasonBadManifest are permanent too: an OCI index (D934), a config that is not the worker config, and a
// document that is not a mftiko worker manifest.
const (
	ReasonTooLarge          = "artifact_too_large"
	ReasonWrongKind         = "artifact_wrong_kind"
	ReasonPlatformMismatch  = "artifact_platform_mismatch"
	ReasonContractsMismatch = "artifact_contracts_mismatch"
	ReasonDigestMismatch    = "artifact_digest_mismatch"
	ReasonNotFound          = "artifact_not_found"
	ReasonUnauthorized      = "artifact_unauthorized"
	ReasonUnavailable       = "artifact_unavailable"
	ReasonNotImage          = "artifact_not_image"
	ReasonWrongMediaType    = "artifact_wrong_mediatype"
	ReasonBadManifest       = "artifact_bad_manifest"
	// ReasonUnsafeLayer is a layer that must not be extracted (D954): a rejected tar entry, a broken
	// archive, or an unpack that would exceed the size or file-count limit (D953). The cached blob stays;
	// its sha256 was already checked. The caller ends the session INIT_FAILED.
	ReasonUnsafeLayer = "artifact_unsafe_layer"
)

// ErrRequest is a caller mistake (empty repository, missing kind, registry address that is not host[:port]).
// It is not a result from the registry: it has no reason, and retrying it changes nothing.
var ErrRequest = errors.New("artifact: bad request")

// Error is a pull that failed. Permanent is false when another attempt might succeed (D928).
type Error struct {
	Reason    string
	Permanent bool
	Err       error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return "artifact: " + e.Reason
	}
	return "artifact: " + e.Reason + ": " + e.Err.Error()
}

func (e *Error) Unwrap() error { return e.Err }

// ReasonOf reports the reason carried by err, or "" when err is not an Error.
func ReasonOf(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Reason
	}
	return ""
}

// Permanent reports whether err is a permanent artifact failure. Anything that is not an Error is not.
func Permanent(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Permanent
	}
	return false
}

func permanent(reason string, err error) error {
	return &Error{Reason: reason, Permanent: true, Err: err}
}

func temporary(reason string, err error) error {
	return &Error{Reason: reason, Permanent: false, Err: err}
}

func badRequest(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrRequest}, args...)...)
}
