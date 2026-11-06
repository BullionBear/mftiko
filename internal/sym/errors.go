package sym

import (
	"context"
	"errors"
	"strings"

	"github.com/BullionBear/mftiko/contract/symbol"
)

// CodeInvalid, CodeNotFound and CodeUnavailable are the business error codes
// (E22-D004). Every protocol sends the same text, Error.Error(); each maps
// the code to its own status. On NATS the text is a natsx error reply
// (option A): natsx.CodeHandler, and Reply.error and Reply.reason stay empty.
const (
	CodeInvalid     = "invalid"
	CodeNotFound    = "not_found"
	CodeUnavailable = "unavailable"
)

// Error is a business failure. Reason names the field, the value, and why.
// Cause is for the log only and never reaches a protocol; a context error
// there lets an adapter answer with its own cancel or deadline code.
type Error struct {
	Code   string
	Reason string
	Cause  error
}

// Error is "<code>: <reason>", the text every protocol sends.
func (e *Error) Error() string {
	if e == nil || e.Code == "" {
		return CodeUnavailable
	}
	if e.Reason == "" {
		return e.Code
	}
	return e.Code + ": " + e.Reason
}

func (e *Error) Unwrap() error { return e.Cause }

func invalid(reason string) error {
	return &Error{Code: CodeInvalid, Reason: reason}
}

func unavailable(reason string, cause error) error {
	return &Error{Code: CodeUnavailable, Reason: reason, Cause: cause}
}

func notFound(reason string) error {
	return &Error{Code: CodeNotFound, Reason: reason}
}

// missed is a not_found reason. includeInactive false searched the active
// table only, which is why a terminal row is absent.
func missed(label string, includeInactive bool) error {
	if includeInactive {
		return notFound(label + " not found in active or terminal table (include_inactive=true)")
	}
	return notFound(label + " not found in active table (include_inactive=false)")
}

// readErr turns a store error into a business error. notFound is the message
// when the row is absent. Other failures name the operation and the cause.
func readErr(err error, op string, absent error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, symbol.ErrNotFound) {
		return absent
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return unavailable(op+": "+err.Error(), err)
	}
	return unavailable(op+": "+causeText(err), err)
}

// causeText is the client-facing part of an unavailable error. A long message
// or one that carries a query is replaced, so the reply names the failure
// without the SQL.
func causeText(err error) string {
	if err == nil {
		return "database read failed"
	}
	msg := strings.Join(strings.Fields(err.Error()), " ")
	lower := strings.ToLower(msg)
	if len(msg) > 300 || strings.Contains(lower, "select ") || strings.Contains(lower, "insert ") || strings.Contains(lower, "update ") {
		return "database read failed"
	}
	return msg
}
