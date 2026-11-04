// Package tdsubject builds the C6 subjects of one TD account (PROTOCOL §7, E21-D006, E21-D007,
// E21-D042–E21-D044).
//
// A subject is td.{order,view,evt}.{td_instance}.{api_id}. The caller supplies td_instance.
// This package does not guess one and does not fall back to td.{order,view,evt}.{api_id}.
// td_instance is one NATS token: empty, '.', '*', '>', and whitespace are refused and not escaped
// (E21-D043).
package tdsubject

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

var (
	// ErrInstance is a td_instance that is empty or contains '.', '*', '>', or whitespace.
	ErrInstance = errors.New("tdsubject: td_instance is empty or contains '.', '*', '>' or whitespace")

	// ErrAPIID is an api_id that is not greater than 0.
	ErrAPIID = errors.New("tdsubject: api_id must be positive")
)

// Token rejects a td_instance that cannot be one NATS subject token (E21-D043).
func Token(inst string) error {
	if inst == "" || strings.ContainsAny(inst, ".*>") || strings.ContainsFunc(inst, unicode.IsSpace) {
		return fmt.Errorf("tdsubject: td_instance %q: %w", inst, ErrInstance)
	}
	return nil
}

// Order is td.order.{td_instance}.{api_id}.
func Order(inst string, apiID int64) (string, error) { return subject("order", inst, apiID) }

// View is td.view.{td_instance}.{api_id}.
func View(inst string, apiID int64) (string, error) { return subject("view", inst, apiID) }

// Evt is td.evt.{td_instance}.{api_id}.
func Evt(inst string, apiID int64) (string, error) { return subject("evt", inst, apiID) }

func subject(kind, inst string, apiID int64) (string, error) {
	if err := Token(inst); err != nil {
		return "", err
	}
	if apiID <= 0 {
		return "", fmt.Errorf("tdsubject: api_id %d: %w", apiID, ErrAPIID)
	}
	return "td." + kind + "." + inst + "." + strconv.FormatInt(apiID, 10), nil
}
