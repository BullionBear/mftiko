// Package decimal is how the platform handles prices, quantities and every other base-10 number (DECISIONS
// D008): they are float64 in Go and double on the wire, and these functions are the only way across the edges.
//
//   - In: Parse a venue's text. NaN and ±Inf are refused; the sign is the caller's to check, field by field.
//     For text of at most 15 significant digits, Parse then Format with the right step gives the text back.
//   - Out: Append a value to a venue request snapped to its step, in plain notation with exactly the step's
//     decimals. Never strconv's 'g' or -1 precision: a venue refuses 1e-05.
//   - Compare: never with ==. Steps turns a value into a whole number of steps; two values are equal when
//     their Steps are.
//
// An optional value that is absent is NaN inside a process (Absent, Has) and an unset optional field on the
// wire; NaN itself never goes on the wire.
package decimal

import (
	"errors"
	"math"
	"strconv"
	"unsafe"
)

// ErrInvalid: the text is not a finite base-10 number.
var ErrInvalid = errors.New("decimal: not a finite number")

// tolerance is how close to a step boundary, in steps, a value is taken to be on it: float64 holds about
// 15.9 significant digits, so a value a few ulps off a boundary is that boundary.
const tolerance = 1e-9

// Absent is the in-process value of an optional number the venue did not send.
func Absent() float64 { return math.NaN() }

// Has reports whether v is present: not Absent.
func Has(v float64) bool { return !math.IsNaN(v) }

// Parse reads s, correctly rounded to the nearest float64. Leading '+', a fraction and an exponent are taken;
// NaN, Inf, hex and underscores are not.
func Parse(s string) (float64, error) {
	if s == "" || !plain(s) {
		return 0, ErrInvalid
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsInf(v, 0) {
		return 0, ErrInvalid
	}
	return v, nil
}

// ParseBytes is Parse on b, without copying it.
func ParseBytes(b []byte) (float64, error) {
	if len(b) == 0 {
		return 0, ErrInvalid
	}
	return Parse(unsafe.String(unsafe.SliceData(b), len(b)))
}

// plain reports whether s has only digits, a sign, '.', 'e' and 'E': what ParseFloat would read as NaN, Inf,
// hex or with underscores is refused before it gets there.
func plain(s string) bool {
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c >= '0' && c <= '9', c == '.', c == '-', c == '+', c == 'e', c == 'E':
		default:
			return false
		}
	}
	return true
}

// Places is how many decimals step has when written out: 0.001 has 3, 0.5 has 1, 5 has 0. A step that is not
// positive and finite has 0.
func Places(step float64) int {
	if !(step > 0) || math.IsInf(step, 0) {
		return 0
	}
	s := strconv.FormatFloat(step, 'f', -1, 64)
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			return len(s) - i - 1
		}
	}
	return 0
}

// Steps is v as a whole number of steps, rounded to the nearest. step must be positive.
func Steps(v, step float64) int64 { return int64(math.Round(v / step)) }

// Equal reports whether a and b are the same number of steps.
func Equal(a, b, step float64) bool { return Steps(a, step) == Steps(b, step) }

// Snap is v rounded to the nearest multiple of step. A step that is not positive returns v.
func Snap(v, step float64) float64 {
	if !(step > 0) {
		return v
	}
	return zero(math.Round(v/step) * step)
}

// SnapDown is the largest multiple of step not above v: a buy price, a quantity that must not exceed v. A
// value within tolerance of a boundary is on it, so 0.3 with step 0.1 stays 0.3.
func SnapDown(v, step float64) float64 {
	if !(step > 0) {
		return v
	}
	return zero(math.Floor(v/step+tolerance) * step)
}

// SnapUp is the smallest multiple of step not below v: a sell price.
func SnapUp(v, step float64) float64 {
	if !(step > 0) {
		return v
	}
	return zero(math.Ceil(v/step-tolerance) * step)
}

// zero turns -0 into 0, so that nothing writes "-0.00".
func zero(v float64) float64 {
	if v == 0 {
		return 0
	}
	return v
}

// Append writes v to dst snapped to the nearest multiple of step, with exactly Places(step) decimals and
// never an exponent: what goes into a venue request. A step that is not positive writes v as it is, in plain
// notation with as many decimals as it needs.
func Append(dst []byte, v, step float64) []byte {
	if !(step > 0) {
		return strconv.AppendFloat(dst, v, 'f', -1, 64)
	}
	return strconv.AppendFloat(dst, Snap(v, step), 'f', Places(step), 64)
}

// Format is Append into a new string.
func Format(v, step float64) string { return string(Append(nil, v, step)) }
