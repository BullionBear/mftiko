package symbol

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// spellDigits is how many digits SpellStrike will expand. A longer literal or
// exponent is rejected so a hostile string cannot allocate a huge integer.
// It is a guard, not a product limit on strikes.
const spellDigits = 10000

// SpellStrike writes a strike in platform form (D203).
//
// 6400.0 and 6400 are one spelling. A fractional strike uses StrikeDecimal,
// so 6.4, 6d4 and 6D4 are 6D4. 7E4 and 10_000 fold to 70000 and 10000; a
// space or a slash does not fold, and the caller must not put that verbatim
// form into a ticker. Empty input is an error.
func SpellStrike(value string) (string, error) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return "", fmt.Errorf("%w: empty strike", ErrInvalidTicker)
	}
	raw = strings.ToUpper(raw)
	folded, ok := stripNumericUnderscores(raw)
	if !ok {
		return "", fmt.Errorf("%w: strike %q", ErrInvalidTicker, value)
	}
	folded = strings.ReplaceAll(folded, StrikeDecimal, ".")
	rat, ok := parseDecimal(folded)
	if !ok {
		return "", fmt.Errorf("%w: strike %q", ErrInvalidTicker, value)
	}
	return formatStrike(rat), nil
}

// stripNumericUnderscores removes '_' between digits (10_000). A '_' that is
// not between digits is rejected, matching a decimal literal.
func stripNumericUnderscores(s string) (string, bool) {
	if !strings.Contains(s, "_") {
		return s, true
	}
	var b strings.Builder
	b.Grow(len(s))
	prevDigit := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '_' {
			if !prevDigit || i+1 >= len(s) || s[i+1] < '0' || s[i+1] > '9' {
				return "", false
			}
			continue
		}
		b.WriteByte(c)
		prevDigit = c >= '0' && c <= '9'
	}
	return b.String(), true
}

// parseDecimal reads a base-10 literal, including an exponent (7E4, 6.4E-1).
// The value is exact; it does not go through a binary float.
func parseDecimal(s string) (*big.Rat, bool) {
	if s == "" {
		return nil, false
	}
	sign := 1
	switch s[0] {
	case '+':
		s = s[1:]
	case '-':
		sign = -1
		s = s[1:]
	}
	if s == "" {
		return nil, false
	}
	exp := 0
	if mant, expStr, sci := strings.Cut(s, "E"); sci {
		n, err := strconv.Atoi(expStr)
		if err != nil || n > spellDigits || n < -spellDigits {
			return nil, false
		}
		exp = n
		s = mant
	}
	if s == "" || strings.Contains(s, "E") {
		return nil, false
	}
	whole, frac, dotted := strings.Cut(s, ".")
	if dotted && strings.Contains(frac, ".") {
		return nil, false
	}
	if whole == "" {
		whole = "0"
	}
	if !asciiDigits(whole) || (dotted && frac != "" && !asciiDigits(frac)) {
		return nil, false
	}
	if len(whole) > spellDigits || len(frac) > spellDigits {
		return nil, false
	}
	digits := strings.TrimLeft(whole+frac, "0")
	if digits == "" {
		digits = "0"
	}
	scale := exp - len(frac)
	if scale > spellDigits || scale < -spellDigits {
		return nil, false
	}
	num := new(big.Int)
	if _, ok := num.SetString(digits, 10); !ok {
		return nil, false
	}
	if sign < 0 {
		num.Neg(num)
	}
	rat := new(big.Rat)
	if scale >= 0 {
		pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
		num.Mul(num, pow)
		rat.SetInt(num)
		return rat, true
	}
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-scale)), nil)
	rat.SetFrac(num, pow)
	return rat, true
}

func asciiDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func formatStrike(r *big.Rat) string {
	if r.IsInt() {
		return r.Num().String()
	}
	num := new(big.Int).Set(r.Num())
	den := new(big.Int).Set(r.Denom())
	neg := num.Sign() < 0
	if neg {
		num.Abs(num)
	}
	intPart := new(big.Int).Quo(num, den)
	rem := new(big.Int).Rem(num, den)
	var frac []byte
	for rem.Sign() != 0 && len(frac) < spellDigits {
		rem.Mul(rem, big.NewInt(10))
		digit := new(big.Int).Quo(rem, den)
		rem.Rem(rem, den)
		frac = append(frac, byte('0'+digit.Int64()))
	}
	for len(frac) > 0 && frac[len(frac)-1] == '0' {
		frac = frac[:len(frac)-1]
	}
	out := intPart.String() + StrikeDecimal + string(frac)
	if neg {
		return "-" + out
	}
	return out
}
