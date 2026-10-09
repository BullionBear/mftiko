package sym

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// decimalDigits bounds a listing decimal. It is a guard against a hostile
// payload, not a product limit on prices.
const decimalDigits = 1000

// canonicalDecimal writes a base-10 literal the way Postgres trim_scale(numeric)::text
// does, so a later refresh can compare a stored bound with the venue's spelling.
// "1.10" and "1.1" are one value. Empty is empty, which the caller stores as NULL.
// The parse is exact: it does not go through a binary float.
func canonicalDecimal(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", nil
	}
	if len(s) > decimalDigits {
		return "", fmt.Errorf("sym: decimal %q is too long", s)
	}
	r, ok := parseDecimal(s)
	if !ok {
		return "", fmt.Errorf("sym: decimal %q", s)
	}
	return formatDecimal(r), nil
}

// parseDecimal reads a base-10 literal, including an exponent (1e-8, 7E4).
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
	upper := strings.ToUpper(s)
	if mant, expStr, sci := strings.Cut(upper, "E"); sci {
		n, err := strconv.Atoi(expStr)
		if err != nil || n > decimalDigits || n < -decimalDigits {
			return nil, false
		}
		exp = n
		s = mant
	}
	if s == "" || strings.ContainsAny(s, "eE") {
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
	if len(whole) > decimalDigits || len(frac) > decimalDigits {
		return nil, false
	}
	digits := strings.TrimLeft(whole+frac, "0")
	if digits == "" {
		digits = "0"
	}
	scale := exp - len(frac)
	if scale > decimalDigits || scale < -decimalDigits {
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

// formatDecimal is the text form of a decimal with trailing fractional zeros removed.
func formatDecimal(r *big.Rat) string {
	if r.Sign() == 0 {
		return "0"
	}
	neg := r.Sign() < 0
	num := new(big.Int).Abs(r.Num())
	den := r.Denom()
	intPart := new(big.Int).Quo(num, den)
	rem := new(big.Int).Rem(num, den)
	if rem.Sign() == 0 {
		if neg {
			return "-" + intPart.String()
		}
		return intPart.String()
	}
	frac := make([]byte, 0, 16)
	for rem.Sign() != 0 && len(frac) <= decimalDigits {
		rem.Mul(rem, big.NewInt(10))
		digit := new(big.Int).Quo(rem, den)
		rem.Rem(rem, den)
		frac = append(frac, byte('0'+digit.Int64()))
	}
	for len(frac) > 0 && frac[len(frac)-1] == '0' {
		frac = frac[:len(frac)-1]
	}
	out := intPart.String() + "." + string(frac)
	if neg {
		return "-" + out
	}
	return out
}
