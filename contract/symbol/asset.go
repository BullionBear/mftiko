package symbol

import (
	"fmt"
	"strings"
	"unicode"
)

// assetAliases is the one native-to-canonical asset table (E02-D003).
// Keys are uppercase native codes. USD, USDC and USDT are absent on purpose:
// they are three assets. Venue integrations add entries here. Targets are
// themselves canonical and are not keys.
var assetAliases = map[string]string{
	"XBT": "BTC",
}

// AssetAliases returns a copy of the alias table. Adapters share this table
// instead of keeping one each; SYM is what applies it (E02-D003).
func AssetAliases() map[string]string {
	out := make(map[string]string, len(assetAliases))
	for k, v := range assetAliases {
		out[k] = v
	}
	return out
}

// CanonicalAsset uppercases an asset code and applies AssetAliases.
// XBT and xbt become BTC. A code that still contains pair punctuation is an
// error: an asset is not a pair. The result is canonical, so applying this
// twice changes nothing.
func CanonicalAsset(code string) (string, error) {
	code = strings.ToUpper(strings.TrimSpace(code))
	if code == "" {
		return "", fmt.Errorf("%w", ErrAsset)
	}
	if mapped, ok := assetAliases[code]; ok {
		code = mapped
	}
	if !assetForm(code) {
		return "", fmt.Errorf("%w: %q", ErrAsset, code)
	}
	return code, nil
}

// IsCanonicalAsset reports whether code is already the form CanonicalAsset
// returns. XBT, btc and a code with surrounding space are not.
func IsCanonicalAsset(code string) bool {
	got, err := CanonicalAsset(code)
	return err == nil && got == code
}

func assetForm(code string) bool {
	if code == "" {
		return false
	}
	if _, alias := assetAliases[code]; alias {
		return false
	}
	if strings.ToUpper(code) != code {
		return false
	}
	for _, r := range code {
		if r == '/' || r == '_' || r == '-' || r == '.' || unicode.IsSpace(r) {
			return false
		}
	}
	return true
}
