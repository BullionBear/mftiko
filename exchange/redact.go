package exchange

import (
	"net/url"
	"regexp"
)

// mask stands in for a secret wherever a value is formatted, logged or put in an error.
const mask = "REDACTED"

// listenKeys finds the value of a listenKey given by name: a query or form parameter (listenKey=…)
// or a JSON member ("listenKey": "…"), in any letter case.
var listenKeys = regexp.MustCompile(`(?i)((?:^|[?&])listenKey=)[^&#\s"]*|("listenKey"\s*:\s*")(?:[^"\\]|\\.)*`)

// MaskListenKey returns b with the value of every listenKey in it replaced by REDACTED, for any venue:
// a listenKey lets whoever holds it read the account's private stream. The rest of b stays as it
// was, for debugging; b itself comes back when it holds none. A listenKey that is a URL path segment
// (Binance's /ws/<listenKey>) has no name to find it by, which is why a URL is shown by MaskURL.
func MaskListenKey(b []byte) []byte {
	if !listenKeys.Match(b) {
		return b
	}
	return listenKeys.ReplaceAll(b, []byte("${1}${2}"+mask))
}

// MaskURL is how a websocket URL is shown: its scheme and host. A private stream's path or query
// carries its listenKey or token, by position as often as by name (Binance UM: /private/ws/<listenKey>),
// so neither is shown, for any stream.
func MaskURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return mask
	}
	s := u.Scheme + "://" + u.Host
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		s += "/" + mask
	}
	return s
}

// appendHeaders appends " [Name: value, …]", every value masked when maskValues.
func appendHeaders(b []byte, hs []Header, maskValues bool) []byte {
	if len(hs) == 0 {
		return b
	}
	b = append(b, " ["...)
	for i, h := range hs {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = append(append(b, h.Name...), ": "...)
		if maskValues {
			b = append(b, mask...)
		} else {
			b = append(b, h.Value...)
		}
	}
	return append(b, ']')
}
