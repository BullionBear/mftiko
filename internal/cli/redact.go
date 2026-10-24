package cli

import "strings"

// secrets are token values that must not reach stdout, stderr, or an error
// (E11-D003). An empty string is not a secret: ReplaceAll with an empty old
// string would rewrite the message.
type secrets []string

func (s *secrets) add(v string) {
	if v == "" {
		return
	}
	for _, have := range *s {
		if have == v {
			return
		}
	}
	*s = append(*s, v)
}

func (s secrets) redact(msg string) string {
	for _, sec := range s {
		if sec == "" {
			continue
		}
		msg = strings.ReplaceAll(msg, sec, "***")
	}
	return msg
}

func (s secrets) redactBytes(b []byte) []byte {
	if len(s) == 0 || len(b) == 0 {
		return b
	}
	return []byte(s.redact(string(b)))
}
