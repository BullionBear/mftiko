// Package bearer is the token file of a service that clients outside mftiko call with
// "Authorization: Bearer <token>" (DECISIONS E22-D007, E22-D016). It knows nothing of the
// protocol: SYM's gRPC adapter uses it today, and an HTTP or WebSocket adapter can use the same
// file without importing another adapter (E22-D002).
//
// The file has one "name token" per line. The name is for the access log. Only the tokens'
// SHA-256 is kept in memory.
package bearer

import (
	"bufio"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// Tokens are the bearer tokens a client may present, each with a name.
type Tokens struct {
	names []string
	sums  [][sha256.Size]byte
}

// Load reads a token file. Blank lines and lines that start with # are skipped. A name or a
// token that appears twice, or a file with no token, is an error. An error names the line,
// never the token.
func Load(path string) (*Tokens, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("token file: %w", err)
	}
	defer f.Close()
	t, err := Parse(f)
	if err != nil {
		return nil, fmt.Errorf("token file %s: %w", path, err)
	}
	return t, nil
}

// Parse is Load over r.
func Parse(r io.Reader) (*Tokens, error) {
	t := &Tokens{}
	names := map[string]bool{}
	sums := map[[sha256.Size]byte]string{}
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("line %d: want \"name token\"", n)
		}
		name, token := fields[0], fields[1]
		if names[name] {
			return nil, fmt.Errorf("line %d: name %q is listed twice", n, name)
		}
		sum := sha256.Sum256([]byte(token))
		if prev, ok := sums[sum]; ok {
			return nil, fmt.Errorf("line %d: %q has the same token as %q", n, name, prev)
		}
		names[name] = true
		sums[sum] = name
		t.names = append(t.names, name)
		t.sums = append(t.sums, sum)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(t.names) == 0 {
		return nil, errors.New("no token")
	}
	return t, nil
}

// Len is the number of tokens. A nil *Tokens has none.
func (t *Tokens) Len() int {
	if t == nil {
		return 0
	}
	return len(t.names)
}

// Match is the name of token. It compares against every entry, so which one matched does not
// decide how long it takes.
func (t *Tokens) Match(token string) (string, bool) {
	sum := sha256.Sum256([]byte(token))
	name, ok := "", false
	for i, s := range t.sums {
		if subtle.ConstantTimeCompare(sum[:], s[:]) == 1 {
			name, ok = t.names[i], true
		}
	}
	return name, ok
}

// FromHeader is the token of an "Authorization: Bearer <token>" header. The scheme is matched
// without regard to case.
func FromHeader(h http.Header) (string, bool) {
	scheme, token, ok := strings.Cut(strings.TrimSpace(h.Get("Authorization")), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	return token, token != ""
}
