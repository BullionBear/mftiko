package symgrpc

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/rs/zerolog"
)

// Tokens are the bearer tokens a client may present, each with a name for the access log
// (E22-D007). Only their SHA-256 is kept.
type Tokens struct {
	names []string
	sums  [][sha256.Size]byte
}

// LoadTokens reads a token file: one "name token" per line. Blank lines and lines that start
// with # are skipped. A name or a token that appears twice, or a file with no token, is an error.
func LoadTokens(path string) (*Tokens, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("grpc token file: %w", err)
	}
	defer f.Close()
	t, err := ParseTokens(f)
	if err != nil {
		return nil, fmt.Errorf("grpc token file %s: %w", path, err)
	}
	return t, nil
}

// ParseTokens is LoadTokens over r. An error names the line, never the token.
func ParseTokens(r io.Reader) (*Tokens, error) {
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

// match is the name of token. It compares against every entry, so which one matched does
// not decide how long it takes.
func (t *Tokens) match(token string) (string, bool) {
	sum := sha256.Sum256([]byte(token))
	name, ok := "", false
	for i, s := range t.sums {
		if subtle.ConstantTimeCompare(sum[:], s[:]) == 1 {
			name, ok = t.names[i], true
		}
	}
	return name, ok
}

// bearer is the token of an "Authorization: Bearer <token>" header.
func bearer(h http.Header) (string, bool) {
	scheme, token, ok := strings.Cut(strings.TrimSpace(h.Get("Authorization")), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	return token, token != ""
}

// gate checks the bearer token of every call it wraps and writes one access log line per call,
// with the token's name (E22-D007).
type gate struct {
	tokens *Tokens
	log    zerolog.Logger
}

var _ connect.Interceptor = (*gate)(nil)

func (g *gate) check(h http.Header) (string, error) {
	token, ok := bearer(h)
	if !ok {
		return "", connect.NewError(connect.CodeUnauthenticated, errors.New("bearer token required"))
	}
	name, ok := g.tokens.match(token)
	if !ok {
		return "", connect.NewError(connect.CodeUnauthenticated, errors.New("invalid bearer token"))
	}
	return name, nil
}

func (g *gate) access(procedure, peer, name string, start time.Time, err error) {
	code := "ok"
	if err != nil {
		code = connect.CodeOf(err).String()
	}
	g.log.Info().
		Str("rpc", procedure).
		Str("name", name).
		Str("peer", peer).
		Str("code", code).
		Dur("took", time.Since(start)).
		Msg("grpc")
}

func (g *gate) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		start := time.Now()
		name, err := g.check(req.Header())
		var resp connect.AnyResponse
		if err == nil {
			resp, err = next(ctx, req)
		}
		g.access(req.Spec().Procedure, req.Peer().Addr, name, start, err)
		return resp, err
	}
}

func (g *gate) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		start := time.Now()
		name, err := g.check(conn.RequestHeader())
		if err == nil {
			err = next(ctx, conn)
		}
		g.access(conn.Spec().Procedure, conn.Peer().Addr, name, start, err)
		return err
	}
}

func (g *gate) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}
