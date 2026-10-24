package conformance

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

// Spec is one venue's registration: its adapters, where its fixtures are, and the REST calls to check.
// A venue's test is one call:
//
//	func TestConformance(t *testing.T) { conformance.Run(t, spec()) }
type Spec struct {
	Venue string // the venue name, as in a UniversalTicker

	// Dir is the fixture directory, relative to the test's package (testdata/conformance/<Venue>).
	// Golden files are written there under MFTIKO_UPDATE_GOLDEN=1. Fixtures reads it; nil means
	// os.DirFS(Dir).
	Dir      string
	Fixtures fs.FS

	// The adapters under test. A nil part is not checked.
	Market  exchange.Market
	REST    exchange.PublicREST
	Account exchange.AccountFactory
	History exchange.HistoryFactory

	// KeyType is the kind of test credentials the account adapters get: KeyHMAC (the default) or
	// KeyEd25519. Passphrase gives them a passphrase as well (Okx).
	KeyType    exchange.KeyType
	Passphrase bool

	// Now is when every request is built and signed, so a signed request is the same bytes on every
	// run. Zero means 2026-01-01T00:00:00Z.
	Now time.Time

	// Instruments for the planning and book checks, besides those the listing cases yield.
	Instruments []*symbol.Info

	// Cases are the REST calls to build and, where http/<name>.json holds a recorded response, to
	// parse. Make them with Call.
	Cases []Case

	// FillPairs tie a private session, or a trade session that carries events, to a History case
	// that covers the same fills (E06-D001).
	FillPairs []FillPair

	// TradeSamples are the requests whose TradeStream frames are pinned in ws/trade_dial.golden.json
	// (E03-D003), encoded at Now with ids 1, 2, ... in order.
	TradeSamples []TradeSample
}

// TradeSample is one request to encode on the TradeStream: a Submit or a Cancel.
type TradeSample struct {
	Name   string
	Submit *exchange.OrderRequest
	Cancel *exchange.CancelRef
}

// FillPair names a private session fixture and a History case returning the same fills.
type FillPair struct {
	Session string // ws/<Session>.jsonl, side private or trade
	Case    string // a Case whose result is a *exchange.FillPage
}

// Env is what a Case builds its request from: the adapters, made with the test credentials, and the
// fixed time.
type Env struct {
	Now         time.Time
	REST        exchange.PublicREST
	Account     exchange.Account
	History     exchange.History
	Credentials exchange.Credentials
}

// Case is one REST call to check.
type Case struct {
	Name  string
	build func(Env) (exchange.HTTPRequest, func(*exchange.HTTPResponse) (any, error), error)
}

// Call makes a Case from a method that builds an exchange.Call, for instance
//
//	conformance.Call("listing_perp", func(e conformance.Env) (exchange.Call[exchange.Page[symbol.Listing]], error) {
//		return e.REST.Listing(symbol.Perp)
//	})
func Call[T any](name string, f func(Env) (exchange.Call[T], error)) Case {
	return Case{Name: name, build: func(e Env) (exchange.HTTPRequest, func(*exchange.HTTPResponse) (any, error), error) {
		c, err := f(e)
		if err != nil {
			return exchange.HTTPRequest{}, nil, err
		}
		return c.Req, func(r *exchange.HTTPResponse) (any, error) { return c.Parse(r) }, nil
	}}
}

// Problem is one failed expectation.
type Problem struct {
	Check string // which check
	Where string // the fixture, case or route
	What  string
}

func (p Problem) String() string { return p.Check + ": " + p.Where + ": " + p.What }

// DefaultNow is Spec.Now when it is zero.
var DefaultNow = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// The test credentials. They are fake: no venue knows them. What matters is that none of their
// secret parts ever shows up in a request, a frame, an error or a golden file.
const (
	TestKey        = "conformance-api-key"
	TestHMACSecret = "conformance-hmac-secret-5f0e1d2c3b4a"
	TestPassphrase = "conformance-passphrase-9a8b"
)

// TestEd25519Key is the private key of the ed25519 test credentials, from a fixed seed.
var TestEd25519Key = ed25519.NewKeyFromSeed([]byte("conformance-ed25519-seed-0123456"))

// TestCredentials returns the test credentials of type k.
func TestCredentials(k exchange.KeyType, passphrase bool) exchange.Credentials {
	c := exchange.Credentials{Type: exchange.KeyHMAC, Key: TestKey, Secret: TestHMACSecret}
	if k == exchange.KeyEd25519 {
		der, err := x509.MarshalPKCS8PrivateKey(TestEd25519Key)
		if err != nil {
			panic(err)
		}
		c.Type = k
		c.Secret = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	}
	if passphrase {
		c.Passphrase = TestPassphrase
	}
	return c
}

// secrets are the substrings of c that must never leave the adapter. The API key is not one of
// them: it travels in a header. OKX also sends the passphrase, in OK-ACCESS-PASSPHRASE and in the
// login frame, so the passphrase is not one of them either. The signing secret must not appear.
func secrets(c exchange.Credentials) []string {
	var out []string
	if c.Type == exchange.KeyEd25519 {
		for _, line := range strings.Split(c.Secret, "\n") {
			if len(line) >= 16 && !strings.HasPrefix(line, "-----") {
				out = append(out, line)
			}
		}
		out = append(out, string(TestEd25519Key.Seed()))
	} else if c.Secret != "" {
		out = append(out, c.Secret)
	}
	return out
}

// Run runs every check the Spec has the parts for, each as a subtest.
func Run(t *testing.T, s Spec) {
	t.Helper()
	r, err := newRunner(s)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range r.checks() {
		t.Run(c.name, func(t *testing.T) {
			for _, p := range c.run() {
				t.Error(p)
			}
		})
	}
}

// Check runs every check and returns what failed, without a testing.T: for tests of the checks
// themselves, and for tools.
func Check(s Spec) ([]Problem, error) {
	r, err := newRunner(s)
	if err != nil {
		return nil, err
	}
	var out []Problem
	for _, c := range r.checks() {
		out = append(out, c.run()...)
	}
	return out, nil
}

type runner struct {
	spec  Spec
	fsys  fs.FS
	env   Env
	creds exchange.Credentials
	seen  []string // everything that left the adapter, for the credentials check

	sessions map[string]*decoded // by fixture name, decoded once
	results  map[string]any      // by case name, parsed once
	insts    []*symbol.Info
}

type check struct {
	name string
	run  func() []Problem
}

func newRunner(s Spec) (*runner, error) {
	if s.Venue == "" {
		return nil, fmt.Errorf("conformance: Spec.Venue is empty")
	}
	fsys := s.Fixtures
	if fsys == nil {
		if s.Dir == "" {
			return nil, fmt.Errorf("conformance: %s: neither Spec.Dir nor Spec.Fixtures", s.Venue)
		}
		if _, err := os.Stat(s.Dir); err != nil {
			return nil, fmt.Errorf("conformance: %s: %w", s.Venue, err)
		}
		fsys = os.DirFS(s.Dir)
	}
	if s.Now.IsZero() {
		s.Now = DefaultNow
	}
	if s.KeyType == "" {
		s.KeyType = exchange.KeyHMAC
	}
	r := &runner{spec: s, fsys: fsys, creds: TestCredentials(s.KeyType, s.Passphrase),
		sessions: map[string]*decoded{}, results: map[string]any{}}
	r.env = Env{Now: s.Now, REST: s.REST, Credentials: r.creds}
	cfg := exchange.AccountConfig{APIID: 1, Credentials: r.creds}
	if s.Account != nil {
		a, err := s.Account(cfg)
		if err != nil {
			return nil, fmt.Errorf("conformance: %s: Account: %w", s.Venue, err)
		}
		r.env.Account = a
	}
	if s.History != nil {
		h, err := s.History(cfg)
		if err != nil {
			return nil, fmt.Errorf("conformance: %s: History: %w", s.Venue, err)
		}
		r.env.History = h
	}
	return r, nil
}

// checks in the order they run: the credentials check last, over everything the others produced.
func (r *runner) checks() []check {
	return []check{
		{"cases", r.checkCases},
		{"listing", r.checkListing},
		{"sessions", r.checkSessions},
		{"unknown_frames", r.checkUnknown},
		{"capabilities", r.checkCaps},
		{"books", r.checkBooks},
		{"planning", r.checkPlanning},
		{"subscribe", r.checkSubscribe},
		{"private_dial", r.checkDial},
		{"trade_dial", r.checkTradeDial},
		{"trade_answers", r.checkTradeAnswers},
		{"fills", r.checkFills},
		{"credentials", r.checkCredentials},
	}
}

// saw keeps what left the adapter, for the credentials check.
func (r *runner) saw(parts ...string) { r.seen = append(r.seen, parts...) }

func (r *runner) sawRequest(q *exchange.HTTPRequest) {
	r.saw(q.Base, q.Path, string(q.Query), string(q.Body))
	for _, h := range q.Header {
		r.saw(h.Name, h.Value)
	}
}
