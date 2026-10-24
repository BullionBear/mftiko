// Package apitest is the fake API server for the mftiko CLI (E11-02).
// It is a contract/gen/api/v1 StrictServerInterface behind NewStrictHandler
// and httptest. Later CLI tickets reuse it.
package apitest

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// Call is one request the CLI made.
type Call struct {
	Operation     string
	Method        string
	Path          string
	Authorization string
	UserAgent     string
	ContentType   string
	Cookie        string
	RawQuery      string
	// Query is the same string as RawQuery. Account tests read it.
	Query string
	// Body is the request body. Tests use it to check credential bytes.
	// Neither field is logged.
	Body []byte
	// IdempotencyKey is the Idempotency-Key header, empty when absent.
	IdempotencyKey string
}

// Option is fixed before the server accepts a connection.
type Option struct {
	// Token is the bearer value GetMe accepts. A mismatch is 401 unauthenticated.
	// The value is not copied into the response.
	Token string
	Name  string
	// Expires is copied onto Me. A nil pointer is JSON null, which is the bearer case.
	Expires *time.Time
	// Problem, when set, is the body of GET /auth/me. Status 401 with code
	// unauthenticated uses the generated response type. Any other problem is
	// written by the strict handler's error callback so the HTTP status can
	// differ from the body's code.
	Problem *apiv1.Problem
	// RequestID is set on X-Request-Id when not empty.
	RequestID string
	// Block makes GET /auth/me wait until the request context ends.
	Block bool
	// EchoAuthorization puts the Authorization header in the 401 detail.
	// Redaction tests use it. The server does not log the value.
	EchoAuthorization bool
	// TLS serves HTTPS. CertPEM returns the certificate the client should trust.
	TLS bool
	// Catalog, when set, serves the read-only lists. Nil keeps those routes
	// on the account fixtures below. Set it before Start; the handler does
	// not mutate it.
	Catalog *Catalog
	// Kinds, Instances, and Accounts are the account fixtures. They are copied
	// before the server accepts a connection. They apply when Catalog is nil.
	Kinds     []apiv1.AccountKind
	Instances []apiv1.Instance
	Accounts  []apiv1.Account
	// PageSize, when set, caps an account, session, or record list page below
	// the requested limit so a test can follow cursors. 0 means honor the
	// request, up to 1000. Strategy pages use Catalog.PageSize.
	PageSize int
	// Freshness is copied onto the first account or session list page.
	// NextFreshness, when set, is copied onto every later page.
	Freshness     apiv1.Freshness
	NextFreshness *apiv1.Freshness
	// ConflictCAS makes update and delete return 409 conflict on /revision.
	ConflictCAS bool
	// EchoCredential makes create return 400 whose detail is the secret.
	// Redaction tests use it. The server does not log the value.
	EchoCredential bool
	// Sessions, Records, and Agents are the session fixtures. They are copied
	// before the server accepts a connection. A name in Agents is returned by
	// GET /instances/{name} ahead of the catalog and account fixtures, so a
	// test can set AgentRecord.last_known.
	Sessions []apiv1.SessionDetail
	Records  []apiv1.SessionRecord
	Agents   map[string]apiv1.AgentStatus
	// DropCreates closes the connection on the first N POST /sessions after
	// the body is read, before the generated decoder runs.
	DropCreates int
	// CreateFields, when non-nil, makes POST /sessions return 400
	// validation_failed with these errors and stores no session.
	CreateFields []apiv1.FieldError
	// StorageUnavailable makes POST /sessions return 503 storage_unavailable.
	StorageUnavailable bool
	// AlgoOverlap makes POST /sessions return 409 algo_overlap.
	AlgoOverlap bool
}

// Server is a started httptest server.
type Server struct {
	mu           sync.Mutex
	once         sync.Once
	hs           *httptest.Server
	stop         chan struct{}
	token        string
	name         string
	expires      *time.Time
	problem      *apiv1.Problem
	reqID        string
	block        bool
	echo         bool
	cat          *Catalog
	hits         int
	calls        []Call
	lastAuth     string
	lastBody     []byte
	kinds        []apiv1.AccountKind
	instances    map[string]apiv1.InstanceDetail
	accounts     []accountRow
	nextID       int64
	pageSize     int
	fresh        apiv1.Freshness
	nextFresh    *apiv1.Freshness
	listPages    int
	conflict     bool
	echoSecret   bool
	sessions     []apiv1.SessionDetail
	records      []apiv1.SessionRecord
	agents       map[string]apiv1.AgentStatus
	sessionPages int
	dropLeft     int
	createFields []apiv1.FieldError
	storageDown  bool
	algoOverlap  bool
	created      map[string]savedSession
	createdOrder []string
	nextSession  int
}

type savedSession struct {
	id   string
	body []byte
}

// Start serves opt. Every field is set before the server goroutine runs.
func Start(opt Option) *Server {
	s := &Server{
		stop:    make(chan struct{}),
		token:   opt.Token,
		name:    opt.Name,
		expires: opt.Expires,
		problem: opt.Problem,
		reqID:   opt.RequestID,
		block:   opt.Block,
		echo:    opt.EchoAuthorization,
		cat:     opt.Catalog,
	}
	s.install(opt)
	h := s.handler()
	if opt.TLS {
		cert, err := leafCert()
		if err != nil {
			panic(err)
		}
		s.hs = httptest.NewUnstartedServer(h)
		s.hs.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
		s.hs.StartTLS()
	} else {
		s.hs = httptest.NewServer(h)
	}
	return s
}

// leafCert is a unique localhost certificate. httptest's shared certificate
// would make two servers trust each other's ca_file.
func leafCert() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "mftiko-apitest"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		DNSNames:              []string{"localhost"},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}

// Close stops the server and unblocks a handler waiting in Block.
func (s *Server) Close() {
	s.once.Do(func() { close(s.stop) })
	s.hs.Close()
}

// URL is the generated client's server, including /api/v1 and no trailing slash.
func (s *Server) URL() string {
	return strings.TrimRight(s.hs.URL, "/") + "/api/v1"
}

// CertPEM is the TLS certificate, or nil when the server is plain HTTP.
func (s *Server) CertPEM() []byte {
	c := s.hs.Certificate()
	if c == nil {
		return nil
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Raw})
}

// Calls returns the requests seen so far.
func (s *Server) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Call, len(s.calls))
	for i, c := range s.calls {
		c.Body = append([]byte(nil), c.Body...)
		out[i] = c
	}
	return out
}

// LastBody is the body of the last response.
func (s *Server) LastBody() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.lastBody...)
}

func (s *Server) handler() http.Handler {
	strict := apiv1.NewStrictHandlerWithOptions(&api{s: s}, []apiv1.StrictMiddlewareFunc{s.middleware}, apiv1.StrictHTTPServerOptions{
		RequestErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			http.Error(w, "bad request", http.StatusBadRequest)
		},
		ResponseErrorHandlerFunc: s.onError,
	})
	inner := apiv1.HandlerWithOptions(strict, apiv1.StdHTTPServerOptions{BaseURL: "/api/v1"})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := readReqBody(r)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if s.takeDrop(r, raw) {
			hijackClose(w)
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), bodyKey, raw))
		s.mu.Lock()
		s.lastBody = nil
		s.mu.Unlock()
		cw := &capture{ResponseWriter: w, s: s}
		inner.ServeHTTP(cw, r)
	})
}

func readReqBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	defer r.Body.Close()
	b, err := io.ReadAll(io.LimitReader(r.Body, 8<<20+1))
	if err != nil {
		return nil, err
	}
	r.Body = io.NopCloser(bytes.NewReader(b))
	return append([]byte(nil), b...), nil
}

func (s *Server) middleware(next apiv1.StrictHandlerFunc, operationID string) apiv1.StrictHandlerFunc {
	return func(ctx context.Context, w http.ResponseWriter, r *http.Request, request any) (any, error) {
		call := Call{
			Operation:      operationID,
			Method:         r.Method,
			Path:           r.URL.Path,
			Authorization:  r.Header.Get("Authorization"),
			UserAgent:      r.Header.Get("User-Agent"),
			ContentType:    r.Header.Get("Content-Type"),
			Cookie:         r.Header.Get("Cookie"),
			RawQuery:       r.URL.RawQuery,
			Query:          r.URL.RawQuery,
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
		}
		if raw, ok := ctx.Value(bodyKey).([]byte); ok {
			call.Body = append([]byte(nil), raw...)
		}
		s.mu.Lock()
		s.calls = append(s.calls, call)
		s.lastAuth = call.Authorization
		block := s.block
		if s.reqID != "" {
			w.Header().Set("X-Request-Id", s.reqID)
		}
		s.mu.Unlock()
		if block {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-s.stop:
				return nil, errors.New("stopped")
			}
		}
		ctx = context.WithValue(ctx, authKey, call.Authorization)
		return next(ctx, w, r, request)
	}
}

func (s *Server) onError(w http.ResponseWriter, _ *http.Request, err error) {
	if !errors.Is(err, errProblem) || s.problem == nil {
		http.Error(w, "unused", http.StatusInternalServerError)
		return
	}
	p := *s.problem
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

var (
	errUnused  = errors.New("not used")
	errProblem = errors.New("problem")
)

type api struct{ s *Server }

func (a *api) GetMe(context.Context, apiv1.GetMeRequestObject) (apiv1.GetMeResponseObject, error) {
	s := a.s
	if s.echo {
		s.mu.Lock()
		auth := s.lastAuth
		s.mu.Unlock()
		return unauth(problem(http.StatusUnauthorized, apiv1.ErrorCodeUnauthenticated, "Unauthorized", auth)), nil
	}
	if s.problem != nil {
		p := *s.problem
		if p.Code == apiv1.ErrorCodeUnauthenticated && p.Status == http.StatusUnauthorized {
			return unauth(p), nil
		}
		return nil, errProblem
	}
	s.mu.Lock()
	auth := s.lastAuth
	s.mu.Unlock()
	if auth != "Bearer "+s.token {
		return unauth(problem(http.StatusUnauthorized, apiv1.ErrorCodeUnauthenticated, "Unauthorized", "bearer token was not accepted")), nil
	}
	me := apiv1.Me{Name: s.name, ExpiresAt: s.expires}
	return apiv1.GetMe200JSONResponse(me), nil
}

func (a *api) Logout(context.Context, apiv1.LogoutRequestObject) (apiv1.LogoutResponseObject, error) {
	return apiv1.Logout204Response{}, nil
}

func (a *api) Login(context.Context, apiv1.LoginRequestObject) (apiv1.LoginResponseObject, error) {
	return nil, errUnused
}

func (a *api) StreamEvents(context.Context, apiv1.StreamEventsRequestObject) (apiv1.StreamEventsResponseObject, error) {
	return nil, errUnused
}
func (a *api) CreateSession(ctx context.Context, req apiv1.CreateSessionRequestObject) (apiv1.CreateSessionResponseObject, error) {
	return a.createSession(ctx, req)
}
func unauth(p apiv1.Problem) apiv1.GetMe401ApplicationProblemPlusJSONResponse {
	return apiv1.GetMe401ApplicationProblemPlusJSONResponse{
		UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(p),
	}
}

func problem(status int, code apiv1.ErrorCode, title, detail string) apiv1.Problem {
	return apiv1.Problem{
		Type:   "about:blank",
		Title:  title,
		Status: status,
		Code:   code,
		Detail: &detail,
	}
}

// capture records the response body before the bytes are written to the connection.
type capture struct {
	http.ResponseWriter
	s *Server
}

func (c *capture) Write(p []byte) (int, error) {
	c.s.mu.Lock()
	c.s.lastBody = append(c.s.lastBody, p...)
	c.s.mu.Unlock()
	return c.ResponseWriter.Write(p)
}
