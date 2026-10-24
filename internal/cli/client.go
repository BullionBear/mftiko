package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	openapi_types "github.com/oapi-codegen/runtime/types"
)

const maxBody = 8 << 20

// Endpoint is one configured API. URL is the generated client's server and
// includes /api/v1. Timeout must be positive.
type Endpoint struct {
	URL          string
	Token        string
	CAFile       string
	InsecureHTTP bool
	Timeout      time.Duration
}

// Result is one HTTP response. Body is limited to maxBody.
type Result struct {
	Status    int
	Body      []byte
	RequestID string
}

type usageError struct{ msg string }

func (e *usageError) Error() string { return e.msg }

func usage(msg string) error { return &usageError{msg: msg} }

func isUsage(err error) bool {
	var u *usageError
	return errors.As(err, &u)
}

// UserAgent is the value sent on every request (E11-D008).
func UserAgent() string {
	return "mftiko-cli/" + buildinfo.Version
}

// API is the HTTP client. It sends Authorization: Bearer and User-Agent, and
// does not add Content-Type (E10-D110, E11-D008). It does not store cookies.
type API struct {
	token string
	ua    string
	hc    *http.Client
	gen   *apiv1.Client
}

// Dial builds a client. Plain HTTP is refused unless InsecureHTTP is set
// (E11-D005). When CAFile is set, that PEM is the only trust root; the system
// pool is not added, and the host name is still checked. Verification is never
// skipped. The generated client joins paths against the server URL, so the
// server passed to it ends in /api/v1/.
func Dial(e Endpoint) (*API, error) {
	raw, tr, err := prepare(e)
	if err != nil {
		return nil, err
	}
	if tokenInvalid(e.Token) {
		return nil, usage("token must be one line")
	}
	a := &API{token: e.Token, ua: UserAgent()}
	a.hc = &http.Client{
		Timeout:       e.Timeout,
		Transport:     tr,
		CheckRedirect: refuseRedirect,
	}
	gen, err := apiv1.NewClient(raw, apiv1.WithHTTPClient(a.hc), apiv1.WithRequestEditorFn(a.edit))
	if err != nil {
		return nil, usage("context url is not usable")
	}
	a.gen = gen
	return a, nil
}

// prepare checks everything except the token. login calls it before reading
// stdin. The returned URL ends in /api/v1/ so the generated client requests
// /api/v1/<operation> rather than /api/<operation>.
func prepare(e Endpoint) (string, *http.Transport, error) {
	if e.Timeout <= 0 {
		return "", nil, usage("timeout must be positive")
	}
	u, err := url.Parse(strings.TrimSpace(e.URL))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "", nil, usage("context url must be an http or https URL with a host")
	}
	if u.User != nil {
		return "", nil, usage("context url must not include userinfo")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", nil, usage("context url must not include a query or fragment")
	}
	if strings.TrimSuffix(u.Path, "/") != "/api/v1" {
		return "", nil, usage("context url path must be /api/v1")
	}
	if u.Scheme == "http" && !e.InsecureHTTP {
		return "", nil, usage("context url is http; set insecure_http: true to allow plaintext")
	}
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return "", nil, usage("HTTP transport is not available")
	}
	tr := base.Clone()
	if u.Scheme == "https" && e.CAFile != "" {
		pemBytes, err := os.ReadFile(e.CAFile)
		if err != nil {
			return "", nil, usage("cannot read ca_file")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return "", nil, usage("ca_file has no PEM certificate")
		}
		tr.TLSClientConfig = &tls.Config{RootCAs: pool}
	}
	u.Path = "/api/v1/"
	u.RawPath = ""
	return u.String(), tr, nil
}

func refuseRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// edit sets the bearer token and the user agent. It does not set Content-Type:
// a request without a body must not gain one (E10-D110, E11-D008).
func (a *API) edit(_ context.Context, req *http.Request) error {
	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("User-Agent", a.ua)
	return nil
}

// GetMe calls GET /auth/me.
func (a *API) GetMe(ctx context.Context) (Result, error) {
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.GetMe(ctx)
	})
}

// ListStrategies calls GET /strategies. limit is the page size sent to the API
// (E11-D010). An empty cursor omits the query parameter.
func (a *API) ListStrategies(ctx context.Context, limit int, cursor string) (Result, error) {
	params := apiv1.ListStrategiesParams{Limit: &limit}
	if cursor != "" {
		params.Cursor = &cursor
	}
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.ListStrategies(ctx, &params)
	})
}

// ListStrategyVersions calls GET /strategies/{id}/versions.
func (a *API) ListStrategyVersions(ctx context.Context, id string, limit int, cursor string) (Result, error) {
	params := apiv1.ListStrategyVersionsParams{Limit: &limit}
	if cursor != "" {
		params.Cursor = &cursor
	}
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.ListStrategyVersions(ctx, id, &params)
	})
}

// GetStrategyVersion calls GET /strategies/{id}/versions/{ref}.
func (a *API) GetStrategyVersion(ctx context.Context, id, ref string) (Result, error) {
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.GetStrategyVersion(ctx, id, ref)
	})
}

// ListInstances calls GET /instances. There is no plane filter and no page.
func (a *API) ListInstances(ctx context.Context) (Result, error) {
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.ListInstances(ctx, nil)
	})
}

// GetInstance calls GET /instances/{name}.
func (a *API) GetInstance(ctx context.Context, name string) (Result, error) {
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.GetInstance(ctx, name)
	})
}

// ListAccountKinds calls GET /account-kinds.
func (a *API) ListAccountKinds(ctx context.Context) (Result, error) {
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.ListAccountKinds(ctx)
	})
}

// ListAccounts calls GET /accounts.
func (a *API) ListAccounts(ctx context.Context, params *apiv1.ListAccountsParams) (Result, error) {
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.ListAccounts(ctx, params)
	})
}

// CreateAccount calls POST /accounts.
func (a *API) CreateAccount(ctx context.Context, body apiv1.AccountCreate) (Result, error) {
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.CreateAccount(ctx, body)
	})
}

// GetAccount calls GET /accounts/{api_id}.
func (a *API) GetAccount(ctx context.Context, id apiv1.APIID) (Result, error) {
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.GetAccount(ctx, id)
	})
}

// UpdateAccount calls PUT /accounts/{api_id}.
func (a *API) UpdateAccount(ctx context.Context, id apiv1.APIID, body apiv1.AccountUpdate) (Result, error) {
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.UpdateAccount(ctx, id, body)
	})
}

// DeleteAccount calls DELETE /accounts/{api_id}. The generated client sends no
// Content-Type (E10-D110).
func (a *API) DeleteAccount(ctx context.Context, id apiv1.APIID, rev apiv1.Revision) (Result, error) {
	params := &apiv1.DeleteAccountParams{Revision: rev}
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.DeleteAccount(ctx, id, params)
	})
}

// ListSessions calls GET /sessions.
func (a *API) ListSessions(ctx context.Context, params *apiv1.ListSessionsParams) (Result, error) {
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.ListSessions(ctx, params)
	})
}

// GetSession calls GET /sessions/{id}.
func (a *API) GetSession(ctx context.Context, id apiv1.SessionID) (Result, error) {
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.GetSession(ctx, id)
	})
}

// EndSession calls POST /sessions/{id}/end. The generated client sends no body
// and no Content-Type (E10-D110).
func (a *API) EndSession(ctx context.Context, id apiv1.SessionID) (Result, error) {
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.EndSession(ctx, id)
	})
}

// DeleteSession calls DELETE /sessions/{id}. The generated client sends no
// Content-Type (E10-D110).
func (a *API) DeleteSession(ctx context.Context, id apiv1.SessionID) (Result, error) {
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.DeleteSession(ctx, id)
	})
}

// ListSessionRecords calls GET /session-records.
func (a *API) ListSessionRecords(ctx context.Context, params *apiv1.ListSessionRecordsParams) (Result, error) {
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.ListSessionRecords(ctx, params)
	})
}

// GetSessionRecord calls GET /session-records/{record_id}.
func (a *API) GetSessionRecord(ctx context.Context, id openapi_types.UUID) (Result, error) {
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.GetSessionRecord(ctx, id)
	})
}

// logout calls POST /auth/logout, which has no body. Tests use it to check
// that Content-Type stays unset.
func (a *API) logout(ctx context.Context) (Result, error) {
	return a.roundTrip(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.gen.Logout(ctx)
	})
}

func (a *API) roundTrip(ctx context.Context, fn func(context.Context) (*http.Response, error)) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	resp, err := fn(ctx)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return Result{}, err
	}
	if len(body) > maxBody {
		return Result{}, errors.New("response is too large")
	}
	return Result{Status: resp.StatusCode, Body: body, RequestID: resp.Header.Get("X-Request-Id")}, nil
}

func tokenInvalid(s string) bool {
	if s == "" {
		return true
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f || unicode.Is(unicode.Cc, r) {
			return true
		}
	}
	return false
}
