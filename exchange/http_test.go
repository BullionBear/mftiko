package exchange

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// pages is a Doer that answers page n of three with its number, and records the request it got.
type pages struct {
	seen []string
	fail error
}

func (p *pages) Do(_ context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	p.seen = append(p.seen, string(req.Query))
	if p.fail != nil && len(p.seen) == 2 {
		return nil, p.fail
	}
	return &HTTPResponse{Status: 200, Header: []Header{{Name: "X-Used", Value: "7"}}, Body: req.Query}, nil
}

// page builds the request for page n, stamped with now as a signed request would be.
func page(n int, now time.Time) Call[Page[int]] {
	return Call[Page[int]]{
		Req: HTTPRequest{Method: "GET", Path: "/p", Query: []byte(strconv.Itoa(n) + "@" + strconv.FormatInt(now.Unix(), 10))},
		Parse: func(resp *HTTPResponse) (Page[int], error) {
			p := Page[int]{Items: []int{n}}
			if n < 2 {
				p.Next = func(now time.Time) (Call[Page[int]], error) { return page(n+1, now), nil }
			}
			return p, nil
		},
	}
}

func TestCollect(t *testing.T) {
	clock := int64(100)
	now := func() time.Time { clock++; return time.Unix(clock, 0) }
	d := &pages{}
	got, err := Collect(context.Background(), d, now, page(0, time.Unix(100, 0)))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != 0 || got[2] != 2 {
		t.Fatalf("items %v", got)
	}
	// Each following page is built with the time it goes out at.
	if want := []string{"0@100", "1@101", "2@102"}; len(d.seen) != 3 || d.seen[1] != want[1] || d.seen[2] != want[2] {
		t.Fatalf("requests %v, want %v", d.seen, want)
	}

	boom := errors.New("boom")
	if got, err := Collect(context.Background(), &pages{fail: boom}, now, page(0, now())); !errors.Is(err, boom) || got != nil {
		t.Fatalf("failed page: %v %v", got, err)
	}
	if _, err := Run(context.Background(), nil, page(0, now())); !errors.Is(err, ErrNoTransport) {
		t.Fatalf("nil Doer: %v", err)
	}
}

// lender answers every request with its query, lent: the body is scribbled over once use returns.
type lender struct {
	pages
	lent bool
}

func (l *lender) Lend(ctx context.Context, req *HTTPRequest, use func(*HTTPResponse) error) error {
	resp, err := l.Do(ctx, req)
	if err != nil {
		return err
	}
	resp.Body = append([]byte(nil), resp.Body...)
	l.lent = true
	err = use(resp)
	l.lent = false
	for i := range resp.Body {
		resp.Body[i] = 'x'
	}
	return err
}

func TestRunLends(t *testing.T) {
	l := &lender{}
	var body []byte
	c := Call[string]{
		Req: HTTPRequest{Method: "GET", Path: "/p", Query: []byte("q=1")},
		Parse: func(resp *HTTPResponse) (string, error) {
			if !l.lent {
				t.Error("Parse ran outside the loan")
			}
			body = resp.Body
			return string(resp.Body), nil
		},
	}
	got, err := Run(context.Background(), l, c)
	if err != nil || got != "q=1" {
		t.Fatalf("Run = %q, %v", got, err)
	}
	// What Parse copied stays; the response itself went back to the Lender.
	if string(body) != "xxx" {
		t.Fatalf("lent body after Run: %q", body)
	}
	boom := errors.New("boom")
	if _, err := Run(context.Background(), &lender{pages: pages{fail: boom, seen: []string{""}}}, c); !errors.Is(err, boom) {
		t.Fatalf("transport error through Lend: %v", err)
	}
	c.Parse = func(*HTTPResponse) (string, error) { return "partial", boom }
	if got, err := Run(context.Background(), l, c); !errors.Is(err, boom) || got != "partial" {
		t.Fatalf("Parse error through Lend: %q, %v", got, err)
	}
}

func TestRequestNeverFormatsHeaders(t *testing.T) {
	r := HTTPRequest{
		Method: "POST", Base: "https://www.okx.com", Path: "/api/v5/trade/order", Query: []byte("a=1"),
		Header: []Header{{Name: "OK-ACCESS-KEY", Value: "k-visible"}, {Name: "OK-ACCESS-PASSPHRASE", Value: "pp-secret"}},
		Body:   []byte(`{"instId":"BTC-USDT"}`),
	}
	want := `POST https://www.okx.com/api/v5/trade/order?a=1 [OK-ACCESS-KEY: REDACTED, OK-ACCESS-PASSPHRASE: REDACTED] {"instId":"BTC-USDT"}`
	if got := r.String(); got != want {
		t.Fatalf("String\n got %s\nwant %s", got, want)
	}

	var buf bytes.Buffer
	log := zerolog.New(&buf)
	log.Info().Interface("req", r).Interface("ptr", &r).Stringer("s", r).Msg("x")
	j, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	c := Call[int]{Req: r}
	for _, out := range []string{
		fmt.Sprint(r), fmt.Sprint(&r), fmt.Sprintf("%v %+v %#v %s %q %x", r, r, r, r, r, r),
		fmt.Sprintf("%v %+v %#v", c, c, c), string(j), buf.String(),
	} {
		for _, secret := range []string{"k-visible", "pp-secret"} {
			if strings.Contains(out, secret) {
				t.Errorf("leaked %q in %s", secret, out)
			}
		}
		if !strings.Contains(out, "OK-ACCESS-PASSPHRASE: REDACTED") {
			t.Errorf("no masked header in %s", out)
		}
	}
}

func TestResponseGet(t *testing.T) {
	r := &HTTPResponse{Header: []Header{{Name: "X-MBX-USED-WEIGHT-1M", Value: "12"}, {Name: "x-mbx-used-weight-1m", Value: "13"}}}
	if got := r.Get("x-mbx-used-weight-1m"); got != "12" {
		t.Fatalf("Get = %q", got)
	}
	if got := r.Get("Retry-After"); got != "" {
		t.Fatalf("Get missing = %q", got)
	}
}
