package fasthttp_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/transport/fasthttp"
)

func newDoer(timeout time.Duration) *fasthttp.Doer {
	return fasthttp.New(nil, fasthttp.Options{Timeout: timeout})
}

func get(base, path string) exchange.HTTPRequest {
	return exchange.HTTPRequest{Method: "GET", Base: base, Path: path}
}

func withTimeout(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// rawServer answers every request with answer and sends what it read, head and body, to the channel.
func rawServer(t *testing.T, answer string) (string, <-chan string) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	raw := make(chan string, 4)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				br := bufio.NewReader(conn)
				for {
					var head strings.Builder
					size := 0
					for {
						line, err := br.ReadString('\n')
						if err != nil {
							return
						}
						head.WriteString(line)
						if line == "\r\n" {
							break
						}
						if v, ok := strings.CutPrefix(strings.ToLower(line), "content-length: "); ok {
							size, _ = strconv.Atoi(strings.TrimSpace(v))
						}
					}
					body := make([]byte, size)
					if _, err := io.ReadFull(br, body); err != nil {
						return
					}
					raw <- head.String() + string(body)
					io.WriteString(conn, answer)
				}
			}()
		}
	}()
	return "http://" + ln.Addr().String(), raw
}

// The request goes out as the adapter built it: method, path, encoded query, header names as spelled
// (net/http's server would hide them by canonicalizing), the signed body, and no Content-Type it did
// not set.
func TestRequestAsBuilt(t *testing.T) {
	base, raw := rawServer(t, "HTTP/1.1 200 OK\r\nX-MBX-USED-WEIGHT-1M: 7\r\nContent-Length: 2\r\n\r\nok")
	d := newDoer(0)

	body := []byte(`{"instId":"BTC-USDT","sz":"0.001"}`)
	req := exchange.HTTPRequest{
		Method: "POST", Base: base, Path: "/api/v3/order",
		Query: []byte("symbol=BTCUSDT&signature=ab%2Fcd%3D&ids=%5B%221%22%5D"),
		Header: []exchange.Header{
			{Name: "X-MBX-APIKEY", Value: "key"}, {Name: "x-bapi-api-key", Value: "b"},
			{Name: "Content-Type", Value: "application/json"},
		},
		Body: body, Mutates: true,
	}
	resp, err := d.Do(withTimeout(t, 5*time.Second), &req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != 200 || string(resp.Body) != "ok" || resp.Get("x-mbx-used-weight-1m") != "7" {
		t.Fatalf("response %d %q %v", resp.Status, resp.Body, resp.Header)
	}
	got := <-raw
	for _, want := range []string{
		"POST /api/v3/order?symbol=BTCUSDT&signature=ab%2Fcd%3D&ids=%5B%221%22%5D HTTP/1.1\r\n",
		"\r\nX-MBX-APIKEY: key\r\n",
		"\r\nx-bapi-api-key: b\r\n",
		"\r\nContent-Type: application/json\r\n",
		"\r\n\r\n" + string(body),
	} {
		if !strings.Contains(got, want) {
			t.Errorf("request lacks %q:\n%s", want, got)
		}
	}

	req = exchange.HTTPRequest{Method: "POST", Base: base, Path: "/x", Body: []byte("a=1")}
	if _, err := d.Do(withTimeout(t, 5*time.Second), &req); err != nil {
		t.Fatal(err)
	}
	if got := <-raw; strings.Contains(strings.ToLower(got), "content-type") {
		t.Errorf("a Content-Type the request did not set:\n%s", got)
	}
}

// pageCall asks for page n; the server says whether there is another. Each request carries the time it
// was built at, as a signed one would.
func pageCall(base string, n int, now time.Time) exchange.Call[exchange.Page[string]] {
	return exchange.Call[exchange.Page[string]]{
		Req: exchange.HTTPRequest{
			Method: "GET", Base: base, Path: "/list",
			Query: []byte("page=" + strconv.Itoa(n) + "&ts=" + strconv.FormatInt(now.UnixMilli(), 10)),
		},
		Parse: func(resp *exchange.HTTPResponse) (exchange.Page[string], error) {
			if resp.Status != 200 {
				return exchange.Page[string]{}, errors.New("status " + strconv.Itoa(resp.Status))
			}
			item, more, _ := strings.Cut(string(resp.Body), ";")
			p := exchange.Page[string]{Items: []string{item}}
			if more == "more" {
				p.Next = func(now time.Time) (exchange.Call[exchange.Page[string]], error) {
					return pageCall(base, n+1, now), nil
				}
			}
			return p, nil
		},
	}
}

func TestCollectPages(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.URL.RawQuery)
		n, _ := strconv.Atoi(r.URL.Query().Get("page"))
		body := "item" + strconv.Itoa(n)
		if n < 3 {
			body += ";more"
		}
		io.WriteString(w, body)
	}))
	defer srv.Close()
	d := newDoer(0)
	clock := time.UnixMilli(1_000)
	now := func() time.Time { clock = clock.Add(time.Millisecond); return clock }

	got, err := exchange.Collect(withTimeout(t, 5*time.Second), d, now, pageCall(srv.URL, 0, clock))
	if err != nil {
		t.Fatal(err)
	}
	// The items outlive the responses they were parsed from, which went back to the pool.
	if strings.Join(got, ",") != "item0,item1,item2,item3" {
		t.Fatalf("items %v", got)
	}
	want := []string{"page=0&ts=1000", "page=1&ts=1001", "page=2&ts=1002", "page=3&ts=1003"}
	if strings.Join(seen, " ") != strings.Join(want, " ") {
		t.Fatalf("requests %v, want %v", seen, want)
	}
}

// A 4xx is an answer for Parse to judge; a 5xx is ErrUnknownOutcome and never reaches Parse.
func TestStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		code, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/"))
		w.WriteHeader(code)
		io.WriteString(w, `{"code":-2010}`)
	}))
	defer srv.Close()
	d := newDoer(0)
	parsed := 0
	call := func(code int) exchange.Call[int] {
		return exchange.Call[int]{
			Req: get(srv.URL, "/"+strconv.Itoa(code)),
			Parse: func(resp *exchange.HTTPResponse) (int, error) {
				parsed++
				if string(resp.Body) != `{"code":-2010}` {
					t.Errorf("body %q", resp.Body)
				}
				return resp.Status, nil
			},
		}
	}
	for _, code := range []int{400, 418, 429} {
		if got, err := exchange.Run(withTimeout(t, 5*time.Second), d, call(code)); err != nil || got != code {
			t.Errorf("%d: %d, %v", code, got, err)
		}
	}
	for _, code := range []int{500, 502, 503, 504} {
		if _, err := exchange.Run(withTimeout(t, 5*time.Second), d, call(code)); !errors.Is(err, exchange.ErrUnknownOutcome) {
			t.Errorf("%d: %v, want ErrUnknownOutcome", code, err)
		}
	}
	if parsed != 3 {
		t.Fatalf("Parse ran %d times, want 3", parsed)
	}
}

// A request still out at the deadline may have reached the venue: ErrUnknownOutcome, at the deadline
// and not after the server answers. A context with no deadline gets Options.Timeout.
func TestTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-time.After(10 * time.Second):
		}
	}))
	defer srv.Close()
	defer close(release)

	for name, tc := range map[string]struct {
		d       *fasthttp.Doer
		timeout time.Duration // of the context; 0 = none
	}{
		"ctx deadline":    {newDoer(time.Minute), 150 * time.Millisecond},
		"Options.Timeout": {newDoer(150 * time.Millisecond), 0},
	} {
		ctx := context.Background()
		if tc.timeout > 0 {
			ctx = withTimeout(t, tc.timeout)
		}
		start := time.Now()
		req := get(srv.URL, "/slow")
		_, err := tc.d.Do(ctx, &req)
		if !errors.Is(err, exchange.ErrUnknownOutcome) {
			t.Errorf("%s: %v, want ErrUnknownOutcome", name, err)
		}
		if el := time.Since(start); el < 100*time.Millisecond || el > 5*time.Second {
			t.Errorf("%s: returned after %v", name, el)
		}
	}
}

// A connection that drops after the request went out is ErrUnknownOutcome, and the request is sent
// once: the transport does not retry, not even a GET.
func TestDisconnect(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		if r.URL.Path == "/partial" {
			io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nshort")
		}
		conn.Close()
	}))
	defer srv.Close()
	d := newDoer(0)
	for _, req := range []exchange.HTTPRequest{
		get(srv.URL, "/drop"),
		{Method: "POST", Base: srv.URL, Path: "/drop", Body: []byte("x=1"), Mutates: true},
		get(srv.URL, "/partial"),
	} {
		hits.Store(0)
		_, err := d.Do(withTimeout(t, 5*time.Second), &req)
		if !errors.Is(err, exchange.ErrUnknownOutcome) {
			t.Errorf("%s %s: %v, want ErrUnknownOutcome", req.Method, req.Path, err)
		}
		if n := hits.Load(); n != 1 {
			t.Errorf("%s %s: sent %d times", req.Method, req.Path, n)
		}
	}
}

// What certainly did not go out is a plain error, never ErrUnknownOutcome.
func TestNotSent(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := "http://" + ln.Addr().String()
	ln.Close()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	past, cancelPast := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelPast()

	d := newDoer(0)
	for name, tc := range map[string]struct {
		ctx context.Context
		req exchange.HTTPRequest
	}{
		"refused":         {withTimeout(t, 5*time.Second), get(closed, "/")},
		"canceled":        {canceled, get(closed, "/")},
		"deadline passed": {past, get(closed, "/")},
		"not http":        {withTimeout(t, 5*time.Second), get("ftp://127.0.0.1", "/")},
		"unresolvable":    {withTimeout(t, 5*time.Second), get("http://venue.invalid", "/")},
	} {
		_, err := d.Do(tc.ctx, &tc.req)
		if err == nil || errors.Is(err, exchange.ErrUnknownOutcome) {
			t.Errorf("%s: %v, want a plain error", name, err)
		}
	}
}

// Do hands over a copy: it stays as it was while later requests reuse the pool.
func TestDoCopies(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Path", r.URL.Path)
		io.WriteString(w, strings.Repeat(r.URL.Path, 100))
	}))
	defer srv.Close()
	d := newDoer(0)
	req := get(srv.URL, "/a")
	first, err := d.Do(withTimeout(t, 5*time.Second), &req)
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		req := get(srv.URL, "/b")
		if _, err := d.Do(withTimeout(t, 5*time.Second), &req); err != nil {
			t.Fatal(err)
		}
	}
	if string(first.Body) != strings.Repeat("/a", 100) || first.Get("X-Path") != "/a" {
		t.Fatalf("first response changed: %q %v", first.Body[:8], first.Header)
	}
}
