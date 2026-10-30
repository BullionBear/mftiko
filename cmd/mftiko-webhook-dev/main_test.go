package main

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func newTestReceiver(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "received.jsonl")
	at := time.Date(2026, 10, 30, 1, 2, 3, 4, time.FixedZone("x", 3600))
	srv := httptest.NewServer(&receiver{out: out, log: zerolog.Nop(), now: func() time.Time { return at }})
	t.Cleanup(srv.Close)
	return srv, out
}

func readLines(t *testing.T, path string) []map[string]json.RawMessage {
	t.Helper()
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var lines []map[string]json.RawMessage
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("line %q: %v", sc.Text(), err)
		}
		lines = append(lines, m)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return lines
}

func post(t *testing.T, url, body string) int {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestReceiverAppendsOneLinePerPost(t *testing.T) {
	srv, out := newTestReceiver(t)
	if code := post(t, srv.URL+"/alertmanager", "{\n  \"status\": \"firing\",\n  \"alerts\": [1, 2]\n}\n"); code != http.StatusOK {
		t.Fatalf("json post: %d", code)
	}
	if code := post(t, srv.URL+"/other", "not json\n"); code != http.StatusOK {
		t.Fatalf("text post: %d", code)
	}
	lines := readLines(t, out)
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
	want := []map[string]string{
		{"time": `"2026-10-30T00:02:03.000000004Z"`, "path": `"/alertmanager"`, "body": `{"status":"firing","alerts":[1,2]}`},
		{"time": `"2026-10-30T00:02:03.000000004Z"`, "path": `"/other"`, "body": `"not json\n"`},
	}
	for i, w := range want {
		for k, v := range w {
			if got := string(lines[i][k]); got != v {
				t.Errorf("line %d %s = %s, want %s", i, k, got, v)
			}
		}
	}
}

func TestReceiverHealthRecordsNothing(t *testing.T) {
	srv, out := newTestReceiver(t)
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET: %d", resp.StatusCode)
	}
	if lines := readLines(t, out); len(lines) != 0 {
		t.Fatalf("GET recorded %d lines", len(lines))
	}
}

func TestReceiverRefusesOtherMethodsAndLargeBodies(t *testing.T) {
	srv, out := newTestReceiver(t)
	req, err := http.NewRequest(http.MethodPut, srv.URL, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("Allow") != "GET, HEAD, POST" {
		t.Fatalf("PUT: %d, Allow %q", resp.StatusCode, resp.Header.Get("Allow"))
	}
	if code := post(t, srv.URL, `"`+strings.Repeat("x", maxBody)+`"`); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("large post: %d", code)
	}
	if lines := readLines(t, out); len(lines) != 0 {
		t.Fatalf("recorded %d lines", len(lines))
	}
}

func TestReceiverStartsAnotherFileAfterRemoval(t *testing.T) {
	srv, out := newTestReceiver(t)
	post(t, srv.URL, `{"n":1}`)
	if err := os.Remove(out); err != nil {
		t.Fatal(err)
	}
	post(t, srv.URL, `{"n":2}`)
	lines := readLines(t, out)
	if len(lines) != 1 || string(lines[0]["body"]) != `{"n":2}` {
		t.Fatalf("lines after removal: %v", lines)
	}
}

func TestProbe(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer ok.Close()
	notReady := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}))
	defer notReady.Close()
	gone := httptest.NewServer(http.NotFoundHandler())
	goneURL := gone.URL
	gone.Close()

	if err := probe(ok.URL, time.Second); err != nil {
		t.Errorf("2xx: %v", err)
	}
	if err := probe(notReady.URL, time.Second); err == nil || !strings.Contains(err.Error(), "503") {
		t.Errorf("503: %v", err)
	}
	if err := probe(goneURL, time.Second); err == nil {
		t.Error("closed server: no error")
	}
}
