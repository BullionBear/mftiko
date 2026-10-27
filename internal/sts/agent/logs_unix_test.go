//go:build unix

package agent

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"archive/tar"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	logv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/log/v1"
)

const (
	logTestID = "a1b2c3"
	emptySHA  = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

func newLogTest(t *testing.T, cfg LogConfig) (*logServer, *logView, *httptest.Server) {
	t.Helper()
	if cfg.Listen == "" {
		cfg.Listen = "127.0.0.1:0"
	}
	if cfg.TokenSHA256 == "" {
		cfg.TokenSHA256 = logTokenHex("log")
	}
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = time.Hour
	}
	cfg.apply()
	tok, err := parseLogToken(cfg.TokenSHA256)
	if err != nil {
		t.Fatal(err)
	}
	view := newLogView()
	p := &plane{
		log:     zerolog.New(io.Discard),
		workDir: t.TempDir(),
		logView: view,
	}
	srv := newLogServer(p, cfg, tok, "host.example")
	view.setSpecs(map[string]struct{}{logTestID: {}})
	ts := httptest.NewServer(srv.routes())
	t.Cleanup(ts.Close)
	return srv, view, ts
}

func writeStream(t *testing.T, srv *logServer, id, stream, body string) string {
	t.Helper()
	path := srv.path(id, stream)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func logDo(t *testing.T, base, path, token string) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, b
}

func readLogFrame(r *bufio.Reader) (*logv1.Frame, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	if n > 1<<20 {
		return nil, fmt.Errorf("log frame %d", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	fr := &logv1.Frame{}
	if len(body) > 0 {
		if err := proto.Unmarshal(body, fr); err != nil {
			return nil, err
		}
	}
	return fr, nil
}

func decodeLogFrames(b []byte) ([]*logv1.Frame, error) {
	r := bufio.NewReader(bytes.NewReader(b))
	var out []*logv1.Frame
	for {
		fr, err := readLogFrame(r)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, fr)
	}
}

func mustFrames(t *testing.T, b []byte) []*logv1.Frame {
	t.Helper()
	frames, err := decodeLogFrames(b)
	if err != nil {
		t.Fatal(err)
	}
	return frames
}

func concatLogData(frames []*logv1.Frame) []byte {
	var b []byte
	for _, fr := range frames {
		b = append(b, fr.GetData()...)
	}
	return b
}

func tailBytes(file []byte, lines int) []byte {
	if lines <= 0 || len(file) == 0 {
		return nil
	}
	need := lines
	atEnd := true
	for j := len(file) - 1; j >= 0; j-- {
		if atEnd {
			atEnd = false
			if file[j] == '\n' {
				continue
			}
		}
		if file[j] == '\n' {
			need--
			if need == 0 {
				return file[j+1:]
			}
		}
	}
	return file
}

func onlyNext(fr *logv1.Frame) bool {
	return fr.GetNext() != "" && len(fr.GetData()) == 0 && !fr.GetGap() && !fr.GetEof() && !fr.GetFinal() && !fr.GetRemoved()
}

func TestAdvertiseAddr(t *testing.T) {
	if got := advertiseAddr("h.example", "", "127.0.0.1:7442"); got != "h.example:7442" {
		t.Fatal(got)
	}
	if got := advertiseAddr("h.example", "pub.example:9", "127.0.0.1:7442"); got != "pub.example:9" {
		t.Fatal(got)
	}
	if got := advertiseAddr("", "", "[::]:7442"); got != ":7442" {
		t.Fatal(got)
	}
	if _, _, err := net.SplitHostPort(advertiseAddr("h.example", "", "127.0.0.1:1")); err != nil {
		t.Fatal(err)
	}
}

func TestLogAuthAndQuery(t *testing.T) {
	_, _, ts := newLogTest(t, LogConfig{})
	empty := sha256.Sum256(nil)
	if hex.EncodeToString(empty[:]) != emptySHA {
		t.Fatalf("empty sha %x", empty)
	}
	cases := []struct {
		path  string
		token string
		want  int
	}{
		{"/v1/sessions/" + logTestID + "/logs/stdout?from=start", "", 401},
		{"/v1/sessions/" + logTestID + "/logs/stdout?from=start", "no", 401},
		{"/v1/sessions/" + logTestID + "/logs/export", "no", 401},
		{"/v1/sessions/A1B2C3/logs/stdout?from=start", "log", 404},
		{"/v1/sessions/a1b2c/logs/stdout?from=start", "log", 404},
		{"/v1/sessions/" + logTestID + "/logs/nope?from=start", "log", 404},
		{"/v1/sessions/ffffff/logs/stdout?from=start", "log", 404},
		{"/v1/sessions/" + logTestID + "/logs/stdout", "log", 400},
		{"/v1/sessions/" + logTestID + "/logs/stdout?from=start&tail=1", "log", 400},
		{"/v1/sessions/" + logTestID + "/logs/stdout?from=head", "log", 400},
		{"/v1/sessions/" + logTestID + "/logs/stdout?tail=-1", "log", 400},
		{"/v1/sessions/" + logTestID + "/logs/stdout?tail=abc", "log", 400},
		{"/v1/sessions/" + logTestID + "/logs/stdout?cursor=bad+cursor", "log", 400},
		{"/v1/sessions/" + logTestID + "/logs/stdout?cursor=aa%3Bbb", "log", 400},
		{"/v1/sessions/" + logTestID + "/logs/stdout?from=start&follow=maybe", "log", 400},
	}
	for _, c := range cases {
		code, _, body := logDo(t, ts.URL, c.path, c.token)
		if code != c.want || len(body) != 0 {
			t.Fatalf("%s token %q: %d body %d, want %d", c.path, c.token, code, len(body), c.want)
		}
	}
	code, hdr, body := logDo(t, ts.URL, "/v1/sessions/"+logTestID+"/logs/stdout?from=start&extra=1", "log")
	if code != http.StatusOK || hdr.Get("Content-Type") != "application/octet-stream" || hdr.Get("Content-Length") != "" {
		t.Fatalf("missing file %d %v", code, hdr)
	}
	frames := mustFrames(t, body)
	if len(frames) != 1 || !frames[0].GetEof() || frames[0].GetNext() != "" || len(frames[0].GetData()) != 0 {
		t.Fatalf("missing file frames %+v", frames)
	}
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/sessions/"+logTestID+"/logs/stdout?from=start", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Basic log")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusUnauthorized || len(b) != 0 {
		t.Fatalf("basic %d %d", resp.StatusCode, len(b))
	}
}

func TestLogBytesTailExport(t *testing.T) {
	srv, view, ts := newLogTest(t, LogConfig{})
	const stdout = "aa\nbb\ncc\n"
	const stderr = "zz\n"
	writeStream(t, srv, logTestID, "stdout", stdout)
	writeStream(t, srv, logTestID, "stderr", stderr)

	code, hdr, body := logDo(t, ts.URL, "/v1/sessions/"+logTestID+"/logs/stdout?from=start", "log")
	if code != http.StatusOK || hdr.Get("Content-Length") != "" {
		t.Fatalf("stdout %d len %q", code, hdr.Get("Content-Length"))
	}
	frames := mustFrames(t, body)
	if len(frames) < 2 || !onlyNext(frames[0]) || !frames[len(frames)-1].GetEof() {
		t.Fatalf("frames %+v", frames)
	}
	for _, fr := range frames {
		if fr.GetGap() {
			t.Fatal("gap")
		}
	}
	if !bytes.Equal(concatLogData(frames), []byte(stdout)) {
		t.Fatalf("stdout %q", concatLogData(frames))
	}

	_, _, body = logDo(t, ts.URL, "/v1/sessions/"+logTestID+"/logs/stdout?tail=1", "log")
	if !bytes.Equal(concatLogData(mustFrames(t, body)), []byte("cc\n")) {
		t.Fatalf("tail %q", concatLogData(mustFrames(t, body)))
	}
	if !bytes.Equal(tailBytes([]byte(stdout), 1), []byte("cc\n")) {
		t.Fatal("tailBytes")
	}
	_, _, body = logDo(t, ts.URL, "/v1/sessions/"+logTestID+"/logs/stdout?tail=0", "log")
	tailFrames := mustFrames(t, body)
	if len(concatLogData(tailFrames)) != 0 || !tailFrames[len(tailFrames)-1].GetEof() || !onlyNext(tailFrames[0]) {
		t.Fatalf("tail 0 %+v", tailFrames)
	}

	code, hdr, body = logDo(t, ts.URL, "/v1/sessions/"+logTestID+"/logs/export?from=start", "log")
	if code != http.StatusOK || hdr.Get("Content-Type") != "application/gzip" || hdr.Get("Content-Length") != fmt.Sprint(len(body)) {
		t.Fatalf("export %d %v %d", code, hdr, len(body))
	}
	names, files := readLogTar(t, body)
	if len(names) != 3 || names[0] != "stdout.log" || names[1] != "stderr.log" || names[2] != "manifest.json" {
		t.Fatalf("tar %v", names)
	}
	if string(files["stdout.log"]) != stdout || string(files["stderr.log"]) != stderr {
		t.Fatalf("tar files %q %q", files["stdout.log"], files["stderr.log"])
	}
	assertManifest(t, files["manifest.json"], files["stdout.log"], files["stderr.log"])

	view.setSpecs(map[string]struct{}{logTestID: {}, "b2c3d4": {}})
	_, _, body = logDo(t, ts.URL, "/v1/sessions/b2c3d4/logs/export", "log")
	_, files = readLogTar(t, body)
	if len(files["stdout.log"]) != 0 || len(files["stderr.log"]) != 0 {
		t.Fatal("empty export had bytes")
	}
	assertManifest(t, files["manifest.json"], nil, nil)
}

func readLogTar(t *testing.T, body []byte) ([]string, map[string][]byte) {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var names []string
	files := map[string][]byte{}
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Typeflag != tar.TypeReg {
			t.Fatalf("type %s %c", hdr.Name, hdr.Typeflag)
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, hdr.Name)
		files[hdr.Name] = b
	}
	return names, files
}

func assertManifest(t *testing.T, raw, stdout, stderr []byte) {
	t.Helper()
	var man logManifest
	if err := json.Unmarshal(raw, &man); err != nil {
		t.Fatal(err)
	}
	checkMeta := func(name string, got logStreamMeta, body []byte) {
		t.Helper()
		sum := sha256.Sum256(body)
		if got.Size != int64(len(body)) || got.SHA256 != hex.EncodeToString(sum[:]) {
			t.Fatalf("%s %+v len %d", name, got, len(body))
		}
	}
	checkMeta("stdout", man.Stdout, stdout)
	checkMeta("stderr", man.Stderr, stderr)
	if len(stdout) == 0 && man.Stdout.SHA256 != emptySHA {
		t.Fatalf("empty sha %s", man.Stdout.SHA256)
	}
}

func TestLogGap(t *testing.T) {
	srv, _, ts := newLogTest(t, LogConfig{})
	writeStream(t, srv, logTestID, "stdout", "one\ntwo\n")
	raw := bytes.Repeat([]byte{1}, 32)
	cur := base64.RawURLEncoding.EncodeToString(raw)
	_, _, body := logDo(t, ts.URL, "/v1/sessions/"+logTestID+"/logs/stdout?cursor="+cur, "log")
	frames := mustFrames(t, body)
	if !sawGap(frames) || !bytes.Equal(concatLogData(frames), []byte("one\ntwo\n")) {
		t.Fatalf("gap %+v", frames)
	}

	writeStream(t, srv, logTestID, "stdout", "one\ntwo\nthree\n")
	_, _, body = logDo(t, ts.URL, "/v1/sessions/"+logTestID+"/logs/stdout?from=start", "log")
	frames = mustFrames(t, body)
	if !onlyNext(frames[0]) {
		t.Fatal(frames[0])
	}
	// A cursor after the first bytes still fails when the file is replaced,
	// even if the inode number is reused: offset 0 hashes an empty prefix.
	var old string
	for _, fr := range frames {
		if len(fr.GetData()) > 0 && fr.GetNext() != "" {
			old = fr.GetNext()
			break
		}
	}
	if old == "" {
		t.Fatal("no cursor")
	}
	if err := os.Remove(srv.path(logTestID, "stdout")); err != nil {
		t.Fatal(err)
	}
	writeStream(t, srv, logTestID, "stdout", "new\n")
	_, _, body = logDo(t, ts.URL, "/v1/sessions/"+logTestID+"/logs/stdout?cursor="+old, "log")
	frames = mustFrames(t, body)
	if !sawGap(frames) || !bytes.Equal(concatLogData(frames), []byte("new\n")) {
		t.Fatalf("replaced %+v data %q", frames, concatLogData(frames))
	}
}

func sawGap(frames []*logv1.Frame) bool {
	var data bool
	for _, fr := range frames {
		if len(fr.GetData()) > 0 {
			data = true
		}
		if fr.GetGap() && !data {
			return true
		}
	}
	return false
}

func TestLogFullRetryAfter(t *testing.T) {
	srv, _, ts := newLogTest(t, LogConfig{MaxStreams: 1})
	if !srv.acquire(httptest.NewRecorder()) {
		t.Fatal("acquire")
	}
	code, hdr, body := logDo(t, ts.URL, "/v1/sessions/"+logTestID+"/logs/export", "log")
	if code != http.StatusServiceUnavailable || hdr.Get("Retry-After") != "1" || len(body) != 0 {
		t.Fatalf("export full %d %q %d", code, hdr.Get("Retry-After"), len(body))
	}
	code, hdr, body = logDo(t, ts.URL, "/v1/sessions/"+logTestID+"/logs/stdout?from=start", "log")
	if code != http.StatusServiceUnavailable || hdr.Get("Retry-After") != "1" || len(body) != 0 {
		t.Fatalf("stream full %d %q %d", code, hdr.Get("Retry-After"), len(body))
	}
	srv.releaseSlot()
	code, _, _ = logDo(t, ts.URL, "/v1/sessions/"+logTestID+"/logs/stdout?from=start", "log")
	if code != http.StatusOK {
		t.Fatalf("after release %d", code)
	}
}

func TestLogHeartbeat(t *testing.T) {
	srv, _, ts := newLogTest(t, LogConfig{Heartbeat: 30 * time.Millisecond})
	writeStream(t, srv, logTestID, "stdout", "")
	var sawNext bool
	frames := followLog(t, ts.URL+"/v1/sessions/"+logTestID+"/logs/stdout?from=start&follow=true", time.Second, func(fr *logv1.Frame) bool {
		if onlyNext(fr) {
			sawNext = true
			return false
		}
		return sawNext && fr.GetNext() == "" && len(fr.GetData()) == 0 && !fr.GetGap() && !fr.GetEof() && !fr.GetFinal() && !fr.GetRemoved()
	})
	if !sawNext {
		t.Fatalf("heartbeat %+v", frames)
	}
}

func TestLogFinalAndRemoved(t *testing.T) {
	srv, view, ts := newLogTest(t, LogConfig{})
	writeStream(t, srv, logTestID, "stdout", "aa\n")
	view.setPhase(logTestID, true)
	frames := followLog(t, ts.URL+"/v1/sessions/"+logTestID+"/logs/stdout?from=start&follow=1", 3*time.Second, func(fr *logv1.Frame) bool {
		return fr.GetFinal()
	})
	if !bytes.Equal(concatLogData(frames), []byte("aa\n")) || frames[len(frames)-1].GetRemoved() {
		t.Fatalf("final %+v", frames)
	}

	view.setPhase(logTestID, false)
	writeStream(t, srv, logTestID, "stdout", "bb\n")
	var dropped bool
	frames = followLog(t, ts.URL+"/v1/sessions/"+logTestID+"/logs/stdout?from=start&follow=", 3*time.Second, func(fr *logv1.Frame) bool {
		if len(fr.GetData()) > 0 && !dropped {
			dropped = true
			view.setSpecs(map[string]struct{}{})
		}
		return fr.GetRemoved()
	})
	if !dropped || !bytes.Equal(concatLogData(frames), []byte("bb\n")) || frames[len(frames)-1].GetFinal() {
		t.Fatalf("removed %+v", frames)
	}
	code, _, body := logDo(t, ts.URL, "/v1/sessions/"+logTestID+"/logs/stdout?from=start", "log")
	if code != http.StatusNotFound || len(body) != 0 {
		t.Fatalf("after remove %d %d", code, len(body))
	}
}

func TestLogRunningDoesNotFinal(t *testing.T) {
	srv, _, ts := newLogTest(t, LogConfig{})
	writeStream(t, srv, logTestID, "stdout", "cc\n")
	frames := followLog(t, ts.URL+"/v1/sessions/"+logTestID+"/logs/stdout?from=start&follow=true", time.Second, func(fr *logv1.Frame) bool {
		return fr.GetFinal() || fr.GetRemoved() || fr.GetEof()
	})
	if len(concatLogData(frames)) == 0 {
		t.Fatal("no data")
	}
	for _, fr := range frames {
		if fr.GetFinal() || fr.GetRemoved() || fr.GetEof() {
			t.Fatalf("ended early %+v", fr)
		}
	}
}

func followLog(t *testing.T, rawURL string, limit time.Duration, stop func(*logv1.Frame) bool) []*logv1.Frame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer log")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("follow %d %q", resp.StatusCode, b)
	}
	br := bufio.NewReader(resp.Body)
	var out []*logv1.Frame
	for {
		fr, err := readLogFrame(br)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return out
			}
			t.Fatalf("read frame after %d: %v", len(out), err)
		}
		out = append(out, fr)
		if stop != nil && stop(fr) {
			return out
		}
	}
}

func TestLogQuiesceWaits(t *testing.T) {
	srv, _, _ := newLogTest(t, LogConfig{})
	rel := srv.hold(logTestID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- srv.quiesce(ctx, logTestID) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		srv.mu.Lock()
		waiting := srv.idle[logTestID] != nil
		srv.mu.Unlock()
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("quiesce did not wait")
		}
		runtime.Gosched()
	}
	select {
	case err := <-errc:
		t.Fatalf("quiesce returned early: %v", err)
	default:
	}
	rel()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
