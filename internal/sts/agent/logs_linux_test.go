package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	logv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/log/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func (e *env) enableLogs(listen string, retry time.Duration) {
	e.cfg.Logs = LogConfig{
		Listen: listen, TokenSHA256: logTokenHex("log"), Heartbeat: time.Hour, Retry: retry,
	}
}

func (e *env) logGet(addr, id, kind, token string, q url.Values) (int, http.Header, []byte, error) {
	u := "http://" + addr + "/v1/sessions/" + id + "/logs/" + kind
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, nil, nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, b, err
}

func lastListen(log string) (addr, adv string, n int, ok bool) {
	for _, line := range strings.Split(log, "\n") {
		if !strings.Contains(line, `"message":"log endpoint listening"`) {
			continue
		}
		var rec struct {
			Addr      string `json:"addr"`
			Advertise string `json:"advertise"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil || rec.Addr == "" {
			continue
		}
		addr, adv, ok = rec.Addr, rec.Advertise, true
		n++
	}
	return addr, adv, n, ok
}

func waitListen(t *testing.T, e *env, want int) (addr, adv string) {
	t.Helper()
	pollUntil(t, "log endpoint", func() bool {
		a, d, n, ok := lastListen(e.log.String())
		if !ok || n < want {
			return false
		}
		c, err := net.DialTimeout("tcp", a, 200*time.Millisecond)
		if err != nil {
			return false
		}
		c.Close()
		rec := e.record()
		if rec == nil || rec.GetLogAddr() != d {
			return false
		}
		addr, adv = a, d
		return true
	})
	host, err := os.Hostname()
	if err != nil || host == "" {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	if adv != net.JoinHostPort(host, port) {
		t.Fatalf("log_addr %s, want %s", adv, net.JoinHostPort(host, port))
	}
	return addr, adv
}

func streamPath(e *env, id, stream string) string {
	return shim.OutputPath(e.cfg.WorkDir, string(state.PlaneSTS), string(WorkerKey(id)), stream)
}

func waitStreamMatch(t *testing.T, e *env, addr, id, stream string) []byte {
	t.Helper()
	path := streamPath(e, id, stream)
	var disk []byte
	pollUntil(t, stream+" matches the file", func() bool {
		code, hdr, body, err := e.logGet(addr, id, stream, "log", url.Values{"from": {"start"}})
		if err != nil || code != http.StatusOK {
			return false
		}
		if hdr.Get("Content-Length") != "" || hdr.Get("Content-Type") != "application/octet-stream" {
			t.Fatalf("%s headers %v", stream, hdr)
		}
		frames, err := decodeLogFrames(body)
		if err != nil {
			return false
		}
		file, err := os.ReadFile(path)
		if err != nil || len(file) == 0 || !bytes.Equal(concatLogData(frames), file) {
			return false
		}
		again, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(again, file) {
			return false
		}
		if len(frames) < 2 || !onlyNext(frames[0]) || !frames[len(frames)-1].GetEof() {
			return false
		}
		disk = append([]byte(nil), file...)
		return true
	})
	return disk
}

func firstDataCursor(t *testing.T, framesBody []byte) (prefix []byte, cursor string) {
	t.Helper()
	frames := mustFrames(t, framesBody)
	var n int
	for _, fr := range frames {
		if len(fr.GetData()) > 0 {
			n++
		}
	}
	if n < 2 {
		t.Fatalf("data frames %d", n)
	}
	for _, fr := range frames {
		if len(fr.GetData()) == 0 || fr.GetNext() == "" {
			continue
		}
		return append([]byte(nil), fr.GetData()...), fr.GetNext()
	}
	t.Fatal("no cursor")
	return nil, ""
}

func TestLogEndpointMatchesAndResumes(t *testing.T) {
	e := setup(t)
	e.enableLogs("127.0.0.1:0", 0)
	const id = "a1b2c3"
	e.setScript(id, fakeworker.Script{FloodBytes: 300 << 10})
	e.putSpec(id, sessionSpec(1, 1))
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	ended := false
	defer func() {
		if ended {
			return
		}
		stop()
		waitDone(t, done)
	}()

	ph.wait(id, statev1.Phase_PHASE_RUNNING)
	pollUntil(t, "flooded", func() bool {
		for _, en := range e.journalEntries(id) {
			if en.Event == "flooded" {
				return true
			}
		}
		return false
	})
	addr, _ := waitListen(t, e, 1)
	stdout := waitStreamMatch(t, e, addr, id, "stdout")
	stderr := waitStreamMatch(t, e, addr, id, "stderr")

	code, _, body, err := e.logGet(addr, id, "stdout", "log", url.Values{"tail": {"2"}})
	if err != nil || code != http.StatusOK || !bytes.Equal(concatLogData(mustFrames(t, body)), tailBytes(stdout, 2)) {
		t.Fatalf("tail %d %v %q", code, err, body)
	}
	code, _, body, err = e.logGet(addr, id, "stdout", "log", url.Values{"tail": {"0"}})
	if err != nil || code != http.StatusOK {
		t.Fatal(err, code)
	}
	tail0 := mustFrames(t, body)
	if len(concatLogData(tail0)) != 0 || !onlyNext(tail0[0]) || !tail0[len(tail0)-1].GetEof() {
		t.Fatalf("tail 0 %+v", tail0)
	}

	code, hdr, body, err := e.logGet(addr, id, "export", "log", nil)
	if err != nil || code != http.StatusOK || hdr.Get("Content-Type") != "application/gzip" || hdr.Get("Content-Length") != fmt.Sprint(len(body)) {
		t.Fatalf("export %d %v %v", code, err, hdr)
	}
	names, files := readLogTar(t, body)
	if len(names) != 3 || names[0] != "stdout.log" || names[1] != "stderr.log" || names[2] != "manifest.json" {
		t.Fatalf("tar %v", names)
	}
	if !bytes.Equal(files["stdout.log"], stdout) || !bytes.Equal(files["stderr.log"], stderr) {
		t.Fatalf("export lengths %d %d", len(files["stdout.log"]), len(files["stderr.log"]))
	}
	assertManifest(t, files["manifest.json"], files["stdout.log"], files["stderr.log"])

	code, _, body, err = e.logGet(addr, id, "stdout", "", url.Values{"from": {"start"}})
	if err != nil || code != http.StatusUnauthorized || len(body) != 0 {
		t.Fatalf("missing token %d %v %d", code, err, len(body))
	}
	code, _, body, err = e.logGet(addr, id, "export", "no", nil)
	if err != nil || code != http.StatusUnauthorized || len(body) != 0 {
		t.Fatalf("wrong token %d %v %d", code, err, len(body))
	}

	code, _, body, err = e.logGet(addr, id, "stdout", "log", url.Values{"from": {"start"}})
	if err != nil || code != http.StatusOK {
		t.Fatal(err, code)
	}
	prefix, cur := firstDataCursor(t, body)
	if !bytes.HasPrefix(stdout, prefix) {
		t.Fatal("prefix is not the file head")
	}

	stop()
	waitDone(t, done)
	e.cfg.Term.ID = t.Name() + "-next"
	stop, done = e.start()
	addr, _ = waitListen(t, e, 2)
	pollUntil(t, "resume", func() bool {
		code, _, body, err = e.logGet(addr, id, "stdout", "log", url.Values{"cursor": {cur}})
		if err != nil || code != http.StatusOK {
			return false
		}
		frames, decErr := decodeLogFrames(body)
		if decErr != nil || len(frames) == 0 || !onlyNext(frames[0]) || !frames[len(frames)-1].GetEof() {
			return false
		}
		for _, fr := range frames {
			if fr.GetGap() {
				return false
			}
		}
		file, rdErr := os.ReadFile(streamPath(e, id, "stdout"))
		got := append(append([]byte{}, prefix...), concatLogData(frames)...)
		return rdErr == nil && bytes.Equal(file, stdout) && bytes.Equal(got, stdout)
	})

	stop()
	waitDone(t, done)
	ended = true
	pollUntil(t, "endpoint closed", func() bool {
		c, dialErr := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if dialErr != nil {
			return true
		}
		c.Close()
		return false
	})
}

func TestLogFollowRemoved(t *testing.T) {
	e := setup(t)
	e.enableLogs("127.0.0.1:0", 0)
	const id = "d4e5f6"
	e.setScript(id, fakeworker.Script{FloodBytes: 64 << 10})
	e.putSpec(id, sessionSpec(1, 1))
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(id, statev1.Phase_PHASE_RUNNING)
	addr, _ := waitListen(t, e, 1)
	var dropped bool
	frames := followLog(t, "http://"+addr+"/v1/sessions/"+id+"/logs/stdout?from=start&follow=true", 20*time.Second, func(fr *logv1.Frame) bool {
		if len(fr.GetData()) > 0 && !dropped {
			dropped = true
			e.delSpec(id)
		}
		return fr.GetRemoved()
	})
	if !dropped || len(frames) == 0 || !frames[len(frames)-1].GetRemoved() || frames[len(frames)-1].GetFinal() {
		t.Fatalf("removed dropped %v frames %d", dropped, len(frames))
	}
	pollUntil(t, "404", func() bool {
		code, _, body, err := e.logGet(addr, id, "stdout", "log", url.Values{"from": {"start"}})
		return err == nil && code == http.StatusNotFound && len(body) == 0
	})
}

func TestLogFollowFinal(t *testing.T) {
	e := setup(t)
	e.enableLogs("127.0.0.1:0", 0)
	const id = "e5f607"
	spec := sessionSpec(1, 1)
	spec.Restart = &sessionv1.RestartPolicy{Mode: sessionv1.RestartMode_RESTART_MODE_NEVER}
	e.setScript(id, fakeworker.Script{FloodBytes: 64 << 10, Exit: fakeworker.ExitDone})
	e.putSpec(id, spec)
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(id, statev1.Phase_PHASE_DONE)
	addr, _ := waitListen(t, e, 1)
	for _, stream := range []string{"stdout", "stderr"} {
		path := streamPath(e, id, stream)
		var stable []byte
		var same int
		pollUntil(t, stream+" stable", func() bool {
			b, err := os.ReadFile(path)
			if err != nil || len(b) == 0 || !bytes.Equal(b, stable) {
				stable = append([]byte(nil), b...)
				same = 0
				return false
			}
			same++
			return same >= 3
		})
		frames := followLog(t, "http://"+addr+"/v1/sessions/"+id+"/logs/"+stream+"?from=start&follow=true", 15*time.Second, func(fr *logv1.Frame) bool {
			return fr.GetFinal()
		})
		disk, err := os.ReadFile(path)
		if err != nil || len(frames) == 0 || !frames[len(frames)-1].GetFinal() || frames[len(frames)-1].GetRemoved() {
			t.Fatalf("%s final err %v frames %d", stream, err, len(frames))
		}
		if !bytes.Equal(concatLogData(frames), disk) {
			t.Fatalf("%s final %d file %d", stream, len(concatLogData(frames)), len(disk))
		}
	}
}

func TestLogListenFailureStillRuns(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e := setup(t)
	e.enableLogs(ln.Addr().String(), 20*time.Millisecond)
	const id = "b2c3d4"
	e.setScript(id, fakeworker.Script{})
	e.putSpec(id, sessionSpec(1, 1))
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(id, statev1.Phase_PHASE_RUNNING)
	pollUntil(t, "listen retry", func() bool {
		if strings.Contains(e.log.String(), `"message":"log endpoint listening"`) {
			t.Fatal("listened while the port was taken")
		}
		rec := e.record()
		return strings.Contains(e.log.String(), `"message":"log listen"`) && rec != nil && rec.GetLogAddr() == ""
	})
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	addr, _ := waitListen(t, e, 1)
	if addr != ln.Addr().String() {
		t.Fatalf("bound %s, want %s", addr, ln.Addr().String())
	}
	pollUntil(t, "stdout after listen", func() bool {
		code, _, _, err := e.logGet(addr, id, "stdout", "log", url.Values{"from": {"start"}})
		return err == nil && code == http.StatusOK
	})
}

func TestLogOmittedTokenStaysDown(t *testing.T) {
	e := setup(t)
	e.cfg.Logs = LogConfig{Listen: "127.0.0.1:0"}
	const id = "c3d4e5"
	e.setScript(id, fakeworker.Script{})
	e.putSpec(id, sessionSpec(1, 1))
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(id, statev1.Phase_PHASE_RUNNING)
	pollUntil(t, "log endpoint disabled", func() bool {
		rec := e.record()
		return rec != nil && rec.GetLogAddr() == "" &&
			strings.Count(e.log.String(), "log endpoint disabled: -log-token-sha256 is empty") == 1 &&
			!strings.Contains(e.log.String(), "log endpoint listening")
	})
}
