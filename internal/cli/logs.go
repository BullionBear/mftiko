package cli

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

const logGapLine = "mftiko: some output was rotated out"

// logsNow is the clock for the default export file name. Tests replace it.
var logsNow = time.Now

var (
	errLogDrop    = errors.New("log event dropped")
	errNoManifest = errors.New("no manifest")
)

func runSessionLogs(a *App) int {
	if code := a.onlyFlags("follow", "tail", "stream", "latest"); code != 0 {
		return code
	}
	if a.JSON {
		return a.exit("-o is not used with session logs")
	}
	id, latest, code := a.logIDUsage("session logs")
	if code != 0 {
		return code
	}
	api, code := a.client()
	if code != 0 {
		return code
	}
	if latest {
		id, code = a.latestSession(api)
		if code != 0 {
			return code
		}
	}
	s := &logFollow{
		a: a, api: api, id: id,
		follow: a.opt.follow, tail: a.opt.tail, tailSet: a.opt.tailSet,
		stream: a.opt.stream, streamSet: a.opt.streamSet,
	}
	return s.run()
}

func runSessionExportLogs(a *App) int {
	if code := a.onlyFlags("latest"); code != 0 {
		return code
	}
	id, latest, code := a.logIDUsage("session export-logs")
	if code != 0 {
		return code
	}
	var api *API
	var target string
	if !latest {
		target, code = a.exportTarget(id)
		if code != 0 {
			return code
		}
	}
	api, code = a.client()
	if code != 0 {
		return code
	}
	if latest {
		id, code = a.latestSession(api)
		if code != 0 {
			return code
		}
		target, code = a.exportTarget(id)
		if code != 0 {
			return code
		}
	}
	return a.writeExport(api, id, target)
}

// logIDUsage checks the session id against --latest before any request
// (E14-D031). latest is true when the id still has to be listed.
func (a *App) logIDUsage(cmd string) (id string, latest bool, code int) {
	if a.opt.latest && len(a.Args) > 0 {
		return "", false, a.exit(cmd + ": --latest and a session id cannot both be set")
	}
	if a.opt.latest {
		return "", true, 0
	}
	if len(a.Args) != 1 {
		return "", false, a.exit(cmd + " takes a session id or --latest")
	}
	if !sessionIDRe.MatchString(a.Args[0]) {
		return "", false, a.exit("session id must be 6 lowercase hex digits")
	}
	return a.Args[0], false, 0
}

// latestSession pages GET /sessions and picks the newest created_at (E14-D031).
// The API order is not created_at. A missing created_at is older than any
// timestamp. An equal timestamp keeps the greater session id.
func (a *App) latestSession(api *API) (string, int) {
	var (
		best      apiv1.Session
		haveBest  bool
		fresh     apiv1.Freshness
		haveFresh bool
		seen      = map[string]bool{}
		cursor    string
		last      Result
	)
	for {
		if cursor != "" {
			if seen[cursor] {
				return "", a.exit("list cursor repeated")
			}
			seen[cursor] = true
		}
		res, err := api.ListSessions(a.ctx, sessionParams(nil, nil, nil, pageLimit, cursor))
		if code := a.checked(res, err); code != 0 {
			return "", code
		}
		last = res
		var page struct {
			Items     []apiv1.Session `json:"items"`
			Next      *string         `json:"next_cursor"`
			Freshness apiv1.Freshness `json:"freshness"`
		}
		if err := json.Unmarshal(res.Body, &page); err != nil {
			return "", a.exit("response is not a session list")
		}
		for _, item := range page.Items {
			if !haveBest || newerSession(item, best) {
				best = item
				haveBest = true
			}
		}
		fresh = unionFresh(fresh, page.Freshness, haveFresh)
		haveFresh = true
		if page.Next == nil || *page.Next == "" || len(page.Items) == 0 {
			break
		}
		cursor = *page.Next
	}
	a.warnFreshness(fresh)
	if !haveBest {
		a.noteResult(last)
		return "", a.exitCode(ExitNotFound, "not_found (12): no session")
	}
	if !sessionIDRe.MatchString(best.Id) {
		return "", a.exit("response is not a session list")
	}
	return best.Id, 0
}

func newerSession(item, best apiv1.Session) bool {
	it, bt := time.Time{}, time.Time{}
	if item.CreatedAt != nil {
		it = *item.CreatedAt
	}
	if best.CreatedAt != nil {
		bt = *best.CreatedAt
	}
	if it.After(bt) {
		return true
	}
	if bt.After(it) {
		return false
	}
	return item.Id > best.Id
}

// logFollow is one session logs invocation (E14-D032, E14-D033).
type logFollow struct {
	a         *App
	api       *API
	id        string
	follow    bool
	tail      int
	tailSet   bool
	stream    string
	streamSet bool
	resume    string
	backoff   time.Duration
	noted     bool
}

func (s *logFollow) run() int {
	for {
		if s.a.ctx.Err() != nil {
			return ExitOK
		}
		if s.backoff > 0 {
			if err := sseRetryWait(s.a.ctx, s.backoff); err != nil {
				if s.a.ctx.Err() != nil {
					return ExitOK
				}
				return s.a.exitCode(ExitNetwork, "network (2): "+err.Error())
			}
			if s.a.ctx.Err() != nil {
				return ExitOK
			}
		}
		code, cont := s.once()
		if !cont {
			return code
		}
	}
}

func (s *logFollow) once() (int, bool) {
	stream, err := s.api.openLogs(s.a.ctx, s.id, s.params())
	if err != nil {
		if s.a.ctx.Err() != nil {
			return ExitOK, false
		}
		return s.again(false)
	}
	defer stream.Close()
	if stream.resp.StatusCode != http.StatusOK {
		body, rerr := io.ReadAll(io.LimitReader(stream.resp.Body, maxBody+1))
		if rerr != nil {
			if s.a.ctx.Err() != nil {
				return ExitOK, false
			}
			return s.again(false)
		}
		if len(body) > maxBody {
			body = body[:maxBody]
		}
		if code, stop := s.a.logHTTP(stream.resp.StatusCode, body, stream.resp.Header.Get("X-Request-Id")); stop {
			return code, false
		}
		return s.again(false)
	}
	if !s.noted {
		s.a.noteResult(Result{RequestID: stream.resp.Header.Get("X-Request-Id")})
		s.noted = true
	}
	got, err := readSSE(s.a.ctx, stream.resp.Body, &s.resume, s.onEvent)
	var stop *stopLive
	if errors.As(err, &stop) {
		return s.finish(stop), false
	}
	if s.a.ctx.Err() != nil {
		return ExitOK, false
	}
	return s.again(got)
}

// again continues the reconnect ladder (E14-D033). A failure after the wait
// has already reached sseBackoffMax exits 2. Any received byte starts the
// next wait at 1s. -1 means try again.
func (s *logFollow) again(got bool) (int, bool) {
	if !got && s.backoff >= sseBackoffMax {
		return s.a.exitCode(ExitNetwork, "network (2): reconnect exhausted"), false
	}
	if got {
		s.backoff = 0
	}
	s.backoff = nextBackoff(s.backoff)
	return 0, true
}

func (s *logFollow) finish(stop *stopLive) int {
	if stop == nil {
		return ExitOK
	}
	if stop.msg != "" {
		s.a.exitCode(stop.code, stop.msg)
	}
	return stop.code
}

func (s *logFollow) params() *apiv1.GetSessionLogsParams {
	p := &apiv1.GetSessionLogsParams{}
	if s.tailSet {
		n := s.tail
		p.Tail = &n
	}
	if s.follow {
		v := true
		p.Follow = &v
	}
	if s.resume != "" {
		id := s.resume
		p.LastEventID = &id
	}
	return p
}

func (s *logFollow) show(stream apiv1.LogStream) bool {
	if s.streamSet {
		return string(stream) == s.stream
	}
	return stream == apiv1.Stdout || stream == apiv1.Stderr
}

func (s *logFollow) onEvent(ev sseEvent) error {
	switch ev.Event {
	case "log":
		var le apiv1.LogEvent
		if err := json.Unmarshal([]byte(ev.Data), &le); err != nil {
			return errLogDrop
		}
		if !s.show(le.Stream) {
			return nil
		}
		w := s.a.Stdout
		if le.Stream == apiv1.Stderr {
			w = s.a.Stderr
		}
		if _, err := io.WriteString(w, s.a.sec.redact(le.Data)); err != nil {
			return &stopLive{code: ExitUsage, msg: err.Error()}
		}
		return nil
	case "gap":
		var g apiv1.LogGapEvent
		if err := json.Unmarshal([]byte(ev.Data), &g); err != nil {
			return errLogDrop
		}
		if !s.show(g.Stream) {
			return nil
		}
		if _, err := fmt.Fprintln(s.a.Stderr, s.a.sec.redact(logGapLine)); err != nil {
			return &stopLive{code: ExitUsage, msg: err.Error()}
		}
		return nil
	case "end":
		var end apiv1.LogEndEvent
		if err := json.Unmarshal([]byte(ev.Data), &end); err != nil {
			return errLogDrop
		}
		return &stopLive{code: ExitOK}
	default:
		return nil
	}
}

// logHTTP classifies a non-2xx log or export response. stop is false when the
// logs command should reconnect. A problem is never retried. A plain 404 is
// an API that does not have the route (E14-D008).
func (a *App) logHTTP(status int, body []byte, requestID string) (int, bool) {
	if _, ok := problemOf(body); ok {
		return reportProblem(a.Stderr, body, requestID, a.sec), true
	}
	if status == http.StatusNotFound {
		return a.exit("API 版本太舊"), true
	}
	return 0, false
}

func problemOf(body []byte) (apiv1.Problem, bool) {
	var p apiv1.Problem
	if json.Unmarshal(body, &p) != nil || p.Code == "" {
		return apiv1.Problem{}, false
	}
	return p, true
}

// openLogs calls GET /sessions/{id}/logs on the stream client, which has no
// Timeout (E14-D033, E11-D030).
func (a *API) openLogs(ctx context.Context, id string, params *apiv1.GetSessionLogsParams) (*streamBody, error) {
	return a.openStream(ctx, func(ctx context.Context) (*http.Response, error) {
		req, err := apiv1.NewGetSessionLogsRequest(a.base, id, params)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		if err := a.edit(ctx, req); err != nil {
			return nil, err
		}
		return a.stream.Do(req)
	})
}

// doExport calls GET /sessions/{id}/logs/export on the timed client so
// --timeout bounds the download (E14-D035).
func (a *API) doExport(ctx context.Context, id string) (*http.Response, error) {
	req, err := apiv1.NewExportSessionLogsRequest(a.base, id)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	if err := a.edit(ctx, req); err != nil {
		return nil, err
	}
	return a.hc.Do(req)
}

func (a *App) exportTarget(id string) (string, int) {
	target := a.opt.exportPath
	if !a.opt.exportPathSet {
		stamp := logsNow().UTC().Format("20060102T150405Z")
		target = fmt.Sprintf("mftiko-%s-logs-%s.tar.gz", id, stamp)
	}
	dir := filepath.Dir(target)
	if dir == "" {
		dir = "."
	}
	st, err := os.Stat(dir)
	if err != nil || !st.IsDir() {
		return "", a.exit("cannot create log file")
	}
	if _, err := os.Lstat(target); err == nil {
		return "", a.exit("file exists: " + target)
	} else if !os.IsNotExist(err) {
		return "", a.exit("cannot create log file")
	}
	return target, 0
}

func (a *App) writeExport(api *API, id, target string) int {
	resp, err := api.doExport(a.ctx, id)
	if err != nil {
		return a.exitCode(ExitNetwork, "network (2): "+err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, rerr := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
		if rerr != nil {
			return a.exitCode(ExitNetwork, "network (2): "+rerr.Error())
		}
		if len(body) > maxBody {
			body = body[:maxBody]
		}
		if code, stop := a.logHTTP(resp.StatusCode, body, resp.Header.Get("X-Request-Id")); stop {
			return code
		}
		return a.exitCode(ExitNetwork, "network (2): incomplete log export")
	}
	dir := filepath.Dir(target)
	tmp, err := os.CreateTemp(dir, ".mftiko-logs-*")
	if err != nil {
		return a.exit("cannot create log file")
	}
	name := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(name)
	}()
	err = readExportArchive(io.TeeReader(resp.Body, tmp))
	if err != nil {
		if errors.Is(err, errNoManifest) {
			return a.exitCode(ExitNetwork, "network (2): log export has no manifest.json")
		}
		return a.exitCode(ExitNetwork, "network (2): incomplete log export")
	}
	if err := tmp.Sync(); err != nil {
		return a.exitCode(ExitNetwork, "network (2): incomplete log export")
	}
	if err := tmp.Close(); err != nil {
		return a.exitCode(ExitNetwork, "network (2): incomplete log export")
	}
	if err := os.Link(name, target); err != nil {
		if errors.Is(err, os.ErrExist) || errors.Is(err, syscall.EEXIST) {
			return a.exit("file exists: " + target)
		}
		return a.exitCode(ExitNetwork, "network (2): incomplete log export")
	}
	a.noteResult(Result{RequestID: resp.Header.Get("X-Request-Id")})
	return ExitOK
}

// readExportArchive accepts one gzip member whose tar has a regular file
// named manifest.json (E14-D034). It does not parse the manifest or compare
// sha256. A truncated gzip or tar returns a generic error.
func readExportArchive(r io.Reader) error {
	zr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	saw := false
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if hdr.Name == "manifest.json" && hdr.Typeflag != tar.TypeDir {
			switch hdr.Typeflag {
			case tar.TypeReg, tar.TypeRegA:
				saw = true
			}
		}
		if _, err := io.Copy(io.Discard, tr); err != nil {
			return err
		}
	}
	if _, err := io.Copy(io.Discard, zr); err != nil {
		return err
	}
	if !saw {
		return errNoManifest
	}
	return nil
}
