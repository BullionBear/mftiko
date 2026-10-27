package api

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	logv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/log/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// Timeouts and retry budget for the API → STS agent log connection (E14-D027, E14-D029).
// logMaxRetries is the number of retries after the first attempt, so a dial sequence tries
// four times and waits logRetrySpacing[0], [1], [2] between them.
const (
	logDialTimeout   = 5 * time.Second
	logHeaderTimeout = 10 * time.Second
	logRetryAfterCap = 2 * time.Second
	logMaxData       = 262144
	logMaxFrame      = logMaxData + 4096
	logMaxRetries    = 3
	logMaxCursor     = 256
	logUserAgent     = "mftiko-api"
	logExportType    = "application/gzip"
)

var logRetrySpacing = [logMaxRetries]time.Duration{
	200 * time.Millisecond,
	400 * time.Millisecond,
	800 * time.Millisecond,
}

var (
	errLogNoLeader = errors.New("api: log leader missing")
	errLogDenied   = errors.New("api: log agent denied")
	errLogAbsent   = errors.New("api: log session absent")
	errLogDown     = errors.New("api: log agent unavailable")
	errLogProtocol = errors.New("api: log frame")
)

const (
	logStdout = 0
	logStderr = 1
)

// logFrame is one agent frame. empty is the follow heartbeat (a zero length prefix).
type logFrame struct {
	data                     []byte
	next                     string
	gap, eof, final, removed bool
	empty                    bool
}

type logQuery struct {
	tail    *int
	follow  bool
	resume  [2]string
	resumed bool
}

// newLogClient is the HTTP client for contract L. There is no overall deadline (E14-D029):
// the dial limit is on the dialer and the header limit is on the transport. The body is not
// timed. Compression is off so a gzip export is copied as the agent sent it (E14-D025).
// Proxy is set so HTTP_PROXY cannot receive the bearer. Redirects are not followed.
func newLogClient() *http.Client {
	dialer := &net.Dialer{Timeout: logDialTimeout}
	return &http.Client{
		Transport: &http.Transport{
			Proxy:                 func(*http.Request) (*url.URL, error) { return nil, nil },
			DialContext:           dialer.DialContext,
			ResponseHeaderTimeout: logHeaderTimeout,
			DisableCompression:    true,
			ForceAttemptHTTP2:     false,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func (s *Server) waitLog(ctx context.Context, d time.Duration) error {
	if s.logWait != nil {
		return s.logWait(ctx, d)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// logWaitFor is the pause before the next attempt. fails is how many attempts have already
// failed. More than logMaxRetries means the sequence is over.
func logWaitFor(fails int, retryAfter time.Duration) (time.Duration, bool) {
	if fails < 1 || fails > logMaxRetries {
		return 0, false
	}
	d := logRetrySpacing[fails-1]
	if retryAfter > 0 {
		d = retryAfter
		if d > logRetryAfterCap {
			d = logRetryAfterCap
		}
	}
	return d, true
}

// parseRetryAfter reads a delay-seconds Retry-After. Anything else, including an HTTP-date,
// is ignored so the caller uses the fixed spacing. Values above the cap are shortened (E14-D027).
func parseRetryAfter(v string) time.Duration {
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n < 1 {
		return 0
	}
	d := time.Duration(n) * time.Second
	if d > logRetryAfterCap {
		return logRetryAfterCap
	}
	return d
}

// lookupLogAddr reads the live leader of the session's STS instance (E14-D015). It does not
// consult the last-known AgentRecord (E10-D096). A missing record or an empty log_addr is
// errLogNoLeader. The session's region being down is errRegionUnavailable, and a missing
// session is errNotFound.
func (s *Server) lookupLogAddr(id string) (string, error) {
	sts, err := s.findSpec(id)
	if err != nil {
		return "", err
	}
	region, err := s.cfg.Topology.RegionOf(sts)
	if err != nil {
		return "", fmt.Errorf("%w: %s", errRegionUnavailable, sts)
	}
	view, ok := s.snapshot(region)
	if !ok {
		return "", fmt.Errorf("%w: %s", errRegionUnavailable, region)
	}
	entry, ok := view.Entries[state.AgentKey(state.PlaneSTS, sts)]
	if !ok {
		return "", errLogNoLeader
	}
	rec := &statev1.AgentRecord{}
	if proto.Unmarshal(entry.Value, rec) != nil || rec.GetLogAddr() == "" {
		return "", errLogNoLeader
	}
	return rec.GetLogAddr(), nil
}

func validSessionID(id string) bool {
	if len(id) != 6 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if c < '0' || (c > '9' && c < 'a') || c > 'f' {
			return false
		}
	}
	return true
}

func validCursor(s string) bool {
	if s == "" || len(s) > logMaxCursor {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}

func logStreamName(stream int) string {
	if stream == logStderr {
		return "stderr"
	}
	return "stdout"
}

// agentLogQuery is one agent read. A cursor replaces tail and from (E14-D012). follow is
// omitted unless the caller asked for it; the value is the boolean true.
func agentLogQuery(q logQuery, cursor string) url.Values {
	v := url.Values{}
	switch {
	case cursor != "":
		v.Set("cursor", cursor)
	case q.tail != nil:
		v.Set("tail", strconv.Itoa(*q.tail))
	default:
		v.Set("from", "start")
	}
	if q.follow {
		v.Set("follow", "true")
	}
	return v
}

func logURL(addr, sessionID, kind string, q url.Values) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || port == "" {
		return "", errLogDown
	}
	if strings.ContainsAny(host, "/@? \t") || strings.ContainsAny(port, "/ \t") {
		return "", errLogDown
	}
	if kind != "stdout" && kind != "stderr" && kind != "export" {
		return "", errLogDown
	}
	if !validSessionID(sessionID) {
		return "", errLogDown
	}
	u := url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(host, port),
		Path:   "/v1/sessions/" + sessionID + "/logs/" + kind,
	}
	if len(q) > 0 {
		u.RawQuery = q.Encode()
	}
	return u.String(), nil
}

func (s *Server) logRequest(addr, sessionID, kind string, q url.Values) (*http.Request, error) {
	raw, err := logURL(addr, sessionID, kind, q)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodGet, raw, nil)
	if err != nil {
		return nil, errLogDown
	}
	req.Header.Set("Authorization", "Bearer "+s.cfg.LogToken)
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("User-Agent", logUserAgent)
	return req, nil
}

type logAttempt struct {
	resp       *http.Response
	retryAfter time.Duration
	retry      bool
	err        error
}

// oneAttempt dials once. retry is set for a connection failure, a timeout, or an agent 503.
// 401, 404, and any other status are not retried (E14-D027). immediateMissing makes a missing
// leader a final error; a reconnect passes false so a new leader can appear inside the budget
// (E14-D028).
func (s *Server) oneAttempt(ctx context.Context, sessionID string, immediateMissing bool, build func(addr string) (*http.Request, error)) logAttempt {
	if ctx.Err() != nil {
		return logAttempt{err: ctx.Err()}
	}
	if s.cfg.LogToken == "" {
		return logAttempt{err: errLogDown}
	}
	addr, err := s.lookupLogAddr(sessionID)
	if err != nil {
		if errors.Is(err, errNotFound) || errors.Is(err, errRegionUnavailable) {
			return logAttempt{err: err}
		}
		if immediateMissing {
			return logAttempt{err: errLogNoLeader}
		}
		return logAttempt{err: errLogDown, retry: true}
	}
	req, err := build(addr)
	if err != nil {
		return logAttempt{err: errLogDown, retry: true}
	}
	req = req.WithContext(ctx)
	client := s.logClient
	if client == nil {
		client = newLogClient()
	}
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return logAttempt{err: ctx.Err()}
		}
		return logAttempt{err: errLogDown, retry: true}
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return logAttempt{resp: resp}
	case http.StatusUnauthorized:
		resp.Body.Close()
		return logAttempt{err: errLogDenied}
	case http.StatusNotFound:
		resp.Body.Close()
		return logAttempt{err: errLogAbsent}
	case http.StatusServiceUnavailable:
		after := parseRetryAfter(resp.Header.Get("Retry-After"))
		resp.Body.Close()
		return logAttempt{err: errLogDown, retry: true, retryAfter: after}
	default:
		resp.Body.Close()
		return logAttempt{err: errLogDown}
	}
}

// dialExport retries until the agent returns 200 or the budget is spent. The body is the
// caller's. A short body is not retried (E14-D025).
func (s *Server) dialExport(ctx context.Context, sessionID string) (*http.Response, error) {
	fails := 0
	var retryAfter time.Duration
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if fails > 0 {
			wait, ok := logWaitFor(fails, retryAfter)
			if !ok {
				return nil, errLogDown
			}
			retryAfter = 0
			if err := s.waitLog(ctx, wait); err != nil {
				return nil, err
			}
		}
		res := s.oneAttempt(ctx, sessionID, fails == 0, func(addr string) (*http.Request, error) {
			return s.logRequest(addr, sessionID, "export", nil)
		})
		if res.resp != nil {
			return res.resp, nil
		}
		if ctx.Err() != nil || errors.Is(res.err, context.Canceled) {
			return nil, ctx.Err()
		}
		if !res.retry {
			return nil, res.err
		}
		retryAfter = res.retryAfter
		fails++
	}
}

// readLogFrame reads one length-prefixed Frame. A zero length is a heartbeat.
func readLogFrame(r *bufio.Reader) (logFrame, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return logFrame{}, err
		}
		return logFrame{}, errLogProtocol
	}
	if n == 0 {
		return logFrame{empty: true}, nil
	}
	if n > logMaxFrame {
		return logFrame{}, errLogProtocol
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return logFrame{}, err
	}
	var pb logv1.Frame
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return logFrame{}, errLogProtocol
	}
	if len(pb.GetData()) > logMaxData || !validCursor(pb.GetNext()) {
		return logFrame{}, errLogProtocol
	}
	return logFrame{
		data:    append([]byte(nil), pb.GetData()...),
		next:    pb.GetNext(),
		gap:     pb.GetGap(),
		eof:     pb.GetEof(),
		final:   pb.GetFinal(),
		removed: pb.GetRemoved(),
	}, nil
}

type logMsg struct {
	stream int
	frame  logFrame
	err    error
}

func sendLog(ctx context.Context, out chan<- logMsg, m logMsg) error {
	select {
	case out <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// readAgentFrames copies frames to out. progress is true once any frame, including a
// heartbeat, has been parsed. cursor is the last next from this body. A clean return is a
// terminal frame (eof, final, or removed). A protocol error is not a disconnect.
func readAgentFrames(ctx context.Context, body io.Reader, stream int, out chan<- logMsg) (progress bool, cursor string, err error) {
	br := bufio.NewReader(body)
	for {
		if ctx.Err() != nil {
			return progress, cursor, ctx.Err()
		}
		frame, err := readLogFrame(br)
		if err != nil {
			return progress, cursor, err
		}
		progress = true
		if frame.empty {
			continue
		}
		if frame.next != "" {
			cursor = frame.next
		}
		if err := sendLog(ctx, out, logMsg{stream: stream, frame: frame}); err != nil {
			return progress, cursor, err
		}
		if frame.eof || frame.final || frame.removed {
			return progress, cursor, nil
		}
	}
}

// pumpLog owns one agent stream. The first missing leader fails immediately. After a frame
// has been parsed, a disconnect starts a new dial sequence. A 200 whose body ends before any
// frame spends one attempt of the current sequence instead of resetting it (E14-D028).
func (s *Server) pumpLog(ctx context.Context, sessionID string, stream int, q logQuery, out chan<- logMsg) {
	cursor := ""
	if q.resumed {
		cursor = q.resume[stream]
	}
	ever := false
	fails := 0
	var retryAfter time.Duration
	kind := logStreamName(stream)
	for {
		if ctx.Err() != nil {
			return
		}
		if fails > 0 {
			wait, ok := logWaitFor(fails, retryAfter)
			if !ok {
				_ = sendLog(ctx, out, logMsg{err: errLogDown})
				return
			}
			retryAfter = 0
			if err := s.waitLog(ctx, wait); err != nil {
				return
			}
		}
		res := s.oneAttempt(ctx, sessionID, !ever && fails == 0, func(addr string) (*http.Request, error) {
			return s.logRequest(addr, sessionID, kind, agentLogQuery(q, cursor))
		})
		if res.resp == nil {
			if ctx.Err() != nil || errors.Is(res.err, context.Canceled) {
				return
			}
			if !res.retry {
				_ = sendLog(ctx, out, logMsg{err: res.err})
				return
			}
			retryAfter = res.retryAfter
			fails++
			continue
		}
		got, next, err := readAgentFrames(ctx, res.resp.Body, stream, out)
		res.resp.Body.Close()
		if next != "" {
			cursor = next
		}
		if got {
			ever = true
		}
		if err == nil {
			return
		}
		if ctx.Err() != nil || errors.Is(err, context.Canceled) {
			return
		}
		if errors.Is(err, errLogProtocol) {
			_ = sendLog(ctx, out, logMsg{err: err})
			return
		}
		if got {
			fails = 0
			retryAfter = 0
			continue
		}
		fails++
	}
}
