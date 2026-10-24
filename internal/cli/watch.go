package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

const (
	waitTargetTerminal = "terminal"
	waitNameTimeout    = "等待逾時"
	waitNameDeleted    = "等待中 session 被刪除"
	endReasonBySpec    = "ended_by_spec"
	endReasonDepsFail  = "deps_failed"
	endReasonDepsTime  = "deps_timeout"
	endReasonBadSpec   = "invalid_spec"
)

// stopLive ends the stream loop with a process result. msg is written as
// `mftiko: <msg>` when set. out is written to stdout when set.
type stopLive struct {
	code int
	msg  string
	out  string
}

func (e *stopLive) Error() string {
	if e == nil {
		return "stop"
	}
	if e.msg != "" {
		return e.msg
	}
	if e.out != "" {
		return strings.TrimRight(e.out, "\n")
	}
	return "stop"
}

var errContinue = errors.New("continue")

type live struct {
	a          *App
	api        *API
	id         string
	target     string
	region     string
	wait       bool
	deadline   timeDeadline
	tty        bool
	streamJSON bool
	resume     string
	warned     bool
	lines      int
	rows       map[string]apiv1.SessionDetail
}

// timeDeadline is a wall time from sseNow, or the zero value when there is
// no deadline. It is a named struct so a zero time.Time is not confused
// with a real instant. wait is false for watch, which has no deadline.
type timeDeadline struct {
	at time.Time
	ok bool
}

func (d timeDeadline) past() bool {
	return d.ok && !sseNow().Before(d.at)
}

func (d timeDeadline) remain() (time.Duration, bool) {
	if !d.ok {
		return 0, false
	}
	return d.at.Sub(sseNow()), true
}

func runSessionWatch(a *App) int {
	if code := a.onlyFlags(); code != 0 {
		return code
	}
	var id string
	switch len(a.Args) {
	case 0:
	case 1:
		if !sessionIDRe.MatchString(a.Args[0]) {
			return a.exit("session id must be 6 lowercase hex digits")
		}
		id = a.Args[0]
	default:
		return a.exit("session watch takes at most one id")
	}
	api, code := a.client()
	if code != 0 {
		return code
	}
	return a.follow(api, id, "", "", false)
}

func (a *App) follow(api *API, id, target, region string, wait bool) int {
	lv := &live{
		a: a, api: api, id: id, target: target, region: region, wait: wait,
		rows: map[string]apiv1.SessionDetail{},
	}
	if wait {
		timeout := 10 * time.Minute
		if a.opt.waitTimeoutSet {
			timeout = a.opt.waitTimeout
		}
		lv.deadline = timeDeadline{at: sseNow().Add(timeout), ok: true}
	} else {
		lv.tty = !a.JSON && stdoutIsTerminal(a.Stdout)
		lv.streamJSON = !lv.tty
	}
	return lv.run()
}

func (s *live) run() int {
	var backoff time.Duration
	for {
		if code, ok := s.stopped(); ok {
			return code
		}
		if backoff > 0 {
			if err := s.sleep(backoff); err != nil {
				if errors.Is(err, errWaitDeadline) {
					return s.timeout()
				}
				if code, ok := s.stopped(); ok {
					return code
				}
				return s.a.exitCode(ExitNetwork, "network (2): "+err.Error())
			}
			if code, ok := s.stopped(); ok {
				return code
			}
		}
		stream, err := s.api.openEvents(s.a.ctx, s.id, s.resume)
		if err != nil {
			if code, ok := s.stopped(); ok {
				return code
			}
			backoff = nextBackoff(backoff)
			continue
		}
		if stream.resp.StatusCode != http.StatusOK {
			err = s.classify(stream.resp)
			stream.Close()
			var stop *stopLive
			if errors.As(err, &stop) {
				return s.finish(stop)
			}
			if code, ok := s.stopped(); ok {
				return code
			}
			backoff = nextBackoff(backoff)
			continue
		}
		got, err := readSSE(s.a.ctx, stream.resp.Body, &s.resume, s.onEvent)
		stream.Close()
		var stop *stopLive
		if errors.As(err, &stop) {
			return s.finish(stop)
		}
		if code, ok := s.stopped(); ok {
			return code
		}
		if got {
			backoff = 0
		}
		backoff = nextBackoff(backoff)
	}
}

var errWaitDeadline = errors.New("wait deadline")

func (s *live) sleep(d time.Duration) error {
	if remain, ok := s.deadline.remain(); ok {
		if remain <= 0 {
			return errWaitDeadline
		}
		if d > remain {
			d = remain
		}
	}
	return sseRetryWait(s.a.ctx, d)
}

func (s *live) stopped() (int, bool) {
	if s.a.ctx.Err() != nil {
		if !s.wait {
			return ExitOK, true
		}
		return s.a.exitCode(ExitNetwork, "network (2): "+s.a.ctx.Err().Error()), true
	}
	if s.deadline.past() {
		return s.timeout(), true
	}
	return 0, false
}

func (s *live) timeout() int {
	return s.a.exitCode(ExitWaitTimeout, fmt.Sprintf("%s (%d)", waitNameTimeout, ExitWaitTimeout))
}

func (s *live) finish(stop *stopLive) int {
	if stop.out != "" {
		if _, err := io.WriteString(s.a.Stdout, s.a.sec.redact(stop.out)); err != nil {
			return s.a.exit(err.Error())
		}
	}
	if stop.msg != "" {
		return s.a.exitCode(stop.code, stop.msg)
	}
	return stop.code
}

func (s *live) classify(resp *http.Response) error {
	body, err := readCapped(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusServiceUnavailable && regionUnavailable(body) {
		s.warnRegion()
		return errContinue
	}
	if resp.StatusCode >= 500 {
		return errContinue
	}
	code := reportProblem(s.a.Stderr, body, resp.Header.Get("X-Request-Id"), s.a.sec)
	return &stopLive{code: code}
}

func readCapped(r io.Reader) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, maxBody+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxBody {
		return nil, errors.New("response is too large")
	}
	return b, nil
}

func regionUnavailable(body []byte) bool {
	var p apiv1.Problem
	if json.Unmarshal(body, &p) != nil {
		return false
	}
	return p.Code == apiv1.ErrorCodeRegionUnavailable
}

func (s *live) warnRegion() {
	if s.warned {
		return
	}
	s.warned = true
	if s.region != "" {
		fmt.Fprintf(s.a.Stderr, "mftiko: warning: unavailable regions: %s\n", s.a.sec.redact(s.region))
		return
	}
	fmt.Fprintln(s.a.Stderr, "mftiko: warning: region unavailable")
}

func (s *live) onEvent(ev sseEvent) error {
	if s.streamJSON && !s.removedWatch(ev) {
		if err := s.writeNDJSON(ev); err != nil {
			return err
		}
	}
	switch ev.Event {
	case "reset":
		return s.reload()
	case "session":
		var d apiv1.SessionDetail
		if json.Unmarshal([]byte(ev.Data), &d) != nil {
			return nil
		}
		if s.id != "" && d.Id != "" && d.Id != s.id {
			return nil
		}
		if d.Region != "" {
			s.region = d.Region
		}
		s.warned = false
		return s.apply(d)
	case "session_removed":
		var rem apiv1.SessionRemovedEvent
		if json.Unmarshal([]byte(ev.Data), &rem) != nil || rem.SessionId == "" {
			return nil
		}
		if s.id != "" && rem.SessionId != s.id {
			return nil
		}
		if s.id != "" && !s.wait {
			return &stopLive{code: ExitOK, out: removedLine(rem.SessionId)}
		}
		if !s.wait {
			delete(s.rows, rem.SessionId)
			if s.tty {
				return s.redraw()
			}
			return nil
		}
		return s.onRemoved(rem.SessionId)
	default:
		return nil
	}
}

func (s *live) removedWatch(ev sseEvent) bool {
	return ev.Event == "session_removed" && s.id != "" && !s.wait
}

func removedLine(id string) string {
	return "mftiko: session " + id + " removed\n"
}

func (s *live) writeNDJSON(ev sseEvent) error {
	line := struct {
		Event string          `json:"event"`
		ID    string          `json:"id,omitempty"`
		Data  json.RawMessage `json:"data,omitempty"`
	}{Event: ev.Event}
	if ev.HasID {
		line.ID = ev.ID
	}
	if ev.Data != "" {
		var buf bytes.Buffer
		if json.Compact(&buf, []byte(ev.Data)) == nil {
			line.Data = append([]byte(nil), buf.Bytes()...)
		} else {
			raw, err := json.Marshal(ev.Data)
			if err != nil {
				return err
			}
			line.Data = raw
		}
	}
	b, err := json.Marshal(line)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = s.a.Stdout.Write(s.a.sec.redactBytes(b))
	return err
}

func (s *live) apply(d apiv1.SessionDetail) error {
	if s.wait {
		hit := settleWait(s.target, d)
		if !hit.done {
			return nil
		}
		return &stopLive{code: hit.code, msg: fmt.Sprintf("%s (%d)", hit.name, hit.code)}
	}
	if d.Id == "" {
		return nil
	}
	s.rows[d.Id] = d
	if !s.tty {
		return nil
	}
	if s.id != "" {
		return s.redrawOne(d)
	}
	return s.redraw()
}

func (s *live) onRemoved(id string) error {
	d, err := s.fetch(id)
	if err != nil {
		if errors.Is(err, errNotFound) {
			return &stopLive{code: ExitWaitDeleted, msg: fmt.Sprintf("%s (%d)", waitNameDeleted, ExitWaitDeleted)}
		}
		var stop *stopLive
		if errors.As(err, &stop) {
			return stop
		}
		if s.a.ctx.Err() != nil {
			return s.a.ctx.Err()
		}
		return nil
	}
	s.warned = false
	return s.apply(d)
}

var errNotFound = errors.New("session not found")

func (s *live) fetch(id string) (apiv1.SessionDetail, error) {
	res, err := s.api.GetSession(s.a.ctx, id)
	if err != nil {
		if s.a.ctx.Err() != nil {
			return apiv1.SessionDetail{}, s.a.ctx.Err()
		}
		return apiv1.SessionDetail{}, errContinue
	}
	if res.Status == http.StatusNotFound {
		return apiv1.SessionDetail{}, errNotFound
	}
	if res.Status == http.StatusServiceUnavailable && regionUnavailable(res.Body) {
		s.warnRegion()
		return apiv1.SessionDetail{}, errContinue
	}
	if res.Status >= 500 {
		return apiv1.SessionDetail{}, errContinue
	}
	if res.Status < 200 || res.Status >= 300 {
		code := reportProblem(s.a.Stderr, res.Body, res.RequestID, s.a.sec)
		return apiv1.SessionDetail{}, &stopLive{code: code}
	}
	var d apiv1.SessionDetail
	if json.Unmarshal(res.Body, &d) != nil {
		return apiv1.SessionDetail{}, errContinue
	}
	if d.Region != "" {
		s.region = d.Region
	}
	return d, nil
}

func (s *live) reload() error {
	if s.id != "" {
		d, err := s.fetch(s.id)
		if err != nil {
			if errors.Is(err, errNotFound) {
				if s.wait {
					return &stopLive{code: ExitWaitDeleted, msg: fmt.Sprintf("%s (%d)", waitNameDeleted, ExitWaitDeleted)}
				}
				return &stopLive{code: ExitOK, out: removedLine(s.id)}
			}
			var stop *stopLive
			if errors.As(err, &stop) {
				return stop
			}
			if s.a.ctx.Err() != nil {
				return s.a.ctx.Err()
			}
			return nil
		}
		s.warned = false
		if !s.wait {
			s.a.warnFreshness(d.Freshness)
		}
		return s.apply(d)
	}
	items, fresh, err := s.listAll()
	if err != nil {
		var stop *stopLive
		if errors.As(err, &stop) {
			return stop
		}
		if s.a.ctx.Err() != nil {
			return s.a.ctx.Err()
		}
		return nil
	}
	next := make(map[string]apiv1.SessionDetail, len(items))
	for _, d := range items {
		next[d.Id] = d
		if s.region == "" && d.Region != "" {
			s.region = d.Region
		}
	}
	s.rows = next
	if !s.wait {
		s.a.warnFreshness(fresh)
	}
	if s.tty {
		return s.redraw()
	}
	return nil
}

type statusResult struct {
	res Result
}

func (e *statusResult) Error() string { return "http status" }

func (s *live) listAll() ([]apiv1.SessionDetail, apiv1.Freshness, error) {
	var items []apiv1.SessionDetail
	seen := map[string]struct{}{}
	regions := map[string]struct{}{}
	absent := false
	cursor := ""
	for {
		if cursor != "" {
			if _, ok := seen[cursor]; ok {
				return nil, apiv1.Freshness{}, errors.New("pagination cursor repeated")
			}
			seen[cursor] = struct{}{}
		}
		res, err := s.api.ListSessions(s.a.ctx, sessionParams(nil, nil, nil, pageLimit, cursor))
		if err != nil {
			return nil, apiv1.Freshness{}, err
		}
		if res.Status < 200 || res.Status >= 300 {
			if res.Status >= 400 && res.Status < 500 {
				code := reportProblem(s.a.Stderr, res.Body, res.RequestID, s.a.sec)
				return nil, apiv1.Freshness{}, &stopLive{code: code}
			}
			if regionUnavailable(res.Body) {
				s.warnRegion()
			}
			return nil, apiv1.Freshness{}, &statusResult{res: res}
		}
		var page apiv1.SessionList
		if json.Unmarshal(res.Body, &page) != nil {
			return nil, apiv1.Freshness{}, errors.New("response is not a session list")
		}
		for _, region := range page.Freshness.UnavailableRegions {
			regions[region] = struct{}{}
		}
		if page.Freshness.ControllerAbsent {
			absent = true
		}
		for _, item := range page.Items {
			raw, err := json.Marshal(item)
			if err != nil {
				return nil, apiv1.Freshness{}, err
			}
			var d apiv1.SessionDetail
			if json.Unmarshal(raw, &d) != nil {
				return nil, apiv1.Freshness{}, errors.New("response is not a session")
			}
			items = append(items, d)
		}
		if page.NextCursor == nil || *page.NextCursor == "" {
			break
		}
		cursor = *page.NextCursor
	}
	names := make([]string, 0, len(regions))
	for region := range regions {
		names = append(names, region)
	}
	sort.Strings(names)
	if names == nil {
		names = []string{}
	}
	return items, apiv1.Freshness{UnavailableRegions: names, ControllerAbsent: absent}, nil
}

func (s *live) redraw() error {
	header, rows := sessionListTable(s.sorted())
	var b strings.Builder
	if err := WriteTable(&b, header, rows); err != nil {
		return err
	}
	return s.paint(b.String())
}

func (s *live) sorted() []apiv1.Session {
	ids := make([]string, 0, len(s.rows))
	for id := range s.rows {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]apiv1.Session, 0, len(ids))
	for _, id := range ids {
		out = append(out, detailToSession(s.rows[id]))
	}
	return out
}

func detailToSession(d apiv1.SessionDetail) apiv1.Session {
	raw, err := json.Marshal(d)
	if err != nil {
		return apiv1.Session{Id: d.Id}
	}
	var sess apiv1.Session
	if json.Unmarshal(raw, &sess) != nil {
		return apiv1.Session{Id: d.Id}
	}
	return sess
}

func (s *live) redrawOne(d apiv1.SessionDetail) error {
	text, err := s.frame(d)
	if err != nil {
		return nil
	}
	return s.paint(text)
}

func (s *live) frame(d apiv1.SessionDetail) (string, error) {
	var agent *apiv1.AgentStatus
	if d.Logs != nil {
		ir, err := s.api.GetInstance(s.a.ctx, d.StsInstance)
		if err == nil && ir.Status >= 200 && ir.Status < 300 {
			var inst apiv1.InstanceDetail
			if json.Unmarshal(ir.Body, &inst) == nil {
				agent = inst.Agent
			}
		}
	}
	return sessionText(d, agent)
}

func (s *live) paint(text string) error {
	if text != "" && !strings.HasSuffix(text, "\n") {
		text += "\n"
	}
	n := strings.Count(text, "\n")
	var b strings.Builder
	if s.lines > 0 {
		fmt.Fprintf(&b, "\x1b[%dA\x1b[J", s.lines)
	}
	b.WriteString(text)
	if _, err := io.WriteString(s.a.Stdout, s.a.sec.redact(b.String())); err != nil {
		return err
	}
	s.lines = n
	return nil
}

type waitHit struct {
	done bool
	code int
	name string
}

// settleWait classifies a session for --wait (E11-D033). The first match
// wins: a named phase target, then not terminal, then ended_by_spec, then
// outcome done, then the outcome codes, then the other end reasons, then
// phase done with nothing else, then phase failed.
func settleWait(target string, d apiv1.SessionDetail) waitHit {
	phase := apiv1.Phase("")
	reason := ""
	var outcome apiv1.Outcome
	outcomeSet := false
	if d.Status != nil {
		phase = d.Status.Phase
		reason = d.Status.Reason
		if d.Status.LastExit != nil && d.Status.LastExit.Outcome != "" {
			outcome = d.Status.LastExit.Outcome
			outcomeSet = true
		}
	}
	endReason := ""
	if d.Assignment != nil && d.Assignment.EndReason != nil {
		endReason = *d.Assignment.EndReason
	}
	if endReason == "" && knownEndReason(reason) {
		endReason = reason
	}
	if target != "" && target != waitTargetTerminal && string(phase) == target {
		return waitHit{true, ExitOK, target}
	}
	if phase != apiv1.PhaseDone && phase != apiv1.PhaseFailed {
		return waitHit{}
	}
	if endReason == endReasonBySpec {
		return waitHit{true, ExitOK, endReasonBySpec}
	}
	if outcomeSet && outcome == apiv1.OutcomeDone {
		return waitHit{true, ExitOK, "done"}
	}
	if outcomeSet {
		switch outcome {
		case apiv1.OutcomeFailed:
			return waitHit{true, ExitOutcomeFailed, "failed"}
		case apiv1.OutcomeInitFailed:
			return waitHit{true, ExitInitFailed, "init_failed"}
		case apiv1.OutcomeCrashed:
			return waitHit{true, ExitCrashed, "crashed"}
		case apiv1.OutcomeHung:
			return waitHit{true, ExitHung, "hung"}
		case apiv1.OutcomeUnknown:
			return waitHit{true, ExitOutcomeUnknown, "unknown"}
		}
	}
	switch endReason {
	case endReasonDepsFail:
		return waitHit{true, ExitDepsFailed, endReasonDepsFail}
	case endReasonDepsTime:
		return waitHit{true, ExitDepsTimeout, endReasonDepsTime}
	case endReasonBadSpec:
		return waitHit{true, ExitInvalidSpec, endReasonBadSpec}
	}
	if phase == apiv1.PhaseDone && !outcomeSet {
		return waitHit{true, ExitOK, "done"}
	}
	return waitHit{true, ExitOutcomeFailed, "failed"}
}

func knownEndReason(s string) bool {
	switch s {
	case endReasonBySpec, endReasonDepsFail, endReasonDepsTime, endReasonBadSpec:
		return true
	default:
		return false
	}
}
