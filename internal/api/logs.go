package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// Audit detail for one log request (E14-D030). These are the result codes, not the problem
// detail sentence. HTTP status stays the status that was actually written.
const (
	auditOK                = "ok"
	auditLogsUnavailable   = "logs_unavailable"
	auditNotFound          = "not_found"
	auditRegionUnavailable = "region_unavailable"
	auditValidation        = "validation_failed"
	auditClientClosed      = "client_closed"
	auditIncomplete        = "incomplete"
)

// GetSessionLogs proxies the STS agent's stdout and stderr as one SSE stream (E14-D011).
// Frames are forwarded in arrival order whether or not follow is set. The first event waits
// until both streams have produced a next cursor. An agent disconnect resumes from that
// cursor and a fresh leader (E14-D028). The stream closes without an end event when the
// reconnect budget is spent.
func (s *Server) GetSessionLogs(w http.ResponseWriter, r *http.Request, sessionID apiv1.SessionID, params apiv1.GetSessionLogsParams) {
	result := auditLogsUnavailable
	defer func() { setAuditResult(w, result) }()
	if s.cfg.LogToken == "" {
		logsUnavailable(w)
		return
	}
	id := string(sessionID)
	if !validSessionID(id) {
		result = auditValidation
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "invalid request", []apiv1.FieldError{{
			Path: "session_id", Reason: "is not a session id",
		}})
		return
	}
	q, fields, err := parseLogParams(params)
	if err != nil {
		result = auditValidation
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "invalid request", fields)
		return
	}
	if _, err := s.lookupLogAddr(id); err != nil {
		if r.Context().Err() != nil {
			result = auditClientClosed
			return
		}
		result = logResult(err)
		s.writeLogProblem(w, err)
		return
	}

	// pumpCtx stops the agent reads. writeCtx stops the SSE writer. They are separate so
	// cancelling the pumps does not drop an end event that is already queued (E14-D011).
	pumpCtx, pumpCancel := context.WithCancel(r.Context())
	defer pumpCancel()
	writeCtx, writeCancel := context.WithCancel(r.Context())
	frames := make(chan logMsg, 8)
	var wg sync.WaitGroup
	for stream := 0; stream < 2; stream++ {
		wg.Add(1)
		go func(stream int) {
			defer wg.Done()
			s.pumpLog(pumpCtx, id, stream, q, frames)
		}(stream)
	}
	go func() {
		wg.Wait()
		close(frames)
	}()

	merge := newLogMerge(q.follow, q.resume)
	var hub *sseHub
	var sseDone <-chan struct{}
	defer func() {
		pumpCancel()
		writeCancel()
		if sseDone == nil {
			return
		}
		select {
		case <-sseDone:
		case <-r.Context().Done():
			if hub != nil && hub.rc != nil {
				_ = hub.rc.SetWriteDeadline(time.Now())
			}
			<-sseDone
		}
	}()

	started := false
	for msg := range frames {
		if msg.err != nil {
			if r.Context().Err() != nil && !merge.ended {
				result = auditClientClosed
			} else if !merge.ended {
				result = logResult(msg.err)
				if !started && r.Context().Err() == nil {
					s.writeLogProblem(w, msg.err)
				}
			}
			pumpCancel()
			return
		}
		outs := merge.apply(msg.stream, msg.frame)
		if len(outs) == 0 {
			continue
		}
		if !started {
			hub, sseDone = s.openLogSSE(w, writeCtx, writeCancel)
			started = true
		}
		for _, ev := range outs {
			if !hub.send(sseMsg{id: ev.id, event: ev.event, data: ev.data}) {
				if !merge.ended {
					result = auditClientClosed
				}
				pumpCancel()
				return
			}
		}
		if merge.ended {
			result = auditOK
			pumpCancel()
			return
		}
	}
	if merge.ended {
		result = auditOK
		return
	}
	if r.Context().Err() != nil {
		result = auditClientClosed
		return
	}
	if !started {
		logsUnavailable(w)
	}
	result = auditLogsUnavailable
}

// ExportSessionLogs forwards the agent's tar.gz body, Content-Type, and Content-Length
// (E14-D014, E14-D025). The bytes are not buffered to disk and are not compressed here.
// A body that ends before Content-Length is an incomplete response.
func (s *Server) ExportSessionLogs(w http.ResponseWriter, r *http.Request, sessionID apiv1.SessionID) {
	result := auditLogsUnavailable
	defer func() { setAuditResult(w, result) }()
	if s.cfg.LogToken == "" {
		logsUnavailable(w)
		return
	}
	id := string(sessionID)
	if !validSessionID(id) {
		result = auditValidation
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "invalid request", []apiv1.FieldError{{
			Path: "session_id", Reason: "is not a session id",
		}})
		return
	}
	if _, err := s.lookupLogAddr(id); err != nil {
		if r.Context().Err() != nil {
			result = auditClientClosed
			return
		}
		result = logResult(err)
		s.writeLogProblem(w, err)
		return
	}
	resp, err := s.dialExport(r.Context(), id)
	if err != nil {
		if r.Context().Err() != nil {
			result = auditClientClosed
			return
		}
		result = logResult(err)
		s.writeLogProblem(w, err)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", exportContentType(resp.Header.Get("Content-Type")))
	if resp.ContentLength >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(resp.ContentLength, 10))
	}
	w.WriteHeader(http.StatusOK)
	n, err := io.Copy(w, resp.Body)
	switch {
	case r.Context().Err() != nil:
		result = auditClientClosed
	case err != nil || (resp.ContentLength >= 0 && n < resp.ContentLength):
		result = auditIncomplete
	default:
		result = auditOK
	}
}

func logsUnavailable(w http.ResponseWriter) {
	writeProblem(w, http.StatusServiceUnavailable, apiv1.ErrorCodeLogsUnavailable, "Logs Unavailable", "logs are unavailable", nil)
}

func (s *Server) writeLogProblem(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errNotFound), errors.Is(err, errLogAbsent):
		writeProblem(w, http.StatusNotFound, apiv1.ErrorCodeNotFound, "Not Found", "not found", nil)
	case errors.Is(err, errRegionUnavailable):
		writeProblem(w, http.StatusServiceUnavailable, apiv1.ErrorCodeRegionUnavailable, "Region Unavailable", "region is unavailable", nil)
	default:
		if !errors.Is(err, errLogNoLeader) && !errors.Is(err, errLogDenied) && !errors.Is(err, errLogDown) && !errors.Is(err, errLogProtocol) {
			s.log.Warn().Err(err).Msg("logs")
		}
		logsUnavailable(w)
	}
}

func logResult(err error) string {
	switch {
	case errors.Is(err, errNotFound), errors.Is(err, errLogAbsent):
		return auditNotFound
	case errors.Is(err, errRegionUnavailable):
		return auditRegionUnavailable
	default:
		return auditLogsUnavailable
	}
}

func parseLogParams(p apiv1.GetSessionLogsParams) (logQuery, []apiv1.FieldError, error) {
	q := logQuery{}
	if p.Follow != nil {
		q.follow = *p.Follow
	}
	if p.LastEventID != nil && *p.LastEventID != "" {
		resume, err := parseLogResume(*p.LastEventID)
		if err != nil {
			return logQuery{}, []apiv1.FieldError{{Path: "Last-Event-ID", Reason: "is not a resume id"}}, err
		}
		q.resume = resume
		q.resumed = true
		return q, nil, nil
	}
	if p.Tail != nil && *p.Tail < 0 {
		return logQuery{}, []apiv1.FieldError{{Path: "tail", Reason: "must not be negative"}}, errors.New("api: negative tail")
	}
	q.tail = p.Tail
	return q, nil, nil
}

func parseLogResume(raw string) ([2]string, error) {
	var out [2]string
	left, right, ok := strings.Cut(raw, ";")
	if !ok || strings.Contains(right, ";") {
		return out, errors.New("api: log resume")
	}
	name0, cur0, ok0 := strings.Cut(left, "=")
	name1, cur1, ok1 := strings.Cut(right, "=")
	if !ok0 || !ok1 || name0 != "stdout" || name1 != "stderr" || strings.Contains(cur0, "=") || strings.Contains(cur1, "=") {
		return out, errors.New("api: log resume")
	}
	if !validCursor(cur0) || !validCursor(cur1) {
		return out, errors.New("api: log resume")
	}
	out[logStdout] = cur0
	out[logStderr] = cur1
	return out, nil
}

func exportContentType(v string) string {
	v = trimHeader(v)
	if v == "" {
		return logExportType
	}
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c <= ' ' || c >= 0x7f {
			return logExportType
		}
	}
	return v
}

func trimHeader(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}

func setAuditResult(w http.ResponseWriter, result string) {
	for w != nil {
		if c, ok := w.(*capture); ok {
			c.result = result
			return
		}
		u, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return
		}
		w = u.Unwrap()
	}
}

func (s *Server) openLogSSE(w http.ResponseWriter, ctx context.Context, cancel context.CancelFunc) (*sseHub, <-chan struct{}) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	qcap := sseQueueCap
	if qcap < 1 {
		qcap = 1
	}
	hb := sseHeartbeat
	if hb <= 0 {
		hb = sseHeartbeatDefault
	}
	hub := newSSEHub(w, qcap, cancel)
	done := make(chan struct{})
	go func() {
		defer close(done)
		hub.writeLoop(ctx, hb)
	}()
	return hub, done
}

type logStaged struct {
	event string
	data  []byte
}

type logOut struct {
	id, event string
	data      []byte
}

// logMerge holds one SSE response. Events stay buffered until both streams have sent a
// next cursor (E14-D011). After that, each frame is emitted in the order it was applied.
// end has no stream. removed wins over eof and final. Once ended, a later frame may still
// supply a missing next, and it does not append another log line.
type logMerge struct {
	follow           bool
	cur              [2]string
	seen             [2]bool
	eof, final, gone [2]bool
	buf              []logStaged
	ended            bool
}

func newLogMerge(follow bool, resume [2]string) *logMerge {
	return &logMerge{follow: follow, cur: resume}
}

func (m *logMerge) id() string {
	return "stdout=" + m.cur[logStdout] + ";stderr=" + m.cur[logStderr]
}

func (m *logMerge) apply(stream int, f logFrame) []logOut {
	if f.empty || (stream != logStdout && stream != logStderr) {
		return nil
	}
	if f.next != "" {
		m.cur[stream] = f.next
		m.seen[stream] = true
	}
	if m.ended {
		return m.flush()
	}
	if f.gap {
		m.stage("gap", logJSON(apiv1.LogGapEvent{Stream: logStream(stream)}))
	}
	if len(f.data) > 0 {
		m.stage("log", logJSON(apiv1.LogEvent{Stream: logStream(stream), Data: string(f.data)}))
	}
	if f.removed {
		m.gone[stream] = true
	}
	if f.eof {
		m.eof[stream] = true
	}
	if f.final {
		m.final[stream] = true
	}
	if reason, ok := m.endReason(); ok {
		m.stage("end", logJSON(apiv1.LogEndEvent{Reason: reason}))
		m.ended = true
	}
	return m.flush()
}

func (m *logMerge) endReason() (apiv1.LogEndReason, bool) {
	if m.gone[logStdout] || m.gone[logStderr] {
		return apiv1.Removed, true
	}
	if m.follow && m.final[logStdout] && m.final[logStderr] {
		return apiv1.Terminal, true
	}
	if !m.follow && m.eof[logStdout] && m.eof[logStderr] {
		return apiv1.Eof, true
	}
	return "", false
}

func (m *logMerge) stage(event string, data []byte) {
	m.buf = append(m.buf, logStaged{event: event, data: data})
}

func (m *logMerge) flush() []logOut {
	if !m.seen[logStdout] || !m.seen[logStderr] || len(m.buf) == 0 {
		return nil
	}
	id := m.id()
	out := make([]logOut, len(m.buf))
	for i, st := range m.buf {
		out[i] = logOut{id: id, event: st.event, data: st.data}
	}
	m.buf = nil
	return out
}

func logStream(stream int) apiv1.LogStream {
	if stream == logStderr {
		return apiv1.Stderr
	}
	return apiv1.Stdout
}

func logJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}
