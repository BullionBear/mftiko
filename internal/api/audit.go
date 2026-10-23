package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// Audit queue (D1049). The bound is a code constant: the decision says the queue has a limit and
// does not pick the number. Tests set auditQueueCap before the server starts.
const (
	auditQueueCapDefault = 256
	auditWriteTimeout    = 2 * time.Second
	auditBodyLimit       = 64 << 10
)

var auditQueueCap = auditQueueCapDefault

const auditInsertSQL = `
INSERT INTO audit (
    at_ns, user_name, method, path, query, status, duration_ns, request_id, remote_addr,
    session_id, api_id, body, body_len, body_truncated, detail
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9,
    $10, $11, $12::jsonb, $13, $14, $15
)`

// auditRow is one request. body is redacted JSON, or nil when there is nothing to store.
type auditRow struct {
	at        time.Time
	user      string
	method    string
	path      string
	query     string
	status    int
	duration  time.Duration
	requestID string
	remote    string
	sessionID *string
	apiID     *int64
	body      []byte
	bodyLen   *int64
	truncated bool
	detail    string
}

// auditor writes rows off the request path (D1049). A full queue or a failed insert is an error
// log and a counter; the HTTP response has already been sent (D1010).
type auditor struct {
	pool *pgxpool.Pool
	log  zerolog.Logger
	ch   chan auditRow

	// hold, when non-nil, blocks inside insert before the query. Tests close it.
	hold chan struct{}

	writing  atomic.Bool
	flushing atomic.Bool
	failed   atomic.Int64
	dropped  atomic.Int64

	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
	poolOnce  sync.Once

	base   context.Context
	cancel context.CancelFunc
	exited chan struct{}
}

// openAudit builds the pool and starts the writer. pgxpool does not dial when MinConns is 0, so
// this returns while Postgres is down (D1048). ConnectTimeout is bounded so a later dial cannot
// outlive the shutdown window.
func openAudit(dsn string, log zerolog.Logger) (*auditor, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("postgres pool")
	}
	cfg.MinConns = 0
	cfg.MaxConns = 2
	cfg.ConnConfig.ConnectTimeout = auditWriteTimeout
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, errors.New("postgres pool")
	}
	a := &auditor{
		pool:   pool,
		log:    log,
		ch:     make(chan auditRow, auditQueueCap),
		exited: make(chan struct{}),
	}
	a.base, a.cancel = context.WithCancel(context.Background())
	go a.loop()
	return a, nil
}

func (a *auditor) enqueue(row auditRow) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		n := a.dropped.Add(1)
		a.log.Error().Str("request_id", row.requestID).Int64("audit_dropped", n).Msg("audit dropped on shutdown")
		return
	}
	select {
	case a.ch <- row:
	default:
		n := a.dropped.Add(1)
		a.log.Error().Str("request_id", row.requestID).Int64("audit_dropped", n).Msg("audit queue full")
	}
}

// shutdown closes the queue and waits until the writer finishes what is already queued, or until
// ctx ends. The caller passes the same context as http.Server.Shutdown, so the drain stays inside
// D1027's window. Rows still queued when the window ends are dropped.
func (a *auditor) shutdown(ctx context.Context) {
	if a == nil {
		return
	}
	a.flushing.Store(true)
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		close(a.ch)
		a.mu.Unlock()
	})
	select {
	case <-a.exited:
	case <-ctx.Done():
		a.cancel()
		<-a.exited
	}
	a.poolOnce.Do(func() { a.pool.Close() })
}

func (a *auditor) loop() {
	defer close(a.exited)
	for {
		if a.base.Err() != nil {
			a.abandon()
			return
		}
		select {
		case <-a.base.Done():
			a.abandon()
			return
		case row, ok := <-a.ch:
			if !ok {
				return
			}
			a.insert(row)
		}
	}
}

func (a *auditor) abandon() {
	for {
		select {
		case row, ok := <-a.ch:
			if !ok {
				return
			}
			n := a.dropped.Add(1)
			a.log.Error().Str("request_id", row.requestID).Int64("audit_dropped", n).Msg("audit dropped on shutdown")
		default:
			return
		}
	}
}

func (a *auditor) insert(row auditRow) {
	if a.hold != nil {
		a.writing.Store(true)
		select {
		case <-a.hold:
			a.writing.Store(false)
		case <-a.base.Done():
			a.writing.Store(false)
			a.fail(row, a.base.Err())
			return
		}
	}
	if err := a.base.Err(); err != nil {
		a.fail(row, err)
		return
	}
	ctx, cancel := context.WithTimeout(a.base, auditWriteTimeout)
	defer cancel()
	a.writing.Store(true)
	defer a.writing.Store(false)
	_, err := a.pool.Exec(ctx, auditInsertSQL,
		row.at.UnixNano(), row.user, row.method, row.path, row.query, row.status, row.duration.Nanoseconds(),
		row.requestID, row.remote, row.sessionID, row.apiID, jsonbArg(row.body), int64Arg(row.bodyLen),
		row.truncated, row.detail,
	)
	if err != nil {
		a.fail(row, err)
	}
}

func jsonbArg(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}

func int64Arg(n *int64) any {
	if n == nil {
		return nil
	}
	return *n
}

func (a *auditor) fail(row auditRow, err error) {
	n := a.failed.Add(1)
	// The line names the request. It does not carry the body, the query, or the token.
	a.log.Error().Str("request_id", row.requestID).Str("path", row.path).Int64("audit_failures", n).Err(err).Msg("audit write")
}

func (s *Server) auditRow(r *http.Request, st *accessState, cw *capture) auditRow {
	status := cw.status
	if status == 0 {
		status = http.StatusOK
	}
	end := s.clock()
	dur := end.Sub(st.start)
	if dur < 0 {
		dur = 0
	}
	user := ""
	if st.actor != nil {
		user = st.actor.name
	}
	stored, n, truncated, parsed := redactedBody(st.body, st.tooBig, isWrite(r.Method))
	sessionID, apiID := pathTargets(requestPath(r))
	sessionID, apiID = queryTargets(r.URL.RawQuery, sessionID, apiID)
	sessionID, apiID = bodyTargets(parsed, sessionID, apiID)
	return auditRow{
		at:        st.start,
		user:      user,
		method:    r.Method,
		path:      clip(requestPath(r), maxAuditText),
		query:     auditQuery(r.URL.RawQuery),
		status:    status,
		duration:  dur,
		requestID: st.id,
		remote:    r.RemoteAddr,
		sessionID: strPtr(sessionID),
		apiID:     apiID,
		body:      stored,
		bodyLen:   n,
		truncated: truncated,
		detail:    responseDetail(status, cw.buf),
	}
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// redactedBody applies D1052. Credential and token fields are removed before the size is measured.
// A non-JSON body, an empty body, or a body over the read limit stores no content.
func redactedBody(raw []byte, tooBig, write bool) (stored []byte, n *int64, truncated bool, parsed any) {
	if !write {
		return nil, nil, false, nil
	}
	length := int64(len(raw))
	n = &length
	if tooBig || len(raw) == 0 {
		return nil, n, tooBig, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, n, false, nil
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, n, false, nil
	}
	redactValue(v)
	b, err := json.Marshal(v)
	if err != nil {
		return nil, n, false, nil
	}
	stored, truncated = truncateJSON(b)
	return stored, n, truncated, v
}

// redactValue removes account credentials and token fields at every object (D1010, D1052).
func redactValue(v any) {
	switch x := v.(type) {
	case map[string]any:
		delete(x, "credentials")
		delete(x, "token")
		for _, child := range x {
			redactValue(child)
		}
	case []any:
		for _, child := range x {
			redactValue(child)
		}
	}
}

type truncMarker struct {
	Truncated bool   `json:"truncated"`
	Len       int    `json:"len"`
	Prefix    string `json:"prefix"`
}

// truncateJSON keeps a redacted body that fits in auditBodyLimit. Over the limit, the stored
// document is a marker whose own encoding also fits.
func truncateJSON(redacted []byte) ([]byte, bool) {
	if len(redacted) <= auditBodyLimit {
		return redacted, false
	}
	prefix := redacted
	budget := auditBodyLimit - 80
	if budget < 0 {
		budget = 0
	}
	if len(prefix) > budget {
		prefix = prefix[:budget]
	}
	for {
		for len(prefix) > 0 && !utf8.Valid(prefix) {
			prefix = prefix[:len(prefix)-1]
		}
		b, err := json.Marshal(truncMarker{Truncated: true, Len: len(redacted), Prefix: string(prefix)})
		if err == nil && len(b) <= auditBodyLimit {
			return b, true
		}
		if len(prefix) == 0 {
			b, _ = json.Marshal(truncMarker{Truncated: true, Len: len(redacted)})
			return b, true
		}
		cut := len(prefix) / 8
		if cut < 1 {
			cut = 1
		}
		prefix = prefix[:len(prefix)-cut]
	}
}

// auditQuery keeps the raw query unless it carries a token or credentials parameter.
func auditQuery(raw string) string {
	if raw == "" {
		return ""
	}
	vals, err := url.ParseQuery(raw)
	if err != nil || (!vals.Has("token") && !vals.Has("credentials")) {
		return clip(raw, maxAuditText)
	}
	vals.Del("token")
	vals.Del("credentials")
	return clip(vals.Encode(), maxAuditText)
}

func pathTargets(p string) (string, *int64) {
	var sessionID string
	var apiID *int64
	if rest, ok := strings.CutPrefix(p, "/api/v1/sessions/"); ok {
		id, _, _ := strings.Cut(rest, "/")
		sessionID = id
	}
	if rest, ok := strings.CutPrefix(p, "/api/v1/accounts/"); ok {
		id, _, _ := strings.Cut(rest, "/")
		if n, err := strconv.ParseInt(id, 10, 64); err == nil {
			apiID = &n
		}
	}
	return sessionID, apiID
}

func queryTargets(raw, sessionID string, apiID *int64) (string, *int64) {
	if raw == "" || (sessionID != "" && apiID != nil) {
		return sessionID, apiID
	}
	q, err := url.ParseQuery(raw)
	if err != nil {
		return sessionID, apiID
	}
	if sessionID == "" {
		sessionID = q.Get("session_id")
	}
	if apiID == nil {
		if n, ok := int64From(q.Get("api_id")); ok {
			apiID = &n
		}
	}
	return sessionID, apiID
}

func bodyTargets(parsed any, sessionID string, apiID *int64) (string, *int64) {
	m, ok := parsed.(map[string]any)
	if !ok {
		return sessionID, apiID
	}
	if sessionID == "" {
		if s, ok := m["session_id"].(string); ok {
			sessionID = s
		}
	}
	if apiID == nil {
		if n, ok := int64From(m["api_id"]); ok {
			apiID = &n
		}
	}
	return sessionID, apiID
}

func int64From(v any) (int64, bool) {
	switch n := v.(type) {
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	case string:
		if n == "" {
			return 0, false
		}
		i, err := strconv.ParseInt(n, 10, 64)
		return i, err == nil
	default:
		return 0, false
	}
}

func responseDetail(status int, buf []byte) string {
	if status < http.StatusBadRequest || len(buf) == 0 {
		return ""
	}
	var p struct {
		Detail string `json:"detail"`
	}
	if json.Unmarshal(buf, &p) != nil {
		return ""
	}
	return clip(p.Detail, detailLimit)
}

func clip(s string, n int) string {
	if len(s) <= n || n < 0 {
		return s
	}
	s = s[:n]
	for len(s) > 0 && !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}
