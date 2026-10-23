package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// How much of a request the audit row keeps. These are code constants, not decisions: D1049 leaves
// the queue bound to the implementation, and the body rules are D1052.
const (
	maxRequestBody = 1 << 20
	detailLimit    = 8 << 10
	maxAuditText   = 8 << 10
)

type accessKey struct{}

// accessState is the per-request data the handler and the audit row share.
type accessState struct {
	id     string
	start  time.Time
	body   []byte
	tooBig bool
	actor  *identity
}

func accessFrom(ctx context.Context) *accessState {
	st, _ := ctx.Value(accessKey{}).(*accessState)
	return st
}

func requestPath(r *http.Request) string {
	p := r.URL.Path
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		return "/" + p
	}
	return p
}

// newRequestID is the server's id for this response (D1053). It is 32 hex characters.
func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		binary.LittleEndian.PutUint64(b[:8], uint64(time.Now().UnixNano()))
		binary.LittleEndian.PutUint64(b[8:], uint64(os.Getpid()))
	}
	return hex.EncodeToString(b[:])
}

// capture records the status and, for an error, enough of the body to read the problem detail.
// The audit row does not store the response body (D1010).
type capture struct {
	http.ResponseWriter
	status int
	wrote  bool
	buf    []byte
}

func (c *capture) WriteHeader(code int) {
	if c.wrote {
		return
	}
	c.status = code
	c.wrote = true
	c.ResponseWriter.WriteHeader(code)
}

func (c *capture) Write(p []byte) (int, error) {
	if !c.wrote {
		c.WriteHeader(http.StatusOK)
	}
	if c.status >= http.StatusBadRequest && len(c.buf) < detailLimit {
		n := detailLimit - len(c.buf)
		if n > len(p) {
			n = len(p)
		}
		c.buf = append(c.buf, p[:n]...)
	}
	return c.ResponseWriter.Write(p)
}

func (c *capture) Flush() {
	if f, ok := c.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (c *capture) Unwrap() http.ResponseWriter { return c.ResponseWriter }

// serveAudited is the /api path: authenticate, handle, then queue one audit row when the handler
// returns. A streaming handler (SSE) returns when the stream ends, so that is when its row is
// queued (D1010). The response is already committed by then; a full queue does not change it.
func (s *Server) serveAudited(w http.ResponseWriter, r *http.Request, st *accessState) {
	s.apiWG.Add(1)
	defer s.apiWG.Done()
	cw := &capture{ResponseWriter: w}
	defer func() {
		if s.audit != nil {
			s.audit.enqueue(s.auditRow(r, st, cw))
		}
	}()
	s.serveAPI(cw, r, st)
}

// serveAPI authenticates before it looks at Content-Type, so an anonymous client gets 401 rather
// than 400. POST /api/v1/auth/login is the only route that skips authentication (D1009).
func (s *Server) serveAPI(w http.ResponseWriter, r *http.Request, st *accessState) {
	if isWrite(r.Method) {
		body, tooBig, err := readBody(w, r)
		if err != nil {
			writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, "invalid request", nil)
			return
		}
		st.body = body
		st.tooBig = tooBig
		r.Body = io.NopCloser(bytes.NewReader(body))
	}
	if !isLogin(r) {
		if !s.authenticate(w, r, st) {
			return
		}
	}
	if st.tooBig {
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, "request body is too large", nil)
		return
	}
	if isWrite(r.Method) && !jsonContentType(r) {
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, "Content-Type must be application/json", nil)
		return
	}
	s.api.ServeHTTP(w, r)
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, false, nil
	}
	// Unwrap so MaxBytesReader can mark the real response when the limit is hit.
	// Otherwise a keep-alive connection would reuse unread body bytes.
	r.Body = http.MaxBytesReader(unwrapWriter(w), r.Body, maxRequestBody)
	b, err := io.ReadAll(r.Body)
	if err == nil {
		return b, false, nil
	}
	var too *http.MaxBytesError
	if errors.As(err, &too) {
		return b, true, nil
	}
	return nil, false, err
}

func unwrapWriter(w http.ResponseWriter) http.ResponseWriter {
	for {
		u, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return w
		}
		next := u.Unwrap()
		if next == nil {
			return w
		}
		w = next
	}
}

func (s *Server) waitAPI(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		s.apiWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}
