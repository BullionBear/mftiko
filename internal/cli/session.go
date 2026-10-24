package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/google/uuid"
)

// sessionAttempts is the first POST plus at most three retries (E11-D022).
// The three waits are 1s, 2s, and 4s, one before each retry.
const sessionAttempts = 4

var sessionBackoffs = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}

// sessionRetryWait sleeps before a retry. Tests replace it so a dropped
// connection does not wait on the wall clock. A canceled parent context ends
// the wait and that attempt is not sent.
var sessionRetryWait = func(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func runSessionCreate(a *App) int {
	key := a.sessionKey()
	if len(a.Args) != 0 {
		return a.failSession(key, ExitUsage, "session create takes no arguments")
	}
	if msg := a.unexpectedFlags("f", flagPair("idempotency", "key"), "limit", "cursor"); msg != "" {
		return a.failSession(key, ExitUsage, msg)
	}
	if a.opt.limitSet || a.opt.cursorSet {
		return a.failSession(key, ExitUsage, "-limit and -cursor are only for strategy list and strategy versions, and account list")
	}
	if !a.opt.file.set || a.opt.file.v == "" {
		return a.failSession(key, ExitUsage, "session create requires -f")
	}
	name := a.opt.file.v
	raw, err := readSessionFile(name, a.Stdin)
	if err != nil {
		return a.failSession(key, ExitUsage, err.Error())
	}
	body, locs, err := convertSession(name, raw)
	if err != nil {
		return a.failSession(key, ExitUsage, err.Error())
	}
	api, code := a.api()
	if code != 0 {
		a.writeSessionKey(key)
		return code
	}
	res, err := a.postSession(api, key, body)
	if err != nil {
		return a.failSession(key, ExitNetwork, "network (2): "+err.Error())
	}
	if res.Status < 200 || res.Status >= 300 {
		return a.reportSession(res, name, locs, key)
	}
	return a.writeSession(res, key)
}

// sessionKey is the Idempotency-Key for this run (E11-D022). A flag value is
// sent unchanged, including empty; otherwise the run gets a new UUID.
func (a *App) sessionKey() string {
	if a.opt.idemKey.set {
		return a.opt.idemKey.v
	}
	return uuid.NewString()
}

func (a *App) failSession(key string, code int, msg string) int {
	a.exitCode(code, msg)
	a.writeSessionKey(key)
	return code
}

func (a *App) writeSessionKey(key string) {
	fmt.Fprintf(a.Stderr, "Idempotency-Key: %s\n", key)
}

// postSession sends the same body and key. It retries only a connection
// failure or a timeout, and only while the parent context is still active
// (E11-D022). An HTTP status, including 409 and 503, is not retried.
func (a *App) postSession(api *API, key string, body []byte) (Result, error) {
	var res Result
	var err error
	for attempt := 0; attempt < sessionAttempts; attempt++ {
		if attempt > 0 {
			if waitErr := sessionRetryWait(a.ctx, sessionBackoffs[attempt-1]); waitErr != nil {
				return Result{}, waitErr
			}
		}
		res, err = api.CreateSession(a.ctx, key, body)
		if err == nil || !retryableNet(a.ctx, err) {
			return res, err
		}
	}
	return res, err
}

// retryableNet reports a connection failure or a timeout. The parent context
// being canceled is not retried. http.Client's own timeout surfaces as a
// deadline on the error while the parent context is still active.
func retryableNet(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var u *url.Error
	if errors.As(err, &u) {
		return u.Timeout() || connErr(u.Err)
	}
	return connErr(err)
}

func connErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var op *net.OpError
	if errors.As(err, &op) {
		return true
	}
	var dns *net.DNSError
	return errors.As(err, &dns)
}

// readSessionFile reads path, or stdin when path is "-". The cap matches the
// response body cap so a session file cannot be larger than a body this
// client will read.
func readSessionFile(name string, stdin io.Reader) ([]byte, error) {
	var r io.Reader
	if name == "-" {
		r = stdin
	} else {
		f, err := os.Open(name)
		if err != nil {
			return nil, fmt.Errorf("cannot read session file: %w", err)
		}
		defer f.Close()
		return readSessionLimited(name, f)
	}
	return readSessionLimited(name, r)
}

func readSessionLimited(name string, r io.Reader) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, maxBody+1))
	if err != nil {
		return nil, fmt.Errorf("cannot read session file: %w", err)
	}
	if len(b) > maxBody {
		return nil, fmt.Errorf("%s: session file is too large", name)
	}
	return b, nil
}

func (a *App) writeSession(res Result, key string) int {
	if a.JSON {
		if _, err := a.Stdout.Write(a.sec.redactBytes(res.Body)); err != nil {
			return a.failSession(key, ExitUsage, err.Error())
		}
	} else {
		var sess apiv1.Session
		if err := json.Unmarshal(res.Body, &sess); err != nil {
			return a.failSession(key, ExitUsage, "response is not a session")
		}
		var buf bytes.Buffer
		header := []string{"SESSION_ID", "DIGEST"}
		rows := [][]string{{sess.Id, sess.Spec.Strategy.Digest}}
		if err := WriteTable(&buf, header, rows); err != nil {
			return a.failSession(key, ExitUsage, err.Error())
		}
		if _, err := a.Stdout.Write(a.sec.redactBytes(buf.Bytes())); err != nil {
			return a.failSession(key, ExitUsage, err.Error())
		}
	}
	a.noteResult(res)
	return ExitOK
}

// reportSession writes the problem the way reportProblem does, then maps each
// JSON Pointer onto the file when the node was recorded (E11-D023). A 409
// conflict adds the sentence that this key was used for different content
// (E11-D022). algo_overlap does not. The key line is last on every error.
func (a *App) reportSession(res Result, file string, locs map[string]sourcePos, key string) int {
	code := writeSessionProblem(a.Stderr, res.Body, res.RequestID, a.sec, file, locs)
	a.writeSessionKey(key)
	return code
}

func writeSessionProblem(w io.Writer, raw []byte, requestID string, sec secrets, file string, locs map[string]sourcePos) int {
	var p apiv1.Problem
	if err := json.Unmarshal(raw, &p); err != nil || p.Code == "" {
		fmt.Fprintf(w, "mftiko: %s\n", sec.redact("response is not a problem"))
		return ExitUsage
	}
	code, ok := ExitCode(p.Code)
	var b bytes.Buffer
	if ok {
		fmt.Fprintf(&b, "mftiko: %s (%d): %s\n", p.Code, code, p.Title)
	} else {
		fmt.Fprintf(&b, "mftiko: %s: %s\n", p.Code, p.Title)
		code = ExitUsage
	}
	if p.Code == apiv1.ErrorCodeRegistryUnavailable {
		fmt.Fprintln(&b, "registry connection problem")
	}
	if p.Detail != nil && *p.Detail != "" {
		fmt.Fprintln(&b, *p.Detail)
	}
	if p.Errors != nil {
		for _, e := range *p.Errors {
			if e.Path == "" {
				continue
			}
			if pos, found := locs[e.Path]; found {
				if e.Reason != "" {
					fmt.Fprintf(&b, "%s:%d:%d %s %s\n", file, pos.Line, pos.Col, e.Path, e.Reason)
				} else {
					fmt.Fprintf(&b, "%s:%d:%d %s\n", file, pos.Line, pos.Col, e.Path)
				}
				continue
			}
			fmt.Fprintln(&b, e.Path)
		}
	}
	if p.Code == apiv1.ErrorCodeConflict {
		fmt.Fprintln(&b, "此 key 已用於不同內容")
	}
	if requestID != "" {
		fmt.Fprintf(&b, "X-Request-Id: %s\n", requestID)
	}
	fmt.Fprint(w, sec.redact(b.String()))
	return code
}
