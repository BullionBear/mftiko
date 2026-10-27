//go:build unix

package agent

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"google.golang.org/protobuf/proto"

	logv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/log/v1"
	"github.com/BullionBear/mftiko/internal/platform/logread"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// startLogs opens the session log endpoint for this term (E14-D019–E14-D024).
// The returned address is empty until listen succeeds. ready is signaled then,
// once, with a buffer of one; it is not closed. stop waits until the listener
// is gone. A disabled config returns a nil address, a nil ready channel, and
// a stop that does nothing.
func (p *plane) startLogs(ctx context.Context, termDone <-chan struct{}, cfg LogConfig) (func() string, <-chan struct{}, func()) {
	if !cfg.enabled() {
		return nil, nil, func() {}
	}
	cfg.apply()
	token, err := parseLogToken(cfg.TokenSHA256)
	if err != nil {
		p.log.Warn().Err(err).Msg("log endpoint disabled")
		return nil, nil, func() {}
	}
	host, herr := os.Hostname()
	if herr != nil || host == "" {
		host = ""
	}
	view := newLogView()
	p.logView = view
	srv := newLogServer(p, cfg, token, host)
	p.logs = srv
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	ready := make(chan struct{}, 1)
	var mu sync.Mutex
	var advertised string
	go func() {
		defer close(done)
		srv.serve(loopCtx, termDone, func(addr string) {
			mu.Lock()
			advertised = addr
			mu.Unlock()
			select {
			case ready <- struct{}{}:
			default:
			}
		})
	}()
	stop := func() {
		cancel()
		srv.close()
		<-done
	}
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		return advertised
	}, ready, stop
}

// logServer is one term's contract L listener.
type logServer struct {
	plane *plane
	cfg   LogConfig
	view  *logView
	token [32]byte
	lim   *logread.Limiter
	slots chan struct{}
	host  string

	mu    sync.Mutex
	ln    net.Listener
	http  *http.Server
	holds map[string]int
	idle  map[string]chan struct{}
}

func newLogServer(p *plane, cfg LogConfig, token [32]byte, host string) *logServer {
	n := cfg.MaxStreams
	if n <= 0 {
		n = DefaultLogStreams
	}
	return &logServer{
		plane: p,
		cfg:   cfg,
		view:  p.logView,
		token: token,
		lim:   logread.NewLimiter(0, cfg.MaxRate),
		slots: make(chan struct{}, n),
		host:  host,
		holds: map[string]int{},
		idle:  map[string]chan struct{}{},
	}
}

var _ logQuiescer = (*logServer)(nil)

func (s *logServer) serve(ctx context.Context, termDone <-chan struct{}, publish func(string)) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if termDone != nil {
		go func() {
			select {
			case <-termDone:
				cancel()
			case <-ctx.Done():
			}
		}()
	}
	for {
		if ctx.Err() != nil {
			return
		}
		ln, err := net.Listen("tcp", s.cfg.Listen)
		if err != nil {
			s.plane.log.Warn().Err(err).Str("listen", s.cfg.Listen).Msg("log listen")
			if !waitDuration(ctx, s.cfg.Retry) {
				return
			}
			continue
		}
		if !s.serveOne(ctx, ln, publish) {
			return
		}
	}
}

func (s *logServer) serveOne(ctx context.Context, ln net.Listener, publish func(string)) bool {
	hs := &http.Server{
		Handler:           s.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ErrorLog:          log.New(io.Discard, "", 0),
	}
	s.mu.Lock()
	if ctx.Err() != nil {
		s.mu.Unlock()
		ln.Close()
		return false
	}
	s.ln = ln
	s.http = hs
	s.mu.Unlock()
	adv := advertiseAddr(s.host, s.cfg.Advertise, ln.Addr().String())
	if publish != nil {
		publish(adv)
	}
	s.plane.log.Info().Str("addr", ln.Addr().String()).Str("advertise", adv).Msg("log endpoint listening")
	err := hs.Serve(ln)
	s.mu.Lock()
	if s.ln == ln {
		s.ln = nil
	}
	if s.http == hs {
		s.http = nil
	}
	s.mu.Unlock()
	if ctx.Err() != nil {
		return false
	}
	s.plane.log.Warn().Err(err).Msg("log serve")
	return true
}

func (s *logServer) close() {
	s.mu.Lock()
	ln := s.ln
	hs := s.http
	s.mu.Unlock()
	if hs != nil {
		hs.Close()
	}
	if ln != nil {
		ln.Close()
	}
}

func advertiseAddr(host, explicit, bound string) string {
	if explicit != "" {
		return explicit
	}
	_, port, err := net.SplitHostPort(bound)
	if err != nil || port == "" {
		return bound
	}
	return net.JoinHostPort(host, port)
}

func waitDuration(ctx context.Context, d time.Duration) bool {
	if d < 0 {
		d = 0
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (s *logServer) routes() http.Handler {
	mux := http.NewServeMux()
	// The literal export pattern wins over the stream wildcard.
	mux.HandleFunc("GET /v1/sessions/{id}/logs/export", s.export)
	mux.HandleFunc("GET /v1/sessions/{id}/logs/{stream}", s.stream)
	return mux
}

func (s *logServer) allow(r *http.Request) bool {
	sum := sha256.Sum256([]byte(presentedToken(r)))
	return subtle.ConstantTimeCompare(sum[:], s.token[:]) == 1
}

func presentedToken(r *http.Request) string {
	h := strings.TrimSpace(r.Header.Get("Authorization"))
	if h == "" {
		return ""
	}
	scheme, rest, found := strings.Cut(h, " ")
	if !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	if !found {
		return ""
	}
	return strings.TrimSpace(rest)
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

func (s *logServer) acquire(w http.ResponseWriter) bool {
	select {
	case s.slots <- struct{}{}:
		return true
	default:
		w.Header().Set("Retry-After", logRetryAfter)
		w.WriteHeader(http.StatusServiceUnavailable)
		return false
	}
}

func (s *logServer) releaseSlot() { <-s.slots }

// hold counts a request that still has id's files open. The returned function
// drops that count. quiesce waits until the count is zero, which is before the
// HTTP body finishes when the bytes have already been copied off the files.
func (s *logServer) hold(id string) func() {
	s.mu.Lock()
	s.holds[id]++
	s.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			s.holds[id]--
			if s.holds[id] <= 0 {
				delete(s.holds, id)
				if ch := s.idle[id]; ch != nil {
					close(ch)
					delete(s.idle, id)
				}
			}
			s.mu.Unlock()
		})
	}
}

func (s *logServer) quiesce(ctx context.Context, id string) error {
	s.mu.Lock()
	if s.holds[id] == 0 {
		s.mu.Unlock()
		return nil
	}
	ch := s.idle[id]
	if ch == nil {
		ch = make(chan struct{})
		s.idle[id] = ch
	}
	s.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *logServer) path(id, stream string) string {
	return shim.OutputPath(s.plane.workDir, string(state.PlaneSTS), string(WorkerKey(id)), stream)
}

func (s *logServer) prepare(w http.ResponseWriter, r *http.Request) (string, bool) {
	if !s.allow(r) {
		w.WriteHeader(http.StatusUnauthorized)
		return "", false
	}
	id := r.PathValue("id")
	if !validSessionID(id) {
		w.WriteHeader(http.StatusNotFound)
		return "", false
	}
	if err := s.view.waitReady(r.Context()); err != nil {
		return "", false
	}
	present, ready := s.view.present(id)
	if !ready || !present {
		w.WriteHeader(http.StatusNotFound)
		return "", false
	}
	return id, true
}

func (s *logServer) fail(w http.ResponseWriter, err error) {
	switch {
	case err == nil || errors.Is(err, context.Canceled):
		return
	case errors.Is(err, logread.ErrCursor) || errors.Is(err, logread.ErrOrigin):
		w.WriteHeader(http.StatusBadRequest)
	case errors.Is(err, logread.ErrBusy):
		w.Header().Set("Retry-After", logRetryAfter)
		w.WriteHeader(http.StatusServiceUnavailable)
	default:
		s.plane.log.Warn().Err(err).Msg("log read")
		w.WriteHeader(http.StatusInternalServerError)
	}
}

var errLogQuery = errors.New("sts: log query")

func parseReadQuery(q map[string][]string) (logread.Options, error) {
	_, from := q["from"]
	_, tail := q["tail"]
	_, cursor := q["cursor"]
	n := 0
	if from {
		n++
	}
	if tail {
		n++
	}
	if cursor {
		n++
	}
	if n != 1 {
		return logread.Options{}, errLogQuery
	}
	var opt logread.Options
	switch {
	case from:
		if firstQuery(q, "from") != "start" {
			return logread.Options{}, errLogQuery
		}
		opt.Origin = logread.OriginStart
	case tail:
		lines, err := strconv.Atoi(firstQuery(q, "tail"))
		if err != nil || lines < 0 {
			return logread.Options{}, errLogQuery
		}
		opt.Origin = logread.OriginTail
		opt.Tail = lines
	default:
		c := firstQuery(q, "cursor")
		if badLogCursor(c) {
			return logread.Options{}, errLogQuery
		}
		opt.Origin = logread.OriginCursor
		opt.Cursor = logread.Cursor(c)
	}
	if _, ok := q["follow"]; ok {
		v := firstQuery(q, "follow")
		if v == "" {
			opt.Follow = true
		} else {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return logread.Options{}, errLogQuery
			}
			opt.Follow = b
		}
	}
	return opt, nil
}

func firstQuery(q map[string][]string, key string) string {
	vs := q[key]
	if len(vs) == 0 {
		return ""
	}
	return vs[0]
}

func badLogCursor(s string) bool {
	for _, r := range s {
		if r == ';' || unicode.IsSpace(r) {
			return true
		}
	}
	return false
}

type logEv struct {
	err     error
	headers bool
	frame   *logv1.Frame
}

func sendEv(ctx context.Context, ch chan<- logEv, ev logEv) bool {
	select {
	case ch <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *logServer) stream(w http.ResponseWriter, r *http.Request) {
	if !s.allow(r) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	id := r.PathValue("id")
	name := r.PathValue("stream")
	if !validSessionID(id) || (name != "stdout" && name != "stderr") {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if err := s.view.waitReady(r.Context()); err != nil {
		return
	}
	if present, ready := s.view.present(id); !ready || !present {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	opt, err := parseReadQuery(r.URL.Query())
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if !s.acquire(w) {
		return
	}
	defer s.releaseSlot()

	readCtx, cancelRead := context.WithCancel(r.Context())
	defer cancelRead()
	go func() {
		select {
		case <-s.view.watch(id):
			cancelRead()
		case <-readCtx.Done():
		}
	}()
	ch := make(chan logEv)
	go s.pump(r.Context(), readCtx, id, name, opt, ch)

	var (
		headers bool
		fl      http.Flusher
		timer   *time.Timer
		tick    <-chan time.Time
	)
	if opt.Follow {
		timer = time.NewTimer(s.cfg.Heartbeat)
		tick = timer.C
		defer timer.Stop()
	}
	for {
		var ev logEv
		var ok bool
		if !headers {
			ev, ok = <-ch
		} else {
			select {
			case ev, ok = <-ch:
			case <-tick:
				if err := writeFrame(w, nil); err != nil {
					return
				}
				if fl != nil {
					fl.Flush()
				}
				timer.Reset(s.cfg.Heartbeat)
				continue
			case <-r.Context().Done():
				return
			}
		}
		if !ok {
			return
		}
		if ev.err != nil {
			if !headers {
				s.fail(w, ev.err)
			} else if !errors.Is(ev.err, context.Canceled) {
				s.plane.log.Warn().Err(ev.err).Str("session", id).Msg("log read")
			}
			return
		}
		if ev.headers {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			fl, _ = w.(http.Flusher)
			if fl != nil {
				fl.Flush()
			}
			headers = true
			if timer != nil {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(s.cfg.Heartbeat)
			}
			continue
		}
		if ev.frame == nil {
			continue
		}
		if err := writeFrame(w, ev.frame); err != nil {
			return
		}
		if fl != nil {
			fl.Flush()
		}
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(s.cfg.Heartbeat)
		}
		if ev.frame.GetEof() || ev.frame.GetFinal() || ev.frame.GetRemoved() {
			return
		}
	}
}

func (s *logServer) pump(reqCtx, readCtx context.Context, id, name string, opt logread.Options, ch chan<- logEv) {
	defer close(ch)
	rel := s.hold(id)
	defer rel()
	// sawTerm asks for one more poll after the phase becomes terminal. The
	// shim can report the exit before the last output bytes are copied; bytes
	// that land during that poll are returned by Next before the next Pause,
	// which then ends the follow (E14-D024).
	var sawTerm bool
	opt.Path = s.path(id, name)
	opt.Limit = s.lim
	opt.Pause = func() bool {
		present, ready := s.view.present(id)
		if ready && !present {
			return true
		}
		if !s.view.terminal(id) {
			sawTerm = false
			return false
		}
		if !sawTerm {
			sawTerm = true
			return false
		}
		return true
	}
	st, err := logread.Open(readCtx, opt)
	if err != nil {
		sendEv(reqCtx, ch, logEv{err: err})
		return
	}
	defer st.Close()
	if !opt.Follow {
		if err := st.Prime(); err != nil {
			if errors.Is(err, io.EOF) {
				if !sendEv(reqCtx, ch, logEv{headers: true}) {
					return
				}
				sendEv(reqCtx, ch, logEv{frame: &logv1.Frame{Eof: true}})
				return
			}
			sendEv(reqCtx, ch, logEv{err: err})
			return
		}
	}
	if !sendEv(reqCtx, ch, logEv{headers: true}) {
		return
	}
	if opt.Follow {
		for {
			err := st.Prime()
			if err == nil {
				break
			}
			if s.finishFollow(reqCtx, id, st, err, ch) {
				return
			}
		}
	}
	if cur := string(st.Cursor()); cur != "" {
		if !sendEv(reqCtx, ch, logEv{frame: &logv1.Frame{Next: cur}}) {
			return
		}
	}
	for {
		rec, err := st.Next()
		if err != nil {
			if !opt.Follow {
				if errors.Is(err, io.EOF) {
					sendEv(reqCtx, ch, logEv{frame: &logv1.Frame{Next: string(st.Cursor()), Eof: true}})
					return
				}
				if errors.Is(err, context.Canceled) {
					return
				}
				sendEv(reqCtx, ch, logEv{err: err})
				return
			}
			if s.finishFollow(reqCtx, id, st, err, ch) {
				return
			}
			continue
		}
		fr := &logv1.Frame{Next: string(rec.Cursor)}
		if rec.Gap {
			fr.Gap = true
		} else {
			fr.Data = rec.Data
		}
		if !sendEv(reqCtx, ch, logEv{frame: fr}) {
			return
		}
	}
}

// finishFollow reports whether the pump is done. A pause that is no longer
// terminal, and whose spec is still present, is not final (E14-D024): the
// caller keeps reading. Spec removal wins over final.
func (s *logServer) finishFollow(reqCtx context.Context, id string, st *logread.Stream, err error, ch chan<- logEv) bool {
	if errors.Is(err, io.EOF) {
		sendEv(reqCtx, ch, logEv{frame: &logv1.Frame{Next: string(st.Cursor()), Eof: true}})
		return true
	}
	present, ready := s.view.present(id)
	gone := ready && !present
	paused := errors.Is(err, logread.ErrPause)
	if paused || (errors.Is(err, context.Canceled) && gone) {
		if !gone && !s.view.terminal(id) {
			return false
		}
		fr := &logv1.Frame{Next: string(st.Cursor())}
		if gone {
			fr.Removed = true
		} else {
			fr.Final = true
		}
		sendEv(reqCtx, ch, logEv{frame: fr})
		return true
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	sendEv(reqCtx, ch, logEv{err: err})
	return true
}

func writeFrame(w io.Writer, f *logv1.Frame) error {
	var body []byte
	if f != nil {
		var err error
		body, err = proto.Marshal(f)
		if err != nil {
			return err
		}
	}
	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], uint64(len(body)))
	buf := make([]byte, n+len(body))
	copy(buf, hdr[:n])
	copy(buf[n:], body)
	_, err := w.Write(buf)
	return err
}

type logManifest struct {
	Stdout logStreamMeta `json:"stdout"`
	Stderr logStreamMeta `json:"stderr"`
}

type logStreamMeta struct {
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

func (s *logServer) export(w http.ResponseWriter, r *http.Request) {
	id, ok := s.prepare(w, r)
	if !ok {
		return
	}
	if !s.acquire(w) {
		return
	}
	defer s.releaseSlot()

	expCtx, cancel := context.WithCancel(r.Context())
	defer cancel()
	go func() {
		select {
		case <-s.view.watch(id):
			cancel()
		case <-expCtx.Done():
		}
	}()
	body, err := s.gzipExport(expCtx, id)
	if body != nil {
		defer os.Remove(body.Name())
		defer body.Close()
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			if r.Context().Err() != nil {
				return
			}
			if present, ready := s.view.present(id); ready && !present {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			return
		}
		s.plane.log.Warn().Err(err).Str("session", id).Msg("log export")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if present, ready := s.view.present(id); ready && !present {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	fi, err := body.Stat()
	if err != nil {
		s.plane.log.Warn().Err(err).Str("session", id).Msg("log export")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Length", strconv.FormatInt(fi.Size(), 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, body)
}

func (s *logServer) gzipExport(ctx context.Context, id string) (*os.File, error) {
	rel := s.hold(id)
	defer rel()
	stdout, err := logread.Export(ctx, s.path(id, "stdout"), s.lim)
	if err != nil {
		return nil, err
	}
	defer stdout.Close()
	stderr, err := logread.Export(ctx, s.path(id, "stderr"), s.lim)
	if err != nil {
		return nil, err
	}
	defer stderr.Close()

	tmp, err := os.CreateTemp("", "mftiko-log-")
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			name := tmp.Name()
			tmp.Close()
			os.Remove(name)
		}
	}()
	gz := gzip.NewWriter(tmp)
	tw := tar.NewWriter(gz)
	outSize, outSum, err := writeSnap(tw, "stdout.log", stdout)
	if err != nil {
		return nil, err
	}
	errSize, errSum, err := writeSnap(tw, "stderr.log", stderr)
	if err != nil {
		return nil, err
	}
	manifest, err := json.Marshal(logManifest{
		Stdout: logStreamMeta{Size: outSize, SHA256: outSum},
		Stderr: logStreamMeta{Size: errSize, SHA256: errSum},
	})
	if err != nil {
		return nil, err
	}
	hdr := &tar.Header{
		Name: "manifest.json", Mode: 0o644, Size: int64(len(manifest)),
		ModTime: time.Unix(0, 0).UTC(), Typeflag: tar.TypeReg,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return nil, err
	}
	if _, err := tw.Write(manifest); err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	ok = true
	return tmp, nil
}

func writeSnap(tw *tar.Writer, name string, snap *logread.Snapshot) (int64, string, error) {
	sum := sha256.New()
	hdr := &tar.Header{
		Name: name, Mode: 0o644, Size: snap.Size(),
		ModTime: time.Unix(0, 0).UTC(), Typeflag: tar.TypeReg,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return 0, "", err
	}
	n, err := io.Copy(io.MultiWriter(tw, sum), snap)
	if err != nil {
		return 0, "", err
	}
	if n != snap.Size() {
		return 0, "", fmt.Errorf("sts: log export %s: short read", name)
	}
	return n, hex.EncodeToString(sum.Sum(nil)), nil
}
