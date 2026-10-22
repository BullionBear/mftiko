package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"net"
	"net/http"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/platform/state"
)

// Server is one API process: a mirror per regional cluster and the HTTP listener (D1024–D1029).
type Server struct {
	cfg      Config
	cl       *state.Clusters
	clusters []*cluster
	byName   map[string]*cluster
	http     *http.Server
	ln       net.Listener
	web      *os.Root
	log      zerolog.Logger
	now      func() time.Time

	draining  atomic.Bool
	drainOnce sync.Once
	stopOnce  sync.Once

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	minBackoff time.Duration
	maxBackoff time.Duration

	mu     sync.Mutex
	logged string // last unavailable_regions logged, joined
}

// cluster is one region's mirror of the whole keyspace.
type cluster struct {
	name   string
	mirror atomic.Pointer[state.Mirror]
}

// Run validates cfg, dials every region and serves until ctx is cancelled. Cancellation is the shutdown path:
// readiness turns 503, the listener stops accepting, in-flight requests get shutdown_timeout, and Run returns
// nil so the process can exit 0 (D1027). A failure before the process is serving returns an error.
func Run(ctx context.Context, cfg Config) error {
	s, err := start(cfg)
	if err != nil {
		return err
	}
	return s.serve(ctx)
}

// Addr is the address the listener bound, host:port.
func (s *Server) Addr() string {
	if s == nil || s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

func start(cfg Config) (*Server, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cl, err := state.NewClusters(cfg.Topology, nil, state.AuthDial(state.UserAPI, cfg.EtcdPassword))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		cfg: cfg, cl: cl, byName: map[string]*cluster{}, log: cfg.Log, now: cfg.Now,
		ctx: ctx, cancel: cancel,
	}
	if s.now == nil {
		s.now = time.Now
	}
	s.minBackoff = cfg.Mirror.MinBackoff
	if s.minBackoff == 0 {
		s.minBackoff = state.DefaultMirrorMinBackoff
	}
	s.maxBackoff = cfg.Mirror.MaxBackoff
	if s.maxBackoff == 0 {
		s.maxBackoff = state.DefaultMirrorMaxBackoff
	}
	if s.maxBackoff < s.minBackoff {
		s.maxBackoff = s.minBackoff
	}
	for _, name := range slices.Sorted(maps.Keys(cfg.Topology.Regions)) {
		c := &cluster{name: name}
		s.clusters = append(s.clusters, c)
		s.byName[name] = c
	}
	if cfg.WebDir != "" {
		root, err := os.OpenRoot(cfg.WebDir)
		if err != nil {
			s.stop()
			return nil, fmtWeb(err)
		}
		s.web = root
	}
	for _, c := range s.clusters {
		s.wg.Add(1)
		go s.runMirror(c)
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		s.stop()
		return nil, fmt.Errorf("listen %s: %w", cfg.Listen, err)
	}
	s.ln = ln
	s.http = &http.Server{
		Handler:  s.routes(),
		ErrorLog: log.New(logWriter{s.log}, "", 0),
	}
	s.log.Info().Str("addr", ln.Addr().String()).Bool("tls", cfg.TLSCert != "").Str("web", cfg.WebDir).
		Strs("regions", regionNames(s.clusters)).Msg("listen")
	return s, nil
}

func fmtWeb(err error) error {
	return errors.New("-web: " + err.Error())
}

func regionNames(cs []*cluster) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.name
	}
	return out
}

func (s *Server) serve(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		var err error
		if s.cfg.TLSCert != "" {
			err = s.http.ServeTLS(s.ln, s.cfg.TLSCert, s.cfg.TLSKey)
		} else {
			err = s.http.Serve(s.ln)
		}
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()
	select {
	case err := <-errCh:
		s.stop()
		return err
	case <-ctx.Done():
		s.drain()
		s.stop()
		<-errCh
		return nil
	}
}

// drain is D1027: readiness goes 503 first, then the listener closes and in-flight requests have
// ShutdownTimeout to finish. Past that, Close drops the connections and drain returns.
func (s *Server) drain() {
	s.drainOnce.Do(func() {
		s.draining.Store(true)
		if s.http == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
		defer cancel()
		if err := s.http.Shutdown(ctx); err != nil {
			s.log.Warn().Err(err).Msg("shutdown")
			s.http.Close()
		}
	})
}

func (s *Server) stop() {
	s.stopOnce.Do(func() {
		s.cancel()
		if s.cl != nil {
			s.cl.Close()
		}
		s.wg.Wait()
		if s.web != nil {
			s.web.Close()
		}
	})
}

// routes is one handler on purpose. ServeMux rejects a GET wildcard registered over the
// all-method /api/ pattern, so the split is done here: /healthz and /readyz are exact,
// /api and /api/ are never files, and anything else is the console build (D1028).
func (s *Server) routes() http.Handler { return http.HandlerFunc(s.serveHTTP) }

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	switch path.Clean("/" + r.URL.Path) {
	case "/healthz":
		if !readMethod(r) {
			methodNotAllowed(w)
			return
		}
		s.healthz(w, r)
		return
	case "/readyz":
		if !readMethod(r) {
			methodNotAllowed(w)
			return
		}
		s.readyz(w, r)
		return
	}
	if apiPath(r.URL.Path) || s.web == nil {
		http.NotFound(w, r)
		return
	}
	if !readMethod(r) {
		methodNotAllowed(w)
		return
	}
	staticHandler{s.web}.ServeHTTP(w, r)
}

func readMethod(r *http.Request) bool {
	return r.Method == http.MethodGet || r.Method == http.MethodHead
}

func methodNotAllowed(w http.ResponseWriter) {
	w.Header().Set("Allow", "GET, HEAD")
	http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) readyz(w http.ResponseWriter, _ *http.Request) {
	body, code := s.readiness(s.now())
	s.note(body.UnavailableRegions)
	writeJSON(w, code, body)
}

func (s *Server) readiness(now time.Time) (readyBody, int) {
	names := make([]string, len(s.clusters))
	snaps := make([]regionSnap, len(s.clusters))
	for i, c := range s.clusters {
		names[i] = c.name
		view, contact, synced := c.observe()
		snaps[i] = regionSnap{name: c.name, synced: synced, contact: contact, rev: view.Rev}
	}
	return readyStatus(names, snaps, now, s.cfg.UnavailableAfter, s.draining.Load())
}

func (s *Server) note(unavail []string) {
	got := strings.Join(unavail, ",")
	s.mu.Lock()
	defer s.mu.Unlock()
	if got == s.logged {
		return
	}
	s.logged = got
	ev := s.log.Info()
	if len(unavail) > 0 {
		ev = s.log.Warn()
	}
	ev.Strs("unavailable_regions", unavail).Msg("regions")
}

func (c *cluster) observe() (state.View, time.Time, bool) {
	m := c.mirror.Load()
	if m == nil {
		return state.View{}, time.Time{}, false
	}
	synced := false
	select {
	case <-m.Synced():
		synced = true
	default:
	}
	return m.Snapshot(), m.LastContact(), synced
}

// snapshot is the region's copy, and whether the first list has finished. Tests use it.
func (s *Server) snapshot(region string) (state.View, bool) {
	c := s.byName[region]
	if c == nil {
		return state.View{}, false
	}
	view, _, synced := c.observe()
	return view, synced
}

func (s *Server) runMirror(c *cluster) {
	defer s.wg.Done()
	bo := s.minBackoff
	for {
		if s.stopped() {
			return
		}
		r, err := s.dial(c.name)
		if s.stopped() {
			return
		}
		if err != nil {
			s.log.Warn().Err(err).Str("region", c.name).Msg("mirror dial")
			if !s.sleep(bo) {
				return
			}
			bo = min(bo*2, s.maxBackoff)
			continue
		}
		opt := s.cfg.Mirror
		l := s.log.With().Str("region", c.name).Logger()
		opt.Log = &l
		m, err := state.NewMirror(r, keyPrefix, opt)
		if err != nil {
			s.log.Error().Err(err).Str("region", c.name).Msg("mirror")
			return
		}
		c.mirror.Store(m)
		err = m.Run(s.ctx)
		if s.stopped() {
			return
		}
		s.log.Warn().Err(err).Str("region", c.name).Msg("mirror stopped")
		if !s.sleep(bo) {
			return
		}
		bo = min(bo*2, s.maxBackoff)
	}
}

// dial waits for the region's client, or for shutdown. Reader's own timeout does not
// follow this process's context; waiting for it on the way out would hold SIGTERM past
// the drain limit (D1027). The dial keeps running until it finishes, and Clusters.Close
// drops a client that arrives after shutdown.
func (s *Server) dial(region string) (state.Reader, error) {
	type result struct {
		r   state.Reader
		err error
	}
	ch := make(chan result, 1)
	go func() {
		r, err := s.cl.Reader(region)
		ch <- result{r, err}
	}()
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case res := <-ch:
		return res.r, res.err
	}
}

func (s *Server) stopped() bool {
	select {
	case <-s.ctx.Done():
		return true
	default:
		return false
	}
}

func (s *Server) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-s.ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(append(b, '\n'))
}

// logWriter sends the net/http server's error log through zerolog.
type logWriter struct{ l zerolog.Logger }

func (w logWriter) Write(p []byte) (int, error) {
	if msg := strings.TrimSpace(string(p)); msg != "" {
		w.l.Error().Msg(msg)
	}
	return len(p), nil
}
