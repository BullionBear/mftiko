package api

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
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
	api      http.Handler
	apiOnce  sync.Once
	audit    *auditor
	apiWG    sync.WaitGroup
	log      zerolog.Logger
	now      func() time.Time
	ids      func() io.Reader // the random source of one id allocation: crypto/rand; tests replace it

	draining  atomic.Bool
	drainOnce sync.Once
	stopOnce  sync.Once

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	minBackoff time.Duration
	maxBackoff time.Duration

	catalog         tagCatalog
	strategyOnce    sync.Once
	strategyHandler http.Handler

	// hook is nil in production. Tests fail a create at "before-etcd" or "after-etcd".
	hook func(step string) error
	// haltBeforeDelete is a test switch. The retention pass closes the row and returns
	// before deleteIf, so a later pass can finish the etcd delete (D1031). It is zero
	// until a test sets it, and the reconcile goroutine only reads it with atomics.
	haltBeforeDelete atomic.Bool
	// missingRecord counts etcd specs with no session_records row (D1072).
	// W5 also counts a terminal session that has no row and is therefore not deleted (D1081).
	missingRecord atomic.Int64
	// hashMismatch counts specs whose immutable content does not match the row (D1071).
	hashMismatch  atomic.Int64
	reconcileKick chan struct{}
	reconcileMu   sync.Mutex
	// reconcilePasses counts completed reconcileOnce calls so a test can wait for one pass.
	reconcilePasses atomic.Uint64

	mu     sync.Mutex
	logged string // last unavailable_regions logged, joined

	// agents remembers the last AgentRecord seen while its region was available (D1096).
	agents agentMemory
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
		ids: defaultIDs,
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
	aud, err := openAudit(cfg.PostgresDSN, s.log)
	if err != nil {
		s.stop()
		return nil, err
	}
	s.audit = aud
	if err := s.useCatalog(); err != nil {
		s.stop()
		return nil, err
	}
	// watchMirror calls kickReconcile as soon as a mirror notifies, and that reads this field.
	// The channel has to exist before those goroutines start (D1070).
	s.reconcileKick = make(chan struct{}, 1)
	for _, c := range s.clusters {
		s.wg.Add(1)
		go s.runMirror(c)
	}
	s.wg.Add(1)
	go s.runReconcile()
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

// defaultIDs is the id source of production: crypto/rand.
func defaultIDs() io.Reader { return rand.Reader }

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
// ShutdownTimeout to finish. The audit queue is written inside that same window (D1049). Past the
// limit, Close drops the connections and whatever is still queued is abandoned.
func (s *Server) drain() {
	s.drainOnce.Do(func() {
		s.draining.Store(true)
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
		defer cancel()
		if s.http != nil {
			if err := s.http.Shutdown(ctx); err != nil {
				s.log.Warn().Err(err).Msg("shutdown")
				s.http.Close()
			}
		}
		s.waitAPI(ctx)
		if s.audit != nil {
			s.audit.shutdown(ctx)
		}
	})
}

func (s *Server) stop() {
	s.stopOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		if s.audit != nil {
			ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
			s.audit.shutdown(ctx)
			cancel()
		}
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
// /api (including the strategy catalog) is authenticated and audited, and anything else
// is the console build (D1028, D1056).
func (s *Server) routes() http.Handler {
	s.apiOnce.Do(func() {
		s.api = apiv1.HandlerWithOptions(s, apiv1.StdHTTPServerOptions{
			BaseURL:          "/api/v1",
			ErrorHandlerFunc: s.openAPIError,
		})
	})
	return http.HandlerFunc(s.serveHTTP)
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	// Every response carries an id the server chose. A client-supplied X-Request-Id is ignored (D1053).
	id := newRequestID()
	w.Header().Set(headerRequestID, id)
	st := &accessState{id: id, start: s.clock()}
	r = r.WithContext(context.WithValue(r.Context(), accessKey{}, st))

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
	// Only /api is authenticated and audited. Health, readiness, and static files are not (D1010).
	// Strategy routes are /api too; serveAPI sends them to the catalog mux after authentication.
	if apiPath(r.URL.Path) {
		s.serveAudited(w, r, st)
		return
	}
	if s.web == nil {
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
		watchCtx, watchCancel := context.WithCancel(s.ctx)
		s.wg.Add(1)
		go s.watchMirror(watchCtx, m)
		err = m.Run(s.ctx)
		watchCancel()
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
// dialRequest is dial that also returns when ctx is canceled, so a client disconnect
// does not leave an SSE handler blocked in Clusters.Reader (D1027, D1094).
func (s *Server) dialRequest(ctx context.Context, region string) (state.Reader, error) {
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
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		return res.r, res.err
	}
}

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
