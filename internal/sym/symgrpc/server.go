package symgrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpchealth"
	"connectrpc.com/grpcreflect"
	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1/symv1connect"
	"github.com/BullionBear/mftiko/internal/sym"
)

const (
	// DefaultShutdownTimeout bounds how long Serve waits for calls in flight once its context ends.
	DefaultShutdownTimeout = 10 * time.Second

	// maxRequestBytes caps one request message. Every SymService request is a few fields.
	maxRequestBytes = 64 << 10
)

// Config is one gRPC server for SymService (E22-D007).
type Config struct {
	// Tokens are required: every SymService and reflection call presents one. Health does not
	// (E22-D014).
	Tokens *Tokens
	// TLS, when set, serves HTTP/2 over TLS. Nil serves h2c, plaintext HTTP/2.
	TLS *tls.Config
	Log zerolog.Logger
	// ShutdownTimeout is how long to wait for calls in flight. Zero selects DefaultShutdownTimeout.
	ShutdownTimeout time.Duration
}

// Server serves SymService, grpc.health.v1 and gRPC reflection on one listener. The wire is
// gRPC; connect also answers the Connect protocol and gRPC-Web, which are for debugging only
// (E22-D005).
type Server struct {
	srv     *http.Server
	health  *grpchealth.StaticChecker
	tls     bool
	timeout time.Duration
	log     zerolog.Logger
}

// New builds the server. Serve gives it a listener.
func New(api sym.API, cfg Config) (*Server, error) {
	if api == nil {
		return nil, errors.New("symgrpc: nil api")
	}
	if cfg.Tokens == nil || len(cfg.Tokens.names) == 0 {
		return nil, errors.New("symgrpc: no tokens")
	}
	timeout := cfg.ShutdownTimeout
	if timeout <= 0 {
		timeout = DefaultShutdownTimeout
	}
	opts := []connect.HandlerOption{
		connect.WithInterceptors(&gate{tokens: cfg.Tokens, log: cfg.Log}),
		connect.WithReadMaxBytes(maxRequestBytes),
	}
	health := grpchealth.NewStaticChecker(symv1connect.SymServiceName)
	reflector := grpcreflect.NewStaticReflector(symv1connect.SymServiceName, grpchealth.HealthV1ServiceName)
	mux := http.NewServeMux()
	mux.Handle(symv1connect.NewSymServiceHandler(&handler{api: api}, opts...))
	mux.Handle(grpcreflect.NewHandlerV1(reflector, opts...))
	mux.Handle(grpcreflect.NewHandlerV1Alpha(reflector, opts...))
	mux.Handle(grpchealth.NewHandler(health, connect.WithReadMaxBytes(maxRequestBytes)))

	var protocols http.Protocols
	protocols.SetHTTP1(true)
	if cfg.TLS != nil {
		protocols.SetHTTP2(true)
	} else {
		protocols.SetUnencryptedHTTP2(true)
	}
	srv := &http.Server{
		Handler:           mux,
		Protocols:         &protocols,
		TLSConfig:         cfg.TLS,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return &Server{srv: srv, health: health, tls: cfg.TLS != nil, timeout: timeout, log: cfg.Log}, nil
}

// Handler is the server's routes, for tests that bring their own transport.
func (s *Server) Handler() http.Handler { return s.srv.Handler }

// Serve serves ln until ctx ends. Then health reports NOT_SERVING, the listener stops taking
// connections, and calls in flight get ShutdownTimeout to finish before the rest are cut
// (E22-D012). It returns nil after a shutdown that ctx asked for.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	done := make(chan error, 1)
	go func() {
		if s.tls {
			done <- s.srv.ServeTLS(ln, "", "")
			return
		}
		done <- s.srv.Serve(ln)
	}()
	s.log.Info().Str("addr", ln.Addr().String()).Bool("tls", s.tls).Msg("serving grpc")
	select {
	case err := <-done:
		return fmt.Errorf("grpc serve: %w", err)
	case <-ctx.Done():
	}
	s.health.SetStatus("", grpchealth.StatusNotServing)
	s.health.SetStatus(symv1connect.SymServiceName, grpchealth.StatusNotServing)
	sctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	if err := s.srv.Shutdown(sctx); err != nil {
		s.log.Warn().Err(err).Msg("grpc shutdown timed out; closing")
		_ = s.srv.Close()
	}
	if err := <-done; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("grpc serve: %w", err)
	}
	return nil
}

// LoadTLS is the server certificate from a PEM certificate chain and key.
func LoadTLS(certFile, keyFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("grpc tls: %w", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}
