// Package natsxtest runs a throwaway core-NATS server in-process. JetStream is off and there is
// no authentication. The server listens on 127.0.0.1 and a free port, and stops when the test ends.
//
// Tests use this instead of the dev compose service. Nothing has to be installed: the server is
// the nats-server module, the same release deploy/dev/compose.yaml runs.
package natsxtest

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

const (
	readyTimeout = 10 * time.Second
	startTries   = 3
)

// Server is one core-NATS process. Shutdown is idempotent; the test cleanup calls it.
type Server struct {
	url string
	ns  *server.Server

	once sync.Once
}

// Start launches a server and waits until it accepts clients. A server that does not come up
// fails the test.
func Start(t testing.TB) *Server {
	t.Helper()
	var errs []error
	for range startTries {
		s, err := start()
		if err == nil {
			t.Cleanup(s.Shutdown)
			return s
		}
		errs = append(errs, err)
	}
	t.Fatalf("natsxtest: nats-server did not start: %v", errors.Join(errs...))
	return nil
}

func start() (*Server, error) {
	opts := &server.Options{
		Host:       "127.0.0.1",
		Port:       -1, // a free port
		NoLog:      true,
		NoSigs:     true,
		JetStream:  false,
		ServerName: "natsxtest",
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		return nil, err
	}
	ns.Start()
	if !ns.ReadyForConnections(readyTimeout) {
		ns.Shutdown()
		ns.WaitForShutdown()
		return nil, fmt.Errorf("not accepting connections after %s", readyTimeout)
	}
	if ns.JetStreamEnabled() {
		ns.Shutdown()
		ns.WaitForShutdown()
		return nil, errors.New("jetstream is enabled")
	}
	return &Server{url: ns.ClientURL(), ns: ns}, nil
}

// URL is the client URL, nats://127.0.0.1:<port>.
func (s *Server) URL() string { return s.url }

// Shutdown stops the server and waits until it has exited.
func (s *Server) Shutdown() {
	if s == nil || s.ns == nil {
		return
	}
	s.once.Do(func() {
		s.ns.Shutdown()
		s.ns.WaitForShutdown()
	})
}
