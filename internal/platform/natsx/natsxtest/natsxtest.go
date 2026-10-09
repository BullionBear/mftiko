// Package natsxtest runs a throwaway core-NATS server in-process. JetStream is off and there is
// no authentication. The server listens on 127.0.0.1 and a free port, and stops when the test ends.
//
// Tests use this instead of the dev compose service. Nothing has to be installed: the server is
// the nats-server module, the same release deploy/dev/compose.yaml runs.
package natsxtest

import (
	"errors"
	"fmt"
	"net"
	"strconv"
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
	return startAt("127.0.0.1", -1)
}

func startAt(host string, port int) (*Server, error) {
	opts := &server.Options{
		Host:       host,
		Port:       port, // -1 asks the OS for a free port
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

// Restart stops the server and starts another on the same address. A client keeps the URL it
// already dialed and reconnects to it. Shutdown still stops whichever server is running.
func (s *Server) Restart() error {
	if s == nil || s.ns == nil {
		return errors.New("natsxtest: no server")
	}
	addr := s.ns.Addr()
	if addr == nil {
		return errors.New("natsxtest: server has no address")
	}
	host, portStr, err := net.SplitHostPort(addr.String())
	if err != nil {
		return fmt.Errorf("natsxtest: address: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("natsxtest: port: %w", err)
	}
	s.ns.Shutdown()
	s.ns.WaitForShutdown()

	var errs []error
	for range startTries {
		next, err := startAt(host, port)
		if err == nil {
			s.ns = next.ns
			s.url = next.url
			return nil
		}
		errs = append(errs, err)
	}
	s.ns = nil
	return fmt.Errorf("natsxtest: restart: %w", errors.Join(errs...))
}

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
