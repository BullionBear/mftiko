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
// Halt leaves the port free so a later Restart can listen there again.
type Server struct {
	url  string
	host string
	port int
	ns   *server.Server

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
	addr := ns.Addr()
	if addr == nil {
		ns.Shutdown()
		ns.WaitForShutdown()
		return nil, errors.New("no address")
	}
	gotHost, portStr, err := net.SplitHostPort(addr.String())
	if err != nil {
		ns.Shutdown()
		ns.WaitForShutdown()
		return nil, err
	}
	gotPort, err := strconv.Atoi(portStr)
	if err != nil {
		ns.Shutdown()
		ns.WaitForShutdown()
		return nil, err
	}
	return &Server{url: ns.ClientURL(), host: gotHost, port: gotPort, ns: ns}, nil
}

// URL is the client URL, nats://127.0.0.1:<port>.
func (s *Server) URL() string { return s.url }

// Restart stops the server and starts another on the same address. A client keeps the URL it
// already dialed and reconnects to it. Halt leaves the server down; Restart after Halt uses the
// address captured at start, because a stopped server has no Addr. Shutdown still stops whichever
// server is running.
func (s *Server) Restart() error {
	if err := s.down(); err != nil {
		return err
	}
	return s.listenAgain()
}

// Halt stops the server and leaves it down. Restart brings the same address back.
func (s *Server) Halt() error {
	return s.down()
}

func (s *Server) down() error {
	if s == nil || s.port == 0 {
		return errors.New("natsxtest: no server")
	}
	s.stopNS()
	return nil
}

func (s *Server) listenAgain() error {
	var errs []error
	for range startTries {
		next, err := startAt(s.host, s.port)
		if err == nil {
			s.ns = next.ns
			s.url = next.url
			s.host = next.host
			s.port = next.port
			return nil
		}
		errs = append(errs, err)
	}
	return fmt.Errorf("natsxtest: restart: %w", errors.Join(errs...))
}

func (s *Server) stopNS() {
	if s == nil || s.ns == nil {
		return
	}
	ns := s.ns
	s.ns = nil
	ns.Shutdown()
	ns.WaitForShutdown()
}

// Shutdown stops the server and waits until it has exited.
func (s *Server) Shutdown() {
	if s == nil {
		return
	}
	s.once.Do(func() { s.stopNS() })
}
