package natsxtest

import (
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestServerIsCoreNATS(t *testing.T) {
	s := Start(t)
	if !strings.HasPrefix(s.URL(), "nats://127.0.0.1:") {
		t.Fatalf("url = %s", s.URL())
	}
	nc, err := nats.Connect(s.URL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, _ := nc.ConnectedServerJetStream()
	if js {
		t.Fatal("server advertised jetstream")
	}
}
