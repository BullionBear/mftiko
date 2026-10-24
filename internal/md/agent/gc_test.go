package agent

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/platform/agent"
)

func TestCommitGCWarns(t *testing.T) {
	var n int
	var buf bytes.Buffer
	p := &plane{
		loaded: true,
		log:    zerolog.New(&buf).Level(zerolog.WarnLevel),
		gc: func() error {
			n++
			return context.DeadlineExceeded
		},
	}
	if err := p.commit(context.Background(), agent.Demand[Entry]{}, nil); err != nil {
		t.Fatal(err)
	}
	if n != 1 || !strings.Contains(buf.String(), "artifact gc") {
		t.Fatalf("calls %d log %s", n, buf.String())
	}
	buf.Reset()
	p.gc = nil
	if err := p.commit(context.Background(), agent.Demand[Entry]{}, nil); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatalf("nil gc logged %s", buf.String())
	}
}
