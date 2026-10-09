package natsx_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/internal/platform/natsx"
	"github.com/BullionBear/mftiko/internal/platform/natsx/natsxtest"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestConnectRejectsBadOptions(t *testing.T) {
	cases := []struct {
		name string
		url  string
		opt  natsx.Options
		want string
	}{
		{name: "empty url", want: "empty url"},
		{name: "negative request timeout", url: "nats://127.0.0.1:1", opt: natsx.Options{RequestTimeout: -1}, want: "negative"},
		{name: "negative connect timeout", url: "nats://127.0.0.1:1", opt: natsx.Options{ConnectTimeout: -1}, want: "negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := natsx.Connect(tc.url, tc.opt)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Connect error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// logCapture records slog messages. Handle does not block: the client's callback dispatcher
// must be able to return while the test is still reading.
type logCapture struct {
	ch chan string
}

func (h *logCapture) Enabled(context.Context, slog.Level) bool { return true }
func (h *logCapture) Handle(_ context.Context, r slog.Record) error {
	select {
	case h.ch <- r.Message:
	default:
	}
	return nil
}
func (h *logCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *logCapture) WithGroup(string) slog.Handler      { return h }

func TestRestartLogsDisconnectThenReconnect(t *testing.T) {
	s := natsxtest.Start(t)
	logs := &logCapture{ch: make(chan string, 8)}
	_ = dial(t, s.URL(), natsx.Options{Logger: slog.New(logs)})
	before := s.URL()
	if err := s.Restart(); err != nil {
		t.Fatal(err)
	}
	if s.URL() != before {
		t.Fatalf("url changed from %s to %s", before, s.URL())
	}
	// The client waits out its reconnect delay, so this timer is only the failure path.
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	var sawDisconnect bool
	for {
		select {
		case msg := <-logs.ch:
			switch msg {
			case "nats disconnected":
				sawDisconnect = true
			case "nats reconnected":
				if !sawDisconnect {
					t.Fatal("reconnected before disconnected")
				}
				return
			case "nats closed":
				t.Fatal("connection closed instead of reconnecting")
			}
		case <-deadline.C:
			t.Fatalf("reconnect log missing (disconnected=%v)", sawDisconnect)
		}
	}
}

func TestPublishAfterClose(t *testing.T) {
	_, c := start(t)
	c.Close()
	err := c.Publish("x", []byte("y"))
	if err == nil || !errorIs(err, natsx.ErrClosed) {
		t.Fatalf("publish after close: %v", err)
	}
}

func TestRequestCancel(t *testing.T) {
	_, c := start(t)
	got := make(chan struct{})
	release := make(chan struct{})
	if _, err := c.Subscribe(t.Context(), "block", "", func(m natsx.Msg) {
		close(got)
		<-release
		_ = c.Publish(m.Reply, nil)
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() { close(release) })
	errc := make(chan error, 1)
	go func() {
		errc <- c.Request(ctx, "block", &wrapperspb.StringValue{}, &wrapperspb.StringValue{})
	}()
	await(t, got)
	cancel()
	if err := await(t, errc); !errorIs(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
}
