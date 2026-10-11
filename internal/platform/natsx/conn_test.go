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
		{name: "inbox prefix", url: "nats://127.0.0.1:1", opt: natsx.Options{InboxPrefix: "_INBOX_md_dev.>"}, want: "invalid custom prefix"},
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

func TestOnReconnect(t *testing.T) {
	s := natsxtest.Start(t)
	c := dial(t, s.URL(), natsx.Options{})
	got := make(chan struct{}, 1)
	c.OnReconnect(func() {
		select {
		case got <- struct{}{}:
		default:
		}
	})
	c.OnReconnect(nil)
	if err := s.Restart(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-got:
	case <-time.After(10 * time.Second):
		t.Fatal("reconnect callback did not run")
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

func TestInboxPrefix(t *testing.T) {
	s := natsxtest.Start(t)
	srv := dial(t, s.URL(), natsx.Options{})
	replies := make(chan string, 1)
	if _, err := srv.Subscribe(t.Context(), "echo", "", func(m natsx.Msg) {
		data := append([]byte(nil), m.Data...)
		select {
		case replies <- m.Reply:
		default:
		}
		_ = srv.Publish(m.Reply, data)
	}); err != nil {
		t.Fatal(err)
	}
	c := dial(t, s.URL(), natsx.Options{InboxPrefix: "_INBOX_md_dev"})
	var out wrapperspb.StringValue
	if err := c.Request(t.Context(), "echo", &wrapperspb.StringValue{Value: "hi"}, &out); err != nil {
		t.Fatal(err)
	}
	if out.GetValue() != "hi" {
		t.Fatalf("value %q", out.GetValue())
	}
	reply := await(t, replies)
	if !strings.HasPrefix(reply, "_INBOX_md_dev.") {
		t.Fatalf("reply subject %q", reply)
	}
}
