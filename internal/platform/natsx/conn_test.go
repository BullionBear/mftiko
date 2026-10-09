package natsx_test

import (
	"context"
	"strings"
	"testing"

	"github.com/BullionBear/mftiko/internal/platform/natsx"
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
		{name: "jwt only", url: "nats://127.0.0.1:1", opt: natsx.Options{UserJWT: "x"}, want: "both be set"},
		{name: "seed only", url: "nats://127.0.0.1:1", opt: natsx.Options{NKeySeed: "x"}, want: "both be set"},
		{
			name: "creds and jwt",
			url:  "nats://127.0.0.1:1",
			opt:  natsx.Options{CredsFile: "x.creds", UserJWT: "j", NKeySeed: "s"},
			want: "mutually exclusive",
		},
		{name: "negative timeout", url: "nats://127.0.0.1:1", opt: natsx.Options{RequestTimeout: -1}, want: "negative"},
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

func TestConnectRejectsBadInboxPrefix(t *testing.T) {
	s, _ := start(t)
	for _, prefix := range []string{"bad.", "bad*", "bad>"} {
		_, err := natsx.Connect(s.URL(), natsx.Options{InboxPrefix: prefix})
		if err == nil || !strings.Contains(err.Error(), "prefix") {
			t.Fatalf("prefix %q: %v", prefix, err)
		}
	}
}

func TestInboxPrefixIsOnTheReply(t *testing.T) {
	s, _ := start(t)
	const prefix = "_INBOX_sess"
	client := dial(t, s.URL(), natsx.Options{InboxPrefix: prefix})
	replies := make(chan string, 1)
	if _, err := client.Subscribe(t.Context(), "svc", "", func(m natsx.Msg) {
		replies <- m.Reply
		_ = client.Publish(m.Reply, nil)
	}); err != nil {
		t.Fatal(err)
	}
	// A second connection, so the reply subject is the requester's, not the server's.
	asker := dial(t, s.URL(), natsx.Options{InboxPrefix: prefix})
	if err := asker.Request(t.Context(), "svc", &wrapperspb.StringValue{}, &wrapperspb.StringValue{}); err != nil {
		t.Fatal(err)
	}
	got := await(t, replies)
	if !strings.HasPrefix(got, prefix+".") {
		t.Fatalf("reply subject %q, want prefix %s.", got, prefix)
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
