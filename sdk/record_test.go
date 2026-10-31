package sdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"testing"

	"google.golang.org/protobuf/proto"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
)

func TestPseudoCommit(t *testing.T) {
	for _, tc := range []struct{ v, want string }{
		{"v0.0.0-20261031090000-0a1b2c3d4e5f", "0a1b2c3d4e5f"},
		{"v1.2.4-0.20261031090000-0a1b2c3d4e5f", "0a1b2c3d4e5f"},
		{"v1.2.3-pre.0.20261031090000-0a1b2c3d4e5f", "0a1b2c3d4e5f"},
		{"v2.0.0-20261031090000-0a1b2c3d4e5f+incompatible", "0a1b2c3d4e5f"},
		{"v1.2.3", ""},
		{"v1.2.3-rc1", ""},
		{"(devel)", ""},
		{"", ""},
		{"v0.0.0-20261031090000-0A1B2C3D4E5F", ""},
		{"v0.0.0-2026103109000-0a1b2c3d4e5f", ""},
		{"v0.0.0-20261031090000-0a1b2c3d4e5", ""},
	} {
		if got := pseudoCommit(tc.v); got != tc.want {
			t.Errorf("pseudoCommit(%q) = %q, want %q", tc.v, got, tc.want)
		}
	}
}

func TestSDKRelease(t *testing.T) {
	const pseudo = "v0.0.0-20261031090000-0a1b2c3d4e5f"
	for _, tc := range []struct {
		name         string
		info         *debug.BuildInfo
		version, rev string
	}{
		{"none", nil, "", ""},
		{"this module", &debug.BuildInfo{
			Main:     debug.Module{Path: sdkModule, Version: "(devel)"},
			Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "1111111111111111111111111111111111111111"}},
		}, "", "1111111111111111111111111111111111111111"},
		{"dependency", &debug.BuildInfo{
			Main: debug.Module{Path: "example.com/strategy", Version: "(devel)"},
			Deps: []*debug.Module{{Path: "example.com/other", Version: "v1.0.0"}, {Path: sdkModule, Version: pseudo}},
		}, pseudo, "0a1b2c3d4e5f"},
		{"tagged dependency", &debug.BuildInfo{
			Main: debug.Module{Path: "example.com/strategy"},
			Deps: []*debug.Module{{Path: sdkModule, Version: "v1.2.3"}},
		}, "v1.2.3", ""},
		{"replaced by a fork", &debug.BuildInfo{
			Main: debug.Module{Path: "example.com/strategy"},
			Deps: []*debug.Module{{Path: sdkModule, Version: "v1.2.3", Replace: &debug.Module{Path: "example.com/fork", Version: pseudo}}},
		}, pseudo, "0a1b2c3d4e5f"},
		{"replaced by a directory", &debug.BuildInfo{
			Main: debug.Module{Path: "example.com/strategy"},
			Deps: []*debug.Module{{Path: sdkModule, Version: "v1.2.3", Replace: &debug.Module{Path: "../mftiko"}}},
		}, "", ""},
		{"not linked", &debug.BuildInfo{
			Main: debug.Module{Path: "example.com/strategy"},
			Deps: []*debug.Module{nil, {Path: "example.com/other", Version: "v1.0.0"}},
		}, "", ""},
	} {
		v, rev := sdkRelease(tc.info)
		if v != tc.version || rev != tc.rev {
			t.Errorf("%s: sdkRelease = %q %q, want %q %q", tc.name, v, rev, tc.version, tc.rev)
		}
	}
}

// TestInitFrameLeavesOutCredentials checks the bytes the init frame carries: everything the
// payload had except InitPayload.nats, however many times the field occurs.
func TestInitFrameLeavesOutCredentials(t *testing.T) {
	init := baseInit()
	init.Nats = &stsctlv1.NatsCredentials{UserJwt: "user-jwt", Seed: "user-seed", InboxPrefix: "_INBOX_s1"}
	init.EventLog = &sessionv1.EventLog{Enabled: true, KeepMb: 16}
	payload := must(t, init)
	again := must(t, &stsctlv1.InitPayload{Nats: &stsctlv1.NatsCredentials{Seed: "user-seed-2"}})
	payload = append(payload, again...)

	out := withoutField(payload, natsField)
	for _, secret := range []string{"user-jwt", "user-seed", "_INBOX_s1"} {
		if bytes.Contains(out, []byte(secret)) {
			t.Fatalf("the init frame still has %q", secret)
		}
	}
	var got stsctlv1.InitPayload
	if err := proto.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	want := proto.Clone(init).(*stsctlv1.InitPayload)
	want.Nats = nil
	if !proto.Equal(&got, want) {
		t.Fatalf("init frame payload %v, want %v", &got, want)
	}
}

func TestFieldBytes(t *testing.T) {
	cmd := must(t, &stsctlv1.Command{Cmd: &stsctlv1.Command_Bindings{Bindings: bind(2, 5)}})
	raw, ok := fieldBytes(cmd, bindingsField)
	if !ok {
		t.Fatal("no bindings in the command")
	}
	var b stsctlv1.Bindings
	if err := proto.Unmarshal(raw, &b); err != nil || !proto.Equal(&b, bind(2, 5)) {
		t.Fatalf("bindings %v %v", &b, err)
	}
	fail := must(t, &stsctlv1.Command{Cmd: &stsctlv1.Command_Fail{Fail: &stsctlv1.Fail{Reason: "stop"}}})
	if _, ok := fieldBytes(fail, bindingsField); ok {
		t.Fatal("a Fail command has bindings")
	}
	if _, ok := fieldBytes([]byte{0x0a, 0x05, 0x01}, bindingsField); ok {
		t.Fatal("a cut message has bindings")
	}
}

// TestResolveOutcomes checks that each recorded Resolve outcome comes back from a replay with
// the same message and the same sentinel.
func TestResolveOutcomes(t *testing.T) {
	for _, tc := range []struct {
		err   error
		flags uint16
		is    error
	}{
		{ErrNotInInit, 5, ErrNotInInit},
		{fmt.Errorf("symclient: get BINANCE_SPOT_DOGEUSDT: %w", symbol.ErrNotFound), 6, symbol.ErrNotFound},
		{ErrStopping, 7, ErrStopping},
		{fmt.Errorf("symclient: get: %w", context.DeadlineExceeded), 2, context.DeadlineExceeded},
		{errors.New("symclient: bad ticker"), 4, nil},
	} {
		flags, text := resolveOutcome(tc.err)
		if flags != tc.flags || text != tc.err.Error() {
			t.Fatalf("%v: outcome %d %q", tc.err, flags, text)
		}
		back := resolveErr(flags, []byte(text))
		if back.Error() != tc.err.Error() {
			t.Fatalf("%v: replayed as %q", tc.err, back)
		}
		if tc.is != nil && !errors.Is(back, tc.is) {
			t.Fatalf("%v: replayed error is not %v", tc.err, tc.is)
		}
	}
	if flags, text := resolveOutcome(nil); flags != 0 || text != "" {
		t.Fatalf("nil outcome %d %q", flags, text)
	}
}
