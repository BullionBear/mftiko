package artifact

import (
	"debug/elf"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/BullionBear/mftiko/sdk"
)

// rustRecord is what a Rust SDK embeds next to the contracts marker (E09-D099).
const rustRecord = "mftiko.sdk=rust 1.2.0\n"

// rustFixture is a linux/amd64 ELF header followed by the contracts marker and the
// given extra bytes. It has no Go buildinfo.
func rustFixture(extra ...string) []byte {
	buf := elfIdent(elf.ELFCLASS64, elf.ELFDATA2LSB, elf.ELFOSABI_NONE, uint16(elf.EM_X86_64))
	buf = append(buf, marker...)
	for _, s := range extra {
		buf = append(buf, s...)
	}
	return buf
}

func packedManifest(t *testing.T, bin []byte) Manifest {
	t.Helper()
	img, err := PackStrategy(bin, "hello", nil)
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err := json.Unmarshal(img.Config, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestPackRustStrategy(t *testing.T) {
	m := packedManifest(t, rustFixture(rustRecord))
	if m.Runtime != "rust" || m.SDK != (SDK{Lang: "rust", Version: "1.2.0"}) {
		t.Fatalf("runtime %q sdk %+v", m.Runtime, m.SDK)
	}
	if m.OS != "linux" || m.Arch != "amd64" || !slices.Equal(m.Entrypoint, []string{strategyEntrypoint}) ||
		!slices.Equal(m.Contracts, sdk.Contracts()) {
		t.Fatalf("manifest %+v", m)
	}

	// No record and no buildinfo: refused as before.
	if _, err := PackStrategy(rustFixture(), "hello", nil); !errors.Is(err, ErrNoSDKVersion) {
		t.Fatalf("no record: %v", err)
	}
	// The record is still required to sit beside a contracts marker.
	bare := elfIdent(elf.ELFCLASS64, elf.ELFDATA2LSB, elf.ELFOSABI_NONE, uint16(elf.EM_X86_64))
	if _, err := PackStrategy(append(bare, rustRecord...), "hello", nil); !errors.Is(err, ErrNoContracts) {
		t.Fatalf("no contracts: %v", err)
	}
}

// A Rust binary that also carries Go buildinfo (mftik-rt before it drops it) packs as Rust.
func TestPackRecordWinsOverBuildinfo(t *testing.T) {
	row := elfArches[0]
	bin := append(fakeStrategyELF(t, row.class, row.data, row.machine), rustRecord...)
	m := packedManifest(t, bin)
	if m.Runtime != "rust" || m.SDK != (SDK{Lang: "rust", Version: "1.2.0"}) {
		t.Fatalf("runtime %q sdk %+v", m.Runtime, m.SDK)
	}
}

// The Go path writes the same config as before E09-D099.
func TestPackGoConfigGolden(t *testing.T) {
	row := elfArches[0]
	img, err := PackStrategy(fakeStrategyELF(t, row.class, row.data, row.machine), "hello", nil)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"schema":"mftiko.worker.v1","kind":"strategy","name":"hello","runtime":"go","os":"linux","arch":"amd64",` +
		`"entrypoint":["bin/strategy"],"env":{},"sdk":{"lang":"go","version":"v0.0.0"},` +
		`"contracts":["worker.v1","stsctl.v1","md.v1","td.v1"]}`
	if string(img.Config) != want {
		t.Fatalf("config\n got %s\nwant %s", img.Config, want)
	}
}

func TestEmbeddedSDK(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []string
		want SDK
		ok   bool
		err  error
	}{
		{name: "none"},
		{name: "one", in: []string{rustRecord}, want: SDK{"rust", "1.2.0"}, ok: true},
		{name: "repeated", in: []string{rustRecord, "xx", rustRecord}, want: SDK{"rust", "1.2.0"}, ok: true},
		{name: "v prefix and pre-release", in: []string{"mftiko.sdk=zig v0.1.0-rc.1+abc\n"}, want: SDK{"zig", "v0.1.0-rc.1+abc"}, ok: true},
		{name: "conflict", in: []string{rustRecord, "mftiko.sdk=rust 1.3.0\n"}, err: ErrSDKConflict},
		{name: "noise skipped", in: []string{"mftiko.sdk=%s\n", rustRecord}, want: SDK{"rust", "1.2.0"}, ok: true},
		{name: "no newline", in: []string{"mftiko.sdk=rust 1.2.0"}},
		{name: "no space", in: []string{"mftiko.sdk=rust\n"}},
		{name: "two spaces", in: []string{"mftiko.sdk=rust  1.2.0\n"}},
		{name: "upper lang", in: []string{"mftiko.sdk=Rust 1.2.0\n"}},
		{name: "lang digit first", in: []string{"mftiko.sdk=9rust 1.2.0\n"}},
		{name: "version letter first", in: []string{"mftiko.sdk=rust x1\n"}},
		{name: "version slash", in: []string{"mftiko.sdk=rust 1.2/0\n"}},
		{name: "too long", in: []string{"mftiko.sdk=rust 1" + string(make([]byte, maxSDKVersion)) + "\n"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var bin []byte
			for _, s := range tc.in {
				bin = append(bin, s...)
			}
			got, ok, err := embeddedSDK(bin)
			if !errors.Is(err, tc.err) || ok != tc.ok || got != tc.want {
				t.Fatalf("got %+v %v %v, want %+v %v %v", got, ok, err, tc.want, tc.ok, tc.err)
			}
		})
	}
}
