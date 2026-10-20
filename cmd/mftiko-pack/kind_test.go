package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"debug/elf"

	"github.com/BullionBear/mftiko/internal/md/conn"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/td/account"
)

// D989 names the contracts the workers actually speak. conn.Contract and account.Contract are
// those C5 majors; a change there has to be decided before pack's fixed list moves.
func TestWorkerContractsMatchImplemented(t *testing.T) {
	if conn.Contract != "mdctl.v1" {
		t.Fatalf("conn.Contract = %q; D989 lists mdctl.v1", conn.Contract)
	}
	if account.Contract != "tdctl.v1" {
		t.Fatalf("account.Contract = %q; D989 lists tdctl.v1", account.Contract)
	}
	md, ok := artifact.WorkerContracts(artifact.KindMDConn)
	if !ok || !slices.Equal(md, []string{"worker.v1", "mdctl.v1"}) || md[1] != conn.Contract {
		t.Fatalf("md.conn contracts %v", md)
	}
	td, ok := artifact.WorkerContracts(artifact.KindTDAccount)
	if !ok || !slices.Equal(td, []string{"worker.v1", "tdctl.v1"}) || td[1] != account.Contract {
		t.Fatalf("td.account contracts %v", td)
	}
}

func TestPackWorkerKind(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	bin := linuxAmd64ELF(t)
	raw, err := os.ReadFile(bin)
	if err != nil {
		t.Fatal(err)
	}
	reg := newCLIRegistry()
	defer reg.Close()
	for _, tc := range []struct {
		kind string
		name string
	}{
		{artifact.KindMDConn, "mftiko-md-conn"},
		{artifact.KindTDAccount, "mftiko-td-account"},
	} {
		var first, second bytes.Buffer
		base := []string{"-bin", bin, "-kind", tc.kind, "-name", tc.name, "-repo", reg.repo(), "-plain-http"}
		if err := run(append(append([]string{}, base...), "-tag", "ci"), &first); err != nil {
			t.Fatal(err)
		}
		if err := run(append(append([]string{}, base...), "-tag", "latest"), &second); err != nil {
			t.Fatal(err)
		}
		d1 := strings.TrimSpace(first.String())
		d2 := strings.TrimSpace(second.String())
		if d1 != d2 || !strings.HasPrefix(d1, "sha256:") {
			t.Fatalf("%s stdout %q then %q", tc.kind, first.String(), second.String())
		}
		img, err := artifact.PackWorker(raw, tc.kind, tc.name, nil)
		if err != nil {
			t.Fatal(err)
		}
		if img.Digest != d1 {
			t.Fatalf("%s cli digest %s, pack %s", tc.kind, d1, img.Digest)
		}
		var m artifact.Manifest
		if err := json.Unmarshal(img.Config, &m); err != nil {
			t.Fatal(err)
		}
		want, ok := artifact.WorkerContracts(tc.kind)
		if !ok || !slices.Equal(m.Contracts, want) || m.SDK != (artifact.SDK{}) || !slices.Equal(m.Entrypoint, []string{"bin/worker"}) || m.Kind != tc.kind {
			t.Fatalf("%s manifest %+v", tc.kind, m)
		}
		for _, ref := range []string{d1, "latest"} {
			body := cliFetch(t, reg.repo(), ref)
			if !bytes.Equal(body, img.Manifest) {
				t.Fatalf("%s pull %s is not the manifest we packed", tc.kind, ref)
			}
		}
	}
}

func TestPackKindRejectsBeforePush(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	reg := newCLIRegistry()
	defer reg.Close()
	bin := linuxAmd64ELF(t)
	for _, kind := range []string{"", "session", "MD.CONN"} {
		err := run([]string{"-bin", bin, "-kind", kind, "-name", "n", "-repo", reg.repo(), "-plain-http"}, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "kind") {
			t.Fatalf("kind %q: %v", kind, err)
		}
	}
	err := run([]string{"-bin", bin, "-kind", artifact.KindMDConn, "-name", "n", "-repo", reg.repo(), "-plain-http", "-env", "MFTIKO_X=1"}, io.Discard)
	if !errors.Is(err, artifact.ErrBadManifest) {
		t.Fatalf("platform env: %v", err)
	}
	if reg.hits() != 0 {
		t.Fatalf("refused pack still contacted the registry, %d requests", reg.hits())
	}
}

// linuxAmd64ELF is a header-only ELF that elfPlatform accepts as linux/amd64.
func linuxAmd64ELF(t *testing.T) string {
	t.Helper()
	buf := make([]byte, 64)
	buf[0], buf[1], buf[2], buf[3] = 0x7f, 'E', 'L', 'F'
	buf[4] = byte(elf.ELFCLASS64)
	buf[5] = byte(elf.ELFDATA2LSB)
	buf[6] = 1
	buf[7] = byte(elf.ELFOSABI_NONE)
	binary.LittleEndian.PutUint16(buf[16:], uint16(elf.ET_EXEC))
	binary.LittleEndian.PutUint16(buf[18:], uint16(elf.EM_X86_64))
	binary.LittleEndian.PutUint32(buf[20:], 1)
	path := filepath.Join(t.TempDir(), "worker")
	if err := os.WriteFile(path, buf, 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}
