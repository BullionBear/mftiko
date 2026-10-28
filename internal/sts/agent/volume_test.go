package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
)

func TestInputNames(t *testing.T) {
	ok := []string{"a", "0", "a0", "a_b", "a-b", "a" + strings.Repeat("b", 63)}
	for _, name := range ok {
		if !inputNameOK(name) {
			t.Fatalf("rejected %q", name)
		}
	}
	bad := []string{"", "A", "-a", "_a", "a.b", "a/b", "..", strings.Repeat("a", 65)}
	for _, name := range bad {
		if inputNameOK(name) {
			t.Fatalf("accepted %q", name)
		}
	}
	decls := []*sessionv1.InputDecl{
		{Name: "model", Digest: "sha256:" + strings.Repeat("ab", 32), Repository: "hello"},
		{Name: "model", Digest: "sha256:" + strings.Repeat("cd", 32), Repository: "hello"},
	}
	if err := validateInputs(decls); err == nil {
		t.Fatal("duplicate name")
	}
	decls[1].Name = "other"
	decls[1].Repository = ""
	if err := validateInputs(decls); err == nil {
		t.Fatal("empty repository")
	}
	if err := validateInputs(nil); err != nil {
		t.Fatal(err)
	}
}

func TestInflightLayers(t *testing.T) {
	p := newPuller(context.Background(), nil, t.TempDir(), 0, artifact.UnpackLimits{}, zerolog.Nop(), time.Now)
	dig := "sha256:" + strings.Repeat("ab", 32)
	p.inputs[inputKey("s", "model")] = &inputJob{running: true, layers: []string{dig, "nope"}}
	got := p.inflightLayers()
	if len(got) != 1 || got[0] != dig {
		t.Fatalf("%v", got)
	}
	p.inputs[inputKey("s", "model")].running = false
	if len(p.inflightLayers()) != 0 {
		t.Fatal("idle job still reported a layer")
	}
}

func TestVolumeLimit(t *testing.T) {
	if got := volumeLimit(&sessionv1.SessionSpec{}, 1024); got != 1024*1024*1024 {
		t.Fatalf("default %d", got)
	}
	spec := &sessionv1.SessionSpec{Limits: &sessionv1.Limits{VolumeMb: 3}}
	if got := volumeLimit(spec, 1024); got != 3*1024*1024 {
		t.Fatalf("spec %d", got)
	}
	if got := volumeLimit(&sessionv1.SessionSpec{}, 0); got != int64(DefaultVolumeMB)*1024*1024 {
		t.Fatalf("zero deployment %d", got)
	}
}

func TestDiskBytesDedupesHardLinks(t *testing.T) {
	dir := t.TempDir()
	body := []byte(strings.Repeat("x", 256*1024))
	a := filepath.Join(dir, "a")
	if err := os.WriteFile(a, body, 0o644); err != nil {
		t.Fatal(err)
	}
	one, err := diskBytes(dir)
	if err != nil {
		t.Fatal(err)
	}
	if one <= 0 {
		t.Fatal("empty usage")
	}
	if err := os.Link(a, filepath.Join(dir, "b")); err != nil {
		t.Skip("hardlink:", err)
	}
	two, err := diskBytes(dir)
	if err != nil {
		t.Fatal(err)
	}
	if two != one {
		t.Fatalf("hardlink counted twice: %d then %d", one, two)
	}
	outside := t.TempDir()
	big := filepath.Join(outside, "big")
	if err := os.WriteFile(big, []byte(strings.Repeat("y", 1024*1024)), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(big, filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	three, err := diskBytes(dir)
	if err != nil {
		t.Fatal(err)
	}
	out, err := diskBytes(outside)
	if err != nil {
		t.Fatal(err)
	}
	if three >= one+out {
		t.Fatalf("followed symlink: dir %d outside %d", three, out)
	}
}

func TestCleanInputTemps(t *testing.T) {
	data := t.TempDir()
	temp := filepath.Join(data, "sessions", "s1", "inputs", inputTempPrefix+"wip")
	keep := filepath.Join(data, "sessions", "s1", "inputs", "model")
	if err := os.MkdirAll(temp, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(temp, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(temp, "f"), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(temp, 0o555); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(keep, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(keep, 0o555); err != nil {
		t.Fatal(err)
	}
	if err := cleanInputTemps(data); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(temp); !os.IsNotExist(err) {
		t.Fatalf("temp: %v", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatal(err)
	}
	if err := cleanInputTemps(t.TempDir()); err != nil {
		t.Fatal(err)
	}
}

func TestVolumeStaysOver(t *testing.T) {
	data := t.TempDir()
	vol := filepath.Join(data, "sessions", "s", "volume")
	if err := os.MkdirAll(vol, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(vol, "f"), []byte(strings.Repeat("z", 64*1024)), 0o644); err != nil {
		t.Fatal(err)
	}
	v := newVolumer(data, time.Hour, zerolog.Nop())
	v.setTargets([]volTarget{{id: "s", limit: 1}})
	v.measure()
	if !v.exceeded("s") {
		n, err := diskBytes(vol)
		t.Fatalf("not over: usage %d err %v", n, err)
	}
	v.setTargets([]volTarget{{id: "s", limit: 1 << 40}})
	v.measure()
	if !v.exceeded("s") {
		t.Fatal("over was cleared")
	}
	if v.exceeded("other") {
		t.Fatal("other")
	}
}

func assertPerm(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	st, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != want {
		t.Fatalf("%s mode %o, want %o", path, st.Mode().Perm(), want)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
