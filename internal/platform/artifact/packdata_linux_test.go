package artifact

import (
	"archive/tar"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
)

func TestPackDataSpecialFiles(t *testing.T) {
	root := t.TempDir()
	keep := filepath.Join(root, "keep")
	if err := os.WriteFile(keep, []byte("same"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(keep, filepath.Join(root, "also")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("keep", filepath.Join(root, "inside")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../outside", filepath.Join(root, "outside")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/tmp/abs-target", filepath.Join(root, "abs")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(root, "pipe"), 0o644); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", filepath.Join(root, "app.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if err := os.WriteFile(filepath.Join(root, "suid"), []byte("s"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(root, "suid"), 0o4755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".wh.gone"), []byte("w"), 0o644); err != nil {
		t.Fatal(err)
	}
	dev := filepath.Join(root, "devnode")
	if err := syscall.Mknod(dev, syscall.S_IFCHR|0o666, int(makedev(1, 3))); err != nil {
		dev = ""
	}

	empty, err := DataVolumeEmpty(root)
	if err != nil || empty {
		t.Fatalf("empty %v err %v", empty, err)
	}
	dest := filepath.Join(t.TempDir(), "layer.gz")
	skipped, err := PackData(root, dest)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		".wh.gone": "whiteout",
		"outside":  "symlink",
		"abs":      "symlink",
		"pipe":     "fifo",
		"app.sock": "socket",
	}
	if dev != "" {
		want["devnode"] = "device"
	}
	gotSkip := map[string]string{}
	for _, s := range skipped {
		gotSkip[s.Path] = s.Reason
	}
	for path, sub := range want {
		reason, ok := gotSkip[path]
		if !ok || !strings.Contains(reason, sub) {
			t.Fatalf("skip %s: %q, want %s in %+v", path, reason, sub, skipped)
		}
	}
	members := readDataLayer(t, dest)
	byName := map[string]tarMember{}
	var names []string
	for _, m := range members {
		byName[m.name] = m
		names = append(names, m.name)
	}
	if !slices.IsSorted(names) {
		t.Fatalf("order %v", names)
	}
	for _, banned := range []string{"pipe", "app.sock", "outside", "abs", ".wh.gone", "devnode"} {
		if _, ok := byName[banned]; ok {
			t.Fatalf("packed %s", banned)
		}
	}
	for _, name := range []string{"also", "inside", "keep", "suid"} {
		if _, ok := byName[name]; !ok {
			t.Fatalf("missing %s in %v", name, names)
		}
	}
	if byName["keep"].typ != tar.TypeReg || byName["also"].typ != tar.TypeReg {
		t.Fatal("hardlink stored as a link")
	}
	if string(byName["keep"].body) != "same" || string(byName["also"].body) != "same" {
		t.Fatal("hardlink body")
	}
	if byName["inside"].typ != tar.TypeSymlink || byName["inside"].link != "keep" || byName["inside"].mode&0o777 != 0o777 {
		t.Fatalf("inside %+v", byName["inside"])
	}
	if byName["keep"].mode&0o777 != 0o644 || byName["suid"].mode&0o777 != 0o755 {
		t.Fatalf("modes keep %o suid %o", byName["keep"].mode, byName["suid"].mode)
	}
	if byName["keep"].uid != 0 || byName["keep"].gid != 0 || byName["keep"].mtime != 0 {
		t.Fatalf("identity %+v", byName["keep"])
	}
	again := filepath.Join(t.TempDir(), "again.gz")
	if _, err := PackData(root, again); err != nil {
		t.Fatal(err)
	}
	if string(mustRead(t, dest)) != string(mustRead(t, again)) {
		t.Fatal("special-file pack is not reproducible")
	}
}

func TestFifoAndOutsideSymlinkEmptiness(t *testing.T) {
	fifo := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(fifo, "pipe"), 0o644); err != nil {
		t.Fatal(err)
	}
	empty, err := DataVolumeEmpty(fifo)
	if err != nil || !empty {
		t.Fatalf("fifo empty %v err %v", empty, err)
	}
	link := t.TempDir()
	if err := os.Symlink("../outside", filepath.Join(link, "out")); err != nil {
		t.Fatal(err)
	}
	empty, err = DataVolumeEmpty(link)
	if err != nil || empty {
		t.Fatalf("symlink empty %v err %v", empty, err)
	}
}

func makedev(major, minor uint32) uint64 {
	return uint64(major)<<8 | uint64(minor)
}
