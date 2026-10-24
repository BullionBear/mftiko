package artifact

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/opencontainers/go-digest"
)

type tarFile struct {
	hdr  *tar.Header
	body []byte
}

func reg(name string, mode int64, body string) tarFile {
	return tarFile{
		hdr:  &tar.Header{Name: name, Mode: mode, Typeflag: tar.TypeReg, Size: int64(len(body))},
		body: []byte(body),
	}
}

func gzipTar(t *testing.T, files ...tarFile) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	for _, f := range files {
		hdr := *f.hdr
		if hdr.Format == 0 {
			hdr.Format = tar.FormatPAX
		}
		if (hdr.Typeflag == tar.TypeReg || hdr.Typeflag == tar.TypeRegA) && hdr.Size == 0 {
			hdr.Size = int64(len(f.body))
		}
		if err := tw.WriteHeader(&hdr); err != nil {
			t.Fatal(err)
		}
		if len(f.body) > 0 {
			if _, err := tw.Write(f.body); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func withBenign(files []tarFile) []tarFile {
	out := make([]tarFile, 0, len(files)+1)
	out = append(out, reg("benign", 0o644, "keep"))
	return append(out, files...)
}

// assertNoExtract fails if a rejected unpack left a file behind: the benign entry, the payload,
// or a temporary directory. The input tar and the sentinel may remain.
func assertNoExtract(t *testing.T, root string) {
	t.Helper()
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		switch d.Name() {
		case "benign", "pwned", "ok":
			t.Errorf("left on disk: %s", p)
		}
		if strings.HasPrefix(d.Name(), tempPrefix) {
			t.Errorf("temporary left on disk: %s", p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRejectMaliciousTar(t *testing.T) {
	// Every rejection in E09-D051, and the symlink-then-write escape in E09-D052. Each archive also
	// carries a benign file before the bad entry, so a partial extract would leave it behind.
	attacks := []struct {
		name    string
		entries func(outside, dest string) []tarFile
	}{
		{
			name: "absolute_path",
			entries: func(outside, _ string) []tarFile {
				return []tarFile{reg(filepath.Join(outside, "pwned"), 0o644, "PWNED")}
			},
		},
		{
			name: "dotdot",
			entries: func(_, _ string) []tarFile {
				return []tarFile{reg("../outside/pwned", 0o644, "PWNED")}
			},
		},
		{
			name: "dotdot_middle",
			entries: func(_, _ string) []tarFile {
				return []tarFile{reg("sub/../../outside/pwned", 0o644, "PWNED")}
			},
		},
		{
			name: "dotdot_stays_inside",
			entries: func(_, _ string) []tarFile {
				return []tarFile{reg("sub/../inside", 0o644, "PWNED")}
			},
		},
		{
			name: "symlink_absolute_target",
			entries: func(outside, _ string) []tarFile {
				return []tarFile{{
					hdr: &tar.Header{Name: "a", Mode: 0o777, Typeflag: tar.TypeSymlink, Linkname: outside},
				}}
			},
		},
		{
			name: "symlink_absolute_inside_rootfs",
			entries: func(_, dest string) []tarFile {
				return []tarFile{{
					hdr: &tar.Header{Name: "a", Mode: 0o777, Typeflag: tar.TypeSymlink, Linkname: filepath.Join(dest, "inside")},
				}}
			},
		},
		{
			name: "symlink_dotdot",
			entries: func(_, _ string) []tarFile {
				return []tarFile{{
					hdr: &tar.Header{Name: "a", Mode: 0o777, Typeflag: tar.TypeSymlink, Linkname: "../outside"},
				}}
			},
		},
		{
			name: "d952_absolute_symlink_then_write",
			entries: func(outside, _ string) []tarFile {
				return []tarFile{
					{hdr: &tar.Header{Name: "a", Mode: 0o777, Typeflag: tar.TypeSymlink, Linkname: outside}},
					reg("a/pwned", 0o644, "PWNED"),
				}
			},
		},
		{
			name: "d952_dotdot_symlink_then_write",
			entries: func(_, _ string) []tarFile {
				return []tarFile{
					{hdr: &tar.Header{Name: "a", Mode: 0o777, Typeflag: tar.TypeSymlink, Linkname: "../outside"}},
					reg("a/pwned", 0o644, "PWNED"),
				}
			},
		},
		{
			name: "char_device",
			entries: func(_, _ string) []tarFile {
				return []tarFile{{hdr: &tar.Header{Name: "dev", Mode: 0o666, Typeflag: tar.TypeChar, Devmajor: 1, Devminor: 3}}}
			},
		},
		{
			name: "block_device",
			entries: func(_, _ string) []tarFile {
				return []tarFile{{hdr: &tar.Header{Name: "dev", Mode: 0o666, Typeflag: tar.TypeBlock, Devmajor: 7, Devminor: 0}}}
			},
		},
		{
			name: "fifo",
			entries: func(_, _ string) []tarFile {
				return []tarFile{{hdr: &tar.Header{Name: "pipe", Mode: 0o644, Typeflag: tar.TypeFifo}}}
			},
		},
		{
			name: "setuid",
			entries: func(_, _ string) []tarFile {
				return []tarFile{reg("bin", 0o4755, "x")}
			},
		},
		{
			name: "setgid",
			entries: func(_, _ string) []tarFile {
				return []tarFile{reg("bin", 0o2755, "x")}
			},
		},
		{
			name: "setgid_directory",
			entries: func(_, _ string) []tarFile {
				return []tarFile{{hdr: &tar.Header{Name: "dir", Mode: 0o2755, Typeflag: tar.TypeDir}}}
			},
		},
		{
			name: "whiteout",
			entries: func(_, _ string) []tarFile {
				return []tarFile{reg("dir/.wh.file", 0o644, "")}
			},
		},
		{
			name: "whiteout_opaque",
			entries: func(_, _ string) []tarFile {
				return []tarFile{reg(".wh..wh..opq", 0o644, "")}
			},
		},
		{
			name: "hardlink_missing",
			entries: func(_, _ string) []tarFile {
				return []tarFile{{hdr: &tar.Header{Name: "h", Mode: 0o644, Typeflag: tar.TypeLink, Linkname: "missing"}}}
			},
		},
		{
			name: "hardlink_not_yet",
			entries: func(_, _ string) []tarFile {
				return []tarFile{
					{hdr: &tar.Header{Name: "h", Mode: 0o644, Typeflag: tar.TypeLink, Linkname: "later"}},
					reg("later", 0o644, "x"),
				}
			},
		},
		{
			name: "hardlink_absolute",
			entries: func(outside, _ string) []tarFile {
				return []tarFile{{hdr: &tar.Header{Name: "h", Mode: 0o644, Typeflag: tar.TypeLink, Linkname: filepath.Join(outside, "sentinel")}}}
			},
		},
		{
			name: "hardlink_dotdot",
			entries: func(_, _ string) []tarFile {
				return []tarFile{{hdr: &tar.Header{Name: "h", Mode: 0o644, Typeflag: tar.TypeLink, Linkname: "../outside/sentinel"}}}
			},
		},
		{
			name: "hardlink_to_symlink",
			entries: func(_, _ string) []tarFile {
				return []tarFile{
					{hdr: &tar.Header{Name: "s", Mode: 0o777, Typeflag: tar.TypeSymlink, Linkname: "benign"}},
					{hdr: &tar.Header{Name: "h", Mode: 0o644, Typeflag: tar.TypeLink, Linkname: "s"}},
				}
			},
		},
		{
			name: "hardlink_to_directory",
			entries: func(_, _ string) []tarFile {
				return []tarFile{
					{hdr: &tar.Header{Name: "d", Mode: 0o755, Typeflag: tar.TypeDir}},
					{hdr: &tar.Header{Name: "h", Mode: 0o644, Typeflag: tar.TypeLink, Linkname: "d"}},
				}
			},
		},
		{
			name: "contiguous",
			entries: func(_, _ string) []tarFile {
				return []tarFile{{hdr: &tar.Header{Name: "c", Mode: 0o644, Typeflag: tar.TypeCont, Size: 1}, body: []byte("x")}}
			},
		},
		{
			name: "gnu_sparse",
			entries: func(_, _ string) []tarFile {
				return []tarFile{{hdr: &tar.Header{Name: "s", Mode: 0o644, Typeflag: tar.TypeGNUSparse}}}
			},
		},
	}

	for _, tc := range attacks {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			outside := filepath.Join(root, "outside")
			if err := os.Mkdir(outside, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(outside, "sentinel"), []byte("sentinel"), 0o644); err != nil {
				t.Fatal(err)
			}
			work := filepath.Join(root, "work")
			if err := os.Mkdir(work, 0o755); err != nil {
				t.Fatal(err)
			}
			dest := filepath.Join(work, "out")
			raw := gzipTar(t, withBenign(tc.entries(outside, dest))...)
			layer := filepath.Join(root, "layer.tar.gz")
			if err := os.WriteFile(layer, raw, 0o644); err != nil {
				t.Fatal(err)
			}
			err := Unpack(context.Background(), []string{layer}, dest, UnpackLimits{})
			wantReason(t, err, ReasonUnsafeLayer, true)
			if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
				t.Fatalf("dest: %v", statErr)
			}
			left, err := os.ReadDir(work)
			if err != nil {
				t.Fatal(err)
			}
			if len(left) != 0 {
				t.Fatalf("work directory left %d entries", len(left))
			}
			if _, statErr := os.Stat(filepath.Join(outside, "pwned")); !os.IsNotExist(statErr) {
				t.Fatalf("escaped file: %v", statErr)
			}
			if _, statErr := os.Stat(filepath.Join(work, "outside", "pwned")); !os.IsNotExist(statErr) {
				t.Fatalf("escaped file: %v", statErr)
			}
			sentinel, err := os.ReadFile(filepath.Join(outside, "sentinel"))
			if err != nil || string(sentinel) != "sentinel" {
				t.Fatalf("sentinel %q %v", sentinel, err)
			}
			assertNoExtract(t, root)
		})
	}
}

func TestUnpackImageRejectKeepsLayer(t *testing.T) {
	data := t.TempDir()
	outside := filepath.Join(data, "outside")
	if err := os.Mkdir(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	raw := gzipTar(t, withBenign([]tarFile{
		{hdr: &tar.Header{Name: "a", Mode: 0o777, Typeflag: tar.TypeSymlink, Linkname: outside}},
		reg("a/pwned", 0o644, "PWNED"),
	})...)
	layer := cacheLayer(t, data, raw)
	c, _ := openAt(t, Registry{Address: "127.0.0.1:9", PlainHTTP: true}, data)
	image := digest.FromBytes([]byte("image")).String()
	_, err := c.UnpackImage(context.Background(), image, []Layer{layer}, UnpackLimits{})
	wantReason(t, err, ReasonUnsafeLayer, true)
	if _, statErr := os.Stat(c.rootfsPath(image)); !os.IsNotExist(statErr) {
		t.Fatalf("rootfs: %v", statErr)
	}
	if !bytes.Equal(mustRead(t, layer.Path), raw) {
		t.Fatal("cached layer was deleted or changed")
	}
	assertNoExtract(t, data)
}

func TestUnpackAccepted(t *testing.T) {
	if DefaultMaxUnpackedBytes != 4<<30 || DefaultMaxFiles != 100_000 {
		t.Fatalf("defaults bytes %d files %d", DefaultMaxUnpackedBytes, DefaultMaxFiles)
	}
	root := t.TempDir()
	work := filepath.Join(root, "work")
	if err := os.Mkdir(work, 0o755); err != nil {
		t.Fatal(err)
	}
	layer1 := gzipTar(t,
		tarFile{hdr: &tar.Header{Name: ".", Mode: 0o755, Typeflag: tar.TypeDir}},
		tarFile{hdr: &tar.Header{
			Name: "bin/strategy", Mode: 0o755, Typeflag: tar.TypeRegA, Size: 3,
			Uid: 65534, Gid: 65534,
		}, body: []byte("exe")},
		tarFile{hdr: &tar.Header{Name: "real", Mode: 0o755, Typeflag: tar.TypeDir}},
		tarFile{hdr: &tar.Header{Name: "a", Mode: 0o777, Typeflag: tar.TypeSymlink, Linkname: "real"}},
		reg("a/hello", 0o644, "hi"),
		reg("b", 0o644, "bee"),
	)
	layer2 := gzipTar(t,
		reg("b", 0o644, "two"),
		tarFile{hdr: &tar.Header{Name: "c", Mode: 0o644, Typeflag: tar.TypeLink, Linkname: "bin/strategy"}},
	)
	p1 := filepath.Join(root, "1.tar.gz")
	p2 := filepath.Join(root, "2.tar.gz")
	if err := os.WriteFile(p1, layer1, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p2, layer2, 0o644); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(work, "out")
	if err := Unpack(context.Background(), []string{p1, p2}, dest, UnpackLimits{}); err != nil {
		t.Fatal(err)
	}
	if string(mustRead(t, filepath.Join(dest, "bin", "strategy"))) != "exe" {
		t.Fatal("strategy")
	}
	st, err := os.Lstat(filepath.Join(dest, "bin", "strategy"))
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm()&0o111 == 0 {
		t.Fatalf("mode %v", st.Mode())
	}
	sys := st.Sys().(*syscall.Stat_t)
	if sys.Uid != uint32(os.Getuid()) || sys.Gid != uint32(os.Getgid()) {
		t.Fatalf("uid %d gid %d, want the agent user", sys.Uid, sys.Gid)
	}
	if got := mustRead(t, filepath.Join(dest, "real", "hello")); string(got) != "hi" {
		t.Fatalf("write through in-root symlink: %q", got)
	}
	link, err := os.Readlink(filepath.Join(dest, "a"))
	if err != nil || link != "real" {
		t.Fatalf("symlink %q %v", link, err)
	}
	if string(mustRead(t, filepath.Join(dest, "b"))) != "two" {
		t.Fatal("overlay")
	}
	exe, err := os.Stat(filepath.Join(dest, "bin", "strategy"))
	if err != nil {
		t.Fatal(err)
	}
	hard, err := os.Stat(filepath.Join(dest, "c"))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(exe, hard) {
		t.Fatal("hardlink does not share the inode")
	}
}

func TestUnpackLimits(t *testing.T) {
	root := t.TempDir()
	one := gzipTar(t, reg("a", 0o644, "abcd"))
	two := gzipTar(t, reg("b", 0o644, "ef"))
	p1 := filepath.Join(root, "1.tar.gz")
	p2 := filepath.Join(root, "2.tar.gz")
	if err := os.WriteFile(p1, one, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p2, two, 0o644); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(root, "out")
	err := Unpack(context.Background(), []string{p1}, dest, UnpackLimits{MaxBytes: 4, MaxFiles: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(dest); err != nil {
		t.Fatal(err)
	}

	err = Unpack(context.Background(), []string{p1}, dest, UnpackLimits{MaxBytes: 3, MaxFiles: 10})
	wantReason(t, err, ReasonUnsafeLayer, true)
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Fatalf("dest: %v", statErr)
	}
	assertNoExtract(t, root)

	err = Unpack(context.Background(), []string{p1, p2}, dest, UnpackLimits{MaxBytes: 5, MaxFiles: 10})
	wantReason(t, err, ReasonUnsafeLayer, true)
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Fatalf("dest: %v", statErr)
	}
	assertNoExtract(t, root)

	dirAndFile := gzipTar(t,
		tarFile{hdr: &tar.Header{Name: "d", Mode: 0o755, Typeflag: tar.TypeDir}},
		reg("d/f", 0o644, "z"),
	)
	p3 := filepath.Join(root, "3.tar.gz")
	if err := os.WriteFile(p3, dirAndFile, 0o644); err != nil {
		t.Fatal(err)
	}
	err = Unpack(context.Background(), []string{p3}, dest, UnpackLimits{MaxFiles: 1})
	wantReason(t, err, ReasonUnsafeLayer, true)
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Fatalf("dest: %v", statErr)
	}
	assertNoExtract(t, root)

	err = Unpack(context.Background(), []string{p1}, dest, UnpackLimits{MaxBytes: -1})
	wantRequest(t, err)
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Fatalf("dest: %v", statErr)
	}
	assertNoExtract(t, root)
}

func TestUnpackNotGzip(t *testing.T) {
	root := t.TempDir()
	layer := filepath.Join(root, "layer")
	if err := os.WriteFile(layer, []byte("not a gzip"), 0o644); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(root, "out")
	err := Unpack(context.Background(), []string{layer}, dest, UnpackLimits{})
	wantReason(t, err, ReasonUnsafeLayer, true)
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Fatalf("dest: %v", statErr)
	}
	assertNoExtract(t, root)
}

func TestUnpackCancel(t *testing.T) {
	root := t.TempDir()
	layer := filepath.Join(root, "layer.tar.gz")
	if err := os.WriteFile(layer, gzipTar(t, reg("a", 0o644, "abc")), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dest := filepath.Join(root, "out")
	err := Unpack(ctx, []string{layer}, dest, UnpackLimits{})
	if !errors.Is(err, context.Canceled) || ReasonOf(err) != "" {
		t.Fatalf("got %v", err)
	}
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Fatalf("dest: %v", statErr)
	}
	assertNoExtract(t, root)
}

func cacheLayer(t *testing.T, data string, body []byte) Layer {
	t.Helper()
	d := digest.FromBytes(body)
	dir := filepath.Join(data, "layers")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, d.String())
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	return Layer{Digest: d.String(), Size: int64(len(body)), Path: path}
}

func testClient(t *testing.T, data string) *Client {
	t.Helper()
	c, _ := openAt(t, Registry{Address: "127.0.0.1:9", PlainHTTP: true}, data)
	return c
}

func TestUnpackImageReuse(t *testing.T) {
	data := t.TempDir()
	body := gzipTar(t, reg("bin/strategy", 0o755, "exe"))
	layer := cacheLayer(t, data, body)
	c := testClient(t, data)
	image := digest.FromBytes([]byte("reuse")).String()
	root, err := c.UnpackImage(context.Background(), image, []Layer{layer}, UnpackLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if string(mustRead(t, filepath.Join(root, "bin", "strategy"))) != "exe" {
		t.Fatal("content")
	}
	list := mustRead(t, filepath.Join(data, "artifacts", image, layersName))
	if string(list) != layer.Digest+"\n" {
		t.Fatalf("layer list %q", list)
	}
	if err := os.WriteFile(filepath.Join(root, "marker"), []byte("m"), 0o644); err != nil {
		t.Fatal(err)
	}
	unpackEnter = func() { t.Error("unpacked again") }
	t.Cleanup(func() { unpackEnter = nil })
	again, err := c.UnpackImage(context.Background(), image, []Layer{layer}, UnpackLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if again != root || string(mustRead(t, filepath.Join(root, "marker"))) != "m" {
		t.Fatal("existing rootfs was rebuilt")
	}
}

func TestUnpackImageSingleFlight(t *testing.T) {
	data := t.TempDir()
	body := gzipTar(t, reg("a", 0o644, "one"))
	layer := cacheLayer(t, data, body)
	c := testClient(t, data)
	image := digest.FromBytes([]byte("flight")).String()

	release := make(chan struct{})
	entered := make(chan struct{}, 2)
	var n atomic.Int32
	unpackEnter = func() {
		n.Add(1)
		entered <- struct{}{}
		<-release
	}
	t.Cleanup(func() {
		unpackEnter = nil
		unpackWait = nil
		select {
		case <-release:
		default:
			close(release)
		}
	})

	type result struct {
		path string
		err  error
	}
	first := make(chan result, 1)
	go func() {
		p, err := c.UnpackImage(context.Background(), image, []Layer{layer}, UnpackLimits{})
		first <- result{p, err}
	}()
	select {
	case <-entered:
	case res := <-first:
		t.Fatalf("leader: %v", res.err)
	}

	joined := make(chan struct{})
	unpackWait = func() { close(joined) }
	second := make(chan result, 1)
	go func() {
		p, err := c.UnpackImage(context.Background(), image, []Layer{layer}, UnpackLimits{})
		second <- result{p, err}
	}()
	select {
	case <-joined:
	case <-entered:
		t.Fatal("second unpack started its own extract")
	}
	close(release)
	a := <-first
	b := <-second
	if a.err != nil || b.err != nil || a.path == "" || a.path != b.path {
		t.Fatalf("leader %+v waiter %+v", a, b)
	}
	if n.Load() != 1 {
		t.Fatalf("extracts %d", n.Load())
	}
	if string(mustRead(t, filepath.Join(a.path, "a"))) != "one" {
		t.Fatal("content")
	}
}

func TestUnpackImageWaiterSurvivesLeaderCancel(t *testing.T) {
	data := t.TempDir()
	body := gzipTar(t, reg("a", 0o644, "one"))
	layer := cacheLayer(t, data, body)
	c := testClient(t, data)
	image := digest.FromBytes([]byte("cancel")).String()

	release := make(chan struct{})
	entered := make(chan struct{}, 2)
	var n atomic.Int32
	unpackEnter = func() {
		n.Add(1)
		entered <- struct{}{}
		<-release
	}
	t.Cleanup(func() {
		unpackEnter = nil
		unpackWait = nil
		select {
		case <-release:
		default:
			close(release)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	leaderErr := make(chan error, 1)
	go func() {
		_, err := c.UnpackImage(ctx, image, []Layer{layer}, UnpackLimits{})
		leaderErr <- err
	}()
	select {
	case <-entered:
	case err := <-leaderErr:
		t.Fatalf("leader: %v", err)
	}
	joined := make(chan struct{})
	unpackWait = func() { close(joined) }
	waiterErr := make(chan error, 1)
	go func() {
		_, err := c.UnpackImage(context.Background(), image, []Layer{layer}, UnpackLimits{})
		waiterErr <- err
	}()
	select {
	case <-joined:
	case <-entered:
		t.Fatal("waiter unpacked before the leader finished")
	}
	cancel()
	close(release)
	if err := <-leaderErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader: %v", err)
	}
	if err := <-waiterErr; err != nil {
		t.Fatal(err)
	}
	if n.Load() != 2 {
		t.Fatalf("extracts %d, want the leader and the waiter", n.Load())
	}
	if string(mustRead(t, filepath.Join(c.rootfsPath(image), "a"))) != "one" {
		t.Fatal("content")
	}
	assertNoExtract(t, data)
}

func TestOpenDropsArtifactTemps(t *testing.T) {
	data := t.TempDir()
	art := filepath.Join(data, "artifacts")
	stale := filepath.Join(art, tempPrefix+"stale", "rootfs")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "benign"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	keep := "sha256:" + strings.Repeat("ab", 32)
	kept := filepath.Join(art, keep, "rootfs")
	if err := os.MkdirAll(kept, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(kept, "ok"), []byte("kept"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Registry{Address: "127.0.0.1:9", PlainHTTP: true}, data); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(art, tempPrefix+"stale")); !os.IsNotExist(err) {
		t.Fatalf("temp dir: %v", err)
	}
	if string(mustRead(t, filepath.Join(kept, "ok"))) != "kept" {
		t.Fatal("finished rootfs was removed")
	}
}

func snap(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte(nil), b...)
}

func TestGCKeepsPinnedAndTemps(t *testing.T) {
	data := t.TempDir()
	c := testClient(t, data)
	keepDigest := "sha256:" + strings.Repeat("aa", 32)
	dropDigest := "sha256:" + strings.Repeat("bb", 32)
	absent := "sha256:" + strings.Repeat("cc", 32)
	layerKeep := "sha256:" + strings.Repeat("11", 32)
	layerShared := "sha256:" + strings.Repeat("22", 32)
	layerDrop := "sha256:" + strings.Repeat("33", 32)
	layerOrphan := "sha256:" + strings.Repeat("44", 32)

	writeArtifact := func(digest, rootBody, list string) {
		t.Helper()
		dir := filepath.Join(data, "artifacts", digest)
		if err := os.MkdirAll(filepath.Join(dir, "rootfs"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "rootfs", "file"), []byte(rootBody), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, layersName), []byte(list), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeArtifact(keepDigest, "keep-bytes", layerKeep+"\n"+layerShared+"\n")
	writeArtifact(dropDigest, "drop-bytes", layerDrop+"\n"+layerShared+"\n")

	layerDir := filepath.Join(data, "layers")
	if err := os.MkdirAll(layerDir, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) string {
		t.Helper()
		p := filepath.Join(layerDir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	keepPath := write(layerKeep, "layer-keep")
	sharedPath := write(layerShared, "layer-shared")
	dropPath := write(layerDrop, "layer-drop")
	orphanPath := write(layerOrphan, "layer-orphan")
	notePath := write("notes.txt", "not-a-digest")
	tempFile := write(tempPrefix+"download", "partial-bytes")

	tempDir := filepath.Join(data, "artifacts", tempPrefix+"unpack", "rootfs")
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		t.Fatal(err)
	}
	tempBody := filepath.Join(tempDir, "wip")
	if err := os.WriteFile(tempBody, []byte("wip-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	tempPlain := filepath.Join(data, "artifacts", tempPrefix+"file")
	if err := os.WriteFile(tempPlain, []byte("temp-file"), 0o644); err != nil {
		t.Fatal(err)
	}

	keptRoot := snap(t, filepath.Join(data, "artifacts", keepDigest, "rootfs", "file"))
	keptList := snap(t, filepath.Join(data, "artifacts", keepDigest, layersName))
	keptLayer := snap(t, keepPath)
	sharedLayer := snap(t, sharedPath)
	tempDownload := snap(t, tempFile)
	tempUnpack := snap(t, tempBody)
	tempFileBody := snap(t, tempPlain)
	note := snap(t, notePath)

	if err := c.GC([]string{"../" + keepDigest}); err == nil || !errors.Is(err, ErrRequest) {
		t.Fatalf("bad keep: %v", err)
	}
	if string(mustRead(t, filepath.Join(data, "artifacts", dropDigest, "rootfs", "file"))) != "drop-bytes" {
		t.Fatal("bad keep deleted a rootfs")
	}

	if err := c.GC([]string{keepDigest, absent}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(mustRead(t, filepath.Join(data, "artifacts", keepDigest, "rootfs", "file")), keptRoot) {
		t.Fatal("kept rootfs changed")
	}
	if !bytes.Equal(mustRead(t, filepath.Join(data, "artifacts", keepDigest, layersName)), keptList) {
		t.Fatal("kept layer list changed")
	}
	if !bytes.Equal(mustRead(t, keepPath), keptLayer) || !bytes.Equal(mustRead(t, sharedPath), sharedLayer) {
		t.Fatal("referenced layer changed")
	}
	if !bytes.Equal(mustRead(t, tempFile), tempDownload) {
		t.Fatal("temp download changed")
	}
	if !bytes.Equal(mustRead(t, tempBody), tempUnpack) {
		t.Fatal("temp unpack changed")
	}
	if !bytes.Equal(mustRead(t, tempPlain), tempFileBody) {
		t.Fatal("temp file changed")
	}
	if !bytes.Equal(mustRead(t, notePath), note) {
		t.Fatal("non-digest file changed")
	}
	if _, err := os.Stat(filepath.Join(data, "artifacts", dropDigest)); !os.IsNotExist(err) {
		t.Fatalf("dropped rootfs: %v", err)
	}
	if _, err := os.Stat(dropPath); !os.IsNotExist(err) {
		t.Fatalf("dropped layer: %v", err)
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("orphan layer: %v", err)
	}
	if _, err := os.Stat(filepath.Join(data, "artifacts", absent)); !os.IsNotExist(err) {
		t.Fatalf("keep created a directory: %v", err)
	}

	if err := c.GC(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(data, "artifacts", keepDigest)); !os.IsNotExist(err) {
		t.Fatalf("empty keep left rootfs: %v", err)
	}
	if _, err := os.Stat(keepPath); !os.IsNotExist(err) {
		t.Fatalf("empty keep left layer: %v", err)
	}
	if _, err := os.Stat(sharedPath); !os.IsNotExist(err) {
		t.Fatalf("empty keep left shared layer: %v", err)
	}
	if !bytes.Equal(mustRead(t, tempFile), tempDownload) || !bytes.Equal(mustRead(t, tempBody), tempUnpack) {
		t.Fatal("empty keep touched a temp")
	}
	if !bytes.Equal(mustRead(t, notePath), note) {
		t.Fatal("empty keep removed a non-digest file")
	}
}

func TestUnpackImageThenGC(t *testing.T) {
	data := t.TempDir()
	c := testClient(t, data)
	keepBody := gzipTar(t, reg("a", 0o644, "keep"))
	dropBody := gzipTar(t, reg("a", 0o644, "drop"))
	keepLayer := cacheLayer(t, data, keepBody)
	dropLayer := cacheLayer(t, data, dropBody)
	keepImage := digest.FromBytes([]byte("keep-image")).String()
	dropImage := digest.FromBytes([]byte("drop-image")).String()
	if _, err := c.UnpackImage(context.Background(), keepImage, []Layer{keepLayer}, UnpackLimits{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UnpackImage(context.Background(), dropImage, []Layer{dropLayer}, UnpackLimits{}); err != nil {
		t.Fatal(err)
	}
	temp := filepath.Join(data, "layers", tempPrefix+"partial")
	if err := os.WriteFile(temp, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := c.GC([]string{keepImage}); err != nil {
		t.Fatal(err)
	}
	if string(mustRead(t, filepath.Join(c.rootfsPath(keepImage), "a"))) != "keep" {
		t.Fatal("kept rootfs")
	}
	if !bytes.Equal(mustRead(t, keepLayer.Path), keepBody) {
		t.Fatal("kept layer")
	}
	if _, err := os.Stat(c.rootfsPath(dropImage)); !os.IsNotExist(err) {
		t.Fatalf("dropped rootfs: %v", err)
	}
	if _, err := os.Stat(dropLayer.Path); !os.IsNotExist(err) {
		t.Fatalf("dropped layer: %v", err)
	}
	if string(mustRead(t, temp)) != "partial" {
		t.Fatal("temp file")
	}
}
