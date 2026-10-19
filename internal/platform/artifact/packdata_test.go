package artifact

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
)

func TestPackDataReproducible(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sub", "note"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "run.sh"), []byte("echo"), 0o755); err != nil {
		t.Fatal(err)
	}
	empty, err := DataVolumeEmpty(root)
	if err != nil || empty {
		t.Fatalf("empty %v err %v", empty, err)
	}
	a := filepath.Join(t.TempDir(), "a.gz")
	b := filepath.Join(t.TempDir(), "b.gz")
	if _, err := PackData(root, a); err != nil {
		t.Fatal(err)
	}
	if _, err := PackData(root, b); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(mustRead(t, a), mustRead(t, b)) {
		t.Fatal("two packs of one tree differ")
	}
	members := readDataLayer(t, a)
	if len(members) != 3 {
		t.Fatalf("entries %d", len(members))
	}
	want := []string{"run.sh", "sub", "sub/note"}
	for i, name := range want {
		if members[i].name != name {
			t.Fatalf("entry %d %q, want %q", i, members[i].name, name)
		}
		if members[i].uid != 0 || members[i].gid != 0 || members[i].mtime != 0 {
			t.Fatalf("%s uid %d gid %d mtime %d", name, members[i].uid, members[i].gid, members[i].mtime)
		}
	}
	if members[0].typ != tar.TypeReg || members[0].mode&0o777 != 0o755 || string(members[0].body) != "echo" {
		t.Fatalf("run.sh %+v", members[0])
	}
	if members[1].typ != tar.TypeDir || members[1].mode&0o777 != 0o755 {
		t.Fatalf("sub %+v", members[1])
	}
	if members[2].typ != tar.TypeReg || members[2].mode&0o777 != 0o644 || string(members[2].body) != "hello" {
		t.Fatalf("note %+v", members[2])
	}
}

func TestDataVolumeEmpty(t *testing.T) {
	missing, err := DataVolumeEmpty(filepath.Join(t.TempDir(), "gone"))
	if err != nil || !missing {
		t.Fatalf("missing empty %v err %v", missing, err)
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := DataVolumeEmpty(file); err == nil {
		t.Fatal("a file root was accepted")
	}
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "only"), 0o755); err != nil {
		t.Fatal(err)
	}
	empty, err := DataVolumeEmpty(root)
	if err != nil || !empty {
		t.Fatalf("dirs only: empty %v err %v", empty, err)
	}
	if err := os.WriteFile(filepath.Join(root, "only", ".wh.note"), []byte("w"), 0o644); err != nil {
		t.Fatal(err)
	}
	empty, err = DataVolumeEmpty(root)
	if err != nil || empty {
		t.Fatalf("whiteout: empty %v err %v", empty, err)
	}
	dest := filepath.Join(t.TempDir(), "layer.gz")
	skipped, err := PackData(root, dest)
	if err != nil {
		t.Fatal(err)
	}
	if len(skipped) != 1 || skipped[0].Path != "only/.wh.note" || !strings.Contains(skipped[0].Reason, "whiteout") {
		t.Fatalf("skipped %+v", skipped)
	}
	if members := readDataLayer(t, dest); len(members) != 1 || members[0].name != "only" {
		t.Fatalf("packed %+v", members)
	}
}

func TestPackDataFileIsTemp(t *testing.T) {
	data := t.TempDir()
	c, err := Open(Registry{Address: "127.0.0.1:1", PlainHTTP: true}, data)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "note"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	path, _, err := c.PackDataFile(root)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(path) != filepath.Join(data, "layers") || !strings.HasPrefix(filepath.Base(path), tempPrefix) {
		t.Fatalf("temp %s", path)
	}
	if _, err := Open(Registry{Address: "127.0.0.1:1", PlainHTTP: true}, data); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("temp left behind: %v", err)
	}
}

func TestPushDataRoundTrip(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	reg := newMemRegistry()
	defer reg.Close()
	host := strings.TrimPrefix(reg.srv.URL, "http://")
	c, err := Open(Registry{Address: host, PlainHTTP: true}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "note"), []byte("kept"), 0o644); err != nil {
		t.Fatal(err)
	}
	layer, _, err := c.PackDataFile(root)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(layer)
	st, err := os.Stat(layer)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	req := DataPush{
		Repository: "state", Tag: "a1b2c3", LayerPath: layer,
		Source: DataSource{SessionID: "a1b2c3", Incarnation: 2, Clean: true},
	}
	first, err := c.PushData(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.PushData(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest == "" || first.Digest != second.Digest || first.Size != st.Size() || second.Size != st.Size() {
		t.Fatalf("first %+v second %+v file %d", first, second, st.Size())
	}
	if _, err := c.PushData(ctx, DataPush{}); !errors.Is(err, ErrRequest) {
		t.Fatalf("empty push: %v", err)
	}
	got, err := c.PullData(ctx, DataRequest{Repository: "state", Digest: first.Digest})
	if err != nil {
		t.Fatal(err)
	}
	if got.Config.Schema != DataSchema || got.Config.Source == nil ||
		got.Config.Source.SessionID != "a1b2c3" || got.Config.Source.Incarnation != 2 || !got.Config.Source.Clean {
		t.Fatalf("config %+v", got.Config)
	}
	dest := filepath.Join(t.TempDir(), "out")
	if err := Unpack(ctx, []string{got.Layer.Path}, dest, UnpackLimits{}); err != nil {
		t.Fatal(err)
	}
	if string(mustRead(t, filepath.Join(dest, "note"))) != "kept" {
		t.Fatal("unpacked body")
	}
	manifest, _, _ := fetch(t, host+"/state", "a1b2c3")
	if digest.FromBytes(manifest).String() != first.Digest {
		t.Fatal("tag does not name the pushed manifest")
	}
	again, _, _ := fetch(t, host+"/state", first.Digest)
	if !bytes.Equal(manifest, again) {
		t.Fatal("digest fetch differs from the tag")
	}
}

func TestPushDataUsesRegistryCredentials(t *testing.T) {
	reg := newMemRegistry()
	defer reg.Close()
	reg.user, reg.pass = "user", "right"
	host := strings.TrimPrefix(reg.srv.URL, "http://")
	good := writeDockerConfig(t, host, "user", "right", true)
	badDir := t.TempDir()
	t.Setenv("DOCKER_CONFIG", badDir)
	raw, err := json.Marshal(map[string]any{
		"auths": map[string]any{
			host: map[string]string{"auth": base64.StdEncoding.EncodeToString([]byte("user:wrong"))},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(badDir, "config.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Open(Registry{Address: host, Credentials: good, PlainHTTP: true}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "note"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	layer, _, err := c.PackDataFile(root)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(layer)
	if _, err := c.PushData(context.Background(), DataPush{
		Repository: "state", Tag: "a1b2c3", LayerPath: layer,
		Source: DataSource{SessionID: "a1b2c3"},
	}); err != nil {
		t.Fatal(err)
	}
}

type tarMember struct {
	name  string
	mode  int64
	uid   int
	gid   int
	mtime int64
	typ   byte
	link  string
	body  []byte
}

func readDataLayer(t *testing.T, path string) []tarMember {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	if zr.Name != "" || zr.OS != 255 || (!zr.ModTime.IsZero() && zr.ModTime.Unix() != 0) {
		t.Fatalf("gzip name %q os %d mod %s", zr.Name, zr.OS, zr.ModTime)
	}
	tr := tar.NewReader(zr)
	var out []tarMember
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, tarMember{
			name: hdr.Name, mode: hdr.Mode, uid: hdr.Uid, gid: hdr.Gid,
			mtime: hdr.ModTime.Unix(), typ: hdr.Typeflag, link: hdr.Linkname, body: body,
		})
	}
}
