package artifact

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestCatalogRoundTrip(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DOCKER_CONFIG", dir)
	if err := os.WriteFile(dir+"/config.json", []byte(`{"auths":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	reg := newMemRegistry()
	defer reg.Close()
	img, err := PackStrategy(hostBinary(t), "hello", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, tag := range []string{"v1", "latest"} {
		if err := Push(ctx, img, Destination{Repository: reg.repo(), Tag: tag, PlainHTTP: true}); err != nil {
			t.Fatal(err)
		}
	}
	reg.tagPageSize = 1
	cat, err := OpenCatalog(Registry{Address: reg.address(), PlainHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	tags, err := cat.Tags(ctx, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if !sameStrings(tags, []string{"latest", "v1"}) {
		t.Fatalf("tags %q", tags)
	}
	got, err := cat.Resolve(ctx, "hello", "latest")
	if err != nil {
		t.Fatal(err)
	}
	if got != img.Digest {
		t.Fatalf("resolve %s, image %s", got, img.Digest)
	}
	info, err := cat.ReadManifest(ctx, "hello", got)
	if err != nil {
		t.Fatal(err)
	}
	if info.Digest != img.Digest {
		t.Fatalf("digest %s", info.Digest)
	}
	var want Manifest
	if err := json.Unmarshal(img.Config, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(info.Manifest, want) {
		t.Fatalf("manifest %+v, want %+v", info.Manifest, want)
	}
	var image ocispec.Manifest
	if err := json.Unmarshal(img.Manifest, &image); err != nil {
		t.Fatal(err)
	}
	var size int64
	size += image.Config.Size
	for _, layer := range image.Layers {
		size += layer.Size
	}
	if info.Size != size {
		t.Fatalf("size %d, want %d", info.Size, size)
	}
}

func TestCatalogTagPages(t *testing.T) {
	reg := newMemRegistry()
	defer reg.Close()
	reg.store("hello", "b", []byte("b"))
	reg.store("hello", "a", []byte("a"))
	reg.tagPageSize = 1
	cat, err := OpenCatalog(Registry{Address: reg.address(), PlainHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	tags, err := cat.Tags(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if !sameStrings(tags, []string{"a", "b"}) {
		t.Fatalf("tags %q", tags)
	}
}

func TestCatalogBasicAuth(t *testing.T) {
	reg := newMemRegistry()
	defer reg.Close()
	reg.user, reg.pass = "user", "secret"
	reg.store("hello", "v1", []byte("v1"))
	host := reg.address()
	raw, err := json.Marshal(map[string]any{
		"auths": map[string]any{
			host: map[string]string{"auth": base64.StdEncoding.EncodeToString([]byte(reg.user + ":" + reg.pass))},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/config.json"
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cat, err := OpenCatalog(Registry{Address: host, Credentials: path, PlainHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	tags, err := cat.Tags(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if !sameStrings(tags, []string{"v1"}) {
		t.Fatalf("tags %q", tags)
	}

	anon, err := OpenCatalog(Registry{Address: host, PlainHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = anon.Tags(context.Background(), "hello")
	if ReasonOf(err) != ReasonUnauthorized || !Permanent(err) {
		t.Fatalf("anonymous: %v", err)
	}
}

func TestCatalogRegistryErrors(t *testing.T) {
	reg := newMemRegistry()
	defer reg.Close()
	cat, err := OpenCatalog(Registry{Address: reg.address(), PlainHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, err = cat.Tags(ctx, "missing")
	if ReasonOf(err) != ReasonNotFound {
		t.Fatalf("missing repo: %v", err)
	}
	reg.status = http.StatusInternalServerError
	_, err = cat.Tags(ctx, "hello")
	if ReasonOf(err) != ReasonUnavailable || Permanent(err) {
		t.Fatalf("500: %v permanent=%v", err, Permanent(err))
	}
}

func TestCatalogManifestRejected(t *testing.T) {
	reg := newMemRegistry()
	defer reg.Close()
	reg.store("hello", "bad", []byte("{"))
	index := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{}]}`)
	reg.store("hello", "index", index)
	cat, err := OpenCatalog(Registry{Address: reg.address(), PlainHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, err = cat.ReadManifest(ctx, "hello", digest.FromBytes([]byte("{")).String())
	if ReasonOf(err) != ReasonBadManifest {
		t.Fatalf("bad json: %v", err)
	}
	_, err = cat.ReadManifest(ctx, "hello", digest.FromBytes(index).String())
	if ReasonOf(err) != ReasonNotImage {
		t.Fatalf("index: %v", err)
	}
	_, err = cat.Resolve(ctx, "hello", "no-such")
	if ReasonOf(err) != ReasonNotFound {
		t.Fatalf("missing tag: %v", err)
	}
}

func TestOpenCatalogRejects(t *testing.T) {
	_, err := OpenCatalog(Registry{Address: "http://127.0.0.1:5000"})
	if !errors.Is(err, ErrRequest) {
		t.Fatalf("scheme: %v", err)
	}
	_, err = OpenCatalog(Registry{Address: "127.0.0.1:5000/repo"})
	if !errors.Is(err, ErrRequest) {
		t.Fatalf("path: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := OpenCatalog(Registry{Address: "127.0.0.1:1", Credentials: t.TempDir() + "/missing.json"})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "credentials") {
			t.Fatalf("missing file: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OpenCatalog blocked; loading credentials must not dial")
	}
}

// TestCatalogReadData checks the catalog read (E09-D072, E10-D012). A strategy image is the wrong
// media type. A data artifact returns its config and does not download the layer.
func TestCatalogReadData(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DOCKER_CONFIG", dir)
	if err := os.WriteFile(dir+"/config.json", []byte(`{"auths":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	reg := newMemRegistry()
	defer reg.Close()
	img, err := PackStrategy(hostBinary(t), "hello", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := Push(ctx, img, Destination{Repository: reg.repo(), PlainHTTP: true}); err != nil {
		t.Fatal(err)
	}
	cat, err := OpenCatalog(Registry{Address: reg.address(), PlainHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = cat.ReadData(ctx, "hello", img.Digest)
	wantReason(t, err, ReasonWrongMediaType, true)

	c, err := Open(Registry{Address: reg.address(), PlainHTTP: true}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.WriteFile(root+"/note", []byte("n"), 0o644); err != nil {
		t.Fatal(err)
	}
	layer, _, err := c.PackDataFile(root)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(layer)
	pushed, err := c.PushData(ctx, DataPush{
		Repository: "state", Tag: "a1b2c3", LayerPath: layer,
		Source: DataSource{SessionID: "a1b2c3", Clean: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	info, err := cat.ReadData(ctx, "state", pushed.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if info.Digest != pushed.Digest || info.Config.Schema != DataSchema {
		t.Fatalf("data %+v", info)
	}
}

func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := map[string]int{}
	for _, s := range got {
		seen[s]++
	}
	for _, s := range want {
		seen[s]--
	}
	for _, n := range seen {
		if n != 0 {
			return false
		}
	}
	return true
}
