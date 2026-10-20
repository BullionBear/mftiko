package artifact

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"debug/buildinfo"
	"debug/elf"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/BullionBear/mftiko/sdk"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/registry/remote"
)

func TestPackReproducible(t *testing.T) {
	bin := hostBinary(t)
	env := map[string]string{"B": "2", "A": "1"}
	a, err := PackStrategy(bin, "hello", env)
	if err != nil {
		t.Fatal(err)
	}
	b, err := PackStrategy(bin, "hello", map[string]string{"A": "1", "B": "2"})
	if err != nil {
		t.Fatal(err)
	}
	if a.Digest != b.Digest || !bytes.Equal(a.Manifest, b.Manifest) || !bytes.Equal(a.Config, b.Config) || !bytes.Equal(a.Layer, b.Layer) {
		t.Fatalf("two packs differ: %s %s", a.Digest, b.Digest)
	}
	if a.Digest != digest.FromBytes(a.Manifest).String() {
		t.Fatalf("digest %s is not the manifest", a.Digest)
	}
	if bytes.Contains(a.Manifest, []byte("org.opencontainers.image.created")) || bytes.Contains(a.Manifest, []byte(`"annotations"`)) {
		t.Fatalf("manifest carries annotations: %s", a.Manifest)
	}
	changed := append(bytes.Clone(bin), 0)
	c, err := PackStrategy(changed, "hello", env)
	if err != nil {
		t.Fatal(err)
	}
	if c.Digest == a.Digest {
		t.Fatal("a different binary packed to the same digest")
	}
	empty, err := PackStrategy(bin, "hello", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(empty.Config, []byte(`"env":{}`)) {
		t.Fatalf("empty env: %s", empty.Config)
	}
}

func TestPackLayerAndManifest(t *testing.T) {
	bin := hostBinary(t)
	img, err := PackStrategy(bin, "hello", map[string]string{"Z": "9", "A": "1"})
	if err != nil {
		t.Fatal(err)
	}
	if img.Layer[0] != 0x1f || img.Layer[1] != 0x8b || img.Layer[2] != 8 {
		t.Fatalf("layer is not gzip: %x", img.Layer[:10])
	}
	if img.Layer[3]&(0x08|0x04|0x10) != 0 {
		t.Fatalf("gzip header flags %02x carry a name, extra or comment", img.Layer[3])
	}
	if binary.LittleEndian.Uint32(img.Layer[4:8]) != 0 {
		t.Fatalf("gzip mtime %d", binary.LittleEndian.Uint32(img.Layer[4:8]))
	}
	if img.Layer[9] != 255 {
		t.Fatalf("gzip OS %d", img.Layer[9])
	}

	zr, err := gzip.NewReader(bytes.NewReader(img.Layer))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(zr)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Name != strategyEntrypoint || hdr.Mode != strategyMode || hdr.Uid != 0 || hdr.Gid != 0 || hdr.ModTime.Unix() != 0 {
		t.Fatalf("tar header name %q mode %#o uid %d gid %d mtime %d", hdr.Name, hdr.Mode, hdr.Uid, hdr.Gid, hdr.ModTime.Unix())
	}
	got, err := io.ReadAll(tr)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, bin) {
		t.Fatal("layer payload is not the binary")
	}
	if _, err := tr.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("extra tar member: %v", err)
	}

	var m Manifest
	if err := json.Unmarshal(img.Config, &m); err != nil {
		t.Fatal(err)
	}
	if m.Schema != Schema || m.Kind != KindStrategy || m.Name != "hello" || m.Runtime != "go" ||
		m.OS != runtime.GOOS || m.Arch != runtime.GOARCH || !slices.Equal(m.Entrypoint, []string{strategyEntrypoint}) ||
		m.SDK.Lang != "go" || !slices.Equal(m.Contracts, sdk.Contracts()) {
		t.Fatalf("manifest %+v", m)
	}
	if m.Env["A"] != "1" || m.Env["Z"] != "9" {
		t.Fatalf("env %v", m.Env)
	}
	if !bytes.Contains(img.Config, []byte(`"env":{"A":"1","Z":"9"}`)) {
		t.Fatalf("env JSON is not sorted: %s", img.Config)
	}
	info, err := buildinfo.Read(bytes.NewReader(bin))
	if err != nil {
		t.Fatal(err)
	}
	if m.SDK.Version != info.Main.Version || info.Main.Version == "" {
		t.Fatalf("sdk version %q, buildinfo %q", m.SDK.Version, info.Main.Version)
	}

	var oci struct {
		MediaType string `json:"mediaType"`
		Config    struct {
			MediaType string `json:"mediaType"`
			Digest    string `json:"digest"`
			Size      int64  `json:"size"`
		} `json:"config"`
		Layers []struct {
			MediaType string `json:"mediaType"`
			Digest    string `json:"digest"`
			Size      int64  `json:"size"`
		} `json:"layers"`
	}
	if err := json.Unmarshal(img.Manifest, &oci); err != nil {
		t.Fatal(err)
	}
	if oci.MediaType != ocispec.MediaTypeImageManifest || oci.Config.MediaType != MediaTypeWorkerConfig ||
		len(oci.Layers) != 1 || oci.Layers[0].MediaType != ocispec.MediaTypeImageLayerGzip {
		t.Fatalf("oci manifest %+v", oci)
	}
	if oci.Config.Digest != digest.FromBytes(img.Config).String() || oci.Config.Size != int64(len(img.Config)) ||
		oci.Layers[0].Digest != digest.FromBytes(img.Layer).String() || oci.Layers[0].Size != int64(len(img.Layer)) {
		t.Fatalf("descriptor digests do not match the blobs")
	}
}

func TestPackRefuses(t *testing.T) {
	bin := hostBinary(t)
	if _, err := PackStrategy(bin, "", nil); err == nil {
		t.Fatal("empty name packed")
	}
	if _, err := PackStrategy([]byte("not elf"), "hello", nil); !errors.Is(err, ErrPlatform) {
		t.Fatalf("non-ELF: %v", err)
	}
	if _, err := PackStrategy(bin, "hello", map[string]string{"MFTIKO_X": "1"}); !errors.Is(err, ErrBadManifest) {
		t.Fatalf("platform env: %v", err)
	}
	bare := elfIdent(elf.ELFCLASS64, elf.ELFDATA2LSB, elf.ELFOSABI_NONE, uint16(elf.EM_X86_64))
	if _, err := embeddedContracts(bare); !errors.Is(err, ErrNoContracts) {
		t.Fatalf("no marker: %v", err)
	}
	two := append(append([]byte{}, bare...), markerLine("worker.v1, md.v1")...)
	two = append(two, markerLine("td.v1, stsctl.v1")...)
	if _, err := embeddedContracts(two); !errors.Is(err, ErrContractsConflict) {
		t.Fatalf("two lists: %v", err)
	}
	same := append(append([]byte{}, bare...), marker...)
	same = append(same, marker...)
	got, err := embeddedContracts(same)
	if err != nil || !slices.Equal(got, sdk.Contracts()) {
		t.Fatalf("repeated marker %q %v", got, err)
	}
	noise := append(append([]byte{}, bare...), markerLine("not-a-list")...)
	noise = append(noise, marker...)
	got, err = embeddedContracts(noise)
	if err != nil || !slices.Equal(got, sdk.Contracts()) {
		t.Fatalf("noise before marker %q %v", got, err)
	}
}

func TestPackWorker(t *testing.T) {
	bare := elfIdent(elf.ELFCLASS64, elf.ELFDATA2LSB, elf.ELFOSABI_NONE, uint16(elf.EM_X86_64))
	bareImg, err := PackWorker(bare, KindMDConn, "mftiko-md-conn", nil)
	if err != nil {
		t.Fatal(err)
	}
	var bareManifest Manifest
	if err := json.Unmarshal(bareImg.Config, &bareManifest); err != nil {
		t.Fatal(err)
	}
	if bareManifest.OS != "linux" || bareManifest.Arch != "amd64" || !slices.Equal(bareManifest.Contracts, []string{"worker.v1", "mdctl.v1"}) || bareManifest.SDK != (SDK{}) {
		t.Fatalf("header-only ELF %+v", bareManifest)
	}
	if _, err := PackWorker(bare, "", "n", nil); err == nil {
		t.Fatal("empty kind packed")
	}
	if _, err := PackWorker(bare, "session", "n", nil); err == nil {
		t.Fatal("unknown kind packed")
	}
	if _, err := PackWorker(bare, KindMDConn, "", nil); err == nil {
		t.Fatal("empty name packed")
	}
	if _, err := PackWorker([]byte("not elf"), KindMDConn, "n", nil); !errors.Is(err, ErrPlatform) {
		t.Fatalf("non-ELF: %v", err)
	}
	if _, err := PackWorker(bare, KindTDAccount, "n", map[string]string{"MFTIKO_X": "1"}); !errors.Is(err, ErrBadManifest) {
		t.Fatalf("platform env: %v", err)
	}
	if _, ok := WorkerContracts(KindStrategy); ok {
		t.Fatal("strategy has a fixed worker contract list")
	}

	bin := hostBinary(t)
	img, err := PackWorker(bin, KindMDConn, "mftiko-md-conn", nil)
	if err != nil {
		t.Fatal(err)
	}
	again, err := PackWorker(bin, KindMDConn, "mftiko-md-conn", map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	if img.Digest != again.Digest || !bytes.Equal(img.Manifest, again.Manifest) || !bytes.Equal(img.Layer, again.Layer) {
		t.Fatal("two packs of one worker differ")
	}
	account, err := PackWorker(bin, KindTDAccount, "mftiko-td-account", nil)
	if err != nil {
		t.Fatal(err)
	}
	if account.Digest == img.Digest {
		t.Fatal("md.conn and td.account packed to the same digest")
	}
	strategy, err := PackStrategy(bin, "mftiko-md-conn", nil)
	if err != nil {
		t.Fatal(err)
	}
	if strategy.Digest == img.Digest {
		t.Fatal("strategy and md.conn packed to the same digest")
	}

	for _, tc := range []struct {
		img  Image
		kind string
		name string
		want []string
	}{
		{img, KindMDConn, "mftiko-md-conn", []string{"worker.v1", "mdctl.v1"}},
		{account, KindTDAccount, "mftiko-td-account", []string{"worker.v1", "tdctl.v1"}},
	} {
		var m Manifest
		if err := json.Unmarshal(tc.img.Config, &m); err != nil {
			t.Fatal(err)
		}
		listed, ok := WorkerContracts(tc.kind)
		if !ok || !slices.Equal(listed, tc.want) || !slices.Equal(m.Contracts, tc.want) {
			t.Fatalf("%s contracts manifest %v listed %v", tc.kind, m.Contracts, listed)
		}
		if m.Schema != Schema || m.Kind != tc.kind || m.Name != tc.name || m.Runtime != "go" ||
			m.OS != "linux" || m.Arch != runtime.GOARCH || !slices.Equal(m.Entrypoint, []string{workerEntrypoint}) ||
			m.SDK != (SDK{}) {
			t.Fatalf("%s manifest %+v", tc.kind, m)
		}
		if !bytes.Contains(tc.img.Config, []byte(`"sdk":{"lang":"","version":""}`)) {
			t.Fatalf("%s sdk JSON: %s", tc.kind, tc.img.Config)
		}
		if bytes.Contains(tc.img.Config, []byte("stsctl.v1")) || bytes.Contains(tc.img.Config, []byte(`"md.v1"`)) {
			t.Fatalf("%s used the strategy contract list: %s", tc.kind, tc.img.Config)
		}
		if slices.Equal(m.Contracts, sdk.Contracts()) {
			t.Fatalf("%s contracts are the SDK list", tc.kind)
		}
		zr, err := gzip.NewReader(bytes.NewReader(tc.img.Layer))
		if err != nil {
			t.Fatal(err)
		}
		tr := tar.NewReader(zr)
		hdr, err := tr.Next()
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Name != workerEntrypoint || hdr.Mode != strategyMode || hdr.Uid != 0 || hdr.Gid != 0 || hdr.ModTime.Unix() != 0 {
			t.Fatalf("tar header name %q mode %#o uid %d gid %d mtime %d", hdr.Name, hdr.Mode, hdr.Uid, hdr.Gid, hdr.ModTime.Unix())
		}
		got, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, bin) {
			t.Fatal("layer payload is not the binary")
		}
		if _, err := tr.Next(); !errors.Is(err, io.EOF) {
			t.Fatalf("extra tar member: %v", err)
		}
	}
}

// marker is the same text the SDK embeds. A second copy is one list, not a conflict.
const marker = "mftiko.contracts=worker.v1,stsctl.v1,md.v1,td.v1\n"

// markerLine builds a marker in memory. Spaces in spaced are removed here, so the
// source does not contain a second contracts list for the linker to keep.
func markerLine(spaced string) []byte {
	i := strings.IndexByte(marker, '=')
	out := append([]byte{}, marker[:i+1]...)
	out = append(out, strings.ReplaceAll(spaced, " ", "")...)
	out = append(out, '\n')
	return out
}

func TestELFPlatform(t *testing.T) {
	for _, tc := range []struct {
		class   elf.Class
		data    elf.Data
		abi     elf.OSABI
		machine elf.Machine
		osName  string
		arch    string
	}{
		{elf.ELFCLASS64, elf.ELFDATA2LSB, elf.ELFOSABI_NONE, elf.EM_X86_64, "linux", "amd64"},
		{elf.ELFCLASS64, elf.ELFDATA2LSB, elf.ELFOSABI_LINUX, elf.EM_AARCH64, "linux", "arm64"},
		{elf.ELFCLASS32, elf.ELFDATA2LSB, elf.ELFOSABI_NONE, elf.EM_386, "linux", "386"},
		{elf.ELFCLASS32, elf.ELFDATA2LSB, elf.ELFOSABI_NONE, elf.EM_ARM, "linux", "arm"},
		{elf.ELFCLASS64, elf.ELFDATA2LSB, elf.ELFOSABI_FREEBSD, elf.EM_X86_64, "freebsd", "amd64"},
		{elf.ELFCLASS64, elf.ELFDATA2MSB, elf.ELFOSABI_NONE, elf.EM_S390, "linux", "s390x"},
		{elf.ELFCLASS64, elf.ELFDATA2LSB, elf.ELFOSABI_NONE, elf.EM_PPC64, "linux", "ppc64le"},
	} {
		osName, arch, err := elfPlatform(elfIdent(tc.class, tc.data, tc.abi, uint16(tc.machine)))
		if err != nil || osName != tc.osName || arch != tc.arch {
			t.Errorf("%s/%s: got %s/%s %v", tc.osName, tc.arch, osName, arch, err)
		}
	}
	if _, _, err := elfPlatform(elfIdent(elf.ELFCLASS64, elf.ELFDATA2LSB, elf.ELFOSABI_NONE, 0)); !errors.Is(err, ErrPlatform) {
		t.Fatalf("unknown machine: %v", err)
	}
	if _, _, err := elfPlatform(elfIdent(elf.ELFCLASS32, elf.ELFDATA2LSB, elf.ELFOSABI_NONE, uint16(elf.EM_X86_64))); !errors.Is(err, ErrPlatform) {
		t.Fatalf("class mismatch: %v", err)
	}
}

func TestPushPull(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	bin := hostBinary(t)
	img, err := PackStrategy(bin, "hello", nil)
	if err != nil {
		t.Fatal(err)
	}
	reg := newMemRegistry()
	defer reg.Close()
	ctx := context.Background()
	dst := Destination{Repository: reg.repo(), PlainHTTP: true, Tag: "v1"}
	if err := Push(ctx, img, dst); err != nil {
		t.Fatal(err)
	}
	if err := Push(ctx, img, dst); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []string{img.Digest, "v1"} {
		manifest, config, layer := fetch(t, reg.repo(), ref)
		if !bytes.Equal(manifest, img.Manifest) || !bytes.Equal(config, img.Config) || !bytes.Equal(layer, img.Layer) {
			t.Fatalf("pull %s did not return the pushed bytes", ref)
		}
	}
}

func TestPackWorkerPush(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	bin := hostBinary(t)
	img, err := PackWorker(bin, KindMDConn, "mftiko-md-conn", nil)
	if err != nil {
		t.Fatal(err)
	}
	reg := newMemRegistry()
	defer reg.Close()
	ctx := context.Background()
	for _, tag := range []string{"ci", "latest"} {
		if err := Push(ctx, img, Destination{Repository: reg.repo(), PlainHTTP: true, Tag: tag}); err != nil {
			t.Fatal(err)
		}
	}
	for _, ref := range []string{img.Digest, "ci", "latest"} {
		manifest, config, layer := fetch(t, reg.repo(), ref)
		if !bytes.Equal(manifest, img.Manifest) || !bytes.Equal(config, img.Config) || !bytes.Equal(layer, img.Layer) {
			t.Fatalf("pull %s did not return the pushed bytes", ref)
		}
	}
}

func TestPushChecksBeforeUpload(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	reg := newMemRegistry()
	defer reg.Close()
	img := Image{Config: []byte(`{"entrypoint":["bin/strategy"],"env":{"MFTIKO_X":"1"}}`)}
	err := Push(context.Background(), img, Destination{Repository: reg.repo(), PlainHTTP: true})
	if !errors.Is(err, ErrBadManifest) {
		t.Fatalf("push: %v", err)
	}
	reg.mu.Lock()
	hits := reg.hits
	reg.mu.Unlock()
	if hits != 0 {
		t.Fatalf("invalid manifest was pushed, %d requests", hits)
	}
}

func TestPushBasicAuth(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DOCKER_CONFIG", dir)
	reg := newMemRegistry()
	defer reg.Close()
	reg.user, reg.pass = "user", "secret"
	host := reg.repo()
	host = host[:strings.LastIndex(host, "/")]
	raw, err := json.Marshal(map[string]any{
		"auths": map[string]any{
			host: map[string]string{"auth": base64.StdEncoding.EncodeToString([]byte(reg.user + ":" + reg.pass))},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/config.json", raw, 0o600); err != nil {
		t.Fatal(err)
	}
	bin := hostBinary(t)
	img, err := PackStrategy(bin, "hello", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Push(context.Background(), img, Destination{Repository: reg.repo(), PlainHTTP: true}); err != nil {
		t.Fatal(err)
	}
	reg.user, reg.pass = "", ""
	manifest, _, _ := fetch(t, reg.repo(), img.Digest)
	if !bytes.Equal(manifest, img.Manifest) {
		t.Fatal("authed push did not round-trip")
	}
}

func fetch(t *testing.T, repo, ref string) (manifest, config, layer []byte) {
	t.Helper()
	r, err := remote.NewRepository(repo)
	if err != nil {
		t.Fatal(err)
	}
	r.PlainHTTP = true
	r.Client = http.DefaultClient
	ctx := context.Background()
	_, rc, err := r.FetchReference(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err = io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	var oci ocispec.Manifest
	if err := json.Unmarshal(manifest, &oci); err != nil {
		t.Fatal(err)
	}
	config = fetchBlob(t, r, oci.Config.Digest.String())
	if len(oci.Layers) != 1 {
		t.Fatalf("layers %d", len(oci.Layers))
	}
	layer = fetchBlob(t, r, oci.Layers[0].Digest.String())
	return manifest, config, layer
}

func fetchBlob(t *testing.T, r *remote.Repository, ref string) []byte {
	t.Helper()
	_, rc, err := r.Blobs().FetchReference(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func hostBinary(t *testing.T) []byte {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("the host binary is an ELF strategy only on linux")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func elfIdent(class elf.Class, data elf.Data, abi elf.OSABI, machine uint16) []byte {
	n := 52
	if class == elf.ELFCLASS64 {
		n = 64
	}
	buf := make([]byte, n)
	buf[0], buf[1], buf[2], buf[3] = 0x7f, 'E', 'L', 'F'
	buf[4] = byte(class)
	buf[5] = byte(data)
	buf[6] = 1 // EV_CURRENT
	buf[7] = byte(abi)
	var order binary.ByteOrder = binary.LittleEndian
	if data == elf.ELFDATA2MSB {
		order = binary.BigEndian
	}
	order.PutUint16(buf[16:], uint16(elf.ET_EXEC))
	order.PutUint16(buf[18:], machine)
	order.PutUint32(buf[20:], 1)
	return buf
}
