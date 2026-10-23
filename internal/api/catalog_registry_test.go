package api

import (
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
)

// D1061: the catalog is checked against registry:2, the image CI already runs.
// A registry given by MFTIKO_REGISTRY is used as-is. Otherwise an already
// listening 127.0.0.1:5000 (the CI service) is used, and otherwise a container
// is started when docker is available. MFTIKO_REQUIRE_REGISTRY turns a missing
// registry into a failure.

const (
	envRegistry        = "MFTIKO_REGISTRY"
	envRequireRegistry = "MFTIKO_REQUIRE_REGISTRY"
)

var errNoRegistry = errors.New("registry:2 is not available")

var (
	regOnce      sync.Once
	regAddr      string
	regErr       error
	stopRegistry func()
)

func TestMain(m *testing.M) {
	code := m.Run()
	if stopRegistry != nil {
		stopRegistry()
	}
	os.Exit(code)
}

func devRegistry(t *testing.T) string {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("packing a strategy needs a linux ELF")
	}
	regOnce.Do(func() { regAddr, regErr = findRegistry() })
	if errors.Is(regErr, errNoRegistry) {
		if os.Getenv(envRequireRegistry) != "" {
			t.Fatalf("%s is set and %v", envRequireRegistry, regErr)
		}
		t.Skipf("%v: set %s, listen on 127.0.0.1:5000, or install docker", regErr, envRegistry)
	}
	if regErr != nil {
		t.Fatal(regErr)
	}
	return regAddr
}

func findRegistry() (string, error) {
	if addr := os.Getenv(envRegistry); addr != "" {
		if err := waitReady(addr, 30*time.Second); err != nil {
			return "", fmt.Errorf("%s: %w", envRegistry, err)
		}
		return addr, nil
	}
	if registryReady("127.0.0.1:5000") {
		return "127.0.0.1:5000", nil
	}
	if _, err := exec.LookPath("docker"); err != nil {
		return "", errNoRegistry
	}
	port, err := freePort()
	if err != nil {
		return "", err
	}
	name := "mftiko-catalog-" + port
	out, err := exec.Command("docker", "run", "-d", "--name", name, "-p", "127.0.0.1:"+port+":5000", "registry:2").CombinedOutput()
	stopRegistry = func() {
		_ = exec.Command("docker", "rm", "-f", name).Run()
	}
	if err != nil {
		return "", fmt.Errorf("docker run registry:2: %w\n%s", err, out)
	}
	addr := "127.0.0.1:" + port
	if err := waitReady(addr, 30*time.Second); err != nil {
		return "", err
	}
	return addr, nil
}

func freePort() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer ln.Close()
	_, port, err := net.SplitHostPort(ln.Addr().String())
	return port, err
}

func registryReady(addr string) bool {
	client := &http.Client{Timeout: 200 * time.Millisecond}
	resp, err := client.Get("http://" + addr + "/v2/")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// waitReady polls GET /v2/ until the registry answers 200. The interval is the
// sample period, not a bound; within is the bound.
func waitReady(addr string, within time.Duration) error {
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(within)
	var last error
	for {
		req, err := http.NewRequest(http.MethodGet, "http://"+addr+"/v2/", nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			err = fmt.Errorf("GET /v2/: %s", resp.Status)
		}
		last = err
		if !time.Now().Before(deadline) {
			return last
		}
		timer := time.NewTimer(pollInterval)
		<-timer.C
	}
}

var (
	helloOnce sync.Once
	helloBin  []byte
	helloErr  error
)

func helloBytes(t *testing.T) []byte {
	t.Helper()
	helloOnce.Do(func() {
		dir, err := os.MkdirTemp("", "mftiko-catalog-hello")
		if err != nil {
			helloErr = err
			return
		}
		path := filepath.Join(dir, "hello")
		_, file, _, ok := runtime.Caller(0)
		if !ok {
			helloErr = errors.New("no caller")
			return
		}
		cmd := exec.Command("go", "build", "-o", path, "github.com/BullionBear/mftiko/examples/strategies/hello")
		cmd.Dir = filepath.Join(filepath.Dir(file), "..", "..")
		out, err := cmd.CombinedOutput()
		if err != nil {
			helloErr = fmt.Errorf("build hello: %w\n%s", err, out)
			return
		}
		helloBin, helloErr = os.ReadFile(path)
	})
	if helloErr != nil {
		t.Fatal(helloErr)
	}
	return helloBin
}

func tinyELF() []byte {
	buf := make([]byte, 64)
	buf[0], buf[1], buf[2], buf[3] = 0x7f, 'E', 'L', 'F'
	buf[4] = byte(elf.ELFCLASS64)
	buf[5] = byte(elf.ELFDATA2LSB)
	buf[6] = 1
	buf[7] = byte(elf.ELFOSABI_NONE)
	binary.LittleEndian.PutUint16(buf[16:], uint16(elf.ET_EXEC))
	binary.LittleEndian.PutUint16(buf[18:], uint16(elf.EM_X86_64))
	binary.LittleEndian.PutUint32(buf[20:], 1)
	return buf
}

func TestCatalogRegistry2(t *testing.T) {
	addr := devRegistry(t)
	dir := t.TempDir()
	t.Setenv("DOCKER_CONFIG", dir)
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"auths":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	helloRepo := "mftiko/cat" + suffix
	connRepo := "mftiko/conn" + suffix

	img, err := artifact.PackStrategy(helloBytes(t), "hello", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, tag := range []string{"a", "latest", "v1"} {
		if err := artifact.Push(ctx, img, artifact.Destination{
			Repository: addr + "/" + helloRepo, Tag: tag, PlainHTTP: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	var want artifact.Manifest
	if err := json.Unmarshal(img.Config, &want); err != nil {
		t.Fatal(err)
	}
	var oci struct {
		Config struct {
			Size int64 `json:"size"`
		} `json:"config"`
		Layers []struct {
			Size int64 `json:"size"`
		} `json:"layers"`
	}
	if err := json.Unmarshal(img.Manifest, &oci); err != nil {
		t.Fatal(err)
	}
	size := oci.Config.Size
	for _, layer := range oci.Layers {
		size += layer.Size
	}

	cat, err := artifact.OpenCatalog(artifact.Registry{Address: addr, PlainHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{
		log: zerolog.Nop(),
		cfg: Config{
			Users:        []User{{Name: "ada", Token: sha256.Sum256([]byte(tokenAda))}},
			Repositories: []string{helloRepo, connRepo},
			Registry:     artifact.Registry{Address: addr, PlainHTTP: true},
		},
		catalog: cat,
	}
	h := s.routes()

	check := func(ref string) apiv1.StrategyVersion {
		t.Helper()
		code, hdr, body := doAuth(h, http.MethodGet, "/api/v1/strategies/cat"+suffix+"/versions/"+ref)
		if code != http.StatusOK || hdr.Get("Content-Type") != "application/json" {
			t.Fatalf("%s: %d %s", ref, code, body)
		}
		var got apiv1.StrategyVersion
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		if got.StrategyId != "cat"+suffix || got.Repository != addr+"/"+helloRepo || got.Digest != img.Digest ||
			got.Name != want.Name || got.Runtime != want.Runtime || got.Os != want.OS || got.Arch != want.Arch ||
			got.Sdk == nil || *got.Sdk != want.SDK.Lang+" "+want.SDK.Version ||
			strings.Join(got.Contracts, ",") != strings.Join(want.Contracts, ",") ||
			strings.Join(got.Tags, ",") != "a,latest,v1" || got.SizeBytes == nil || *got.SizeBytes != size {
			t.Fatalf("%s: %+v", ref, got)
		}
		return got
	}
	check("latest")
	check(img.Digest)

	var listed apiv1.StrategyTagList
	code, _, body := doAuth(h, http.MethodGet, "/api/v1/strategies/cat"+suffix+"/versions")
	if code != http.StatusOK {
		t.Fatalf("list %d %s", code, body)
	}
	if err := json.Unmarshal(body, &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Items) != 3 || listed.Items[0].Tag != "a" || listed.Items[1].Tag != "latest" || listed.Items[2].Tag != "v1" {
		t.Fatalf("tags %+v", listed.Items)
	}
	for _, item := range listed.Items {
		if item.Digest != img.Digest {
			t.Fatalf("tag %s digest %s", item.Tag, item.Digest)
		}
	}

	if err := artifact.Push(ctx, img, artifact.Destination{
		Repository: addr + "/" + helloRepo, Tag: "z", PlainHTTP: true,
	}); err != nil {
		t.Fatal(err)
	}
	code, _, body = doAuth(h, http.MethodGet, "/api/v1/strategies/cat"+suffix+"/versions")
	if code != http.StatusOK {
		t.Fatalf("list again %d %s", code, body)
	}
	if err := json.Unmarshal(body, &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Items) != 4 || listed.Items[3].Tag != "z" || listed.Items[3].Digest != img.Digest {
		t.Fatalf("after push %+v", listed.Items)
	}

	code, p := getProblem(t, h, "/api/v1/strategies/cat"+suffix+"/versions/no-such")
	if code != http.StatusNotFound || p.Code != apiv1.ErrorCodeNotFound {
		t.Fatalf("missing tag %d %+v", code, p)
	}
	missing := "sha256:" + strings.Repeat("b", 64)
	if missing == img.Digest {
		missing = "sha256:" + strings.Repeat("c", 64)
	}
	code, p = getProblem(t, h, "/api/v1/strategies/cat"+suffix+"/versions/"+missing)
	if code != http.StatusNotFound || p.Code != apiv1.ErrorCodeNotFound {
		t.Fatalf("missing digest %d %+v", code, p)
	}

	worker, err := artifact.PackWorker(tinyELF(), artifact.KindMDConn, "conn", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := artifact.Push(ctx, worker, artifact.Destination{
		Repository: addr + "/" + connRepo, Tag: "w", PlainHTTP: true,
	}); err != nil {
		t.Fatal(err)
	}
	code, p = getProblem(t, h, "/api/v1/strategies/conn"+suffix+"/versions/w")
	if code != http.StatusBadGateway || p.Code != apiv1.ErrorCodeInvalidArtifact || problemDetail(p) != "manifest is not a strategy artifact" {
		t.Fatalf("worker %d %+v", code, p)
	}
}
