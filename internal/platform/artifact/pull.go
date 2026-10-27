package artifact

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"
	"oras.land/oras-go/v2/registry/remote/errcode"
)

// MediaTypeConfig is the config blob of every worker artifact (PROTOCOL §8.1).
const MediaTypeConfig = "application/vnd.mftiko.worker.config.v1+json"

// DefaultMaxBytes is the total of the config and the layers when the deployment sets no limit (E09-D013).
const DefaultMaxBytes int64 = 1 << 30

// dockerMediaTypeManifestList is a multi-platform list, the same thing as an OCI index for this check (E09-D034).
const dockerMediaTypeManifestList = "application/vnd.docker.distribution.manifest.list.v2+json"

// Registry is how a client reaches a registry (E09-D010, E09-D033, E10-D122).
//
// Address, when set, is one host[:port] with no scheme. Repository names passed to this client are paths
// on that host, and PlainHTTP selects http for every one of them. That is the MD and TD worker client
// (E10-D126). An empty Address means each repository name is already host[:port]/path; the host is taken
// from that name (E10-D122). PlainHTTPHosts lists the hosts that use http. Credentials is one Docker
// config.json, read with oras-go's credentials store, or empty for anonymous. A host with no auth entry
// is anonymous (E10-D123). The file stays on the host: it is not in etcd or in a spec (E09-D010).
type Registry struct {
	Address        string
	Credentials    string
	PlainHTTP      bool
	PlainHTTPHosts []string
}

// Request is one worker artifact, addressed by the image manifest digest (E09-D011). OS and Arch default to
// the host. Contracts are the majors the caller needs; the manifest must contain every one of them (E09-D029).
// MaxBytes is the limit on the config plus the layers; zero means DefaultMaxBytes (E09-D013).
type Request struct {
	Repository string
	Digest     string
	Kind       string
	OS         string
	Arch       string
	Contracts  []string
	MaxBytes   int64
}

// Layer is one verified layer in the cache. Path is ${DATA}/layers/<digest>.
type Layer struct {
	Digest    string
	Size      int64
	MediaType string
	Path      string
}

// Fetched is a worker artifact that passed the checks. Layers are on disk, in manifest order; nothing
// has been unpacked.
type Fetched struct {
	Digest   string
	Manifest Manifest
	Layers   []Layer
}

// Client pulls artifacts into one data directory and unpacks them there.
type Client struct {
	reg    Registry
	data   string
	layers string
	http   *auth.Client

	mu   sync.Mutex
	held map[string]int // layer digests held across GC (E09-D073)
}

// Open checks the registry settings and drops temporary files left in ${DATA}/layers by a process that
// exited mid-download (E09-D030), and temporary directories left under ${DATA}/artifacts by a process that
// exited mid-unpack (E09-D055). dataDir is ${DATA}.
func Open(reg Registry, dataDir string) (*Client, error) {
	if err := prepareRegistry(reg); err != nil {
		return nil, err
	}
	if dataDir == "" {
		return nil, badRequest("data directory is required")
	}
	dir := layersDir(dataDir)
	if err := cleanTemps(dir); err != nil {
		return nil, fmt.Errorf("artifact: layers: %w", err)
	}
	if err := cleanTemps(filepath.Join(dataDir, "artifacts")); err != nil {
		return nil, fmt.Errorf("artifact: artifacts: %w", err)
	}
	ac, err := newAuthClient(reg)
	if err != nil {
		return nil, err
	}
	return &Client{reg: reg, data: dataDir, layers: dir, http: ac}, nil
}

// newAuthClient is the registry HTTP client (E09-D028, E09-D033). It does not retry, and it does not dial:
// a credentials file is only loaded. Catalog and Pull share it (E10-D056).
func newAuthClient(reg Registry) (*auth.Client, error) {
	ac := &auth.Client{
		// A plain client: the retrying client would repeat 5xx, and this package does not retry (E09-D028).
		Client: &http.Client{},
		Header: http.Header{"User-Agent": []string{"mftiko-artifact"}},
		Cache:  auth.NewCache(),
	}
	if reg.Credentials != "" {
		store, err := credentials.NewStore(reg.Credentials, credentials.StoreOptions{})
		if err != nil {
			return nil, fmt.Errorf("artifact: credentials: %w", err)
		}
		ac.Credential = credentials.Credential(store)
	}
	return ac, nil
}

func checkAddress(address string) error {
	if address == "" || strings.Contains(address, "://") || strings.Contains(address, "/") {
		return badRequest("registry address must be host[:port], not %q", address)
	}
	ref, err := registry.ParseReference(address + "/repo")
	if err != nil || ref.Registry != address || ref.Repository != "repo" {
		return badRequest("registry address must be host[:port], not %q", address)
	}
	return nil
}

// CheckHost reports whether address is host[:port], with no scheme and no path (E10-D122).
func CheckHost(address string) error {
	return checkAddress(address)
}

// SplitRepository splits a repository into its registry host and path (E10-D122).
// repository is host[:port]/path with no tag and no digest. The host is what credentials match.
func SplitRepository(repository string) (host, path string, err error) {
	ref, perr := registry.ParseReference(repository)
	if perr != nil || ref.Reference != "" || ref.Registry == "" || ref.Repository == "" {
		return "", "", badRequest("repository must be host[:port]/path, not %q", repository)
	}
	if err := checkAddress(ref.Registry); err != nil {
		return "", "", badRequest("repository must be host[:port]/path, not %q", repository)
	}
	return ref.Registry, ref.Repository, nil
}

// prepareRegistry checks Address, when set, and every plain-HTTP host (E10-D122, E10-D124).
// An empty Address is the full-reference client. Duplicate plain-HTTP hosts are rejected.
func prepareRegistry(reg Registry) error {
	if reg.Address != "" {
		if err := checkAddress(reg.Address); err != nil {
			return err
		}
	}
	seen := make(map[string]struct{}, len(reg.PlainHTTPHosts))
	for _, host := range reg.PlainHTTPHosts {
		if err := checkAddress(host); err != nil {
			return err
		}
		if _, ok := seen[host]; ok {
			return badRequest("duplicate plain http host %q", host)
		}
		seen[host] = struct{}{}
	}
	return nil
}

// Pull fetches the image manifest and config for req.Digest, checks them, and downloads any layer the
// cache does not already have (PROTOCOL §8.3 steps 1 and 2).
func (c *Client) Pull(ctx context.Context, req Request) (*Fetched, error) {
	if req.Repository == "" {
		return nil, badRequest("repository is required")
	}
	if req.Kind == "" {
		return nil, badRequest("kind is required")
	}
	want, err := digest.Parse(req.Digest)
	if err != nil || want.Algorithm() != digest.SHA256 {
		return nil, permanent(ReasonDigestMismatch, fmt.Errorf("digest %q is not sha256", req.Digest))
	}
	if req.OS == "" {
		req.OS = runtime.GOOS
	}
	if req.Arch == "" {
		req.Arch = runtime.GOARCH
	}
	maxBytes := req.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultMaxBytes
	}
	if maxBytes < 0 {
		return nil, badRequest("max bytes %d", maxBytes)
	}

	repo, err := c.repository(req.Repository)
	if err != nil {
		return nil, err
	}
	desc, rc, err := repo.FetchReference(ctx, want.String())
	if err != nil {
		return nil, classify(err)
	}
	raw, err := readVerified(rc, desc.Size, want)
	rc.Close()
	if err != nil {
		return nil, err
	}
	if desc.Digest != want {
		return nil, permanent(ReasonDigestMismatch, fmt.Errorf("registry returned %s for %s", desc.Digest, want))
	}

	manifest, err := workerManifest(raw, desc.MediaType)
	if err != nil {
		return nil, err
	}
	if err := checkDeclaredSize(manifest, maxBytes); err != nil {
		return nil, err
	}
	if manifest.Config.MediaType != MediaTypeConfig {
		return nil, permanent(ReasonWrongMediaType, fmt.Errorf("config media type %q", manifest.Config.MediaType))
	}

	cfg, err := c.fetchBlob(ctx, repo, manifest.Config)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(cfg, &m); err != nil {
		return nil, permanent(ReasonBadManifest, fmt.Errorf("config: %w", err))
	}
	if m.Schema != Schema {
		return nil, permanent(ReasonBadManifest, fmt.Errorf("schema %q", m.Schema))
	}
	if m.Kind != req.Kind {
		return nil, permanent(ReasonWrongKind, fmt.Errorf("kind %q, want %q", m.Kind, req.Kind))
	}
	if m.OS != req.OS || m.Arch != req.Arch {
		return nil, permanent(ReasonPlatformMismatch, fmt.Errorf("manifest is %s/%s, host wants %s/%s", m.OS, m.Arch, req.OS, req.Arch))
	}
	if !covers(m.Contracts, req.Contracts) {
		return nil, permanent(ReasonContractsMismatch, fmt.Errorf("manifest has %q, need %q", m.Contracts, req.Contracts))
	}

	layers := make([]Layer, len(manifest.Layers))
	for i, layer := range manifest.Layers {
		path, err := ensureLayer(ctx, repo, c.layers, layer)
		if err != nil {
			return nil, err
		}
		layers[i] = Layer{Digest: layer.Digest.String(), Size: layer.Size, MediaType: layer.MediaType, Path: path}
	}
	return &Fetched{Digest: want.String(), Manifest: m, Layers: layers}, nil
}

func (c *Client) repository(name string) (*remote.Repository, error) {
	return openRepository(c.reg, c.http, name)
}

func openRepository(reg Registry, client remote.Client, name string) (*remote.Repository, error) {
	ref := name
	host := reg.Address
	if reg.Address != "" {
		ref = reg.Address + "/" + name
	} else {
		h, _, err := SplitRepository(name)
		if err != nil {
			return nil, err
		}
		host = h
	}
	repo, err := remote.NewRepository(ref)
	if err != nil {
		return nil, badRequest("repository: %v", err)
	}
	repo.PlainHTTP = usesPlainHTTP(reg, host)
	repo.Client = client
	return repo, nil
}

// usesPlainHTTP is true for the single-address client when PlainHTTP is set, and for a host listed in
// PlainHTTPHosts (E10-D124). Other hosts stay on https.
func usesPlainHTTP(reg Registry, host string) bool {
	if reg.PlainHTTP && reg.Address != "" && host == reg.Address {
		return true
	}
	for _, h := range reg.PlainHTTPHosts {
		if h == host {
			return true
		}
	}
	return false
}

// readVerified reads r, stopping one byte past the declared size, and checks the sha256 (E09-D030, E09-D032).
// The deployment limit is the sum of the declared sizes, checked before any blob is fetched.
func readVerified(r io.Reader, size int64, want digest.Digest) ([]byte, error) {
	if size < 0 {
		return nil, permanent(ReasonBadManifest, fmt.Errorf("descriptor size %d", size))
	}
	var buf bytes.Buffer
	got, n, err := hashReader(&buf, r, size)
	if err != nil {
		return nil, err
	}
	if n > size {
		return nil, permanent(ReasonTooLarge, fmt.Errorf("read %d bytes, descriptor says %d", n, size))
	}
	if n != size || got != want {
		return nil, permanent(ReasonDigestMismatch, fmt.Errorf("%s: got %s, %d bytes", want, got, n))
	}
	return buf.Bytes(), nil
}

func (c *Client) fetchBlob(ctx context.Context, repo *remote.Repository, desc ocispec.Descriptor) ([]byte, error) {
	return fetchVerifiedBlob(ctx, repo, desc)
}

func fetchVerifiedBlob(ctx context.Context, repo *remote.Repository, desc ocispec.Descriptor) ([]byte, error) {
	rc, err := repo.Blobs().Fetch(ctx, desc)
	if err != nil {
		return nil, classify(err)
	}
	defer rc.Close()
	return readVerified(rc, desc.Size, desc.Digest)
}

// workerManifest accepts one OCI image manifest and nothing else (E09-D034).
func workerManifest(raw []byte, mediaType string) (ocispec.Manifest, error) {
	var shape struct {
		MediaType string          `json:"mediaType"`
		Config    json.RawMessage `json:"config"`
		Layers    json.RawMessage `json:"layers"`
		Manifests json.RawMessage `json:"manifests"`
	}
	if err := json.Unmarshal(raw, &shape); err != nil {
		return ocispec.Manifest{}, permanent(ReasonBadManifest, fmt.Errorf("manifest: %w", err))
	}
	if isIndex(mediaType) || isIndex(shape.MediaType) || len(shape.Manifests) > 0 {
		return ocispec.Manifest{}, permanent(ReasonNotImage, fmt.Errorf("digest points at %s, not one image manifest", firstNonEmpty(mediaType, shape.MediaType)))
	}
	mt := mediaType
	if mt == "" {
		mt = shape.MediaType
	}
	if mt != ocispec.MediaTypeImageManifest || (shape.MediaType != "" && shape.MediaType != ocispec.MediaTypeImageManifest) {
		return ocispec.Manifest{}, permanent(ReasonNotImage, fmt.Errorf("manifest media type %q", mt))
	}
	if len(shape.Config) == 0 || len(shape.Layers) == 0 {
		return ocispec.Manifest{}, permanent(ReasonBadManifest, errors.New("image manifest has no config or layers"))
	}
	var manifest ocispec.Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return ocispec.Manifest{}, permanent(ReasonBadManifest, fmt.Errorf("manifest: %w", err))
	}
	// schemaVersion 2 is the historical value every OCI image manifest carries.
	if manifest.SchemaVersion != 2 {
		return ocispec.Manifest{}, permanent(ReasonBadManifest, fmt.Errorf("schemaVersion %d", manifest.SchemaVersion))
	}
	return manifest, nil
}

func isIndex(mediaType string) bool {
	return mediaType == ocispec.MediaTypeImageIndex || mediaType == dockerMediaTypeManifestList
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func checkDeclaredSize(manifest ocispec.Manifest, maxBytes int64) error {
	total, err := declaredSize(manifest)
	if err != nil {
		return err
	}
	if total > maxBytes {
		return permanent(ReasonTooLarge, fmt.Errorf("config and layers are %d bytes, limit is %d", total, maxBytes))
	}
	return nil
}

// declaredSize is the config plus the layers, the same total Pull checks before downloading (E09-D013).
func declaredSize(manifest ocispec.Manifest) (int64, error) {
	if err := checkDescriptor(manifest.Config); err != nil {
		return 0, err
	}
	total := manifest.Config.Size
	for _, layer := range manifest.Layers {
		if err := checkDescriptor(layer); err != nil {
			return 0, err
		}
		if total > math.MaxInt64-layer.Size {
			return 0, permanent(ReasonTooLarge, errors.New("declared size overflows"))
		}
		total += layer.Size
	}
	return total, nil
}

func checkDescriptor(d ocispec.Descriptor) error {
	if d.Size < 0 {
		return permanent(ReasonBadManifest, fmt.Errorf("descriptor %s has size %d", d.Digest, d.Size))
	}
	if err := d.Digest.Validate(); err != nil || d.Digest.Algorithm() != digest.SHA256 {
		return permanent(ReasonDigestMismatch, fmt.Errorf("descriptor digest %q", d.Digest))
	}
	return nil
}

// covers reports whether have contains every element of need (E09-D029).
func covers(have, need []string) bool {
	if len(need) == 0 {
		return true
	}
	set := make(map[string]struct{}, len(have))
	for _, c := range have {
		set[c] = struct{}{}
	}
	for _, c := range need {
		if _, ok := set[c]; !ok {
			return false
		}
	}
	return true
}

// classify turns a registry client error into an Error (E09-D028). Caller cancellation is returned as itself.
func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var already *Error
	if errors.As(err, &already) {
		return err
	}
	if errors.Is(err, errdef.ErrNotFound) {
		return permanent(ReasonNotFound, err)
	}
	var resp *errcode.ErrorResponse
	if errors.As(err, &resp) {
		switch resp.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			return permanent(ReasonUnauthorized, err)
		case http.StatusNotFound:
			return permanent(ReasonNotFound, err)
		case http.StatusTooManyRequests:
			return temporary(ReasonUnavailable, err)
		default:
			if resp.StatusCode >= 500 {
				return temporary(ReasonUnavailable, err)
			}
			return permanent(ReasonUnavailable, err)
		}
	}
	var netErr net.Error
	var urlErr *url.Error
	if errors.As(err, &netErr) || errors.As(err, &urlErr) {
		return temporary(ReasonUnavailable, err)
	}
	msg := err.Error()
	if strings.Contains(msg, "digest mismatch") || strings.Contains(msg, "mismatch Content-Length") {
		return permanent(ReasonDigestMismatch, err)
	}
	// A token exchange that failed because the credentials were refused is permanent. A token endpoint
	// that could not be reached was already classified above.
	if errors.Is(err, auth.ErrBasicCredentialNotFound) || strings.Contains(msg, "basic credential not found") {
		return permanent(ReasonUnauthorized, err)
	}
	return temporary(ReasonUnavailable, err)
}
