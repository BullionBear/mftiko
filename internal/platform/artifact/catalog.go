package artifact

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/opencontainers/go-digest"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
)

// Catalog reads a registry without storing a catalog and without a data directory (E10-D056, E10-D013).
// Tags, Resolve, ReadManifest and ReadData are read-only. They do not download layers and do not
// check os/arch: the catalog is not a spawn. Pull and PullData are unchanged.
type Catalog struct {
	reg  Registry
	http *auth.Client
}

// ManifestInfo is one image manifest and the worker manifest in its config blob.
// Size is the declared sum of the config and the layers (E09-D013). Layers are not downloaded.
type ManifestInfo struct {
	Digest   string
	Manifest Manifest
	Size     int64
}

// OpenCatalog checks the registry address and, when set, loads the Docker config.json (E09-D033, E10-D060).
// It does not dial the registry and does not create a data directory.
func OpenCatalog(reg Registry) (*Catalog, error) {
	if err := prepareRegistry(reg); err != nil {
		return nil, err
	}
	// A set path has to name a file. NewStore treats a missing file as anonymous, which would
	// hide a wrong path; the catalog checks it here. Open (the pull client) is unchanged (E10-D056, E10-D060).
	if reg.Credentials != "" {
		info, err := os.Stat(reg.Credentials)
		if err != nil {
			return nil, fmt.Errorf("artifact: credentials: %w", err)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("artifact: credentials: %s is not a file", reg.Credentials)
		}
	}
	ac, err := newAuthClient(reg)
	if err != nil {
		return nil, err
	}
	return &Catalog{reg: reg, http: ac}, nil
}

// Reachable reports whether GET /v2/ on the registry returns 200 (E12-D038). A nil catalog,
// a missing client, or an empty address is not configured. Any other final status, including
// 401, is an error. The body is discarded and is not part of the error.
func (c *Catalog) Reachable(ctx context.Context) error {
	if c == nil || c.http == nil || c.reg.Address == "" {
		return errors.New("artifact: registry is not configured")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, registryPingURL(c.reg), nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, copyErr := io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("artifact: registry status %d", resp.StatusCode)
	}
	return copyErr
}

func registryPingURL(reg Registry) string {
	scheme := "https"
	if reg.PlainHTTP {
		scheme = "http"
	}
	return scheme + "://" + reg.Address + "/v2/"
}

func (c *Catalog) repository(name string) (*remote.Repository, error) {
	if name == "" {
		return nil, badRequest("repository is required")
	}
	return openRepository(c.reg, c.http, name)
}

// Tags returns every tag in repository. The client follows the registry's pages (E10-D058).
// The order is the registry's order. An empty repository is an empty slice.
func (c *Catalog) Tags(ctx context.Context, repository string) ([]string, error) {
	repo, err := c.repository(repository)
	if err != nil {
		return nil, err
	}
	var tags []string
	err = repo.Tags(ctx, "", func(page []string) error {
		tags = append(tags, page...)
		return nil
	})
	if err != nil {
		return nil, classify(err)
	}
	if tags == nil {
		tags = []string{}
	}
	return tags, nil
}

// Resolve returns the sha256 digest reference names. reference is a tag or a digest (E09-D034).
func (c *Catalog) Resolve(ctx context.Context, repository, reference string) (string, error) {
	if reference == "" {
		return "", badRequest("reference is required")
	}
	repo, err := c.repository(repository)
	if err != nil {
		return "", err
	}
	return resolveReference(ctx, repo, reference)
}

// Resolve returns the sha256 digest reference names, using the pull client's registry settings (E12-D012).
// The background latest poll calls it. Reconcile does not.
func (c *Client) Resolve(ctx context.Context, repository, reference string) (string, error) {
	if repository == "" {
		return "", badRequest("repository is required")
	}
	repo, err := c.repository(repository)
	if err != nil {
		return "", err
	}
	return resolveReference(ctx, repo, reference)
}

func resolveReference(ctx context.Context, repo *remote.Repository, reference string) (string, error) {
	if reference == "" {
		return "", badRequest("reference is required")
	}
	desc, err := repo.Resolve(ctx, reference)
	if err != nil {
		return "", classify(err)
	}
	if desc.Digest.Algorithm() != digest.SHA256 {
		return "", permanent(ReasonDigestMismatch, fmt.Errorf("digest %q is not sha256", desc.Digest))
	}
	return desc.Digest.String(), nil
}

// ReadManifest fetches the image manifest and its config blob for digest. It accepts one OCI image
// manifest, the same check Pull makes (E09-D034), and parses the worker manifest with the same parser.
// It does not download layers, and it does not check kind, os/arch or contracts (E10-D056).
func (c *Catalog) ReadManifest(ctx context.Context, repository, digestStr string) (ManifestInfo, error) {
	want, err := digest.Parse(digestStr)
	if err != nil || want.Algorithm() != digest.SHA256 {
		return ManifestInfo{}, permanent(ReasonDigestMismatch, fmt.Errorf("digest %q is not sha256", digestStr))
	}
	repo, err := c.repository(repository)
	if err != nil {
		return ManifestInfo{}, err
	}
	desc, rc, err := repo.FetchReference(ctx, want.String())
	if err != nil {
		return ManifestInfo{}, classify(err)
	}
	raw, err := readVerified(rc, desc.Size, want)
	rc.Close()
	if err != nil {
		return ManifestInfo{}, err
	}
	if desc.Digest != want {
		return ManifestInfo{}, permanent(ReasonDigestMismatch, fmt.Errorf("registry returned %s for %s", desc.Digest, want))
	}
	image, err := workerManifest(raw, desc.MediaType)
	if err != nil {
		return ManifestInfo{}, err
	}
	size, err := declaredSize(image)
	if err != nil {
		return ManifestInfo{}, err
	}
	if image.Config.MediaType != MediaTypeConfig {
		return ManifestInfo{}, permanent(ReasonWrongMediaType, fmt.Errorf("config media type %q", image.Config.MediaType))
	}
	cfg, err := fetchVerifiedBlob(ctx, repo, image.Config)
	if err != nil {
		return ManifestInfo{}, err
	}
	var m Manifest
	if err := json.Unmarshal(cfg, &m); err != nil {
		return ManifestInfo{}, permanent(ReasonBadManifest, fmt.Errorf("config: %w", err))
	}
	if m.Schema != Schema {
		return ManifestInfo{}, permanent(ReasonBadManifest, fmt.Errorf("schema %q", m.Schema))
	}
	return ManifestInfo{Digest: want.String(), Manifest: m, Size: size}, nil
}

// DataInfo is a data artifact's manifest digest and config (E09-D072, E10-D012). The layer is not downloaded.
// Size is the config plus the layer as the manifest declares them, the total a pull checks (E09-D013, E15-D006).
type DataInfo struct {
	Digest string
	Config DataConfig
	Size   int64
}

// ReadData fetches the image manifest and its config for digest and checks that they are a mftiko
// data artifact (E09-D072). It does not download the layer and does not apply PullData's size cap.
// PullData is unchanged.
func (c *Catalog) ReadData(ctx context.Context, repository, digestStr string) (DataInfo, error) {
	want, err := digest.Parse(digestStr)
	if err != nil || want.Algorithm() != digest.SHA256 {
		return DataInfo{}, permanent(ReasonDigestMismatch, fmt.Errorf("digest %q is not sha256", digestStr))
	}
	repo, err := c.repository(repository)
	if err != nil {
		return DataInfo{}, err
	}
	desc, rc, err := repo.FetchReference(ctx, want.String())
	if err != nil {
		return DataInfo{}, classify(err)
	}
	raw, err := readVerified(rc, desc.Size, want)
	rc.Close()
	if err != nil {
		return DataInfo{}, err
	}
	if desc.Digest != want {
		return DataInfo{}, permanent(ReasonDigestMismatch, fmt.Errorf("registry returned %s for %s", desc.Digest, want))
	}
	manifest, err := dataManifest(raw, desc.MediaType)
	if err != nil {
		return DataInfo{}, err
	}
	size, err := declaredSize(manifest)
	if err != nil {
		return DataInfo{}, err
	}
	cfg, err := fetchVerifiedBlob(ctx, repo, manifest.Config)
	if err != nil {
		return DataInfo{}, err
	}
	var parsed DataConfig
	if err := json.Unmarshal(cfg, &parsed); err != nil {
		return DataInfo{}, permanent(ReasonBadManifest, fmt.Errorf("config: %w", err))
	}
	if parsed.Schema != DataSchema {
		return DataInfo{}, permanent(ReasonBadManifest, fmt.Errorf("schema %q", parsed.Schema))
	}
	return DataInfo{Digest: want.String(), Config: parsed, Size: size}, nil
}
