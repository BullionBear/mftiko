package artifact

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// Media types of a data artifact (PROTOCOL §8.4, E09-D072).
const (
	MediaTypeDataArtifact = "application/vnd.mftiko.data.v1"
	MediaTypeDataConfig   = "application/vnd.mftiko.data.config.v1+json"
	MediaTypeDataLayer    = "application/vnd.mftiko.data.layer.v1.tar+gzip"
)

// DataSchema is the config document's schema (PROTOCOL §8.4).
const DataSchema = "mftiko.data.v1"

// DataRequest is one data artifact, addressed by the manifest digest (E09-D004, E09-D072).
// MaxBytes limits the config plus the layer the way a worker pull does; zero means DefaultMaxBytes.
// OnLayers, when set, runs after the manifest and config have been checked and before the layer
// download, so the caller can hold those digests across GC (E09-D073).
type DataRequest struct {
	Repository string
	Digest     string
	MaxBytes   int64
	OnLayers   func(digests []string) error
}

// DataSource is the optional record of which session produced an uploaded volume (PROTOCOL §8.4).
// An artifact an operator uploaded has no source.
type DataSource struct {
	SessionID   string `json:"session_id,omitempty"`
	Incarnation uint32 `json:"incarnation,omitempty"`
	Clean       bool   `json:"clean,omitempty"`
}

// DataConfig is the config blob of a data artifact.
type DataConfig struct {
	Schema string      `json:"schema"`
	Source *DataSource `json:"source,omitempty"`
}

// DataFetched is a data artifact that passed the checks. Layer is on disk. Nothing has been unpacked.
type DataFetched struct {
	Digest string
	Config DataConfig
	Layer  Layer
}

// PullData fetches one data artifact (E09-D072). It checks artifactType, the config media type, and that
// there is a single layer of the data media type. Download, verification, the layer cache, and
// single-flight are the worker pull's. The download cap is MaxBytes, the same deployment limit as a
// strategy pull (E09-D013, default DefaultMaxBytes). The unpack that follows uses the deployment's
// UnpackLimits (E09-D053, E15-D012); a zero value is 4 GiB and 100,000 files.
func (c *Client) PullData(ctx context.Context, req DataRequest) (*DataFetched, error) {
	if req.Repository == "" {
		return nil, badRequest("repository is required")
	}
	want, err := digest.Parse(req.Digest)
	if err != nil || want.Algorithm() != digest.SHA256 {
		return nil, permanent(ReasonDigestMismatch, fmt.Errorf("digest %q is not sha256", req.Digest))
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
	manifest, err := dataManifest(raw, desc.MediaType)
	if err != nil {
		return nil, err
	}
	if err := checkDeclaredSize(manifest, maxBytes); err != nil {
		return nil, err
	}
	cfg, err := c.fetchBlob(ctx, repo, manifest.Config)
	if err != nil {
		return nil, err
	}
	var parsed DataConfig
	if err := json.Unmarshal(cfg, &parsed); err != nil {
		return nil, permanent(ReasonBadManifest, fmt.Errorf("config: %w", err))
	}
	if parsed.Schema != DataSchema {
		return nil, permanent(ReasonBadManifest, fmt.Errorf("schema %q", parsed.Schema))
	}
	layer := manifest.Layers[0]
	if req.OnLayers != nil {
		if err := req.OnLayers([]string{layer.Digest.String()}); err != nil {
			return nil, err
		}
	}
	path, err := ensureLayer(ctx, repo, c.layers, layer)
	if err != nil {
		return nil, err
	}
	return &DataFetched{
		Digest: want.String(),
		Config: parsed,
		Layer:  Layer{Digest: layer.Digest.String(), Size: layer.Size, MediaType: layer.MediaType, Path: path},
	}, nil
}

// dataManifest accepts one OCI image manifest that is a mftiko data artifact (E09-D072, E09-D034).
func dataManifest(raw []byte, mediaType string) (ocispec.Manifest, error) {
	manifest, err := workerManifest(raw, mediaType)
	if err != nil {
		return ocispec.Manifest{}, err
	}
	if manifest.ArtifactType != MediaTypeDataArtifact {
		return ocispec.Manifest{}, permanent(ReasonWrongMediaType, fmt.Errorf("artifactType %q", manifest.ArtifactType))
	}
	if manifest.Config.MediaType != MediaTypeDataConfig {
		return ocispec.Manifest{}, permanent(ReasonWrongMediaType, fmt.Errorf("config media type %q", manifest.Config.MediaType))
	}
	if len(manifest.Layers) != 1 {
		return ocispec.Manifest{}, permanent(ReasonBadManifest, fmt.Errorf("%d layers, want 1", len(manifest.Layers)))
	}
	if manifest.Layers[0].MediaType != MediaTypeDataLayer {
		return ocispec.Manifest{}, permanent(ReasonWrongMediaType, fmt.Errorf("layer media type %q", manifest.Layers[0].MediaType))
	}
	return manifest, nil
}
