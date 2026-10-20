package artifact

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"

	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote"
)

// DataPush is one data artifact upload (D979, D925, D933). Repository is the name
// without the registry host. Tag is the session id. Source is written into the config.
// LayerPath is the packed layer on disk; it is streamed, not loaded into memory.
type DataPush struct {
	Repository string
	Tag        string
	Source     DataSource
	LayerPath  string
}

// PushedData is a data manifest that landed in the registry.
// Digest is the manifest digest. Size is the compressed layer in bytes.
type PushedData struct {
	Digest string
	Size   int64
}

// PushData streams one data artifact (D979). Credentials and plain HTTP are the
// client's registry settings, the same ones a pull uses (D933). The manifest is tagged
// with Tag and is also addressable by its digest.
func (c *Client) PushData(ctx context.Context, req DataPush) (PushedData, error) {
	if req.Repository == "" {
		return PushedData{}, badRequest("repository is required")
	}
	if req.Tag == "" {
		return PushedData{}, badRequest("tag is required")
	}
	if req.LayerPath == "" {
		return PushedData{}, badRequest("layer is required")
	}
	f, err := os.Open(req.LayerPath)
	if err != nil {
		return PushedData{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return PushedData{}, err
	}
	if !st.Mode().IsRegular() {
		return PushedData{}, badRequest("layer is not a file")
	}
	sum, err := digest.FromReader(f)
	if err != nil {
		return PushedData{}, err
	}
	end, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return PushedData{}, err
	}
	if end != st.Size() {
		return PushedData{}, fmtLayerChanged(req.LayerPath)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return PushedData{}, err
	}
	cfg, err := json.Marshal(DataConfig{Schema: DataSchema, Source: &req.Source})
	if err != nil {
		return PushedData{}, err
	}
	cfgDesc := ocispec.Descriptor{
		MediaType: MediaTypeDataConfig, Digest: digest.FromBytes(cfg), Size: int64(len(cfg)),
	}
	layerDesc := ocispec.Descriptor{
		MediaType: MediaTypeDataLayer, Digest: sum, Size: st.Size(),
	}
	raw, err := json.Marshal(ocispec.Manifest{
		Versioned:    specs.Versioned{SchemaVersion: 2},
		MediaType:    ocispec.MediaTypeImageManifest,
		ArtifactType: MediaTypeDataArtifact,
		Config:       cfgDesc,
		Layers:       []ocispec.Descriptor{layerDesc},
	})
	if err != nil {
		return PushedData{}, err
	}
	manDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest, Digest: digest.FromBytes(raw), Size: int64(len(raw)),
	}
	repo, err := c.repository(req.Repository)
	if err != nil {
		return PushedData{}, err
	}
	if err := pushBlob(ctx, repo, cfgDesc, bytes.NewReader(cfg)); err != nil {
		return PushedData{}, err
	}
	if err := pushBlob(ctx, repo, layerDesc, f); err != nil {
		return PushedData{}, err
	}
	if err := pushBlobRef(ctx, repo, manDesc, bytes.NewReader(raw), req.Tag); err != nil {
		return PushedData{}, err
	}
	return PushedData{Digest: manDesc.Digest.String(), Size: st.Size()}, nil
}

func fmtLayerChanged(path string) error {
	return badRequest("layer %s changed while hashing", path)
}

func pushBlob(ctx context.Context, repo *remote.Repository, desc ocispec.Descriptor, r io.Reader) error {
	err := repo.Push(ctx, desc, r)
	if err == nil || errors.Is(err, errdef.ErrAlreadyExists) {
		return nil
	}
	return classify(err)
}

func pushBlobRef(ctx context.Context, repo *remote.Repository, desc ocispec.Descriptor, r io.Reader, ref string) error {
	err := repo.PushReference(ctx, desc, r, ref)
	if err == nil || errors.Is(err, errdef.ErrAlreadyExists) {
		return nil
	}
	return classify(err)
}
