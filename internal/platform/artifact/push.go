package artifact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"
)

// Destination is where a strategy image is pushed (D936, D940).
type Destination struct {
	// Repository is registry/repository, for example 127.0.0.1:5000/hello.
	Repository string
	// Tag is optional. It is a name for people to look up; the manifest digest does not use it.
	Tag string
	// PlainHTTP speaks HTTP instead of HTTPS. The dev registry needs it (D910, D936).
	PlainHTTP bool
}

// Push uploads img. The manifest is checked with CheckLaunchFields first, and a failure is not
// uploaded (D941). Credentials come from Docker's config.json, ~/.docker/config.json by default
// (D940). On success the caller prints img.Digest.
func Push(ctx context.Context, img Image, dst Destination) error {
	var m Manifest
	if err := json.Unmarshal(img.Config, &m); err != nil {
		return fmt.Errorf("%w: config: %v", ErrBadManifest, err)
	}
	if err := m.CheckLaunchFields(); err != nil {
		return err
	}
	repo, err := remote.NewRepository(dst.Repository)
	if err != nil {
		return err
	}
	repo.PlainHTTP = dst.PlainHTTP
	client, err := dockerClient()
	if err != nil {
		return err
	}
	repo.Client = client

	config := content.NewDescriptorFromBytes(MediaTypeWorkerConfig, img.Config)
	layer := content.NewDescriptorFromBytes(ocispec.MediaTypeImageLayerGzip, img.Layer)
	manifest := content.NewDescriptorFromBytes(ocispec.MediaTypeImageManifest, img.Manifest)
	if got := manifest.Digest.String(); got != img.Digest {
		return fmt.Errorf("artifact: manifest digest is %s, Image.Digest is %s", got, img.Digest)
	}
	for _, blob := range []struct {
		desc ocispec.Descriptor
		body []byte
	}{
		{config, img.Config},
		{layer, img.Layer},
	} {
		if err := repo.Push(ctx, blob.desc, bytes.NewReader(blob.body)); err != nil {
			return err
		}
	}
	ref := manifest.Digest.String()
	if dst.Tag != "" {
		ref = dst.Tag
	}
	return repo.PushReference(ctx, manifest, bytes.NewReader(img.Manifest), ref)
}

func dockerClient() (remote.Client, error) {
	store, err := credentials.NewStoreFromDocker(credentials.StoreOptions{})
	if err != nil {
		return nil, fmt.Errorf("artifact: docker config: %w", err)
	}
	return &auth.Client{
		Client:     http.DefaultClient,
		Cache:      auth.NewCache(),
		Credential: credentials.Credential(store),
	}, nil
}
