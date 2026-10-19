package artifact

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func dataConfig(t *testing.T, source bool) []byte {
	t.Helper()
	cfg := DataConfig{Schema: DataSchema}
	if source {
		cfg.Source = &DataSource{SessionID: "s1", Incarnation: 2, Clean: true}
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func dataLayer(t *testing.T, name, body string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	if err := tw.WriteHeader(&tar.Header{
		Name: name, Mode: 0o644, Typeflag: tar.TypeReg, Size: int64(len(body)), Format: tar.FormatPAX,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: "dir", Mode: 0o755, Typeflag: tar.TypeDir, Format: tar.FormatPAX}); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func (s *fakeReg) dataArtifact(t *testing.T, cfg, layer []byte, artifactType, configType, layerType string) string {
	t.Helper()
	if artifactType == "" {
		artifactType = MediaTypeDataArtifact
	}
	if configType == "" {
		configType = MediaTypeDataConfig
	}
	if layerType == "" {
		layerType = MediaTypeDataLayer
	}
	config := s.put(configType, cfg)
	var layers []ocispec.Descriptor
	if layer != nil {
		layers = []ocispec.Descriptor{s.put(layerType, layer)}
	}
	return s.publish(ocispec.Manifest{
		Versioned:    specs.Versioned{SchemaVersion: 2},
		MediaType:    ocispec.MediaTypeImageManifest,
		ArtifactType: artifactType,
		Config:       config,
		Layers:       layers,
	})
}

func TestPullData(t *testing.T) {
	s := newFake(t)
	layer := dataLayer(t, "note.txt", "hello")
	id := s.dataArtifact(t, dataConfig(t, true), layer, "", "", "")
	c, data := s.open(t, "")
	got, err := c.PullData(context.Background(), DataRequest{Repository: "mftiko/data", Digest: id})
	if err != nil {
		t.Fatal(err)
	}
	if got.Digest != id || got.Config.Schema != DataSchema || got.Config.Source == nil || got.Config.Source.SessionID != "s1" || !got.Config.Source.Clean {
		t.Fatalf("fetched %+v", got.Config)
	}
	if got.Layer.MediaType != MediaTypeDataLayer || got.Layer.Path != filepath.Join(data, "layers", got.Layer.Digest) {
		t.Fatalf("layer %+v", got.Layer)
	}
	dest := filepath.Join(data, "out")
	if err := Unpack(context.Background(), []string{got.Layer.Path}, dest, UnpackLimits{}); err != nil {
		t.Fatal(err)
	}
	if string(mustRead(t, filepath.Join(dest, "note.txt"))) != "hello" {
		t.Fatal("unpacked body")
	}
	again, err := c.PullData(context.Background(), DataRequest{Repository: "mftiko/data", Digest: id})
	if err != nil || again.Layer.Path != got.Layer.Path {
		t.Fatalf("second pull %v %+v", err, again)
	}
}

func TestPullDataRejects(t *testing.T) {
	s := newFake(t)
	layer := dataLayer(t, "a", "x")
	cfg := dataConfig(t, false)
	good := s.dataArtifact(t, cfg, layer, "", "", "")
	c, _ := s.open(t, "")
	ctx := context.Background()
	req := func(id string) DataRequest {
		return DataRequest{Repository: "mftiko/data", Digest: id}
	}

	_, err := c.PullData(ctx, DataRequest{Digest: good})
	if !errors.Is(err, ErrRequest) {
		t.Fatalf("empty repository: %v", err)
	}
	_, err = c.PullData(ctx, DataRequest{Repository: "mftiko/data", Digest: good, MaxBytes: -1})
	if !errors.Is(err, ErrRequest) {
		t.Fatalf("negative max: %v", err)
	}
	_, err = c.PullData(ctx, DataRequest{Repository: "mftiko/data", Digest: "nope"})
	wantReason(t, err, ReasonDigestMismatch, true)

	wrongType := s.dataArtifact(t, cfg, layer, "application/vnd.mftiko.data.v2", "", "")
	_, err = c.PullData(ctx, req(wrongType))
	wantReason(t, err, ReasonWrongMediaType, true)

	wrongCfg := s.dataArtifact(t, cfg, layer, "", "application/vnd.mftiko.worker.config.v1+json", "")
	_, err = c.PullData(ctx, req(wrongCfg))
	wantReason(t, err, ReasonWrongMediaType, true)

	wrongLayer := s.dataArtifact(t, cfg, layer, "", "", ocispec.MediaTypeImageLayerGzip)
	_, err = c.PullData(ctx, req(wrongLayer))
	wantReason(t, err, ReasonWrongMediaType, true)

	two := s.publish(ocispec.Manifest{
		Versioned:    specs.Versioned{SchemaVersion: 2},
		MediaType:    ocispec.MediaTypeImageManifest,
		ArtifactType: MediaTypeDataArtifact,
		Config:       s.put(MediaTypeDataConfig, cfg),
		Layers:       []ocispec.Descriptor{s.put(MediaTypeDataLayer, layer), s.put(MediaTypeDataLayer, layer)},
	})
	_, err = c.PullData(ctx, req(two))
	wantReason(t, err, ReasonBadManifest, true)

	badSchema := s.dataArtifact(t, []byte(`{"schema":"other"}`), layer, "", "", "")
	_, err = c.PullData(ctx, req(badSchema))
	wantReason(t, err, ReasonBadManifest, true)

	plain := s.dataArtifact(t, cfg, layer, "", "", "")
	got, err := c.PullData(ctx, req(plain))
	if err != nil || got.Config.Source != nil {
		t.Fatalf("no source: %v %+v", err, got)
	}

	_, err = c.PullData(ctx, DataRequest{Repository: "mftiko/data", Digest: good, MaxBytes: 1})
	wantReason(t, err, ReasonTooLarge, true)

	_, err = c.PullData(ctx, DataRequest{Repository: "mftiko/data", Digest: "sha256:" + strings.Repeat("ab", 32)})
	wantReason(t, err, ReasonNotFound, true)

	held := false
	_, err = c.PullData(ctx, DataRequest{
		Repository: "mftiko/data", Digest: plain, OnLayers: func(digests []string) error {
			held = len(digests) == 1 && digestOK(digests[0])
			return nil
		},
	})
	if err != nil || !held {
		t.Fatalf("on layers %v held %v", err, held)
	}
}
