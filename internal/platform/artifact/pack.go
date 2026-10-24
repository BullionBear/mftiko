package artifact

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
)

// MediaTypeWorkerConfig is the config blob of a worker artifact (PROTOCOL §8.1).
const MediaTypeWorkerConfig = "application/vnd.mftiko.worker.config.v1+json"

const (
	strategyEntrypoint = "bin/strategy"
	workerEntrypoint   = "bin/worker"
	strategyMode       = 0o755
	// layerCompression is fixed so two packs of one binary match (E09-D038).
	layerCompression = gzip.DefaultCompression
	sdkModule        = "github.com/BullionBear/mftiko"
)

// ErrNoSDKVersion means the binary's buildinfo has no mftiko module version (E09-D002).
var ErrNoSDKVersion = errors.New("artifact: no sdk version in buildinfo")

// Image is a packed worker artifact: the OCI image manifest and the two blobs it names.
// Digest is the manifest digest. For a strategy that digest is StrategyRef.code (PROTOCOL §8.1, E09-D036).
type Image struct {
	Digest   string
	Manifest []byte
	Config   []byte
	Layer    []byte
}

// PackStrategy packs a Go strategy binary into one reproducible OCI image (E09-D002, E09-D035–E09-D038).
// It does not push. env may be nil. The manifest is checked with CheckLaunchFields before the
// image is built (E09-D041).
func PackStrategy(binary []byte, name string, env map[string]string) (Image, error) {
	if name == "" {
		return Image{}, errors.New("artifact: name is empty")
	}
	osName, arch, err := elfPlatform(binary)
	if err != nil {
		return Image{}, err
	}
	contracts, err := embeddedContracts(binary)
	if err != nil {
		return Image{}, err
	}
	version, err := sdkVersion(binary)
	if err != nil {
		return Image{}, err
	}
	if env == nil {
		env = map[string]string{}
	}
	m := Manifest{
		Schema:     Schema,
		Kind:       KindStrategy,
		Name:       name,
		Runtime:    "go",
		OS:         osName,
		Arch:       arch,
		Entrypoint: []string{strategyEntrypoint},
		Env:        env,
		SDK:        SDK{Lang: "go", Version: version},
		Contracts:  contracts,
	}
	if err := m.CheckLaunchFields(); err != nil {
		return Image{}, err
	}
	config, err := marshalManifest(m)
	if err != nil {
		return Image{}, err
	}
	layer, err := strategyLayer(binary)
	if err != nil {
		return Image{}, err
	}
	manifest, err := imageManifest(config, layer)
	if err != nil {
		return Image{}, err
	}
	return Image{
		Digest:   digest.FromBytes(manifest).String(),
		Manifest: manifest,
		Config:   config,
		Layer:    layer,
	}, nil
}

// WorkerContracts is the fixed contract list pack writes for a non-strategy kind (E09-D089).
// strategy is not in the list: a strategy's contracts come from the binary (E09-D035).
func WorkerContracts(kind string) ([]string, bool) {
	switch kind {
	case KindMDConn:
		return []string{"worker.v1", "mdctl.v1"}, true
	case KindTDAccount:
		return []string{"worker.v1", "tdctl.v1"}, true
	default:
		return nil, false
	}
}

// PackWorker packs a conn or account worker into one reproducible OCI image (E09-D088–E09-D090).
// kind is md.conn or td.account. Contracts are WorkerContracts(kind); the SDK marker and the
// module version in the binary are ignored, and sdk is left empty. The layer is one member,
// bin/worker. env may be nil. The manifest is checked with CheckLaunchFields before the image
// is built (E09-D041). It does not push.
func PackWorker(binary []byte, kind, name string, env map[string]string) (Image, error) {
	contracts, ok := WorkerContracts(kind)
	if !ok {
		return Image{}, fmt.Errorf("artifact: kind %q is not md.conn or td.account", kind)
	}
	if name == "" {
		return Image{}, errors.New("artifact: name is empty")
	}
	osName, arch, err := elfPlatform(binary)
	if err != nil {
		return Image{}, err
	}
	if env == nil {
		env = map[string]string{}
	}
	m := Manifest{
		Schema:     Schema,
		Kind:       kind,
		Name:       name,
		Runtime:    "go",
		OS:         osName,
		Arch:       arch,
		Entrypoint: []string{workerEntrypoint},
		Env:        env,
		SDK:        SDK{},
		Contracts:  append([]string(nil), contracts...),
	}
	if err := m.CheckLaunchFields(); err != nil {
		return Image{}, err
	}
	config, err := marshalManifest(m)
	if err != nil {
		return Image{}, err
	}
	layer, err := fileLayer(workerEntrypoint, binary)
	if err != nil {
		return Image{}, err
	}
	manifest, err := imageManifest(config, layer)
	if err != nil {
		return Image{}, err
	}
	return Image{
		Digest:   digest.FromBytes(manifest).String(),
		Manifest: manifest,
		Config:   config,
		Layer:    layer,
	}, nil
}

func marshalManifest(m Manifest) ([]byte, error) {
	env := m.Env
	if env == nil {
		env = map[string]string{}
	}
	return json.Marshal(struct {
		Schema     string            `json:"schema"`
		Kind       string            `json:"kind"`
		Name       string            `json:"name"`
		Runtime    string            `json:"runtime"`
		OS         string            `json:"os"`
		Arch       string            `json:"arch"`
		Entrypoint []string          `json:"entrypoint"`
		Env        map[string]string `json:"env"`
		SDK        SDK               `json:"sdk"`
		Contracts  []string          `json:"contracts"`
	}{
		Schema: m.Schema, Kind: m.Kind, Name: m.Name, Runtime: m.Runtime,
		OS: m.OS, Arch: m.Arch, Entrypoint: m.Entrypoint, Env: env,
		SDK: m.SDK, Contracts: m.Contracts,
	})
}

func imageManifest(config, layer []byte) ([]byte, error) {
	// No annotations. org.opencontainers.image.created in particular is absent (E09-D038).
	manifest := ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    content.NewDescriptorFromBytes(MediaTypeWorkerConfig, config),
		Layers: []ocispec.Descriptor{
			content.NewDescriptorFromBytes(ocispec.MediaTypeImageLayerGzip, layer),
		},
	}
	return json.Marshal(manifest)
}

// strategyLayer is one tar+gzip member, bin/strategy (E09-D002, E09-D038).
func strategyLayer(binary []byte) ([]byte, error) {
	return fileLayer(strategyEntrypoint, binary)
}

// fileLayer is one tar+gzip member at name. The gzip header has no name and no time, the level
// is fixed, and the tar member is mode 0755, uid 0, gid 0, mtime 0 (E09-D002, E09-D038).
func fileLayer(name string, binary []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, layerCompression)
	if err != nil {
		return nil, err
	}
	zw.Name = ""
	zw.ModTime = time.Time{}
	zw.OS = 255
	tw := tar.NewWriter(zw)
	if err := tw.WriteHeader(&tar.Header{
		Name:     name,
		Mode:     strategyMode,
		Uid:      0,
		Gid:      0,
		Size:     int64(len(binary)),
		ModTime:  time.Unix(0, 0),
		Typeflag: tar.TypeReg,
		Format:   tar.FormatUSTAR,
	}); err != nil {
		return nil, err
	}
	if _, err := tw.Write(binary); err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func sdkVersion(binary []byte) (string, error) {
	info, err := buildinfo.Read(bytes.NewReader(binary))
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrNoSDKVersion, err)
	}
	if info.Main.Path == sdkModule {
		if info.Main.Version == "" {
			return "", ErrNoSDKVersion
		}
		return info.Main.Version, nil
	}
	for _, dep := range info.Deps {
		if dep.Path != sdkModule {
			continue
		}
		mod := dep
		if dep.Replace != nil {
			mod = dep.Replace
		}
		if mod.Version == "" {
			return "", ErrNoSDKVersion
		}
		return mod.Version, nil
	}
	return "", ErrNoSDKVersion
}
