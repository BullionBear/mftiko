package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/platform/artifact"
)

// After a strategy Pull returns, and before UnpackImage runs, GC must not delete
// the layers. An unheld orphan in the same GC is deleted, so the keep is the hold (E09-D073).
func TestStrategyLayersHeldBeforeUnpack(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	img, err := packedStrategy()
	if err != nil {
		t.Fatal(err)
	}
	reg := newMemRegistry()
	t.Cleanup(reg.Close)
	pushCtx, pushCancel := context.WithTimeout(context.Background(), 30*time.Second)
	err = artifact.Push(pushCtx, img, artifact.Destination{Repository: reg.repo(), PlainHTTP: true})
	pushCancel()
	if err != nil {
		t.Fatal(err)
	}
	data := t.TempDir()
	client, err := artifact.Open(artifact.Registry{Address: reg.addr(), PlainHTTP: true}, data)
	if err != nil {
		t.Fatal(err)
	}
	p := newPuller(context.Background(), client, data, 0, artifact.UnpackLimits{}, zerolog.Nop(), time.Now)

	var mu sync.Mutex
	var hookErr error
	hooked := false
	p.afterHold = func(fetched *artifact.Fetched) {
		err := gcBetweenPullAndUnpack(data, client, fetched)
		mu.Lock()
		hookErr = err
		hooked = true
		mu.Unlock()
	}

	req := artifact.Request{Repository: "hello", Digest: img.Digest}
	pollUntil(t, "strategy unpacked", func() bool {
		mu.Lock()
		err := hookErr
		mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		snap := p.ensure(req)
		if snap.perm != "" {
			t.Fatalf("pull rejected: %s", snap.perm)
		}
		return snap.ready
	})
	mu.Lock()
	defer mu.Unlock()
	if !hooked {
		t.Fatal("gc did not run between pull and unpack")
	}
	if hookErr != nil {
		t.Fatal(hookErr)
	}
}

func gcBetweenPullAndUnpack(data string, client *artifact.Client, fetched *artifact.Fetched) error {
	if len(fetched.Layers) == 0 {
		return errors.New("strategy pull returned no layers")
	}
	orphan := "sha256:" + strings.Repeat("ab", 32)
	for _, layer := range fetched.Layers {
		if layer.Digest == orphan {
			return errors.New("orphan digest collided with a strategy layer")
		}
	}
	orphanPath := filepath.Join(data, "layers", orphan)
	if err := os.WriteFile(orphanPath, []byte("orphan"), 0o644); err != nil {
		return err
	}
	if err := client.GC(nil); err != nil {
		return err
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		return errors.New("gc left an unheld layer")
	}
	for _, layer := range fetched.Layers {
		if _, err := os.Stat(layer.Path); err != nil {
			return err
		}
	}
	return nil
}
