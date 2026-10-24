package artifact

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// GC deletes unpacked images whose digest is not in keep, then cached layers that no remaining
// rootfs references (E09-D056). keep is the image digests the caller still needs. GC does not decide
// which digest is the newest; a digest that is being pulled or unpacked is included by the caller.
//
// layers, when passed, is one extra set of layer digests to retain even with no rootfs reference
// (E09-D073). STS passes input layers that are being pulled or unpacked. After unpack the caller stops
// passing them. Omitting layers, or passing nil, is the E09-D056 behavior. A keep or layer entry that
// is not a sha256 digest is a caller mistake and deletes nothing. Temporary files and directories
// are left alone.
func (c *Client) GC(keep []string, layers ...[]string) error {
	extra, err := oneLayerSet(layers)
	if err != nil {
		return err
	}
	pinned, err := pinDigests(keep)
	if err != nil {
		return err
	}
	held, err := pinDigests(extra)
	if err != nil {
		return err
	}
	if err := c.removeUnkept(pinned); err != nil {
		return err
	}
	refs, err := c.referencedLayers()
	if err != nil {
		return err
	}
	return c.removeUnreferencedLayers(refs, held)
}

// oneLayerSet accepts the optional E09-D073 argument. More than one set is a caller mistake.
func oneLayerSet(layers [][]string) ([]string, error) {
	switch len(layers) {
	case 0:
		return nil, nil
	case 1:
		return layers[0], nil
	default:
		return nil, badRequest("layer keep takes one set")
	}
}

func pinDigests(list []string) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(list))
	for _, k := range list {
		if !digestOK(k) {
			return nil, badRequest("keep digest %q", k)
		}
		out[k] = struct{}{}
	}
	return out, nil
}

func (c *Client) removeUnkept(keep map[string]struct{}) error {
	dir := filepath.Join(c.data, "artifacts")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("artifact: gc: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, tempPrefix) {
			continue
		}
		if _, ok := keep[name]; ok {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("artifact: gc: %w", err)
		}
	}
	return nil
}

// referencedLayers reads the layer list of every remaining artifact directory (E09-D050, E09-D056).
func (c *Client) referencedLayers() (map[string]struct{}, error) {
	refs := map[string]struct{}{}
	dir := filepath.Join(c.data, "artifacts")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return refs, nil
	}
	if err != nil {
		return nil, fmt.Errorf("artifact: gc: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, tempPrefix) || !e.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, name, layersName))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("artifact: gc: %w", err)
		}
		sc := bufio.NewScanner(bytes.NewReader(raw))
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" || !digestOK(line) {
				continue
			}
			refs[line] = struct{}{}
		}
		if err := sc.Err(); err != nil {
			return nil, fmt.Errorf("artifact: gc: %w", err)
		}
	}
	return refs, nil
}

func (c *Client) removeUnreferencedLayers(refs, extra map[string]struct{}) error {
	entries, err := os.ReadDir(c.layers)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("artifact: gc: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, tempPrefix) || !e.Type().IsRegular() {
			continue
		}
		_, referenced := refs[name]
		_, kept := extra[name]
		if referenced || kept || !digestOK(name) {
			continue
		}
		if err := c.removeLayer(name); err != nil {
			return err
		}
	}
	return nil
}

// removeLayer deletes one cached layer unless a pull is holding it (E09-D073). The hold check and the
// delete share c.mu, and HoldLayers is taken before the layer file is created, so a layer that is
// being downloaded or unpacked is not removed.
func (c *Client) removeLayer(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.held[name] > 0 {
		return nil
	}
	if err := os.Remove(filepath.Join(c.layers, name)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("artifact: gc: %w", err)
	}
	return nil
}

// HoldLayers keeps these layer digests across GC until release is called (E09-D073). A data pull holds
// its layer before the download. A strategy pull holds Fetched.Layers after Pull returns and releases
// when UnpackImage finishes. release is safe to call more than once. A digest that is not sha256 holds
// nothing and returns ErrRequest.
func (c *Client) HoldLayers(digests []string) (release func(), err error) {
	for _, d := range digests {
		if !digestOK(d) {
			return nil, badRequest("keep digest %q", d)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.held == nil {
		c.held = map[string]int{}
	}
	for _, d := range digests {
		c.held[d]++
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			for _, d := range digests {
				if c.held[d] == 0 {
					continue
				}
				c.held[d]--
				if c.held[d] == 0 {
					delete(c.held, d)
				}
			}
		})
	}, nil
}
