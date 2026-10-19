package artifact

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// GC deletes unpacked images whose digest is not in keep, then cached layers that no remaining
// rootfs references (D956). keep is the image digests the caller still needs. GC does not decide
// which digest is the newest; a digest that is being pulled or unpacked is included by the caller.
// Temporary files and directories are left alone. A keep entry that is not a sha256 digest is a
// caller mistake and deletes nothing.
func (c *Client) GC(keep []string) error {
	pinned := make(map[string]struct{}, len(keep))
	for _, k := range keep {
		if !digestOK(k) {
			return badRequest("keep digest %q", k)
		}
		pinned[k] = struct{}{}
	}
	if err := c.removeUnkept(pinned); err != nil {
		return err
	}
	refs, err := c.referencedLayers()
	if err != nil {
		return err
	}
	return c.removeUnreferencedLayers(refs)
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

// referencedLayers reads the layer list of every remaining artifact directory (D950, D956).
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

func (c *Client) removeUnreferencedLayers(refs map[string]struct{}) error {
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
		if _, ok := refs[name]; ok || !digestOK(name) {
			continue
		}
		if err := os.Remove(filepath.Join(c.layers, name)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("artifact: gc: %w", err)
		}
	}
	return nil
}
