package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// ConfigPath is ~/.config/mftiko/config.yaml (E11-D003). home is the user's
// home directory. XDG_CONFIG_HOME is not consulted; the decision names this path.
func ConfigPath(home string) string {
	return filepath.Join(home, ".config", "mftiko", "config.yaml")
}

// File is the config file. The only key is contexts (E11-D004).
type File struct {
	Contexts map[string]Context `yaml:"contexts"`
}

// Context is one named API endpoint. url is the Go client's server and includes
// /api/v1. token is written by login. ca_file and insecure_http are E11-D005.
type Context struct {
	URL          string `yaml:"url"`
	Token        string `yaml:"token,omitempty"`
	CAFile       string `yaml:"ca_file,omitempty"`
	InsecureHTTP bool   `yaml:"insecure_http,omitempty"`
}

// Load reads path. A mode wider than 0600 is refused before the bytes are read,
// so a world-readable token never enters the process (E11-D003). 0400 is narrower
// and is accepted. The file must be a regular file. Unknown YAML keys are refused.
func Load(path string) (*File, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	perm := fi.Mode().Perm()
	if perm&^os.FileMode(0o600) != 0 {
		return nil, fmt.Errorf("%s is mode %04o; refuse a config wider than 0600", path, perm)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	var f File
	if err := dec.Decode(&f); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%s is empty", path)
		}
		// The decoder quotes the offending value. A token typed into the wrong
		// key would otherwise land in the error (E11-D003).
		return nil, fmt.Errorf("%s: invalid YAML", path)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: invalid YAML", path)
	}
	for name := range f.Contexts {
		if name == "" {
			return nil, fmt.Errorf("%s: empty context name", path)
		}
	}
	return &f, nil
}

// Context returns the named context.
func (f *File) Context(name string) (Context, error) {
	if name == "" {
		return Context{}, errors.New("empty context name")
	}
	if f == nil || f.Contexts == nil {
		return Context{}, fmt.Errorf("unknown context %q", name)
	}
	c, ok := f.Contexts[name]
	if !ok {
		return Context{}, fmt.Errorf("unknown context %q", name)
	}
	return c, nil
}

// Save writes f to path as mode 0600, replacing the file. The directory is
// created as 0700. login rewrites the whole file and does not keep comments.
func Save(path string, f *File) error {
	b, err := yaml.Marshal(f)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".config-*.yaml")
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmp.Name())
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// CreateTemp's mode is masked by umask. Force 0600 on the inode we rename.
	if err := os.Chmod(tmp.Name(), 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	ok = true
	return nil
}
