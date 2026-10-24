package conformance

import "errors"

// ErrArtifact is returned when a caller asks to unpack an OCI artifact.
// Unpacking is left as an interface for a later phase (E08-D036).
var ErrArtifact = errors.New("conformance: OCI artifact unpacking is not implemented")

// Worker is something the harness can exec. Command is a worker argv.
// Artifact is the OCI reference, not implemented in this phase (E08-D036).
type Worker interface {
	Args() ([]string, error)
}

// Command is a worker argv. The first element is the executable.
type Command []string

// Args returns the argv. An empty command is an error.
func (c Command) Args() ([]string, error) {
	if len(c) == 0 || c[0] == "" {
		return nil, errors.New("conformance: empty worker command")
	}
	out := make([]string, len(c))
	copy(out, c)
	return out, nil
}

// Artifact is an OCI reference. Args returns ErrArtifact (E08-D036).
type Artifact struct {
	Reference string
}

// Args reports that unpacking is not implemented.
func (Artifact) Args() ([]string, error) { return nil, ErrArtifact }
