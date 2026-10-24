package artifact

import (
	"bytes"
	"errors"
	"slices"
)

const contractsMarkerPrefix = "mftiko.contracts="

// ErrNoContracts means the binary has no embedded contracts list (E09-D035).
var ErrNoContracts = errors.New("artifact: no embedded contracts list")

// ErrContractsConflict means the binary embeds two different contracts lists.
var ErrContractsConflict = errors.New("artifact: embedded contracts lists disagree")

// embeddedContracts reads the list sdk writes into a Go strategy binary (E09-D035).
// The marker is contractsMarkerPrefix, the comma-separated majors, and a newline.
// Identical copies are one list. Copies that disagree are an error.
func embeddedContracts(binary []byte) ([]string, error) {
	prefix := []byte(contractsMarkerPrefix)
	var found []string
	for from := 0; from < len(binary); {
		i := bytes.Index(binary[from:], prefix)
		if i < 0 {
			break
		}
		at := from + i
		if list, ok := parseContractList(binary[at+len(prefix):]); ok {
			if found == nil {
				found = list
			} else if !slices.Equal(found, list) {
				return nil, ErrContractsConflict
			}
		}
		from = at + 1
	}
	if found == nil {
		return nil, ErrNoContracts
	}
	return found, nil
}

// parseContractList reads majors up to a newline. A name is letters, digits and
// dots, with at least one dot, so a stray prefix cannot swallow the next string.
func parseContractList(b []byte) ([]string, bool) {
	n := bytes.IndexByte(b, '\n')
	if n <= 0 {
		return nil, false
	}
	parts := bytes.Split(b[:n], []byte{','})
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if !validContract(p) {
			return nil, false
		}
		out = append(out, string(p))
	}
	return out, true
}

func validContract(p []byte) bool {
	if len(p) == 0 || p[0] == '.' || p[len(p)-1] == '.' || bytes.Contains(p, []byte("..")) {
		return false
	}
	dot := false
	for _, c := range p {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '.':
			dot = true
		default:
			return false
		}
	}
	return dot
}
