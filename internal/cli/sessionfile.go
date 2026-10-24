package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// jsonNumberRe is a JSON number (RFC 8259). YAML scalars tagged as numbers
// whose text does not match are ambiguous: leading zeros, 0x, 0o, .inf, .nan
// (E11-D020). The original text is what gets written; it is never parsed as a
// float and never guessed as octal or hex (E11-D019).
var jsonNumberRe = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$`)

func jsonNumber(s string) bool { return jsonNumberRe.MatchString(s) }

// sourcePos is a 1-based line and column of one YAML node (E11-D023).
type sourcePos struct {
	Line int
	Col  int
}

// convertSession turns one YAML document into JSON. JSON is accepted because
// it is a YAML subset, so a YAML file, a JSON file, and -f - share this
// parser (E11-D019). locs maps each emitted value's JSON Pointer to the node
// that produced it. Numbers are copied as text.
func convertSession(name string, in []byte) ([]byte, map[string]sourcePos, error) {
	dec := yaml.NewDecoder(bytes.NewReader(in))
	var doc yaml.Node
	if err := dec.Decode(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil, fmt.Errorf("%s: session file is empty", name)
		}
		return nil, nil, fmt.Errorf("%s: %w", name, err)
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, nil, fmt.Errorf("%s: session file has more than one YAML document", name)
		}
		return nil, nil, fmt.Errorf("%s: %w", name, err)
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 {
		return nil, nil, fmt.Errorf("%s: session file is empty", name)
	}
	c := &conv{name: name, locs: map[string]sourcePos{}}
	if err := c.emit(doc.Content[0], "", true); err != nil {
		return nil, nil, err
	}
	return c.buf.Bytes(), c.locs, nil
}

type conv struct {
	name     string
	buf      bytes.Buffer
	locs     map[string]sourcePos
	visiting map[*yaml.Node]bool
}

func (c *conv) errAt(n *yaml.Node, msg string) error {
	if n == nil || n.Line == 0 {
		return fmt.Errorf("%s: %s", c.name, msg)
	}
	return fmt.Errorf("%s:%d:%d: %s", c.name, n.Line, n.Column, msg)
}

// emit writes n as JSON. record stores n's line and column at ptr. An alias
// stores the alias node's position and then expands the target without
// replacing that position; children keep the target nodes' positions (E11-D023).
func (c *conv) emit(n *yaml.Node, ptr string, record bool) error {
	if n == nil {
		return fmt.Errorf("%s: empty YAML node", c.name)
	}
	if c.visiting[n] {
		return c.errAt(n, "YAML alias cycle")
	}
	if c.visiting == nil {
		c.visiting = map[*yaml.Node]bool{}
	}
	if n.Kind == yaml.AliasNode {
		if n.Alias == nil {
			return c.errAt(n, "YAML alias is missing its target")
		}
		c.visiting[n] = true
		if record {
			c.locs[ptr] = sourcePos{Line: n.Line, Col: n.Column}
		}
		err := c.emit(n.Alias, ptr, false)
		delete(c.visiting, n)
		return err
	}
	c.visiting[n] = true
	defer delete(c.visiting, n)
	if record {
		c.locs[ptr] = sourcePos{Line: n.Line, Col: n.Column}
	}
	switch n.Kind {
	case yaml.ScalarNode:
		return c.scalar(n)
	case yaml.SequenceNode:
		return c.sequence(n, ptr)
	case yaml.MappingNode:
		return c.mapping(n, ptr)
	case yaml.DocumentNode:
		return c.errAt(n, "nested YAML document")
	default:
		return c.errAt(n, "unsupported YAML node")
	}
}

func (c *conv) scalar(n *yaml.Node) error {
	switch canonicalTag(n.Tag) {
	case "!!str", "!!timestamp", "!!binary":
		if err := appendJSONString(&c.buf, n.Value); err != nil {
			return c.errAt(n, "string is not JSON")
		}
		return nil
	case "!!bool":
		// YAML 1.2 booleans only. yes/no/on/off are strings and are not warned
		// (E11-D021); yaml.v3 already tags those as strings.
		switch n.Value {
		case "true", "True", "TRUE":
			c.buf.WriteString("true")
		case "false", "False", "FALSE":
			c.buf.WriteString("false")
		default:
			return c.errAt(n, fmt.Sprintf("boolean %q is not true or false", n.Value))
		}
		return nil
	case "!!null":
		c.buf.WriteString("null")
		return nil
	case "!!int", "!!float":
		if !jsonNumber(n.Value) {
			return c.errAt(n, fmt.Sprintf("ambiguous number %q; quote it", n.Value))
		}
		c.buf.WriteString(n.Value)
		return nil
	default:
		return c.errAt(n, fmt.Sprintf("unsupported YAML tag %s", n.Tag))
	}
}

func (c *conv) sequence(n *yaml.Node, ptr string) error {
	c.buf.WriteByte('[')
	for i, child := range n.Content {
		if i > 0 {
			c.buf.WriteByte(',')
		}
		if err := c.emit(child, fmt.Sprintf("%s/%d", ptr, i), true); err != nil {
			return err
		}
	}
	c.buf.WriteByte(']')
	return nil
}

func (c *conv) mapping(n *yaml.Node, ptr string) error {
	if len(n.Content)%2 != 0 {
		return c.errAt(n, "YAML mapping is malformed")
	}
	c.buf.WriteByte('{')
	seen := map[string]struct{}{}
	for i := 0; i < len(n.Content); i += 2 {
		keyNode, valNode := n.Content[i], n.Content[i+1]
		key, err := c.objectKey(keyNode)
		if err != nil {
			return err
		}
		if _, ok := seen[key]; ok {
			return c.errAt(keyNode, fmt.Sprintf("duplicate key %q", key))
		}
		seen[key] = struct{}{}
		if i > 0 {
			c.buf.WriteByte(',')
		}
		if err := appendJSONString(&c.buf, key); err != nil {
			return c.errAt(keyNode, "object key is not JSON")
		}
		c.buf.WriteByte(':')
		if err := c.emit(valNode, ptr+"/"+escapePointer(key), true); err != nil {
			return err
		}
	}
	c.buf.WriteByte('}')
	return nil
}

// objectKey requires a string. An ambiguous number is reported as that, not as
// a non-string key (E11-D020). Merge keys are not JSON.
func (c *conv) objectKey(start *yaml.Node) (string, error) {
	n := start
	seen := map[*yaml.Node]bool{}
	for n != nil && n.Kind == yaml.AliasNode {
		if seen[n] {
			return "", c.errAt(start, "YAML alias cycle")
		}
		seen[n] = true
		n = n.Alias
	}
	if n == nil || n.Kind != yaml.ScalarNode {
		return "", c.errAt(start, "object key is not a string")
	}
	switch canonicalTag(n.Tag) {
	case "!!merge":
		return "", c.errAt(n, "YAML merge is not JSON")
	case "!!str":
		return n.Value, nil
	case "!!int", "!!float":
		if !jsonNumber(n.Value) {
			return "", c.errAt(n, fmt.Sprintf("ambiguous number %q; quote it", n.Value))
		}
		return "", c.errAt(n, "object key is not a string")
	case "!!bool", "!!null", "!!timestamp", "!!binary":
		return "", c.errAt(n, "object key is not a string")
	default:
		return "", c.errAt(n, fmt.Sprintf("unsupported YAML tag %s", n.Tag))
	}
}

func canonicalTag(tag string) string {
	const long = "tag:yaml.org,2002:"
	if strings.HasPrefix(tag, long) {
		return "!!" + tag[len(long):]
	}
	return tag
}

// appendJSONString writes a JSON string without HTML escaping, so a quoted
// "<<", "&", or "<" stays in the request bytes. Numbers are not written here.
func appendJSONString(buf *bytes.Buffer, s string) error {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		return err
	}
	buf.Write(bytes.TrimRight(b.Bytes(), "\n"))
	return nil
}

// escapePointer encodes one JSON Pointer reference token (RFC 6901).
func escapePointer(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	return strings.ReplaceAll(s, "/", "~1")
}
