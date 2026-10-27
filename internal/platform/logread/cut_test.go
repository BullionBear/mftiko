//go:build unix

package logread

import (
	"bytes"
	"testing"
)

func TestCutNewlineAndRune(t *testing.T) {
	you := []byte("你") // 3 bytes
	for _, c := range []struct {
		name  string
		in    []byte
		max   int
		force bool
		chunk []byte
		rest  []byte
	}{
		{name: "newline", in: []byte("a\n你"), max: 10, force: false, chunk: []byte("a\n"), rest: you},
		{name: "wait", in: []byte("ab"), max: 10, force: false, chunk: nil, rest: []byte("ab")},
		{name: "force", in: []byte("ab"), max: 10, force: true, chunk: []byte("ab"), rest: nil},
		{name: "rune", in: append(append([]byte("xx"), you...), 'y'), max: 4, force: true, chunk: []byte("xx"), rest: append(append([]byte{}, you...), 'y')},
		{name: "ascii max", in: bytes.Repeat([]byte("x"), 10), max: 4, force: true, chunk: bytes.Repeat([]byte("x"), 4), rest: bytes.Repeat([]byte("x"), 6)},
		{name: "last newline in window", in: []byte("ab\ncd\nef"), max: 8, force: true, chunk: []byte("ab\ncd\n"), rest: []byte("ef")},
	} {
		t.Run(c.name, func(t *testing.T) {
			chunk, rest := cut(c.in, c.max, c.force)
			if !bytes.Equal(chunk, c.chunk) || !bytes.Equal(rest, c.rest) {
				t.Fatalf("chunk %q rest %q", chunk, rest)
			}
		})
	}
}

func TestMaxChunkConstant(t *testing.T) {
	if maxChunk != 256<<10 {
		t.Fatalf("maxChunk %d", maxChunk)
	}
}
