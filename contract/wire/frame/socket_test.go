//go:build unix

package frame

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

// socketpair is C4's transport: one end for the shim, one for the worker (its fd 3), as plain files.
func socketpair(t *testing.T) (shim, worker *os.File) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	shim, worker = os.NewFile(uintptr(fds[0]), "shim"), os.NewFile(uintptr(fds[1]), "worker")
	t.Cleanup(func() { shim.Close(); worker.Close() })
	return shim, worker
}

// unixSocket is C3's transport: a listener on a socket file, as the shim has, and the agent's connection to it.
func unixSocket(t *testing.T) (server, client net.Conn) {
	t.Helper()
	dir := t.TempDir()
	if len(dir) > 90 { // sun_path holds about 104 bytes
		d, err := os.MkdirTemp("/tmp", "frame")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(d) })
		dir = d
	}
	l, err := net.Listen("unix", filepath.Join(dir, "s.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			t.Error(err)
		}
		accepted <- c
	}()
	client, err = net.Dial("unix", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server = <-accepted
	t.Cleanup(func() { server.Close(); client.Close() })
	return server, client
}

// stamped is a frame whose body says who wrote it and which one it is, padded to size bytes of a known pattern.
func stamped(writer, seq, size int) *wrapperspb.BytesValue {
	b := payload(writer*100003+seq, max(size, 8))
	binary.BigEndian.PutUint32(b, uint32(writer))
	binary.BigEndian.PutUint32(b[4:], uint32(seq))
	return wrapperspb.Bytes(b)
}

// Many goroutines share one Writer over a socketpair; the reader gets every frame whole, each writer's in order.
func TestSocketpairConcurrentWriters(t *testing.T) {
	shim, worker := socketpair(t)
	const writers, each = 8, 300
	w := NewWriter(worker)
	var wg sync.WaitGroup
	for id := range writers {
		wg.Go(func() {
			for seq := range each {
				size := 64
				if seq%50 == 0 {
					size = 200 << 10 // bigger than the socket buffer: written in several pieces
				}
				if err := w.Write(stamped(id, seq, size)); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		worker.Close() // the worker's end goes away: the shim sees io.EOF
		close(done)
	}()

	r := NewReader(shim)
	next := make([]int, writers)
	for {
		var m wrapperspb.BytesValue
		err := r.Read(&m)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		id, seq := int(binary.BigEndian.Uint32(m.Value)), int(binary.BigEndian.Uint32(m.Value[4:]))
		if id >= writers || seq != next[id] {
			t.Fatalf("frame from writer %d seq %d, expected seq %d", id, seq, next[id])
		}
		want := stamped(id, seq, len(m.Value))
		if string(want.Value) != string(m.Value) {
			t.Fatalf("frame %d/%d corrupted", id, seq)
		}
		next[id]++
	}
	<-done
	for id, n := range next {
		if n != each {
			t.Fatalf("writer %d: %d frames arrived, want %d", id, n, each)
		}
	}
}

// The worker dies halfway through a frame: the shim sees ErrTruncated, not a clean end.
func TestSocketpairPeerDiesMidFrame(t *testing.T) {
	shim, worker := socketpair(t)
	if _, err := worker.Write([]byte{0, 0, 0, 10, 1, 2, 3}); err != nil { // 10 bytes promised, 3 sent
		t.Fatal(err)
	}
	worker.Close()
	_, err := NewReader(shim).Next()
	if !errors.Is(err, ErrTruncated) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("peer died mid-frame: %v", err)
	}
}

// Over a unix socket, as C3: the largest frame, small ones, both directions, then a clean end.
func TestUnixSocket(t *testing.T) {
	server, client := unixSocket(t)
	sizes := []int{MaxLen, 3, 100, MaxLen - 1, 0}
	errs := make(chan error, 1)
	go func() {
		w := NewWriter(client)
		for _, n := range sizes {
			if err := w.Write(bytesOfSize(t, n)); err != nil {
				errs <- err
				return
			}
		}
		// The reply direction works on the same connection.
		var reply wrapperspb.StringValue
		if err := NewReader(client).Read(&reply); err != nil || reply.GetValue() != "ack" {
			errs <- errors.Join(err, errors.New("bad reply "+reply.GetValue()))
			return
		}
		errs <- client.Close()
	}()

	r := NewReader(server)
	for _, n := range sizes {
		body, err := r.Next()
		if err != nil {
			t.Fatalf("frame of %d: %v", n, err)
		}
		if len(body) != n {
			t.Fatalf("frame of %d arrived as %d", n, len(body))
		}
	}
	if err := NewWriter(server).Write(wrapperspb.String("ack")); err != nil {
		t.Fatal(err)
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("after the client closed: %v, want io.EOF", err)
	}
}
