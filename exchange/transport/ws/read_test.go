package ws

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"
)

// floodServer takes one websocket handshake by hand and then writes block, a run of ready-made
// frames, until the client goes away: the server side allocates nothing while the client is
// measured.
func floodServer(tb testing.TB, block []byte) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go flood(conn, block)
		}
	}()
	return "ws://" + ln.Addr().String()
}

func flood(conn net.Conn, block []byte) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	req, err := http.ReadRequest(br)
	if err != nil {
		return
	}
	sum := sha1.Sum([]byte(req.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	if _, err := io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
		"Sec-WebSocket-Accept: "+base64.StdEncoding.EncodeToString(sum[:])+"\r\n\r\n"); err != nil {
		return
	}
	// Whatever the client sends (its close frame) ends the flood.
	go func() { br.ReadByte(); conn.Close() }()
	for {
		if _, err := conn.Write(block); err != nil {
			return
		}
	}
}

// frames is n unmasked server text frames of payload.
func frames(payload []byte, n int) []byte {
	var head []byte
	switch l := len(payload); {
	case l < 126:
		head = []byte{0x81, byte(l)}
	case l < 1<<16:
		head = binary.BigEndian.AppendUint16([]byte{0x81, 126}, uint16(l))
	default:
		head = binary.BigEndian.AppendUint64([]byte{0x81, 127}, uint64(l))
	}
	var out []byte
	for range n {
		out = append(append(out, head...), payload...)
	}
	return out
}

// depthUpdate is a Binance-like diff depth message of about size bytes.
func depthUpdate(size int) []byte {
	b := []byte(`{"e":"depthUpdate","E":1760054400000,"s":"BTCUSDT","U":157,"u":160,"b":[`)
	for i := 0; len(b) < size-24; i++ {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, `["6`...)
		b = strconv.AppendInt(b, int64(1000+i), 10)
		b = append(b, `.12","0.0315"]`...)
	}
	return append(b, `],"a":[]}`...)
}

var errDone = errors.New("done")

// readMessages runs a connection to the flood server and calls at with each message's count.
func readMessages(tb testing.TB, size int, at func(n int) bool) {
	tb.Helper()
	payload := depthUpdate(size)
	c, err := Dial(context.Background(), floodServer(tb, frames(payload, max(1, 64<<10/len(payload)))), Options{ReadLimit: 1 << 20})
	if err != nil {
		tb.Fatal(err)
	}
	n := 0
	err = c.Run(context.Background(), func(frame []byte) error {
		if len(frame) != len(payload) {
			return errors.New("short frame")
		}
		n++
		if at(n) {
			return errDone
		}
		return nil
	})
	if !errors.Is(err, errDone) {
		tb.Fatal(err)
	}
}

// The read path allocates nothing per message once the buffer has grown (D302).
//
// MemStats counts the whole process, and the runtime allocates when it starts an OS thread: on a busy
// machine it does, to take a P back from a goroutine held in a socket syscall. A window in which a
// thread started is measured again; an allocation on the read path shows in every window.
func TestReadDoesNotAllocate(t *testing.T) {
	const warm, count, retries = 1_000, 20_000, 4
	threads := pprof.Lookup("threadcreate")
	for _, size := range []int{256, 4 << 10, 128 << 10} {
		var before, after runtime.MemStats
		var allocs uint64
		started, end, retried := 0, warm+count, 0
		readMessages(t, size, func(n int) bool {
			if n == warm {
				started = threads.Count()
				runtime.ReadMemStats(&before)
			}
			if n != end {
				return false
			}
			runtime.ReadMemStats(&after)
			allocs = after.Mallocs - before.Mallocs
			if allocs > 0 && threads.Count() != started && retried < retries {
				t.Logf("%d B messages: %d allocations while the runtime started a thread; measuring again", size, allocs)
				retried++
				end += count
				started = threads.Count()
				runtime.ReadMemStats(&before)
				return false
			}
			return true
		})
		if allocs > 0 {
			t.Errorf("%d B messages: %d allocations in %d reads", size, allocs, count)
		}
	}
}

// BenchmarkRead is the steady-state read of one connection: one op is one message, read whole into
// the connection's buffer and lent to the handler.
func BenchmarkRead(b *testing.B) {
	for _, size := range []int{256, 4 << 10, 64 << 10} {
		b.Run(strconv.Itoa(size)+"B", func(b *testing.B) {
			const warm = 1_000
			b.SetBytes(int64(len(depthUpdate(size))))
			b.ReportAllocs()
			readMessages(b, size, func(n int) bool {
				switch n {
				case warm:
					b.ResetTimer()
				case warm + b.N:
					b.StopTimer()
					return true
				}
				return false
			})
		})
	}
}
