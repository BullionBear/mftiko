package exchangetest

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// logonTakes is how long a venue with events takes to finish a logon. Binance spot finishes it
// after reading the next frame: a subscribe written right behind the logon finds the session
// unauthenticated (-1193).
const logonTakes = 20 * time.Millisecond

// SetURL is the URL the codec dials: the ws:// URL of an httptest server running Handler.
func (v *TradeVenue) SetURL(u string) { v.mu.Lock(); v.url = u; v.mu.Unlock() }

// Handler serves the trade websocket. Each frame the client sends goes to Handle, and whatever is
// then queued is written back at once, in order: a test that wants to choose the order of answers
// drives Handle and Release itself instead. Answers to requests made Drop never come. With events,
// the logon is answered logonTakes later, and a subscribe that comes before is refused, as Binance
// spot does.
func (v *TradeVenue) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		c := &streamConn{ws: ws, wake: make(chan struct{}, 1)}
		v.mu.Lock()
		v.conns[c] = struct{}{}
		v.connsTotal++
		v.mu.Unlock()
		defer func() {
			v.mu.Lock()
			delete(v.conns, c)
			v.mu.Unlock()
		}()
		go c.write(ctx)
		var authed atomic.Bool
		for {
			_, b, err := ws.Read(ctx)
			if err != nil {
				return
			}
			var f clientFrame
			_ = json.Unmarshal(b, &f)
			if v.events && f.Op == "subscribe" && !authed.Load() {
				c.send(mustJSON(venueFrame{ID: "sub", Status: 400, Code: -1193, Msg: "session not authenticated"}))
				continue
			}
			if v.Handle(b) != nil {
				ws.Close(websocket.StatusPolicyViolation, "bad frame")
				return
			}
			out := v.Release()
			if v.events && f.Op == "logon" {
				go func() {
					select {
					case <-time.After(logonTakes):
					case <-ctx.Done():
						return
					}
					authed.Store(true)
					for _, a := range out {
						c.send(a)
					}
				}()
				continue
			}
			for _, f := range out {
				c.send(f)
			}
		}
	})
}

// Push sends frame (a Fill) to every connection Handler serves.
func (v *TradeVenue) Push(frame []byte) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for c := range v.conns {
		c.send(frame)
	}
}

// Disconnect drops every connection Handler serves, without a close handshake.
func (v *TradeVenue) Disconnect() {
	v.mu.Lock()
	conns := make([]*streamConn, 0, len(v.conns))
	for c := range v.conns {
		conns = append(conns, c)
	}
	v.mu.Unlock()
	for _, c := range conns {
		c.ws.CloseNow()
	}
}

// Connections is how many connections Handler serves now, and how many it has accepted.
func (v *TradeVenue) Connections() (open, total int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.conns), v.connsTotal
}
