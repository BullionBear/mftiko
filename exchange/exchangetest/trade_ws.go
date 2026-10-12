package exchangetest

import (
	"context"
	"net/http"

	"github.com/coder/websocket"
)

// SetURL is the URL the codec dials: the ws:// URL of an httptest server running Handler.
func (v *TradeVenue) SetURL(u string) { v.mu.Lock(); v.url = u; v.mu.Unlock() }

// Handler serves the trade websocket. Each frame the client sends goes to Handle, and whatever is
// then queued is written back at once, in order: a test that wants to choose the order of answers
// drives Handle and Release itself instead. Answers to requests made Drop never come.
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
		for {
			_, b, err := ws.Read(ctx)
			if err != nil {
				return
			}
			if v.Handle(b) != nil {
				ws.Close(websocket.StatusPolicyViolation, "bad frame")
				return
			}
			for _, f := range v.Release() {
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
