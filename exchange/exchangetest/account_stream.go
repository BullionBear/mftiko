package exchangetest

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/coder/websocket"

	"github.com/BullionBear/mftiko/exchange"
)

// Handler serves the private websocket that the stream codec dials at /private. A connection's first
// message must be the login. After a successful one the connection gets the venue's events, from the
// next one on; a venue keeps nothing for a connection that is not there.
func (v *AccountVenue) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/private" {
			http.NotFound(w, r)
			return
		}
		v.mu.Lock()
		switch {
		case v.failDials > 0:
			v.failDials--
			v.mu.Unlock()
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		case v.opts.ListenKey && (v.listenKey == "" || r.URL.Query().Get("listenKey") != v.listenKey):
			v.mu.Unlock()
			http.Error(w, "invalid listenKey", http.StatusBadRequest)
			return
		}
		v.mu.Unlock()
		// A test holds the upgrade here to drop the live link while this dial has not logged in (E06-D035).
		if !v.waitHandshake(r.Context()) {
			return
		}
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		v.serve(r.Context(), ws)
	})
}

// streamConn is one logged-in connection: the frames the venue has for it, written in order by
// its own goroutine.
type streamConn struct {
	ws    *websocket.Conn
	mu    sync.Mutex
	queue [][]byte
	wake  chan struct{}
	gone  chan struct{} // closed once serve has removed the connection from v.conns
}

func (c *streamConn) send(frame []byte) {
	c.mu.Lock()
	c.queue = append(c.queue, frame)
	c.mu.Unlock()
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *streamConn) write(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.wake:
		}
		c.mu.Lock()
		q := c.queue
		c.queue = nil
		c.mu.Unlock()
		for _, f := range q {
			if err := c.ws.Write(ctx, websocket.MessageText, f); err != nil {
				return
			}
		}
	}
}

func (v *AccountVenue) serve(ctx context.Context, ws *websocket.Conn) {
	_, b, err := ws.Read(ctx)
	if err != nil {
		return
	}
	var login streamFrame
	if json.Unmarshal(b, &login) != nil || login.Op != "login" {
		ws.Close(websocket.StatusPolicyViolation, "login first")
		return
	}
	v.mu.Lock()
	ok := v.refuse == 0 && login.Key == v.key
	if v.refuse > 0 {
		v.refuse--
	}
	if !ok {
		v.mu.Unlock()
		_ = ws.Write(ctx, websocket.MessageText, mustJSON(streamFrame{Op: "login", Status: 401}))
		ws.Close(websocket.StatusPolicyViolation, "login refused")
		return
	}
	c := &streamConn{ws: ws, wake: make(chan struct{}, 1), gone: make(chan struct{})}
	ack := mustJSON(streamFrame{Op: "login", Status: 200})
	v.conns[c] = struct{}{}
	v.connsTotal++
	if v.stallLogin > 0 {
		v.stallLogin--
		v.stalled = append(v.stalled, heldLogin{c: c, frame: ack})
	} else {
		c.send(ack)
	}
	v.mu.Unlock()
	defer func() {
		v.mu.Lock()
		delete(v.conns, c)
		v.mu.Unlock()
		close(c.gone)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go c.write(ctx)
	// The client only logs in; anything else it sends is read and dropped, which also answers its
	// pings. The read ends when the client or Disconnect closes the connection.
	for {
		if _, _, err := ws.Read(ctx); err != nil {
			return
		}
	}
}

// broadcast sends frame to every logged-in connection, or holds it. v.mu held.
func (v *AccountVenue) broadcast(frame []byte) {
	if v.hold {
		v.held = append(v.held, frame)
		return
	}
	for c := range v.conns {
		c.send(frame)
	}
}

// Hold keeps the venue's events back, in order, until Release. A test uses it to choose whether an
// answer or the event of the same change arrives first.
func (v *AccountVenue) Hold() { v.mu.Lock(); v.hold = true; v.mu.Unlock() }

// Release sends the held events to the connections there now, and stops holding.
func (v *AccountVenue) Release() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.hold = false
	held := v.held
	v.held = nil
	for _, f := range held {
		v.broadcast(f)
	}
}

// EmitOrder and EmitFill push an event without changing the venue's state: a duplicate, an event
// out of order, or one about an order the venue does not hold.
func (v *AccountVenue) EmitOrder(o exchange.OrderUpdate) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.broadcast(orderFrame(&o))
}

func (v *AccountVenue) EmitFill(f exchange.Fill) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.broadcast(fillFrame(&f))
}

// Disconnect drops every stream connection at once, as a venue or the network would, without a
// close handshake. It returns once the venue no longer counts them in Connections: closing a socket
// does not wait for its serve goroutine, so the client can see the drop and reconnect first.
func (v *AccountVenue) Disconnect() {
	v.mu.Lock()
	conns := make([]*streamConn, 0, len(v.conns))
	for c := range v.conns {
		conns = append(conns, c)
	}
	v.mu.Unlock()
	for _, c := range conns {
		c.ws.CloseNow()
	}
	for _, c := range conns {
		<-c.gone
	}
}

// RefuseLogins makes the next n logins fail with a refusal the codec reports as ErrAuth.
func (v *AccountVenue) RefuseLogins(n int) { v.mu.Lock(); v.refuse = n; v.mu.Unlock() }

// FailDials makes the next n websocket handshakes fail with 503.
func (v *AccountVenue) FailDials(n int) { v.mu.Lock(); v.failDials = n; v.mu.Unlock() }

// ExpireListenKey ends the listen key (ListenKey): connections get the venue's expiry event, and the
// key no longer dials or refreshes until Prepare creates a new one.
func (v *AccountVenue) ExpireListenKey() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.listenKey = ""
	v.broadcast(mustJSON(streamFrame{Event: "expired", Reason: "listenKeyExpired"}))
}

// heldLogin is a logged-in connection whose acknowledgement has not been written yet.
type heldLogin struct {
	c     *streamConn
	frame []byte
}

// StallHandshakes holds the next n websocket upgrades until ReleaseHandshakes. The socket is not
// logged in, so a broadcast does not reach it. A test uses the wait to drop the live link while the
// replacement is still dialing (E06-D035).
func (v *AccountVenue) StallHandshakes(n int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if n <= 0 {
		return
	}
	v.stallHS += n
	if v.hsRelease == nil {
		v.hsRelease = make(chan struct{})
	}
}

// ReleaseHandshakes lets every stalled upgrade proceed.
func (v *AccountVenue) ReleaseHandshakes() {
	v.mu.Lock()
	ch := v.hsRelease
	v.hsRelease = nil
	v.stallHS = 0
	v.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// StalledHandshakes is how many upgrades are waiting in StallHandshakes.
func (v *AccountVenue) StalledHandshakes() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.hsWaiting
}

func (v *AccountVenue) waitHandshake(ctx context.Context) bool {
	v.mu.Lock()
	if v.stallHS == 0 {
		v.mu.Unlock()
		return true
	}
	v.stallHS--
	v.hsWaiting++
	ch := v.hsRelease
	v.mu.Unlock()
	select {
	case <-ch:
		v.mu.Lock()
		v.hsWaiting--
		v.mu.Unlock()
		return true
	case <-ctx.Done():
		v.mu.Lock()
		v.hsWaiting--
		v.mu.Unlock()
		return false
	}
}

// StallLogins holds the next n login acknowledgements. The connection is already in the broadcast
// set, so one event reaches the old link and the new one together; ReleaseLogins writes the acks.
func (v *AccountVenue) StallLogins(n int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if n > 0 {
		v.stallLogin += n
	}
}

// ReleaseLogins writes the login acknowledgements StallLogins held.
func (v *AccountVenue) ReleaseLogins() {
	v.mu.Lock()
	held := v.stalled
	v.stalled = nil
	v.mu.Unlock()
	for _, h := range held {
		h.c.send(h.frame)
	}
}

// StalledLogins is how many login acknowledgements are waiting.
func (v *AccountVenue) StalledLogins() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.stalled)
}

// Connections is how many logged-in stream connections are open now, and how many there have been.
func (v *AccountVenue) Connections() (open, total int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.conns), v.connsTotal
}
