package conn

import (
	"context"
	"sync"
)

// Bus is the NATS connectivity a conn worker reports. Set is non-blocking and
// may run on the NATS client's callback goroutine. Get is the latest value.
type Bus struct {
	mu     sync.Mutex
	up     bool
	known  bool
	signal chan struct{}
}

// NewBus starts known, with the value from a successful connect (true) or whatever
// the caller last observed. A zero Bus is not usable; Set on it does nothing.
func NewBus(up bool) *Bus {
	return &Bus{up: up, known: true, signal: make(chan struct{}, 1)}
}

// Set records up and wakes the sampler when the value changes. An unchanged
// value does not wake. The wake is one buffered slot, so a burst collapses to
// the latest value and Set returns immediately.
func (b *Bus) Set(up bool) {
	if b == nil || b.signal == nil {
		return
	}
	b.mu.Lock()
	same := b.known && b.up == up
	b.up = up
	b.known = true
	b.mu.Unlock()
	if same {
		return
	}
	select {
	case b.signal <- struct{}{}:
	default:
	}
}

// Get is the latest value. known is false until NewBus or Set.
func (b *Bus) Get() (up, known bool) {
	if b == nil {
		return false, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.up, b.known
}

// BindBus registers disconnect and reconnect callbacks that only call Set.
// onDisconnect and onReconnect are natsx.Conn's methods. Reconnect reports up
// immediately (E12-D065): it does not wait for the reconnect buffer and does
// not change the client's ReconnectBufSize. A nil argument does nothing.
func BindBus(bus *Bus, onDisconnect, onReconnect func(func())) {
	if bus == nil || onDisconnect == nil || onReconnect == nil {
		return
	}
	onDisconnect(func() { bus.Set(false) })
	onReconnect(func() { bus.Set(true) })
}

// watchBus samples Bus and sends a Report when connectivity changes. It also
// sends the value already known at start, which is up after a successful connect.
func (c *Conn) watchBus(ctx context.Context) {
	if c == nil || c.bus == nil {
		return
	}
	c.noteBus()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.bus.signal:
			c.noteBus()
		}
	}
}

// noteBus copies the latest sample onto the next Report. It does not hold c.mu
// across publishStatus: that takes reportMu and then c.mu.
func (c *Conn) noteBus() {
	if c == nil || c.bus == nil {
		return
	}
	up, known := c.bus.Get()
	if !known {
		return
	}
	c.mu.Lock()
	changed := !c.busSet || c.busUp != up
	if changed {
		c.busSet = true
		c.busUp = up
		c.statusDirty = true
	}
	c.mu.Unlock()
	if changed {
		c.publishStatus()
	}
}
