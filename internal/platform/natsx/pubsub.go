package natsx

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
)

// Msg is one delivered message. Data is valid only until the callback returns: the client
// reuses the buffer, and this package does not copy it, so a market-data callback can read the
// bytes in place. Copy Data to keep it.
type Msg struct {
	Subject string
	Reply   string
	Data    []byte
}

// Subscription is one interest on the server. Unsubscribe is idempotent.
type Subscription struct {
	sub  *nats.Subscription
	stop func() bool
}

// Unsubscribe removes the interest and stops the callback. A second call does nothing.
func (s *Subscription) Unsubscribe() error {
	if s == nil || s.sub == nil {
		return errors.New("natsx: nil subscription")
	}
	if s.stop != nil {
		s.stop()
	}
	if err := s.sub.Unsubscribe(); err != nil && !errors.Is(err, nats.ErrBadSubscription) {
		return fmt.Errorf("natsx: unsubscribe: %w", err)
	}
	return nil
}

// Publish sends data on subject. data is the payload the subscriber reads; nothing is added
// in front of it, and an empty or nil slice is an empty payload. A subject with no subscriber
// is not an error: core NATS keeps nothing.
func (c *Conn) Publish(subject string, data []byte) error {
	if err := c.usable(); err != nil {
		return err
	}
	if err := c.nc.Publish(subject, data); err != nil {
		return classify("publish", subject, err)
	}
	return nil
}

// PublishMsg is Publish with an optional reply subject. Bytes are not inspected. Request uses
// it; callers do not need it for ordinary fanout.
func (c *Conn) PublishMsg(m Msg) error {
	if err := c.usable(); err != nil {
		return err
	}
	if err := c.nc.PublishMsg(&nats.Msg{Subject: m.Subject, Reply: m.Reply, Data: m.Data}); err != nil {
		return classify("publish", m.Subject, err)
	}
	return nil
}

// Subscribe delivers each message on subject to fn, one at a time, on the connection's read
// goroutine. fn must return promptly: while it runs, no other callback on this connection runs.
// queue, when not empty, is the queue group, and the server hands each message to one member.
//
// Subscribe returns only after the server has registered the interest. The interest is removed
// when ctx is cancelled. A cancelled ctx does not subscribe.
func (c *Conn) Subscribe(ctx context.Context, subject, queue string, fn func(Msg)) (*Subscription, error) {
	if err := c.usable(); err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, errors.New("natsx: nil handler")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cb := func(m *nats.Msg) {
		fn(Msg{Subject: m.Subject, Reply: m.Reply, Data: m.Data})
	}
	var (
		sub *nats.Subscription
		err error
	)
	if queue == "" {
		sub, err = c.nc.Subscribe(subject, cb)
	} else {
		sub, err = c.nc.QueueSubscribe(subject, queue, cb)
	}
	if err != nil {
		return nil, classify("subscribe", subject, err)
	}
	if err := c.nc.FlushTimeout(flushTimeout); err != nil {
		_ = sub.Unsubscribe()
		return nil, classify("subscribe", subject, err)
	}
	if err := ctx.Err(); err != nil {
		_ = sub.Unsubscribe()
		return nil, err
	}
	s := &Subscription{sub: sub}
	s.stop = context.AfterFunc(ctx, func() { _ = sub.Unsubscribe() })
	return s, nil
}
