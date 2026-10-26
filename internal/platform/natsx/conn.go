package natsx

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	// DefaultRequestTimeout is how long Request waits when its context has no deadline.
	// Callers with their own deadline, including a short one, override it.
	DefaultRequestTimeout = 5 * time.Second

	// flushTimeout is how long Subscribe waits for the server to register the subscription
	// before it returns. Callers wait on that return, not on a timer of their own.
	flushTimeout = 5 * time.Second
)

// ErrClosed: the connection is closed, so the operation was not sent.
var ErrClosed = errors.New("natsx: connection closed")

// Options configures one connection. The zero value is an unauthenticated connection, which is
// what natsxtest accepts. MD and md.fetch set CredsFile (E04-D015). A session does not: its JWT
// arrives in Init (E09-D006).
type Options struct {
	// Name is the connection name the server shows in monitoring. Empty leaves it unset.
	Name string

	// Logger receives disconnects, reconnects, closes, and asynchronous errors such as a slow
	// consumer. Nil uses slog.Default().
	Logger *slog.Logger

	// RequestTimeout is Request's wait when the context has no deadline. Zero means
	// DefaultRequestTimeout. A negative value is rejected.
	RequestTimeout time.Duration

	// ConnectTimeout is the dial timeout. Zero keeps the client's own default.
	ConnectTimeout time.Duration

	// InboxPrefix is the reply inbox prefix, passed to nats.CustomInboxPrefix.
	// Empty keeps the client default _INBOX. Sessions set _INBOX_{session_id} (E09-D006).
	// Platform processes set _INBOX_{plane} (E04-D016): _INBOX_md and _INBOX_fetch.
	InboxPrefix string

	// CredsFile is a NATS user credentials file, a user JWT and its seed.
	// Empty connects without one. An MD or md.fetch process sets it from
	// MFTIKO_NATS_CREDS (E04-D015). A session worker leaves it empty: its
	// credentials arrive in Init and never touch this field (E09-D006).
	CredsFile string

	// UserJWT and Seed are a user's credentials held in memory: a session or account worker's, which
	// arrive in Init and are never written to a file (E09-D006, E06-D011). Set both or neither, and not with
	// CredsFile.
	UserJWT string
	Seed    string
}

// Conn is one core-NATS connection. It is safe for concurrent use. It has no JetStream handle.
type Conn struct {
	nc         *nats.Conn
	reqTimeout time.Duration

	mu         sync.Mutex
	reconnect  []func()
	disconnect []func()
}

// Connect dials url, which may be a comma-separated list. The first attempt must succeed;
// after that a dropped connection reconnects without a limit, so a blip does not end the process.
// Disconnects, reconnects, closes, and asynchronous errors are logged on Options.Logger.
func Connect(url string, opt Options) (*Conn, error) {
	if url == "" {
		return nil, errors.New("natsx: empty url")
	}
	if err := opt.validate(); err != nil {
		return nil, err
	}
	timeout := opt.RequestTimeout
	if timeout == 0 {
		timeout = DefaultRequestTimeout
	}
	log := opt.Logger
	if log == nil {
		log = slog.Default()
	}
	c := &Conn{reqTimeout: timeout}
	nopts := []nats.Option{
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Warn("nats disconnected", "err", err)
			} else {
				log.Info("nats disconnected")
			}
			c.notifyDisconnect()
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info("nats reconnected", "url", nc.ConnectedUrlRedacted())
			c.notifyReconnect()
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			if err := nc.LastError(); err != nil {
				log.Info("nats closed", "err", err)
				return
			}
			log.Info("nats closed")
		}),
		// Replaces the client's stderr handler. A slow consumer is reported here.
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			subject := ""
			if sub != nil {
				subject = sub.Subject
			}
			log.Error("nats async error", "subject", subject, "err", err)
		}),
	}
	if opt.Name != "" {
		nopts = append(nopts, nats.Name(opt.Name))
	}
	if opt.ConnectTimeout > 0 {
		nopts = append(nopts, nats.Timeout(opt.ConnectTimeout))
	}
	if opt.InboxPrefix != "" {
		nopts = append(nopts, nats.CustomInboxPrefix(opt.InboxPrefix))
	}
	if opt.CredsFile != "" {
		nopts = append(nopts, nats.UserCredentials(opt.CredsFile))
	}
	if opt.UserJWT != "" {
		nopts = append(nopts, nats.UserJWTAndSeed(opt.UserJWT, opt.Seed))
	}
	nc, err := nats.Connect(url, nopts...)
	if err != nil {
		return nil, fmt.Errorf("natsx: connect: %w", err)
	}
	c.nc = nc
	return c, nil
}

// OnReconnect registers fn, called after each successful reconnect on the
// client's reconnect goroutine. fn must not block: a slow fn stalls every
// later callback and the client's own reconnect bookkeeping. A nil fn is
// ignored. Callbacks stay registered until the connection is discarded.
func (c *Conn) OnReconnect(fn func()) {
	if c == nil || fn == nil {
		return
	}
	c.mu.Lock()
	c.reconnect = append(c.reconnect, fn)
	c.mu.Unlock()
}

// OnDisconnect registers fn, called when the client notices the server is gone,
// on the client's callback goroutine. fn must not block, for the same reason as
// OnReconnect. A nil fn is ignored. Callbacks stay registered until the
// connection is discarded.
func (c *Conn) OnDisconnect(fn func()) {
	if c == nil || fn == nil {
		return
	}
	c.mu.Lock()
	c.disconnect = append(c.disconnect, fn)
	c.mu.Unlock()
}

func (c *Conn) notifyReconnect() {
	c.mu.Lock()
	fns := append([]func(){}, c.reconnect...)
	c.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
}

func (c *Conn) notifyDisconnect() {
	c.mu.Lock()
	fns := append([]func(){}, c.disconnect...)
	c.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
}

func (o Options) validate() error {
	if o.RequestTimeout < 0 || o.ConnectTimeout < 0 {
		return errors.New("natsx: negative timeout")
	}
	if (o.UserJWT == "") != (o.Seed == "") {
		return errors.New("natsx: UserJWT and Seed go together")
	}
	if o.UserJWT != "" && o.CredsFile != "" {
		return errors.New("natsx: credentials in memory and in a file")
	}
	return nil
}

func (c *Conn) usable() error {
	if c == nil || c.nc == nil || c.nc.IsClosed() {
		return ErrClosed
	}
	return nil
}

// Close closes the connection immediately. In-flight calls fail with ErrClosed. It is safe to
// call more than once.
func (c *Conn) Close() {
	if c == nil || c.nc == nil {
		return
	}
	c.nc.Close()
}

// Flush waits until the server has processed the publishes already submitted.
// A change notification uses it so a subscriber can observe the message once
// the publisher has returned.
func (c *Conn) Flush() error {
	if err := c.usable(); err != nil {
		return err
	}
	if err := c.nc.FlushTimeout(flushTimeout); err != nil {
		return classify("flush", "", err)
	}
	return nil
}

// Drain stops new work, delivers what is already buffered, and then closes.
func (c *Conn) Drain() error {
	if err := c.usable(); err != nil {
		return err
	}
	if err := c.nc.Drain(); err != nil {
		return classify("drain", "", err)
	}
	return nil
}

// classify maps a client error onto this package's sentinels. subject is empty when the
// operation has none.
func classify(op, subject string, err error) error {
	if err == nil {
		return nil
	}
	where := "natsx: " + op
	if subject != "" {
		where += " " + subject
	}
	switch {
	case errors.Is(err, nats.ErrConnectionClosed):
		return fmt.Errorf("%s: %w", where, ErrClosed)
	default:
		return fmt.Errorf("%s: %w", where, err)
	}
}
