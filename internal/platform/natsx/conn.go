package natsx

import (
	"errors"
	"fmt"
	"log/slog"
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

// Options configures one connection. Zero is an unauthenticated connection, which is what the
// dev server at nats://127.0.0.1:4222 accepts. Credentials and the per-session inbox prefix
// are not configured here.
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
}

// Conn is one core-NATS connection. It is safe for concurrent use. It has no JetStream handle.
type Conn struct {
	nc         *nats.Conn
	reqTimeout time.Duration
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
	nopts := []nats.Option{
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Warn("nats disconnected", "err", err)
				return
			}
			log.Info("nats disconnected")
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info("nats reconnected", "url", nc.ConnectedUrlRedacted())
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
	nc, err := nats.Connect(url, nopts...)
	if err != nil {
		return nil, fmt.Errorf("natsx: connect: %w", err)
	}
	return &Conn{nc: nc, reqTimeout: timeout}, nil
}

func (o Options) validate() error {
	if o.RequestTimeout < 0 || o.ConnectTimeout < 0 {
		return errors.New("natsx: negative timeout")
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
