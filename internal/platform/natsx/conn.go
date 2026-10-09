package natsx

import (
	"errors"
	"fmt"
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

// Options is how one connection authenticates and where its replies come back.
// Zero is an unauthenticated connection with NATS's own inbox prefix, which is what the dev
// server at nats://127.0.0.1:4222 accepts.
type Options struct {
	// Name is the connection name the server shows in monitoring. Empty leaves it unset.
	Name string

	// InboxPrefix is the prefix of reply subjects, without a trailing dot. Session workers set
	// it to _INBOX_{session_id} so one session cannot read another's replies (D30). Empty keeps
	// NATS's _INBOX. "*", ">" and a trailing dot are rejected.
	InboxPrefix string

	// UserJWT and NKeySeed authenticate a connection that already has a signed user JWT (D30).
	// Set both, or neither. This package does not sign the JWT.
	UserJWT  string
	NKeySeed string

	// CredsFile is a credentials file, the MD and TD instance credential (MFTIKO_NATS_CREDS).
	// It is mutually exclusive with UserJWT.
	CredsFile string

	// RequestTimeout is Request's wait when the context has no deadline. Zero means
	// DefaultRequestTimeout. A negative value is rejected.
	RequestTimeout time.Duration

	// ConnectTimeout is the dial timeout. Zero keeps the client's own default.
	ConnectTimeout time.Duration
}

// Conn is one core-NATS connection. It is safe for concurrent use. It has no JetStream handle;
// a caller that needs a second connection, for example so a slow RPC handler does not stall
// market data, opens one.
type Conn struct {
	nc         *nats.Conn
	reqTimeout time.Duration
}

// Connect dials url, which may be a comma-separated list. The first attempt must succeed;
// after that a dropped connection reconnects without a limit, so a blip does not end the process.
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
	nopts := []nats.Option{
		nats.MaxReconnects(-1),
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
	switch {
	case opt.CredsFile != "":
		nopts = append(nopts, nats.UserCredentials(opt.CredsFile))
	case opt.UserJWT != "":
		nopts = append(nopts, nats.UserJWTAndSeed(opt.UserJWT, opt.NKeySeed))
	}
	nc, err := nats.Connect(url, nopts...)
	if err != nil {
		return nil, fmt.Errorf("natsx: connect: %w", err)
	}
	return &Conn{nc: nc, reqTimeout: timeout}, nil
}

func (o Options) validate() error {
	if (o.UserJWT == "") != (o.NKeySeed == "") {
		return errors.New("natsx: user JWT and nkey seed must both be set, or neither")
	}
	if o.CredsFile != "" && o.UserJWT != "" {
		return errors.New("natsx: credentials file and user JWT are mutually exclusive")
	}
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
