package sdk

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"

	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
)

const (
	// connectBackoffMin is the first wait after a failed NATS dial. It doubles until
	// connectBackoffMax. That is the platform schedule already used for Recon (E08-D042).
	// E08-D050 says to back off inside deadline_ms; it does not pick a new schedule.
	connectBackoffMin = time.Second
	connectBackoffMax = 30 * time.Second

	// connectAttempt is how long one dial waits when Init has no deadline, and the
	// cap when it does. The attempt is shortened to whatever remains of deadline_ms.
	connectAttempt = time.Second
)

// connectNATS dials until the connection is up, the Init context ends, or Stop
// cancels it (E08-D050, E08-D007). A dial error is not returned: its text can quote the
// credentials. When the deadline expires the error is the context's, so c4 reports
// the reason "deadline".
func (p *plane) connectNATS(ctx context.Context, url string, creds *stsctlv1.NatsCredentials) (*nats.Conn, error) {
	delay := connectBackoffMin
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		nc, err := p.dial(url, creds, attemptTimeout(ctx))
		if err == nil && nc != nil && ctx.Err() == nil && !p.closing() {
			return nc, nil
		}
		if nc != nil {
			nc.Close()
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if p.closing() {
			return nil, context.Canceled
		}
		p.note("nats connect failed")
		wait := delay
		if dl, ok := ctx.Deadline(); ok {
			remain := time.Until(dl)
			if remain <= 0 {
				return nil, ctx.Err()
			}
			if wait > remain {
				wait = remain
			}
		}
		if err := p.sleep(ctx, wait); err != nil {
			return nil, err
		}
		delay = growBackoff(delay)
	}
}

func (p *plane) dial(url string, creds *stsctlv1.NatsCredentials, timeout time.Duration) (*nats.Conn, error) {
	if p.host.tryConn != nil {
		return p.host.tryConn(url, creds, timeout)
	}
	return nats.Connect(url,
		nats.Name("mftiko-session"),
		nats.UserJWTAndSeed(creds.GetUserJwt(), creds.GetSeed()),
		nats.CustomInboxPrefix(creds.GetInboxPrefix()),
		nats.MaxReconnects(-1),
		nats.Timeout(timeout),
		nats.ReconnectHandler(func(*nats.Conn) {
			if s := p.symPtr.Load(); s != nil {
				s.Reconnect()
			}
		}),
	)
}

func (p *plane) sleep(ctx context.Context, d time.Duration) error {
	if p.host.after != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.host.after(d):
			return nil
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func attemptTimeout(ctx context.Context) time.Duration {
	if dl, ok := ctx.Deadline(); ok {
		remain := time.Until(dl)
		if remain <= 0 {
			return time.Millisecond
		}
		if remain < connectAttempt {
			return remain
		}
	}
	return connectAttempt
}

func growBackoff(d time.Duration) time.Duration {
	n := d * 2
	if n > connectBackoffMax || n < d {
		return connectBackoffMax
	}
	return n
}
