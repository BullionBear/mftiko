package sym

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
)

const (
	// ChangedWildcard is sym.v1.changed.> (D457). Session, account worker,
	// md.fetch and the historical backfill subscribe this. An MD instance's
	// JWT allows it too; the conn worker itself subscribes only its venue.
	ChangedWildcard = "sym.v1.changed.>"
)

// ChangedPublisher is invoked after a revision commit. The revision is the
// value just committed for that (venue, category). Implementations must not
// block the caller for long and must not call back into the store.
type ChangedPublisher interface {
	Changed(ctx context.Context, venue, category string, revision int64)
}

// ChangedSubject is sym.v1.changed.{venue}.{category} (D457). Venue and
// category are the human-readable names. A token that cannot be one NATS
// subject token is rejected. The request subject stays exactly "sym".
func ChangedSubject(venue, category string) (string, error) {
	if err := subjectToken("venue", venue); err != nil {
		return "", err
	}
	if err := subjectToken("category", category); err != nil {
		return "", err
	}
	return "sym.v1.changed." + venue + "." + category, nil
}

// ChangedVenueWildcard is sym.v1.changed.{venue}.> (D457). A conn worker
// subscribes this and no other venue. The MD instance JWT stays
// ChangedWildcard, because one instance can hold more than one venue.
func ChangedVenueWildcard(venue string) (string, error) {
	if err := subjectToken("venue", venue); err != nil {
		return "", err
	}
	return "sym.v1.changed." + venue + ".>", nil
}

func subjectToken(field, value string) error {
	if value == "" {
		return fmt.Errorf("sym: %s is empty", field)
	}
	if strings.ContainsAny(value, ".*> \t") {
		return fmt.Errorf("sym: %s %q is not a subject token", field, value)
	}
	return nil
}

// NATSChanged publishes Changed on sym.v1.changed.{venue}.{category} and flushes
// so the message is on the server before Changed returns. A publish failure
// is logged. The revision is already committed; a missed message is what the
// periodic revision check is for.
func NATSChanged(c *natsx.Conn, log zerolog.Logger) ChangedPublisher {
	return natsChanged{conn: c, log: log}
}

type natsChanged struct {
	conn *natsx.Conn
	log  zerolog.Logger
}

func (p natsChanged) Changed(ctx context.Context, venue, category string, revision int64) {
	_ = ctx
	subject, err := ChangedSubject(venue, category)
	if err != nil {
		p.log.Error().Err(err).Str("venue", venue).Str("category", category).Msg("sym change subject")
		return
	}
	b, err := natsx.Marshal(&symv1.Changed{
		Venue:    venue,
		Category: category,
		Revision: revision,
	})
	if err != nil {
		p.log.Error().Err(err).Str("subject", subject).Msg("sym change encode")
		return
	}
	if err := p.conn.Publish(subject, b); err != nil {
		p.log.Error().Err(err).Str("subject", subject).Int64("revision", revision).Msg("sym change")
		return
	}
	if err := p.conn.Flush(); err != nil {
		p.log.Error().Err(err).Str("subject", subject).Int64("revision", revision).Msg("sym change flush")
	}
}
