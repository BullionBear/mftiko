package sts

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
)

// Permissions is the allow list of one session user (E08-D018, E08-D021).
// Pub and Sub are sorted and deduplicated. There is no deny list.
type Permissions struct {
	Pub []string
	Sub []string
}

var (
	// ErrNilSpec is a nil SessionSpec.
	ErrNilSpec = errors.New("sts: nil session spec")

	// ErrSelector is a spec that declares selectors. Phase one does not expand
	// them (E08-D022).
	ErrSelector = errors.New("sts: spec has selectors")

	// ErrBadToken is a session id, MD instance, topic, or ticker that is empty
	// or contains '.', '*', '>', or whitespace. Those characters are not
	// escaped (E08-D019). An empty string is not a subject token.
	ErrBadToken = errors.New("sts: subject token is empty or contains '.', '*', '>' or whitespace")

	// ErrFeed is a feed key that is not topic.ticker.
	ErrFeed = errors.New("sts: feed key is not topic.ticker")

	// ErrAPIID is an account whose api_id is not greater than 0 (PROTOCOL §2).
	ErrAPIID = errors.New("sts: api_id must be positive")

	// ErrNoResolver is feeds with no feedInst. The MD instance is the caller's
	// to resolve (E08-D020).
	ErrNoResolver = errors.New("sts: feed instance resolver is nil")
)

// SessionPermissions is the NATS allow list for sessionID, taken from spec
// (E08-D018–E08-D022, E10-D015, E10-D030, amending E09-D006).
//
// feedInst maps a feed key, topic.UniversalTicker, to the MD instance that
// holds it. The caller resolves that instance. This function does not read
// FeedDecl.md_instance and does not dial etcd or NATS (E08-D020).
//
// Each declared feed adds one publish subject, md.v1.view.{inst}.{topic}.{ticker},
// and one subscribe subject, md.v1.f.{inst}.{topic}.{ticker}. md.v1.fetch.> is
// always published. Each account adds td.order.{api_id}, td.view.{api_id}, and
// td.evt.{api_id}. No account means no td subject. No feed means no md.v1.f
// and no md.v1.view. The fixed subjects are sym, _INBOX_{session_id}.>, and
// sym.v1.changed.>. There is no log.sts.{session_id} (E10-D015, E10-D030).
//
// Pub and Sub are deduplicated and sorted, so the same input yields the same
// lists. A spec with any selector is ErrSelector (E08-D022). A session id, MD
// instance, topic, or ticker that is empty or contains '.', '*', '>', or
// whitespace is ErrBadToken, and is not escaped (E08-D019). UniversalTicker's
// grammar does not allow '.' (PROTOCOL §2: a fractional strike spells D), so
// a canonical ticker passes that check. An api_id that is not greater than 0
// is ErrAPIID.
//
// The signed JWT's size is not checked here. internal/sts/issuer.SignSession
// measures the CONNECT argument nats-server compares to max_control_line and
// rejects a spec with too many feeds (E08-D027). This function does not sign.
func SessionPermissions(sessionID string, spec *sessionv1.SessionSpec, feedInst func(feed string) string) (Permissions, error) {
	if spec == nil {
		return Permissions{}, ErrNilSpec
	}
	if len(spec.GetSelectors()) > 0 {
		return Permissions{}, ErrSelector
	}
	if err := checkToken("session_id", sessionID); err != nil {
		return Permissions{}, err
	}

	pub := []string{
		"md.v1.fetch.>",
		"sym",
	}
	sub := []string{
		"_INBOX_" + sessionID + ".>",
		"sym.v1.changed.>",
	}
	for _, account := range spec.GetAccounts() {
		id := account.GetApiId()
		if id <= 0 {
			return Permissions{}, fmt.Errorf("sts: api_id %d: %w", id, ErrAPIID)
		}
		s := strconv.FormatInt(id, 10)
		// A positive decimal contains none of the E08-D019 characters. The check
		// is the same rule the other tokens go through, applied to the token
		// that is written into the subject.
		if err := checkToken("api_id", s); err != nil {
			return Permissions{}, err
		}
		pub = append(pub, "td.order."+s, "td.view."+s)
		sub = append(sub, "td.evt."+s)
	}
	for _, decl := range spec.GetFeeds() {
		feed := decl.GetFeed()
		topic, ticker, ok := splitFeed(feed)
		if !ok {
			return Permissions{}, fmt.Errorf("sts: feed %q: %w", feed, ErrFeed)
		}
		if err := checkToken("topic", topic); err != nil {
			return Permissions{}, err
		}
		if err := checkToken("ticker", ticker); err != nil {
			return Permissions{}, err
		}
		if feedInst == nil {
			return Permissions{}, ErrNoResolver
		}
		inst := feedInst(feed)
		if err := checkToken("md instance", inst); err != nil {
			return Permissions{}, err
		}
		pub = append(pub, "md.v1.view."+inst+"."+topic+"."+ticker)
		sub = append(sub, "md.v1.f."+inst+"."+topic+"."+ticker)
	}
	return Permissions{Pub: sortedUnique(pub), Sub: sortedUnique(sub)}, nil
}

// checkToken rejects a value that cannot be one NATS subject token (E08-D019).
func checkToken(field, value string) error {
	if value == "" || strings.ContainsAny(value, ".*>") || strings.ContainsFunc(value, unicode.IsSpace) {
		return fmt.Errorf("sts: %s %q: %w", field, value, ErrBadToken)
	}
	return nil
}

// splitFeed splits a feed key on the first dot. The topic is the part before
// it, so a topic cannot contain a dot; the ticker is the rest.
func splitFeed(s string) (topic, ticker string, ok bool) {
	topic, ticker, ok = strings.Cut(s, ".")
	if !ok || topic == "" || ticker == "" {
		return "", "", false
	}
	return topic, ticker, true
}

func sortedUnique(in []string) []string {
	slices.Sort(in)
	return slices.Clip(slices.Compact(in))
}
