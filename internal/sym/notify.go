package sym

import "context"

// ChangedPublisher is invoked after a revision commit. The revision is the
// value just committed for that (venue, category). Implementations must not
// block the caller for long and must not call back into the store.
type ChangedPublisher interface {
	Changed(ctx context.Context, venue, category string, revision int64)
}
