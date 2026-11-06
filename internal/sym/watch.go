package sym

import (
	"context"
	"errors"
	"fmt"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
)

// WatchFilter selects the tables a watch follows. An empty Venue is every venue; an empty
// Category is every category of Venue. A Category without a Venue is invalid.
type WatchFilter struct {
	Venue    string
	Category string
}

func (f WatchFilter) match(venue, category string) bool {
	return (f.Venue == "" || f.Venue == venue) && (f.Category == "" || f.Category == category)
}

// Watch is one change stream: Snapshot, the current revision of each matching table, then every
// later commit from Next. Nothing at or below the snapshot's revision is returned (E22-D009).
type Watch struct {
	// Snapshot is ordered by venue, then category.
	Snapshot *symv1.RevisionList

	hub   *Hub
	sub   *subscription
	floor map[tableKey]int64
}

var errWatchClosed = errors.New("sym: watch closed")

// Next is the next commit after the snapshot. It returns ErrSlowWatcher when this watch fell
// behind, an unavailable *Error when the hub stopped, and ctx's error when ctx ends.
func (w *Watch) Next(ctx context.Context) (*symv1.Changed, error) {
	for {
		select {
		case <-w.sub.done:
			return nil, w.sub.err
		default:
		}
		select {
		case <-w.sub.done:
			return nil, w.sub.err
		case <-ctx.Done():
			return nil, ctx.Err()
		case ch := <-w.sub.ch:
			if ch.GetRevision() <= w.floor[tableKey{ch.GetVenue(), ch.GetCategory()}] {
				continue
			}
			return ch, nil
		}
	}
}

// Close ends the watch. A second call does nothing.
func (w *Watch) Close() {
	w.hub.unsubscribe(w.sub)
	w.sub.end(errWatchClosed)
}

// Watch subscribes to the hub first and reads the snapshot second, so a commit between the two
// is in the snapshot or in Next (E22-D009).
func (sv *Service) Watch(ctx context.Context, f WatchFilter) (*Watch, error) {
	w, err := sv.watch(ctx, f)
	return w, sv.note(err)
}

func (sv *Service) watch(ctx context.Context, f WatchFilter) (*Watch, error) {
	if f.Venue == "" && f.Category != "" {
		return nil, invalid(fmt.Sprintf("category %q needs a venue", f.Category))
	}
	if f.Venue != "" {
		if err := CheckVenue(f.Venue); err != nil {
			return nil, err
		}
	}
	if f.Category != "" {
		if _, err := ParseCategory(f.Category); err != nil {
			return nil, err
		}
	}
	if sv.hub == nil {
		return nil, unavailable("watch is not served by this process", nil)
	}
	sub, ok := sv.hub.subscribe(f)
	if !ok {
		return nil, hubStopped()
	}
	all, err := sv.revisions(ctx, &symv1.RevisionsRequest{})
	if err != nil {
		sv.hub.unsubscribe(sub)
		return nil, err
	}
	w := &Watch{Snapshot: &symv1.RevisionList{}, hub: sv.hub, sub: sub, floor: map[tableKey]int64{}}
	for _, t := range all.GetTables() {
		if !f.match(t.GetVenue(), t.GetCategory()) {
			continue
		}
		w.Snapshot.Tables = append(w.Snapshot.Tables, t)
		w.floor[tableKey{t.GetVenue(), t.GetCategory()}] = t.GetRevision()
	}
	return w, nil
}
