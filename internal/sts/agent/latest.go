package agent

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/platform/artifact"
)

// latestTag is the tag a release moves (E09-D087, E12-D012).
const latestTag = "latest"

// latestState is the last resolve of one repository. ok is false when it has not succeeded, including
// after a later failure (E12-D013): GC then keeps every recorded rootfs of that repository.
type latestState struct {
	ok     bool
	digest string
}

// latestTags resolves latest for repositories that have a recorded rootfs (E12-D011, E12-D012).
// One goroutine resolves at start and then every poll. GC reads the cache and does not call the registry.
type latestTags struct {
	client *artifact.Client
	poll   time.Duration
	log    zerolog.Logger

	mu    sync.Mutex
	state map[string]latestState
}

func newLatestTags(client *artifact.Client, poll time.Duration, log zerolog.Logger) *latestTags {
	if poll <= 0 {
		poll = time.Minute
	}
	return &latestTags{client: client, poll: poll, log: log, state: map[string]latestState{}}
}

// Run resolves at once, then every poll interval, until ctx is done.
func (t *latestTags) Run(ctx context.Context) {
	for {
		t.resolve(ctx)
		timer := time.NewTimer(t.poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (t *latestTags) resolve(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	repos, err := t.client.RootfsRepos()
	if err != nil {
		t.log.Warn().Err(err).Msg("latest repositories")
		return
	}
	for _, repo := range slices.Sorted(maps.Keys(repos)) {
		if ctx.Err() != nil {
			return
		}
		dig, err := t.client.Resolve(ctx, repo, latestTag)
		if ctx.Err() != nil {
			return
		}
		t.mu.Lock()
		prev := t.state[repo]
		if err != nil || !digestOK(dig) {
			t.state[repo] = latestState{}
			t.mu.Unlock()
			if err == nil {
				err = fmt.Errorf("digest %q", dig)
			}
			t.log.Warn().Err(err).Str("repository", repo).Msg("latest not resolved")
			continue
		}
		t.state[repo] = latestState{ok: true, digest: dig}
		t.mu.Unlock()
		if !prev.ok || prev.digest != dig {
			t.log.Info().Str("repository", repo).Str("digest", dig).Msg("latest resolved")
		}
	}
}

// protect is what GC adds for latest (E12-D011–E12-D013). It does not call the registry.
// A repository whose latest is not known contributes every recorded rootfs. A resolved one contributes
// that digest. Rootfs with no repository record are absent.
func (t *latestTags) protect() ([]string, error) {
	repos, err := t.client.RootfsRepos()
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	set := map[string]struct{}{}
	for repo, digests := range repos {
		st, seen := t.state[repo]
		if !seen || !st.ok || !digestOK(st.digest) {
			for _, d := range digests {
				if digestOK(d) {
					set[d] = struct{}{}
				}
			}
			continue
		}
		set[st.digest] = struct{}{}
	}
	return slices.Sorted(maps.Keys(set)), nil
}
