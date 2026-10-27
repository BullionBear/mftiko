package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// popStep takes the step word of absent and region off the arguments, before the flags.
func popStep() string {
	if len(os.Args) < 2 || strings.HasPrefix(os.Args[1], "-") {
		return ""
	}
	step := os.Args[1]
	os.Args = append(os.Args[:1], os.Args[2:]...)
	return step
}

func flowFromFlags() (*flow, context.CancelFunc, error) {
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	f, err := newFlow(ctx)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return f, cancel, nil
}

// tickerFlow: the API takes a feed whose ticker does not exist (E02-D002). MD rejects it, the
// controller ends the session deps_failed, and the reason stays on the terminal status as a
// dependency condition until the spec is deleted (E10-D014, E10-D093).
func tickerFlow() error {
	ticker := flag.String("ticker", "BinanceUM_Perp_NOSUCHUSDT", "a ticker SYM does not have")
	topic := flag.String("topic", "orderbook_delta", "the feed's topic")
	f, cancel, err := flowFromFlags()
	if err != nil {
		return err
	}
	defer cancel()
	feed := *topic + "." + *ticker
	body := idleSession("accept-ticker", stsInst)
	body.Feeds = &[]apiv1.FeedDeclInput{{Feed: feed, MdInstance: mdInst}}
	s, err := f.create(f.a, body, "")
	if err != nil {
		return fmt.Errorf("create: %w (the API must not check tickers, E02-D002)", err)
	}
	id := s.Id
	say("session %s with %s", id, feed)
	var reason string
	if err := until(f.ctx, time.Minute, "the feed REJECTED", func() error {
		s, _, err := f.get(f.a, id)
		if err != nil {
			return err
		}
		for _, d := range s.Dependencies {
			if d.Feed != nil && *d.Feed == feed && d.State == apiv1.EntryStateRejected {
				if d.Reason != nil {
					reason = *d.Reason
				}
				return nil
			}
		}
		if terminal(phaseOf(s)) {
			return nil // the terminal status carries it; checked below
		}
		return fmt.Errorf("dependencies %s", depsLine(s.Dependencies))
	}); err != nil {
		return err
	}
	if reason != "" {
		say("feed rejected by %s: %q", mdInst, reason)
	}
	end, err := f.waitPhase(f.a, id, 2*time.Minute, apiv1.PhaseDone, apiv1.PhaseFailed)
	if err != nil {
		return err
	}
	if end.Status.Reason != "deps_failed" {
		return fmt.Errorf("terminal reason %q, want deps_failed", end.Status.Reason)
	}
	i := slices.IndexFunc(end.Status.Conditions, func(c apiv1.Condition) bool {
		return c.Type == "dependency" && !c.Status && strings.Contains(c.Message, feed)
	})
	if i < 0 {
		return fmt.Errorf("terminal status has no dependency condition for %s: %+v", feed, end.Status.Conditions)
	}
	c := end.Status.Conditions[i]
	say("terminal %s deps_failed; condition dependency reason %q message %q", end.Status.Phase, c.Reason, c.Message)
	if reason != "" && c.Reason != reason {
		return fmt.Errorf("condition reason %q, the ack said %q", c.Reason, reason)
	}
	rejected := slices.ContainsFunc(end.Dependencies, func(d apiv1.Dependency) bool {
		return d.Feed != nil && *d.Feed == feed && d.State == apiv1.EntryStateRejected
	})
	if !rejected {
		return fmt.Errorf("terminal session's dependencies %s do not show the rejection", depsLine(end.Dependencies))
	}
	if err := f.remove(f.a, id); err != nil {
		return err
	}
	if _, code, _ := f.get(f.b, id); code != http.StatusNotFound {
		return fmt.Errorf("after delete, replica b answers %d", code)
	}
	say("session %s deleted", id)
	return f.audited()
}

// invalidFlow: specs the API must refuse with 400 validation_failed and the field that is wrong,
// with nothing written: both replicas list the same sessions before and after.
func invalidFlow() error {
	f, cancel, err := flowFromFlags()
	if err != nil {
		return err
	}
	defer cancel()
	before, err := f.bothIDs()
	if err != nil {
		return err
	}
	ok := idleSession("accept-invalid", stsInst)
	cases := []struct {
		name, path string
		body       func() ([]byte, error)
	}{
		{"unknown sts instance", "/sts_instance", mutate(ok, func(s *apiv1.SessionCreate) { s.StsInstance = "nope" })},
		{"sts instance of another plane", "/sts_instance", mutate(ok, func(s *apiv1.SessionCreate) { s.StsInstance = mdInst })},
		{"strategy not allowed", "/strategy/repository", mutate(ok, func(s *apiv1.SessionCreate) { s.Strategy.Repository = "example.invalid/nope" })},
		{"feed on a td instance", "/feeds/0/md_instance", mutate(ok, func(s *apiv1.SessionCreate) {
			s.Feeds = &[]apiv1.FeedDeclInput{{Feed: "orderbook_delta.BinanceUM_Perp_BTCUSDT", MdInstance: tdInst}}
		})},
		{"algo_id out of range", "/accounts/0/algo_ids", mutate(ok, func(s *apiv1.SessionCreate) {
			s.Accounts = &[]apiv1.AccountDeclInput{{Name: "x", ApiId: 1, AlgoIds: []int{1024}}}
		})},
		{"account that does not exist", "/accounts/0/api_id", mutate(ok, func(s *apiv1.SessionCreate) {
			s.Accounts = &[]apiv1.AccountDeclInput{{Name: "x", ApiId: 999_999_999, AlgoIds: []int{1}}}
		})},
		{"params not an object", "/params", rawBody(`{"name":"x","sts_instance":"` + stsInst + `","strategy":{"repository":"` + strategyRepository + `","ref":"` + strategyTag + `"},"params":[1]}`)},
		{"field the API does not take", "/selectors", rawBody(`{"name":"x","sts_instance":"` + stsInst + `","strategy":{"repository":"` + strategyRepository + `","ref":"` + strategyTag + `"},"selectors":[]}`)},
	}
	for _, c := range cases {
		body, err := c.body()
		if err != nil {
			return err
		}
		resp, err := f.a.CreateSessionWithBodyWithResponse(f.ctx, nil, "application/json", bytes.NewReader(body))
		if err != nil {
			return err
		}
		if resp.StatusCode() != http.StatusBadRequest {
			return fmt.Errorf("%s: %w", c.name, statusOf(resp.HTTPResponse, resp.Body))
		}
		p, err := problem(resp.Body)
		if err != nil {
			return err
		}
		if p.Code != "validation_failed" || p.Errors == nil || !slices.ContainsFunc(*p.Errors, func(e apiv1.FieldError) bool { return e.Path == c.path }) {
			return fmt.Errorf("%s: want validation_failed at %s, got %s", c.name, c.path, resp.Body)
		}
		say("%s: 400 %s at %s", c.name, p.Code, c.path)
	}
	after, err := f.bothIDs()
	if err != nil {
		return err
	}
	if !slices.Equal(before, after) {
		return fmt.Errorf("sessions changed: before %v, after %v", before, after)
	}
	say("no session was written (%d before and after)", len(after))
	return f.audited()
}

func mutate(base apiv1.SessionCreate, change func(*apiv1.SessionCreate)) func() ([]byte, error) {
	return func() ([]byte, error) {
		s := base
		change(&s)
		return json.Marshal(s)
	}
}

func rawBody(s string) func() ([]byte, error) {
	return func() ([]byte, error) { return []byte(s), nil }
}

// bothIDs lists the sessions on both replicas and requires them to agree.
func (f *flow) bothIDs() ([]string, error) {
	var a, b []string
	err := until(f.ctx, 15*time.Second, "both replicas list the same sessions", func() error {
		var err error
		if a, _, err = f.sessionIDs(f.a); err != nil {
			return err
		}
		if b, _, err = f.sessionIDs(f.b); err != nil {
			return err
		}
		slices.Sort(a)
		slices.Sort(b)
		if !slices.Equal(a, b) {
			return fmt.Errorf("a %v, b %v", a, b)
		}
		return nil
	})
	return a, err
}

// absentFlow is run around a stopped controller. create: both replicas report controller_absent
// (E10-D014); the API still writes a spec, and no assignment appears. resume: after the controller
// is back, the same session is assigned, runs, and is ended and deleted.
func absentFlow() error {
	step := popStep()
	f, cancel, err := flowFromFlags()
	if err != nil {
		return err
	}
	defer cancel()
	switch step {
	case "create":
		if err := f.waitAbsent(true); err != nil {
			return err
		}
		s, err := f.create(f.a, idleSession("accept-absent", stsInst), "")
		if err != nil {
			return fmt.Errorf("create without a controller: %w", err)
		}
		say("session %s written while the controller is away", s.Id)
		time.Sleep(5 * time.Second)
		got, _, err := f.get(f.b, s.Id)
		if err != nil {
			return err
		}
		if !got.Freshness.ControllerAbsent {
			return errors.New("replica b does not mark the session stale")
		}
		if got.Assignment != nil || got.Status != nil {
			return fmt.Errorf("session %s has an assignment or status without a controller", s.Id)
		}
		say("replica b shows it with controller_absent, no assignment, no status")
		if err := remember("absent", s.Id); err != nil {
			return err
		}
	case "resume":
		id, err := recall("absent")
		if err != nil {
			return err
		}
		if err := f.waitAbsent(false); err != nil {
			return err
		}
		if _, err := f.waitPhase(f.a, id, 2*time.Minute, apiv1.PhaseRunning); err != nil {
			return err
		}
		if err := f.endAndClear(f.b, id); err != nil {
			return err
		}
	default:
		return errors.New("absent create|resume")
	}
	return f.audited()
}

func (f *flow) waitAbsent(want bool) error {
	return until(f.ctx, time.Minute, fmt.Sprintf("controller_absent %v on both replicas", want), func() error {
		for name, c := range map[string]*apiv1.ClientWithResponses{"a": f.a, "b": f.b} {
			_, fresh, err := f.sessionIDs(c)
			if err != nil {
				return err
			}
			if fresh.ControllerAbsent != want {
				return fmt.Errorf("replica %s: controller_absent %v", name, fresh.ControllerAbsent)
			}
		}
		say("both replicas: controller_absent %v", want)
		return nil
	})
}

// regionFlow is run around a stopped etcd2 (region accept2, where sts2 lives). before: a session
// runs on sts2. down: both replicas stay ready and name accept2 unavailable; its session and
// writes to it are 503 region_unavailable; region accept still takes a session that runs. up:
// accept2 is back, its session still runs, and is ended and deleted.
func regionFlow() error {
	step := popStep()
	f, cancel, err := flowFromFlags()
	if err != nil {
		return err
	}
	defer cancel()
	switch step {
	case "before":
		if err := f.waitUnavailable(nil); err != nil {
			return err
		}
		s, err := f.create(f.a, idleSession("accept-region2", sts2Inst), "")
		if err != nil {
			return err
		}
		if _, err := f.waitPhase(f.a, s.Id, 2*time.Minute, apiv1.PhaseRunning); err != nil {
			return err
		}
		if err := remember("region2", s.Id); err != nil {
			return err
		}
	case "down":
		id, err := recall("region2")
		if err != nil {
			return err
		}
		if err := f.waitUnavailable([]string{region2}); err != nil {
			return err
		}
		for name, c := range map[string]*apiv1.ClientWithResponses{"a": f.a, "b": f.b} {
			if _, code, err := f.get(c, id); code != http.StatusServiceUnavailable || !strings.Contains(fmt.Sprint(err), "region_unavailable") {
				return fmt.Errorf("replica %s: session in %s answers %d %v", name, region2, code, err)
			}
		}
		say("session %s in %s: 503 region_unavailable on both replicas", id, region2)
		if _, err := f.create(f.b, idleSession("accept-region2-down", sts2Inst), ""); err == nil || !strings.Contains(err.Error(), "region_unavailable") {
			return fmt.Errorf("create on %s while its region is down: %v", sts2Inst, err)
		}
		say("create on %s: 503 region_unavailable", sts2Inst)
		s, err := f.create(f.b, idleSession("accept-region1", stsInst), "")
		if err != nil {
			return fmt.Errorf("create on %s while %s is down: %w", stsInst, region2, err)
		}
		if _, err := f.waitPhase(f.a, s.Id, 2*time.Minute, apiv1.PhaseRunning); err != nil {
			return err
		}
		if err := f.endAndClear(f.a, s.Id); err != nil {
			return err
		}
	case "up":
		id, err := recall("region2")
		if err != nil {
			return err
		}
		if err := f.waitUnavailable(nil); err != nil {
			return err
		}
		if _, err := f.waitPhase(f.b, id, 2*time.Minute, apiv1.PhaseRunning); err != nil {
			return err
		}
		if err := f.endAndClear(f.b, id); err != nil {
			return err
		}
	default:
		return errors.New("region before|down|up")
	}
	return f.audited()
}

// waitUnavailable waits until both replicas' /readyz and session list name exactly want.
func (f *flow) waitUnavailable(want []string) error {
	return until(f.ctx, time.Minute, fmt.Sprintf("unavailable_regions %v on both replicas", want), func() error {
		for name, base := range map[string]string{"a": *apiA, "b": *apiB} {
			got, code, err := readyz(f.ctx, base)
			if err != nil {
				return err
			}
			if code != http.StatusOK {
				return fmt.Errorf("replica %s /readyz %d", name, code)
			}
			if !sameSet(got, want) {
				return fmt.Errorf("replica %s /readyz unavailable_regions %v", name, got)
			}
		}
		for name, c := range map[string]*apiv1.ClientWithResponses{"a": f.a, "b": f.b} {
			_, fresh, err := f.sessionIDs(c)
			if err != nil {
				return err
			}
			if !sameSet(fresh.UnavailableRegions, want) {
				return fmt.Errorf("replica %s sessions unavailable_regions %v", name, fresh.UnavailableRegions)
			}
		}
		say("both replicas ready; unavailable_regions %v", want)
		return nil
	})
}

// readyz is not under /api and is not audited (E10-D010).
func readyz(ctx context.Context, base string) ([]string, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+"/readyz", nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	var body struct {
		UnavailableRegions []string `json:"unavailable_regions"`
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	if err := json.Unmarshal(b, &body); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("/readyz %d: %s", resp.StatusCode, b)
	}
	return body.UnavailableRegions, resp.StatusCode, nil
}

func sameSet(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

// replicasFlow sends the same writes to both replicas at once (E10-D006, E10-D068). One Idempotency-Key
// sent n times to each gives one session; n different keys give n sessions with distinct ids; both
// replicas list them all; ending on one is seen on the other.
func replicasFlow() error {
	n := flag.Int("n", 4, "requests per replica in each round")
	f, cancel, err := flowFromFlags()
	if err != nil {
		return err
	}
	defer cancel()

	key := randomHex(16)
	body := idleSession("accept-replicas-same", stsInst)
	same, err := f.burst(*n, func(c *apiv1.ClientWithResponses, _ int) (string, error) {
		s, err := f.create(c, body, key)
		if err != nil {
			return "", err
		}
		return s.Id, nil
	})
	if err != nil {
		return err
	}
	slices.Sort(same)
	same = slices.Compact(same)
	if len(same) != 1 {
		return fmt.Errorf("one Idempotency-Key on two replicas made sessions %v", same)
	}
	say("%d creates with one Idempotency-Key on both replicas: one session %s", 2**n, same[0])

	distinct, err := f.burst(*n, func(c *apiv1.ClientWithResponses, i int) (string, error) {
		s, err := f.create(c, idleSession(fmt.Sprintf("accept-replicas-%d", i), stsInst), randomHex(16))
		if err != nil {
			return "", err
		}
		return s.Id, nil
	})
	if err != nil {
		return err
	}
	ids := slices.Clone(distinct)
	slices.Sort(ids)
	if len(slices.Compact(ids)) != len(distinct) {
		return fmt.Errorf("%d creates with distinct keys gave ids %v", len(distinct), distinct)
	}
	say("%d creates with distinct keys on both replicas: %d distinct ids", len(distinct), len(distinct))

	all := append(same, distinct...)
	listed, err := f.bothIDs()
	if err != nil {
		return err
	}
	for _, id := range all {
		if !slices.Contains(listed, id) {
			return fmt.Errorf("session %s is not listed", id)
		}
	}
	say("both replicas list all %d", len(all))

	for i, id := range all {
		ender, viewer := f.a, f.b
		if i%2 == 1 {
			ender, viewer = f.b, f.a
		}
		if err := f.end(ender, id); err != nil {
			return fmt.Errorf("end %s: %w", id, err)
		}
		if err := until(f.ctx, 15*time.Second, "the other replica sees the end", func() error {
			s, _, err := f.get(viewer, id)
			if err != nil {
				return err
			}
			if s.Spec.DesiredState != apiv1.DesiredStateEnded {
				return fmt.Errorf("desired_state %s", s.Spec.DesiredState)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for _, id := range all {
		if _, err := f.waitPhase(f.a, id, 90*time.Second, apiv1.PhaseDone, apiv1.PhaseFailed); err != nil {
			return err
		}
		if err := f.remove(f.b, id); err != nil {
			return err
		}
	}
	say("ended across replicas, terminal, and deleted")
	return f.audited()
}

// burst runs do n times on each replica at once and returns what each call returned.
func (f *flow) burst(n int, do func(c *apiv1.ClientWithResponses, i int) (string, error)) ([]string, error) {
	out := make([]string, 2*n)
	errs := make([]error, 2*n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range 2 * n {
		c := f.a
		if i%2 == 1 {
			c = f.b
		}
		wg.Go(func() {
			<-start
			out[i], errs[i] = do(c, i)
		})
	}
	close(start)
	wg.Wait()
	return out, errors.Join(errs...)
}
