package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// idleSession is hello with no params: it resolves nothing, binds no feed or account, and runs
// until ended. The fault flows use it where only the control plane matters.
func idleSession(name, sts string) apiv1.SessionCreate {
	return apiv1.SessionCreate{
		Name:        name,
		StsInstance: sts,
		Strategy:    apiv1.StrategySelector{Id: strategyID, Ref: strategyTag},
	}
}

func (f *flow) create(c *apiv1.ClientWithResponses, body apiv1.SessionCreate, key string) (*apiv1.Session, error) {
	var params *apiv1.CreateSessionParams
	if key != "" {
		params = &apiv1.CreateSessionParams{IdempotencyKey: &key}
	}
	resp, err := c.CreateSessionWithResponse(f.ctx, params, body)
	if err != nil {
		return nil, err
	}
	if resp.JSON201 == nil {
		return nil, statusOf(resp.HTTPResponse, resp.Body)
	}
	return resp.JSON201, nil
}

// get returns the session, or nil with the HTTP status when there is none to show.
func (f *flow) get(c *apiv1.ClientWithResponses, id string) (*apiv1.SessionDetail, int, error) {
	resp, err := c.GetSessionWithResponse(f.ctx, id)
	if err != nil {
		return nil, 0, err
	}
	if resp.JSON200 == nil {
		return nil, resp.StatusCode(), statusOf(resp.HTTPResponse, resp.Body)
	}
	return resp.JSON200, http.StatusOK, nil
}

func (f *flow) end(c *apiv1.ClientWithResponses, id string) error {
	resp, err := c.EndSessionWithResponse(f.ctx, id)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return statusOf(resp.HTTPResponse, resp.Body)
	}
	return nil
}

// remove deletes a terminal session now, instead of waiting for W5 (D1017).
func (f *flow) remove(c *apiv1.ClientWithResponses, id string) error {
	resp, err := c.DeleteSessionWithResponse(f.ctx, id)
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusNoContent {
		return statusOf(resp.HTTPResponse, resp.Body)
	}
	return nil
}

func phaseOf(s *apiv1.SessionDetail) apiv1.Phase {
	if s.Status == nil {
		return ""
	}
	return s.Status.Phase
}

func terminal(p apiv1.Phase) bool { return p == apiv1.PhaseDone || p == apiv1.PhaseFailed }

// waitPhase polls until the session is in one of phases, printing each phase it passes through.
func (f *flow) waitPhase(c *apiv1.ClientWithResponses, id string, d time.Duration, phases ...apiv1.Phase) (*apiv1.SessionDetail, error) {
	var last apiv1.Phase = "-"
	var got *apiv1.SessionDetail
	err := until(f.ctx, d, fmt.Sprintf("session %s %v", id, phases), func() error {
		s, _, err := f.get(c, id)
		if err != nil {
			return err
		}
		p := phaseOf(s)
		if p != last {
			say("session %s phase %q %s", id, p, depsLine(s.Dependencies))
			last = p
		}
		if slices.Contains(phases, p) {
			got = s
			return nil
		}
		if terminal(p) {
			return stop(fmt.Errorf("terminal %s, reason %q", p, s.Status.Reason))
		}
		return fmt.Errorf("phase %q", p)
	})
	return got, err
}

// endAndClear ends a session, waits for its terminal status, and deletes it.
func (f *flow) endAndClear(c *apiv1.ClientWithResponses, id string) error {
	if err := f.end(c, id); err != nil {
		return fmt.Errorf("end %s: %w", id, err)
	}
	if _, err := f.waitPhase(c, id, 90*time.Second, apiv1.PhaseDone, apiv1.PhaseFailed); err != nil {
		return err
	}
	if err := f.remove(c, id); err != nil {
		return fmt.Errorf("delete %s: %w", id, err)
	}
	say("session %s ended and deleted", id)
	return nil
}

func (f *flow) sessionIDs(c *apiv1.ClientWithResponses) ([]string, apiv1.Freshness, error) {
	var ids []string
	var fresh apiv1.Freshness
	var cursor *string
	for {
		resp, err := c.ListSessionsWithResponse(f.ctx, &apiv1.ListSessionsParams{Cursor: cursor})
		if err != nil {
			return nil, fresh, err
		}
		if resp.JSON200 == nil {
			return nil, fresh, statusOf(resp.HTTPResponse, resp.Body)
		}
		for _, s := range resp.JSON200.Items {
			ids = append(ids, s.Id)
		}
		fresh = resp.JSON200.Freshness
		if resp.JSON200.NextCursor == nil || *resp.JSON200.NextCursor == "" {
			return ids, fresh, nil
		}
		cursor = resp.JSON200.NextCursor
	}
}

func depsLine(deps []apiv1.Dependency) string {
	parts := make([]string, 0, len(deps))
	for _, d := range deps {
		name := ""
		if d.Feed != nil {
			name = *d.Feed
		}
		if d.ApiId != nil {
			name = fmt.Sprintf("api_id %d", *d.ApiId)
		}
		s := fmt.Sprintf("%s %s@%s=%s", d.Kind, name, d.ProviderInstance, d.State)
		if d.Reason != nil && *d.Reason != "" {
			s += "(" + *d.Reason + ")"
		}
		parts = append(parts, s)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// problem decodes an RFC 9457 body.
func problem(body []byte) (apiv1.Problem, error) {
	var p apiv1.Problem
	if err := json.Unmarshal(body, &p); err != nil {
		return p, fmt.Errorf("not a problem body: %s", body)
	}
	return p, nil
}

// stopErr ends an until early: polling more cannot change the answer.
type stopErr struct{ error }

func stop(err error) error { return stopErr{err} }

func (e stopErr) Unwrap() error { return e.error }

func isStop(err error) bool {
	var s stopErr
	return errors.As(err, &s)
}
