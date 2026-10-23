package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/platform/shim"
)

// mainFlow is #12's done criterion through the API alone: create an account, deploy hello on it,
// see the account and the feed READY and the session RUNNING, find its log on this host and see
// hello's order cancelled in it, end it, see it terminal, wait for W5 to delete the spec and for the
// STS agent to clear the log, delete the account, and find every request in audit.
func mainFlow() error {
	credsFile := flag.String("creds", "/work/binanceum.creds.json", "credentials JSON for the account ({key_type, key, secret[, passphrase]})")
	kind := flag.String("kind", "BinanceUM", "account kind")
	ticker := flag.String("ticker", "BinanceUM_Perp_BTCUSDT", "the ticker hello quotes")
	topic := flag.String("topic", "orderbook_delta", "the book feed's topic")
	algo := flag.Int("algo", 1, "algo_id hello trades under")
	offset := flag.Float64("offset", 2000, "hello's buy limit is this far below the best bid")
	qty := flag.Float64("qty", 0.002, "hello's order quantity")
	deleted := flag.Duration("deleted-within", 4*time.Minute, "how long W5 and the STS cleanup may take after the session ends")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	f, err := newFlow(ctx)
	if err != nil {
		return err
	}
	if err := f.mainSteps(*credsFile, *kind, *ticker, *topic, *algo, *offset, *qty, *deleted); err != nil {
		return err
	}
	return f.audited()
}

func (f *flow) mainSteps(credsFile, kind, ticker, topic string, algo int, offset, qty float64, deleted time.Duration) error {
	me, err := f.a.GetMeWithResponse(f.ctx)
	if err != nil {
		return err
	}
	if me.JSON200 == nil {
		return statusOf(me.HTTPResponse, me.Body)
	}
	say("signed in as %s", me.JSON200.Name)

	kinds, err := f.a.ListAccountKindsWithResponse(f.ctx)
	if err != nil {
		return err
	}
	if kinds.JSON200 == nil {
		return statusOf(kinds.HTTPResponse, kinds.Body)
	}
	if !slices.ContainsFunc(kinds.JSON200.Items, func(k apiv1.AccountKind) bool { return k.Kind == kind && k.Supported }) {
		return fmt.Errorf("account kind %s is not listed as supported", kind)
	}

	creds, err := readCreds(credsFile)
	if err != nil {
		return err
	}
	acc, err := f.a.CreateAccountWithResponse(f.ctx, apiv1.AccountCreate{
		Name: "accept-" + strings.ToLower(kind), Kind: kind, TdInstance: tdInst, Credentials: &creds,
	})
	if err != nil {
		return err
	}
	if acc.JSON201 == nil {
		return fmt.Errorf("create account: %w", statusOf(acc.HTTPResponse, acc.Body))
	}
	apiID := acc.JSON201.ApiId
	say("account api_id %d on %s", apiID, tdInst)

	versions, err := f.a.ListStrategyVersionsWithResponse(f.ctx, strategyID, nil)
	if err != nil {
		return err
	}
	if versions.JSON200 == nil {
		return statusOf(versions.HTTPResponse, versions.Body)
	}
	if !slices.ContainsFunc(versions.JSON200.Items, func(t apiv1.StrategyTag) bool { return t.Tag == strategyTag }) {
		return fmt.Errorf("%s has no version tagged %s: was it packed?", strategyID, strategyTag)
	}

	params, err := json.Marshal(map[string]any{
		"tickers": []string{ticker}, "api_id": apiID, "algo_id": algo, "offset": offset, "qty": qty,
	})
	if err != nil {
		return err
	}
	raw := json.RawMessage(params)
	feed := topic + "." + ticker
	body := apiv1.SessionCreate{
		Name: "accept-main", StsInstance: stsInst,
		Strategy: apiv1.StrategySelector{Id: strategyID, Ref: strategyTag},
		Params:   &raw,
		Feeds:    &[]apiv1.FeedDeclInput{{Feed: feed, MdInstance: mdInst}},
		Accounts: &[]apiv1.AccountDeclInput{{Name: "main", ApiId: apiID, AlgoIds: []int{algo}}},
	}
	created, err := f.create(f.a, body, randomHex(16))
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	id := created.Id
	say("session %s on %s, strategy %s", id, stsInst, created.Spec.Strategy.Digest)

	s, err := f.waitPhase(f.a, id, 3*time.Minute, apiv1.PhaseRunning)
	if err != nil {
		return err
	}
	if err := f.depsReady(id, feed, apiID); err != nil {
		return err
	}
	logs := s.Logs
	if logs == nil {
		return fmt.Errorf("session %s is running but shows no log location", id)
	}
	if err := checkLogs(logs); err != nil {
		return err
	}
	say("log on %s: %s", logs.Host, logs.Stdout)
	if err := until(f.ctx, 2*time.Minute, "hello's order cancelled in its stdout", func() error {
		b, err := os.ReadFile(logs.Stdout)
		if err != nil {
			return err
		}
		if !strings.Contains(string(b), `"hook":"OnRecon"`) || !strings.Contains(string(b), `"status":"ORDER_STATUS_CANCELLED"`) {
			return errors.New("no OnRecon and cancel lines yet")
		}
		return nil
	}); err != nil {
		return err
	}
	say("stdout has hello's OnRecon and its cancelled order")

	if err := f.end(f.a, id); err != nil {
		return fmt.Errorf("end: %w", err)
	}
	s, err = f.waitPhase(f.a, id, 90*time.Second, apiv1.PhaseDone)
	if err != nil {
		return err
	}
	say("terminal: %s, reason %q, outcome %s", s.Status.Phase, s.Status.Reason, outcomeOf(s))
	if _, err := os.Stat(logs.Stdout); err != nil {
		return fmt.Errorf("the log is gone while the spec is still there (D1016): %w", err)
	}

	if err := until(f.ctx, deleted, "W5 deletes the spec", func() error {
		_, code, err := f.get(f.a, id)
		if code == http.StatusNotFound {
			return nil
		}
		if err == nil {
			return errors.New("still there")
		}
		return err
	}); err != nil {
		return err
	}
	say("session %s deleted by W5", id)
	if err := until(f.ctx, time.Minute, "the STS agent clears the log", func() error {
		for _, p := range []string{logs.Stdout, logs.Stderr} {
			if _, err := os.Stat(p); !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("%s still there", p)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	say("stdout and stderr cleared from %s", logs.Host)

	got, err := f.b.GetAccountWithResponse(f.ctx, apiID)
	if err != nil {
		return err
	}
	if got.JSON200 == nil {
		return statusOf(got.HTTPResponse, got.Body)
	}
	if len(got.JSON200.ReferencedBy) != 0 {
		return fmt.Errorf("account %d still referenced by %v after its session was deleted", apiID, got.JSON200.ReferencedBy)
	}
	del, err := f.a.DeleteAccountWithResponse(f.ctx, apiID, &apiv1.DeleteAccountParams{Revision: got.JSON200.Revision})
	if err != nil {
		return err
	}
	if del.StatusCode() != http.StatusNoContent {
		return fmt.Errorf("delete account: %w", statusOf(del.HTTPResponse, del.Body))
	}
	say("account %d deleted", apiID)
	return nil
}

// depsReady is "the TD account is READY": the feed and the account each show ready, with the
// provider that acked them.
func (f *flow) depsReady(id, feed string, apiID int64) error {
	return until(f.ctx, time.Minute, "dependencies ready", func() error {
		s, _, err := f.get(f.a, id)
		if err != nil {
			return err
		}
		var feedOK, accOK bool
		for _, d := range s.Dependencies {
			switch {
			case d.Kind == apiv1.DependencyKindFeed && d.Feed != nil && *d.Feed == feed:
				feedOK = d.State == apiv1.EntryStateReady && d.ProviderInstance == mdInst
			case d.Kind == apiv1.DependencyKindAccount && d.ApiId != nil && *d.ApiId == apiID:
				accOK = d.State == apiv1.EntryStateReady && d.ProviderInstance == tdInst
			}
		}
		if !feedOK || !accOK {
			return fmt.Errorf("dependencies %s", depsLine(s.Dependencies))
		}
		say("dependencies %s", depsLine(s.Dependencies))
		return nil
	})
}

// checkLogs is the log location the API shows (D1014), on this host: the runner is the STS
// agent's host. The files and their directory have the modes of D1015.
func checkLogs(l *apiv1.LogLocation) error {
	host, err := os.Hostname()
	if err != nil {
		return err
	}
	if l.Host != host {
		return fmt.Errorf("log host %q, this host is %q", l.Host, host)
	}
	for _, p := range []string{l.Stdout, l.Stderr} {
		fi, err := os.Stat(p)
		if err != nil {
			return err
		}
		if fi.Mode().Perm() != shim.LogFileMode {
			return fmt.Errorf("%s has mode %v, want %v", p, fi.Mode().Perm(), shim.LogFileMode)
		}
	}
	dir, err := os.Stat(filepath.Dir(l.Stdout))
	if err != nil {
		return err
	}
	if dir.Mode().Perm() != shim.LogDirMode {
		return fmt.Errorf("%s has mode %v, want %v", filepath.Dir(l.Stdout), dir.Mode().Perm(), shim.LogDirMode)
	}
	return nil
}

func outcomeOf(s *apiv1.SessionDetail) string {
	if s.Status == nil || s.Status.LastExit == nil {
		return "-"
	}
	return string(s.Status.LastExit.Outcome)
}

func readCreds(path string) (apiv1.Credentials, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return apiv1.Credentials{}, err
	}
	var c struct {
		KeyType    string `json:"key_type"`
		Key        string `json:"key"`
		Secret     string `json:"secret"`
		Passphrase string `json:"passphrase"`
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return apiv1.Credentials{}, fmt.Errorf("%s: %w", path, err)
	}
	out := apiv1.Credentials{KeyType: apiv1.KeyType(c.KeyType), Key: c.Key, Secret: c.Secret}
	if c.Passphrase != "" {
		out.Passphrase = &c.Passphrase
	}
	return out, nil
}
