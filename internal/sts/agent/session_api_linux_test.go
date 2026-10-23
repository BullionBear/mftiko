package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/api"
	"github.com/BullionBear/mftiko/internal/platform/pgtest"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
	"github.com/BullionBear/mftiko/migrations"
)

// sessionAPIToken is the bearer the test sends. The API stores only its SHA-256 (D1009).
const sessionAPIToken = "tok-0123456789abcdef0123456789abcdef"

// TestScenarioSessionCreateAndEnd is the E10-11 acceptance: after create, the controller
// derives a demand and STS starts a worker; after end, the session is terminal (D1073, #316).
// D1073 names a testplane end-to-end test. This file is that harness. testplane cannot decode
// SessionDemand, and the API package cannot import the STS agent. The controller daemon, the
// fake worker, and the in-process registry already live here.
func TestScenarioSessionCreateAndEnd(t *testing.T) {
	e := setup(t)
	pg := pgtest.Start(t)
	applySessionSchema(t, pg.DSN())

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listen := ln.Addr().String()
	ln.Close()

	dep, err := json.Marshal(struct {
		Regions   map[string]state.Region `json:"regions"`
		Instances map[string]string       `json:"instances"`
		Planes    map[string]string       `json:"planes"`
	}{
		Regions:   e.cfg.Topology.Regions,
		Instances: e.cfg.Topology.Instances,
		Planes:    map[string]string{testInst: "sts"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(sessionAPIToken))
	conf, err := json.Marshal(map[string]any{
		"repositories": []string{"hello"},
		"registry":     e.reg.addr(),
		"plain_http":   true,
		"users": []map[string]string{{
			"name":         "ada",
			"token_sha256": hex.EncodeToString(sum[:]),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	buf := &syncBuf{}
	log := zerolog.New(buf).Level(zerolog.InfoLevel)
	cfg, err := api.Load(bytes.NewReader(dep), bytes.NewReader(conf), api.Input{
		EtcdPassword: etcdtest.Password(state.UserAPI),
		CookieKey:    "cookie-key",
		PostgresDSN:  pg.DSN(),
		Mirror:       state.MirrorOptions{ProbeInterval: 50 * time.Millisecond, Log: &log},
		Log:          log,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Listen = listen
	cfg.ReconcileEvery = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- api.Run(ctx, cfg) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("api: %v\n%s", err, buf.String())
			}
		case <-time.After(15 * time.Second):
			t.Errorf("api did not stop\n%s", buf.String())
		}
		if t.Failed() {
			t.Logf("api log:\n%s", buf.String())
		}
	})

	base := "http://" + listen
	pollSession(t, 30*time.Second, "api ready", func() bool {
		resp, err := http.Get(base + "/readyz")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var raw bytes.Buffer
		_, _ = raw.ReadFrom(resp.Body)
		return resp.StatusCode == http.StatusOK && bytes.Contains(raw.Bytes(), []byte(`"unavailable_regions":[]`))
	})

	code, hdr, body := sessionPost(t, base+"/api/v1/sessions", `{"name":"hello","sts_instance":"sts-tw-1","strategy":{"id":"hello","ref":"`+e.digest+`"}}`)
	if code != http.StatusCreated {
		t.Fatalf("create %d %s\napi log:\n%s", code, body, buf.String())
	}
	var created apiv1.Session
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	if hdr.Get("Location") != "/api/v1/sessions/"+created.Id || created.Id == "" {
		t.Fatalf("location %q id %q", hdr.Get("Location"), created.Id)
	}
	spec := e.sessionSpec(created.Id)
	if spec.GetStrategy().GetRepository() != "hello" || spec.GetStrategy().GetName() != "fake" || spec.GetStrategy().GetCode() != e.digest {
		t.Fatalf("stored strategy %+v", spec.GetStrategy())
	}

	e.startController()
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()

	pollSession(t, 30*time.Second, "worker running", func() bool {
		st := e.status(created.Id)
		return st.GetPhase() == statev1.Phase_PHASE_RUNNING && e.hasDemand(created.Id)
	})

	code, _, body = sessionPost(t, base+"/api/v1/sessions/"+created.Id+"/end", `{}`)
	if code != http.StatusOK {
		t.Fatalf("end %d %s", code, body)
	}
	pollSession(t, 30*time.Second, "session done", func() bool {
		st := e.status(created.Id)
		return st.GetPhase() == statev1.Phase_PHASE_DONE && st.GetReason() == "ended_by_spec"
	})
}

func applySessionSchema(t *testing.T, dsn string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, name := range []string{"0003_audit.up.sql", "0004_session_records.up.sql"} {
		b, err := migrations.FS.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := pgtest.ExecSQL(ctx, conn, string(b)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func sessionPost(t *testing.T, url, body string) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+sessionAPIToken)
	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var raw bytes.Buffer
	if _, err := raw.ReadFrom(resp.Body); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, raw.Bytes()
}

func (e *env) sessionSpec(id string) *sessionv1.SessionSpec {
	e.t.Helper()
	ctx, cancel := e.opCtx()
	defer cancel()
	resp, err := e.root.Get(ctx, state.SessionSpecKey(testInst, id))
	if err != nil {
		e.t.Fatal(err)
	}
	if len(resp.Kvs) != 1 {
		e.t.Fatalf("spec keys %d", len(resp.Kvs))
	}
	spec := &sessionv1.SessionSpec{}
	if err := proto.Unmarshal(resp.Kvs[0].Value, spec); err != nil {
		e.t.Fatal(err)
	}
	return spec
}

func pollSession(t *testing.T, limit time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for {
		if ok() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
