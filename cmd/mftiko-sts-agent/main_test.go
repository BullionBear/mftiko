package main

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/sts/agent"
)

func TestParseArgsVersion(t *testing.T) {
	_, version, err := parseArgs([]string{"-version"})
	if err != nil || !version {
		t.Fatalf("version=%v err=%v", version, err)
	}
}

func baseArgs(extra ...string) []string {
	args := []string{
		"-instance", "sts-tw-1",
		"-topology", "topo.json",
		"-etcd-password", "user",
		"-work-dir", "/tmp/sts",
		"-shim", "/usr/bin/mftiko-shim",
		"-data", "/var/lib/mftiko",
		"-nats-account", "Aexample",
		"-nats-signing-key", "/keys/mftiko.nk",
	}
	return append(args, extra...)
}

func TestParseArgsDefaults(t *testing.T) {
	o, version, err := parseArgs(baseArgs("-max-restarts", "9"))
	if err != nil || version {
		t.Fatalf("version=%v err=%v", version, err)
	}
	if o.instance != "sts-tw-1" || o.dataDir != "/var/lib/mftiko" || o.password != "user" {
		t.Fatalf("options %+v", o)
	}
	if o.natsURL != "nats://127.0.0.1:4222" || o.natsAccount != "Aexample" {
		t.Fatalf("nats %+v", o)
	}
	if o.artifactPoll != time.Minute ||
		o.maxArtifactBytes != agent.DefaultMaxArtifactBytes ||
		o.recvWindowMS != agent.DefaultRecvWindowMS ||
		o.hookTimeoutS != agent.DefaultHookTimeoutS ||
		o.startTimeoutS != agent.DefaultStartTimeoutS ||
		o.volumeMB != agent.DefaultVolumeMB ||
		o.volumeInterval != agent.DefaultVolumeInterval ||
		o.grace != agent.DefaultGrace ||
		o.backoffMin != agent.DefaultBackoffMin ||
		o.backoffMax != agent.DefaultBackoffMax ||
		o.restartWindowS != agent.DefaultRestartWindowS ||
		o.maxRestarts != 9 ||
		o.plainHTTPHosts != nil || o.dataRepo != "" ||
		o.logs.Listen != agent.DefaultLogListen || o.logs.Advertise != "" || o.logs.TokenSHA256 != "" ||
		o.logs.MaxStreams != agent.DefaultLogStreams || o.logs.MaxRate != agent.DefaultLogRate ||
		o.logs.MaxBytes != 0 || o.logs.Keep != 0 {
		t.Fatalf("defaults %+v, grace %s", o, o.grace)
	}
	if agent.DefaultGrace != 10*time.Second {
		t.Fatalf("grace default %s", agent.DefaultGrace)
	}
}

func TestParseArgsRegistryHosts(t *testing.T) {
	o, _, err := parseArgs(baseArgs(
		"-plain-http-hosts", "reg.example, 127.0.0.1:5000",
		"-data-repository", "reg.example/data/state",
	))
	if err != nil {
		t.Fatal(err)
	}
	if len(o.plainHTTPHosts) != 2 || o.plainHTTPHosts[0] != "reg.example" || o.plainHTTPHosts[1] != "127.0.0.1:5000" {
		t.Fatalf("hosts %+v", o.plainHTTPHosts)
	}
	if o.dataRepo != "reg.example/data/state" {
		t.Fatalf("data %q", o.dataRepo)
	}

	// A strategy pull does not need one registry address (E10-D122).
	if _, _, err := parseArgs(baseArgs()); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		baseArgs("-plain-http-hosts", "http://reg.example"),
		baseArgs("-plain-http-hosts", "reg.example,reg.example"),
		baseArgs("-data-repository", "state"),
	} {
		if _, _, err := parseArgs(args); err == nil {
			t.Fatalf("accepted %q", strings.Join(args, " "))
		}
	}
}

func TestParseArgsLogFlags(t *testing.T) {
	sum := sha256.Sum256([]byte("log"))
	hash := hex.EncodeToString(sum[:])
	upper := strings.ToUpper(hash)
	rejected := [][]string{
		baseArgs("-log-token-sha256", upper),
		baseArgs("-log-listen", "", "-log-token-sha256", strings.Repeat("z", 64)),
		baseArgs("-log-max-bytes", "100"),
		baseArgs("-log-keep", "101"),
		baseArgs("-log-max-streams", "-1"),
		baseArgs("-log-max-rate", "-1"),
	}
	for _, args := range rejected {
		if _, _, err := parseArgs(args); err == nil {
			t.Fatalf("accepted %q", strings.Join(args, " "))
		}
	}
	o, _, err := parseArgs(baseArgs("-log-listen", "", "-log-max-bytes", "0", "-log-keep", "0"))
	if err != nil || o.logs.Listen != "" || o.logs.MaxBytes != 0 || o.logs.Keep != 0 {
		t.Fatalf("%v %+v", err, o.logs)
	}
	o, _, err = parseArgs(baseArgs(
		"-log-token-sha256", hash,
		"-log-advertise", "h.example:7442",
		"-log-max-bytes", strconv.FormatUint(shim.MinOutputMaxBytes, 10),
		"-log-keep", "2",
	))
	if err != nil || o.logs.TokenSHA256 != hash || o.logs.Advertise != "h.example:7442" ||
		o.logs.MaxBytes != shim.MinOutputMaxBytes || o.logs.Keep != 2 || o.logs.Listen != agent.DefaultLogListen {
		t.Fatalf("%v %+v", err, o.logs)
	}
}
