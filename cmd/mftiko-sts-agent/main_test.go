package main

import (
	"testing"
	"time"

	"github.com/BullionBear/mftiko/internal/sts/agent"
)

func TestParseArgsVersion(t *testing.T) {
	_, version, err := parseArgs([]string{"-version"})
	if err != nil || !version {
		t.Fatalf("version=%v err=%v", version, err)
	}
}

func TestParseArgsDefaults(t *testing.T) {
	o, version, err := parseArgs([]string{
		"-instance", "sts-tw-1",
		"-topology", "topo.json",
		"-etcd-password", "secret",
		"-work-dir", "/tmp/sts",
		"-shim", "/usr/bin/mftiko-shim",
		"-data", "/var/lib/mftiko",
		"-registry", "127.0.0.1:5000",
		"-nats-account", "Aexample",
		"-nats-signing-key", "/keys/mftiko.nk",
		"-max-restarts", "9",
	})
	if err != nil || version {
		t.Fatalf("version=%v err=%v", version, err)
	}
	if o.instance != "sts-tw-1" || o.dataDir != "/var/lib/mftiko" || o.registry != "127.0.0.1:5000" || o.password != "secret" {
		t.Fatalf("options %+v", o)
	}
	if o.natsURL != "nats://127.0.0.1:4222" || o.natsAccount != "Aexample" {
		t.Fatalf("nats %+v", o)
	}
	if o.maxArtifactBytes != agent.DefaultMaxArtifactBytes ||
		o.recvWindowMS != agent.DefaultRecvWindowMS ||
		o.hookTimeoutS != agent.DefaultHookTimeoutS ||
		o.startTimeoutS != agent.DefaultStartTimeoutS ||
		o.volumeMB != agent.DefaultVolumeMB ||
		o.grace != agent.DefaultGrace ||
		o.backoffMin != agent.DefaultBackoffMin ||
		o.backoffMax != agent.DefaultBackoffMax ||
		o.restartWindowS != agent.DefaultRestartWindowS ||
		o.maxRestarts != 9 ||
		o.plainHTTP {
		t.Fatalf("defaults %+v, grace %s", o, o.grace)
	}
	if agent.DefaultGrace != 10*time.Second {
		t.Fatalf("grace default %s", agent.DefaultGrace)
	}
}

func TestParseArgsRequiresRegistry(t *testing.T) {
	_, _, err := parseArgs([]string{
		"-instance", "sts-tw-1",
		"-topology", "topo.json",
		"-etcd-password", "secret",
		"-work-dir", "/tmp/sts",
		"-shim", "/usr/bin/mftiko-shim",
		"-data", "/var/lib/mftiko",
		"-nats-account", "Aexample",
		"-nats-signing-key", "/keys/mftiko.nk",
	})
	if err == nil {
		t.Fatal("missing -registry was accepted")
	}
}
