# Test and CI time

Research for #97. Measurements only: no test, CI, or product change.

Measured tree: `abf63969` (`E2-08: SYM service on the sym subject`, #95), which was `main` when the suite was timed. `origin/main` has since merged #96. The CI window below stops at the run of `abf63969`.

Local machine: Linux 6.12.94+ (KVM), 4 vCPU (Intel Xeon, family 6 model 207), 16 GB RAM (`MemTotal` 16398384 kB), no cgroup CPU quota. `GOMAXPROCS=4`, `NumCPU=4`. Go `go1.26.0 linux/amd64`, `CGO_ENABLED=1`. etcd v3.7.2 at `MFTIKO_ETCD_BIN`. PostgreSQL 16.15 (`/usr/lib/postgresql/16/bin/postgres`) at `MFTIKO_PG_BIN`. Every local run set `MFTIKO_REQUIRE_ETCD=1` and `MFTIKO_REQUIRE_PG=1`. The system Postgres cluster was stopped so only the test servers ran.

CI is one `check` job on `ubuntu-24.04` (`.github/workflows/ci.yml`). For a private repo that label is 2 vCPU, 8 GB RAM, 14 GB SSD ([GitHub-hosted runners](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)). `make test-race` is `go test -race -count=1 ./...`.

Percentiles are nearest rank: rank = ceil(p/100 × n), on Actions step timestamps (1 second resolution). For 20 runs, p50 is the 10th value and p90 the 18th.

## 1. CI steps

Window: 80 listed `ci` runs, 78 completed (70 success, 6 failure, 2 cancelled), from 2026-10-08 14:28Z (`37792725391`) through 2026-10-09 14:07Z (`37941894013`, `main`, `abf63969`). Two runs were still in progress when the list was taken. Step times are the latest attempt.

The issue's 5–8 minutes is this mixed window. Recent successes are bimodal: a Go build-cache hit finishes in about 5.3 minutes, and a `go.sum` miss in about 7.3.

`go vet` ≤ 5s means `setup-go` restored the build cache (checked on `37941894013`: 282 MB, key `setup-go-Linux-x64-ubuntu24-go-1.26.0-…`). `go vet` ≥ 20s means a miss (checked on `37919342196`: "Cache is not found", then a save). The npm cache hit on both (~69 MB).

### Recent 20 successes (2026-10-09 09:10Z–14:07Z)

Mixed hits and misses. This is the 5–8 minute band.

| Step | n | p50 (s) | p90 (s) | max (s) |
| --- | ---: | ---: | ---: | ---: |
| Queue before the job | 20 | 3 | 67 | 545 |
| checkout | 20 | 1 | 1 | 2 |
| setup-go | 20 | 17 | 22 | 26 |
| setup-node | 20 | 3 | 4 | 6 |
| Install etcd v3.7.2 | 20 | 1 | 2 | 2 |
| Install PostgreSQL | 19 | 10 | 17 | 18 |
| buf lint | 20 | 5 | 5 | 7 |
| buf breaking | 20 | 0 | 1 | 3 |
| generated code is current | 20 | 4 | 24 | 25 |
| go vet | 20 | 2 | 59 | 63 |
| build | 20 | 3 | 3 | 4 |
| go test -race | 20 | 268 | 327 | 332 |
| svelte-check | 20 | 4 | 5 | 5 |
| **job** | 20 | **332** | **463** | **467** |

`Install PostgreSQL` is missing on one of these 20 because that run predates the step. Across all 70 successes the step exists on 21. Queue p90 is one wait of 545s (`37912609162`, attempt 2) and one of 417s. Excluding waits over 30s, the recent-20 queue median is 3s.

### Same 25 newest successes, split by the vet proxy

19 cache hits (vet ≤ 5s):

| Step | n | p50 (s) | p90 (s) |
| --- | ---: | ---: | ---: |
| setup-go | 19 | 19 | 24 |
| Install PostgreSQL | 13 | 10 | 17 |
| generated code | 19 | 4 | 6 |
| go vet | 19 | 2 | 3 |
| go test -race | 19 | 262 | 272 |
| **job** | 19 | **320** | **336** |

9 cache misses in the whole window (vet ≥ 20s), clustered on `go.sum` changes (NATS around 02:40Z, Postgres schema around 05:57Z and 09:24Z–10:50Z):

| Step | n | p50 (s) | p90 (s) |
| --- | ---: | ---: | ---: |
| setup-go | 9 | 13 | 16 |
| generated code | 9 | 23 | 25 |
| go vet | 9 | 59 | 63 |
| go test -race | 9 | 308 | 332 |
| **job** | 9 | **439** | **467** |

A miss costs about 2 minutes on the job (439 − 320): test step +46s, vet +57s, codegen +19s, setup-go −6s (no 282 MB restore). 9 of 70 successes. The all-history hit median (job 269s, test 223s, n=60) is lower because it includes older, shorter trees. The recent-hit row is the current suite.

On the measured commit, cache-hit run `37941894013` spent 268s in `go test -race` and 333s in the job.

## 2. Per-package time

`go test` prints package elapsed for the test binary. Log lines are buffered, so many `ok` lines share a timestamp; the elapsed field is the one to use. Packages with no tests are omitted (47 of 68).

### CI, `go test -race -count=1`, run `37941894013` (cache hit, this commit)

| Package | Elapsed (s) |
| --- | ---: |
| internal/platform/state | 193.079 |
| internal/platform/agent/testplane | 58.260 |
| internal/platform/shim | 43.894 |
| internal/platform/shim/fakeworker | 8.531 |
| internal/sym | 6.414 |
| internal/platform/natsx | 4.207 |
| internal/platform/agent/c3 | 4.167 |
| migrations | 1.748 |
| contract/wire/frame | 1.608 |
| internal/archtest | 1.460 |
| internal/platform/agent/runner | 1.317 |
| 10 other packages | 1.01–1.03 each |
| **sum** | **334.9** |

The test step was 268s, so package overlap hid about 67s, and the step also compiled. Cache-miss run `37919342196` (older tree, test step 327s) executed the same work: state 193.472s, testplane 58.877s, shim 44.326s. The extra minute in that step is compile, which the package elapsed does not include.

### Local, same commit

`go test -json -count=1 ./...`, empty `GOCACHE` for the non-race run. The race `-json` run followed a one-test race compile of `state`.

| Package | Non-race (s) | Race (s) |
| --- | ---: | ---: |
| internal/platform/state | 186.170 | 189.336 |
| internal/platform/agent/testplane | 45.618 (fail) | 57.302 |
| internal/platform/shim | 8.534 | 42.927 |
| internal/platform/natsx | 3.134 | 4.181 |
| internal/sym | 2.392 (fail) | 3.444 |
| internal/platform/shim/fakeworker | 2.218 | 8.464 |
| internal/platform/agent/c3 | 0.837 | 3.891 |
| migrations | 0.438 | 1.435 |
| internal/platform/agent/runner | 0.307 | 1.317 |
| internal/archtest | 0.208 | 1.311 |
| contract/wire/frame | 0.155 | 1.331 |
| 10 other packages | ≤ 0.014 | 1.01–1.02 |
| **sum** | **250.1** | **325.1** |

Wall clock, 4 cores: non-race cold 212.8s (exit 1), race `-json` 213.8s (exit 0). `state` alone, non-race, warm cache: wall 186.2s, package 185.7s, user CPU 51.6s. It is wait-bound, and the rest of the suite does not slow it.

329 top-level tests, 197 subtests.

## 3. Slowest 20 tests

From the local race `-json -count=1` run (exit 0). Non-race times are the cold `-json -count=1` run. Parent tests only; subtests are inside the parent.

| # | Test | Race (s) | Non-race (s) | Why |
| ---: | --- | ---: | ---: | --- |
| 1 | state `TestTokenExpiry` | 11.87 | 11.84 | One etcd. Token TTL 2s, session TTL 5s. Sleeps 4s, then 6s. |
| 2 | testplane `TestAdoptAfterKill9` | 11.12 | 9.63 | Four workers, one with `ExitAfterMS: 1000`. SIGKILL the old agent, sleep 1s. etcd. |
| 3 | state `TestRecordGoesWithTheAgent` | 10.46 | 10.67 | Session TTL 2s, then the proxy is cut. Polls up to 10s for etcd to drop the lease. The drop takes ~8–10s. |
| 4 | shim `TestExitInfoEndings` | 9.52 | 0.36 | Eight real shim+worker processes. Race: 1.04s × 6, plus 2.04s and 1.24s. Non-race: 0.02s each (one 0.22s). |
| 5 | state `TestMirrorRandomized` | 7.08 | 6.36 | `time.Sleep(4s)` of proxy cuts, compaction, and writes. One etcd. |
| 6 | testplane `TestBeatTimeout` | 7.03 | 4.89 | 15 beats at 100ms, then a 2s silence timeout. Race adds process startup. |
| 7 | testplane `TestTermEnds` | 6.79 | 6.72 | etcd and processes. Revokes the session lease, then sleeps 200ms. |
| 8 | state `TestMirrorAfterTokenExpiry` | 5.96 | 5.96 | Sleeps 4s so a 2s token expires. One etcd. |
| 9 | testplane `TestLiveApplied` | 5.27 | 5.25 | Sleeps 1s, 1.5s, and 1s. `LiveRetry` 300ms. |
| 10 | state `TestMirrorLastContact` | 5.05 | 4.92 | Sleeps 5 probes, then 6, at 200ms. |
| 11 | state `TestMirrorReconnects` | 4.96 | 4.86 | Three rounds of a 300ms cut, then reconnect. |
| 12 | state `TestMirrorQuietAfterReconnect` | 4.87 | 4.83 | 5 probes, a 300ms cut, then 10 probes, at 200ms. |
| 13 | testplane `TestStartsAndStops` | 4.75 | 2.72 | etcd, a shim, and `time.Sleep(1s)`. Race is about 2× on process start. |
| 14 | testplane `TestRotation` | 4.66 | 2.73 | Two agents, sleep 500ms. Process-bound under `-race`. |
| 15 | state `TestMirrorQuietSharedStream` | 4.64 | 4.59 | Sleeps 15 × 200ms. |
| 16 | state `TestMirrorProbeUnderLoad` | 4.57 | 4.54 | Sleeps 3s. Each `Get` is delayed 30ms. |
| 17 | shim `TestWorkerOutputIncarnations` | 4.10 | 0.05 | Two shim processes and a 200KiB flood. Race process startup. |
| 18 | state `TestRecordGoesWithSupersededTerm` | 4.07 | 4.06 | Waits out a 2s session TTL. |
| 19 | state `TestMirrorLeaseExpiry` | 4.06 | 4.08 | `Grant` TTL 1s, then polls until the key leaves the mirror. |
| 20 | state `TestWritesTwoRegions` | 3.85 | 3.76 | Two etcd processes. |

`TestPutRecord`, one etcd and no deliberate sleep, is 1.64s race and 1.65s non-race in the full suite. Three warm non-race runs took 4.87s of package time (~1.62s each). That is the cold-start floor.

Inside `state` (race, 82 tests, sum 188.3s):

| Band | Tests | Sum (s) |
| --- | ---: | ---: |
| ≤ 10ms, no etcd | 16 | 0.01 |
| 1.40–2.16s, one cold etcd | 34 | 57.6 (median 1.65) |
| > 2.2s, sleeps or a second etcd | 32 | 130.7 |

The 16 fast tests: `TestAgentRegions`, `TestAuthRefused`, `TestAuthTables`, `TestCampaignArgs`, `TestClustersAllowedAndLazy`, `TestEndAndBind`, `TestKeys`, `TestMirrorResumePoint`, `TestNewClustersRejects`, `TestOwnerRejects`, `TestParseEpoch`, `TestRegionOf`, `TestRouteTxn`, `TestValidate`, `TestWorkerHash`, `TestWriteScope` (10ms; the rest are ~0).

The 11 `state` tests over 4s sum 67.6s. About 1.6s of each is the etcd boot; the rest is the sleeps and the lease wait in the table above.

## 4. Cost of `-race` and of `-count=1`

### `-race`

| | Non-race | Race |
| --- | ---: | ---: |
| `state` package | 186.2s | 189.3s (~2%) |
| `shim` package | 8.5s | 42.9s |
| `fakeworker` | 2.2s | 8.5s |
| `c3` | 0.84s | 3.9s |
| `migrations` | 0.44s | 1.4s |
| each tiny package | ≤ 0.02s | ~1.01s |
| 4-core wall, full rerun | 191.9s (`-count=1`, warm, exit 0) | 199s (warm, sym flake) and 213.8s (`-json -count=1`, exit 0) |
| 2-core wall, `-race -count=1`, warm | | 255.7s (exit 0) |

`state` is not race-bound. The race cost is about one second of runtime startup per process. `TestExitInfoEndings` is eight processes: 0.36s becomes 9.52s. `TestWorkerOutputIncarnations` goes from 0.05s to 4.10s.

On 4 cores, `shim` and `testplane` hide behind `state`, so the wall clock moves by roughly 10%. On 2 cores they do not. Local `GOMAXPROCS=2` took 255.7s with `state` still at 188.8s; package sum was 324.2s, so overlap saved 68s. CI on this commit took 268s in the test step with `state` at 193s. The two numbers are the same suite on 2 cores.

Dropping `-race` would leave `state` (~186s) as the pole. On the CI runner the test step would fall by about a minute, the part of `shim` and `testplane` that currently sticks out past `state`. It would not cut the 189s.

### `-count=1`

Go 1.26 caches a successful package test when every test flag is in `-benchtime`, `-coverprofile`, `-cpu`, `-failfast`, `-fullpath`, `-list`, `-outputdir`, `-parallel`, `-run`, `-short`, `-skip`, `-timeout`, `-v` (`go help test`). `-count` and `-json` are outside that set, so either one disables the cache. `-race` is kept as a separate binary: a second `go test -race ./...` printed `(cached)`.

| Run | Wall | What ran |
| --- | ---: | --- |
| Non-race `go test ./...`, warm | 190.0s | everything (exit 1, testplane flake) |
| Non-race again, cache on | 45.7s | only testplane, 45.1s. Everything else `(cached)`. |
| Non-race `go test -count=1 ./...`, warm | 191.9s | everything again (exit 0) |
| Race `go test -race ./...`, warm | 199.1s | everything (exit 1, sym flake) |
| Race again, cache on | 4.7s | only sym, 3.7s. Everything else `(cached)`. |

A failed package is not cached, which is why the second run redid testplane and then sym. A fully green race rerun with the cache enabled would be a couple of seconds. `setup-go` already restores that cache (~282 MB). `-count=1` is why CI never uses it.

The cache key includes files the test opens inside the module and environment variables it reads. `state` reads `MFTIKO_ETCD_BIN`; Postgres tests read `MFTIKO_PG_BIN`. CI points both at `$RUNNER_TEMP`. The result is reusable only when that path string is the same from job to job. The cache does not hash the etcd or postgres bytes.

What the cache can skip:

| What changed | Test-step time left |
| --- | --- |
| Nothing the tests read (docs, web, comments) | ~0. The whole ~262s median, ~268s on this commit. |
| Anything but `state` and the packages `state` imports | ~193s. `state` still runs, and it is the pole, so the wall clock barely moves. |
| `state` itself | The full test step. Caching the other packages saves ~0 of wall clock while `state` runs. |

## 5. Parallelism

No test calls `t.Parallel`. Nothing calls `testing.Short`. The only build tag on a test file is `//go:build unix` (`contract/wire/frame/socket_test.go`). There is no unit/integration tag.

`-p` defaults to `GOMAXPROCS`. Tests inside one package run one at a time. Nothing takes a package-wide lock or a fixed port. `etcdtest` and `pgtest` pick an ephemeral port and retry the start 3 times (`readyTimeout` 20s for etcd, poll 50ms).

| Cores | Race wall | `state` | Overlap (package sum − wall) |
| --- | ---: | ---: | ---: |
| 4 (local `-json`) | 213.8s | 189.3s | ~111s of 325s |
| 2 (local `-count=1`) | 255.7s | 188.8s | ~68s of 324s |
| 2 (CI `37941894013`) | 268s step | 193.1s | ~67s of 335s |

`state` alone is 186s, the same as `state` inside the suite. More package parallelism does not shorten it. The 4-core win over CI is the ~40–55s of `shim` and `testplane` that a second pair of cores hides behind `state`.

## 6. External services

| Service | How a test starts it | Where the time goes |
| --- | --- | --- |
| etcd v3.7.2 | One process per `etcdtest.Start`. Auth on, JWT ES256, bcrypt password hashes. | The 1.6s floor. bcrypt is "tens of milliseconds" idle and more when busy (`mirror.go`). |
| PostgreSQL 16 | `pgtest`: `initdb` + `postgres` per `Start`, trust auth, its own port. | `migrations` 1.4s race, part of `internal/sym` (3.4s local race, 6.4s on CI). |
| NATS | `natsxtest`: in-process `nats-server` v2.15.0. No extra binary. | `internal/platform/natsx` 4.2s race. |

No test shares a server. `Server.Restart` and `Server.Rebuild` exist and are used by tests that must crash or recreate etcd; those need a private process.

Unit vs integration is the skip in `etcdtest` / `pgtest`: no binary, the test skips; `MFTIKO_REQUIRE_*=1` (set on CI and in these runs) turns that into a failure. `archtest` shells out to `go list` once (~1.3s under `-race`, mostly process startup).

## 7. Job split and caching

On a cache hit the steps that `go test` does not need still run first, in the same job: setup-node (~4s), buf lint (~5s), buf breaking (~1s), codegen (~4s), vet (~2s), build (~3s), and svelte-check (~5s) after the tests. That is about 20–25s of serial work on a second runner's worth of checks.

A test job that only checks out, sets up Go, installs etcd and Postgres, and runs `make test-race` is the critical path (~293s from the recent-hit medians: setup-go 19 + etcd 1 + Postgres 10 + test 262, plus a second of checkout). The lint/codegen/vet/svelte job is ~45s and finishes while tests run. Wall-clock saving is about 25s, provided it is a second runner. On the same 2 vCPUs, vet and codegen would compile while tests run and would contend with them.

`Install PostgreSQL` is `apt-get update` plus `apt-get install` (p50 10s, p90 17s). etcd is already a checked tarball and costs 1–2s; caching it saves nothing that shows up at this resolution. A pinned Postgres binary or a cached deb, the way etcd is installed, removes the apt minute-fraction (~10s).

`setup-go` already caches the module and build cache, and the test-result cache with them. The miss case (§1) is the expensive one: ~2 minutes, 9 of 70 greens, always after `go.sum` changed. There is no extra cache key to add for that. `-count=1` is what discards the test-result half (§4).

## 8. Flaky tests

### `TestMirrorSlowWatchCreation` (#90)

Failed twice on the old test:

| Run | Where | Time | Result |
| --- | --- | ---: | --- |
| `37912609162` attempt 1 | PR #87, `e1/acceptance` | 3.24s | `mirror_test.go:1103` stats `{Lists:1 Watches:2 Errors:1}`: a watch waiting for its confirmation was reopened |
| `37916946715` | `main` `677889b` | 3.43s | same assertion |

#87 did not change `state`. The old test used `ProbeInterval` 50ms, so the stall window was 3 probes = 150ms, and it kept writing after etcd's confirmation. Under `-race` and load, delivery after the confirmation missed that window and the mirror reopened the watch. Production `DefaultMirrorProbeInterval` is 5s, so the same rule is a 15s window.

`146ca5e` (merged as #94, `1adc3b3`, 11:48Z) rewrote the test and does not change `Mirror`. The confirmation is held until the writes are in hand, then events follow by a fixed 200ms inside a 400ms `stallWindow`. Local race time is now 2.30s. It has not failed in the four successes after that merge (`37926010276`, `37937968085`, `37940626307`, `37941894013`). The later failure `37943902680` is a different test. That sample is small.

`8f5c6a8` (`E1-23: a mirror watch that delivers late is not stalled`, branch `e1/mirror-stall`) reads the same CI failures as a detector bug: contact moved only when a probe's read was reached, so a watch that delivered more than one probe interval late was reopened even while events arrived. That commit is not in `abf63969` or current `origin/main`. The failure output matches the old test's margin. Whether the production rule is still too strict is what `e1/mirror-stall` claims; this run did not re-measure it.

### `TestStartsAndStops` (testplane)

Three CI failures, all `shim_release:"dev"` on a `RUNNING` status, all well under the 20s wait:

| Run | Where | Time | Line then |
| --- | --- | ---: | --- |
| `37911079255` | `main` `b0f5f3a` | 1.93s | `testplane_linux_test.go:330` |
| `37915523860` | PR `f1666af` | 1.78s | `:330` |
| `37916946715` | `main` `677889b` | 2.47s | `:336` |

`buildinfo.Version` is `"dev"` when the test binary is not linked with ldflags. The fake worker's Hello release defaults to `"fake"`. The assertion is `GetRelease() == "fake"` at the first `RUNNING` status (`testplane_linux_test.go:335` on this tree). The `time.Sleep(1s)` later in the test ("50 Beats, 20 resyncs", revision must not change) was not the line that failed. On `37916946715` the agent log also recorded the worker-status write's context canceled and "lost the shim". Run alone here: package 13.3s, exit 0, about 2.7s per iteration across five counts. In the full race suite it passed in 4.75s.

The first `RUNNING` status is observed before Hello's release is stored, and a canceled etcd write drops the shim. The test treats the first `RUNNING` as the steady state.

### `TestShimKilled` (same package, reproduced here, not in the CI list above)

`accept_linux_test.go:213`: the worker journalled 0 EOFs on fd 3. The agent log shows `shim exited` with `signal: killed`, then "lost the shim", then incarnation 2. Failed in the full non-race suite (1.81s) and twice in an isolated package run of 8.66s (1.80s and 1.71s), which is about five iterations. Passed in the race `-json` suite and in the `GOMAXPROCS=2` run.

The shim's own `TestShimKilledWorkerSeesEOF` sets `IgnoreSIGTERM` because `Pdeathsig` is `SIGTERM` (`spawn_linux.go`) and it arrives at the same moment as EOF on fd 3. `TestShimKilled` does not set that, and it requires exactly one EOF.

### `TestReloadDoesNotPublishHalfTable` (`internal/sym`)

Fails when four readers never observe both parities of a 40-iteration reload: `seen[0]==0 || seen[1]==0`. Under load the readers only sample the final even generation.

| Where | Result |
| --- | --- |
| Local non-race full suite | 0.00s, `even=4 odd=0` |
| Local race warm full suite | 0.00s, `even=5 odd=0` |
| CI `37943902680` (D008 PR, `8a659ba`) | 0.01s, `even=178 odd=0` |
| Run alone here | exit 0, package 0.009s |

Passed again when it was the only uncached package (3.7s). The failure is the sampling, not a decimal change: D008's CI hit the same `even`/`odd` check.

### Other red runs in the window

`37856223563` `TestC3StopOutlivesAgent` (10.09s) and `37813324019` `TestStreamNeverBlocks` (0.32s). Neither is in the top 20. Failed runs still spent 201–321s in `go test`; a flake does not make CI fail fast.

## 9. Recommendations

Savings are against the CI test step and the job, because `state` is the critical path on both 2 and 4 cores. The etcd-reuse and sleep numbers are estimates from the measured 1.64s floor and the sleeps in §3. They were not prototyped.

| # | Change | Estimated saving | Risk |
| --- | --- | --- | --- |
| 1 | Reuse one warm etcd across `state` tests. Reset keys, leases, and users between tests. Keep a private server for `Restart`, `Rebuild`, and a non-default token TTL. | ~50s from the 34-test floor (1.65s → ~0.2s, the 0.2s is assumed). On the order of 80–100s if the other single-server tests also stop paying a cold start. The job drops by the same amount. | Medium. A leaked key, role, or lease contaminates the next test. Auth setup still costs if it is redone every test. Two-member tests still need two processes. |
| 2 | Shorten the fixed waits in the 11 `state` tests over 4s (token sleeps of 4s and 6s, mirror sleeps of 3–4s, probe sleeps at 200ms, and the ~8s lease-expiry wait past a 2s TTL). Revoke the lease where the test only needs the key gone. | ~30–40s, in series on the pole. The wait portion of those 11 tests is ~50s after the 1.6s boot. | Low to medium. Keep the assertion. One test that really waits out etcd's expiry can stay slow. |
| 3 | Keep `-race`. | Removing it saves about a minute on the 2-vCPU runner (the part of `shim` and `testplane` past `state`) and ~2% of `state`. | High. The detector goes away. `shim` under `-race` is where several of the flakes show up. |
| 4 | Stop passing `-count=1` once the flakes in §8 are fixed. `setup-go` already restores the cache. Keep `-count=1` on `state`, `shim`, `testplane`, `sym`, and `migrations` until then. | Up to the whole test step (~262s recent-hit median, 268s on this commit) when nothing the tests read has changed. ~0 of wall clock if `state` still runs. | A green flake is cached until inputs change. A failed package is not cached (measured). The etcd/postgres path in the environment has to stay stable (§4). |
| 5 | Run `go test` in a job that only needs Go, etcd, and Postgres. Put buf, codegen, vet, build, and svelte-check in a parallel job. Install Postgres from a pinned binary or a cached deb. | ~25s of serial checks leave the critical path. apt-get is another ~10s (p50). etcd's download is 1–2s. | Low, on a second runner. On one runner the compiles contend with the tests for 2 cores. |
| 6 | `t.Parallel` inside `state`. | Small next to a shared etcd. Overlapping cold starts on 2 cores still pays every boot, and the package is already the whole pole. | Medium. The load flakes in §8 get more room. |

Doing 1 and 2 is the time cut. Doing 4 is the large cut on PRs that do not touch `state`, and it waits on the flakes. Doing 5 is the small, safe cut. Doing 3 is not where the time is.

## 10. Follow-up tickets

Titles and scope only. Not opened.

1. **Reuse one warm etcd across `state` tests.** Add a reset that drops keys, leases, and users without re-executing etcd, or one server in `TestMain`. Tests that call `Restart`, `Rebuild`, or `TokenTTL` keep a private `Start`. Record package time before and after. Target: remove the 1.6s floor.

2. **Shorten the deliberate waits in `state`.** Cover `TestTokenExpiry`, `TestMirrorAfterTokenExpiry`, `TestMirrorRandomized`, `TestMirrorQuietSharedStream`, `TestMirrorProbeUnderLoad`, `TestMirrorLastContact`, `TestMirrorQuietAfterReconnect`, `TestMirrorReconnects`, `TestRecordGoesWithTheAgent`, `TestRecordGoesWithSupersededTerm`, and `TestMirrorLeaseExpiry`. Use a shorter TTL or probe, or revoke the lease, and keep the assertion. Do not decide a stall by sleeping (already true of `TestMirrorSlowWatchCreation`).

3. **Make `TestShimKilled`, `TestStartsAndStops`, and `TestReloadDoesNotPublishHalfTable` deterministic under `-race` and `GOMAXPROCS=2`.** `TestShimKilled` should ignore `SIGTERM` the way `TestShimKilledWorkerSeesEOF` does, or accept either EOF or `SIGTERM`. `TestStartsAndStops` should wait until `shim_release` is `"fake"`, not the first `RUNNING`. `TestReloadDoesNotPublishHalfTable` should make the readers overlap the 40 reloads, or stop requiring both parities from a sample that can fall entirely after the writer finishes.

4. **Start `go test` beside lint and codegen, and stop `apt-get update` for Postgres.** A second job, or steps reordered so the test job needs only Go, etcd, and Postgres. Keep the `Makefile` `CHECKS` list as the set of checks. Install Postgres from a pinned binary or a cached deb, as etcd already is.

5. **Drop `-count=1` after ticket 3.** Change `make test-race`. Document that `setup-go` already restores the build cache and the test-result cache, and that `MFTIKO_ETCD_BIN` / `MFTIKO_PG_BIN` must be stable paths. Until ticket 3, limit the cache to packages that do not start etcd, Postgres, or a shim.
