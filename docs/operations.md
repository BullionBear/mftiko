# 運維：主機、log 與保留期

依 [DECISIONS.md](DECISIONS.md) E10-D014–E10-D017、E10-D021、E10-D022、E10-D080–E10-D083 與 [PROTOCOL.md](PROTOCOL.md) §4.5 S4、§3.2 W5（E10-19，#324）。一期沒有 log 的資料流（E10-D015）：要看 worker 的輸出，就到 console 顯示的主機上讀檔。

## log 在哪裡

每個 worker 的 stdout、stderr 由它的 shim 寫檔，放在該 agent 的 `-work-dir`（以下寫作 `${WORK_DIR}`）底下：

| 檔案 | 內容 |
|---|---|
| `${WORK_DIR}/log/{plane}/{h}.stdout` | worker 的 stdout |
| `${WORK_DIR}/log/{plane}/{h}.stderr` | worker 的 stderr，Go SDK 的 log 也在這裡 |
| `${WORK_DIR}/log/{plane}/{h}.shim.log` | shim 自己的 log（PROTOCOL §4.6） |

- `{plane}` 是 `sts`、`md`、`td`。`h` 是 worker key 的雜湊（`state.WorkerHash`），STS 的 worker key 是 `sts/<session_id>`。
- 檔案依 worker key 累積，不依 incarnation：重啟後的 worker 接著寫同一組檔案，開頭有一行 `[mftiko-shim] <時間> worker <key> incarnation <n>`。
- **session 的 log 不用自己算。** console 的 session 詳細頁與 `GET /api/v1/sessions/{id}` 的 `logs` 列出主機（agent 的 `os.Hostname()`）與 stdout、stderr 的完整路徑，來自該 STS agent 的 `AgentRecord` 裡的 `host`、`work_dir`（E10-D014）。agent 不在時顯示最後已知的值並加註；`AgentRecord` 沒有 `host`（E10-13 之前的 agent）時不顯示。
- shim 寫檔跟不上（暫存超過 2 MiB）或寫入失敗（磁碟滿）時，丟掉的輸出會留一行 `[mftiko-shim] N bytes of stdout lost here: …`。worker 不會因為輸出而阻塞。

## 權限

| | 模式 | 說明 |
|---|---|---|
| `${WORK_DIR}/log/{plane}/` | `0750` | shim 每次啟動都 chmod 成 0750；目錄上已有的 setgid 保留（E10-D015、E10-D022） |
| stdout、stderr、`.shim.log` | `0640` | 新檔建立後 chmod，不受 umask 影響（E10-D021）；已存在的舊檔不改（E10-D022） |

讓運維不必 sudo 就能讀：部署時把 `${WORK_DIR}/log` 的 group 設成運維的 group `mftiko-ops`（E12-D005），並加上 setgid（`chgrp mftiko-ops ${WORK_DIR}/log && chmod 2750 ${WORK_DIR}/log`）。之後建立的 `{plane}` 目錄與檔案都沿用這個 group。worker 與 agent 本身不需要這個 group。只有 `log/` 底下是給運維讀的；`${WORK_DIR}` 的其他東西（shim 的 socket、session 目錄等）不在這個約定裡。

## 主機上的使用者與檔案

依 [DECISIONS.md](DECISIONS.md) E12-D003–E12-D005（E12-01，#374）。

| 服務 | Unix 使用者 | 設定與祕密 | 狀態（`-work-dir`、`${DATA}`） |
|---|---|---|---|
| md-agent | `mftiko-md` | `/etc/mftiko/md-agent/` | `/var/lib/mftiko/md-agent/` |
| td-agent | `mftiko-td` | `/etc/mftiko/td-agent/` | `/var/lib/mftiko/td-agent/` |
| sts-agent | `mftiko-sts` | `/etc/mftiko/sts-agent/` | `/var/lib/mftiko/sts-agent/` |
| controller | `mftiko-controller` | `/etc/mftiko/controller/` | 無 |
| API | `mftiko-api` | `/etc/mftiko/api/` | 無 |
| SYM、md.fetch | `mftiko-sym` | `/etc/mftiko/sym/`、`/etc/mftiko/mdfetch/` | 無 |

- host binary 在 `/opt/mftiko/releases/<tag>/`，`/opt/mftiko/current` 指到現用的一版。安裝、換版與清理用 tarball 裡的 `bin/mftiko-release`（E12-D022–E12-D026，[release.md](release.md)）。
- shim 與 worker 沿用 agent 的使用者。TD 的 seal key 只有 `mftiko-td` 讀得到。
- `/etc/mftiko/<服務>/` 是 `root:<使用者>` 0750；設定檔 0640；祕密檔（NATS creds、signing key、seal key、registry 的 config.json）0600，屬服務的使用者。
- etcd 密碼放在 `/etc/mftiko/<服務>/env`（0600，root），以 `EnvironmentFile=` 帶進 `MFTIKO_ETCD_PASSWORD`。不用 `LoadCredential=`：unit 停下時那個目錄會被刪，agent 停下後還在跑的 worker 就讀不到 creds 了。instance、registry、NATS 也在這份 env，見下方 systemd。
- 改祕密就改檔案，路徑不變，再重啟 unit。
- 同一個使用者底下，策略讀得到 sts-agent 的祕密檔與 shim 的環境，一期接受（E12-D004）。

## systemd

依 [DECISIONS.md](DECISIONS.md) E12-D016–E12-D021（E12-05，#378）。unit 檔在 `deploy/systemd/`，安裝時複製到 `/etc/systemd/system/`。打進發佈 tarball 是 E12-04（#377），這裡不打包。

| unit | Unix 使用者 | 停的方式 |
|---|---|---|
| `mftiko-md-agent.service` | `mftiko-md` | `KillMode=process`，`OOMPolicy=continue`，`TimeoutStopSec=30s` |
| `mftiko-td-agent.service` | `mftiko-td` | 同上 |
| `mftiko-sts-agent.service` | `mftiko-sts` | 同上 |
| `mftiko-controller.service` | `mftiko-controller` | 預設 `KillMode=control-group` |
| `mftiko-api.service` | `mftiko-api` | 預設 `KillMode=control-group`。console 是 `-web /opt/mftiko/current/web`，沒有另一個 unit |
| `mftiko-sym.service` | `mftiko-sym` | 預設 `KillMode=control-group` |
| `etcd.service` | `etcd` | 官方 binary `/usr/bin/etcd`，資料在 `/var/lib/etcd` |
| `nats.service` | `nats` | 官方 binary `/usr/bin/nats-server -c /etc/nats/nats-server.conf` |

- Postgres 用發行版套件的 unit，不在這份清單。md.fetch 沒有 unit：E12-05 的清單沒有它。
- etcd、NATS 的使用者是各自套件的預設使用者，要先存在。資料目錄 `StateDirectory=etcd` 由 systemd 建立；NATS 的儲存看它自己的設定檔。
- 不設 `LimitNOFILE=`、`Type=notify`、`PrivateTmp=`、`ExecStop=`。重啟是 `Restart=always`、`RestartSec=1s`、`StartLimitIntervalSec=0`（寫在 `[Unit]`），不照上游 unit 的重啟政策。
- 跨主機的 TLS 是 E12-06。在那之前流量只走私有網路或 VPN。

**`systemctl kill` 會殺掉整個 cgroup。** `KillMode=process` 管的是 `systemctl stop` 與 `restart`：只對 agent 送訊號，shim 與 worker 留在同一個 cgroup，新的 agent 起來收養。`systemctl kill` 不看 `KillMode=`，預設對 cgroup 裡每一個行程送訊號，worker 會一起死。只停 agent 時用 `stop`、`restart`，或對 MainPID 送訊號。

agent 的 unit 另外：

- 沒有 `ExecStop=`。systemd 送 SIGTERM；`TimeoutStopSec=30s`，逾時的 SIGKILL 只打到 agent。
- 不開 `PrivateTmp=`、`ProtectSystem=`，也不用 `RuntimeDirectory=`。狀態目錄是 `StateDirectory=mftiko/<服務>`，即 `/var/lib/mftiko/<服務>`。`-work-dir` 與 `-data` 都是這個目錄。`${WORK_DIR}/log` 的 `mftiko-ops` 與 setgid 仍是安裝時做的，不是 unit 指令。
- binary 是 `/opt/mftiko/current/bin/…`，`-shim` 是 `/opt/mftiko/current/bin/mftiko-shim`。不帶 `-conn`、`-account`。
- `After=etcd.service` 與 `After=nats.service` 只排順序，沒有 `Requires=`。etcd 不是這個 unit 時（例如驗收用容器），agent 還是起得來。

同一份 unit 用在兩台主機。部署值放在 `/etc/mftiko/<服務>/env`（0600，root），unit 用 `${VAR}` 展開。沒有寫進 unit 的旗標（etcd 密碼、`NATS_URL`、`DATABASE_URL`）由行程讀環境變數，變數名就是旗標原本的預設來源。

| 服務 | env 裡要有的 | 正式環境的值 |
|---|---|---|
| 三種 agent、controller、API | `MFTIKO_ETCD_PASSWORD` | 該服務 etcd user 的密碼 |
| 三種 agent | `MFTIKO_INSTANCE`、`MFTIKO_REGISTRY`、`MFTIKO_PLAIN_HTTP` | registry 是 `ghcr.io`，`MFTIKO_PLAIN_HTTP=false` |
| 三種 agent | `MFTIKO_REGISTRY_CREDS`（可省略） | 空的就是匿名；有帳密時是 Docker `config.json` 的路徑，0600，屬該使用者 |
| md-agent | `NATS_URL`、`MFTIKO_NATS_CREDS` | creds 檔 `/etc/mftiko/md-agent/nats.creds` |
| md-agent | `MFTIKO_MD_ENDPOINTS`、`MFTIKO_MD_REST_ENDPOINTS`（可省略） | 空的就是交易所預設 URL；有覆寫時是 JSON 檔的路徑 |
| td-agent、sts-agent | `NATS_URL`、`MFTIKO_NATS_ACCOUNT` | account 的公鑰。簽章種子是固定路徑 `/etc/mftiko/<服務>/nats-signing.nk` |
| td-agent | `MFTIKO_TD_ENDPOINTS`、`MFTIKO_TD_REST_ENDPOINTS`（可省略） | 同 MD，空的就是預設 |
| sts-agent | `MFTIKO_STS_DATA_REPOSITORY`（可省略） | 資料 artifact 的 repository，不含 registry host |
| API | `MFTIKO_COOKIE_KEY`、`MFTIKO_POSTGRES_DSN` | cookie 的 HMAC 金鑰、Postgres DSN |
| SYM | `DATABASE_URL`、`NATS_URL` | `-nats-creds` 固定 `/etc/mftiko/sym/nats.creds`。`MFTIKO_SYM_FEEDS` 可省略，省略就是全部 feed |
| etcd | `/etc/etcd/etcd.env` 的 `ETCD_NAME`、`ETCD_LISTEN_CLIENT_URLS`、`ETCD_ADVERTISE_CLIENT_URLS`、`ETCD_LISTEN_PEER_URLS`、`ETCD_INITIAL_ADVERTISE_PEER_URLS`、`ETCD_INITIAL_CLUSTER`、`ETCD_INITIAL_CLUSTER_STATE` | 資料目錄由 unit 設成 `/var/lib/etcd`，env 不要改 `ETCD_DATA_DIR` |

`MFTIKO_PLAIN_HTTP` 一定要是 `true` 或 `false`。unit 把它展開成 `-plain-http=${MFTIKO_PLAIN_HTTP}`，空字串這個旗標不收。

## 輪替

- 每個 stream（stdout、stderr 各自）寫到 64 MiB 就輪替：`{h}.stdout` 改名成 `{h}.stdout.1`，原來的 `.1` 改成 `.2`，依此類推。
- 每個 stream 保留 5 個檔案，包含正在寫的那個；最舊的刪掉。所以一個 worker 的 stdout 最多約 320 MiB，stderr 也一樣。
- 這是預設值（`Launch.output.max_bytes`、`keep`）。下限 64 KiB，`keep` 上限 100。
- `{h}.shim.log` 寫到 10 MiB 就輪替，保留 3 個檔案，含正在寫的（E12-D042）。agent 仍把這個檔接到 shim 的 stdout、stderr，不接 pipe；shim 自己重新開啟並輪替，所以 agent 不在之後還寫得進去。三個平面的 shim 都一樣。
- MD、TD 在啟動時，以及之後每小時，刪掉不再出現的 worker key 的 stdout、stderr、`.shim.log` 與輪替檔。最後修改時間要嚴格早於 `-log-retain-days`（預設 7，小於 1 不接受）。mtime 剛好在界線上的留下。這個旗標不寫進 systemd unit。
- 還算「出現」的不刪：agent 看得到的 worker、這一輪計畫裡的、仍留著 `WorkerStatus` 的、接不上的 shim、以及 `{h}.sock` 還在的。正在跑的 worker 的舊輪替檔因此留下，份數由上面的保留數管，不由 7 天管。
- 只刪一般檔。名稱是 16 個十六進位字的 `.stdout`、`.stderr`、`.shim.log`，輪替的數字沒有前導零。其他檔（例如 `notes.txt`）、目錄、symlink 留下。log 目錄不在不是錯誤。socket 目錄讀不了就這一輪完全不刪。
- 清理失敗只寫 warn，agent 不因此退出。STS 不走這條：仍是 spec 被刪之後才清，輪替出來的 `.shim.log.N` 也在那一次刪掉。

## 監控與告警

依 [DECISIONS.md](DECISIONS.md) E12-D038–E12-D043（E12-10，#383）。mftiko 只輸出自己的狀態。磁碟、CPU、記憶體用 node_exporter，不在 mftiko 裡。mftiko 不打 webhook，也不自己判斷門檻。下面的規則是**範例**：`for` 與門檻由部署端決定。

`GET /metrics` 與 `HEAD /metrics` 在 API server，跟 `/healthz` 一樣不認證、不記稽核、不在 OpenAPI。靜態目錄裡若有一個叫 `metrics` 的檔，不會被這個路徑送出去。刮取不帶 bearer。

```yaml
scrape_configs:
  - job_name: mftiko-api
    static_configs:
      - targets: ["api.example.invalid:8080"]
```

| metric | 標籤 | 意義 |
|---|---|---|
| `mftiko_agent_up` | `instance`、`plane`、`region` | 該 region 第一次同步之後，`AgentRecord` 的 key 在就是 1。後來 region 過期仍維持，系列不會消失 |
| `mftiko_controller_leader` | `instance`、`region` | 同上，controller 的 `AgentRecord`（instance 是 `controller`） |
| `mftiko_worker_phase` | `instance`、`phase`、`plane`、`region` | `WorkerStatus.phase` 的數量。`STARTING`、`RUNNING`、`STOPPING`、`BACKOFF`、`EXITED`、`LOST` 連 0 都有；另外的非空 phase 也有 |
| `mftiko_region_available` | `region` | 與 `/readyz` 同一個判斷：第一次同步完成，且 `LastContact` 沒有嚴格舊過 `unavailable_after`（預設 15 秒，E12-D040）。尚未同步是 0 |
| `mftiko_postgres_up` | 無 | 刮取當下 ping 稽核用的 Postgres 連線池。沒有池是 0。`/readyz` 仍不看 Postgres |
| `mftiko_registry_up` | 無 | 刮取當下 `GET /v2/`，最終狀態 200 才是 1。沒設定或 401 是 0 |

第一次同步之前，那個 region 的 agent、controller、worker 系列不出現（不是 0）。API 行程自己抓不到，看 Prometheus 的 `up`，不是 mftiko 的 gauge。

`WorkerStatus` 沒有 `PENDING`、`DEAD` 這兩個 phase。需求項目的狀態是另一個東西：`BACKOFF` 期間需求是 `PENDING`；政策放棄之後的 `EXITED` 對到 `DEAD`；`LOST` 維持最後一次的項目狀態。MD 行程內換 `conn_epoch`、TD 行程內 `DEGRADED`，都不改 `WorkerStatus`，也不是 `DEAD`。API 不連 NATS，也不探測交易所。

```yaml
groups:
  - name: mftiko.example
    rules:
      # 範例。for 由部署端改。
      - alert: MftikoAPIDown
        expr: up{job="mftiko-api"} == 0
        for: 1m
      - alert: MftikoRegionUnavailable
        expr: mftiko_region_available == 0
        for: 1m
      - alert: MftikoAgentDown
        expr: mftiko_agent_up == 0 and on(region) mftiko_region_available == 1
        for: 2m
      - alert: MftikoControllerNoLeader
        expr: mftiko_controller_leader == 0 and on(region) mftiko_region_available == 1
        for: 1m
      - alert: MftikoWorkerLost
        expr: mftiko_worker_phase{phase="LOST"} > 0
        for: 5m
      - alert: MftikoWorkerBackoff
        expr: mftiko_worker_phase{phase="BACKOFF"} > 0
        for: 10m
      - alert: MftikoWorkerExited
        expr: mftiko_worker_phase{phase="EXITED"} > 0
        for: 15m
      - alert: MftikoPostgresDown
        expr: mftiko_postgres_up == 0
        for: 2m
      - alert: MftikoRegistryDown
        expr: mftiko_registry_up == 0
        for: 5m
      - alert: MftikoDiskLow
        expr: node_filesystem_avail_bytes{fstype!~"tmpfs|overlay"} / node_filesystem_size_bytes{fstype!~"tmpfs|overlay"} < 0.1
        for: 10m
```

`MftikoAgentDown` 與 `MftikoControllerNoLeader` 要求同一個 `region` 的 `mftiko_region_available == 1`，避免 region 分割時每一台都叫。`MftikoWorkerExited` 的 `for` 放長：短暫的 `EXITED` 是正常的，status 刪掉之後這個數回到 0；一直留著才對應需求 `DEAD`。`MftikoWorkerBackoff` 對應需求 `PENDING`。`MftikoDiskLow` 用的是 node_exporter，不是 mftiko 的 metric。

#385 的演練清單（實測留到那張票）對到的訊號：

| 演練 | 看哪裡 | 範例告警 |
|---|---|---|
| 輪換 md、td、sts 與 controller，含換版。worker 不斷，新 agent 收養 | 空窗時 `mftiko_agent_up` 或 `mftiko_controller_leader` 變 0，回來變 1。worker 的 phase 維持 `RUNNING` | `MftikoAgentDown`、`MftikoControllerNoLeader`（空窗長過 `for` 才響） |
| 殺 worker。照平面規則重啟，策略端的影響見 §9.5 | phase 離開 `RUNNING`。重啟退避是 `BACKOFF`（需求 `PENDING`）；政策放棄是 `EXITED`（需求 `DEAD`）；shim 不在、最後狀態留著是 `LOST` | `MftikoWorkerBackoff`、`MftikoWorkerExited`、`MftikoWorkerLost` |
| 重啟 agent 的 systemd unit。shim 與 worker 還在，新 agent 收養 | `WorkerStatus` 不綁 lease，phase 維持 `RUNNING`。`AgentRecord` 隨 term 消失，`mftiko_agent_up` 在空窗是 0 | `MftikoAgentDown` |
| 拔掉 agent 與 etcd | API 仍看得到該 region 時，lease 到期後 `mftiko_agent_up` 變 0，`mftiko_region_available` 維持 1。API 也看不見時，`mftiko_region_available` 變 0，agent 的 0 不另外叫 | `MftikoAgentDown` 或 `MftikoRegionUnavailable` |
| 拔掉 agent 與 NATS | `AgentRecord` 與 `WorkerStatus` 都不動。`mftiko_agent_up` 維持 1，phase 維持 `RUNNING`。沒有另開的探測 | 不響。這就是看得到的訊號 |
| 拔掉交易所連線 | 同上。行程內重連不改 `WorkerStatus`，不是 `DEAD` | 不響。`mftiko_agent_up` 維持 1，phase 維持 `RUNNING` |
| 拔掉 controller 與遠端 region | controller 的 `AgentRecord` 在 home，`mftiko_controller_leader` 維持 1。API 到該 region 的 mirror 一起過期時 `mftiko_region_available` 變 0。只斷 controller、API 仍連得上時，這個 gauge 維持 1（W9：controller 維持現狀、不寫該 region） | `MftikoRegionUnavailable`（API 也看不到時）。只斷 controller 時不響 |
| Postgres 停機。API 照 E10-D080 降級，恢復後補上 | `mftiko_postgres_up` 變 0。`/readyz` 不變 | `MftikoPostgresDown` |
| registry 不可用時 spawn 新的 worker | `mftiko_registry_up` 變 0。需求項目維持 `PENDING`（那是項目狀態，不是 `worker_phase`） | `MftikoRegistryDown` |
| 磁碟用量 | node_exporter 的檔案系統剩餘比例。mftiko 不輸出這項 | `MftikoDiskLow` |

## 保留期

session 的東西分兩段清，比照 `docker rm`（E10-D016）：

1. **session 結束（terminal）之後**：STS agent 推完 `state_out`（volume 是空的則立刻），只刪 volume 與 inputs。log、`terminal.json`、incarnation key 與 `sessions/<session_id>/` 留著，所以結束之後在 console 還找得到 log，主機上也讀得到。
2. **spec 被刪之後**：STS agent 刪掉 `terminal.json`、這個 session 的 log 檔（含輪替出來的與 `.shim.log`）、incarnation key，最後是整個 `sessions/<session_id>/`。agent 每輪 reconcile 都會檢查，重啟之後也清得掉。

spec 什麼時候被刪，只由 API 決定（W5，E10-D017）：

- **預設 7 天**，從 terminal 的 `SessionStatus.updated_at` 起算。設定在 API 的部署設定 `retention`（Go duration，例如 `"168h"`）。
- 每個 API 副本每 30 秒的 reconcile 都會掃一次，過期的就刪 spec 與 status；多個副本重複刪除沒有影響（E10-D083）。刪除前先把 `session_records` 那一列關掉，Postgres 不可用時這一輪不刪（E10-D080）。
- 想早點清：對 terminal 的 session 呼叫 `DELETE /api/v1/sessions/{id}`，立即刪除。非 terminal 的回 409，要先 `POST /api/v1/sessions/{id}/end`。
- 刪除寫進 `audit`。spec 刪掉之後，`/session-records` 仍查得到這個 session 的紀錄。

**所以一個 session 的 log 在主機上最多留到「結束 + 保留期 + 一輪 reconcile」**。需要留得更久，在保留期內自行複製。

不隨 spec 刪除的：

- registry 上的資料 artifact（`state_out`）一期不刪，新的 session 可以用 digest 接手；容量由運維管理（E10-D017）。
- `audit` 的列一期不刪（E10-D010）。

## 舊驗收庫補記 schema_migrations

依 [DECISIONS.md](DECISIONS.md) E12-D031（E12-13，#391）。`accept.sh up` 從空資料庫呼叫 `mftiko-migrate up`，不再讀 `accept_migrations`。這段給 #384 的 runbook 收錄。

空的資料庫不要做這段，直接 `mftiko-migrate up`（或 `make migrate-up`）。

開發用的資料庫若已經被舊的 `accept.sh` 套過 `migrations/0001`–`0005`，而 `schema_migrations` 還沒有這些列，手動插入一次，再跑 `mftiko-migrate up` 套更新的檔案。`sha256` 是該 `.up.sql` 原始位元組的 SHA-256（小寫十六進位）。填錯會讓下一次 `up` 停住、不套任何新檔。

在 repo 根目錄（macOS 沒有 `sha256sum` 時，把下面的 `sha256sum` 換成 `shasum -a 256`）：

```sh
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
  -v sha1="$(sha256sum migrations/0001_history.up.sql | awk '{print $1}')" \
  -v sha2="$(sha256sum migrations/0002_symbol.up.sql | awk '{print $1}')" \
  -v sha3="$(sha256sum migrations/0003_audit.up.sql | awk '{print $1}')" \
  -v sha4="$(sha256sum migrations/0004_session_records.up.sql | awk '{print $1}')" \
  -v sha5="$(sha256sum migrations/0005_session_records_final.up.sql | awk '{print $1}')" <<'SQL'
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    sha256     TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL
);
INSERT INTO schema_migrations (version, name, sha256, applied_at) VALUES
    (1, 'history', :'sha1', now()),
    (2, 'symbol', :'sha2', now()),
    (3, 'audit', :'sha3', now()),
    (4, 'session_records', :'sha4', now()),
    (5, 'session_records_final', :'sha5', now());
SQL
mftiko-migrate up
```

`:'sha1'` 這類是 psql 的字串引用，不要改成把雜湊拼進 SQL。舊的 `accept_migrations` 可以留著，`mftiko-migrate` 不讀它。


## 永久退役一個 instance

步驟見 PROTOCOL §3.7（E12-D033–E12-D037）。這一節不在 testnet 上演練；演練是 #385。

1. 先把新的 etcd 角色表套上去。`mftiko-etcd-roles -format etcdctl` 印出的表裡，該 instance 的使用者要能讀自己的 `/mftiko/v1/retired/{inst}`，`api` 要能寫這把精確 key。`EnsureAuth` 只加不收回。角色表還沒重套時，agent 讀這把 key 會 permission denied，當成還沒退役、照常參選，避免升級之後全部起不來。
2. 停掉並取消開機啟動，讓 `AgentRecord` 消失：`systemctl disable --now <unit>`。unit 仍是 `Restart=always`；只 `stop` 的話 systemd 會把它拉起來。等 `mftiko instance show <inst>` 的 agent 不在。
3. `mftiko instance retire <inst>`。確認之後才送。`AgentRecord` 還在是 exit 13，`agent record is present`，什麼都沒寫。agent 不在時再執行一次仍成功。
4. controller 看到 `/retired/{inst}` 之後：STS 上還沒結束的 session，`SessionDemand.end_reason` 寫成 `instance_retired`，再依 N4 的順序刪 MD、TD 需求與指派，`algo_id` 放開。MD、TD 不再接受新指派；已經指派出去的 session 不因此改判 `invalid_spec`。
5. `mftiko-retire-clean -in <部署 JSON> -instance <inst>`，密碼是該 instance 的 etcd 密碼（`-etcd-password` 或 `$MFTIKO_ETCD_PASSWORD`）。它以該使用者刪 W1 屬於它的 key。spec、demand、account 與 `/retired/{inst}` 留著。條件不滿足就停，不刪。
6. `mftiko-etcd-roles -drop <inst>` 只印命令。以 root 在該 region 執行印出的 `user revoke-role`、`user delete`、`role delete`。
7. 這個名字不能再用。同名 agent 看到 marker 就退出，不參選。

## 驗收

`deploy/accept/accept.sh e2e main` 會走完一次：session RUNNING 時在主機上找到 log，檢查目錄 0750、檔案 0640；結束之後 log 還在；保留期（驗收設成 1 分鐘）過後 spec 被刪，log 也被清掉。見 [deploy/accept/README.md](../deploy/accept/README.md)。
