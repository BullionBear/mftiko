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
- `{h}.shim.log` 不輪替。MD、TD 的 worker key 不再出現時，它留下的檔案也不會被清掉。這兩項記為 E12 的磁碟風險（E10-D015）。

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
