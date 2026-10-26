# Runbook（一期）

依 [DECISIONS.md](DECISIONS.md) E12-D059–E12-D061（E12-11，#384）、E12-D066（#401）、E12-D071（#403）與 E12-D074（#405）。一期的常見操作與故障處理，照順序做。每一項先寫**告警**與**先看**，再寫**做**與**確認**。長的程序在 [operations.md](operations.md) 與 [release.md](release.md)，這裡連過去，不重抄；那邊是參考，這裡是入口。

告警名稱是 [operations.md「監控與告警」](operations.md#監控與告警)的範例規則（E12-10）。`for` 與門檻由部署端決定。

## 0. 共通

**主機。** 主機 A 是 home region：etcd、NATS、Postgres、SYM、API、console、controller、三種 agent，以及 root 的 etcd 密碼（`/etc/mftiko/etcd/`）與快照（`/var/backups/mftiko/etcd/`）。主機 B 是遠端 region：etcd 與 sts-agent。以下命令都以 root 執行；沒寫主機的就是主機 A。

**看狀態。**

| 要看什麼 | 怎麼看 |
|---|---|
| 告警與 gauge | Prometheus 的 `mftiko_*`（[metric 表](operations.md#監控與告警)）；API 本身是 `up{job="mftiko-api"}` |
| API 是否就緒 | `curl -fsS http://<api>/readyz`：200 是每個 region 都同步且不過舊 |
| instance 與 agent | `mftiko instance list`、`mftiko instance show <inst>`（`AGENT`、`EPOCH`、`RELEASE`、`HOST`，以及 `WORKERS` 的 phase 與 incarnation、`DEMANDS`） |
| session | `mftiko session list`、`mftiko session show <id>`（phase、reason、log 的主機與路徑） |
| 服務 | `systemctl status <unit>`、`systemctl show -p NRestarts,ActiveState <unit>`、`journalctl -u <unit> --since -15min` |
| worker 的輸出 | `${WORK_DIR}/log/{plane}/{h}.stdout`、`.stderr`、`.shim.log`（[log 在哪裡](operations.md#log-在哪裡)） |
| etcd | 見下方 `etcdctl` 的設定 |

**etcdctl。** auth 開著，健康檢查與讀取都要帶 root（E12-D051）：

```sh
export ETCDCTL_ENDPOINTS=<region 的 client URL> ETCDCTL_USER=root ETCDCTL_PASSWORD="$(cat /etc/mftiko/etcd/root)"
etcdctl endpoint health
```

要以別的 user 測權限（`etcdctl --user …`）時，先 `unset ETCDCTL_USER ETCDCTL_PASSWORD`，否則 etcdctl 因為環境變數與旗標衝突而退出（E12-D057）。

**不要做的事。**

- **不要 `systemctl kill` agent。** 它殺整個 cgroup，worker 一起死。只停 agent 用 `systemctl stop`、`restart`（[systemd](operations.md#systemd)）。
- **不要手動改 `/mftiko/v1/` 底下的 key。** 每個前綴只有一個寫入者（PROTOCOL §3.2 W1）。例外只有本文件與 operations.md 寫出來的步驟（退役的 `-drop`、還原的 `-raise-epochs`）。
- **不要讓兩個 controller 同時跑**（E12-D044、E13-D038）。
- **不要先改 etcd 密碼再重啟服務**：先停、再改、再啟動（E12-D050）。
- **不要手動強制 SYM refresh。** 活躍表是空的 feed 在 SYM 啟動時自己補一次（E12-D073）。`mftiko-md-accept refresh` 只給驗收，不是部署步驟。

## 1. 告警對照

| 告警 | 先看 | 處理 |
|---|---|---|
| `MftikoAPIDown` | `systemctl status mftiko-api` | [§3.9](#39-api-不在) |
| `MftikoRegionUnavailable` | 哪個 `region`；API 主機到該 region etcd 的網路 | [§3.5](#35-網路斷了) |
| `MftikoAgentDown` | `instance`、`plane`；該主機的 unit | [§3.2](#32-agent-不在) |
| `MftikoControllerNoLeader` | 主機 A 的 `mftiko-controller` | [§3.3](#33-controller-沒有-leader) |
| `MftikoWorkerBackoff` | `instance`、`plane`；worker 的 stderr | [§3.1](#31-worker-被殺退避或放棄) |
| `MftikoWorkerExited` | 同上 | [§3.1](#31-worker-被殺退避或放棄) |
| `MftikoWorkerLost` | 同上，加上該主機有沒有重開 | [§3.1](#31-worker-被殺退避或放棄)、[§3.4](#34-主機重開) |
| `MftikoPostgresDown` | 主機 A 的 Postgres | [§3.6](#36-postgres-停機) |
| `MftikoRegistryDown` | `ghcr.io` 是否可達、registry 帳密 | [§3.7](#37-registry-不可用) |
| `MftikoArtifactPullFailing` | `instance`、`region`、`digest`。`mftiko_registry_up` 是否仍是 1 | [§3.7](#37-registry-不可用) |
| `MftikoDiskLow` | `df -h`；`${WORK_DIR}/log`、`/var/lib/etcd`、`/var/backups` | [§3.8](#38-磁碟快滿) |
| `mftiko-etcd-backup.service` 是 failed | `journalctl -u mftiko-etcd-backup` | [§2.4](#24-etcd-備份) |
| 不響，但交易所或 NATS 斷了 | worker 的 stderr | [§3.5](#35-網路斷了) |

## 2. 例行操作

### 2.1 換版

告警：空窗長過 `for` 時 `MftikoAgentDown`、`MftikoControllerNoLeader`。worker 不中斷（`KillMode=process`，新 agent 收養）。

1. **兩台都先 fetch、install**，這一步不動任何行程：

   ```sh
   mftiko-release fetch <tag> && mftiko-release install
   ```

   第一次部署用 tarball 裡的 `bin/mftiko-release`；之後用 `/opt/mftiko/current/bin/mftiko-release`。

   **`dd73a2d` 與更早的 `mftiko-release` 把裝好的 release 留成 0700**，服務的使用者執行不了，`switch` 之後每個 unit 都 `Permission denied`（E12-D076）。從這種版本換版時，install 用新版 tarball 裡的 `bin/mftiko-release`；已經裝好的就 `chmod 0555 /opt/mftiko/releases/<tag>`。確認：`ls -ld /opt/mftiko/releases/<tag>` 是 `dr-xr-xr-x`。

2. **先主機 A，再主機 B**：`mftiko-release switch <tag>`。它先 `mftiko-migrate up`，失敗就什麼都不動；再裝 unit、換 `current`，依序重啟 SYM、md.fetch、API、controller、md、td、sts agent（[release.md](release.md#mftiko-release)）。主機 B 的 `DATABASE_URL` 指到主機 A 的 Postgres。
3. **確認**，每台 switch 之後：
   - `mftiko-release list` 的 `current` 是新版。
   - 每個 unit `systemctl show -p NRestarts` 不再增加。agent 的 log 有 `shims adopted`，沒有 `stale_epoch`。
   - `mftiko_agent_up`、`mftiko_controller_leader` 回到 1；`mftiko_worker_phase{phase="RUNNING"}` 的數量跟換版前一樣。
   - `mftiko instance show <inst>` 的 `RELEASE` 是新版。
   - SYM 啟動後一分鐘內 `symbol_active` 有列（E12-D074）。主機 A 用 `/etc/mftiko/sym/env` 的 `DATABASE_URL`，不要印出這條 URL：

     ```sh
     psql "$DATABASE_URL" -c 'SELECT venue, category, count(*) FROM symbol_active GROUP BY 1, 2'
     ```

     有列即可，不要再手動 refresh。已有列的 feed 重啟後仍照抖動間隔，這裡看到的是原本的列。

### 2.2 回退

`mftiko-release rollback`，先回退出問題的那台。它把 `current` 指回上一版、裝回上一版的 unit、照同一順序重啟。**不降 schema**：migration 只做向後相容的新增，舊版照樣能跑（E12-D024）。上一版已被 gc 掉時會拒絕：改用 `fetch`、`install`、`switch` 舊的 tag。確認同 §2.1 第 3 步。

### 2.3 重啟一個 agent 或 controller

例如改了 env 或祕密之後（E12-D004）。

```sh
systemctl restart mftiko-<md|td|sts>-agent      # 或 mftiko-controller
```

- agent：舊 agent 停下時 revoke lease，`AgentRecord` 立刻消失；新 agent 當選後收養 shim（`shims adopted`），worker 的 phase 維持 `RUNNING`。
- agent 被 SIGKILL（或 OOM）時沒有 revoke：systemd 立刻拉起來，但新 agent 要等舊的選主 lease 過期，最多 15 秒才收養。testnet 實測 9–15 秒（E12-D077）。
- controller：新的當選、重新 list 之後只改有差異的需求（N6），不先刪光。
- 確認：`NRestarts` 不增加（手動的 `restart` 不算進去；增加表示新行程起來之後又退出了），`mftiko_agent_up` 或 `mftiko_controller_leader` 回到 1。

### 2.4 etcd 備份

主機 A 的 `mftiko-etcd-backup.timer` 每小時存每個 region 的快照（[etcd 備份與還原](operations.md#備份)）。

- 例行確認：`ls -l /var/backups/mftiko/etcd/*/ | tail`，每個 region 最新一份在兩小時內。
- `mftiko-etcd-backup.service` failed：`journalctl -u mftiko-etcd-backup` 看是哪個 region。一個 region 失敗不擋其他 region。常見的是那個 region 連不上（[§3.5](#35-網路斷了)）或 root 密碼改過而 `/etc/mftiko/etcd/root` 沒更新。修好之後 `systemctl start mftiko-etcd-backup.service` 補一份。
- 把 `/var/backups/mftiko/etcd/` 複製到主機 B 是部署端的排程（E12-D054）。

### 2.5 etcd 認證的輪換

- **服務的密碼：** [輪換服務的密碼](operations.md#輪換服務的密碼)。先停、再改、再啟動；agent 停著的時候 worker 照跑。
- **root 的密碼：** `etcdctl user passwd root`，更新 `/etc/mftiko/etcd/root`。不用重啟服務。之後手動跑一次 `systemctl start mftiko-etcd-backup.service` 確認備份還能用。
- **jwt 金鑰：** [輪換 jwt 金鑰](operations.md#輪換-jwt-金鑰)。每台 etcd 的重啟要在 10 秒內完成。

### 2.6 其他祕密的輪換

規則是 E12-D004：**在原地改寫檔案，路徑不變，再重啟 unit。** worker 拿到的是路徑，重連時讀到新內容。

| 祕密 | 檔案 | 重啟 |
|---|---|---|
| NATS 的 user creds | `/etc/mftiko/md-agent/nats.creds`、`/etc/mftiko/sym/nats.creds`（0600，屬該服務的使用者） | `systemctl restart mftiko-md-agent`、`mftiko-sym`。worker 照跑，下次重連用新的 |
| registry 的帳密 | `MFTIKO_REGISTRY_CREDS` 指的 `config.json`（0600） | 該 agent，或 API（`registry_creds`） |
| API 的 cookie 金鑰 | `/etc/mftiko/api/env` 的 `MFTIKO_COOKIE_KEY` | `mftiko-api`。console 的登入全部失效，要重新登入 |
| Postgres 的密碼 | API 的 `MFTIKO_POSTGRES_DSN`、SYM 與 `mftiko-release` 的 `DATABASE_URL` | 先在 Postgres 改密碼，再改 env、重啟 `mftiko-api`、`mftiko-sym`。中間的空窗長過 `for` 時 `MftikoPostgresDown` 會響 |

- 先寫到同一個目錄的 `<路徑>.new`（`install -m 0600 -o <使用者> <新檔> <路徑>.new`），再 `mv <路徑>.new <路徑>`。rename 是原子的；先刪再建的話，worker 剛好重連時會讀不到。
- 一期不在 runbook 裡：NATS 的 account signing key（要同時改 NATS server 的 account 設定）與 TD 的 seal key（每個帳號都要用新公鑰重新 seal）。見 E12-D060。

### 2.7 instance 增減

- **新增：** 部署 JSON 加上 instance → [套角色表](operations.md#新-cluster開-auth套角色表)第 3 步 → 把印出的密碼寫進該服務的 env → 啟動該 unit。
- **永久下線：** [永久退役一個 instance](operations.md#永久退役一個-instance)，照七步做。重點：先套新的角色表、`systemctl disable --now`（只 `stop` 會被拉起來），等 agent 不在才 `mftiko instance retire`。名字不能再用。

## 3. 故障處理

### 3.1 worker 被殺、退避或放棄

告警：`MftikoWorkerBackoff`、`MftikoWorkerExited`、`MftikoWorkerLost`。

**先看：** `mftiko session show <id>` 或該 instance 的 `mftiko_worker_phase`；worker 的 `.stderr` 與 `.shim.log` 最後幾行。

| phase | 意思 | 會自己恢復嗎 | 做 |
|---|---|---|---|
| `BACKOFF` | 進程結束了，退避後再拉（需求是 `PENDING`） | 會 | 看 stderr 為什麼一直結束；一直退避就是 worker 或設定的問題 |
| `EXITED`（一直留著） | 政策放棄了（需求是 `DEAD`） | 不會 | 修好原因後，讓需求方重放：STS 是 end 再重建 session；MD、TD 的需求由 controller 依 session 重新推導 |
| `LOST` | shim 不見了，沒有結束紀錄 | MD、TD 會照政策重拉；STS 不會 | 見下 |

- **STS 的 session `FAILED`、`shim_lost`** 不會重啟（E09-D070）：agent 無法確認舊進程已經結束，重啟可能兩個實例同時下單。先在該主機確認舊進程真的不在（`ps -u mftiko-sts -f`），再 `mftiko session end <id>`（若尚未 terminal）並重建。
- **STS 的 `restart_limit`**：`window_s` 內重啟太多次。修好原因再重建 session。
- **STS 的 `no_beat`（`HUNG`）**：worker 沒有心跳被 SIGKILL，照重啟政策處理。看 stderr 為什麼卡住。
- 策略端看到的影響見 PROTOCOL §9.5。
- 確認：phase 回到 `RUNNING`，告警解除；`EXITED` 的 status 刪掉之後數量回到 0。

### 3.2 agent 不在

告警：`MftikoAgentDown`（同 region 的 `mftiko_region_available` 是 1）。

1. 該主機 `systemctl status mftiko-<plane>-agent`、`journalctl -u … --since -15min`。
2. 依 log：
   - **etcd 認證失敗**（`authentication failed`）：env 的 `MFTIKO_ETCD_PASSWORD` 與 etcd 不一致，通常是輪換沒做完。照 [輪換服務的密碼](operations.md#輪換服務的密碼)第 3 步失敗的處理。
   - **`stale_epoch`**：有更新的 term，或 etcd 從快照還原後沒有加 epoch。還原過就照 [還原](operations.md#還原)第 6 步補做；否則找出另一個在跑的同名 agent（同一個 instance 不能在兩台跑）。
   - **log 說這個 instance 已退役**：`/retired/{inst}` 在，agent 照設計不參選、退出（PROTOCOL §3.7）。`systemctl disable --now`。
   - **連不上 etcd**：見 [§3.5](#35-網路斷了)。
   - **unit 是 inactive**：有人 `stop` 了，`systemctl start`。
3. worker 在 agent 不在時照跑；agent 回來後收養。
4. 確認：`mftiko_agent_up` 是 1，log 有 `shims adopted`。

### 3.3 controller 沒有 leader

告警：`MftikoControllerNoLeader`。

1. 主機 A `systemctl status mftiko-controller`、`journalctl -u mftiko-controller --since -15min`。
2. 依 log：
   - **`superseded by a newer epoch: writer key in <region> holds epoch …`，一直重啟**：home 的 etcd 從快照還原後沒有加 epoch（E12-D056）。停 controller，補做 home region 的 `-raise-epochs`（[還原](operations.md#還原)第 6 步），`-by` 是那次還原的 bump。不知道那次的 bump 時，`-by` 取「各 region `/mftiko/v1/ctl/controller/writer` 的最大值 − home 的 `/mftiko/v1/ctl/controller/epoch` + 1000000000」。再啟動。
   - **兩個 controller**：另一台也在跑 `mftiko-controller` 就停掉那台（`systemctl disable --now`）。
   - **剛還原過 home**：最多等 15 秒，舊的選主 lease 過期才當選（E12-D057）。
   - **etcd 認證或連線**：同 §3.2。
3. controller 不在時需求原樣保留（N6），MD、TD、STS 照舊；只是新的 session 不會被指派。
4. 確認：`mftiko_controller_leader` 是 1，`NRestarts` 不再增加。

### 3.4 主機重開

告警：重開期間 `MftikoAgentDown`（或 `MftikoRegionUnavailable`，etcd 在那台時）；之後可能 `MftikoWorkerLost`。

- 重開會結束 shim 與 worker：沒有東西可以收養。
- 所有 unit 都 `WantedBy=multi-user.target`、`Restart=always`，開機後自己起來。確認 `systemctl is-enabled` 是 enabled：`systemctl list-units 'mftiko-*' etcd nats`。
- **MD、TD：** 依需求重新 spawn，incarnation 加一（E06-D004）。ack 在 worker `RUNNING` 之前是 `PENDING`。
- **STS：** 還沒 terminal 的 session 依需求重新 spawn，已經 terminal 的不動（E12-D061：#385 演練確認）。`mftiko instance show <inst>` 的 `WORKERS` 看 incarnation 是否加一。
- etcd 在這台時，先等 `etcdctl endpoint health`，再看 agent。
- 確認：`mftiko_agent_up` 回到 1，`mftiko_worker_phase{phase="RUNNING"}` 回到重開前的數量，`MftikoWorkerLost` 解除。

### 3.5 網路斷了

| 斷的是 | 看得到的 | 自己恢復嗎 | 做 |
|---|---|---|---|
| agent ↔ 它的 etcd | lease 到期後 `MftikoAgentDown`（API 仍看得到該 region） | 會：連回來後重新參選、收養 | 修網路。worker 照跑 |
| API ↔ 某個 region 的 etcd | `MftikoRegionUnavailable`，`/readyz` 503 | 會 | 修網路。agent 的 0 不另外叫 |
| controller ↔ 遠端 region | 只斷 controller 時不響；API 也斷時 `MftikoRegionUnavailable` | 會 | controller 維持現狀，不刪那裡的需求（W9、E13-D006）。修網路 |
| md conn worker ↔ NATS | `mftiko_worker_bus_up` 變 0，`MftikoWorkerBusDown`（範例 `for: 15s`）。`mftiko_agent_up` 維持 1，phase 維持 `RUNNING`，feed ack 維持 `READY`。TD、STS 沒有這個系列 | 會。重連 handler 一觸發 gauge 回到 1，不等緩衝排空。訂單簿要幾秒才追上 | 修網路。看 conn worker 的 stderr。斷線期間策略收不到行情 |
| worker ↔ 交易所 | 不響。行程內重連（MD 的 `conn_epoch`、TD 的 `DEGRADED`），不是 `DEAD` | 會 | 看 worker 的 stderr 與交易所狀態 |

- 斷線不需要重啟任何服務。重啟反而讓 agent 失去 term，worker 還是照跑。
- 確認：`mftiko_region_available`、`mftiko_agent_up` 回到 1；md conn worker 的 `mftiko_worker_bus_up` 回到 1；`etcdctl endpoint health` 正常。TD、STS 沒有 bus 這個訊號。

### 3.6 Postgres 停機

告警：`MftikoPostgresDown`。`/readyz` 不看 Postgres，不變。

- API 降級（E10-D080）：保留期掃描跳過；`session create`、`session delete`、`record list` 回 503 `storage_unavailable`；`session list` 照常，但沒有建立時間與建立者；`session end` 照常（只寫 etcd）。已經在跑的 session 不受影響。恢復後自己補上，停機中結束的 session 的 record 是 closed（E12-D077）。
- SYM 也用這個 Postgres（`DATABASE_URL`），看 `journalctl -u mftiko-sym`。
- `mftiko-release switch` 會在 migrate 失敗，不要在這時候換版。
- 做：`systemctl status postgresql`、`journalctl -u postgresql`；磁碟滿見 [§3.8](#38-磁碟快滿)。恢復後不用重啟 API，下一輪掃描補上。
- 確認：`mftiko_postgres_up` 是 1。

### 3.7 registry 不可用

告警：`MftikoRegistryDown`。

- 已經在跑的 worker 不受影響。已經有就緒版本的 agent 照用那一版 spawn（E12-D009）：worker 被殺照樣從快取重拉。
- **新的 session 建不了。** API 建立時要從 registry 解析策略，以 tag 或 digest 都回 `registry is unavailable`。這是設計：建立時以 registry 驗證策略，API 不設快取（E10-D012、E10-D057）。
- 一個就緒版本都沒有時（新主機、新的 conn 或 account 種類、新策略），需求維持 `PENDING`，reason 是 `artifact_pulling` 或 `artifact_unavailable`，不會變 `DEAD`。registry 恢復就自己接上。
- 401、403：`MFTIKO_REGISTRY_CREDS` 指的 `config.json` 過期或沒有權限。更新檔案（0600，屬該服務的使用者）再重啟該 agent。
- **只有 agent 連不到 registry。** `mftiko_registry_up` 是 API 自己探的結果，維持 1，所以 `MftikoRegistryDown` 不響。新 session 維持 `pending`。`SessionStatus` 有 `artifact_pull`（失敗次數與第一次失敗時間），`mftiko_artifact_pull_failing` 是 1。告警是 `MftikoArtifactPullFailing`（範例 `for: 5m`）。agent 的 log 是 `artifact pull will retry: artifact_unavailable`。
- **先看：** 那台 STS agent 主機到 registry 的網路，以及該服務的 registry 帳密檔（`MFTIKO_REGISTRY_CREDS` 指到的 `config.json`，mode 0600）。agent 會照原退避繼續拉，不用為了這個訊號重啟。
- 恢復：condition 清掉，`mftiko_artifact_pull_failing` 回到 0。被拒絕的 artifact 仍是 `FAILED`，沒有這個 gauge。
- 確認：`mftiko_registry_up` 是 1，`PENDING` 的需求變 `READY`，`pending` 的 session 變 `running`。`MftikoArtifactPullFailing` 解除時，`mftiko_artifact_pull_failing` 是 0。

### 3.8 磁碟快滿

告警：`MftikoDiskLow`。

- 先看是誰：`du -sh /var/lib/mftiko/*/log /var/lib/etcd /var/backups/mftiko /var/lib/postgresql 2>/dev/null`。
- **worker 的 log：** 輪替有上限（每個 stream 最多約 320 MiB，[輪替](operations.md#輪替)）。MD、TD 每小時清掉 7 天前不再出現的 worker 的 log。STS 的 log 在 spec 被刪時清（[保留期](operations.md#保留期)）。要立刻釋放就對 terminal 的 session `mftiko session delete <id>`。不要刪還在跑的 worker 正在寫的檔。
- **etcd 快照：** 每個 region 72 份。可以暫時 `-keep` 少一點手動跑一次，不要刪最新的。
- **還原留下的 `/var/lib/etcd.before-restore.*`**：確認還原沒問題後可以刪。
- 磁碟真的滿了，shim 會丟輸出並留一行 `bytes of stdout lost here`，worker 不阻塞。

### 3.9 API 不在

告警：`MftikoAPIDown`。

- session 與 worker 不受影響；console 與 CLI 不能用，新的 session 建不了。
- `systemctl status mftiko-api`、`journalctl -u mftiko-api`。常見：`MFTIKO_ETCD_PASSWORD` 錯（輪換沒做完）、Postgres DSN 錯、設定檔解不開。
- 確認：`up{job="mftiko-api"}` 是 1，`/readyz` 200。

### 3.10 etcd 壞了或資料錯了

- 單一 member 停了：`systemctl status etcd`，`Restart=always` 會拉起來。起不來看 `journalctl -u etcd`（磁碟滿、data dir 權限）。
- 資料毀損或被誤改：從快照還原，照 [還原](operations.md#還原)的 8 步做，**bump 一律計算**，epoch 加同一個 bump。快照之後的變更會沒有（E12-D057）。
