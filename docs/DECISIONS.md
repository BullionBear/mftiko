# DECISIONS

> 決策紀錄（2026-10-27 E14-D011–E14-D015（E14-02，#437）：契約 L 寫上 agent 的 GET /v1/sessions/{session_id}/logs/{stream}（tail 以行計、from=start、cursor、follow）與 …/export，以及 Authorization: Bearer。API 並行讀 stdout、stderr，SSE 事件帶 stream，不保證跨 stream 順序。agent 的 401 與 503（Retry-After）轉成 logs_unavailable，404 轉成 not_found。匯出是 agent 產出的 tar.gz，API 原樣轉送、不暫存。log_addr 取自 spec 的 sts_instance 當選 leader。log.proto 草案，列入 breaking.ignore。api.v1 info.version 2.1.0。PROTOCOL 為 v1.83。2026-10-27 E15-D001–E15-D011（E15-01，#446）：資料 artifact 由 `mftiko-pack data` 上傳，stdout 印 manifest digest，`-tag` 選填。運維上傳的 config 沒有 `source`。解開時會被拒的項目（絕對路徑或含 `..` 的 symlink、socket、device、fifo、`.wh.*`）讓打包失敗；`-L` 跟著 symlink 走。推送前檢查解開後 4 GiB、100,000 個 entry 與壓縮後 1 GiB（`-max-bytes`），已有的 blob 不重傳。API 加 `max_input_bytes`（預設 1 GiB），inputs 的 repository 改受 `allow` 管（修改 E10-D122），STS 的 data repository 必須在 `allow` 內。`mftiko-pack` 隨 CLI 的 tarball 發佈（修改 E11-D035）。Python 的端到端在 lynxlinkage/mftik。PROTOCOL 為 v1.82。2026-10-27 E14-D001–E14-D010（E14-01，#436）：STS agent 讀 shim 的檔、開 HTTP 端點，API 依 `AgentRecord.log_addr` 連過去，不經 NATS（契約 L）。一份共用 bearer，API 存 token、agent 存 SHA-256。`log_addr` 在當選時寫，預設 `<hostname>:7442`。agent 回 varint 長度前綴的 `mftiko.log.v1.Frame`。匯出是 tar.gz，`manifest.json` 在最後。CLI 是 `session logs` 與 `session export-logs`，`--tail` 預設全部，加 `--latest`。同時串流 16 與讀取 16 MiB/s 是 flag；frame 256 KiB、follow 心跳 15 秒寫死。錯誤是 `logs_unavailable`（503，CLI exit 22）。範圍只有 STS session worker 的 stdout、stderr。一期不做 `-t`、`--since`、`--until`。PROTOCOL 為 v1.81。2026-10-27 E09-D097、E09-D098（#433）：session end 只改 spec 的 `desired_state`。STS 的 `decide` 在 spec 已是 `ENDED`、demand 仍是 `RUNNING` 時當作 ended，不換 incarnation。終態在 worker 已 release、觀測為空時仍寫最後一個 incarnation 與 `last_exit`。hash 仍是整份 spec bytes（修訂 E09-D047）。PROTOCOL 不變（v1.80）。同票把 E10-D031、E10-D035、E13-D003、E13-D022、E12-D021、E12-D071、E12-D077 裡過時的句子改成與現行程式一致。2026-10-27 E10-D122–E10-D128（#430）：策略 image 的 host 取自完整 `repository`，API 拿掉 `registry`，STS 拿掉 `--registry`。帳密仍是一份 Docker `config.json`，依 host 對上 `auths`。`plain_http` 布林改成 `plain_http_hosts`。STS 的資料 repository 是完整路徑。`mftiko-agent-latest` 與 MD、TD 不變。401、403 仍是 `registry_unavailable`。`mftiko_registry_up` 維持 0，範例告警 `MftikoRegistryDown` 拿掉。PROTOCOL 為 v1.80。2026-10-27 E10-D118–E10-D121（#429）：`StrategySelector` 改為 `repository` 與 `ref`，拿掉 `id`，沒有相容期。`api.v1` 在原地升到 `2.0.0`（路徑仍是 `/api/v1`），`oasdiff` 以 `contract/openapi/api-v1-err-ignore.txt` 放行這一次。`allow` 取代 `repositories`，單獨的 `*` 只開這一個 registry。建立 session 讀 manifest 並檢查 kind。列出改為 `GET /strategies?repository=`。每台 registry 的帳密不在這張（#430）。GC 仍依 digest（E09-D056）。PROTOCOL 為 v1.79。2026-10-26 E12-D079（#413）：SYM 的 NATS user 由 `mftiko-nats-dev` 簽發（`-sym-inst`，`creds/sym-{inst}.creds`），publish `sym.v1.changed.>`、subscribe `sym`、回答 `sym` 帶 response permission，沒有 inbox。`-check` 驗證它的允許與拒絕。沒有 SYM user 的舊 manifest 由 Load 拒絕，重跑產生即補上。accept 的 SYM 改用這個 user。PROTOCOL 為 v1.78。2026-10-26 E12-D075–E12-D078（E12-12，#385）：testnet 演練在 cp（jp）與 sys（jp2）上，兩台與其他服務共用，mftiko 的元件都用自己的 port 與容器，斷網只擋 mftiko 使用者的 UID。release 本機建、經 mftiko-release 安裝，不發佈 tag；artifact 推到主機 A 自己的 registry。BinanceUM testnet，ed25519 金鑰，策略是 `examples/strategies/drill`。清單每一項都通過，紀錄在 `deploy/drill/README.md`。演練中修掉三個問題：mftiko-release 把 release 留成 0700、Binance 種類宣稱接受 hmac、`restart_count` 從來沒寫。runbook 補上換版、SIGKILL、NATS、Postgres、registry 的細節。其餘問題開成 #401–#405。PROTOCOL 為 v1.77。2026-10-26 E12-D073–E12-D074（#405）：SYM 啟動時，活躍表沒有列的 feed 立刻 refresh 一次，已有列的 feed 仍先等抖動。不另做運維 refresh 指令。runbook 的部署確認改為 SYM 啟動後一分鐘內有活躍表。範圍只有 SYM。MD、TD、STS、controller 的行為不變。PROTOCOL 不變（v1.76）。2026-10-26 E12-D067–E12-D071（#403）：STS agent 暫時拉不到 artifact 時不改重試，session 維持 PENDING、照原退避、不放棄。SessionStatus 寫 `artifact_pull` condition（失敗次數與第一次失敗時間），成功即清除；永久錯誤仍 FAILED、不寫這個 condition。API 加 `mftiko_artifact_pull_failing` 與 `mftiko_artifact_pull_fail_since_seconds`，門檻由部署端決定，範例 `for: 5m`。範圍只有 STS。PROTOCOL 為 v1.76。2026-10-26 E12-D062–E12-D066（#401）：md conn worker 以 NATS 的斷線與重連 handler 經 shim socketpair 上報 optional `bus_up`（`mdctl.Report` 欄位 8），不經 NATS。md-agent 寫成 `WorkerStatus.conditions`（欄位 10）type `bus_disconnected`（status true 斷線、false 連著）。feed 的 `READY` 與 phase 不變。API gauge `mftiko_worker_bus_up{instance,plane,region,worker}` 為 0 或 1，mftiko 不計時，範例 `for: 15s`。重連立即回到 1，不改 `ReconnectBufSize`，訂單簿要幾秒才追上。TD、STS、controller 的行為不變。PROTOCOL 為 v1.75。2026-10-26 E12-D072（#404）：STS agent 背景解析 `latest`（E12-D011）的結果，每個 repository 只在第一次失敗與狀態改變時記 log；狀態不變的重複結果不再記，等級維持現狀。沒有 `latest` 的 repository 不會每分鐘 warn。MD、TD、controller 的行為不變。PROTOCOL 不變（v1.74）。2026-10-26 E12-D059–E12-D061（E12-11，#384）：一期的 runbook 是 `docs/runbook.md`，依告警與依操作各一個目錄，每一項寫告警、先看、做、確認。已經在 operations.md、release.md 的程序只連過去，不重抄。範圍包含換版、回退、重啟、etcd 的備份還原與輪換、其他放在檔案裡的祕密、instance 增減與退役，以及 E12-10 演練清單的每一種故障。NATS signing key 與 TD seal key 的輪換一期不做。演練之前沒有實測的條目列在 E12-D061，#385 照 runbook 演練。程式與 PROTOCOL 不變。2026-10-26 E12-D054–E12-D058（E12-07，#380）：主機 A 的 `mftiko-etcd-backup.timer` 每小時以 root 存每個 region 的 etcd 快照到 `/var/backups/mftiko/etcd/<region>/`，每個 region 留 72 份，異地複製交給部署端。還原一律 `etcdutl snapshot restore --bump-revision <bump> --mark-compacted`，bump = R_old − R_s + 10 億（R_old 讀不到時用最新快照的 revision），cluster ID 不變，revision 不倒退，`Mirror` 重新 list。還原之後、agent 與 controller 起來之前，`mftiko-etcd-backup -raise-epochs -by <bump>` 把該 region 的 epoch 加上同一個 bump，shim 不回 `stale_epoch`、controller 不被其他 region 的 writer key 取代。auth 回到快照時，重跑 `-apply`、快照之後輪換過的密碼再設一次。home 還原後要等舊的選主 lease（15 秒）。testnet 在 cp（jp）與 sys（jp2）走過備份、還原與不加 epoch、固定 bump 的失敗情形；agent 與 API 沒部署，worker 的影響留到 #385。MD、TD、controller 的行為不變。PROTOCOL 為 v1.74。2026-10-25 E12-D049–E12-D053（E12-06，#379）：`mftiko-etcd-roles -apply` 以 root 呼叫 `EnsureAuth`，只補 cluster 缺少的 role、user 與 permission，不刪多出來的。新 user 的密碼在 `-secrets` 同名檔案，沒有就先產生並寫成 0600。已有的 user 不改密碼。主本在主機 A 的 `/etc/mftiko/etcd/`。輪換先停服務，再 `etcdctl user passwd`，再啟動。jwt 是 ES256、`ttl=5m`，每個 region 一對金鑰；換金鑰後舊 token 被拒，client 重新認證，服務不用重啟。etcd 與 NATS 一期不開 TLS。MD、TD、controller 的行為不變。controller 的密碼仍依 E12-D045。PROTOCOL 為 v1.73。2026-10-25 E12-D044–E12-D048（E12-08，#381）：一期只在 home region 的主機 A 跑一個 controller，不做熱備援，靠 systemd `Restart=always` 拉回。每個 region 的 etcd 使用者都是 `controller`，密碼先讀 `MFTIKO_ETCD_PASSWORD_<region>`，沒有再用 `MFTIKO_ETCD_PASSWORD`。region 可寫 `dial_timeout`、`request_timeout`、`stale_after`，沒寫或 0 用現在的預設。W9 斷線行為不變，告警用 `mftiko_region_available`。搬 home region 時兩個 home 不同的 controller 不要重疊（E13-D038）。CI 測密碼順序與這三個欄位。MD、TD 的行為不變。testnet 的重啟、換版、斷線恢復留給 #385。PROTOCOL 為 v1.72。2026-10-25 E12-D038–E12-D043（E12-10，#383）：監控在 API server 的 Prometheus `/metrics`。mftiko 只輸出自己的 metric：每個 agent 與 controller 的 `AgentRecord` 在不在、controller 的 leader、`WorkerStatus` 各 phase 的數量、region 是否可用（沿用 15 秒，E10-D014），以及 API 觀察到的 registry 與 Postgres。磁碟、CPU、記憶體交給 node_exporter。agent 不另開端點。不打 webhook、不判斷門檻；範例規則在 `docs/operations.md`，包含 API 本身抓不到（`up == 0`）。演練清單的每一種故障對到一個 metric 或範例告警。MD、TD 在啟動時與每小時清掉不再出現的 worker key 留下的 log，mtime 嚴格早於可設定的天數（預設 7）。`{h}.shim.log` 依大小輪替（10 MiB，保留 3 份，含正在寫的），agent 仍把 stdout／stderr 接到檔案、不接 pipe。STS 的清理與 MD、TD、controller 的控制行為不變。testnet 實測留到 #385。PROTOCOL 為 v1.71。2026-10-25 E12-D033–E12-D037（E12-09，#382）：instance 永久退役。`mftiko instance retire` 經 API 寫 Postgres `retired_instances` 與 etcd `/retired/{inst}`，`AgentRecord` 在則 409。controller 把該 STS 上非 terminal session 的 `SessionDemand.end_reason` 寫成 `instance_retired`，再依原順序刪需求與指派，釋放 `algo_id`。MD、TD 退役後不再接受新指派。`mftiko-retire-clean` 清 W1 的 key；`mftiko-etcd-roles -drop` 只印刪使用者與角色的命令。同名不能再註冊。P11 移到 PROTOCOL §3.7。PROTOCOL 為 v1.70。2026-10-25 E12-D027–E12-D032（E12-13，#391）：自己寫 migrator，不引入 golang-migrate 或 goose；讀嵌入的 `migrations/*.up.sql`，依編號排序，跳過 `schema_migrations` 已紀錄且 sha256 相符的；表上記 version、name、檔案 sha256 與套用時間。子指令只有 `up` 與 `status`，沒有 `down`。每個檔案與它的紀錄同一個交易，失敗回滾該檔並以非 0 結束；先拿 `pg_advisory_lock`，兩台一次只跑一個。已套用檔案的 sha256 不同、name 不同、或紀錄沒有對應檔案，就停、不套新檔。不讀 `accept_migrations`、不猜既有的表；`accept.sh` 改呼叫 `mftiko-migrate up`，驗收從空資料庫開始。發佈腳本測試改用真正的 `mftiko-migrate`。PROTOCOL 為 v1.69。2026-10-25 E12-D016–E12-D021（E12-05，#378）：agent unit 用 `KillMode=process` 與 `OOMPolicy=continue`，shim 留在同一個 cgroup，`stop`／`restart` 只殺 agent，新 agent 收養；不用 `systemd-run --scope`，不開 `PrivateTmp=` 等 namespace，不用 `RuntimeDirectory=`，狀態目錄是 `StateDirectory=mftiko/<服務>`；沒有 `ExecStop=`，`TimeoutStopSec=30s`；全部 unit `Restart=always`、`RestartSec=1s`、`StartLimitIntervalSec=0`；etcd 與 NATS 自己寫 unit、跑官方 binary，Postgres 用發行版的 unit；CI 跑 `systemd-analyze verify`，另一個 Ubuntu job 用 systemd 驗三種 agent 的 restart、stop 再 start、SIGKILL 收養。controller 停機的演練在 #385。PROTOCOL 為 v1.68。2026-10-25 E12-D022–E12-D026（E12-04，#377）：host binary 的安裝與換版是 tarball 裡的 `bin/mftiko-release`（root），子指令 `fetch`、`install`、`switch`、`rollback`、`gc`、`list`；`fetch` 以 `SHA256SUMS` 核對 GitHub Release；`switch` 先 `mftiko-migrate up`（失敗不動 `current`），再安裝該版 unit、原子換 `current`、只重啟本機有啟用的 SYM、md.fetch、API、controller 與三種 agent，然後 gc；`rollback` 把 `current` 指回上一版並重啟，不降 schema；migration 只做向後相容的新增；gc 留最近 3 版與 `current`，`/proc/<pid>/exe` 指著的不刪。打包是 `build-server.sh` 與 `make server-dist`，與 CLI 同一份 `SHA256SUMS`。PROTOCOL 為 v1.67。2026-10-25 E12-D011–E12-D015（E12-03，#376）：STS 只查磁碟上有 rootfs 的 repository 的 `latest`，解開時記下 repository，沒有紀錄的照舊只留被釘住的；每個 repository 在背景每 60 秒（`-artifact-poll`）解析 `latest`，GC 只讀快取，reconcile 不呼叫 registry；registry 不可用、`latest` 不存在、或尚未成功解析過，該 repository 的 rootfs 全部保留；MD、TD 的 keep 是正在跑的 worker 用的 digest、正在拉或解開中的 digest，加上 `latest.Source` 最後一個通過驗證的 digest，每輪 reconcile 做一次本機 GC，失敗只寫 warn log。PROTOCOL 為 v1.66。2026-10-25 E12-D008–E12-D010（E12-02，#375）：md-agent、td-agent 在背景每 60 秒（`-artifact-poll`）把 `latest` 解析成 digest，拉取、驗證、解開，spawn 用最後一個通過驗證的版本，reconcile 不呼叫 registry；跑著的 worker 不換（P9）；版本看 `WorkerStatus.release`；暫時錯誤以 1–60 秒退避重試，內容有問題的 digest 不再拉，已就緒的版本照用；一個就緒的版本都沒有時 spawn 失敗重試，需求項目維持 `PENDING`，reason 是 artifact 的 reason（`artifact_pulling` 等），不寫 `REJECTED`；正式環境用 `-registry`、`-data`、`-repository`，`-conn`、`-account` 只給開發與測試，兩者二選一；`deploy/accept` 改成從 `registry:2` 的 `latest` 拉，accept 工具以 `MFTIKO_PLANE` 找 worker。PROTOCOL 為 v1.65。2026-10-24 E12-D001–E12-D007（E12-01，#374）：testnet 演練用兩台真實 Linux VM，一台一個 region（A 是 home，跑 etcd、NATS、Postgres、SYM、md.fetch、API、controller 與三種 agent；B 跑 etcd 與 sts-agent），每個 region 的 etcd 一期是單一 member，NATS 只有一台；host binary 隨 `v*` tag 發佈成 `mftiko-server_<tag>_linux_amd64.tar.gz`（`bin/`、`web/`、`systemd/`），與 CLI 共用一份 `SHA256SUMS`；`/opt/mftiko/releases/<tag>/` 加上 `current` symlink，換版不覆寫舊檔，保留 3 版、有進程在用就不刪，狀態在 `/var/lib/mftiko/<服務>/`；設定與祕密放在 `/etc/mftiko/<服務>/`，etcd 密碼用 `EnvironmentFile=`，不用 `LoadCredential=`（unit 停下時它的目錄會被刪掉，worker 讀不到 creds）；每個服務一個 Unix 使用者，log 開給 `mftiko-ops`；一期只有一個 controller，放在 home。PROTOCOL 為 v1.64。2026-10-24 E01-D013 修訂（E1-24，#277）：維持公式（最後 keepalive 回應 + TTL − TTL/3）。auth token 過期會拆掉 keepalive stream 再重連，所以 2/3 TTL 必須大於一次 token 重連所需的時間；TTL 設太短，term 會在 server 端 lease 仍有效時提前結束，這是設定的下限，不另外處理。`TestTokenExpiry` 調大 session TTL，讓 2/3 TTL 蓋得住一次重連。不採「重連中放寬」。2026-10-24 E01-D012–E01-D016（E1-24，#277）：term 在 server 端選主 lease 過期之前結束。同一個 lease 另外呼叫一次 `cli.KeepAlive`，只取回應時間，不換掉 `concurrency.Session`，選主不變。期限 = 最後一次 keepalive 回應時間 + TTL − TTL/3（2/3 TTL）。TTL 用 server 回應裡的值，時間用單調時鐘。第一次回應前以 Grant 回應時間起算。期限到即以 `ErrSessionLost` 結束 term（訊息註明 keepalive 期限），與 `Session.Done()` 同一條路；term 結束時原有的行為不變，不另外 Revoke。只改 `internal/platform/state` 的 `Term`。`TestScenarioZombieWindow` 改斷言 term 結束早於 server 端選主 key 被刪。PROTOCOL 為 v1.63（與 #371 的 v1.62 平行；後合併者 merge main 並維持版本單調）。2026-10-24 E10-D117（#371）：`session_records` 加 `final_status`（migration 0005），存 `statev1.SessionStatus` 的 proto bytes；W5 與立即 `DELETE` 關閉時都寫，較新的 status 取代較舊的；`GET /session-records` 以 `final` 回傳，舊列與解不開的 bytes 省略 `final`；契約不變。PROTOCOL 為 v1.62。2026-10-24 E11-D038（E11-09，#361）：CLI 的端到端驗收沿用 `mftiko-api-accept`，加 `cli main|faults|region`；CLI 是唯一的 client，經記錄 `X-Request-Id` 的 reverse proxy 打副本 a，每個請求（含分頁、SSE、重試）都比對 `audit`，401 那列沒有使用者；輸出出現 token 或憑證即失敗；異常項目比對 exit code 10–17 與本地的 1。2026-10-24 E11-D035–E11-D037（E11-08，#360）：CLI 在 E11 隨 `v*` tag 發佈到 GitHub Release，`release.yml` 的 `cli` job 建 darwin/arm64、darwin/amd64、linux/amd64、linux/arm64，每個平台一個只含 `mftiko` 的 tarball，外加 `SHA256SUMS`；`.github/scripts/build-cli.sh` 給 release、`make cli-dist` 與 CI 共用；另可 `go install`；不用 goreleaser，不做簽章與 Homebrew。`mftiko version` 與 `-version` 印 CLI 版本（`-ldflags` 注入，未注入時用 module 版本）與 `api.v1` 的 `info.version`（常數，測試比對 `v1.yaml`），`-o json` 可用，不讀設定也不呼叫 API。文件在 `docs/cli.md`，README 與 `docs/release.md` 連過去。PROTOCOL 為 v1.61。2026-10-24 E11-D029–E11-D034（E11-07，#359）：SSE client 自寫，用標準庫逐行解析 `event`、`id`、`data`，不加套件。重連指數退避 1–30 秒，帶最後的 `Last-Event-ID`；45 秒（3 次心跳）沒有內容視為斷線；收到 `reset` 重新 list 再續接。`watch` 無限重試，`--wait` 受逾時限制。`session watch` 不帶 id 時 TTY 重畫表格，`-o json` 或非 TTY 每事件一行 JSON；帶 id 重畫 `session show`，`session_removed` 印一行說明並 exit 0。`create --wait <phase|terminal>` 等到指定 phase，`end --wait` 等到 terminal，`--wait-timeout` 預設 10 分鐘；未到目標 phase 就已 terminal 時依結束原因回 exit code。到達目標、`outcome=done` 或 `end_reason=ended_by_spec` 回 0，其餘 30 `failed`、31 `init_failed`、32 `crashed`、33 `hung`、34 `unknown`、35 `deps_failed`、36 `deps_timeout`、37 `invalid_spec`、38 等待逾時、39 等待中 session 被刪除，文件與輸出號碼、名稱並列。等待期間 region 不可用則 stderr 警告並重試到逾時（exit 38）。PROTOCOL 為 v1.60。2026-10-24 E11-D024–E11-D028（E11-06，#358）：session 與 record 的讀取與操作。`session show` 印 spec 摘要、狀態、需求項目與結束資訊，最後附 log 位置；`session list` 的篩選是 `--phase`、`--active`、`--sts-instance`。log 印 `<sts_instance> <host>:<完整路徑>`，最後已知註明「最後已知，agent 目前不在線」，一期不讀 log 內容。`session end` 需確認，已結束仍成功。`session delete` 用共用確認，非 terminal 時 exit 14 並提示先執行 `session end`。`record list` 的時間收 RFC 3339 或 `YYYY-MM-DD`（本地時區午夜），不收相對時間，分頁照 E11-D010。PROTOCOL v1.59。2026-10-24 E11-D019–E11-D023（E11-05，#357）：session 檔用 `gopkg.in/yaml.v3` 在節點層級轉成 JSON，數字保留原文；JSON 視為 YAML 子集，YAML、JSON 與 `-f -` 同一個解析器，重複 key 報錯；不明確的數字報錯並要求加引號；布林照 YAML 1.2；`Idempotency-Key` 每次執行一個 UUID，`--idempotency-key` 可指定，連線失敗或逾時重試最多 3 次（1/2/4 秒）且沿用同一個 key，409 說明「此 key 已用於不同內容」，錯誤輸出一律附上 key；JSON Pointer 對回檔案行號，建立成功印 `session_id` 與 digest。PROTOCOL 為 v1.58。2026-10-24 E11-D014–E11-D018（E11-04，#356）：`mftiko account` 的憑證只從檔案或 TTY 不回顯讀入，ed25519 PEM 原樣送出，HMAC 只去一個結尾換行；送出前依 `/account-kinds` 擋下不支援的種類，`--td-instance` 必須是 td；update 先讀 `revision` 再 CAS，沒給憑證檔就不送 `credentials`；409 `conflict` 不重試；delete 帶 `revision` 並用 `cli.Confirm`，409 `referenced` 列出 session。PROTOCOL 為 v1.57（與 #355 的 v1.56 並行）。2026-10-24 E11-D010–E11-D013（E11-03，#355）：策略、instance、帳號種類的唯讀查詢；列表預設以 limit=1000 自動翻完，--limit 限制總筆數，--cursor 只取一頁；自動翻頁的 -o json 合併成 {"items":[...]}，unavailable_regions 取聯集且不帶 next_cursor，--cursor 則原樣輸出；unavailable_regions 或 controller_absent 時 stderr 警告一次並列出缺少的 region，exit code 不變，單一資源 503 是 region_unavailable（17），registry_unavailable（19）明寫為 registry 連線問題；strategy list 顯示 latest 的 tag 與 digest 前 12 碼，versions 在 digest 等於 latest 的列標 latest，show 印完整 digest。PROTOCOL 為 v1.56。2026-10-24 E11-D001–E11-D009（E11-02，#354）：`mftiko` 只對 API 說話，`mftiko-pack` 與 `mftiko-seal` 維持獨立；標準庫 `flag` 分派 `mftiko <資源> <動作>` 與 `help`，不做 shell 補全；設定檔 `~/.config/mftiko/config.yaml`，寬於 0600 拒絕，token 來自 `MFTIKO_TOKEN` 或設定檔且環境變數優先，不進旗標、輸出與錯誤；具名 context 以 `--context` 或 `MFTIKO_CONTEXT` 選擇，都沒設就不挑預設；TLS 預設驗證，`ca_file` 是唯一信任根，明文 HTTP 要 `insecure_http: true`；`login` 從 stdin 讀 token，`GET /auth/me` 成功才寫入，`whoami` 只呼叫 `/auth/me`，只用 bearer；exit 0／1／2，API code 10–21 固定編號；預設表格與本地時區，`-o json` 原樣輸出，錯誤列 `errors[].path` 與 `X-Request-Id`，逾時預設 30 秒，`User-Agent: mftiko-cli/<版本>`，無 body 不補 Content-Type；TTY 詢問 y/N，非 TTY 要 `--yes`。PROTOCOL 為 v1.55。2026-10-24 E06-D040（#217）：Binance（UM 與現貨）下單一律帶 `newOrderRespType=ACK`，下單回覆只是收到確認（`OrderStatusNew`），狀態全部由私有串流推動，契約不變；post-only 會吃單時兩邊都在回覆直接拒絕（UM -5022、現貨 -2010），都對到 204。2026-10-24 決策編號改成 `Eee-Dsss`（epic 兩位、序號三位，見下方的編號說明），只換號碼，條文不變。E10 的舊號碼 D1100–D1116 超出兩位序號，改為 E10-D100–E10-D116，E11 不再和它撞號；D461、D462 標的是 E8-01，號碼卻在 E4 的段，改為 E08-D065、E08-D066。PROTOCOL 為 v1.54，`api.v1` 的 `info.version` 依 E10-D116 改為 `1.0.1`（只改說明）。2026-10-24 E10-D116（E10-20，#325）：`api.v1` 定案，`info.version` 為 `1.0.0`，`OASDIFF_FAIL_ON` 預設 `ERR`，CI 擋不相容的改動；相容擴充加 minor、只改說明加 patch、不相容另開 v2；PROTOCOL §10 移出 P8；epic #12 的完成標準分成四層。2026-10-23 E10-D110–E10-D115（E10-19，#324）：`Content-Type: application/json` 只對 cookie 認證的寫入與 login 要求，bearer 不檢查（修改 E10-D009），console 以 middleware 對所有寫入補上；端到端驗收擴充 `deploy/accept`（registry、第二個 region 只放 `sts2`、controller、`sts1`、`sts2`、兩個 API 副本，副本 a 對外 `127.0.0.1:18080`），驅動程式 `mftiko-api-accept` 只經 API 的 Go client，主流程用 hello，每個流程以 `X-Request-Id` 比對 `audit`，保留期 1 分鐘；異常流程在驗收腳本裡跑不進 CI；Playwright smoke 在本機打驗收環境，固定 1.64.1、只用 Chromium；dev compose 加 `mftiko-api` 服務 console，設定由 `api-init.sh` 產生到 gitignore 的目錄，重建 console 要重啟 API；驗收環境的 migration 記在 `accept_migrations`。2026-10-23 E10-D104–E10-D109（E10-18，#323）：結束按鈕先確認，session 已是 `ended` 或 terminal 時不顯示，呼叫 `POST /sessions/{id}/end`（冪等）；log 顯示 `LogLocation` 的主機與完整路徑並附複製按鈕，`AgentRecord` 不在而用最後已知值時加註（E10-D096）；`state_out` 只顯示 digest 加複製，不做下載；每頁一條 EventSource，只訂該頁需要的 topic，收到 `reset` 重新 list，沿用 `web/src/lib/live.svelte.ts`；agents 頁依平面分組，最後已知的 `AgentRecord` 加標記，`controller_absent` 與 `unavailable_regions` 以頁頂橫幅顯示；market 頁唯讀列出 MD instance 與各 feed 狀態，不加搜尋或篩選。PROTOCOL 為 v1.51。2026-10-23 E10-D092–E10-D097（E10-13，#318）：`AgentRecord` 加 `host`、`work_dir`，在共用 agent 骨架當選時寫入，所有平面（MD、TD、STS、controller）都寫，`state.proto` 相容擴充；STS 寫 terminal `SessionStatus` 時，依當下 `SessionDemand.bindings` 的 `REJECTED`、`DEAD` 項目各寫一個 `dependency` Condition，若當下 `SessionDemand` 已不在則不寫，不特別處理；每條 SSE 連線對每個 region 各開一個 etcd watch，從 `Last-Event-ID` 該 region 的 revision+1 開始，compacted 送 `reset`，不經 mirror；每條連線的送出緩衝有上限，滿了就斷線，由瀏覽器帶 `Last-Event-ID` 重連；每個副本在記憶體保留最後看到的 `AgentRecord`，回應標明是最後已知值，副本重啟後沒有就只標示不在；`/session-records` 分頁與篩選照 OpenAPI 現有參數，依 `created_at` 由新到舊。PROTOCOL 為 v1.50。2026-10-23 E10-D098–E10-D103（E10-16，#321）：`/account-kinds` 中 `supported` 為 false 的種類在選單顯示為灰色不可選，附原因；TD instance 自由輸入，由 API 依 E10-D065 驗證，不依賴 `/instances`（#318），兩票不互卡；更新帳號時憑證區預設收起，勾「更換憑證」才展開，不勾則沿用原密文（E10-D062）；ed25519 私鑰是 PEM PKCS#8，`pem.Decode` 需要保留換行，key type 為 ed25519 時 secret 用保留換行的多行文字框，並可選 `.pem` 檔讀入，關閉自動填入與拼字檢查，送出後立即清除，不存任何地方，成功或失敗都不回顯，送出的 bytes 為原樣 PEM 文字（含換行），HMAC 的 key、secret、passphrase 維持 password；刪除先確認，409 `referenced` 列出 `referenced_by` 的 session，409 `conflict` 重新載入後再確認；列表顯示種類、TD instance、`referenced_by`（session 可點過去）。PROTOCOL 為 v1.49（與 #318 的 v1.48 平行；後合併者 merge main 並維持版本單調）。2026-10-23 E10-D086–E10-D091（E10-17，#322）：params 用 JSON 文字框，console 只檢查是合法 JSON、原樣送出，manifest 有預設 params 時預填，`StrategyVersion` 沒有預設 params 所以文字框是空的；feeds 可增刪、自由輸入，console 不比對清單，由 API 驗證；帳號從 /accounts 下拉，algo_id 由使用者在本 session 自行宣告（1–1023），換帳號時清空，帳號不存 algo_ids（E06-D002），不新增每帳號的 algo 清單；每次打開建立精靈產生一個 Idempotency-Key UUID，送出失敗重試沿用同一個（E10-D068）；API 錯誤的 JSON Pointer 對到欄位，對不到的放表單頂端；session 列表列 /sessions 現存 session（狀態、策略、帳號、建立者、時間）並以 SSE 更新，/session-records 歷史不在本票，建立成功後導向 session 詳細頁。PROTOCOL 為 v1.47（與 #317 的 v1.46 平行；後合併者 merge main 並維持版本單調）。2026-10-23 E10-D080–E10-D085（E10-12，#317）：terminal session 過保留期由既有 reconcile 刪 spec 與 status，先關閉 `session_records` 再 `deleteIf`；Postgres 不可用則該輪不刪，`DELETE /sessions/{id}` 回 503 `storage_unavailable`；etcd 有 session 而沒有列則不刪、只告警；只有 status 時以該 key 的 `mod_revision` 刪；立即刪除非 terminal 回 409 `not_terminal`，成功與重複刪除回 204，稽核沿用中介層。PROTOCOL 為 v1.46。2026-10-23 E10-D068–E10-D073（E10-11，#316）：建立 session 可選帶 Idempotency-Key，同 key 同 body 回同一個 session，同 key 不同 body 回 409，保留 24 小時；pending 逾 2 分鐘且 etcd 沒有對應 spec 標 abandoned，之後 spec 的 create_revision 與雜湊對上則改回同一列 created；每個副本每 30 秒以及 mirror 變動時 reconcile，有條件 upsert、不選主；雜湊不同只記 error log 與計數器，只同步 desired_state 與 terminal 結局；etcd 有 spec 而 Postgres 沒有列時只告警不補列，E10-D072 修改 E10-D031 的不變式 1。PROTOCOL 為 v1.45。2026-10-23 E10-D062–E10-D067（E10-09，#314）：建立帳號必帶憑證，更新省略則保留原密文；從 OpenAPI 的 `Account` 刪除 `credentials_fingerprint` 並重新產碼，讀取不回傳明文、密文或指紋（修改 E10-D011）；憑證只做格式檢查，錯誤只說欄位，不向交易所驗證 key；`td_instance` 必須在 topology、是 TD、且 `td_keys` 有公鑰，缺公鑰要寫明；`name` 依副本 best-effort 不重複；驗收測試讓 API 封裝的帳號由 TD agent 解開並在 fake exchange 上到 READY，掃 etcd 確認沒有明文，掃 API 回應、log 與 `audit` 確認沒有明文也沒有密文。PROTOCOL 為 v1.44。2026-10-23 E10-D074–E10-D079（E10-15，#320）：console 不加 UI 框架，Svelte scoped CSS 加一份全域 CSS 變數；SSE 用瀏覽器原生 EventSource，cookie 自動帶上，斷線由瀏覽器重連並送 Last-Event-ID，收到 reset 就重新 list；任何 API 回 401 導向 /login，登入後回原頁，登入頁只有 token 欄位，token 不進 localStorage；狀態是 Svelte 5 runes 的全域 store（登入使用者、region 可用性、controller_absent）；vite dev 把 /api proxy 到 localhost:8080，cookie 同源；CI 跑 svelte-check 與 build，不加 vitest 或 Playwright，端到端留到 #324。PROTOCOL 為 v1.43（#314 v1.41、#316 v1.42 平行，後合併者 merge main 並維持版本單調）。2026-10-23 E10-D048–E10-D055（E10-07，#312）：API 啟動時建立 Postgres 連線池但不等它連上，修改 E10-D024，readiness 仍只看 etcd，Postgres 不可用只影響稽核；稽核列進有上限的佇列由背景寫入，滿了丟棄並記 error log 與計數器，SIGTERM 在 E10-D027 的 10 秒收尾內寫完；cookie 一律 Secure；Bearer 優先，無效不退回 cookie；稽核 body 是 jsonb、上限 64 KiB，超過截斷並標記，非 JSON 只記長度，憑證欄位先移除；X-Request-Id 由伺服器產生；來源只記 RemoteAddr；登入不限速，token 至少 32 bytes 隨機值寫進部署說明，設定只存 SHA-256。PROTOCOL 為 v1.40。2026-10-23 E10-D056–E10-D061（E10-10，#315）：策略目錄沿用 `internal/platform/artifact` 的 oras-go client 與 manifest 解析，只加唯讀的 list tags 與 tag→digest，既有函式不變、STS 不受影響；不設快取，解析同時最多 8 個；一次取回完整 tag 清單並依名稱排序，`latest` 是普通項，以既有 limit／cursor 分頁；連不上或 5xx 回 503 `registry_unavailable`，manifest 無效或不是策略 artifact 回 502 `invalid_artifact`，找不到回 404；帳密與 `plain_http` 與 STS 相同，放在 API 設定，載入不連線；測試用 `registry:2`。PROTOCOL 為 v1.39。2026-10-23 E10-D043–E10-D047（E10-04，#309）：buf 改以 `go install` 裝進 `bin/tools`（v1.72.0），`buf.gen.yaml` 只留 `protoc-gen-go`；Go 工具 `oapi-codegen` v2.8.0、`oasdiff` v1.34.1、buf 由 `make tools` 釘版，Redocly CLI 2.60.0 與 `openapi-typescript` 7.13.0 以精確版本放在 web 的 devDependencies（peer 是 TypeScript 5，`web/.npmrc` 用 `legacy-peer-deps` 裝在 TypeScript 6 旁），`openapi-fetch` 0.17.0 放在 dependencies；Go 產碼在 `contract/gen/api/v1`（package `apiv1`），型別、strict server（`std-http-server`）、client 各一個 `.gen.go`；草案期間 `oasdiff breaking` 對 base 印出報告但不擋，base 沒有 spec 就略過；`make openapi` 做 lint 與產碼，`make all` 與 `make gen-check` 涵蓋 proto 與 openapi，TypeScript 在 `web/src/lib/gen/api/v1.ts`。並依 E10-D032 把 `FeedDecl.md_instance` 註解改成「API 驗證後填入」。PROTOCOL 為 v1.37。2026-10-23 E10-D042（E10-08，#313）：API 的寫入走另一條路徑 `state.APIWriter`，put 只限 spec 與 account，status 只能刪；條件不成立回 409，etcd 不可用回 503（寫入可能已生效），其他回 500；配號只查可用 region 的副本，要寫入的 region 不可用就直接回 503；任何 key 帶著 `session_id` 都算佔用，包括沒有 spec 的 status；reconcile 只刪沒有 spec 的 status，並以 spec 的 `create_revision` 對應 `session_records`。PROTOCOL 為 v1.36。2026-10-23 E10-D040、E10-D041（E10-06，#311）：`status/` 的寫入權是刻意的缺口，etcd RBAC 不分寫入與刪除，W5 要刪 status 所以 `api` 拿整個前綴的寫入權，W1 在 status 上只靠 API 程式守，不改成由 STS 清 status；測試補在 `TestRBACEnforced`。PROTOCOL 為 v1.35。2026-10-23 E10-D031–E10-D039（E10-03，#308）：Postgres 加 `session_records`，與 etcd 以寫入順序與 reconcile 保持一致（建立與 W5 刪除先寫 Postgres，結束先寫 etcd），Postgres 停機時不能建立 session；`sts_instance` 與 `md_instance` 由使用者指定、必填，`td_instance` 由帳號帶入；`params` 是 JSON object，YAML 只在編輯端；帳號的 `enabled` 不公開、一律 true，更新帳號會重啟 account worker；策略以 repository 路徑最後一段識別；時間 RFC 3339、enum 小寫、錯誤帶 `code`、labels 有上限；帳號以 body 的 `revision` 做 CAS，結束 session 伺服器端重試；單一 `/events`，事件帶完整狀態；端點清單與一期不做的。2026-10-22 E10-D030（E10-14，#319）：session JWT 刪除 `log.sts.{session_id}`（E10-D015，修改 E09-D006、E08-D018）；conformance `-violate subject` 改把單送到 `td.view.{api_id}`，有權限送但不是下單 subject，單不會出現在 `td.order`，檢查仍判不合格。PROTOCOL 為 v1.33。2026-10-22 E10-D023–E10-D029（E10-05，#310）：API 設定分部署 JSON、API JSON 與環境變數三類；這張票驗證全部設定，不連 Postgres、不連 registry，readiness 只看 etcd；每個 cluster 一個 Mirror，監看整個 keyspace；任一 region 可用則 /readyz 回 200 並列出 unavailable_regions，全部不可用才 503，/healthz 只看進程；SIGTERM 後 readiness 先轉 503、停止接新請求、等進行中請求最多 10 秒（可設定），以 0 結束；-web 從磁碟提供 web/build，非 /api/ 且找不到檔案回 index.html，未設則只提供 API；有憑證則 HTTPS 與 HTTP/2，否則明文 HTTP 只給 dev，監聽預設 :8080。PROTOCOL 為 v1.32。2026-10-20 E10-D019–E10-D022（E10-02，#307）：第二段清理只看 reconcile 手上的 spec 檢視，等第一次完整 list 完成才開始，避免空快取誤刪 log；刪除順序是 shim log、incarnation key、terminal.json、sessions/<id>/；log 檔與目錄建立後 Chmod 成 0640/0750，不受 umask 影響；shim 每次啟動把 ${WORK_DIR}/log/{plane} chmod 成 0750，已存在的舊 log 檔不改。2026-10-20 E09-D095、E09-D096（#327）：worker 結束之後，`SessionStatus` 要等重啟策略判斷過這個 incarnation 才寫 `RESTARTING` 或 terminal，還沒判斷寫 `STOPPING`；inputs 在 rename 之前就改成唯讀，`MakeReadOnly`、`RemoveWritable` 搬到 `internal/platform/artifact`。2026-10-20 E10-D002–E10-D018（E10-01，#306）：API 的契約用 OpenAPI 3.0.3，`api.v1` 不 import `session.v1`，編碼是 JSON、以 `oasdiff` 檢查相容；Go 以 oapi-codegen、TypeScript 以 openapi-typescript 產碼，前端不再產 proto；錯誤用 problem+json，列表用游標分頁；狀態串流用 SSE，event id 是各 region 的 revision，部署要求 HTTP/2；API 不參選，寫入以 create-only 與 CAS 為條件（P8，W10）；`session_id` 隨機配號，只保證存活與保留期內唯一；`api_id` 從 1 到 2⁵³−1 隨機配號；認證是每人一個靜態 token、單一角色，console 用 HMAC 簽章的 cookie；每個請求由中介層寫 Postgres 的 `audit`；帳號由 API 封裝、只寫不讀，一期不收交易多個 venue 的種類，仍被引用不能刪；algo_id 跨 session 做 best-effort 檢查；策略目錄以 registry 為準，最新版是 `latest` tag；讀取端每個 cluster 一個 Mirror，region 不可用時回部分結果，terminal 的 status 留下依賴失敗的原因，`AgentRecord` 加 `host`、`work_dir`；一期不接 `log.*`，運維上機器讀，log 檔 0640；STS 的 log 留到 spec 被刪才清；W5 保留期預設 7 天，registry 的資料 artifact 一期不刪；E10 結束時 OpenAPI 定案。2026-10-20 E09-D094：C3、C4 定案（#302）。2026-10-20 E09-D093：artifact manifest 的相容規則；`state.proto`、`session.proto`、`stsctl.proto` 與 manifest 定案（E9-12，#290）。2026-10-20 E09-D091：selector 的型別搬到各自 package 的 `selector.proto`，留在草案到 E5；E09-D092：定案前刪掉 `session.proto` 的 `reserved`（E9-12，#290）。2026-10-20 E09-D085–E09-D090（E9-10，#288）：推 `v*` tag 與 `workflow_dispatch` 打包 conn、account worker，推 `ghcr.io/bullionbear/` 的 `mftiko-md-conn`、`mftiko-td-account`，tag 是 git tag 與 `latest`；workflow 只推、不設可見度，第一個 tag 前由 org owner 把 Package creation 設成 Public。PROTOCOL §8 為 v1.25。2026-10-19 E09-D078–E09-D084（E9-09，#287）：worker 進程確定結束後才打包、另寫資料推送（暫存檔再串流）、打包內容與空 volume、terminal.json、state_out 欄位與 1–60 秒退避、清理順序。PROTOCOL §8.4 為 v1.24。2026-10-19 E09-D072–E09-D077（E9-08，#286）：拉資料 artifact、GC 保留正在拉或正在解的 input 層，以及 Pull 回傳後到 UnpackImage 結束前的策略層、inputs 與 rootfs 一起在背景拉取、解開後唯讀、每 10 秒量 volume、volume 在第一次 spawn 前建立。PROTOCOL §3.3、§5.4、§8.3、§8.4 為 v1.23。2026-10-19 E09-D066–E09-D071（E9-07，#285）：沒心跳直接 SIGKILL、`HUNG`／`no_beat` 由 STS agent 記在 `SessionStatus`、分階段的 `BeatTimeout`、`window_s` 在記憶體計數、shim 掉了不重啟、退避中是 `RESTARTING` 且同一 epoch 的 seq 接下去加。PROTOCOL §3.3、§6、§9.7 為 v1.22。2026-10-19 E09-D058–E09-D065（E9-06，#284）：背景拉取與解開、每一輪 GC(keep)、拿掉 `-worker`（取代 E09-D043）、Start 帶第一份 bindings 之後以 Command 推送、每個 session 的 grace_ms、ENDED 後的 terminal phase、worker 自己結束不重啟、測試沿用假 registry。PROTOCOL §3、§6、§8 為 v1.21。2026-10-19 E09-D050–E09-D057（E9-03，#281）：層直接解壓不 hardlink、記錄所用層；接受的 tar 項目與 `os.Root`；解壓大小與檔案數上限；永久 reason `artifact_unsafe_layer`；同一 digest 只解一次；`GC(keep)` 不判斷最新版；inputs 共用解開函式。PROTOCOL §8 為 v1.20。2026-10-19 E09-D028–E09-D034（E9-02，#280）：錯誤分永久與暫時、contracts 必須全含、下載不留半檔、快取只比大小、總大小先加總、帳密用 Docker config.json 與 plain_http、只接受單一 image manifest。PROTOCOL §8 為 v1.19。2026-10-19 E09-D042–E09-D049：E9-05 STS agent 骨架（#283）：與 #284 的分界、`-worker`、沒有 spec 不寫 status、terminal 不回頭、phase 對到骨架狀態、spec 編碼當 spawn hash、部署旗標、testplane 測試。2026-10-19 E09-D035–E09-D041（E9-04，#282）：contracts 由 sdk 嵌入 binary，pack 讀出填入、不比對 sdk 版本（E09-D035 取代 E09-D012）；CLI、os／arch 取自 ELF、可重現的層、`sdk.Contracts` 回傳複本、Docker `config.json` 憑證、推送前先驗證。PROTOCOL §8 為 v1.17。2026-10-19 E09-D009–E09-D027：E9 開工前的決定（E9-01，#279）：oras-go、一個共用的 registry、策略只依 digest 拉、pack 的 `contracts` 取自 buildinfo、大小上限、MD／TD 只做到 CI 打包、GC 在 reconcile 裡、只照指派 spawn、algo 重疊、預設值、`RestartPolicy`、no_beat、phase、`Bindings.Version`、清理、JWT 的位置、`state_out` 的 tag 與重試、定案範圍。2026-10-19 E13-D037–E13-D039（E13-07，#268）：reconcile 把寫入交給 region 佇列就繼續，不等結果；同一個 key 在佇列裡只留最新值，和本 term 寫過、view 還沒讀到的相同值不再寫；provider region 未完成首次同步時，`bindings` 保留現有的 status；home region 不一致時每個 cluster 只由 home 在那裡的 controller 寫入，一期視為設定錯誤；舊 leader 在新 leader 當選後仍可能寫入新 leader 還沒 fence 的 region，時間窗小於一個 TTL，由 #277 關掉。2026-10-18 E13-D033–E13-D035（E13-06，#267）：刪完 MD、TD 需求並在 view 確認不在才刪 `SessionDemand`，任一 provider region 過舊則指派跟著等；孤兒須所有 STS region 已同步且不過舊、沒有任何 instance 有這個 session；controller 只刪需求項目與 `SessionDemand`。2026-10-18 E13-D028–E13-D032、E13-D036（E13-05，#266）：結束不可逆，`end_reason` 留第一次的值；同一輪 `ended_by_spec` > `deps_failed` > `deps_timeout`；`ENDED` 之後不更新 `bindings`；逾時以最早到期設計時器，不等 60 秒全量；過舊 region 的 `READY` 不放行 `start`，`bindings` 照抄；#266 與 #267 平行，後合併者負責 rebase 解衝突。2026-10-18 E13-D022–E13-D027（E13-04，#265）：spec 的 revision 與指派的 `spec_revision` 不同時不重推、不新增不刪除、只記一次 log，不以 `invalid_spec` 結束；比較忽略 `updated_at_ns` 與 `DemandEntry.spec_revision`；`bindings` 寫目標、`status` 留空、`start` 固定 false；跨 region 不保證先後；一開始就 `ENDED` 且尚無指派則不寫；plane 標錯也是 `invalid_spec`。2026-10-18 E13-D017–E13-D021（E13-03，#264）：每個 instance 一個 Mirror，監看整個前綴再分類進 view；首次同步前的 region 視為過舊且不寫入；單一 reconcile 迴圈，dirty 立即再跑，60 秒全量走同一迴圈；view 帶每個 region 的 revision 與是否過舊；解不開的值不進 view、不寫不刪。過舊門檻預設 15 秒，放在 daemon 設定。2026-10-17 E13-D012–E13-D016（E13-02，#263）：controller 讀 mftiko-etcd-roles 同一份部署 JSON；骨架即有依 region 的寫入佇列，寫入入口先只供測試；AgentRecord 當選後寫一次，正常停下 revoke；ErrSuperseded 或選主斷線以非 0 結束，SIGTERM revoke 後以 0 結束；etcdtest 一次起多個 region。2026-10-17 E13-D001–E13-D011（E13-01，#262）：controller 分純邏輯與 daemon；`deps_timeout_s` 從 `SessionDemand.issued_at_ns` 起算，0 取 60 秒；spec 只有 `desired_state` 可改，刪掉當成 `ended_by_spec`；不合法的 spec 以 `invalid_spec` 結束；每個 region 第一次寫入前才 fence；和 region 斷線時保持現狀、寫入依 region 排隊；N4 看 `WorkerStatus`；全量 resync 60 秒；E13 拆成 E13-02–07。PROTOCOL §3 為 v1.10。2026-10-17 E08-D062–E08-D064（E8-13，#237）：`hello` 從 params 解析商品、讀 Book 與 Recon、遠離市價掛一張限價單，`OnOrder` 確認 `ORDER_STATUS_ACCEPTED` 後撤單；`tape-keeper` 不改。conformance 檢查改到符合後面的決定，不改 SDK。每個 PR 用 `examples/strategies/conform` 跑完整份 Go SDK conformance。PROTOCOL §9.8 為 v1.09。2026-10-17 E08-D057–E08-D061（E8-15，#239）：本地訂單簿由 SDK 向 MD 的 `md.view` 要快照再套 `orderbook_delta`，不向交易所要；斷號或 `conn_epoch` 改變就重同步；`not_synced` 與其他讀取失敗沿用 E04-D024 的 1–30 秒退避；等快照的緩衝上限 10000 筆或 10 秒，到了就丟掉並重要，這些 delta 不是 hook；檔數用 `book.DefaultN`，與 MD conn worker 一致，不用快照窗口大小；tick 仍取自 SYM `Info` 的 `price_tick`；未同步時 `Book` 回 `(nil, false)`；`reset` 清掉本地本並重要快照。PROTOCOL §7、§9.4、§9.5 為 v1.08。Go 實作在 `sdk/internal/bookpath`。E08-D057 改寫 E04-D002 晚加入那一句，不改編號。2026-10-17 E08-D053–E08-D056（E8-14，#238）：餘額三格由 TD 填、SDK 原樣覆蓋；Recon 整份換 OMS，該次 ReconRequest 送出之後才 Submit 且快照沒有的 pending_new 保留；Fill 不動 Ledger；讀取回傳 protobuf 複製、不阻塞。PROTOCOL §7.2、§9.6 為 v1.07。Go 實作在 `sdk/internal/tdpath`。2026-10-17 E08-D050–E08-D052（E8-10，#234）：Init 連不上 NATS 時在 deadline_ms 內退避重試，到期 INIT_FAILED；讀取順序只是策略自己呼叫的函式，不在啟動時自動還原；volume 原子寫入是暫存檔、fsync、rename、目錄 fsync。PROTOCOL §9.1、§9.2、§9.3、§9.4 為 v1.06。Go 實作在 `sdk`。2026-10-17 E08-D046–E08-D049（E8-09，#233）：binding 只套用較新的完整快照，一期 selector 回 error、不解析成員，subject 用 binding 給的並依集合增刪訂閱；SDK 不自動要 tape、不扣住 live，策略用 `TapePage.head` 去重（改寫 E04-D037 的 SDK 那一句）；`md.view` 與 `md.fetch` 等 `recv_window_ms`，逾時或無 responder 時 callback 收到 error，回覆走 TD buffer；壞 envelope 丟掉、計入 `QueueStat.dropped`、寫 log、不結束 session。PROTOCOL §7、§9.3、§9.4、§9.5 為 v1.05。Go 實作在 `sdk/internal/mdpath`。2026-10-17 E08-D040–E08-D045（E8-08，#232）：OMS 先做自己 algo 的單，Ledger 與完整狀態機留給 #238；第一份 Recon 扣住事件，上限 65536，滿了 FAILED；Recon 逾時或無 responder 以 1–30 秒退避重問；Submit 逾時用 recv_window_ms，標 UNKNOWN；新 incarnation 或 seq 斷號重走 Recon；第一份回覆時，扣住的較舊 incarnation 可丟、較新的再問一次；撤單逾時不標 UNKNOWN。PROTOCOL §9.4、§9.6 為 v1.04。Go 實作在 `sdk/internal/tdpath`。2026-10-16 E08-D028–E08-D039（E8-12，#236）：conformance 是命令列工具加假 shim、行程內 nats-server，與一次一種違規的 fakeworker；每個 PR 的 CI 跑這些違規檢查。標準策略的設定是 `InitPayload.params` 的 JSON，欄位與檢查 id、報告 JSON 寫進 PROTOCOL §9.8。這一期只收執行命令。`Init` 之前的亂序與 `Start` 之前的 `Stop` 見 E08-D037。Go SDK 整套通過留到 E8-13。同日 E08-D023–E08-D026（E8-07，#231）：事件照常立即交付，SDK 不為沒見過的 UID 查 SYM、不扣住事件，只追蹤 Resolve 過的商品；`Info(uid)` 回 `(symbol.Info, error)`，查不到回 error，框架不重試；已解析商品下架時以 `GetByUidRequest` 加 `include_inactive` 重拿，保留最後一份並標為不活躍。E08-D004 追蹤範圍那句改寫，不改編號。PROTOCOL §9.4 第 8 條為 v1.02。客戶端在 `sdk/internal/symclient`。2026-10-16 E08-D027（E8-11，#235）：維持每個 feed 一條權限，natsdev 產生的 server 設定把 `max_control_line` 調到 262144，STS 在簽發 session JWT 時檢查 CONNECT 引數，超過就拒絕並寫明 feed 過多。同日 E08-D018–E08-D022（E8-11，#235）：session 的 md 權限精確到每個 feed（修改 E09-D006），publish `md.v1.view.{inst}.{topic}.{ticker}`、subscribe `md.v1.f.{inst}.{topic}.{ticker}`，不再用 `{inst}.>`；`md.v1.fetch.>` 不變；td 每個 api_id 各一條；去重後排序，只列 allow；沒有帳號不給 td，沒有 feed 不給 md.v1.f／md.v1.view。token 含 `.`、`*`、`>` 或空白回 error，不轉義。`SessionPermissions` 不連 etcd、NATS。spec 含 selectors 回 error。UniversalTicker 文法不含 `.`（小數點是 `D`），E08-D019 不需另議。同日 E08-D015–E08-D017（E8-05，#229）：TD buffer 上限是 SDK 常數 65536，不進契約，滿了以 FAILED 結束；MD ring 是一塊預先配好的 32 MiB `[]byte`，每則可變長，放不下就丟最舊的若干則直到放得下（跳過正在讀的），單則大於整個 ring 直接丟並計入 `dropped`；hook 執行超過 100ms 計一次 `hook_slow`，`hook_blocked_ms` 是目前這個 hook 已執行的時間。同日 E08-D011–E08-D014（E8-06，#230）：cloid 產生器一個進程一顆計數器，所有 algo、所有帳號共用；seq 用完往前借毫秒且不設上限，`ts_ms` 可能超前牆鐘；狀態不持久化，跨 incarnation 撞號由 TD 冪等回 106。產生器在 `sdk/internal/cloidgen`，以一個 uint64 的 CAS 更新。E06-D002 的 seq 與跨 incarnation 兩句改寫，不改編號。同日 E08-D006–E08-D010（E8-03，#227）：c4 加 `Stopper`（`OnStop` 在 `Exiting` 與 `Stopped` 之間）；`Run` 返回時 `OnStop` 套 plane 在 Init 後交出的 `hook_timeout_s`，逾時 exit 21、不送 `Stopped`；`deadline_ms` 逾時不等 hook，exit 10，不呼叫 `Stopper`；Start 失敗（非逾時）才呼叫；SIGTERM 由 main 的 `NotifyContext` 傳入 `Serve`；刪除 `sdk/internal/c4`。同日 E08-D005：策略的 hook 依 topic 拆開；SDK 先更新本地 book、OMS、Ledger 再呼叫 hook；`OnOrder` 是單的每一次變化，`OnReject` 只給撤單類請求的拒絕（E8-04，#228）。2026-10-16 E08-D003：`recv_window_ms`、`hook_timeout_s` 由 STS agent 解析成實值，經 `stsctl.InitPayload` 交給 worker，不會是 0（E8-02，#226）。2026-10-16 E06-D038：成交發在 `td.evt`，目前沒有人落庫，由誰落庫見 PROTOCOL §10，不會是 TD agent（修訂 E06-D001）。同日 E06-D039：TD agent 沒有 NATS user（修訂 E06-D011）。同日定案事件連線的計畫性換線跑一次 recon、期間不報 DEGRADED、reason 仍是 RECONNECT，E06-D035–E06-D037（#200，修改 E06-D014）。2026-10-16 定案 `td.proto`（E06-D034 的後續）：從 `buf.yaml` 的 `breaking.ignore` 移出，PROTOCOL §1 改回定案。2026-10-16 定案 `read.proto`（E08-D066 的後續）：從 `buf.yaml` 的 `breaking.ignore` 移出，PROTOCOL §1 改回定案。同日 E06-D034：比照 E6-15 分兩個 PR。#242 刪項目、照 E06-D033 不留 `reserved`，`td.proto` 仍留在 `breaking.ignore`；#242 合併後另開小 PR 移出清單、PROTOCOL §1 改回定案。同日 E08-D066：比照 E6-15 分兩個 PR。#241 刪欄位、照 E08-D065 不留 `reserved`，`read.proto` 仍留在 `buf.yaml` 的 `breaking.ignore`，PROTOCOL §1 維持草案；#241 合併後另開小 PR 移出清單並把 §1 改回定案。同日 E08-D003：`orderbook_delta` 不進 prehook，一律套進本地 book，只有呼叫 `OnBook` 受 `recv_window_ms` 管；prehook 的 `now` 在 dispatcher 取出時取。同日 E08-D003 補上結束的順序：收到 `Stop` 先送 `Exiting`，交付完 TD buffer 再跑 `OnStop`，`CRASHED` 不交付；`grace_ms` 管整個收尾；自己結束的路徑上 `OnStop` 套 `hook_timeout_s`。同日定案 P15：策略 API 以 UID 為商品身分，ticker 只在 `OnInit`、`OnStart` 的 `Resolve`；SDK 只追蹤看過的商品，不從 spec 推要載入的表，E08-D004。2026-10-15 每一則行情的 payload 帶 UID，不帶 ticker 字串：`FeedEvent`、`BookSnapshot`、`FetchReply` 加 `uid`，`md.fetch` 的請求以 UID 指定商品，subject 不變；`read.proto` 回到草案，E8-01（#222）改完再定案，E08-D065。同日 `td.view` 的 OMS 與帳本查詢換成一個 `ReconRequest`，回一份 `Recon`；拿掉 `Submit.algo_id`、`Cancel.algo_id` 與 104，TD 不解碼 `client_order_id`；`td.proto` 回到草案，E6-17（#220）改完再定案，E06-D033。同日定案 `tdctl.proto`、`td.proto`（E6-15，#175）：兩份移出草案清單，PROTOCOL §1 改為定案，E06-D029。同日拿掉 `OmsViewRequest.settled`（欄位 2），改成 `reserved 2` 與 `reserved "settled"`，`algo_id` 仍是 3，E06-D032。同日定案前刪掉 `td.proto` 的 `reserved`，欄位號碼不重排；`tdctl.proto` 沒有 `reserved`；兩份仍在草案清單，移出清單與 PROTOCOL §1 定案是這份合併後的後續，E06-D029。同日保留歷史補正欄位（`AccountSettings` 的 interval 與手動觸發、`td.backfill` 相關欄位），E06-D030。同日每一則 `td.view` 回覆帶 `cancel_batch`，給策略拆批，E06-D031。2026-10-15 修訂 E06-D010（E6-14，#174）：撤單沒有回答、過了界線查到單仍在簿上時，清掉 `cancel_sent_ns`，發 `CANCEL_REJECTED`、code 120，策略可以再撤。2026-10-15 定案兩把鑰匙：OMS 與交易所之間（事件、recon、查單）以 order_id 對單，沒有 client id 的單仍以 order_id 追蹤；系統裡面（策略、td.order、Cancel、BatchCancel、td.view、向外的 OrderUpdate）以 cloid 為鍵。Cancel 與 BatchCancel 只收 client_order_id，order_id 欄位保留，OMS 送出時換成那張單的 order_id。解不開成平台 cloid 的不屬於任何 algo。BatchCancel 不檢查歸屬。E06-D002、E06-D005、E06-D015、E06-D017、E06-D023、E06-D025、E06-D026 改寫，不改編號。2026-10-15 修訂批次撤單（E6-09，#169）：BatchCancel 是明確的訂單清單，TD 一對一轉成一筆交易所請求，不挑單、不分批；CancelBatch 為 1 時本地拒絕；超過上限仍送出並原樣回報交易所的拒絕；一批算 OrderRate 的一筆；商品解不出來只影響那一筆。一次撤掉某 algo 的全部掛單改由 STS SDK 做。E06-D002、E06-D025–E06-D028 改寫，不改編號。2026-10-14 定案下單與撤單（E6-08，#168）：記憶體 cloid 冪等、同 cloid 內容不同回 106、每筆 `recv_window_ms`、`OrderRate` 用到 90% 回 108、查單失敗逾 5 分鐘 `DEGRADED`、`UNKNOWN` 可撤、`TradeStream` 不可用改走 REST，E06-D018–E06-D024（修改 E06-D005、E06-D017）。2026-10-14 定案以 cloid 查單（`Account.QueryOrder`，不存在是 `ErrNotFound`）、請求有效期 `AccountConfig.Validity`，以及三家讀持倉模式；Bybit 以 `positionIdx` 判斷，E06-D017。2026-10-13 定案 td-agent：兩個前綴、ack、spec、endpoint 經 Init；執行中不檢查持倉模式，拿掉 `UNAVAILABLE`；algo 由 STS agent 檢查，E06-D016（修改 E06-D002、E06-D007、E06-D012、E06-D014）。2026-10-13 定案 `mdctl.proto`、`md.proto`、`read.proto`（E4-12，#152）：三份移出草案清單，PROTOCOL §1 改為定案，E04-D060。2026-10-13 刪掉 `mdctl.proto`、`md.proto` 的 `reserved`，現有欄位號碼不重排；三份仍在草案清單，移出清單與 PROTOCOL §1 定案是 #204 合併後的後續，E04-D060。2026-10-13 定案 account worker 的 OMS 與帳本：OMS 只放活著的單，`settled` 不再有作用；recon 期間事件先排著再重放；斷線期間消失的掛單記成 UNKNOWN；槓桿取自持倉，E06-D015（修改 E06-D005、E06-D006）。同日定案 tape 的 worker 級 arena（#189）：64 KiB chunk、硬上限含 slot 與索引、回收全 worker 最舊的 chunk，E04-D058–E04-D059。2026-10-13 定案 P14（#181）：行情 subject `md.v1.f.{inst}.{topic}.{ticker}`、`md.v1.view.{inst}.{topic}.{ticker}`，`md.v1.fetch.{venue}`，`sym.v1.changed.{venue}.{category}`，E04-D055–E04-D057。實作在 #194。2026-10-12 定案 account worker 的連線角色與狀態：只有事件連線決定 DEGRADED，計畫性換線先接後斷；一期不做 BinancePM，E06-D014。同日拿掉 top-N 與 FeedEnd（#180）：book topic 只剩 `orderbook_delta`，寫 `orderbook` 回 `invalid`，E04-D053–E04-D054。2026-10-12 定案 tape ring 與 `md.view`（E4-06）：一個 feed 一個 ring，讀取無鎖，以 `head` 接 live，E04-D035–E04-D052。同日定案帳號憑證以 TD instance 的 X25519 公鑰封裝：NaCl sealed box 加版本與指紋，`api_id`、`kind` 一起封、解開後比對，封裝工具 `mftiko-seal`，E06-D013。同日定案持倉模式由 `AccountSnapshot.HedgeMode` 帶回，以及給 account worker 測試用的假帳號 `exchangetest.AccountVenue`，E06-D012。2026-10-11 定案 TD 的契約與 NATS 身分（E6-02）：`td.order` 只回 ack 或本地拒絕，單的每一次變化都是 `OrderUpdate`，1xx 只列一期會碰到的；訂單狀態加 `ACCEPTED`（上簿），撤單在途改成旗標 `cancel_sent_ns`，拿掉 `PENDING_CANCEL`；account worker 每個帳號一份 NATS 憑證，由 TD agent 簽，E06-D010–E06-D011。同日修訂 E06-D001：TD agent 不查 SYM，歷史補正的交易所讀取比照 md.fetch。同日修訂 E02-D002、E04-D001：MD agent 不查 SYM，conn worker 用活躍表推出 atom 並 diff 訂閱，並在 Report 以 FeedStatus 回報每一筆 feed。同日更新 E04-D015、E04-D026：MD instance 訂閱 `sym.changed.>`，conn worker 在 Init 載入該 venue 的活躍表。同日定案本地訂單簿：一張快照加增量、seq、top-N、序號權威與 fixture，E04-D024–E04-D029。同日再定本地簿為刻度環，E04-D030–E04-D033。同日再定 SnapshotStream 斷號或 checksum 失敗時只重訂該 channel，E04-D034。同日修訂 E04-D026：SnapshotREST 不設價格邊界、不因距離重抓。同日定案 TD 的幾件事（E6-01）：連線與重建是 TD 的事、STS 是最後一道防線；結果未知分兩層，STS 交出 UNKNOWN、TD 主動以 cloid 查單；`td.view` 只讀本地快照；一期只支援單向持倉；TD 的 Policy 與限速，E06-D004–E06-D009。同日更新 E04-D020：`Used.ResetAt` 是計數器歸零的絕對時間，用量到點作廢；429 與 418 依 `Retry-After` 停送，沒有才等到 `ResetAt`。同日定案 md.fetch：subject 分 venue、K 線分頁、限速、不快取、活躍表解析、平台執行檔，E04-D018–E04-D023。同日定案 dev NATS：operator／account JWT、full resolver、MD 與 md.fetch 的權限與 inbox，E04-D012–E04-D017。同日更新：`md.f` 的 v1 envelope 固定 40 bytes，拿掉 `len`，加 `digest`（payload 的 xxHash64），見 E04-D005–E04-D011。2026-10-11 定案 P6 與 P9：`DEAD` 的時機，以及 worker 版本只在下次 spawn 取用，見 E01-D006、E09-D002 與 PROTOCOL §3.5、§8。2026-10-09 更新：幣與數量的單位、SYM listing 分活躍表與終結表，以及 symbol UID，E02-D003。同日再定 UID 的分配：SYM 發隨機、唯一、永不重用的整數，不是單調序號；跟列持久化，重啟與重載不改。寬度與範圍已定：契約型別 `uint32`，`1..4294967295`（`1..2^32-1`），0 非法；Postgres 欄位是 `bigint`，`CHECK (uid BETWEEN 1 AND 4294967295)`，見 PROTOCOL §10 P13）。只收已經講定的事；還在討論的放在 [§待討論](#待討論)。協議與契約見 [PROTOCOL.md](PROTOCOL.md)。>> 不照抄 mftik `refactor/process-planes`，也不照抄 Strategon 的 agent。兩者都只當參考。
>
> **編號。** 決策編號是 `Eee-Dsss`：`ee` 是 epic 的兩位編號，`sss` 是這個 epic 內的三位序號，例如 E10-D009 是 E10 的第 9 條決策。每個 epic 自己往下編，不跟其他 epic 共用流水號。不屬於單一 epic 的跨平面、架構層決策歸 E00（E00-D001、E00-D002、…）。決策編號和票號是兩回事：E10-07 是 E10 的第 7 張票，E10-D007 是 E10 的第 7 條決策。2026-10-24 以前的 commit、issue 與 PR 用舊編號 `D<epic><兩位序號>`（例如 D1009 是 E10-D009，D105 是 E01-D005，D003 是 E00-D003），例外見 2026-10-24 的決策紀錄。

## 已定

### E00-D001 三個平面是三種專門的 agent

STS（策略）、MD（行情）、TD（執行）各是一種 agent，角色接近 Strategon 的 agent：收 desired、reconcile、拉起並看管進程。但每種 agent 只管**一種已知形式**的 worker，不需要 Strategon 為了跑任意程式而有的 OCI driver 與 download／verify／switch 部署階段。

- desired 來自 controller（E09-D007），agent 之間沒有控制流。
- 三種 worker 都從 artifact 起（E09-D002）：STS 的是各個策略；MD、TD 各只有一種，都在這個 repo 裡實作，由 CI 打包。

| agent | worker 形式 | worker 身分 |
|---|---|---|
| STS | Strategy Actor 的宿主：runtime ＋ 一個繼承 Strategy Actor 的策略 | `session_id` |
| TD | 一個帳號的私有 websocket ＋ HTTP 連線池 | `api_id` |
| MD | 一條公用 websocket | `(venue, route, n)`（E04-D003） |

### E01-D001 agent 的骨架

| 抽象 | 職責 | 共用或平面自有 |
|---|---|---|
| Intake | 收 desired（watch etcd）：需求項目與 STS 的指派都來自 controller（E09-D007） | 共用 |
| Planner | desired → 每個 worker 的 spawn spec ＋ live config，純函數 | 平面自有 |
| Fleet | 以 shim 為底的 worker 集合：spawn、attach、推 live config、觀測、release。supervisor 在這裡，只管 incarnation 與 backoff | 共用 |
| Reconciler | 單一 loop，diff 計畫與觀測，產生動作 | 共用 |
| Policy | 失敗語意與重啟規則 | 平面自有 |
| Provider | MD、TD 依需求項目推導資源並回 ack／nack。等依賴就緒的 Gate 在 controller（E09-D007），STS agent 只照指派送 `Start` | 平面自有 |

各平面最重要的設計決定，是哪些欄位放在 **spawn spec**（變了只能換 incarnation），哪些放在 **live config**（推給活著的 worker）。

| | spawn spec | live config |
|---|---|---|
| STS | 策略、參數、依賴綁定的初始值 | stop／fail、依賴綁定的變化 |
| TD | 帳號、venue、憑證 | drain |
| MD | venue、route、連線 URL（endpoint 參數，E04-D003） | atom 集合（帶 generation） |

### E01-D002 shim 保留，是平面輪換時收養 worker 的地方

不採用 Strategon agent 的 adopt（pid ＋ start_time ＋ pidfd）。shim 是 worker 唯一的父進程，agent 輪換時 worker 留在 shim 底下。

### E01-D003 shim 與 worker 同壽命，輪換只換 agent

- shim 不原地升級、不重啟。shim 的版本就是 spawn 它的 release。
- 新 agent 必須能和舊版 shim 溝通：shim 協定獨立版本、只做向後相容的擴充。
- shim 的修正只在 worker 換 incarnation 時生效。

### E01-D004 shim 消失，worker 必須死

- shim 消失時，worker 自行 graceful stop。
- 理由是 at-most-one：agent 必須能把「shim 不在」當成「worker 不在」。parent 關係也轉不走，新 shim 拿不到 wait status，也收不到孫進程。
- agent spawn 新 incarnation 之前，仍以 pid ＋ start_time 確認舊 worker 已消失。

### E01-D005 控制權的 fencing 在 shim

agent 每次啟動帶 epoch。shim 只接受 epoch 最大的 agent 的指令；更大的 epoch attach 時，舊連線被踢掉。新舊 agent 可以短暫重疊。

### E00-D002 控制走 shim，資料走 NATS

- 控制：agent ⇄ shim ⇄ worker。shim 只轉送、不解讀。live config 的推送因此天然受 epoch fencing 保護，控制面不依賴 NATS。
- agent 不在時，shim 把 worker 的事件帶 seq 緩衝；新 agent attach 時從上次 ack 的 seq 補讀。exit 另外落檔。
- 資料面（行情、下單、回報）走 NATS。
- controller 與 agent 之間的控制走 etcd（E00-D004），不走 NATS。agent 之間沒有控制。

### E01-D006 incarnation

- 同一個 worker 身分下，實體進程（shim ＋ worker）的計數。每換一個進程加 1。
- **計數存在 etcd，不綁 lease。** key 是 `/{plane}/{inst}/incarnation/{h}`，寫入者是該平面的 agent，三個平面共用同一套（PROTOCOL §3.2 W6）。spawn 前先以 txn 取號，條件是 writer key 的 rev（§3.2 W2）：被取代的 agent 取不到號，也就 spawn 不了。
  - 不放 `WorkerStatus`：worker Release 之後它就被刪掉。同一個 worker key 再起來時，號碼會從 1 重來，和舊進程的 `td.evt` seq、`Recon` 撞號。
  - 不放 `/td/{inst}/account/{api_id}`：那是 API 寫的宣告（W1）。TD agent 寫進去會和 API 搶同一個 key，每次 spawn 都改動宣告的 revision，RBAC 也得讓 TD agent 能寫放憑證的前綴。MD 也沒有對應的宣告可放。
  - 不由 `(epoch, 本機 seq)` 推導：incarnation 和 epoch 就不再是兩個獨立的計數（見本節最後一條），型別也要從 `uint32` 改成兩個欄位。
  - 不用雜湊：雜湊只分得出不同，分不出新舊，「舊的晚到就丟」需要比大小。
  - 號碼只保證遞增，可以跳號。key 不刪：worker 停掉之後同一個 worker key 還會再起來，刪了就從 1 重來。例外是 STS 的 session 結束之後，見 PROTOCOL §3.2 W6。
- 訊息與事件帶 `(身分, incarnation)`：舊的晚到就丟；下游看到 incarnation 變了就重新同步。
- 換 incarnation 一律 delete-before-create：舊的確認消失之後才起新的。
- 換版不排程換 incarnation。下次 spawn 才取當時的版本：MD、TD 取最新（PROTOCOL §8），同一個 session 重拉仍用 spec 釘住的策略 digest。跑著的進程保持原 binary。部署新的 agent，或選主輪換，都不因此重啟 worker：行情連線不斷、tape 還在，account worker 與 session worker 也不斷。
- **MD 連線不做 make-before-break。** 新舊版並存時，新版 worker 發在新的 subject，平台內不做遷移。SDK 一律用 binding 給的 subject，不自己組（PROTOCOL §7）。版號放哪裡已由 E04-D055 取代：前段現在就是 `v1`（`md.v1.f.{inst}.{topic}.{ticker}`），不是之後才加。
- 和 agent 的 epoch 是兩個獨立的計數：輪換 agent 不動 worker，換 incarnation 只動一個 worker。

### E09-D001 跨平面依賴由 controller 以宣告式的需求表達

```
API server ──Spec、Account──▶ etcd
controller ──STS 指派──────▶ STS agent
controller ──需求項目──────▶ MD agent、TD agent
MD agent、TD agent ──ack／nack──▶ controller
STS agent ──SessionStatus──▶ controller、API server
STS、MD、TD ─────▶ SYM
```

所有箭頭都是「寫 etcd、對方 watch」（E00-D004）。agent 之間沒有控制流；平面之間只有資料流，走 NATS。2026-10-07 以前的版本由 STS agent 發綁 TTL 的 lease，改寫的理由見本條最後。

- **controller 從 session spec 推導需求，不是 STS agent。** 每個非 terminal 的 session：每個 feed、每個 selector 一個 MD 需求項目；每個帳號一個 TD 需求項目。controller 同時持有所有平面的狀態（ack、SessionStatus、AgentRecord、WorkerStatus），跨平面的判斷都在它那裡做（E09-D007）。
- **需求是宣告式的，不綁 TTL。** 需求項目是一般的 key，存在的理由是「controller 決定它應該存在」，和 controller 活著與否無關。controller 停機或輪換時，所有需求原樣保留，MD、TD 照常服務。
- **只有 controller 明確刪除時，需求才消失：**
  - session 結束：controller 先把 STS 的指派改成結束，等 `SessionStatus` 進入 terminal、從 WorkerStatus 確認 session worker 已經消失，再刪這個 session 在 MD、TD 的需求項目，最後刪指派（PROTOCOL §3.3）。
  - 孤兒：controller 每次當選、每次全量 resync，都比對「應該存在的」與「實際存在的」。key 裡有 `session_id`，掃前綴就知道哪些項目的 session 不存在或已經 terminal，刪掉。回收靠比對，不靠計時器。
  - 新 leader 接手時先 relist 再比對，只改有差異的項目，不先全部刪掉再重建。萬一刪掉又馬上加回，linger（E09-D005）會吸收掉。
- **需求的粒度是行情原子，或能推導成原子的規格**（feed、selector）。controller 不自己展開：需求項目送到 MD agent，由它的 orchestrator 推導成 atom，再 reconcile → shim → conn worker。期權鏈、滾動合約這類動態訂閱全在 MD 內部消化，需求不必跟著變。controller 只管到資源這一層，不碰進程：要起哪些 worker 由各 agent 決定。
- **兩層 refcount：**
  - atom：0→1 訂閱，1→0 退訂。
  - 連線（conn worker）：refcount = 0 若且唯若這條連線上所有 atom 的 refcount 都是 0。
- **refcount 是 owner 集合的大小，不是加減計數器。** 每個 atom 記 `{(session, feed 或 selector)}`。需求項目會重寫、會重試，集合是 level-triggered，計數器會漂移。多個 feed 共用一個 atom 也自然處理。agent 重啟時 list 一次需求前綴，就能重建所有 owner 集合，不需要保存計數。
- **只有一層 linger（E09-D005）。** atom 的 owner 集合歸零後，等 linger 才退訂；連線在最後一個 atom 退訂時就停，最後一個 atom 的 linger 就是連線的 linger，不另設參數。
- **TD 和 MD 一樣依 refcount 起停 worker。** 需求的單位是帳號，每個 `api_id` 一個 owner 集合 `{(session, api_id)}`。0→1 時拉起 account worker（私有 websocket ＋ HTTP 連線池，Start 時 recon）；1→0 時 linger 之後才停。worker 存在就等於這個帳號可以交易，沒有另外的交易層開關，也不保留 mftik 的常駐層。閒置帳號的歷史不為此拉起 worker。成交發在 `td.evt`，由誰落庫見 PROTOCOL §10，不會是 TD agent；交易所讀取是無狀態服務（E06-D001、E06-D038）。
  - 代價：帳號的第一個 session 要冷啟動（建連線、recon）。`OnInit` 可以和它並行，影響有限。
  - session 進入 terminal 時，TD 需求項目依上面的順序刪除。平台不替策略撤單（E06-D002）。
  - `restarting` 期間需求項目不刪，策略重啟不會讓 account worker 跟著起停。
- **ack／nack 是三種狀態，粒度是資源：**
  - ack：資源正常。
  - nack：provider agent 明確知道某個資源不成立或死了（它透過 shim 管 worker，所以知道）。controller 收到 nack 就結束 session（E09-D007）。
  - 沉默：不知道。ack 不存在表示 provider 還沒處理這個項目。provider 的 agent 不在（`AgentRecord` 不在，E00-D004）時，ack 是它最後寫下的狀態；可能只是在輪換，worker 照跑。兩種情況 controller 都維持現狀，不能當成 nack。
- 資料面的依賴（session worker ⇄ conn worker、account worker）走 NATS，不經過 agent。
- **為什麼不再由 STS agent 發綁 TTL 的 lease：**
  - TTL 把「需求還在不在」綁在「發送者還活不活著」上。發送者只是斷線，TTL 一到，正在交易的資源就被收掉。controller 只有一個、橫跨所有 region（E00-D005），綁 TTL 的話，跨 region 的網路一抖就會停掉遠端 region 的交易。
  - 需求留太久，最壞是多開一條連線，浪費但不危險。所以「不確定就保留」，回收交給 controller 的比對。
  - 這和 Strategon 的 fencing lease 不同：那個保護的是「同一個策略只有一份在下單」，交易所無法檢查 token，只能靠時間，所以必須綁 TTL。mftiko 一期不做跨主機的故障轉移，不需要它（E09-D008）。

### E00-D003 Strategon 那一側的前提

「輪換時 shim 不動」依賴以下四條。任何一條破了，就等於 shim 被動了，worker 跟著死。

1. 停 agent 的信號只送到 agent 的 process group。shim 以 `setsid` 開自己的 session。
2. 開 `oci_host_pid`，不建 PID namespace。
3. 不以 `cgroup.kill` 整個 slot 停止。要有測試鎖住。
4. 舊 release 的 rootfs 仍被 shim 或 worker 當作 root 時不 GC。

### E02-D001 Symbol 照抄 mftik

UniversalTicker（`<Venue>_<Category>_<SYMBOL>`）、`Parse` 嚴格／`Resolve` 寬鬆的分工、SymbolResolver（翻譯是查表不是字串轉換）、listing 與 filters、feed key `topic.UniversalTicker`。SYM 是普通服務，不是 agent。

### E04-D001 feed 與 atom

- **atom** 是交易所那一側的一個訂閱項：在一條 websocket 上 subscribe 一次、交易所 ack 一次的最小單位，身分是 `(venue, route, channel)`，channel 逐字照抄交易所參數；route 是 WS 路徑，由 adapter 從商品推得（E04-D003、E02-D002）。conn worker 交給 adapter 的是活躍表上的那一列。
- **feed** 是策略宣告的行情：`topic.UniversalTicker`。
- **在同一個 venue 上，feed → atom 一律 1:1。** 一個平台 topic 在一個 venue 上只對應一個 channel。
- **atom → 事件可以 1:N。** 一個 frame 解碼出多種事件（Deribit `ticker.*` → quote、greeks、OI），是解碼端的扇出，不需要 join。
- topic 的模型依「交易所原生同時推送的欄位」定義。某 venue 不推的欄位是 optional，並提供能力表給策略查詢。`ticker` 只有 24h 統計；最佳買賣是 `bestquote`。
- 多個 feed 可以共用一個 atom。owner 是 feed 的集合，由 MD agent 算（E09-D001），沒有組合語意。conn worker 用 feed 清單與活躍表推出 atom，再對已訂閱的 channel 做 subscribe／unsubscribe 的 diff。
- 沒有 projector、沒有多 atom 組合出來的 down 狀態、沒有多 atom 的 late-joiner 等待。feed 的狀態就是它那一個 atom 的狀態。
- 交易所沒有推送的東西不做成 feed，只能查詢（例如 Binance UM 的 open interest，走 md.fetch，E04-D002），不以 REST 輪詢假裝成 feed。
- 策略需要兩個 feed 的最新值時，SDK 提供 opt-in 的小工具，那是函式庫，不是平台語意。

### E10-D001 API 的角色

- API 層以宣告式設定操作平面，只負責開始與結束（start／end）。
- 控制平面分兩個進程（E09-D007）：API server 只寫使用者的宣告（session spec、帳號）到 etcd（E00-D004）；controller 依宣告與各平面的狀態推導需求、決定 session 起停。兩者都不直接呼叫 agent。
- API 不查 SYM（E02-D002）。

### E00-D004 控制面在 etcd，agent 無狀態

- 宣告、需求項目、STS 的指派、ack／nack、不能重新推導的 agent 狀態，全部放 etcd。agent 和 controller 在本機都不留狀態：desired 從 etcd 讀，worker 的實際狀態從 shim 取回。
- API server → etcd、controller → agent、agent → controller 都是「寫 etcd、對方 watch」，沒有 NATS request-reply。控制面的契約就是 etcd 的 key 結構，加上每個前綴的寫入者。
- 每個前綴的寫入者由 etcd 的 RBAC 強制，不只靠程式自律：每個進程有自己的使用者，角色從 instance 清單產生（PROTOCOL §3.2 W1）。被取代的舊 agent 靠 writer key 擋（W2），寫錯前綴的程式靠 RBAC 擋，兩道各自獨立。
- **etcd lease 只有一種：選主 session（PROTOCOL §3.2 W2、W7）。** agent 與 controller 的 `AgentRecord` 綁在自己的選主 session 上：它在，就表示這個 instance 有活著的 agent，以及是哪一個 term。需求項目（E09-D001）、ack／nack、`WorkerStatus` 都不綁 lease，只會被它的寫入者刪掉。讀的一方把 ack、`WorkerStatus` 和寫入者的 `AgentRecord` 一起看：`AgentRecord` 不在時，那些值是最後一次已知的狀態。
- **2026-10-08 拿掉 agent 自己的 lease（E1-07 的第一版，PR #51）。** 那個設計讓 AgentRecord、WorkerStatus、ack 綁在一個 TTL 60 秒的 lease 上，agent 消失就由 etcd 刪掉。拿掉的理由：
  - controller 對沉默維持現狀，TTL 不影響它的任何決定，只讓它晚一點看到沉默。
  - 「key 不存在」因此有兩種意思：寫入者刪了，或 lease 到期。STS agent 掛掉超過 TTL，`WorkerStatus` 消失，controller 依 PROTOCOL §3.3 N4 可能誤以為 session worker 已經消失而刪掉需求；shim 底下的 worker 其實還在跑。
  - agent 在不在，選主 session 本來就知道，不需要第二個 lease。
- **也不改成由 API server 輪詢 agent。** 那要讓 agent 開 server、做服務發現與認證，違反本條；量到的是 API 到 agent 的網路，不是 agent 自己能不能工作（TW 的 API 輪詢 JP 的 agent，WAN 一斷就把健康的 agent 當成死了，正是 E00-D005 要避免的）；API server 有多個副本，結果不一致，也沒有地方放（PROTOCOL §3.2 W1）。
- agent 重啟時 list 加 watch，owner 集合立刻完整恢復，不需要寬限期。
- etcd 本身故障不會造成選主 session 大量過期：沒有 leader 時 lease 不會過期，新 leader 會把所有 lease 延長一個 TTL。
- etcd 不在時，controller 和所有 agent 都不能改狀態，但資料面照跑。
- NATS 只負責資料面。

### E01-D007 生命週期由 shim 送給 worker，worker 回報結束方式

- agent 決定時機，經 shim 對 worker 送 `Init`、`Start`、`Stop`。shim 認得這組動詞與 worker 目前的階段，但不解讀內容。agent 不在時（例如主機關機），shim 可以自己送 `Stop`。
- worker 結束前告訴 shim 它要怎麼結束（`Exiting`：正常結束、初始化失敗、主動 fail、crash、卡死），`OnStop` 跑完再送 `Stopped`。exit code 是備援：shim 是 parent，一定拿得到 wait status。
- 沒有送 `Exiting` 又被 signal 結束的，就是被殺。
- **生命週期分兩段（`Init`、`Start`），加上 `Stop`。**

  | 階段 | agent 何時送 | 策略做什麼 |
  |---|---|---|
  | `Init` → `OnInit` | spawn 後立刻送，不等任何需求 | 解析參數、載模型、讀檔。沒有行情，不能下單 |
  | `Start` → `OnStart` | controller 判定所有 MD、TD 需求都 READY，在 STS 的指派上放行之後（E09-D007） | 訂閱行情、需要行情的熱身（tape、歷史資料）。可以下單；`OnStart` 回傳之後才開始交付 hook |
  | `Stop` → `OnStop` | — | 收尾 |

  controller 在 session 一開始就發 STS 的指派，`OnInit` 因此可以和需求的建立並行，慢的準備工作不會拉長啟動時間。mftik 的三段制（多一個 `Ready`：拿行情熱身但平台禁止下單，帳號就緒後才開放）先不做；之後需要時以新的契約版本加入，只對在 `Hello` 宣告支援的 worker 送。

### E08-D001 策略不處理可用性

- 策略只寫交易邏輯，不處理容錯。SDK 沒有 `OnMdUpdate`、`OnTdUpdate`。
- 行情斷了：策略收不到事件，自然不會反應。
- TD 壞了：下單失敗，策略拿到原因（帶原因的拒單）。
- 存活判斷由 controller 依 ack／nack 處理（E09-D001、E09-D007），不經過策略。

### E00-D005 每個 region 一個 etcd cluster

一開始就每個 region 各架一個 cluster，不先用全域的。理由是故障範圍要和實際的依賴對齊：全域 etcd 在 TW、JP 斷線超過 TTL 時，少數方 region 的 agent 連不到 etcd，選主 session 被健康的 leader 判定過期，整個 region 的控制面停擺，連只用本地資源的 session 也起停不了；每個 region 一個，則只有真的跨 region 的依賴受影響，而那些依賴的資料面（NATS）在同一次斷線裡本來就斷了。

1. **key 放在「會依據它行動」的 agent 所在的 region。** session spec、STS 的指派與 status 在 STS 的 region；需求項目與 ack／nack 在 provider（MD／TD）的 region；其他 key 在該 instance 的 region。所有存取經過路由函數 `clusterFor(instance)`。
2. **只有 controller 和 API server 連多個 cluster。** 三種 agent 都只連自己 region 的 cluster。
3. **fencing 不跨 cluster。** agent 與 controller 在每個要寫入的 cluster 各放一個 writer key（記錄 epoch），寫入時只比對那個 cluster 裡的 writer key。
4. **版本號不用 etcd revision。** agent 的 epoch 是持久化的計數器；推給 worker 的指令帶 `(epoch, seq)`。不同 cluster 的 revision 不能比較，cluster 重建時 revision 也會從頭開始，計數器不受影響。
5. **instance 名稱帶 region**（`md-tw-1`），在所有 cluster 之間唯一。
6. **跨 region 的需求**（TW 的 session 要 JP 的行情，controller 寫進 JP 的 cluster）是一般的寫入，不綁 lease。controller 連不到某個 region 時，那裡既有的需求原樣保留、照常服務，只是改不了；恢復連線後 controller 先 relist 再比對。監控跨 region 的 watch 中斷次數。
7. 只部署一個 region 時，就只有一個 cluster，程式碼不變。

### E09-D002 每個 worker 都從 artifact 起，以 OCI image 格式打包

- **三種 worker 都是 artifact，manifest 帶 `kind`：** `strategy`（STS 的 session worker）、`md.conn`（conn worker）、`td.account`（account worker）。agent 只接受自己那一種。agent 依指派找到要起的 worker，加上 manifest 的 entrypoint 與參數，起動進程（PROTOCOL §8）。策略的 digest 在 session spec；MD、TD 在 spawn 時取當時的最新。
- **MD、TD 的 artifact 各只有一種，都在這個 repo 裡實作，** 由 CI 在 release 時建好推上 registry。不按 instance 釘版本：每次 spawn 取當時的最新，跑著的進程保持原 binary，換 agent 不因此換 incarnation（E01-D006、PROTOCOL §8）。E9 的 artifact 管線做好之前，md-agent 與 td-agent 執行這個 repo 建出的 binary。做好之後，主機上的拉取與驗證和 STS 用同一套程式（`internal/platform/artifact`，E00-D006）。
- 以下各條原本只針對策略 artifact，現在三種都適用，只有標明「策略」的除外。
- 每個策略各自編譯、各自發版，與平台的 release 無關。session 釘住策略的 image digest，重新掛起也沿用同一個 digest。
- **只拿 OCI 當打包格式，不跑容器。** STS 把 image 解開成 rootfs，shim 以 rootfs 為工作目錄直接 exec。之後若要隔離，可以換成 Strategon 的 rootless OCI driver，格式不變。
- **分層讓策略之間共用：**

  | 層 | Go 策略 | 其他語言（由該語言的 SDK 決定，E08-D002） | 共用 |
  |---|---|---|---|
  | runtime | — | 解譯器與 SDK，例如 python-build-standalone 加上自行實作的 Python SDK | 同一個 runtime 的策略 |
  | 依賴 | — | 第三方套件，例如依 `uv.lock` 安裝的 site-packages | lockfile 相同者 |
  | 策略本體 | 靜態 binary | 策略的程式碼 | 否 |

- **config blob 放 mftiko 的 manifest**：schema、名稱、runtime、os／arch、entrypoint、env、SDK 版本、支援的契約（格式見 PROTOCOL §8）。
- **build 不需要在目標平台執行任何東西。** Go 的 SDK 版本由 `debug/buildinfo` 從 binary 讀出。其他語言由該語言 SDK 的打包工具產生各層與 manifest，格式照 PROTOCOL §8。
- **每一層可重現**：檔案依路徑排序、mtime 歸零、uid／gid 為 0。輸入相同，digest 就相同。
- **發佈：** 推到 OCI registry；API 的策略目錄記錄 `name → digest`。
- **主機上（三種 agent 一樣）：** 先檢查 `kind`、os／arch、契約與大小，不符就不 spawn（策略是 `INIT_FAILED`）；下載缺少的層並驗 sha256；解開時拒絕絕對路徑、`..`、指出根目錄的 symlink、裝置檔、setuid；GC 刪掉沒有進程在用、也不是指定版本的 rootfs（策略是沒被非 terminal session 釘住、也不是最新版），以及沒有引用的層。
- 依賴都在 artifact 裡，不需要 mftik 的節點層級 extras。
- 結果之一：STS worker 與 shim 會有版本偏差。部署時以 manifest 的 `contracts` 檢查，執行時以 `Hello` 協商（PROTOCOL §1）。

### E08-D002 一期只做 Go SDK；其他語言照契約自行實作

- **不做 Python 綁 Go。** Python 呼叫 Go 的生態還不成熟：沒有像 PyO3 那樣官方支援的方案，只能 `-buildmode=c-shared` 編成 `.so` 再用 ctypes 匯入，還得處理一個進程只能有一個 Go runtime、cgo 的指標規則、GIL 與 Go thread 的互動、manylinux 編譯等問題。成本和風險都高，一期不做。
- **平台只出 Go SDK，它是參考實作。**
- **平台契約與語言無關。** session worker 只要做到 PROTOCOL §9 的每一條，就能用任何語言實作（例如另外做一個純 Python 的 SDK），以策略 artifact（E09-D002）打包，平台不需要為它改任何東西。
- **一致性由 conformance 測試保證。** 平台提供一套黑箱測試：假的 shim（走 fd 3）加上假的 NATS 對端，逐條驗 §9 的契約。Go SDK 也跑同一套。SDK 通過之後，manifest 的 `contracts` 才能宣稱支援。
- **平台另有一道不依賴 SDK 的保險：** 不是每種語言都能保證 heartbeat 不被 hook 卡住（例如 Python 的 hook 卡在不放 GIL 的 C 擴充裡）。STS agent 以 `Beat` 判斷：超過 `hook_timeout_s` 再加 10 秒沒有 `Beat`，就經 shim 送 SIGKILL，outcome 記為 `HUNG`（PROTOCOL §9.7）。

### E06-D001 歷史補正：交易所讀取比照 md.fetch；成交發在 `td.evt`，落庫不在 TD agent（E06-D038）

account worker 依 refcount 起停，沒有常駐層（E09-D001），所以閒置帳號的歷史不為此拉起 worker。交易所的 REST 讀取與商品反查是無狀態服務，比照 md.fetch（E04-D002、E04-D018–E04-D023）。TD agent 不查 SYM。

成交由 account worker 發在 `td.evt.{api_id}`（E06-D038）。目前沒有人落庫。由誰落庫見 PROTOCOL §10，不會是 TD agent。TD agent 沒有 NATS user（E06-D039），不訂 `td.evt`，不呼叫 `td.backfill.{venue}`，也不寫 Postgres。

下列 queue、cursor 與過濾是落庫的規則，不是 TD agent 的工作。誰執行見 §10。

- **Postgres 的 cursor 是成交進度的權威。** live 路徑寫進去的都是暫定（provisional）；哪一段已經確認，只看 cursor。
- **scheduler 不在 TD agent，也不在 E01-D001 的共用骨架裡。** 誰跑見 §10。觸發：
  - 啟動時從自己 region 的 etcd 讀帳號、從 Postgres 讀 cursor，為每個帳號載入 backfill job。
  - 固定 interval 加 jitter（`AccountSettings.backfill_interval_s`，必須短於各交易所成交歷史的保留期）。
  - 帳號 refcount 1→0 之後，隔 SAFETY_LAG 跑一次（取代 mftik 的 detach 觸發）。
  - `td.evt` 的 seq 斷號時提前。
  - API 改 `backfill_requested_at_ns` 時手動觸發。
- **兩條路徑進同一個 queue：**
  - provisional：worker 發在 `td.evt.{api_id}` 的 `Fill`、`Order`。目前沒有人訂閱把它們收進 queue（E06-D038）。
  - confirmed：向 `td.backfill.{venue}` 要來的。服務已經把交易所寫法反查成 ticker。誰呼叫見 §10，不是 TD agent。
  - queue 在記憶體裡；一個每 5 秒的 cron 把它清進 Postgres。目前沒有人跑這個 cron。
- **cursor 的推進也是 queue 的項目**，排在它涵蓋的那一頁後面。drain 依序寫入，同一批在同一個交易裡 commit。執行中斷時資料和 cursor 一起消失，下一輪從原處重讀，確認線不會越過沒寫進去的成交。queue 滿時 backfill 擋住上游，provisional 可以丟。
- **provisional 丟了補得回來，所以不用 JetStream。** 每一輪都重讀 `[確認線, now − SAFETY_LAG]`，漏收的 `td.evt` 一定落在之後某一輪的範圍裡。前提是 refcount 歸零後的那一輪不能省。
- **cursor 的語意沿用 mftik 的 executor：**
  - 還在追趕時，以交易所的 id 續讀。
  - 追上之後，改從確認線重讀，因為交易所的歷史 endpoint 可能比它自己的 stream 晚出現資料。
  - 確認線只推到 `now − SAFETY_LAG`（60 秒）。
  - 降序翻頁的交易所，沒讀完之前不推確認線。
- **只有一條 cursor（成交），order 由成交帶出：**
  - 只落有成交的終態訂單：終態且 `filled_qty > 0`，包括部分成交後撤銷的。沒有成交的撤單、拒單、過期單不落。
  - 成交的 `order_id` 在 orders 表還沒有終態列的，這一輪向 `td.backfill.{venue}` 按 id 查單：終態就落，否則下一輪再查。待查清單由 DB 算出（fills LEFT JOIN orders），不另存。查單是服務的 REST，不是 TD agent。
  - provisional 照同一規則過濾：`td.evt` 的 `Order` 只有終態且有成交的才進 queue。
  - session 歸屬由 `order_id` join orders 得到，不在寫成交時決定。REST 拿回的成交不帶 `client_order_id`；provisional 又丟了的話，歸屬延到該訂單終態落庫之後。
  - 代價：DB 算不出撤單率、order-to-trade ratio，需要時從事件流或 log 算。
- **合併：**
  - 成交以 `(api_id, fill_id)` 去重。`fill_id` 必須是 WS 和 REST 都會給的交易所 trade id，屬於 adapter 的契約（PROTOCOL §7.2）。
  - provisional 寫入 `ON CONFLICT DO NOTHING`；confirmed 寫入 `DO UPDATE`，fee、fee_asset、liquidity 以 REST 為準。confirmed 不會被降回 provisional。
  - 重複寫入沒有影響。目前沒有人寫（E06-D038）。
- **範圍從下單決定，不從成交決定。** 一旦下單，就建立那個商品的 backfill cursor：
  - 平台的每一筆單都經過 account worker 的 `Submit`，所以在任何成交之前，worker 就知道帳號動了哪個商品。`tdctl.Report` 帶 `first_submit_ns`（這個 incarnation 每個商品第一次下單的時間）。
  - 走 Report 是因為它不會丟：shim 保留最新一份，worker 死了也留著，agent attach 時一定拿得到。`td.evt` 在輪換、Postgres 不在、私有連線斷線（E06-D003）時都可能漏掉成交，若剛好是某個商品的第一批，就永遠進不了範圍。
  - 新商品的第一筆單：worker 先把 Report 寫給 shim（一次本機寫入，幾 µs），再送單，不留「單已送出、Report 還沒出去」的窗口。之後的單只查一次記憶體裡的 map，成本可忽略。每個商品每個 incarnation 只發生一次。
  - scope `(api_id, uid, since)` 經由同一個 queue 寫進 Postgres，`since` 取 `LEAST`（重啟、換 incarnation 都不會把起點往後推）。key 是 `Report.first_submit_ns` 的 UID（E06-D010）。誰寫見 §10，不是 TD agent。cursor 的建立完全是非同步的，不影響下單。
  - 起點是第一次下單的時間，一定早於任何成交，所以第一批成交即使 provisional 丟了，也讀得到。
  - 不涵蓋：人工在新商品上的交易。人工單不屬於平台（E06-D002）；之後需要時，可以由 adapter 用交易所的帳戶層級介面發現商品。
  - 資金流水（出入金）之後以同樣的方式加入，另一條 cursor。
- **交易所讀取是無狀態服務，比照 md.fetch。**
  - 每個 region 部署，NATS queue group，任何 replica 都能回答。不歸 TD agent 管，沒有 lease、fencing，不落庫。
  - subject 是 `td.backfill.{venue}`。venue 是人讀的名字，與 `md.v1.fetch.{venue}` 相同。每個 replica 只訂它連得到的 venue，queue group 的名稱是 subject。哪些 venue 由部署設定決定。程式不選 region：本地有 responder 就由本地回答。恰好 subject `td.backfill` 不能訂。
  - 啟動時以 `List` 載入這些 venue 的活躍表，訂閱 `sym.v1.changed.>`，漏收或 NATS 重連時比 revision，落後就重載（E02-D003、E04-D022）。反查只讀這份記憶體。
  - REST 拿回 `(venue, category, exch_ticker)`，在表裡換成 UniversalTicker。category 是 key 的一部分（E02-D002）。不在活躍表的這一筆，以 `include_inactive` 直接問 SYM 的 Reverse，拿終結列原本的 UID，不進快取。兩張表都沒有，這筆這輪跳過，確認線不越過它。熱路徑不走 Reverse。
  - 限速比照 E04-D020：429、418、`ResetAt`，不排隊、不重試。一個帳號失敗不影響其他帳號。
  - 帳號憑證以 TD instance 的公鑰封裝。誰把這一輪解開的憑證放進請求見 §10，不是 TD agent（它沒有 NATS user，E06-D039）。服務用完不留，也沒有這把私鑰。md.fetch 沒有這一步，因為它讀的是公開 REST。
  - 平台服務，不放進 td-agent，不是 worker artifact。自己的 NATS user：publish 恰好 subject `sym`，subscribe `td.backfill.>`、`sym.v1.changed.>`、`_INBOX_backfill.>`。回答時一則、兩分鐘。
- **排程仍是固定 interval 加 jitter。** 交易所那一側的限速在服務上，比照 E04-D020。
- **TD agent 不落庫，也不查 SYM。** 它不讀 venue 的 REST，不連 Postgres，沒有 NATS user（E06-D039）。帳號憑證仍由它解開，經 `Init` 交給 account worker（E06-D013）；那不是歷史補正的請求。
- worker 不依賴落庫。TD agent 不在時 worker 照跑（E01-D002–E01-D004）。補正與落庫目前沒有人做（E06-D038）。

### E04-D002 行情狀態的權威在 conn worker；md.fetch 是無狀態服務

行情狀態（book、tape）的權威在持有該 atom 的 conn worker 記憶體裡，跟 TD 的 ledger 在 account worker 裡是同一個模式：快照打底，fold 串流的增量，有變化就推出去，晚加入的人向持有者要目前的狀態。

- **book：**
  - conn worker 維護一本本地全量 book，只從 `orderbook_delta` 推出。topic 對到交易所的增量 channel；沒有增量 channel 的交易所，用它最完整的 book channel。`orderbook` 已刪除，寫這個名字是 `invalid`，不換成 delta（E04-D053）。
  - 跟交易所同步的邏輯全在 conn worker 裡：快照與增量對齊、驗序號、斷號後重抓。快照用 adapter 的 REST reader 在進程內抓（Binance 類），或用訂閱時 WS 推來的那一張（OKX、Bybit 類）。不經過 md.fetch 服務：正確性不依賴另一個服務，而且對齊有時序要求，多一跳 NATS 只會讓快照變慢。
  - `orderbook_delta`：正規化後的增量原樣轉發（`OrderBookDelta`：價位 → 數量，`0` 為刪除），worker 不預先過濾（E04-D032）。重新同步之後推一則 `reset`，內容是窗內的完整 book（E04-D030）。第一次成功的快照也是一則 `reset`（E04-D025）。ring 丟掉造成 seq 跳號時，SDK 向 `md.view` 重拿（E08-D057、E08-D003）。
  - 環長與 `price_tick` 見 E04-D030、E04-D033。`exchTicker` 見 E04-D026。重抓只在斷號或 checksum 失敗。重抓節奏見 E04-D024。SnapshotREST 抓 `BookSync.Depth()`（E04-D053）。
  - 晚加入（2026-10-17，E08-D057 改寫 SDK 這一句）：SDK 維護本地 book。快照只向 MD 的 `md.view` 要，不向交易所要。拿到 seq S 時的 book 之後，只套用 `(conn_epoch, seq)` 大於 S 且連續的增量。seq 斷號或 `conn_epoch` 改變時重新要一次。未同步不呼叫 `OnBook`。細節見 E08-D057–E08-D061。
- **tape：**
  - 只錄 `trade`、`aggtrade`、`liquidation`，book 類不錄。錄不錄由 conn worker 依 topic 決定。`AtomAssignment.record` 忽略（E04-D047）。一個 feed 一個 ring（E04-D049）。
  - ring 存已經發佈的訊息 bytes（envelope ＋ `FeedEvent`，E08-D003）。`md.view` 去掉 envelope，不重新編碼（E04-D035）。上限見 E04-D038、E04-D049、E04-D058。
  - WS 重連時在 ring 裡記一個 gap（E04-D040）。ring 跨 `conn_epoch` 保留。
  - 接 live：SDK 不自動要 tape，也不扣住 live；live 立即交付。策略自己呼叫 `Tape`，以 `TapePage.head` 去重：`(conn_epoch, seq)` 大於 head 是新的，小於等於是重複（E08-D047，改寫 E04-D037 的 SDK 那一句）。沒有 id 的 liquidation 也適用。
  - 不用 Redis。tape 跟著 worker 進程和 atom 一起活：只有進程重新掛起（crash，或停掉後 spawn 新的 incarnation）、或 atom 的 linger 結束，tape 才會沒了。
  - worker 不隨版本遷移（E01-D003、E01-D006）：發新版只影響之後 spawn 的 worker，跑著的進程繼續用原本的 binary，tape 不受影響。tape 在這條連線的進程換掉時才沒：crash 後重拉，或連線停掉之後再 spawn。
  - tape 只是熱身用，回覆會說明涵蓋多少。要一直熱的，跑一個 tape keeper：不下單、不綁帳號的策略，跟 mftik 一樣，tape 跟著它的生命週期走（E04-D004）。
- **md.view：** 持有該 feed 的 conn worker 服務 `md.v1.view.{inst}.{topic}.{ticker}`（E04-D055、E04-D048）。E04-D044 已由 E04-D055 取代。形狀跟 `md.v1.f` 一樣，對應 `td.view.{api_id}`。回覆依 topic：trade 類回 tape 分頁（由新到舊，以 `(conn_epoch, seq)` 當游標，每頁約 900 KiB，E04-D039），book 類回現在的窗內 book，未同步是 `not_synced`（E04-D041、E04-D042）。沒有 responder，就是沒有 worker 持有這個 feed。
- **md.fetch：**
  - 無狀態的服務，跟 SYM 一樣：每個 region 部署，NATS queue group，任何 replica 都能回答。不需要 agent、lease、fencing。不落庫。subject、分頁、限速、解析與部署見 E04-D018–E04-D023。
  - 只轉發交易所的公開 REST：K 線，以及交易所不推送、只能查詢的值（E04-D001，例如 Binance UM 的 open interest）。
  - K 線接 live 的 kline feed，以 `open_time` 對齊。
  - book 快照不走 md.fetch，走 `md.view`。best quote 這類交易所有推送的值，訂閱 feed。
- **共用的是 adapter 的 REST reader 程式碼，不是服務：** conn worker 在進程內呼叫它，md.fetch 服務拿它轉發。SDK 那一層可以都叫 fetch（`fetch_book`、`fetch_klines`），線上依回答者不同走兩個 subject。

### E09-D003 一期的策略完全宣告式

- 策略要的行情全部寫在 session spec 裡，需求項目由 controller 只從 spec 推導。
- 沒有讓策略在執行中加減訂閱的介面。動態的需求（期權鏈重新置中、滾動合約換月）由 selector 在 MD 內部處理（E09-D001）。
- 一次性的讀取（K 線、只能查詢的值）走 md.fetch（E04-D002），不算訂閱。

### E09-D004 策略狀態：session volume 與資料 artifact

spec 維持不可變，平台不解讀策略狀態，重啟仍是新進程從零開始。不同的是，新進程有兩個地方可以讀：

- **volume**：每個 session 一個可寫目錄，在 STS 主機上（`MFTIKO_VOLUME_DIR`）。跨 incarnation 保留，所以同一個 session 內 crash 後重拉，讀得到 crash 前最後寫入的內容。容量上限是 `Limits.volume_mb`。
- **資料 artifact**：不可變、以 digest 定址的檔案樹，存在 E09-D002 的 OCI registry，只是 mediaType 不同（PROTOCOL §8.4）。新 session 的 spec 以 `inputs` 引用（名稱 → digest），STS 主機在 spawn 前拉下、驗 sha256，解到 `MFTIKO_INPUT_DIR/<名稱>/`。上一個 session 的狀態，以及運維上傳的模型、參數表，都走這條路。
- **為什麼要兩個：** spec 不可變，注入的 inputs 在整個 session 期間固定。session 內重啟如果只讀 inputs，會回到 session 開始時的狀態；最新的狀態只會在 volume 裡。
- **spec 只放引用，不放內容。** etcd 的值預設上限是 1.5 MiB，狀態檔很容易超過。
- **session 進入 terminal 後，STS agent 把 volume 打包成資料 artifact 推到 registry。**
  - digest 記在 `SessionStatus.state_out`，下一個 session 拿它注入。
  - 標上 `clean`，取最後一個 incarnation 的 `ExitInfo.on_stop_completed`，讓接手的人知道這份是不是正常結束時寫的。
  - 推成功之後才刪 volume；registry 不在就保留 volume 並重試。volume 是空的就不推。
- **讀取順序由 SDK 的小工具處理，不是平台語意：** volume 有就用 volume，否則用 inputs，兩者都沒有就從零開始。
- **寫入：** SDK 提供原子寫入（暫存檔加 rename），OnStop 逾時被殺也不會留下寫到一半的檔案。
- **只在 OnStop 寫，crash 時就沒有狀態可用。** crash、HUNG、被 kill 都不跑 OnStop。要讓 crash 後也能 rebuild，策略得在執行中定期寫；寫不寫、多久寫一次，由策略自己決定。
- **讀檔放 `OnInit`**（E01-D007 的分工）。狀態檔是策略自己的認知，帳上的部位與掛單以 TD 為準，對帳在第一次 `OnRecon`（E06-D003）。
- **volume 在主機本地。** session 綁在一個 STS instance 上，主機掛了 volume 就沒了，只能退回 inputs 那份較舊的狀態。一期接受。
- **容量：** STS agent 定期量 volume 的大小，超過 `Limits.volume_mb` 就以 `FAILED` 結束 session（原因 `volume_over_limit`），因為磁碟滿會拖累同一台主機上所有 session。
- 名稱上區分「策略 artifact」（E09-D002，程式）與「資料 artifact」（本條，狀態與輸入）。

### E08-D003 session worker 的執行模型、背壓與重啟

**執行模型**

- **單一 dispatcher（Actor）。** hook 一次一個、依序執行，策略程式碼不需要鎖。下單這類呼叫不阻塞，結果以事件回來。
- 跨 thread 只有一個入口 `Post`：策略自己開的 thread 或 goroutine 用它把結果放回 TD buffer。其他 SDK 呼叫只能在 hook 裡做。
- NATS 收訊、心跳、watchdog 跑在 Go 核心的 goroutine 上（E08-D002），hook 卡住不影響它們。mftik 的雙 thread、offload、breathe 都不需要。

**兩個 buffer**

| buffer | 放什麼 | 規則 |
|---|---|---|
| TD | `td.evt`、下單／撤單的回覆、timer、`Post`、`md.view`／`md.fetch` 的回覆 | FIFO，不丟。有上限，滿了以 `FAILED` 結束：跟不上自己的訂單回報是不安全的。多個 goroutine 寫入，量小，用 mutex 即可。同一個 timer 積了多次只觸發一次 |
| MD | 所有行情 | 一個 FIFO ring，每格存收到的訊息 bytes。滿了丟最舊的，跳過正在讀的那一格 |

- dispatcher 每跑完一個 hook：TD 有東西就先取 TD；TD 空了才取 MD。
- TD、MD 之間沒有順序保證：兩邊來自不同進程、不同 subject，本來就沒有，分開只是不假裝有。

**prehook：系統只做簡單的抽濾**

- 事件交給 hook 之前，prehook 只看 envelope 做過濾，通過的才把 payload 解析成資料交給 hook。
- MD 的規則只有一條：`now − envelope.md_ts_ns > recv_window_ms` 就丟。`recv_window_ms` 是 session 的設定，有預設值。STS agent 把 spec 的 0 換成預設值，以 `InitPayload.recv_window_ms` 交給 worker；worker 拿到的一定是實值，不必知道預設值，收到 0 以 `INIT_FAILED` 結束（E8-02，#226）。`now` 在 dispatcher 從 ring 取出時取，所以 window 包含本地排隊。
- **`orderbook_delta` 不進 prehook（2026-10-16）。** delta 是對 book 的增量，丟掉一則，後面的就接不上，book 得重拿快照；prehook 正是在 MD 塞住時才丟，於是越忙越常重同步。所以把兩件事拆開：
  - **套進 book：** SDK 內部的工作。delta 不論多舊，一律解碼、套用，book 保持連續。
  - **呼叫 `OnBook`：** 交給策略的事件，仍受 `recv_window_ms` 管。太舊的 delta 照樣套用，不呼叫 hook；下一則新鮮的 delta 套用之後才呼叫，那時 `ctx.Book(uid)` 已含前面所有的增量。
  - 代價：過載時 delta 每一則都要解碼與套用，不能在 envelope 就跳過；套用是寫陣列，遠比一次 hook 便宜。ring 滿了丟最舊那一格仍會跳號，照樣重同步；這條只消掉「太舊被丟」這個原因。
  - `QueueStat.dropped` 不計這種「套用了、沒呼叫 hook」的 delta：它沒有被丟。
- envelope 是 `md.f` 訊息開頭、由版本決定長度的欄位（PROTOCOL §7.1、E04-D005–E04-D011），不用 NATS header：header 是文字、nats.go 每則都要解析成 map。欄位用 little-endian 的 `encoding/binary` 讀，不用 unsafe，因為 `msg.Data` 不保證 8-byte 對齊（E04-D007）。
- envelope 的時間是 MD 主機的時鐘，所以 window 包含傳輸延遲與本地排隊。跨 region 的 session 要設得比 WAN 延遲大；主機之間需要時鐘同步。
- 交易所的時間戳在資料裡。策略對舊資料有自己的判斷，就在 hook 裡 early return，系統不管。
- 丟掉之後，策略用 `(conn_epoch, seq)` 判斷連續性自己處理。SDK 提供可選的小工具：已收盤的 kline 被丟了就用 `md.fetch` 補。`orderbook_delta` 由 SDK 維護本地 book（E8-15）：跳號（ring 滿了丟掉的）就向 `md.view` 重拿快照（E04-D002、E04-D041），策略不感知，只在 `OnBook` 讀 `ctx.Book(uid)`。
- 持續過載時，送達的事件延遲會穩定在接近 `recv_window_ms`，這是 FIFO 加過期丟棄的性質。以 `Report` 的 `QueueStat`（`depth`、`dropped`、`max_age_ms`）讓它看得見。

**slot 的壽命**

- 正在讀的那一格被釘住，直到 hook 結束。
- Go SDK 的 buffer 是預先配好的 `[]byte`：不含指標的記憶體，GC 不掃描內容。
- 交給 Go 策略的資料預設是複製出來的。要 zero-copy 的另外提供只在 hook 內有效的 view；debug build 在 slot 釋放時填 `0xFF`，誤用會立刻現形。

**hook 的時間預算**

- 軟上限：超過只計數、記 log，反映在 `Report` 的 `hook_slow`、`hook_blocked_ms`。
- 硬上限 `Limits.hook_timeout_s`：超過判定 `HUNG`，watchdog 結束進程（exit 21），`OnStop` 不跑。值的來源同 `recv_window_ms`：STS agent 解析之後放在 `InitPayload.hook_timeout_s`（E8-02）。
- `OnInit`、`OnStart` 用 `start_timeout_s`（經 `Init`、`Start` 的 `deadline_ms`）。`OnStop` 的時限看怎麼結束（2026-10-16）：
  - 收到 `Stop`，或 SIGTERM：整個收尾（等目前的 hook、交付 TD、`OnStop`、`Stopped`）都在 `Stop.grace_ms` 內，計時在 shim（PROTOCOL §4.3）。`grace_ms` 不是只給 `OnStop` 的。
  - 策略 `Exit()`、`Fail()`、agent 的 `Command.fail`、hook crash、fd 3 讀到 EOF：沒有人替 `OnStop` 計時，而 `Beat` 由獨立的 goroutine 送，§9.7 的 no-beat 保險也不會觸發。所以 `OnStop` 算一個 hook，套 `hook_timeout_s`。超過就以 exit 21 結束：`Exiting` 已經送了，outcome 照原本的；不送 `Stopped`，`on_stop_completed` 是 false，volume 因此不是 `clean`（E09-D004）。

**結束的順序（2026-10-16）**

- 停止交付 MD，丟掉 MD ring；等目前的 hook 跑完。
- 立刻送 `Exiting`，不等後面的交付：交付途中被 SIGTERM、SIGKILL，`ExitInfo` 仍知道原本打算怎麼結束。
- `DONE`（`Stop`、`Exit()`）與 `FAILED`：退訂 `td.evt`，把 TD buffer 裡已經收到的事件交付完，再跑 `OnStop`。`OnStop` 常要把狀態寫進 volume，少了最後幾筆成交就寫錯。交付與 `OnStop` 共用同一個時限，不另切比例；TD buffer 有上限、事件小，通常幾 ms。交付中的 hook 照樣受 `hook_timeout_s` 管：卡住就 exit 21，outcome 仍是 `Exiting` 說的那個，exit code 看得出卡住。
- `CRASHED`：不交付，直接 `OnStop`。hook 剛 panic 或回傳 error，策略的狀態可能已經不對，不再餵它事件。
- 之後送 `Stopped(on_stop_completed)`，以 PROTOCOL §5.3 的 exit code 結束。

**重啟（`RestartPolicy`）**

- `ON_FAILURE` 時重啟：`CRASHED`、`HUNG`、不是 agent 要求的 signal。不重啟：`DONE`、`FAILED`（策略自己宣告的失敗）、`INIT_FAILED`（設定或環境的問題，重來也一樣）。
- 指數退避，1 秒起、上限 60 秒；`window_s` 內重啟超過 `max_restarts` 次就轉成 `FAILED`。
- **平台不清場（E06-D002）。** 新的 incarnation 從零開始；上一個進程留下的掛單，由策略的 SDK 自己清：照 `CancelBatch` 拆成 `BatchCancel`，沒有批次撤單就改走單筆 `Cancel`（E06-D025），或接手。第一次 `OnRecon` 會帶上自己 algo 的掛單與帳號的部位，對帳在那裡做（E06-D003）。
- 重啟期間 MD、TD 的需求項目不刪（E09-D001），account worker 與行情不受影響。

### E08-D004 策略 API 以 UID 為商品身分；ticker 只在 Resolve（P15，2026-10-16）

- **策略交易哪些商品是策略自己的設定。** 它寫在 `params`，平台不解讀（E01-D007）。SDK 不從 feed、selector 或 `AccountDecl` 推策略會下哪些商品：`AccountDecl` 只說用哪個帳號，不列商品；看現貨 feed、下單在 perp 是正常的用法。ticker 因此可能在 spec 的 feed 與策略的設定裡各寫一次，這個重複可以接受。
- **SYM 先於策略。** SYM 查不到的 ticker 就不是平台支援的商品。
- **兩個呼叫：**
  - `Resolve(ticker)`：只在 `OnInit`、`OnStart` 可用，以 `sym` 的 `GetRequest` 查，可以阻塞（E01-D007 讓這兩段做慢的準備工作，時限是 `deadline_ms`）。回傳 `contract/symbol.Info`。查不到是設定錯誤：策略回傳 error，以 `INIT_FAILED` 結束，不重啟（E08-D003）。在其他 hook 呼叫同步回傳 error。
  - `Info(uid)`：任何 hook 都可用，只讀 SDK 的記憶體，不阻塞。介面是 `(symbol.Info, error)`；查不到回 error，框架不重試（E08-D024、E08-D025）。
  - 策略保存 UID，不保存 `Info`：`Info` 是值，`sym.v1.changed` 之後 status、filter 可能變了。要用欄位時（例如下單前對齊 tick）再以 `Info(uid)` 拿目前的。
- **之後一律是 UID。** `Order`、`Cancel`、`BatchCancel` 以 UID 指定商品；行情事件帶 `FeedEvent.uid`（E08-D065），TD 事件帶 UID 與帳號的 `api_id`（E06-D033）。修訂 E8（#10）原本「hook 與 spec 用 UniversalTicker，SDK 在邊界轉換」的設計（E02-D003 變更，#75）：邊界從每個 hook 前移到 `Resolve`。spec 仍寫 UniversalTicker（E02-D003）。
- **SDK 只追蹤看過的商品，不載整張表。** 只追蹤 `Resolve` 過的商品，訂閱它們所屬的 `sym.v1.changed.{venue}.{category}`，收到通知、約每 5 分鐘比對 revision 落後、或 NATS 重連時，以 `GetByUidRequest` 重拿那些列。事件帶來沒見過的 UID 不查、不扣住事件（E08-D023）。下架後以 `include_inactive` 重拿（E08-D026）。修訂 PROTOCOL §9.4 第 8 條原本的「啟動時載入 spec 用到的 `(venue, category)` 活躍表」。
- **`sdk.Order` 跟著改：** `Ticker string` 換成 `UID uint32`；`Side`、`OrderType` 改用 `common.v1` 的 enum（E00-D006），補上 `QuoteQty`、`TIF`、`RecvWindowMs`，與 `td.Submit` 對齊。實作在 E8。
- 不採用：hook 看 ticker、SDK 每次在邊界轉換。行情與 TD 的訊息都已是 UID，轉回字串只為了給策略看，熱路徑多一次查表，還要處理已下架商品的 ticker 反查。不採用：SDK 從 spec 推要載入的 `(venue, category)`，或 `AccountDecl` 加商品清單。

### E08-D005 策略的 hook：依 topic 拆開、先更新再呼叫、`OnOrder` 與 `OnReject` 的分界（E8-04，#228）

- **行情的 hook 依 topic 拆開。** `OnBestQuote`、`OnTrade`、`OnAggTrade`、`OnKline`、`OnTicker`、`OnLiquidation`、`OnFundingRate`、`OnOpenInterest`、`OnGreeks`、`OnMarkPrice`，各是一個可選介面，策略只實作需要的。沒有 hook 的 topic，SDK 在解碼之前就跳過。`OnKline` 收所有 `kline_<interval>`，interval 在 payload 裡。
- **`orderbook_delta` 沒有原始 hook。** SDK 把 delta 套進本地 book，之後呼叫 `OnBook`；策略用 `Book(uid)` 讀（E08-D003 的 prehook 例外）。
- **先更新本地狀態，再呼叫 hook。** book、OMS（自己 algo 的單）、Ledger（帳號的持倉、餘額）由 SDK 維護。hook 被呼叫時，`Context` 讀到的狀態已經包含這一則事件：`OnBook` 時 `Book` 是最新的，`OnOrder` 時 `Order`、`OpenOrders` 已含這次變化，`OnPosition`、`OnBalance` 時 `Position`、`Balance` 已更新，`OnRecon` 時 OMS 與 Ledger 已整份換掉。策略不感知同步、重同步。沒見過的 UID 不補查、事件不因此停住（E08-D023）。
- **`OnOrder` 是單的每一次狀態變化。** 包括 `td.evt` 的 `OrderUpdate`、`Submit` 的本地 1xx 與沒有 responder（101，本地的 `PENDING_NEW` 變成 `REJECTED`）、`td.order` 逾時（變成 `UNKNOWN`，E06-D005）。事件帶 `Origin` 分出這三種。`Submit` 在送出前就在本地建立 `PENDING_NEW`，所以 ack 不改狀態，ack 與 `OrderUpdate` 的先後不必處理。
- **`OnReject` 只給單沒變的請求拒絕：** `Cancel`、`BatchCancel` 的 1xx（例如 107）。下單被拒是 `OnOrder`。
- **呼叫錯誤同步回 error，不是事件：** algo 沒宣告給該帳號、UID 不認得、帳號沒綁定、在 `OnInit`／`OnStart` 以外呼叫 `Resolve`。
- **讀取請求以 callback 回覆。** `md.view` 的 tape、book 與 `md.fetch` 的 klines、open interest：回覆進 TD buffer，在 dispatcher 上呼叫 callback，和 `After`、`Post` 同一個模式；callback 自己對得上請求，不必 correlation id。`RequestRecon` 例外：回覆走 `OnRecon`，因為帳號狀態的重置只有一條路（E06-D003）。
- **TD 事件帶 `APIID`。** `td.*` 的訊息本身沒有，帳號由 subject 決定（E06-D033）。MD 事件帶 UID（payload）、`md_ts_ns`、`conn_epoch`、`seq`（envelope）；交易所的時間在 payload 的 `Meta`。
- 不採用：一個 `OnFeed` 加 oneof。策略要自己 switch，SDK 也不知道哪些 topic 沒人要。不採用：下單被拒走 `OnReject`。單的生命週期會散在兩條路上。

### E08-D006 SIGTERM 由呼叫端處理（E8-03，2026-10-16）

原則與 MD conn worker、TD account worker 相同。

- worker main 以 `signal.NotifyContext(ctx, SIGTERM)` 取得 ctx，傳入 `Serve`。`mftiko-md-conn`、`mftiko-td-account` 已經是這樣。ctx 取消即視為 `Stop`。
- `Serve` 不自行攔訊號。約定寫在 `contract/wire/c4`。
- Go SDK 的 session worker 用同樣的寫法。

### E08-D007 `deadline_ms` 逾時不等 hook（E8-03，2026-10-16）

- `Init.deadline_ms`、`Start.deadline_ms` 逾時：不等 hook 返回，直接送 `Done(verb, false)` → `Exiting(INIT_FAILED)` → `Stopped(false)` → exit 10。
- `deadline_ms` 為 0 表示不限時。

### E08-D008 誰呼叫 Stopper（E8-03，2026-10-16）

- Start 失敗（非逾時）時呼叫 `Stopper`：Init 已經成功，需要收尾。`Stopped(true)`。
- Start 逾時不呼叫：hook 仍卡住。`Stopped(false)`。
- Init 失敗、`HUNG` 不呼叫。

### E08-D009 Stopper 的介面與時限（E8-03，2026-10-16；同日修訂）

- 介面是 `OnStop(ctx context.Context) error`，在 `Exiting` 與 `Stopped` 之間呼叫。沒實作的 plane 行為不變，比照 `Relayer`。回傳 error 時 `Stopped(false)`，outcome 與 exit code 不變，因為 `Exiting` 已經送出。
- 因 `Stop` 或 SIGTERM 結束時，ctx 的期限是 `grace_ms` 的剩餘時間。worker 讀到 `Stop` 的時刻加上 `grace_ms`；`grace_ms` 為 0 就是已經到期。只因呼叫端 ctx 取消而結束、且沒有讀到帶 `grace_ms` 的 `Stop` 時，沒有期限可計，c4 不另造一個。
- worker 自行結束（`Run` 返回）時，ctx 的期限是 plane 在 Init 成功後交給 c4 的 `hook_timeout_s`。c4 不解析 C5 payload。介面是 `HookTimeout`：`HookTimeout() time.Duration`，`Serve` 在 `Init` 成功返回後讀一次。時間由 plane 自己把 payload 換成 `time.Duration`（STS 是 `InitPayload.hook_timeout_s` 的秒數）。回 0，或 plane 不是 `HookTimeout`（MD、TD），就不限時，行為與這一條修訂前相同。
- 逾時照 E08-D003：不等 `OnStop` 返回，exit 21。`Exiting` 已經送了，outcome 照原本的；不送 `Stopped`。
- Start 失敗（非逾時）不是 `Run` 返回，不套這個期限。

### E08-D010 刪除 `sdk/internal/c4`（E8-03，2026-10-16）

- 2026-10-05 骨架留下的空 package。#191 已把實作移到 `contract/wire/c4`。
- `internal/archtest` 的依賴改指 `contract/wire/c4`。

### E08-D011 seq 計數器一個進程一個（E8-06，2026-10-16）

- 一個進程一顆 seq 計數器，所有 algo、所有帳號共用。
- 每毫秒共 1024 筆，seq 0..1023。不是每個 algo 或每個帳號各 1024 筆。

### E08-D012 往前借毫秒不設上限（E8-06，2026-10-16）

- 同一毫秒的 seq 用完時，`ts_ms` 往前借下一毫秒，不設上限。
- `ts_ms` 因此可能超前牆鐘。實務上不會發生：要每秒超過一百萬筆。
- 時鐘倒退不把 `ts_ms` 往回撥，沿用上次的 `ts_ms`（E08-D014）。

### E08-D013 不持久化產生器狀態（E8-06，2026-10-16）

- 不持久化產生器狀態。進程重來，計數器從 0 起算。
- 接受跨 incarnation 撞號的理論風險。撞到時 TD 依既有冪等處理（E06-D018、E06-D019）：內容不同回 106 `INVALID_ORDER`，內容相同回原本的結果。不新增拒絕碼。

### E08-D014 產生器的位置與原子計數（E8-06，2026-10-16）

- 位置 `sdk/internal/cloidgen`。介面 `Next(account, algo, nowMs) (string, error)`。`account` 是 `AccountDecl`。`nowMs` 由呼叫端傳入，產生器不讀時鐘。`algo` 不在該帳號的 `algo_ids` 內就同步回 error，不取號。編碼用 `cloid.Encode`。
- 不用 mutex。`ts_ms` 與 seq 併成一個 uint64：高位是 `ts_ms - cloid.EpochMs`，低 10 bits 是 seq。以 `atomic.CompareAndSwap` 迴圈一次更新，並行不撞號，`ts_ms` 單調。
- 時鐘倒退沿用上次的 `ts_ms`。

### E08-D015 TD buffer 的上限（E8-05，2026-10-16）

- TD buffer 的上限是 SDK 內的常數 **65536** 筆。不進契約，spec 不能設定。
- 內容仍照 E08-D003：`td.evt`、下單與撤單的回覆、timer、`Post`、`md.view` 與 `md.fetch` 的回覆。FIFO，不丟。已經在 buffer 裡的不因為後來放不下而丟掉。
- 滿了以 `FAILED` 結束。
- 不採用：把上限放進 session spec 或 `InitPayload`。跟不上自己的訂單回報是不安全的，這個上限不是策略要調的參數。

### E08-D016 MD ring 的形狀（E8-05，2026-10-16）

- MD ring 是一塊預先配好的 **32 MiB** `[]byte`。每則訊息可變長，緊鄰存放，不是固定格數。
- 放不下一則時，丟最舊的若干則直到放得下。正在被 hook 讀的那一則跳過，不移動、不覆蓋（E08-D003 的 slot 壽命）。
- 單則大於整個 ring：直接丟，計入 `QueueStat.dropped`。
- 不採用：固定格數、每格一份訊息。行情長度差很多，固定格會浪費，也對不上 E08-D003「一塊預先配好的 `[]byte`、GC 不掃描」。

### E08-D017 hook 的軟上限（E8-05，2026-10-16）

- hook 執行超過 **100ms** 計一次 `hook_slow`，並記 log。比較是嚴格大於。100ms 是 SDK 常數，不是 spec 欄位。
- `hook_blocked_ms` 是目前這個 hook 已經執行的時間。沒有 hook 在跑就是 0。
- 硬上限仍是 `hook_timeout_s`（E08-D003），與此無關。

### E08-D018 md 權限精確到每個 feed（E8-11，2026-10-16）

修改 E09-D006。

- publish `md.v1.view.{inst}.{topic}.{ticker}`、subscribe `md.v1.f.{inst}.{topic}.{ticker}`，每個宣告的 feed 一條，不再用 `{inst}.>`。
- `md.v1.fetch.>` 不變。
- td 權限每個 `api_id` 各展開一條：publish `td.order.{api_id}`、`td.view.{api_id}`，subscribe `td.evt.{api_id}`。
- 去重後排序輸出，同一份輸入產生相同清單。
- 沒有帳號不給 `td.*`。沒有 feed 不給 `md.v1.f`／`md.v1.view`。
- 其餘 subject 仍照 E09-D006：publish `sym`；subscribe `_INBOX_{session_id}.>`、`sym.v1.changed.>`。沒有 `log.sts.{session_id}`（E10-D015、E10-D030）。

### E08-D019 特殊字元回 error，不轉義（E8-11，2026-10-16）

- `api_id` 的十進位、MD instance、`session_id`、topic、ticker 含 `.`、`*`、`>` 或空白時回 error，不轉義。空字串不是一個 subject token，同樣回 error。
- `api_id` 小於等於 0 回 error。PROTOCOL §2 的 `api_id` 大於 0；正整數的十進位不含上述字元。
- ticker 依 E04-D055 不轉義。UniversalTicker 文法不允許 `.`：小數點寫成 `D`（PROTOCOL §2，`contract/symbol` 拒絕 `.`）。實作時確認文法不允許 `.`，不需另議。

### E08-D020 `SessionPermissions` 的簽名（E8-11，2026-10-16）

- `SessionPermissions(sessionID string, spec *SessionSpec, feedInst func(feed string) string) (Permissions, error)`。
- feed 對應的 MD instance 由呼叫端經 `feedInst` 傳入。函數不讀 `FeedDecl.md_instance`，不連 etcd、NATS。
- 回傳 `Permissions{Pub, Sub []string}` 與 error。`Pub`、`Sub` 是 allow，已去重並排序。

### E08-D021 只列 allow（E8-11，2026-10-16）

- 權限只列 allow，不加 deny。`Permissions` 沒有 deny 清單。

### E08-D022 一期 spec 含 selectors 回 error（E8-11，2026-10-16）

- spec 的 `selectors` 非空時回 error，不展開。
- 未來方向（不在本票）：selector 解成一個 digest，選到的行情都進該 digest 的單一 subject，每個 selector 只需一條權限。

### E08-D027 session JWT 維持精確權限，`max_control_line` 調到 256 KB（E8-11，2026-10-16）

#253 實測：每個 feed 精確列一條（E08-D018）時，24 個 `trade` feed 或 15 個期權 `orderbook_delta` feed 就超過 nats-server 預設 `max_control_line` 4096。session id 是 6 個小寫 hex，一個帳號 `api_id` 7，MD instance 是 `md-tw-1`。連得上的最後一檔 JWT 是 3706 與 3710 bytes，被拒的是 3830 與 3914 bytes。500 條的 JWT 是 62854 與 102854 bytes，當時只簽、沒有連。調高之後，這兩份在嵌入式 server 上連得上。

- **採方案 a。** 維持 JWT，每個 feed 的精確權限不改。不改 SDK。auth callout 不在本票，留作日後動態授權的選項。
- **natsdev 產生的 server 設定寫 `max_control_line: 262144`（256 KB）。** dev compose 與 CI 的嵌入式 server 都吃這份設定（E04-D012）。寫進 PROTOCOL §7 與部署說明。
- **大小檢查在簽發處，不在 `SessionPermissions`。** `internal/sts/issuer.SignSession` 簽完 user JWT 之後量 CONNECT 的 JSON 引數。`SessionPermissions` 只列 subject，不簽 JWT，量不到 server 會拒絕的那一行。server 比較的是這個引數的長度（動詞 `CONNECT` 與 CRLF 不算），大於 `max_control_line` 就斷線。簽發處用同一個比較：長度大於 262144 就拒絕這份 spec，不把 JWT 交出去，錯誤是 `sts: too many feeds`。等於 262144 可以連。
- **量的是 session 連線實際會送的那一行，不是另設一個餘裕。** nats.go 的 CONNECT JSON：user JWT、nonce 的 ed25519 簽章（長度固定）、client name 留空（`natsx` 的 `Name` 空著時）、headers 與 `no_responders` 開（這份 server 設定沒關 header）。inbox 前綴不在這行裡。之後若 session 連線設了 client name，這一行會變長，簽發處要跟著那份連線改；這一版不為還沒有的 name 預留位元組。
- **理由：** 一般策略不會訂這麼多行情。

### E08-D023 事件不為沒見過的 UID 停住（E8-07，2026-10-16）

- 事件一律照常立即交付。SDK 不在背景為沒見過的 UID 查 SYM，也不扣住事件。
- 只追蹤策略 `Resolve` 過的商品。
- 取代 E08-D004「沒見過的 UID 先查再交付」。一期沒有 selector（E08-D022）。
- 不採用：收到事件才在背景 `GetByUid`，查完再交給 dispatcher。

### E08-D024 `Info` 查不到回 error（E8-07，2026-10-16）

- `Info` 查不到即回 error。
- 是否因此提早結束由策略自己實作。框架不處理、不重試。

### E08-D025 `Info` 的介面（E8-07，2026-10-16）

- `Info(uid) (symbol.Info, error)`。

### E08-D026 已解析的商品下架（E8-07，2026-10-16）

- 已解析的商品下架，`changed` 之後搬到終結表時，以 `GetByUidRequest` 加 `include_inactive` 重拿。
- 保留最後一份並標為不活躍。`Info` 仍查得到。

### E08-D040 OMS 分工（E8-08，2026-10-17）

- #232 先做 OMS 中「自己 algo 的單」，足夠支撐 `Submit`（本地 `pending_new`）、`Cancel`、`BatchCancel` 及 `Order`、`OpenOrders`。
- Ledger（持倉、餘額）與完整狀態機留給 #238。
- Go 實作在 `sdk/internal/tdpath`。

### E08-D041 第一份 Recon 扣住事件（E8-08，2026-10-17）

- 照 E06-D033、PROTOCOL §9.6 保留（正確性需要，與 E08-D023 不同）。
- 扣住期間沿用 TD buffer 上限 65536，滿了以 FAILED 結束（E08-D015）。

### E08-D042 Recon 請求逾時或無 responder（E8-08，2026-10-17）

- 指數退避重問（1–30 秒），事件繼續扣住。

### E08-D043 `Submit` 逾時（E8-08，2026-10-17）

- 時限用 `InitPayload.recv_window_ms`。
- 逾時標 UNKNOWN，不查不重送（E06-D005）。

### E08-D044 新 incarnation 或 `seq` 斷號（E8-08，2026-10-17）

- SDK 自動重走一次 Recon 流程（扣住事件，回覆交給 `OnRecon`）。
- 第一份 `ReconReply` 放行時，扣住的事件若 incarnation 比回覆舊，可以丟掉；比回覆新，留在佇列裡再送一次 `ReconRequest`，與接上之後相同。重複的詢問由 E08-D042 的退避擋住。

### E08-D045 撤單逾時（E8-08，2026-10-17）

- 照 E06-D010，不標 UNKNOWN、狀態不變，SDK 不另外通知策略，等 `td.evt` 結果。

### E08-D046 binding 的 diff（E8-09，2026-10-17）

- 一期的 selector 回 error，不解析成員（E08-D018、E08-D022）。
- 只套用 `Version` 比手上嚴格更大的快照；先比 epoch，再比 seq。手上還沒有快照時，第一份就套用，包含 version 0。
- 集合是這份快照列出的 feed subject。新集合多出來的訂閱，少掉的退訂。subject 用 binding 給的，不自己組（E01-D006）。
- 測試要驗證 feed 的增刪與手上的集合完全一致。

### E08-D047 tape 不自動接 live（E8-09，2026-10-17；改寫 E04-D037 的 SDK 那一句）

- SDK 不自動向 `md.view` 要 tape，也不扣住 live。live 立即交付，仍走 prehook。
- 策略要歷史時自己呼叫 `Tape`。`TapePage.head` 仍是 E04-D037 定義的 high water。策略把 `(conn_epoch, seq)` 小於等於 `head` 的 live 當成重複。
- `head` 的定義、寫進 ring 與發佈的順序、ring 空時的 `(conn_epoch, seq 0)` 都不改。

### E08-D048 `md.view` 與 `md.fetch` 的逾時（E8-09，2026-10-17）

- 等待用 `InitPayload.recv_window_ms`，與 E08-D043 相同。
- 逾時或沒有 responder：callback 收到 error。
- 回覆走 TD buffer。

### E08-D049 壞的 envelope（E8-09，2026-10-17）

- 長度不對、version 不對、或保留 byte 不是 0：丟掉這則，計入 `QueueStat.dropped`，寫 log，不結束 session。

### E08-D050 Init 連不上 NATS（E8-10，2026-10-17）

- `Init` 期間連不上 NATS，在 `deadline_ms` 內以退避重試，到期以 `INIT_FAILED` 結束。

### E08-D051 volume 的讀取順序（E8-10，2026-10-17）

- 讀取順序只是策略自己呼叫的函式：volume 有該檔就用 volume，否則用 inputs，兩者都沒有就從零開始（E09-D004）。
- SDK 不在啟動時自動還原。

### E08-D052 volume 的原子寫入（E8-10，2026-10-17）

- 寫入是同一目錄的暫存檔，寫完 fsync，rename 蓋上目標，再 fsync 目錄。
- 中途被殺時，目標檔要不在、要不仍是上一份完整內容。

### E08-D053 餘額三格（E8-14，2026-10-17）

- SDK 不自行計算。`BalanceUpdate`、`Recon` 原樣覆蓋。
- `td.v1.Balance` 的三格互不重疊，加總是總額。`free` 可用；`prelock` 是單已送出、交易所未確認的預扣，確認後轉入 `locked`；`locked` 是掛單凍結。三格都由 TD 填。
- 寫在 `td.proto` 的註解與 PROTOCOL。

### E08-D054 Recon 與本地 `pending_new`（E8-14，2026-10-17）

- Recon 整份替換 OMS。
- 快照中沒有、且在送出該次 `ReconRequest` 之後才 `Submit` 的 `pending_new` 保留。
- 該次是這份回覆所回答的 `ReconRequest`。`td.evt` 推來的 `Recon` 不是那次回覆，快照沒有的單不保留。

### E08-D055 `Fill` 不動 Ledger（E8-14，2026-10-17）

- 持倉與餘額只由 `PositionUpdate`、`BalanceUpdate`、`Recon` 更新。
- SDK 不從 `Fill` 推算。

### E08-D056 讀取回傳複製（E8-14，2026-10-17）

- `Order`、`OpenOrders`、`Position`、`Balance` 每次回傳 protobuf 複製。
- 讀取不阻塞。

### E08-D057 自動快照與重同步（E8-15，2026-10-17）

- SDK 自己向 MD 的 `md.view` 要快照，斷號時重同步。本地 book 由 SDK 維護（E08-D005）。
- 快照是 MD 的本地本。SDK 不向交易所要。
- `not_synced` 沿用 E04-D041 的退避，曲線與 E04-D024 相同：1 秒起、加倍、上限 30 秒。成功把窗口歸零。已經在等、或快照已在途，不另起一次（E04-D034）。逾時、沒有 responder、以及其他讀取失敗走同一條退避；book 維持未同步。
- 斷號含 ring 丟掉的，以及 `conn_epoch` 改變。未同步不呼叫 `OnBook`。

### E08-D058 等快照期間的 delta（E8-15，2026-10-17）

- 留在內部緩衝。快照之後只套 `(conn_epoch, seq)` 較大的，字典序。緩衝裡有洞就留下，再要一張快照。
- 上限沿用 E04-D024：10000 筆，或從這一次嘗試的第一筆緩衝更新起算 10 秒。任一個到了（含等於）就丟掉緩衝、進行中的快照作廢、再要一張。
- 這些 delta 不是 hook。

### E08-D059 檔數與 tick size（E8-15，2026-10-17；同日修訂）

- 檔數用 `book.DefaultN`，與 MD conn worker 相同（E04-D030）。不用快照的窗口大小。`BookSnapshot` 不帶檔數。
- tick size 仍取自 SYM `Info` 的 `price_tick`。缺、或小於等於 0，只記日誌、不維護（E04-D033）。
- UID 還沒 `Resolve`：維持未同步、記日誌、不自己問 SYM（E08-D023）。

### E08-D060 未同步的讀取（E8-15，2026-10-17）

- `Book(uid)` 回 `(nil, false)`。

### E08-D061 `reset`（E8-15，2026-10-17）

- `reset` 清掉本地本、標成未同步、重要 `md.view` 的快照（E04-D024 的重抓）。不套這則訊息裡的檔位。
- 裝上的快照，游標不得早於這次 `reset`。更早的快照不算，退避繼續。

### E08-D062 範例內容（E8-13，2026-10-17）

- `tape-keeper` 不改：不下單、不看帳，只把訂閱留著（E04-D004）。
- `hello` 是參考範例。`OnInit` 依 params 的 ticker 呼叫 `Resolve`。`OnBook` 讀 `Book`。`OnRecon` 印出活單。掛一張限價單，價格用 params 的 offset 離市價，不會成交。`OnOrder` 確認狀態是 `ORDER_STATUS_ACCEPTED`（上簿）之後才 `Cancel`，再用 `OnOrder` 看撤單。
- params 的欄位名稱是這個範例自己的，平台不解讀。

### E08-D063 conformance 檢查項與後續決定衝突（E8-13，2026-10-17）

- 檢查項若和後來的決定衝突，改檢查，不改 SDK 去滿足舊檢查。
- 例如 E08-D047：tape 不自動接 live、不扣住 live。E08-D044、E08-D054：Recon 的水位套上之後才交付，交付在 `OnRecon` 之後。E06-D033：不是自己 algo 的 cloid 不進 `OnOrder`。E08-D041：Recon 回覆前的帳號事件先進 hold，滿了以 `td buffer full` 結束。
- 不新增檢查 id（E08-D039）。通過條件維持 PROTOCOL §9.8 的表。

### E08-D064 CI（E8-13，2026-10-17）

- 補上 E08-D034 留到 E8-13 的那一段。每個 PR 用 `examples/strategies/conform` 跑完整份 Go SDK conformance，必須全綠。
- fakeworker 的違規檢查留在 `go test`，不放進這次全套跑。

### E08-D028 `mftiko-conformance` 的形狀（E8-12，2026-10-16）

- 執行檔是 `cmd/mftiko-conformance`。
- 輸入是 worker 的執行命令。工具拉起子進程，自己當假 shim，控制面在子進程的 fd 3。
- 不在這一票組 `sdk.Run`。

### E08-D029 標準測試策略（E8-12，2026-10-16）

- 測什麼寫在 PROTOCOL，行為由設定控制（hook 睡多久、下幾筆單）。
- 每種語言各自實作這份策略。這個 repo 先出 Go 版，放在 `examples/strategies/conform`。

### E08-D030 行程內的 nats-server（E8-12，2026-10-16）

- 用 natsdev 在行程內嵌 Go 的 nats-server。不依賴外部 binary，也不依賴 compose。
- server 設定的 `max_control_line` 是 262144（E08-D027）。

### E08-D031 報告（E8-12，2026-10-16）

- 每一項是 pass、fail 或 skip。
- stdout 出 JSON，stderr 出同內容的表格。
- 任一項 fail，行程就非零結束。

### E08-D032 報告不進部署（E8-12，2026-10-16）

- 這一期只產生報告。
- 「宣稱支援某契約，必須先通過對應的檢查」是發布時的流程規範。
- STS agent 在 spawn 時不讀這份報告。

### E08-D033 一次一種違規（E8-12，2026-10-16）

- fakeworker 用旗標把違規一個一個打開（不送 Beat、結束順序錯、cloid 不合版面，等等）。
- 每一種都必須被報告成對應的那一項失敗，不能混成別項，也不能被當成通過。

### E08-D034 這一票測 harness，不測整套 Go SDK（E8-12，2026-10-16）

- **採方案 C。** 這一票是 harness，加上會違規的 worker。每一種違規都要被報成對應的失敗。
- 每個 PR 的 CI 跑 fakeworker 的違規檢查。
- Go SDK 整套通過 conformance，留到 E8-13（`sdk.Run` 組好之後，#237）。
- 不在這一票實作 E8-08、E8-09、E8-10、E8-14、E8-15，也不要求 `sdk.Run` 通過這套測試。

### E08-D035 設定放在 `InitPayload.params`（E8-12，2026-10-16）

- 標準策略的設定是 `InitPayload.params` 的 JSON。
- 欄位清單寫進 PROTOCOL v1.03（§9.8）。
- 由 conformance 工具產生，不由策略作者手寫。

### E08-D036 這一期只收執行命令（E8-12，2026-10-16）

- 工具這一期只接受 worker 的執行命令。
- OCI artifact 的解開留介面，這一期不實作。`internal/platform/artifact` 目前只有 manifest。

### E08-D037 `Init` 之前與 `Start` 之前的結束（E8-12，2026-10-16）

- `Init` 被接受之前，收到 `Init` 以外的訊息，或第二則 `Init`：以 `INIT_FAILED` 結束。不呼叫策略 hook。
- `Init` 已經成功、`Start` 還沒成功時收到 `Stop`：走正常的結束順序（`DONE`）。不呼叫 `OnStart`，也不呼叫 `OnStop`。
- 寫進 PROTOCOL §5.2 與 §9.2。

### E08-D038 檢查 id 與報告是跨語言契約（E8-12，2026-10-16）

- 檢查的 id，以及 JSON 報告的欄位，寫進 PROTOCOL，各種語言的 SDK 與工具都用同一份。
- `skip` 只在該項相依的契約沒有出現在 `Hello.contracts` 時使用。沒宣告就 skip，不算 fail。

### E08-D039 違規清單與 fakeworker（E8-12，2026-10-16）

- 違規清單就是這一票原本的檢查清單，不增不減：流程與 exit code、Beat、`deadline_ms`、buffer 優先順序、prehook 丟棄（含慢 hook 把 ring 塞滿）、TD 溢出、結束順序與 `OnStop` 時限、cloid 版面、subject 與權限、Recon 水位、hook 被呼叫時 context 已經含這則事件。
- 擴充 `internal/platform/shim/fakeworker`。每一種違規一個命令列旗標，一次開一種。

### E06-D002 策略的單由策略自己管：algo_id

- **平台不替策略撤單。** crash、`FAILED`、STS agent 不在時留下的掛單，平台都不自動處理：要嘛由宣告同一個 algo_id 的 session 下一次啟動時自己清，要嘛人工撤。帳號層級的 cancel-on-disconnect（`AccountSettings`）是另一回事，照帳號的設定。
- **algo_id 是訂單的命名空間，由設定宣告，不由 API 分配。**
  - 一個 session 可以宣告多個，例如同一個帳號上 BTCUSDT、ETHUSDT、BNBUSDT 各一個。宣告在 spec 的帳號綁定上（`AccountDecl.algo_ids`），策略下單時自己選用哪一個。
  - 1–1023（10 bits），0 保留，讓人工單或格式不對的 id 不會被當成某個 algo。只需要在同一個帳號內唯一：`client_order_id` 與 `batch_cancel` 都以帳號為範圍。
  - 跨 session、跨 incarnation 不變：新 session 宣告同樣的 algo_id，就能接手上一個 session 留下的單。
- **同一個帳號上，一個 algo_id 同時只屬於一個非 terminal 的 session**，否則兩邊會撤到對方的單。由 STS agent 檢查它自己 instance 上的 session；跨 instance 的重疊由使用者保證，TD 不檢查（E06-D016 修改）。
- **編進 `client_order_id`：** uint64 `ver(4) | algo(10) | ts_ms(40) | seq(10)`，以十進位字串傳。沿用 mftik 的 uint64 形式，欄位重新分配：
  - `ts_ms`：自 2026-01-01 UTC 起算的毫秒，40 bits 可用約 34 年（到 2060 年）。
  - `seq`：一個進程一顆計數器，所有 algo、所有帳號共用，每毫秒 1024 個（E08-D011）。超過時 `ts_ms` 往前借一毫秒，不設上限，可能超前牆鐘；實務上不會發生，要每秒超過一百萬筆（E08-D012）。
  - `ver` 只用 1–7，最高位維持 0，用 int64 解析也不會壞。十進位最多 19 位，各家交易所的長度限制都放得下。
  - 產生器狀態不持久化，新進程的 seq 從 0 起算。跨 incarnation 撞號是接受的理論風險，由 TD 的冪等處理（E08-D013）；不靠重啟退避保證 `ts_ms` 不重疊。
  - `Submit` 本身仍帶 `session_id`。
- **從 `(api_id, algo, ts_ms)` 對到 session。** 同一個帳號上一個 algo 同時只有一個 session，所以靠持有紀錄就能對：TD agent 在 ack READY 時寫入取得時間、需求項目消失時寫入釋放時間，存成 Postgres 的 `(api_id, algo_id, session_id, acquired_at, released_at)`。需求項目在 session 結束時就從 etcd 刪了，不能當歷史用。
  - 交接時，舊 session 的進程結束、需求被刪、TD agent ack 新 session、新 session spawn 到 Start，這段實際間隔至少幾百 ms，遠大於主機之間 ms 級的時鐘誤差，所以不需要冷卻期。
  - 落庫的歸屬（E06-D001）就用這條規則。
- **只能動自己的單（E06-D033 修改）。** SDK 只允許使用 spec 裡宣告給該帳號的 algo_id，`client_order_id` 由 SDK 編碼；其他語言的 SDK 由 conformance 驗 cloid 版面。TD 不解碼 `client_order_id`，也不檢查 algo：原本的 104 只驗 `algo_id` 欄位與 cloid 的 algo 欄位一致，兩個值都由同一個 SDK 產生，擋不到誤用別人的 algo。`BatchCancel` 也不檢查清單裡的單屬於哪個 algo（E06-D025）。解不開成平台 cloid 的 `client_order_id`（交易所自己編的、人工單）不屬於任何 algo：SDK 篩 `Recon` 時不會挑到它，OMS 仍以 `order_id` 追蹤沒有 client id 的單。這是防呆，不是防惡意。
- **SDK 只提供能力，怎麼用是策略的事：**
  - `ctx.batch_cancel(api_id, algo_id, …)`：SDK 從自己的活單挑出這個 algo，依 `td.view` 回覆的 `cancel_batch` 拆成多筆 `BatchCancel`；值是 1 就改走單筆 `Cancel`（E06-D031）。TD 不挑單、不拆批（E06-D025）。SDK 這一段是後續，不在 E6-09。
  - 以 algo 查自己的掛單：`td.view` 的 `ReconRequest` 拿整個帳號的活單，SDK 以 cloid 的 algo 欄位篩（E06-D033）。
  - 策略重啟或新 session 接手時，在 `OnStart` 用它們清場或接手。
- STS agent 不再需要 `td.order` 的權限。session 進入 terminal 時，controller 依 E09-D001 的順序刪除 TD 需求項目。

### E04-D003 MD 連線的身分、endpoint 與放置

- **venue 以「一條連線、一組商品」為單位**，照 mftik 的 venue registry（E02-D001）。host 或產品線不同就是不同 venue，例如 `Binance`（現貨）、`BinanceUM`、`BinanceCM`、`BinanceEO`。憑證屬於帳號種類，不屬於 venue；`BinancePM` 是帳號種類，不是 venue（E03-D001）。
- **endpoint 是連線的可選參數，不是身分。** 同一家交易所提供的替代 host（例如針對某個雲端優化的 endpoint）視為同一個 venue，由 MD instance 的設定選擇，進 conn worker 的 spawn spec。換 endpoint 要換 incarnation（E01-D006）。
- **route：同一個 venue 裡，商品和 topic 決定要連哪條 WS 路徑。** 例如 OKX 的 K 線在 `/ws/v5/business`，其他公開行情在 `/ws/v5/public`；Bybit 的公開行情依類別分成 `/v5/public/spot`、`linear`、`inverse`、`option`。不同 route 不能共用連線。OKX、Bybit 是一份憑證的統一帳戶，拆成不同 venue 會破壞 UniversalTicker 與 TD 的帳號，所以由 adapter 從商品推出 route（E02-D002）。conn worker 用活躍表上的商品。MD agent 只為了選 worker，用 ticker 問 route，不訂那個 channel。大多數 venue 只有一個 route。
- **連線的身分是 `(venue, route, n)`。** 之後加版號（E01-D006）就是 `(version, venue, route, n)`。同一組 atom 一律共用連線，兩個 session 要同一個 atom 就是 refcount=2，不會多開連線。
- **只有連線達到交易所的上限才開 n+1。** 上限由 adapter 宣告，例如 Binance 一條連線 1024 個 stream。新 atom 放進同一個 `(venue, route)` 下還有空間、n 最小的那條。共用同一個 channel 的 feed 是同一個 atom，只佔一份額度，放在同一條連線。agent 用 ticker 問出的 channel 只做這次放置，不拿來訂閱；conn worker 再用活躍表推出真正要訂的 channel。不整併：1024 連期權鏈也用不完，實際上幾乎只會有 n=0。

### E06-D003 行情過舊與 TD 降級

- **行情過舊是策略的風險，平台不兜底。** timer 驅動的策略、用多個 feed 算價差的策略，下單前自己驗事件裡的 `md_ts_ns`、`exch_ts_ns`。系統不在下單時擋：SDK 不知道一筆單依賴哪個 feed。掛著的單要不要撤，也是策略的事（E06-D002）。
- **TD 私有 websocket 斷線（`DEGRADED`）時：**
  - 新單以 1xx 拒絕（`account_degraded`）。收不到回報時送出去的單，策略只會拿到 accepted、看不到後續，很容易重複送單。這也就是 E08-D001 的「TD 壞了 → 下單失敗，拿到原因」。
  - `Cancel`、`BatchCancel` 照常走 HTTP：降低風險的動作不擋。
  - account worker 自己重連。重連後 recon，在 `td.evt` 發一則 `Recon`，之後才恢復接受新單。
  - TD 的 ack 維持 `READY`：worker 還活著，只是暫時降級，改成 `DEAD` 會讓 controller 結束 session（E09-D007），太重。
- **`OnRecon(reconState)`：帳號狀態重置只有這一條路徑。**
  - 內容：自己 algo 的 open orders，帳號的 positions、balances。
  - 觸發：私有連線重連並 recon 之後；account worker 換 incarnation 之後（取代原本的 `AccountReset`）；`OnStart` 之後 SDK 以 `td.view` 的 `ReconRequest` 取得的第一份（E06-D033）。E09-D004 說的對帳、E06-D002 說的接手或清場，都在這裡做。
  - 和其他 TD 事件共用 seq，順序由 TD buffer 的 FIFO 保證。策略在 `OnRecon` 裡整份換掉自己的狀態，之後的 `Order`、`Fill` 照常疊上去。
  - 快照是整個帳號的。SDK 把 `open_orders` 篩成自己宣告的 algo_id；`positions`、`balances` 是帳號層級、所有 algo 共用的，原樣給。
  - 斷線期間的個別成交不會補發給策略，策略拿到的是重置後的狀態。DB 的完整紀錄由 E06-D001 的 backfill 補。
  - 這不是 E08-D001 說的可用性 hook：策略不會被告知「TD 斷了」，只會拿到拒單，之後拿到一份新的狀態。

### E06-D004 連線與重建是 TD 的事，STS 是最後一道防線

- **TD 自己處理連線與重建。** account worker 的私有連線斷了就自己重連（E06-D003）。worker 被重新拉起（換 incarnation，E01-D006）之後，第一件事是 recon：重建倉位、餘額與活訂單，建完就在 `td.evt` 發一則 `Recon(INCARNATION)`。STS 以最快的方式拿到新的帳號狀態，不必自己去要。
- **SDK 沒有任何讓 TD 重建或重連的請求。** `td.order` 只有下單與撤單，`td.view` 只讀本地快照（E06-D006）。
- **TD 自己也處理不了時，STS 自保。** `DEGRADED`、沒有 responder，策略拿到的是 1xx 拒單，例如不再送單；平台不替策略做別的事（E06-D002、E08-D001）。
- incarnation 只在 worker 的實體進程換掉時加一：crash 後重拉、linger 停掉之後又被需要、刻意換版、主機重開機。換 td-agent 的行程（收養，換的是 epoch）與私有連線重連（`DEGRADED` → `Recon(RECONNECT)`）都不換 incarnation。

### E06-D005 結果未知：STS 交出 UNKNOWN，TD 主動查單

結果未知有兩層，是兩件獨立的事，不混在一起。

- **STS：`td.order` 逾時是結果未知，不是 1xx。** 1xx 的意思是「什麼都沒送出」（PROTOCOL §7.2）。逾時的單可能已經到了交易所，當成被拒再送一次，部位就加倍。
  - 沒有 responder（account worker 不在）與 TD 的本地拒絕才是 1xx。
  - 逾時：SDK 把這張單以 `ORDER_STATUS_UNKNOWN` 交給策略。不查 `td.view`、不重送、不推論。
  - 之後這個 `client_order_id` 的 `OrderUpdate` 來了，SDK 照常更新。一直沒有下文的單怎麼處理是策略的事，例如以 `BatchCancel` 清掉，或等 `OnRecon`。
- **TD：交易所沒有回答的單，由 worker 主動查交易所。** 單已經有 `order_id` 時以它查，還沒有才用 `client_order_id`（E06-D017）。
  - 下單或撤單送出之後沒有讀得到的回答（`ErrUnknownOutcome`，E03-D003），OMS 把這張單記成 `UNKNOWN`，在 `td.evt` 發出。撤單沒有回答時不記成 `UNKNOWN`，見 E06-D010。
  - 等到「送出時間 ＋ 請求有效期 ＋ 時鐘誤差」之後查單（已有 `order_id` 用它，否則用 cloid；`Account.QueryOrder`，REST，E6-16）。有效期是 Binance、Bybit 的 `recvWindow`、OKX 的 `expTime`，下單與撤單一律帶上。過了這條界線，請求就算才到交易所也會被拒，「查無此單」才等於「沒下成」。預設有效期 5 秒、時鐘誤差 1 秒，是部署設定。
  - 查到：照交易所的狀態更新 OMS，發 `OrderUpdate`。查無此單：`REJECTED`，reason `not_placed`。查詢本身失敗：退避重試，直到有結果。不重送原單。
  - 只靠私有事件與 recon 不夠：單沒到交易所時兩者都不會出現；單在私有連線斷線期間就終結時，事件漏了，recon 只讀 open orders 也看不到。
  - 打交易所查單的只有 worker。
- **以 `client_order_id` 冪等。** 同一個 incarnation 裡，account worker 收到還活著的單的 cloid，回原本的結果，不再送交易所。已經終結的單不在 OMS 裡，不保證認得（E06-D015 修改）。
- 不採用：SDK 逾時之後查 `td.view` 並以同一個 cloid 重送。worker 換了 incarnation 之後，新的 worker 不認得上一個 incarnation 收過的 cloid，重送可能讓已經成交的單再下一次。

### E06-D006 `td.view` 只讀本地快照

- **任何查詢都只讀 account worker 的記憶體，不打交易所，不消耗交易所的 REST 額度。** `Recon`（活單、持倉、餘額，E06-D033）與槓桿都是。帳本與交易所的差異只由 recon 與 `Recon` 處理（E06-D003、E06-D004）。不提供「向交易所重新查詢」的選項。
- **槓桿讀本地快取。** 原本的 `EnsureLeverage` 對應 `Account.Leverage`，是一次 REST 呼叫，違反這條。改名 `Leverage`：快取取自持倉的 leverage（E06-D015）；快取裡沒有的商品回未知，不去查。設定槓桿是寫入，一期不做。
- **快照還沒好也照樣回，不擋、不等。** 還沒完成第一次 recon，或正在 `DEGRADED` 時，回當下的快照，附上 `AccountState` 與 `reconciled_at_ns`，由呼叫端判斷新舊。
- **回覆帶 incarnation 與當下的 `td.evt` seq。** 快照加上 seq 之後的事件，能接成同一份狀態。STS 自己這一端漏收 `td.evt`（NATS 斷線、被當成 slow consumer）時，TD 不會知道；SDK 怎麼用它接回屬於 E8。
- **帳本可以依商品篩選。** 已由 E06-D033 取代：OMS 與帳本合成一個 `ReconRequest`，回整個帳號，不篩商品、不篩 algo。
- **OMS 只回活著的單（E06-D015 修改，E06-D032、E06-D033）。** 終結的單從 `td.evt` 與落庫的歷史拿（E06-D001），熱路徑上不查資料庫。`Recon.open_orders` 只有活單。

### E06-D007 一期只支援單向持倉

- **雙向持倉（hedge mode）一期不支援。** 雙向持倉下 `Submit` 的 side 分不出開多或平空，交易所要求帶持倉方向（Binance 的 `positionSide`、OKX 的 `posSide`、Bybit 的 `positionIdx`）。E3 的 adapter 把持倉方向折進帶號的 `Size`，同一商品的多空兩列會互相蓋掉。支援它要動契約（`Position` 以 `(UID, 方向)` 為鍵、`Submit` 與事件帶方向）、adapter 與 ledger，一期不做。
- **檢查：**
  - account worker `Start` 時確認持倉模式。Binance UM 是帳號層級的 `dualSidePosition`（與 CM 共用），OKX 是帳號的 `posMode`，Bybit 依商品或結算幣。是雙向就 `INIT_FAILED`，reason `hedge_mode`；TD 的 ack 是 `REJECTED`，不重拉（E06-D008）。
  - 執行中不檢查：假設 worker 執行期間沒有人改持倉模式（E06-D016 修改）。
  - `Submit.params` 帶 `positionSide`、`posSide`、`positionIdx` 一律以 1xx 拒絕，不讓這個逃生口繞過。
- 不採用：跟著帳號的設定走、不指定模式。持倉方向必須進契約與帳本，成本見上。

### E06-D008 TD 的 Policy

- **與 MD 相同：結束了的 account worker 一律重拉，需求過舊時也重拉（`KeepWhileStale` 為真，E01-D009）。** 需求過舊時本來就不停正在跑的 worker，重拉一顆 crash 的 worker 是同一個立場：策略還在跑，帳號應該維持可以交易。代價是失聯期間有人停用了帳號，要等聯繫恢復、下一輪 reconcile 才生效。
- **設定錯誤不重拉：** `ErrAuth`（簽章錯誤、金鑰沒有權限或被撤銷）與 `hedge_mode`（E06-D007）。重拉也一樣會失敗。ack 是 `REJECTED`，reason 是 `auth` 或 `hedge_mode`：這個帳號在現在的設定下不可能成立（PROTOCOL §3.5）。
- **退避與放棄：** 退避 1 秒起，每次加倍，上限 60 秒。連續重拉 10 次（其間沒有穩定跑過 `StableAfter`，預設 1 分鐘）就放棄，依 P6 寫 `DEAD`（PROTOCOL §3.5）。
- **Beat 逾時 10 秒。** 逾時就停掉，照上面的規則重拉。
- 以上數字是部署設定。`DEGRADED` 不是 `DEAD`（E06-D003）。

### E06-D009 TD 的限速，以及一期不做的

- **每個 account worker 自己限速。** 依 `AccountLimits.OrderRate` 與交易所回報的 `Usage`；429、418 照 E04-D020 停送。
- **同一個出口 IP 上多個帳號共用的額度一期不處理。** Binance 的 request weight 依 IP 計，UM 與 CM 共用下單額度（E03-D001）。多個 account worker 在同一台主機上會一起吃同一份額度，各自限速看不到彼此。記在 PROTOCOL §10 P17。
- **一期不做：** 改單（Amend）與條件單。之後在 `OrderRequest` 加 oneof 分支，是相容擴充。

### E06-D010 `td.order` 只回 ack 或本地拒絕；單的每一次變化都是 `OrderUpdate`

- **`td.order` 的回覆只有兩種。** `accepted` 為真：TD 收下了，接著送往交易所，不代表交易所接受。`accepted` 為假：本地拒絕，code 是 1xx，什麼都沒送出。回覆裡沒有 `Order`，之後的狀態一律從 `td.evt` 拿。
  - `BatchCancel` 也一樣：回覆只說收下了，每一張單撤成或撤不成，各自是一則 `OrderUpdate`。拿掉 `BatchCancelResult` 與 `timeout_ms`。
  - account worker 完成 `Start` 之後才服務 `td.order`；在那之前送單和 worker 不在一樣，是 101。`td.view` 從連上 NATS 起就回答（E06-D006）。
- **單的每一次變化都是一則 `OrderUpdate{order, cause, code, reason}`。** `order` 是變化之後的完整狀態，`cause` 說為什麼變（PROTOCOL §7.2 的表）。成交明細仍是另一則 `Fill`。
  - 拒單也是 `OrderUpdate`：status `REJECTED`，cause `REJECTED`。交易所的拒絕與查單後確定沒下成（120）都是。
  - 撤單被拒也是：cause `CANCEL_REJECTED`，清掉 `cancel_sent_ns`，status 不變（見下一條）。
  - 拿掉 `OrderReject`、`CancelReject`、`OrderAck`。`Order.reject_reason` 移到 `OrderUpdate.code`、`reason`：`Order` 是狀態紀錄，`OmsView`、`Recon` 照舊用它；拒絕的原因屬於那一次變化。
  - 本地拒絕只在 `td.order` 的回覆裡，不發到 `td.evt`：什麼都沒送出，這張單在 TD 不存在。
- **`OrderStatus` 是單在交易所那一側的狀態。** 重新編號（草案，沒有已存的資料）：
  - `PENDING_NEW`：TD 已送出下單，交易所還沒回答。TD 自己的標記，adapter 不產生它。
  - `NEW`：交易所收到請求，當下沒有拒絕。還沒上簿，之後仍可能被拒（例如 Bybit 的 `Created` 之後 `Rejected`）。
  - `ACCEPTED`：在訂單簿上，還沒有成交。回覆本身就代表上簿的交易所（Binance 的 `NEW` 是撮合引擎收下）從 `PENDING_NEW` 直接到這裡。OKX、Bybit 的下單回覆只有 order id，是 `NEW`；OKX `live`、Bybit `New` 是 `ACCEPTED`。對照表在 PROTOCOL §7.2 與各 adapter 的 doc。
  - 市價單、IOC 可能不經過 `ACCEPTED`，從 `NEW` 直接成交或過期。
- **撤單在途是旗標，不是狀態：`Order.cancel_sent_ns`。** TD 送出撤單的時間，0 表示沒有撤單在途。
  - 撤單途中 `status` 照交易所的事件走，上簿、成交照常更新。撤單被拒只清掉旗標，沒有要還原的狀態。以「撤單前的狀態」還原是錯的：撤單途中可能已經上簿或部分成交。
  - 撤單沒有回答時，`status` 不變、旗標保留，worker 照 E06-D005 的界線查單。修改 E06-D005：撤單沒有回答不把單記成 `UNKNOWN`，單還在交易所上，不確定的只有撤單本身。
  - 查到單已終結就照常更新。查到單還在簿上：過了界線撤單已經不會生效，清掉旗標，發 cause `CANCEL_REJECTED`、code 120（`NOT_PLACED`），reason `cancel was not placed`，策略可以再撤。不清的話，之後的撤單都只回 `accepted` 不送出，這張單再也撤不掉。
  - 進終態時一律清掉旗標；之後才到的撤單結果不再發 `OrderUpdate`。目標單已經有撤單在途時再撤，回 `accepted`，不再送交易所。
  - 交易所自己的 pending-cancel 狀態（Binance `PENDING_CANCEL`、Bybit `PendingCancel`）是還在簿上的單：`ACCEPTED`，有成交就是 `PARTIALLY_FILLED`（`exchange.OnBook`）。
  - 拿掉 `ORDER_STATUS_PENDING_CANCEL` 與 `exchange.OrderStatusPendingCancel`。`OrderCause` 加 `ACKNOWLEDGED`（→ `NEW`），`ACCEPTED` 改指上簿。
  - 不採用：保留 `PENDING_CANCEL` 狀態，另加 `prev_status` 供還原。`prev_status` 要對，撤單途中就得跟著事件更新，那它就是底層狀態本身；快照裡一張 `PENDING_CANCEL` 的單也看不出底下是 `NEW` 還是 `ACCEPTED`。這和 FIX、Binance 把 pending cancel 當成單的狀態不同，是刻意的。
- **同一次變化只發一則。** 同一張單的同一次變化，交易所的回答與私有事件常常都會報（WS 下單的回答、REST 的回應、user data stream）。worker 依 `client_order_id` 與變化後的狀態去重再發佈，所以每個交易所都是同一個語意。
- **1xx 以 code 區分，`reason` 只給人讀。** `RejectCode` 替 1xx 取名，名字要讓寫策略的人看得懂：沒有 responder 叫 `ACCOUNT_OFFLINE`。
  - 只列一期會碰到、有票驗收的，之後碰到新的情況再加。表在 PROTOCOL §7.2。
  - 101–119 是本地拒絕，只出現在 `td.order` 的回覆；120 起是送出之後確定沒下成，只出現在 `OrderUpdate`。
  - E03-D001、E06-D003、E06-D005、E06-D007 寫的 `venue_not_tradable`、`account_degraded`、`not_placed`、`hedge_mode`，分別是 105、102、120、103。etcd ack 的 reason `hedge_mode`（E06-D008）不是拒單碼，不變。
  - 撤單的目標不是掛單（107）與限速（108，E06-D009 照 E04-D020 不排隊）都是一期就會碰到的，所以列入。drain 中的新單沒有拒單碼：一期沒有人送 `Drain`，等 drain-replace 落地時再加。
- **商品以 UID 指定（E02-D003）。** `Submit`、`Cancel`、`BatchCancel`、`Order`、`Fill`、`Position`、`LeverageRequest` 與 `tdctl.Report.first_submit_ns` 的 key 都換成 `uint32` UID。subject 仍是 `td.*.{api_id}`。
- **`td.view` 的狀態欄位放在 `ViewReply` 上**：`state`、`reconciled_at_ns`、`incarnation`、`seq`，每一種回覆共用（E06-D006）。`OmsView`、`LedgerView` 原本各自的 `incarnation`、`as_of_ns` 拿掉；這兩個訊息後來由 E06-D033 換成 `Recon`。`AccountState` 從 `tdctl.v1` 移到 `td.v1`，`tdctl.Report` 改用它。`LeverageInfo.leverage` 是 optional：不設表示快取裡沒有。
- **`OmsViewRequest` 拿掉 `session_id`。** 單上只帶 algo（E06-D002），TD 沒有 session 可以篩。`OmsViewRequest` 後來整個由 E06-D033 換成 `ReconRequest`。`Submit`、`Cancel` 的 `session_id` 只給 log。
- 不採用：`td.order` 等交易所回答才回覆，或回覆帶 `Order`。結果會在回覆與 `td.evt` 各報一次，策略要對兩個來源排序、去重；回覆的逾時也得蓋過交易所的延遲。
- 不採用：保留 `OrderReject`、`CancelReject` 當獨立事件。同一張單的狀態散在三種事件裡，策略要自己拼。

### E06-D011 TD 的 NATS 身分：account worker 每個帳號一份；TD agent 沒有 user（E06-D039）

- **account worker 每個 `api_id` 一份 user JWT，由 TD agent 簽。** 和 session 的憑證一樣（E09-D006）：spawn 時在本機簽，經 `Init` 的 socketpair 送進去（`tdctl.InitPayload.nats`），不放環境變數、不落檔，不設過期。
- **為什麼每個帳號一份。** 一顆 account worker 只能服務自己的 `td.order.{api_id}`。用平面共用的憑證時，一顆 worker 訂得到別的帳號的 `td.order`；同一個帳號被兩顆 worker 同時服務，同一筆請求會送兩次。交易所只在掛單之間檢查 `client_order_id` 唯一（例如 Binance），已經終結的單擋不住第二次，不能靠交易所去重。
- **TD agent 沒有 NATS user（E06-D039）。** 不發 instance 憑證，沒有 `MFTIKO_NATS_CREDS`，沒有 inbox。`cmd/mftiko-nats-dev` 不產生 `td-{inst}`，也沒有 `-td-inst`。它不 publish、不 subscribe。簽 account worker 用的是 account 的簽章金鑰，不是它自己的 user。
- **account worker 的權限（`tdAccountPerms`，E06-D039 不改）：**

  | | account worker（`td-account-{api_id}`） |
  |---|---|
  | publish | `td.evt.{api_id}`、`sym` |
  | subscribe | `td.order.{api_id}`、`td.view.{api_id}`、`sym.v1.changed.>`、`_INBOX_td_{api_id}.>` |
  | 回覆 | 一則、兩分鐘（E04-D015） |

  - account worker 的 inbox 前綴是 `_INBOX_td_{api_id}`，一份憑證一個前綴，和 session 一樣。`session_id` 是六個 hex，不會和它撞。`_INBOX_td` 沒有 user（E06-D039）。
  - account worker 的 `sym` 是 publish 恰好這個 subject，不是 `sym.>`（E04-D015）。
- **TD agent 持有 `MFTIKO` 的簽章金鑰**，和 STS agent 同一把（E04-D014）。系統是單人使用，兩把只多了輪換的工作。之後要限制 TD agent 能簽出的權限，可以改用 NATS 的 scoped signing key。
- **檢查（E04-D017、E06-D039）：** 嵌入式 nats-server 上，在記憶體裡簽一顆 account worker，確認它服務得了自己帳號的 `td.order`、`td.view`，發得出自己的 `td.evt`；別的帳號的 `td.evt`、`td.order`、`td.view`、別人的 inbox 一律被拒。產生的目錄裡沒有 TD agent 的 user：manifest 的 `td` 是空的，沒有 `td-*.creds`，沒有 `user-td-*.nk`。creds 裡的 user JWT 名字不得以 `td-` 開頭（`td-account-` 也不落檔），subscribe 不得含 `td.evt.>`。看到就失敗。
- 修改 E09-D006 最後一條與 E04-D014 的簽章金鑰。E04-D015 留給 TD 的權限由這一條補上；SYM 仍未定。

### E06-D012 持倉模式由 Snapshot 帶回；假帳號 `AccountVenue`

- **`exchange.AccountSnapshot` 加 `HedgeMode`。** 帳號在任何地方是雙向持倉就是真，任何一頁說是就是。worker 只在 `Start` 的第一次 recon 檢查（`INIT_FAILED`，reason `hedge_mode`），之後的 recon 不看它（E06-D016 修改）。
  - 不另加方法：模式和持倉是同一份帳號狀態，recon 時一起讀；`Start` 的檢查也只是第一次 recon。
  - 真實 adapter 還沒讀持倉模式，一律是假：Binance UM 要多一個 `GET /fapi/v1/positionSide/dual`，OKX 讀 `GET /api/v5/account/config` 的 `posMode`，Bybit 看持倉的 `positionIdx`。在 E6-16 做完之前，這個檢查對真實交易所不生效。這一條只定介面。
- **`exchangetest.AccountVenue` 是一個帳號的假交易所**，給 account worker 的測試用（E6-05）。
  - 狀態在記憶體：掛單、持倉、餘額、槓桿、持倉模式。測試經由它改狀態（`PutOrder`、`Fill`、`SetBalance`、`SetPosition`）時，同時推出交易所會發的事件，跑完之後可以拿 worker 的 `td.view` 與它比對。不改狀態的重複或亂序事件用 `EmitOrder`、`EmitFill`。
  - REST：它自己就是 `exchange.Doer`。`Snapshot` 可分頁，可在指定頁失敗；`Submit`、`Cancel`、`CancelBatch` 依 cloid 指定接受、拒絕或不回答（`ErrUnknownOutcome`，可選交易所是否照做）；可以回 429（`Retry-After`）與回報用量（`ResetAt`）；換 key 之後一律 `ErrAuth`。`Requests` 列出收到的每個請求。
  - 私有串流：`Handler` 是真的 websocket，以 httptest 跑，worker 照正式路徑用 `exchange/transport/ws` 連。可以拒絕登入、讓握手失敗、一次斷掉所有連線、暫扣事件以決定先到的是回答還是事件，以及 Binance 式的 listenKey 與到期。
  - 不撮合：單一直掛著，直到測試成交或撤掉。不驗簽章，只比對 key。查單與「請求晚到」在 E6-16 加。
  - 不採用：私有串流用行程內的假連線。那樣 worker 的連線路徑就少測了 ws 傳輸；MD conn worker 的測試也用 httptest。

### E06-D013 帳號憑證以 TD instance 的公鑰封裝

- **每個 TD instance 一對 X25519 金鑰，不是每個帳號、也不是整個平面一把。** 一個帳號只會被一個 TD instance 跑（E09-D008，不跨主機接手），封給它就夠；一台主機被拿走，損失只限於封給它的帳號。私鑰是該主機上的檔案，0600，路徑是部署設定；權限比 0600 寬就拒絕載入。
- **格式是 `version(1) | fingerprint(8) | NaCl sealed box`**（`golang.org/x/crypto/nacl/box` 的 `SealAnonymous`：X25519、XSalsa20-Poly1305，每次封用新的暫時金鑰）。fingerprint 是收件公鑰 SHA-256 的前 8 bytes，讓 TD agent 不必試解就分得出「封給別的 instance」和「壞掉」。換演算法就換 version。
  - 不採用：自己組 X25519＋HKDF＋AES-GCM。sealed box 是現成的、介面小，不用自己決定 nonce 與 KDF。
  - 不採用：age。功能多於需要（多收件人、passphrase、armor），格式也不是我們控制。
- **明文是 `state.v1.AccountCredentials`，`api_id` 與 `kind` 一起封進去。** sealed box 沒有 associated data，密文從一個帳號搬到另一個帳號底下照樣解得開；TD agent 解開之後比對 etcd key 的 `api_id` 與 `Account.kind`，不符就不用。
- **解不開時，需求 `REJECTED`，reason 分三種：** `credentials_other_key`（指紋不是本 instance 的，用它的公鑰重封）、`credentials_unreadable`（空、版本不認得、長度不足或損壞；竄改指紋也算這個）、`credentials_other_account`。由 `sealbox.Reason` 產生；TD agent 寫 ack 在 E6-10。
- **公鑰的交付，一期是檔案或 CLI。** `cmd/mftiko-seal keygen` 寫私鑰並印出公鑰，`pubkey` 從私鑰檔印公鑰，`seal` 從 stdin 讀憑證 JSON、印出 base64 的密文。憑證只走 stdin，不進 flag，免得留在 shell history 與行程列表。TD instance 把公鑰發布到 etcd、API server 寫帳號時自己封，留到 E10。
- **明文只在記憶體裡。** `sealbox.Credentials` 與私鑰對任何 fmt 動詞與 JSON 都印 `REDACTED`；錯誤只說哪一步失敗，不帶 bytes。解開後經 `tdctl.InitPayload.credentials` 由 socketpair 送進 account worker（§5.4）。歷史補正的那次請求若要帶憑證，誰送見 PROTOCOL §10，不是 TD agent（E06-D038、E06-D039）；服務用完不留。
- **換 key：** 先把舊檔移開才能寫新的（`keygen` 不覆蓋），之後封給舊 key 的帳號全部要重封；在重封之前它們是 `credentials_other_key`。

### E06-D014 account worker：連線角色、狀態與一期的範圍

- **一期不做 BinancePM。** PM 依 ticker 的 venue 把單送到 margin、UM、CM 不同的 WS API，一個 api_id 要有多條 trade stream；`Account.NewTradeStream()` 只回一個 codec。account worker 遇到交易不只一個 venue 的帳號種類就 `INIT_FAILED`，reason `kind_unsupported`；TD agent 對這種帳號的需求寫 `REJECTED`，同一個 reason（E6-10）。E03-D001 不變：PM 仍是合法的帳號種類，E3 的 papi 碼留著。之後要做，從 `NewTradeStream` 依 venue 取 codec 改起。
- **連線依角色區分，不寫死條數。**
  - 事件連線：帶私有事件的那一條（帶事件的 trade stream，否則 `PrivateStream`）。
  - 下單連線：不帶事件的 trade stream。
  - 一期每個 api_id 至多各一條；換線期間會暫時多一條。
- **只有事件連線決定 `DEGRADED`（E06-D003）。** 事件斷了，OMS 就看不到單的變化，所以拒新單、放行撤單，重連並 recon 之後才回到 `READY`。下單連線斷了，下單改走 REST（E03-D003 的 `Submit`、`Cancel`），狀態不變。
  - 不採用：任何一條斷線都拒新單。這樣比較保守，但 OKX、Bybit 的 trade stream 一抖就會停單，而 REST 明明可以下。
- **計畫性換線先接後斷。** 連線到 `MaxLifetime` 減兩個 margin（`ws.MaxLifetimeMargin`）時撥新的，新的 Ready 之後才關舊的。期間不是 `DEGRADED`。事件連線換完跑一次 recon，reason 是 `RECONNECT`，期間不報 `DEGRADED`（E06-D035–E06-D037）。下單連線的換線不 recon、狀態不變。兩條連線重疊的那一小段，同一則事件可能到兩次；recon 期間的事件先排著，快照之後依序重放，相同的不發（E06-D015）。新的撥不上時，舊的照常在 lifetime 前一個 margin 關掉，之後依斷線處理。
  - 不採用：先斷後接。每條連線每天會有一次固定的 `DEGRADED` 和一次 recon（Binance 是 24 小時）。
  - 不採用：換線不 recon。新連線還在撥的時候舊的若斷了，`serve` 沒有在看舊連線，這段缺口的成交與委託變化不會進 OMS（#200）。
- **reason：**
  - `auth`：交易所拒絕憑證。Start 時是 `INIT_FAILED`，執行中重撥被拒是 `CRASHED`（exit 20）。
  - `hedge_mode`：Start 時是 `INIT_FAILED`；執行中不檢查（E06-D016）。
  - `kind_unsupported`。
  - `stream_lost`：`DEGRADED` 的 reason。

  E06-D008 的 Policy 不重拉 `auth` 與 `hedge_mode`。
- **listenKey：**
  - 每 `RefreshEvery` 續約一次。續約失敗只記 log，不斷線：key 真的到期時，交易所會送到期事件（`Expired`），worker 收到才重連並重新 `Prepare`。
  - 續約被拒絕憑證時，依 `auth` 處理。
- **endpoint 由 Init 帶入**（`tdctl.InitPayload` 的 `rest_url`、`stream_url`、`simulated`），值是 td-agent 依帳號種類的設定（E06-D016 修改；原本是 worker 的旗標）。

### E06-D015 OMS 只放活著的單；recon 期間的事件先排著

- **OMS 只放活著的單。** 一張單送出終態的 `OrderUpdate`（FILLED、CANCELLED、REJECTED、EXPIRED）之後，就從 OMS 刪掉，記憶體裡不保留已終結的單。熱路徑上不查資料庫：`td.view` 只回活著的單。`settled` 沒有作用，欄位已拿掉（E06-D032）；終結的單從 `td.evt` 拿，歷史由誰落庫尚未定（E06-D038，PROTOCOL §10），不會是 TD agent。
  - 撤單指到不在 OMS 裡的單，以 107（`ORDER_NOT_OPEN`）本地拒絕，什麼都不送（E6-08）。
  - 冪等只涵蓋活著的單：已終結單的 cloid 再送一次，worker 不認得，照新單送出，交易所是否拒絕重複的 cloid 由它決定。同一個 cloid 重送本來就是 SDK 不做的事（E06-D005）。
  - 記得最近終結的單的 key（有上限，與 fill_id 同量），只用來丟掉同一個終態的重複（交易所的回答與私有事件都報了、換線時兩條連線都送了），不回答查詢。
- **單一 goroutine 持有帳號狀態。** OMS、持倉、餘額、槓桿快取與 `td.evt` 的 seq 由一個 goroutine 持有；連線讀到的事件先複製（frame 是借來的，E03-D002）再交給它，`td.view` 也經它回答，所以回覆的 seq 與內容一致（E06-D006）。
- **與交易所之間以 `order_id` 對單。** 事件與 recon 都以 `order_id` 找到活單。交易所還沒給號的單，第一則帶著同一個 `client_order_id` 的訊息先對上，再記下 `order_id`。沒有 client id 的單只以 `order_id` 追蹤；查單表以 client id 為鍵，所以不替它們發起查單。斷線期間從快照消失的，留在 `UNKNOWN`，下一份帶著同一個 `order_id` 的快照或事件再對上。系統裡面（策略、`td.order`、`Cancel`、`BatchCancel`、`td.view`、向外的 `OrderUpdate`）以 cloid 為鍵。
- **recon 期間的事件先排著。** recon 開始時，之後到的事件不套用、不發出。快照回來之後：整份取代 OMS 的活著的單、持倉、餘額 → 發 `Recon`（新 incarnation 的第一次是 `INCARNATION`，之後是 `RECONNECT`）→ 依到達順序重放排著的事件，有變化的才發 `td.evt`。
  - 為什麼重放就對：同一條連線上交易所依發生順序送事件，比快照舊的那則之後的變化也一定排在後面，重放完就是交易所的現況。
  - 防護：單的 `filled_qty` 不倒退，狀態依 `PENDING_NEW < NEW < ACCEPTED < PARTIALLY_FILLED < 終態` 不倒退，終態不復活。與現有紀錄相同的事件不發（只差時間戳也算相同）。
  - 排著的事件有上限（十萬則），超過就丟掉並記 log：之後的快照有狀態。成交的落庫尚未定（E06-D038）。
  - recon 跑完時事件連線已經又斷了：這份快照不套用，維持 `DEGRADED`，事件繼續排著，等下一次 recon（補上 E6-06 的縫）。
- **斷線期間消失的掛單記成 `UNKNOWN`。** OMS 裡活著、快照裡沒有的單，是在看不到的時候終結了（或從沒下成），結果不知道。recon 之後它在 OMS 與 `Recon.open_orders` 裡是 `UNKNOWN`，等 E6-08 查單（已有 `order_id` 用它，否則用 cloid；`Account.QueryOrder`，E6-16）收斂。沒有 client id 的不排查單。還是 `PENDING_NEW` 的單（交易所還沒回答）照舊。交易所之後對它的任何消息都算新的。
  - 不採用：E6-07 直接依賴 E6-16 查單。E6-08 本來就要接 `QueryOrder`，收斂邏輯放在一起。
- **商品換成 UID，數量換成平台單位（E02-D003）。** 交易所的寫法先以活躍表的記憶體索引 `Reverse`，沒有再以 `includeInactive` 問 SYM（剛下架的商品，有逾時）。SYM 從沒見過的商品記 log、丟掉，不進 OMS 與帳本，也不發 `td.evt`。數量（`qty`、`filled_qty`、`Fill.qty`、`Position.size`）乘以 `ContractSize`：adapter 回報交易所的單位（OKX 合約是張數）。價格不換。
- **槓桿快取取自持倉的 leverage。** recon 與持倉事件帶的 `Position.leverage` 寫進快取；持倉平掉之後仍保留（槓桿是商品的設定）。沒有持倉過的商品回未知。不另外打 `Account.Leverage`。
  - 不採用：recon 時對每個商品打 `Leverage`。對哪些商品查沒有好答案，額度也不划算；策略需要「還沒下單就知道槓桿」時另開票。
- **account worker 從 Init 起連 NATS。** 用 `InitPayload.nats` 的 user JWT 與 seed（不落檔，`natsx.Options.UserJWT`、`Seed`），inbox `_INBOX_td_{api_id}`；載入帳號種類那個 venue 的活躍表並訂閱變更；訂閱 `td.view.{api_id}`，第一次 recon 之前就回答（E06-D006）。連不上或載不到是 `INIT_FAILED`。

### E06-D016 td-agent；執行中不檢查持倉模式；algo 由 STS agent 檢查

- **td-agent 照 MD agent 的寫法，在 `internal/td/agent`。** 讀兩個前綴：controller 寫的需求 `/td/{inst}/demand/{session}/{api_id}` 與 API server 寫的帳號 `/td/{inst}/account/{api_id}`。一個 `api_id` 的 owner 是它的需求集合；有 owner 就跑一顆 account worker（worker key `account/{api_id}`），最後一個 owner 離開後等 linger 才停（E09-D001、E09-D005）。需求離開時 ack 立刻刪掉，linger 留的是 worker。
- **不跑 worker、ack 寫 `REJECTED` 的帳號：** 不存在（`account_not_found`）、停用（`account_disabled`）、種類不認得（`unknown_kind`）、一期不跑的種類（`kind_unsupported`，E06-D014）、封裝的憑證打不開（`sealbox.Reason`，E06-D013）。帳號在 linger 期間被停用，worker 立刻停。
- **ack：** worker 回報 `READY` 或 `DEGRADED` 時是 `READY`（E06-D003）；worker 以 `auth`、`hedge_mode`、`kind_unsupported` 結束時是 `REJECTED`，不重拉（E06-D008）；重拉 10 次放棄時是 `DEAD`（P6）；其他是 `PENDING`。
- **spec：** 帳號 id、種類、封裝憑證的 digest、`cancel_on_disconnect_s`、這個種類的 endpoint。任何一項變了就換 incarnation：重封的憑證、改了 host。TD 沒有 live config，`Init` 帶全部。
- **spawn 時才解封**，明文只在組 `Init` 的那一刻。每次 spawn 以 MFTIKO account 的簽章金鑰簽一個新的 NATS user（`natsdev.Signer`，E06-D011），JWT 不設到期：worker 重拉就換一份。
- **endpoint 比照 MD（E04-D003）：** td-agent 的 `-endpoints`（帳號種類 → 私有 websocket URL）、`-rest-endpoints`（帳號種類 → REST URL）、`-simulated`，經 `InitPayload` 交給 worker，算進 spec。
- **持倉模式只在 Start 檢查。** 假設 worker 執行期間沒有人改持倉模式；改了也不偵測。`AccountState` 拿掉 `UNAVAILABLE`，103 只剩 `Submit.params` 帶持倉方向鍵的拒絕。修改 E06-D007、E06-D012。
- **algo_id 重疊不由 TD 檢查。** STS agent 檢查它自己 instance 上的 session；跨 STS instance 的重疊由使用者保證不發生。`DemandEntry.algo_ids` 留著，TD 不讀。修改 E06-D002。
  - 不採用：TD 以需求的建立順序拒絕較晚的那筆（`algo_conflict`）。使用者行為受限，保持簡單。

### E06-D017 以 order_id 查單、請求有效期、讀持倉模式（E6-16）

- **`exchange.Account` 加 `QueryOrder(now, ref CancelRef)`，回 `Call[*OrderUpdate]`。** E3 定案的介面（E00-D006）在這裡擴充。一律走 REST：Bybit、OKX 的 WS 下單通道沒有查單。單已經有 `order_id` 時只帶它查，不帶 `client_order_id`；還沒有 `order_id` 才用 `client_order_id`。找得到已終結的單（成交、撤單），不只掛單。
  - 交易所說沒有這張單時，`Parse` 回 `exchange.ErrNotFound`，不是 `*Reject`，也不是一般錯誤：Binance 的 `-2013`、OKX 的 `51603`、Bybit `GET /v5/order/realtime` 帶 `orderLinkId` 回空清單。worker 只在過了有效期之後才把它讀成「沒下成」（E06-D005）。
  - Bybit 以 `orderLinkId` 查時不看 `openOnly`，已終結的單只保留每個 category 最近 500 張；Binance UM 沒有成交、已撤或過期的單，建立三天後查不到。兩者都遠長於 worker 等待的時間。
- **有效期是 `AccountConfig.Validity`，由 worker 給，adapter 寫進請求。** 0 是 `exchange.DefaultValidity`（5 秒）；上限 `exchange.MaxValidity`（60 秒，Binance `recvWindow` 的上限），須為整毫秒，factory 建帳號時檢查。
  - Binance：每個簽名的 REST 請求，以及 WS API 的 `session.logon`、`order.place`、`order.cancel`，都帶 `recvWindow`。
  - Bybit：每個 REST 請求與 trade socket 每個下單撤單 frame 的 `X-BAPI-RECV-WINDOW`。
  - OKX：`Submit`、`Cancel`、`CancelBatch` 的 `expTime` header，值是簽名時間加 `Validity`（Unix ms），不在 prehash 裡。讀取不帶。
  - 放在 `AccountConfig`，不放在每個請求：有效期是部署設定（E06-D005），一個帳號只有一個值，trade stream 的 codec 也從同一處拿。worker 把它接到旗標，和用它算查單時點，屬於 E6-08；在那之前一律是預設 5 秒，與原本寫死的 `recvWindow` 相同。
- **持倉模式由 `Snapshot` 讀（E06-D012）：**
  - Binance UM：多一個 `GET /fapi/v1/positionSide/dual`（weight 30），`dualSidePosition` 為真即是。現貨沒有持倉，恆為假。
  - OKX：多一個 `GET /api/v5/account/config`，`posMode` 是 `long_short_mode` 即是；不認得的值是錯誤。
  - Bybit：v5 沒有讀持倉模式的 API（只有 `switch-mode`，是寫入），帶 `settleCoin` 的持倉清單也不列空倉。所以以掛單與持倉的 `positionIdx` 判斷：1 或 2 只會出現在雙向持倉。空帳號讀起來是單向，`Start` 的檢查會放行；持倉模式只在 `Start` 檢查（E06-D016），之後也不會再發現。在雙向模式下不帶 `positionIdx` 的單會被交易所拒絕，所以不會在錯的假設下成交，只是這個帳號下不了單。
  - 不採用：Bybit 以 `switch-mode` 試探（已是該模式時回錯誤碼）。那是寫入，帳號有倉位或掛單時還可能真的切換失敗或改到設定。
  - 不採用：Bybit 對每個要交易的商品帶 `symbol` 查持倉（帶 symbol 時空倉也會回 `positionIdx`）。`Snapshot` 不知道要交易哪些商品，請求數也隨商品數長。
- **假帳號（E06-D012）：** `AccountVenue` 保留已終結的單，`QueryOrder` 查得到；`Outcome.Late` 讓 `Submit` 或 `Cancel` 在路上耽擱：`Do` 回 `ErrUnknownOutcome`，交易所什麼都不做，直到測試呼叫 `Arrive(cloid, at)`。`at` 不晚於「簽名時間＋`Validity`」就照常生效並推事件，晚了就被拒、什麼都不變。只有 REST 的請求能晚到；`TradeVenue` 沒有。
- **conformance：** 四家都有查單的資料（掛單、部分成交、成交、撤單、不存在）與持倉模式的資料（單向、雙向；Bybit 是帶 `positionIdx` 1、2 的掛單與持倉）。這些回應是依交易所文件手寫的，不是 testnet 錄下來的；在 E6-12 到 E6-14 的 testnet 驗收時以真實回應核對。

### E06-D018 cloid 冪等（E6-08）

- **E06-D018 cloid 冪等：** TD 只在記憶體記得本次執行見過的 cloid（最近 24 小時或 10 萬筆，取先到者），不持久化。重啟後靠 recon 取回仍在交易所的單，已結束的單交由交易所自身的重複 cloid 檢查。
- 修改 E06-D005：冪等不是「同一個 incarnation 裡還活著的單」。這次執行結束就忘掉；重啟之後不認得已結束的單。

### E06-D019 同 cloid 內容不同（E6-08）

- **E06-D019 同 cloid 內容不同：** 同一 cloid 重送但內容不同，回 106 `INVALID_ORDER`，不送交易所。內容相同則回原結果。
- 比的是 `uid`、`side`、`order_type`、`qty`、`quote_qty`、`price`、`tif`、`reduce_only`、`params`、`algo_id`，不含 `session_id` 與 `recv_window_ms`。數量與價格用刻度相等（E00-D008）。本地拒絕也記下來，重送回原本那一次的結果，不送出。recon 取回、記憶體裡還沒有的活單：重疊的欄位相同回原結果（`accepted`），不同回 106，而且這次不對的嘗試不記進記憶體。撤單不是同一筆 cloid 的重送。

### E06-D020 有效期（E6-08）

- **E06-D020 有效期：** `Submit`/`Cancel` 加有效期欄位（即 Binance `recvWindow`、Bybit `recv_window`、OKX `expTime`），由策略或 SDK 填（SDK 預設 5 秒，對應 E8 E08-D003）。0 表示用 TD instance 預設值，超過交易所上限就壓到上限。時鐘誤差由 TD instance 設定（預設 1 秒）。此欄位一併納入 E6-15 td proto 定案。
- 修改 E06-D017 的「不放在每個請求」：`AccountConfig.Validity` 仍是 TD instance 的預設（0 是 5 秒，上限 60 秒，須為整毫秒），每一筆 `Submit`、`Cancel` 的 `recv_window_ms` 可以蓋過它。0 用預設，負值當 0，超過上限壓到上限，不拒絕。worker 接到 `-validity`（0 即 5 秒）與 `-clock-skew`（預設 1 秒）。

### E06-D021 本地限速（E6-08）

- **E06-D021 本地限速：** 用到 `OrderRate` 的 90% 就回 108 `RATE_LIMITED`。
- 送出前檢查，`used * 10 >= limit * 9` 就拒絕。`Submit` 與 `Cancel` 都算；本地拒絕與冪等重放不扣額度。429 與 418 仍照 E04-D020 停送，不排隊、不重試。

### E06-D022 查單失敗（E6-08）

- **E06-D022 查單失敗：** 指數退避 1–30 秒並持續重試；超過 5 分鐘仍查不到結果，帳號標成 `DEGRADED`。
- 用既有的 `ACCOUNT_STATE_DEGRADED`，不另開狀態。reason 是 `query_failed`；已經是 `stream_lost` 時不改寫。事件連線斷線會把 `query_failed` 改成 `stream_lost`。新單仍是 102，撤單照常，查單繼續重試。5 分鐘從該筆查單的第一次失敗起算。每一筆失敗中的查單都有結論（查到或 `ErrNotFound`），而且事件連線在，才回到 `READY`。重連後的 recon 若先回到 `READY`，還沒查到的單仍可能再標成 `DEGRADED`。限速停送不算查單失敗。

### E06-D023 撤 UNKNOWN 單（E6-08）

- **E06-D023 撤 UNKNOWN 單：** 狀態為 `UNKNOWN` 的單可以用 cloid 撤，不回 107。`Cancel` 只收 `client_order_id`（欄位 4 已刪，號碼不重排，E06-D029）。OMS 以 cloid 找到活單，送出時帶上那張單的 `order_id`；還沒有就用 `client_order_id`。

### E06-D024 WS 斷線（E6-08）

- **E06-D024 WS 斷線：** `TradeStream` 不可用時改走 REST，不拒單。
- 請求還沒寫出去才改走 REST。寫出去之後的錯誤是 `ErrUnknownOutcome`，不重送、不改走 REST。`DEGRADED` 時撤單走 HTTP（E06-D003）。

### E06-D025 範圍（E6-09）

- **E06-D025 範圍：** E6-09 只做 `BatchCancel`。cancel-on-disconnect（Binance `countdownCancelAll`、OKX `cancel-all-after`、Bybit DCP）不做：`AccountSettings.cancel_on_disconnect_s` 與 adapter 的 `CancelOnDisconnect` 不接，完成標準第 3 條拿掉。
- `BatchCancel` 帶明確的 `client_order_id` 清單（`order_id` 欄位已刪，號碼不重排，E06-D029），對應一筆交易所批次撤單。TD 以 client id 找到 OMS 裡的活單，送出時改用那張單的 `order_id`（還沒有才用 client id）。不依 `algo_id` 或 `uids` 挑單，也不檢查這些單屬於哪個 algo。解不開成平台 cloid 的 client id 不屬於任何 algo，但仍能指名那張單。TD 一對一轉發，不分批、不排隊、不重試。
- 交易所沒有批次撤單（`Limits.CancelBatch` 為 1，例如 Binance 現貨）時，TD 本地拒絕：這個方法不能用，不改走單筆 `Cancel`。
- 清單長過交易所的批次上限時仍送出，交易所的拒絕原樣回報。
- 清單裡任何一張對不到活單，或商品解不出來，只拒絕這一筆，什麼都不送；別的 `BatchCancel` 不受影響。
- 「一次撤掉這個 algo 的全部掛單」改由 STS SDK 做：SDK 知道自己的活單，讀 `td.view` 回覆的 `cancel_batch`（這個帳號的 `Limits.CancelBatch`，E06-D031）自己拆批，沒有批次撤單就改走單筆 `Cancel`。這一期的 TD 不做。

### E06-D026 批次結果不明（E6-09）

- **E06-D026 批次結果不明：** 這筆批次撤單的結果不明（`ErrUnknownOutcome`）時，批內每張單照 E06-D022 查單收斂（已有 `order_id` 用它，否則用 cloid），不重送。

### E06-D027 DEGRADED（E6-09）

- **E06-D027 DEGRADED：** 帳號 `DEGRADED` 時 `BatchCancel` 照常，與 `Cancel` 相同（E06-D003）。

### E06-D028 撤單限速（E6-09）

- **E06-D028 撤單限速：** `Cancel` 與 `BatchCancel` 不受 E06-D021 的 90% 本地餘量限制，可用到 `OrderRate` 的 100%；交易所回 429/418 時仍依 `Retry-After` 停送（E04-D020）。
- 修改 E06-D021：90% 只適用 `Submit`。`Cancel` 算一張。`BatchCancel` 算一筆，不問清單裡有幾張；不再因為張數放不進窗口而整筆回 108。放不進這一筆才回 108，什麼都不送、不排隊。

### E06-D029 定案前刪掉 `reserved`（E6-15，#175）

- **系統尚未上線。** `td.proto` 裡已刪欄位留下的 `reserved` 直接刪除，不保留。現有欄位號碼不重新編號。`tdctl.proto` 沒有 `reserved`。
- **刪掉的是：**
  - `AccountState` 的 3 `ACCOUNT_STATE_UNAVAILABLE`（E06-D016）
  - `OrderRequest` 的 3 `cancel_session`（E06-D002）
  - `Cancel` 的 4 `order_id`
  - `BatchCancel` 的 1 `algo_id`、2 `uids`、3 `timeout_ms`
  - `BatchCancelOrder` 的 2 `order_id`
  - `OmsViewRequest` 的 1 `session_id`（E06-D002、E06-D010）
  - `OmsView` 的 2 `incarnation`、3 `as_of_ns`（E06-D006）
  - `LedgerView` 的 3 `incarnation`、4 `as_of_ns`（E06-D006）
  - `AccountEvent` 的 12 `order_reject`、13 `cancel_reject`（E06-D010）、16 `account_reset`（E06-D003）
  - `Order` 的 12 `reject_reason`（E06-D010）
- **這兩份定案。** 從 `buf.yaml` 的 `breaking.ignore` 移出，`buf breaking` 開始檢查。PROTOCOL §1 的 `tdctl.proto` 列，以及 `td.proto`、`client_order_id`（§2）那一列，改為定案。`OmsViewRequest` 的 `reserved 2` 與 `reserved "settled"` 留下（E06-D032）。歷史補正欄位留下（E06-D030）。先刪舊 `reserved`、合併、再移出，是因為 `RESERVED_MESSAGE_NO_DELETE` 與 `RESERVED_ENUM_NO_DELETE`。
- **`OmsViewRequest.settled` 不在 E06-D029 的刪除裡。** 它當時不是 `reserved`。拿掉並改成 `reserved` 是 E06-D032。
- **修訂 E06-D023、E06-D025 裡「order_id 欄位保留」的說法。** 那兩個訊息上的 `order_id` 欄位已刪，號碼不重排。OMS 送出時仍帶那張單的 `order_id`，那是交易所那一側的身分，不是訊息上的欄位。

### E06-D030 歷史補正欄位保留（E6-15，#175）

- **`AccountSettings` 的補正 interval 與手動觸發照現狀保留。** `backfill_interval_s`、`backfill_requested_at_ns` 屬暫緩的 E7（#9）。定案 `tdctl.proto`、`td.proto` 時不拿掉。它們在 `state.proto`，`state.proto` 仍是草案。
- **`td.backfill` 相關欄位照現狀保留。** `tdctl.Report.first_submit_ns`（backfill 的範圍從下單決定，E06-D001）留下。PROTOCOL 裡 `td.backfill.{venue}` 的契約留下。不另刪欄位。
- 這兩份 proto 移出草案清單時，這些欄位一起留下。

### E06-D031 `cancel_batch` 給策略（E6-15，#175）

- **td proto 原本沒有把帳號的 `CancelBatch` 給策略。** `Limits.CancelBatch` 只在 account worker 裡。策略經 `td.order`、`td.view`、`td.evt` 看不到。
- **每一則 `td.view` 回覆都帶 `ViewReply.cancel_batch`。** 值是這個帳號的 `Limits.CancelBatch`。1 表示交易所沒有批次撤單（E06-D025）。問 OMS、帳本、槓桿，或請求是空的，回覆都帶。從連上 NATS 起就有，不必等 `Start`。
- **SDK 照這個數拆批（#211）。** 修訂 E06-D002、E06-D025：SDK 讀的是 `cancel_batch`。值是 1 就改走單筆 `Cancel`。TD 仍不挑單、不拆批。
- 欄位號碼 14，接在 `seq` 後面。不重用已刪的號碼。

### E06-D032 拿掉 `OmsViewRequest.settled`（E6-15，#175）

- **`settled`（欄位 2）拿掉。** 它沒有作用（E06-D015）：OMS 只放活著的單，終結的單從 `td.evt` 與落庫的歷史拿。
- **比照 E06-D029，改成 `reserved`，不重新編號。** `OmsViewRequest` 寫 `reserved 2;` 與 `reserved "settled";`。`algo_id` 仍是欄位 3。
- **這條 `reserved` 留下。** E06-D029 刪的是定案前已經存在、系統尚未上線所以不保留的舊 `reserved`。這條是刪 `settled` 時加上的。移出草案清單時沒有刪它：再刪會被 `buf breaking` 的 `RESERVED_MESSAGE_NO_DELETE` 擋住。E06-D033 讓 `td.proto` 回到草案，連同 `OmsViewRequest` 一起刪掉。

### E06-D033 `td.view` 改成一份 Recon；TD 不解碼 cloid（E6-17，#220）

- **第一份帳號狀態只問一次。** OMS 與帳本原本是兩個請求，回來的是兩份快照、兩個 seq，SDK 得各自接上 `td.evt`（E06-D003、E06-D006）。E06-D003 的帳號狀態本來就是 open orders、positions、balances 一份，`td.evt` 的 `Recon` 也是這個形狀。改成一個請求：
  - `ViewRequest.recon`（`ReconRequest`，沒有欄位）。回覆 `ViewReply.recon`，就是 `td.evt` 的 `Recon`，`reason` 是新的 `RECON_REASON_REQUESTED`，策略分得出是自己問的還是 TD 推的。
  - 三份在 account worker 的 book 裡同一次組出。book 是單一 goroutine，`td.view` 與事件排在同一個佇列，所以三份都在同一個 `ViewReply.seq` 上。
  - 回整個帳號：所有活單、所有持倉、所有餘額。帳號由 subject `td.view.{api_id}` 決定，訊息本體不帶 api_id（與 `td.order` 相同，E09-D006 的權限也以 subject 授權）。
  - 拿掉 `OmsViewRequest`、`LedgerViewRequest`、`OmsView`、`LedgerView`，以及它們在 `ViewRequest`、`ViewReply` 上的欄位 1、2。帳本的 UID 篩選一起拿掉。`leverage` 留下。
- **SDK 以 seq 接上 `td.evt`。** reply inbox 與 `td.evt` 是兩個 subscription，交付順序不保證。SDK 先訂 `td.evt` 再問，回覆到之前收到的事件先扣住；回覆交給 `OnRecon` 之後，只放行 incarnation 相同、`seq` 大於 `ViewReply.seq` 的事件（PROTOCOL §9.6）。
- **TD 不解碼 `client_order_id`。** 原本解碼的三個地方都沒有作用：
  - `Submit`、`Cancel` 驗 `algo_id` 欄位與 cloid 的 algo 欄位一致（104）。兩個值都由同一個 SDK 產生，只驗到 SDK 自己前後一致；誤用別的 session 的 algo 時兩邊一起錯，104 照樣放行。TD 也不知道 algo 屬於哪個 session。
  - 解不開是 106。TD 拿 cloid 當冪等與查單的 key（E06-D005、E06-D017），任何非空字串都能用。
  - OMS 以 algo 篩選。SDK 拿到整本自己篩就好。
- **因此：**
  - 拿掉 `Submit.algo_id`（12）、`Cancel.algo_id`（5）、`REJECT_CODE_ALGO_MISMATCH`（104）。冪等比對（E06-D019）不再含 `algo_id`。
  - 106 拿掉「`client_order_id` 解不開」，改成「`Submit`、`Cancel` 的 `client_order_id` 是空的」（`Cancel` 原本是 104）。106 的其他情況不變。
  - 防線在 SDK 與 conformance：SDK 只用 `AccountDecl.algo_ids` 宣告給該帳號的 algo，cloid 由 SDK 編碼；其他語言的 SDK 由 conformance 驗 cloid 版面（PROTOCOL §9.8）。仍是防呆，不是防惡意（E06-D002）。
  - SDK 以 `cloid.AlgoOf` 把 `Recon.open_orders` 篩成自己的 algo；`OrderUpdate`、`Fill` 也以它判斷是不是自己的。交給策略的事件帶上 api_id：`Recon`、`OrderUpdate`、`Fill` 本身沒有，帳號由 subject 決定。
  - `internal/td` 不再 import `contract/wire/cloid`，由 `internal/archtest` 檢查。
  - 解碼只留在落庫的歸屬（E06-D001、E06-D002，E7）：從 `(api_id, algo, ts_ms)` 對到 session。解不開的單沒有歸屬，與人工單相同。
- **`td.proto` 回到草案。** 它在 E6-15 剛定案（E06-D029），`buf breaking` 的 FILE 規則擋刪訊息、刪欄位。系統尚未上線，比照 E06-D029：放回 `breaking.ignore`，直接刪，不留 `reserved`，不重新編號；E6-17 改完再移出。`client_order_id` 的版面不變，仍是定案。
- 修訂 E06-D002（只能動自己的單、以 algo 查掛單）、E06-D003（第一份 `Recon` 的來源）、E06-D006（OMS 與帳本兩種查詢、帳本依商品篩選）、E06-D010（`LedgerViewRequest`、`OmsView`、`OmsViewRequest`）、E06-D032（`OmsViewRequest` 的 `reserved`）。
- 不採用：保留 `oms`、`ledger`，只加 `recon`。純新增，`buf breaking` 不擋，但同一件事有兩條路，兩份快照的接法仍得寫。不採用：`ReconRequest` 帶 `algo_ids` 讓 TD 篩。篩選只要 cloid 的 algo 欄位，SDK 自己做；TD 少一個解碼的地方。

### E06-D034 `td.proto` 分兩個 PR 移出草案（E6-17，#220）

- **比照 E6-15。** #242 刪項目，照 E06-D033 不留 `reserved`，不重新編號。那一版 `td.proto` 留在 `buf.yaml` 的 `breaking.ignore`，PROTOCOL §1 維持草案。
- **#242 合併後的小 PR 定案。** `td.proto` 從 `breaking.ignore` 移出，`buf breaking` 開始檢查。PROTOCOL §1 改回定案。`client_order_id` 的版面不變，仍是定案。這個 PR 不改欄位。
- 修訂 E06-D033 的「E6-17 改完再移出」。移出清單不是 #242。`buf breaking` 比對的是合併前的 `main`，同一份 PR 裡又刪又移出會被 FILE 規則擋住，與 E06-D029 相同。

### E06-D035 計畫性換線只有事件連線 recon（E6，#200）

- **事件連線的計畫性換線，成功之後跑一次 recon。** 新連線 Ready、舊的關掉，就算換完。不論舊連線在新的還在撥的時候斷了沒有，都不偵測，一律 recon：那段 `serve` 不看舊連線的 `done`，缺口裡的成交與委託變化靠這份快照補上。
- **下單連線的計畫性換線照舊。** 不 recon，狀態不變。
- 換線完成時若已經有一份 recon 在讀快照，那一份不套用。它開始於換線完成之前，補不到這次的缺口；換線自己的那一次才算。
- 代價是每次事件連線換線多一次 REST 快照。重疊的兩條連線可能把同一則事件送兩次；recon 期間到的先排著，快照之後依序重放，相同的不發（E06-D015）。
- 修改 E06-D014 的「計畫性換線不重新 recon」。下單連線仍不 recon。
- 不採用：換線時去看舊連線是不是還在，斷了才 recon。要偵測的窗口就是現在漏看的那個，多一條路徑，而每次換線本來就付得起一次快照。

### E06-D036 換線的 recon 不報 DEGRADED（E6，#200）

- **這次 recon 不報 `DEGRADED`。** 計畫性換線沒有先把事件連線拆掉，帳號維持 `READY`，新單不因這次 recon 被拒。
- recon 完成仍送 `Report(READY)`：每次 recon 都送，因為 `reconciled_at_ns` 變了。中間沒有 `Report(DEGRADED)`。
- recon 期間到的事件照 E06-D015 先排著，不套用、不發出，快照取代狀態、發出 `Recon` 之後依序重放。
- 不採用：換線的 recon 期間先報 `DEGRADED`。連線沒有斷；拒新單擋不住已經在換線窗口裡漏掉的事件，那些靠快照補。

### E06-D037 換線的 reason 沿用 RECONNECT（E6，#200）

- **`Recon.reason` 沿用 `RECONNECT`，不新增 enum。** 對策略來說這和斷線重連是同一件事：一份新快照，接在既有的 `td.evt` seq 後面。這個 incarnation 的第一次仍是 `INCARNATION`。

### E06-D038 成交發在 `td.evt`；目前沒有人落庫（#201）

- **成交由 account worker 發在 `td.evt.{api_id}`。** `Fill`，以及 E06-D001 裡終態且 `filled_qty > 0` 的 `OrderUpdate`，都走這條 subject。
- **目前沒有人落庫。** 由誰寫 Postgres 見 PROTOCOL §10 P18。不會是 TD agent：它不訂 `td.evt`，也不寫這些成交。
- **修訂 E06-D001。** 拿掉「TD agent 訂 `td.evt` 並落庫」。留下兩句：TD agent 不查 SYM；交易所讀取是無狀態服務，比照 md.fetch。queue、cursor 與過濾仍是落庫的規則，執行的人見 P18。誰呼叫 `td.backfill.{venue}`、誰把解開的憑證放進那次請求，同一題，也不會是 TD agent。
- 不採用：把落庫留在 TD agent，只是先不實作。人已經定了不是它。

### E06-D039 TD agent 沒有 NATS user（#201）

- **TD agent 不連 NATS。** 沒有 user JWT，沒有 creds，沒有 inbox。`cmd/mftiko-nats-dev` 不產生 `td-{inst}`，也不再有 `-td-inst`。再跑一次會刪掉以前留下的 `td-*.creds` 與 `user-td-*.nk`。
- **`check` 看到 TD agent 的 user 就失敗，而且在撥號之前。** manifest 的 `td` 必須是空的；creds 檔名不得以 `td-` 開頭；keys 裡不得有 `user-td-*.nk`。磁碟上的 user JWT，名字不得以 `td-` 開頭（`td-account-` 是 account worker，本來就不落檔），subscribe 不得含 `td.evt.>`。
- **account worker 的 `tdAccountPerms` 不變（E06-D011）。** 仍是每個 `api_id` 一份，由 TD agent 以簽章金鑰在 spawn 時簽，經 `Init` 送進去，不落檔。簽發不是 TD agent 自己的 NATS user。
- 不採用：留著 user，只拿掉 publish `sym`。沒有人需要這份憑證。
- 修訂 E06-D011 的 TD agent 欄、E04-D016 的 `_INBOX_td`。`_INBOX_td_{api_id}` 仍是 account worker 的前綴。

### E06-D040 Binance 下單用 `newOrderRespType=ACK`（E6，#217）

- **BinanceUM 與 Binance 現貨的下單一律帶 `newOrderRespType=ACK`**，交易 websocket 與 REST 都是。下單回覆只是收到確認：adapter 回 `OrderStatusNew` 與 order id，不看回覆裡的 `status`。之後的狀態（`ACCEPTED`、部分成交、`FILLED`、`EXPIRED` 等）全部由私有串流（UM `ORDER_TRADE_UPDATE`、現貨 `executionReport`）推動，和 Bybit、OKX 的下單回覆一樣是 `NEW`。
- **契約不變。** `exchange.OrderResult` 不加 `FilledQty`、`AvgPrice`。
- testnet 實測（2026-10-24）：UM 的 ACK 回覆仍寫 `status: NEW`、`executedQty: 0`，市價單已成交也一樣；現貨的 ACK 回覆沒有 `status`。所以原本 `RESULT` 回覆的 `FILLED` 會讓 TD 發出 `filled_qty` 0 的終態並丟掉串流的正確 `FILLED`（#214），改成 ACK 後由串流的 `FILLED` 帶成交量與均價。回覆的 `NEW` 比串流的 `ACCEPTED` 晚到時，依 rank 是舊的，不發；早到時是一則 `NEW`（`ACKNOWLEDGED`）再一則 `ACCEPTED`，各是一次變化（E06-D010）。
- **post-only 會吃單時，兩邊都在下單回覆直接拒絕**，不會先收下再 `EXPIRED`：UM `GTX` 回 -5022，現貨 `LIMIT_MAKER` 回 -2010 `Order would immediately match and take.`。兩者都對到 204；現貨的 -2010 其他訊息仍是 201（餘額不足）。
- Bybit、OKX 的下單回覆本來就只給 order id，回 `OrderStatusNew`，沒有同類問題。
- 不採用：`OrderResult` 加成交量與均價，讓 `RESULT` 回覆的終態帶齊欄位。要改契約，且同一次變化仍會從回覆與串流各來一則。

### E09-D005 linger；需求沒有 TTL

- **需求項目沒有 TTL（E09-D001）。** 原本「STS agent 消失超過 TTL 就回收」的路徑整個拿掉，需求只在 controller 明確刪除時消失。
- **linger 只有一層，MD、TD 同一個概念：** owner 集合歸零後，等 linger 才釋放資源。
  - MD：atom 的 owner 歸零 → linger → 退訂。連線在最後一個 atom 退訂時就停 worker，不另設連線的 linger。
  - TD：帳號的 owner 歸零 → linger → 停 account worker。最後一個需求項目的 linger 就是帳號的 linger。
- **linger 吸收的是 controller 的短暫反覆：** 例如比對時刪掉又馬上加回同一個項目。其他情況都不靠 linger：
  - 期權鏈重新置中：由 orchestrator 先訂新的、再退訂舊的，置中時一直聽 perp 的行情，不會反覆進出。
  - 換參數重開 session：中間的空白無法控制，照規則走，該退訂就退訂。
  - tape 保溫：跑一個 tape keeper 策略（E04-D004），tape 跟著它的生命週期走。
- **預設值：**
  - linger：5 秒。
  - 選主 session 的 TTL：15 秒。`AgentRecord` 綁在它上面（E00-D004），所以它也決定 controller、API 多久之後看到 agent 不在。ack、`WorkerStatus` 不綁 lease，agent 不在時照樣留著，controller 維持現狀（E09-D001），不會因此回收資源。
- 這些值是 MD、TD、STS instance 的部署設定，不放進 session spec。

### E04-D004 沒有常駐訂閱，行情只由策略持有

- MD 的 owner 只有 session 的需求項目。拿掉常駐訂閱（`StandingSubscription`）這個 API，少一個特殊的入口。
- 要讓行情或 tape 一直保持熱的，就跑一個 **tape keeper**：不下單、不綁帳號的策略，跟 mftik 一樣。它和其他策略一樣經 API 開始、結束。
- 使用者的認知只有一條：行情由策略持有，要全部關掉行情，就是把策略全部關掉。
- tape keeper 的需求和其他 session 一樣由 controller 發、不綁 TTL：controller 或 STS agent 停機時，它持有的 atom 不會被釋放。

### E04-D005 `version` 是大版本

- `version`（uint8）是大版本。只有行情 schema 出現 breaking change 時才升。envelope 加任何欄位也算 breaking，要升版本。
- 讀到不認識的版本，讀取端拒收。不靠長度去跳過後代的尾巴：加欄位就是新版本，舊的讀取端不讀。

### E04-D006 v1 envelope 固定 40 bytes

- 長度由版本決定。v1 是 40 bytes，payload 緊接在後面。比 40 bytes 短的訊息拒收。
- **拿掉 `len`。** offset 1 到 7 全部是保留欄位：寫入填 0，讀取端看到不是 0 就拒收。不留一個必須等於 40 的 byte。保留欄位只有一種合法值，讀取端只檢查是不是 0。
- 不採用：用 `len` 跳到 payload，並容忍比 32 bytes 長的後代 envelope。那是加欄位仍相容的做法；E04-D005 把加欄位定義成 breaking。

### E04-D007 little-endian，用 `encoding/binary`

- 欄位是 little-endian，用 `encoding/binary` 讀寫。
- 不用 `unsafe`。NATS 的 `msg.Data` 不保證 8-byte 對齊。
- 編碼與解碼要 0 alloc。`contract` 只依賴標準庫與 protobuf，所以 xxHash64 寫在 `contract/wire/envelope` 裡，不另加函式庫。

### E04-D008 `Append`、`Parse`、`MDTsNs`

- 實作在 `contract/wire/envelope`。`Append` 寫一則 v1（envelope 接 payload），`Parse` 讀一則，`MDTsNs` 只讀 `md_ts_ns`，給 prehook。三個都做 E04-D006 的拒收檢查。
- 附 golden bytes。`Parse` 把 payload 指回輸入的那段，不複製。

### E04-D009 `seq` 與 `conn_epoch` 留在 envelope

- `seq`（uint64）和 `conn_epoch`（uint64）做兩件事。一是傳輸層的連續性：conn worker 到策略之間的缺口，包括 prehook 丟棄和 NATS 掉訊息。二是 tape 和 `md.view` 的對齊（E04-D002）。不必解開 payload 就能檢查連續性。
- 行情本身的連續性另外由交易所在 payload 裡的序號負責（Binance `U`／`u`／`pu`、Bybit `seq`）。兩層並存。
- `seq` 以 `(feed, conn_epoch)` 起算，重連後從 1 重新起算。conn worker 重啟後 `conn_epoch` 要繼續單調遞增。遞增是 conn worker 的事，不在 envelope 的讀寫裡：下一個號先寫進 C4 的 `Report`，寫完才撥，`md.f` 才用它（PROTOCOL §6.2）。

### E04-D010 `digest`

- `digest`（uint64）是只對正規化後的 payload（不含 envelope）算的 xxHash64，seed 0。
- 用途是以後跨連線去重。去重這次不設計也不實作，讀取端不核對 digest，照讀出。
- 寫入的一端一律要算出這個值。`Append` 自己算。

### E04-D011 v1 的位元組

40 bytes，全部 little-endian。8-byte 欄位對齊 8。順序沿用下面這張，因為它已經對齊，不必再調：

| offset | 欄位 |
|---|---|
| 0 | `version` u8 |
| 1 | 保留 7 bytes，填 0 |
| 8 | `md_ts_ns` i64 |
| 16 | `conn_epoch` u64 |
| 24 | `seq` u64 |
| 32 | `digest` u64 |
| 40 | payload |

最終的 offset 也寫在 PROTOCOL §7.1。
### E04-D012 dev NATS 用 operator／account JWT 與 full resolver

- **dev 與之後的部署用同一種授權。** operator JWT、account JWT、full resolver。不用 MEMORY resolver：account 變更不必重載設定，多台 server 也不必改設定。
- **account JWT 在 server 啟動前放進 resolver 目錄。** 檔名是 account 的公鑰加 `.jwt`。不走 `$SYS.REQ.CLAIMS.UPDATE`。這是兩種寫法裡較簡單的一種。
- **resolver 目錄可寫。** compose 把該目錄掛成 volume。種子與 user creds 不掛進容器。
- **要有 system account。** 名稱 `SYS`，只給 resolver 與 server 監控，沒有 user。
- **CI 的嵌入式 nats-server 用同一份設定。** 同一個 formatter 產生 compose 的設定與測試的設定：resolver 種類、operator、`SYS`、`MFTIKO` 相同，listen 位址用測試的自由 port。
- **`max_control_line` 是 262144（E08-D027）。** 產生的 `nats-server.conf` 寫這個值。nats-server 預設 4096 放不下每個 feed 一條的 session JWT。compose 與嵌入式測試都用這份設定。
- **不開 TLS。** 這一票只有一台 dev server。多 region 的拓撲見 PROTOCOL §10 P16。

### E04-D013 金鑰不進 repo

- **小工具 `cmd/mftiko-nats-dev` 產生金鑰，不用 `nsc`。** `make dev-up` 先跑它，再起 compose。
- **輸出在 `deploy/dev/data/nats/`，這個目錄被 gitignore。** 裡面有 operator、`MFTIKO`、`SYS`、簽章金鑰，以及 MD 與 md.fetch 的 user creds。沒有 session 的 user JWT：那是 STS 在 spawn 時簽的（E09-D006）。也沒有 TD agent 的 user（E06-D039）。
- **金鑰檔已存在就沿用，JWT 與 creds 重簽。** 再跑 `make dev-up` 不換 account 身分。種子檔壞了或是空的就失敗，不靜默換一把新的。
- **repo 裡沒有種子。** 公鑰、設定、account JWT 可以出現在本機輸出目錄，不提交（GitGuardian，見 #122）。

### E04-D014 一個 MFTIKO account

- **所有平面共用一個 account `MFTIKO`。** 同一個 subject 空間，沒有 import／export。
- **`MFTIKO` 另有一把簽章金鑰，給 STS 簽 session user JWT，TD agent 簽 account worker 的 user JWT（E06-D011）。** account 的身分金鑰不外流，只留在 gitignore 的目錄。MD 與 md.fetch 的 user JWT 由 account 身分金鑰簽，不由這把簽章金鑰簽。
- **`SYS` 是 system account**，給 resolver 與監控，沒有業務 user。

### E04-D015 MD 與 md.fetch 兩個 user

寫進 PROTOCOL §7。TD 與 SYM 的權限留到它們自己的票，這一票不加（TD 見 E06-D011）。subject 拼法已由 E04-D055–E04-D057 取代；下面是新的字串。請求 subject 仍是恰好 `sym`。

- **MD instance（`MFTIKO_NATS_CREDS`）：** publish `md.v1.f.{inst}.>`、`sym`；subscribe `md.v1.view.{inst}.>`、`sym.v1.changed.>`。conn worker 只訂自己的 venue：`sym.v1.changed.{venue}.>`（E04-D057）；JWT 仍是上面的萬用字元，一個 instance 可以有多個 venue。Init 載入自己那個 venue 的活躍表，變更通知到了就重載（E02-D003、E04-D026）。inbox 見 E04-D016。
- **md.fetch（E04-D002，無狀態，不是 MD worker）：** queue group subscribe `md.v1.fetch.>`（每個 replica 只訂閱部署設定裡它連得到的 `md.v1.fetch.{venue}`，E04-D018）；subscribe `sym.v1.changed.>`（NATS Resolver，E04-D022）；publish `sym`。inbox 見 E04-D016。可 request `sym` 是 publish 恰好 subject `sym`，不是 `sym.>`。恰好 subject `md.v1.fetch` 不能訂。
- **回覆權限。** E04-D016 不給 `_INBOX.>` 的 publish，服務者仍要能回一則。兩個 user 的 JWT 帶 NATS 的 response permission，上限沿用 server `allow_responses` 的預設：一則、兩分鐘。這不是新的決策編號。session 的呼叫者沒有這條，它只訂自己的 inbox。

### E04-D016 inbox 前綴

- **session 維持 E09-D006 的 `_INBOX_{session_id}`。**
- **平台行程用 `_INBOX_{plane}`，不含 instance。** MD 的 plane token 是 `md`，所以是 `_INBOX_md`。md.fetch 的 plane token 是 `fetch`，所以是 `_INBOX_fetch`。歷史補正服務的 plane token 是 `backfill`，所以是 `_INBOX_backfill`（E06-D001）。同一個平面的每個 instance 共用這個前綴。`fetch` 與 `backfill` 是讓前綴不撞，記在這一條，不另編號。
- **權限只開自己平面的前綴**（`_INBOX_md.>`、`_INBOX_fetch.>`、`_INBOX_backfill.>`）。account worker 是 `_INBOX_td_{api_id}.>`，一份憑證一個前綴（E06-D011）。TD agent 沒有 inbox，`_INBOX_td` 沒有 user（E06-D039）。沒有任何 user 拿到預設的 `_INBOX.>`。
- **同一個平面的 instance 可以讀到彼此的回覆。** nats.go 在前綴後面加隨機 nuid，平常的 mux 只訂 `_INBOX_{plane}.<nuid>.*`，所以不會自動收到別的 instance 的回覆，inbox 也不會撞。這把平面憑證仍然可以另外訂閱整個 `_INBOX_{plane}.>`，讀到該平面每一個 instance 的回覆。這是接受的：系統是單人使用，跨 region 取資料是設計目的，因為有些交易所鎖 region，要另一個 region 轉發。
- **`natsx/conn` 加 inbox 前綴選項**，傳給 `nats.CustomInboxPrefix`。空的維持客戶端預設。`natsx/rpc` 的介面不改。

### E04-D017 權限檢查

- **CI 用嵌入式 nats-server 載入同一份設定與產生的 creds**，檢查三件事：發佈 `md.f`；用自訂 inbox 做 request／reply；未授權的發佈與訂閱被拒（另一個平面的 inbox、session 的 inbox、`_INBOX.>`，以及另一個 instance 的 `md.f`）。
- **檢查會在記憶體裡用簽章金鑰簽一個暫時的 session user**，當合法的呼叫者。不把 session creds 寫進磁碟。
- **md.fetch 的 queue 名稱在檢查裡用 `md.v1.fetch.Binance`。** 這是一個 venue subject；natsx 的 `Serve` 在 queue 空著時用 subject 本身（E04-D018）。恰好 subject `md.v1.fetch` 不能訂。JWT 不鎖 queue 名稱。
- **compose 的煙測腳本跑同一組檢查：** `deploy/dev/nats-smoke.sh`。
- **不開 TLS。**

### E04-D018 md.fetch 的 subject 分 venue

- **subject 字串已由 E04-D056 取代。** 現在是 `md.v1.fetch.{venue}`。venue 仍是人讀的名字，與 `sym.v1.changed.{venue}.{category}` 的 venue 相同。下面的分 venue、queue group、不選 region、權限萬用字元仍有效。沒有版號的舊寫法不再用。
- **每個 replica 只訂閱它連得到的 venue。** queue group 的名稱是 subject（natsx `Serve` 在 queue 空著時的預設）。哪些 venue 由部署設定決定，不是全部。
- **程式不寫跨 region 路由。** queue group 優先本地，本地沒有 responder 才轉到別的 region。
- **權限用 `md.v1.fetch.>`。** 這是「每個 venue 一條」與「萬用字元加上部署清單」裡較簡單的一種。行程仍只訂清單裡的 venue。session 的 publish 是這個萬用字元（E09-D006、E04-D056）。SDK 的 fetch 走 `md.v1.fetch.{venue}`，venue 取自 ticker，不選 region。
- **恰好 subject `md.v1.fetch` 沒有 venue，不能訂。**

### E04-D019 K 線分頁與上限

- **`limit` 不在 1 到 5000 回 `invalid`。** 5000 是單次最多的已收盤 K 線。
- **超過 `KlinePage()` 時**，服務用 `Klines(endNs)` 從已拿到的最早一根往回翻。回覆只有已收盤的，由舊到新。某一頁失敗就整筆失敗，不回半頁。邊界那根若又出現，丟掉，不重試。
- **回覆達到或超過 NATS 的 1 MiB 回 `invalid`，不送出。** 5000 根的大小由測試驗證，在這個上限以下。

### E04-D020 限速

- **`exchange.Used` 帶 `ResetAt`。** 這是該計數器歸零的絕對時間。`Usage(resp, now)` 由呼叫者傳入收到回應的時間，adapter 不讀時鐘（E03-D002）。Binance 依 bucket 的窗口對齊下一個邊界：`weight_1m` 與 `orders_1m` 是 1 分鐘，`orders_10s` 是 10 秒，`orders_1d` 是 24 小時，對齊 Unix epoch。回應剛好落在邊界上時，屬於剛打開的那一窗，歸零時間是下一個邊界。Bybit 讀 `X-Bapi-Limit-Reset-Timestamp`（毫秒）。各 venue 真實的窗口與標頭在 E4-09 到 E4-11 驗收時確認。
- **用 `PublicREST.Usage` 記下交易所在回應裡回報的用量。** 這個數字含同一 IP 上的其他人，包括同主機的 conn worker。
- **到 `ResetAt` 用量作廢。** 該 bucket 不再算進閾值，預先拒絕跟著解除。沒有 `ResetAt` 的用量不會自己過期。
- **閾值是設定。** 未設定（0）不預先拒絕：平台不知道交易所的上限。設了之後，任一尚未作廢的 bucket 的回報用量達到閾值，下一筆對該 venue 的呼叫不送出，回 `rate_limited`。
- **429 與 418 用 `RateLimited.RetryAfterMs`。** adapter 從 `Retry-After`（秒）填上。這段時間內對該 venue 的呼叫都不送出。418 是 IP ban：期間內一筆都不送，也不送探測。時間到了才允許下一筆真正的請求。回應裡的 `ResetAt` 比較早也不提前放行。
- **429 沒有 `Retry-After` 時等到回報過的 `ResetAt`。** 有多個 bucket 就等到最晚的那個，讓回報過的計數器都已歸零。`ResetAt` 也沒有，才用可設定的保守預設：行程旗標 `-rate-hold`，預設 1 分鐘。418 沒有 `Retry-After` 時同樣不送出，用同一個後備。
- **不排隊、不重試。** 只送一次。

### E04-D021 不快取

- **這一版不快取。** 每次請求都打交易所。服務維持無狀態。

### E04-D022 商品解析

- **ticker 經 NATS Resolver 換成 `symbol.Info`。** 只查活躍表。不在活躍表的（含已下架、已到期）回 `invalid`。
- **商品的 venue 與 subject 的 venue 不符回 `invalid`。** 請求以 UID 指定商品（E08-D065）：以 UID 查活躍表，查不到或 venue 不符都是 `invalid`。
- **`ErrUnsupported` 回 `unsupported`。** 其他讀取失敗回 `venue_error`。Resolver 暫時不可用不是這四個業務碼，走 natsx 的錯誤回覆。
- **md.fetch user 另外 subscribe `sym.v1.changed.>`**（E04-D057），Resolver 才能收到變更通知。publish `sym` 維持 E04-D015。

### E04-D023 位置與部署

- **程式在 `internal/md/fetch`，執行檔是 `cmd/mftiko-mdfetch`。** 平台服務，跟 SYM 一樣隨平台發版、逐台重啟 replica。不放進 md-agent，不是 worker artifact。
- **連線用 md.fetch 的 user creds（`MFTIKO_NATS_CREDS`）與 `_INBOX_fetch`。** `natsx.Options.CredsFile` 把 creds 交給客戶端。

### E04-D024 重抓節奏

- **快照 HTTP 失敗用指數退避。** 1 秒起，加倍，上限 30 秒，與連線重撥同一條曲線。成功套進本地本的一張把這個窗口歸零。
- **429 與 418 不走這條指數。** `Retry-After` 優先。沒有就用這一回 `Usage` 裡最晚的 `ResetAt`。兩個都沒有，用行程旗標 `-rate-hold`，預設 1 分鐘，跟 md.fetch 同一個預設。這段時間不送出，418 也一樣，不送探測。`-rate-hold` 不在 `InitPayload`。
- **不跟 md.fetch 共用計數。** 各自讀自己那次回應的 `Usage`。conn worker 不設預先拒絕的權重閾值。
- **等快照的緩衝有兩個上限，都在 `InitPayload`。** 0 表示預設 10000 筆、10 秒。任一個到了（含等於）就丟掉緩衝，進行中的那次 Depth 作廢，重新要一張。時間從這一次嘗試的第一筆緩衝更新起算。
- **斷號與 checksum 失敗的第一次重抓不等這段指數。** HTTP 失敗才走。

### E04-D025 簿的訊息佔 seq

- **`orderbook_delta` 是唯一的 book feed。** 每發布一則佔該 feed 的下一個 envelope `seq`。沒有發出去的變化不佔號。
- **`reset` 是一則 `orderbook_delta`（`reset` 為 true），佔這個 feed 的 seq。** 內容是窗內的完整 book（E04-D030）。
- **第一次成功的快照，在把緩衝裡接得上的更新套完之後，也推一則 `reset`。** 重抓同樣。套用緩衝的過程不逐筆推增量。
- **斷號或 checksum 失敗不發布那一筆。** checksum 失敗的那筆不留在緩衝裡重套。
- **重抓不改 `conn_epoch`。** 序號在同一條連線上連續。重連才換 epoch，本地本跟著那條連線丟掉。
- **交易所序號只給 `BookSync.Check`。** 不進 envelope，也不進 `OrderBookDelta`。

### E04-D026 一張快照加增量（SnapshotREST）

- **SnapshotREST 抓一張快照，之後只套增量，不自己重抓。** 靠近盤口的檔更新得快，價格走到某一檔之前，增量已經把它更新過了。
- **重抓只由斷號（`BookResync`）或 OKX checksum 失敗觸發。** 沒有價格邊界，也不因距離重抓。視窗滑動不重抓；窗外的檔見 E04-D031。`md.view` 回的是窗內的 book（E04-D030、E04-D041）。
- **交易所回多少檔就用多少。** REST 的 limit 是 `BookSync.Depth()`（E04-D053）。不因為少於某個檔數而重抓，也不因為想要的檔數大於 `Depth()` 而退出。沒有 N。
- **SnapshotStream 不打 REST。** 全本就是串流給的。斷號或 checksum 丟掉本地本，並只重訂那一個 channel（E04-D034）。下一則串流快照是 `reset`。快照之前緩衝的增量在快照到達時丟掉。
- **`exch_ticker` 來自該 venue 的活躍表。** conn worker 在 Init 載入這張表，並跟著 `sym.v1.changed` 重載（E04-D057）。Agent 只把 `topic.UniversalTicker` 送進 worker 的 reconcile loop；worker 用表上的 `symbol.Info` 呼叫 `Market.Atom`，自己 diff 要訂與要退的 channel。不從 channel 解析。沒有這份表時才用 assignment 上的 atom id 與 `exch_ticker`（測試）。SnapshotREST 沒有這個欄位就不打 Depth，只記日誌。

### E04-D027 `orderbook` 不節流

- **已由 E04-D053 取代。** 沒有 top-N，也沒有 `orderbook` 這個 feed。增量原樣轉成 `orderbook_delta`（E04-D032），沒有另外的節流。推送頻率的上限就是交易所自己的推送頻率；策略跟不上由 prehook 丟（E08-D003）。

### E04-D028 以交易所序號為權威

- **序號正確就套用。** 不另外檢查交叉簿。
- **只有斷號（`BookResync`）或 `BookChecksum` 失敗才因為序號重抓並推 `reset`。** OKX 的 checksum 仍是 E4-11；有實作就呼叫。

### E04-D029 fixture

- **從公開端點錄，不需要 API key。** BinanceUM 的 depth session 加 REST 快照，Bybit 的全本串流。fixture commit 進 repo，測試只重播，不連線。
- **OKX 留到 E4-11。**

### E04-D030 刻度環狀訂單簿

- **本地 book 是 `sdk/book` 的環。** STS worker 與 MD conn worker 用同一份。bids、asks 各一個固定長度的 slice，長度 N，預設 2048，必須是 2 的冪，索引是 `tick & (N-1)`。這一 N 是環長。沒有發布的 top-N（E04-D053）。
- **每一格只存 `tick int64` 與 `size float64`。** 不另存 bool，也不把格子清掉。讀的時候，格子裡的 tick 與要的 tick 不同就是空的；`size == 0` 是刪除。
- **價格換成 tick 用 `round(price / price_tick)`，一半進位遠離零。** 從環上讀回的價格是 `tick * price_tick`。`price_tick` 見 E04-D033。
- **`BestBidTick`、`BestAskTick` 是目前的最佳 tick。** 比最佳價更好、而且 size > 0 的價格改寫最佳。最佳檔被刪掉時，往外掃到下一個 tick 相符且 size > 0 的格子。
- **Iterator 從最佳價往外走，跳過空格，不配置記憶體。** `reset` 與 `md.view` 走完整個窗（E04-D041）。

### E04-D031 窗外的檔

- **離最佳價達 N 個 tick（含）的更新，在遠端那一側，套用時丟掉。** 窗是 N 個連續 tick，最遠是 N-1；再遠一格與最佳價落在同一個索引。
- **同一則更新由最佳價那一側先套。** 每一檔用套到它的那一刻的最佳 tick 判斷在不在窗內。
- **被丟掉的深檔不會因為最佳價後來移近就自己出現。** 格子不清，有效與否只看 tick 是否相符。等下一筆增量再寫進那一格。視窗滑動不重抓（E04-D026 的重抓條件不變）。

### E04-D032 `orderbook_delta` 原樣轉發

- **`orderbook_delta` 轉發正規化後的增量，內容與順序都不改，worker 不先過濾。** 不對整本做 diff。qty 0 仍是刪除。
- **丟不丟只在 `sdk/book` 套用時決定，依那一刻的最佳 tick。**
- **`reset` 是窗內的完整 book。** 第一次成功的快照與重抓都是這則 `reset`。套用緩衝的過程仍不逐筆推增量（E04-D025）。

### E04-D033 `price_tick` 在指派上

- **`price_tick` 是 SYM 的 `price_tick`。** 跟 `exch_ticker` 同一張活躍表、同一時刻。conn worker 在推出 atom 時從記憶體讀。沒有這份表時才用 assignment 上的值。
- **缺，或小於等於 0，這本訂單簿只記日誌、不維護。** 不打 Depth，也不套用串流。

### E04-D034 SnapshotStream 斷號後重訂該 channel

- **Bybit 與 OKX 的全本只在訂閱之後由串流送來。** 斷號或 checksum 失敗把本地本丟掉之後，後面的增量不會再變成一張快照。不重訂的話這本簿停在斷掉之前的窗內狀態，直到整條連線重連。
- **從已確認的訂閱拿掉這個 channel，經現有的訂閱路徑先退訂再訂閱。** 仍由 session 的速率限制發幀。不拆整條連線，其他 channel 不動。`conn_epoch` 不變。
- **每一本簿自己一條指數退避，與 E04-D024 相同：1 秒起，加倍，上限 30 秒。** 成功裝上新的串流快照把這本的窗口歸零，下一次斷號再從 1 秒起。已經在退避或重訂途中的，不另起一次，所以連續斷號不會熱迴圈。
- **下一則串流快照裝上本地本，並推 `reset`。** 快照之前緩衝的增量丟掉（E04-D026）。這一版不打 REST。

### E04-D035 `md.f` 不變，`md.view` 的 tape 不帶 envelope

- **`md.f` 仍是 40-byte envelope ＋ protobuf `FeedEvent`（E04-D005–E04-D011）。** ring 存發佈出去的整段 bytes：發一次、存一次，不重新編碼。
- **`md.view` 回 tape 時去掉每則的 envelope。** 只帶對齊要用的欄位與 `FeedEvent`：`TapeEvent` 是 `conn_epoch`、`seq`、`md_ts_ns`、`event`。`TapePage.events` 是 `repeated TapeEvent`。不帶 `digest`。
- **不重新編碼。** embedded message 與 bytes 的 wire 格式相同。worker 以 protowire 把 ring 裡 offset 40 之後的 payload 原樣寫進 field 4，不 unmarshal 再 marshal。
- **`read.proto` 已在 E4-12 定案（E04-D060）。** 定案之後同一個 major 只做相容擴充。

### E04-D036 tape 分頁，可以用時間查

- **由新到舊。** 游標是 `(conn_epoch, seq)`，字典序。`before` 不含本身。
- **`ViewRequest` 加 `optional int64 since_ns`。** 只回 `md_ts_ns` ≥ `since_ns` 的事件。讀到比 `since_ns` 舊的就停，`more` 為假。可以和 `before` 一起用。
- **要讀完整段就照 `next` 一頁一頁拿。** `next` 是這一頁最舊一筆的游標，當下一頁的 `before`。`more` 為假時不填。

### E04-D037 `TapePage` 帶回覆當下的 high-water

- **`TapePage.head` 是回覆當下這個 feed 已發佈到 `md.f` 的最新 `(conn_epoch, seq)`。**
- **寫進 ring 與發佈 `md.f` 在同一個臨界區。** 先寫進 ring 並發布索引，再發 `md.f`。第一頁最新一則一定 ≤ 該頁帶回的 `head`。讀取端用它讀到的那份索引上的 `head`，不另採一個還沒進這一頁的較新游標。
- **接 live（2026-10-17，E08-D047 改寫 SDK 這一句）：** SDK 不自動要 tape，也不扣住 live。live 立即交付，仍走 prehook。策略要歷史時自己呼叫 `Tape`，把 `(conn_epoch, seq)` 小於等於 `TapePage.head` 的 live 當成重複、大於 `head` 的當成新的。`head` 的定義、寫入順序、空 ring 的 `(conn_epoch, seq 0)` 見上面三條，不改。PROTOCOL §7 與 §9.4 照這條。
- **ring 裡沒有事件時，`head` 是目前的 `conn_epoch`、seq 0。** 留下的事件後來被砍光也一樣。有事件時，`head` 是最後發佈到 `md.f` 的游標。

### E04-D038 每個 feed 的 ring 上限

- **每個 feed 10 分鐘或 100000 筆，先到先砍。** 時間看事件的 `md_ts_ns`，以寫入那一筆時的時鐘往回算。筆數只算事件，不算 gap。
- **整個 worker 總上限 256 MiB。** 超過時砍全 worker 最舊的事件。位元組只算留下的 `md.f` 訊息（envelope 加 payload）。**這一段的位元組與回收由 E04-D058 取代。**
- **三個值放 `mdctl.InitPayload`，可設定，0 用預設。** 欄位與單位見 E04-D049。

### E04-D039 一頁大小

- **`limit` 為 0 時是 1000，上限 5000。** 超過回 `invalid`，不截短。`limit` 只數事件。
- **一頁編碼到約 900 KiB 就截斷，`more` 為真。** 900 KiB 是 900×1024。壓在 NATS 1 MiB 以下，留餘量。大小含這一頁的 `more` 與 `next`，所以送出去的回覆本身不超過這個上限。

### E04-D040 gap

- **`Gap` 有 `start_ns`、`end_ns`、`Cursor last`、`Cursor first`。** `start_ns` 是斷線時的 md 主機時鐘，`end_ns` 是重連完成時的。`last` 是斷線前最後一則已發佈的游標；那之前沒有事件就不填。
- **ring 跨 `conn_epoch` 保留。** 重連後 seq 從 1 起算（E04-D009）。gap 的排序鍵是新 epoch 的 `(conn_epoch, 0)`，夾在舊事件與新事件之間。
- **`first` 是重連後第一則 tape 事件。** 那則還沒進 ring 就不填。它是 gap 下一格的事件，讀取時從索引得出，不回頭改已經寫入的 gap。
- **`continuous_since_ns` 維持：從這個時間起，留下的 tape 沒有 gap。** 還有 gap 時，是最新一個 gap 的 `end_ns`。沒有 gap 時，是留下的最舊一則事件的 `md_ts_ns`。空的是 0。每一頁帶同一個值。

### E04-D041 `BookSnapshot` 只回現在那本

- **不支援指定歷史 seq。** 回最後套用的 `(conn_epoch, seq)` 的窗內全本（E04-D030）。
- **快照上的游標是 `orderbook_delta` 已發佈的 `(conn_epoch, seq)`。** 增量接縫比的是它（E04-D025、E04-D053）。這個 feed 還沒發佈過時，簿還沒同步好。
- **簿還沒同步好時回 `not_synced`。** 快照中、退避中、窗口無效，或這條連線已經斷，都是。`ViewReply.error` 加這個值，不帶 book。SDK 退避重試。
- **讀取不拿連線鎖。** 寫入端在發布之後換上一份不可變的快照；`md.view` 只讀那份。

### E04-D042 依 topic 回什麼

- **book topic 只有 `orderbook_delta`，回 `BookSnapshot`。** 沒有 top-N。`orderbook` 已刪除（E04-D053）。
- **`trade`、`aggtrade`、`liquidation` 回 tape。**
- **其他 topic（kline、best_quote 等）回 `unsupported`。** 錄不錄不看 `record`（E04-D047）。持有這個 feed 就有 responder；沒有 responder 是沒有持有，不是 `unsupported`。

### E04-D043 linger

- **feed 還在指派裡就照常回 `md.view`。** linger 期間 agent 仍留著它（E09-D005）。conn worker 自己不算 linger。
- **feed 從清單移除時退訂 `md.view` 並清掉 ring。** REJECTED 也一樣（E04-D048）。

### E04-D044 subject

- **已由 E04-D055 取代。** subject 是 `md.v1.view.{inst}.{topic}.{ticker}`。`inst` 保留。#181 沒有拿掉它。

### E04-D045 讀取不擋發布

- **`md.view` 在另一個 goroutine 上處理。**
- **讀取完全不拿鎖（E04-D050）。** conn worker 是單一寫入端，只追加。寫完用 atomic 推進「已寫到第幾筆」。讀取端讀索引、讀資料，再以序號確認該段未被覆蓋；被覆蓋就往後縮或重讀。
- **讀取絕不阻擋 websocket 讀取與 `md.f` 的發布。**

### E04-D046 `FeedEnd`

- **已由 E04-D053、E04-D054 取代。** `FeedEnd` 從 proto 與 `exchange.Sink` 拿掉。沒有這個訊息可錄。

### E04-D047 錄不錄由 worker 依 topic 決定

- **`trade`、`aggtrade`、`liquidation` 錄進 tape，其他不錄（E04-D002、E04-D042）。**
- **`AtomAssignment.record` 忽略。** 不刪這個欄位，也不標成 reserved。E4-12 定案時照現狀保留（E04-D060）。

### E04-D048 feed 模式下 ring 的生命週期

- **在 `project()` 換掉 `wanted` 的同一個臨界區裡，依 `wanted` 以 feed key 算 owners，同步 tape。** 鎖外再 rebind `md.view`。訂閱不在持有連線鎖時做。
- **feed 從清單移除，或變成 REJECTED 時，退訂 `md.view` 並清掉 ring（E04-D043）。**
- **在表上但不是 `TRADABLE`（PENDING）時，保留已有的 ring 和 `md.view`，直到 feed 真的從清單移除。** 保留是留下已經有的。從來沒進過 `wanted` 的 PENDING 不新建 ring、也不訂 `md.view`。進了 `wanted` 才建；之後變成 PENDING 就留著。

### E04-D049 一個 feed 一個 ring

- **ring 的鍵是 feed key（`topic.UniversalTicker`），不是 atom。** 每個 feed 各自 10 分鐘或 100000 筆，先到先砍。worker 總上限 256 MiB 不變（E04-D038）。
- **三個值在 `mdctl.InitPayload`。** `tape_window_ms`（uint32，0 是 600000，單位毫秒）、`tape_max_events`（uint32，0 是 100000）、`tape_budget_bytes`（uint64，0 是 256 MiB）。
- **100000 筆只算事件。** gap 佔 ring 的一格，不計入筆數，也不計入位元組。位元組是留下的 `md.f` 訊息長度。超過總上限時，從最舊的事件砍，連坐落在它前面的 gap 一起拿掉。**位元組怎麼算、以及超過總上限時砍的單位，由 E04-D058 取代。** 筆數與「gap 不計入筆數」仍有效。
- **feed 移除或 REJECTED 就清掉它的 ring（E04-D048）。**

### E04-D050 讀取路徑無鎖

- **每個 ring 只由 conn worker 單一寫入，只追加。** 寫完用 atomic 推進「已寫到第幾筆」。覆蓋舊格子之前，先發布新的尾端，讀取端才不會把正在被蓋掉的格子當成還在範圍裡。
- **`md.view` 讀取完全不拿鎖。** 讀 atomic 索引、讀資料，讀完以序號確認該段未被寫入端覆蓋。序號對不上就重讀。仍被覆蓋就往後縮：停在那一格，不把它算進這一頁，`more` 為假。
- **讀取絕不阻擋 websocket 讀取與 `md.f` 發布。** book 的 `md.view` 同樣只讀寫入端換上的快照，不拿連線鎖（E04-D041）。
- **重用一塊 chunk 時，確認改看 chunk 的 generation（E04-D059）。** 先發布新的尾端，再改寫。讀取端以 atomic 字組複製，generation 對不上就重讀。

### E04-D051 建頁成本只跟頁大小有關

- **`before` 與 `since_ns` 在索引上二分搜尋，定位這一頁的起點。** 再往回讀最多 `limit` 筆事件，邊讀邊累加編碼大小，到約 900 KiB 就停（E04-D039）。**先二分 chunk，再二分 chunk 裡的 slot（E04-D059）。**
- **成本是 O(limit)。** 讀取路徑不掃整個 ring，不重算整頁，也不 trim。
- **砍舊資料只在寫入端做。** 時間窗與筆數從最舊的事件拿。worker 的總上限回收最舊的 chunk（E04-D058）。

### E04-D052 只接正式的 feed 路徑

- **tape 與 `md.view` 只接在 `setFeeds` → `project()`。** 錄不錄依 E04-D047。
- **測試走 `setFeeds`，不得手動設 `Record`。**
- **舊的 `apply()` 路徑不接 tape。** 那條路徑沒有 ring，也不訂 `md.view`。

### E04-D053 沒有 top-N，book topic 只剩 `orderbook_delta`

- **`orderbook` 刪掉，不是 `orderbook_delta` 的別名。** 設定或訂閱寫 `orderbook` 回 `invalid`，不自動換成 delta。只有 `orderbook_delta`。
- **`FeedEvent` 拿掉 `OrderBook` 與 `FeedEnd`。** field 4、100 與 `InitPayload.book_depth` 的 `reserved` 已刪除，號碼不重排（E04-D060）。
- **SnapshotREST 抓 `BookSync.Depth()`。** 不再因為 N 大於 `Depth()` 而退出。沒有 N。
- **修訂 E04-D002 的 book、E04-D025、E04-D026、E04-D030、E04-D032、E04-D041、E04-D042。** E04-D027 整條由這條取代。

### E04-D054 `exchange.Sink` 沒有 `FeedEnd`

- **拿掉方法與型別。** 沒有 adapter 發布它，proto 訊息也沒了。留一個空方法會讓每個 Sink 都實作空函數。
- **修訂 E04-D046。**

### E04-D055 行情 subject 是 `md.v1.f.{inst}.{topic}.{ticker}`（P14，#181）

- **保留 `inst`。** 它是 instance 名稱（PROTOCOL §2），一個字串，不綁 region。subject 不另帶 region。兩個 MD instance 持有同一個 atom，subject 不同；session 仍只訂綁定的那個 instance 上宣告的 feed（E08-D018）。`md.view` 仍由該 instance 裡持有這個 feed 的 conn worker 回答，一個 subject 一個 responder。
- **版號放 subject 前段，現在是 `v1`。** `md.v1.f.{inst}.{topic}.{ticker}`、`md.v1.view.{inst}.{topic}.{ticker}`。新舊版 conn worker 並存時，新的發在新的根，舊的繼續發舊的，平台內不做遷移。這是 C6 不相容時換 subject 根。SDK 仍用 binding 給的 subject，不自己組。selector 的前綴是 `md.v1.f.{inst}`，成員再加 `.{topic}.{ticker}`。`md.view` 是把開頭的 `md.v1.f.` 換成 `md.v1.view.`。
- **行情 subject 不帶 venue。** venue 只出現在 `md.v1.fetch.{venue}` 與 `sym.v1.changed.{venue}.{category}`。
- **ticker 不轉義。** UniversalTicker 原樣當一個 token。它不含 `.`（PROTOCOL §2），也不含 `*`、`>`。
- **`topic` 是 `exchange.Topic` 的字串。** 例如 `trade`、`aggtrade`、`orderbook_delta`、`bestquote`、`kline_<interval>`，其餘見 PROTOCOL §2。不另取 subject 用的名字。
- **權限：** session 的行情權限已由 E08-D018 改成每個 feed 一條：publish `md.v1.view.{inst}.{topic}.{ticker}`、subscribe `md.v1.f.{inst}.{topic}.{ticker}`。MD instance 仍是 publish `md.v1.f.{inst}.>`、subscribe `md.v1.view.{inst}.>`。
- **實作在 #194。** 這一條只定拼法。改名、JWT 與 dev creds 不在這張。
- **修訂 E01-D006 的版號位置、E04-D002 的 `md.view` subject。** E04-D044 整條由這一條取代。

### E04-D056 `md.fetch` 的 subject 是 `md.v1.fetch.{venue}`

- **版號同樣放前段。** venue 仍是人讀的名字，與 `sym.v1.changed.{venue}.{category}` 的 venue 相同。
- **沿用 E04-D018 的其餘部分。** 每個 replica 只訂連得到的 venue；queue group 的名稱是 subject；程式不選 region。權限用 `md.v1.fetch.>`，行程只訂清單裡的 venue。恰好 subject `md.v1.fetch` 不能訂。SDK 走 `md.v1.fetch.{venue}`，venue 取自 ticker。
- **修訂 E04-D018 的 subject 字串。** venue 分開與不寫跨 region 路由仍有效。沒有版號的舊寫法不再用。
- 實作在 #194。

### E04-D057 變更通知是 `sym.v1.changed.{venue}.{category}`

- **SYM 只有一個，不帶 `inst`。**
- **保留 venue 與 category。** 刷新單位仍是 `(venue, category)`。conn worker 只訂自己的 venue：`sym.v1.changed.{venue}.>`。MD instance 的 JWT 仍是 `sym.v1.changed.>`，一個 instance 可以有多個 venue，憑證是 instance 的（E04-D015）。session、account worker、md.fetch、歷史補正的 subscribe 是 `sym.v1.changed.>`。
- **請求 subject 仍是恰好 `sym`。** 不是 `sym.>`，也不是 `sym.v1`。決定裡「`md.fetch`、`sym` 同理」落在變更通知這條，以及 `md.v1.fetch.{venue}`（E04-D056）。
- **內容仍是 `Changed{venue, category, revision}`**，用人讀的名字。漏收與重載見 PROTOCOL §7.3。
- **修訂 E2-09 的 subject 字串，以及 E02-D002、E02-D003 裡「P14 定案時可能改名」。**
- 實作在 #194。

### E04-D058 worker 級的 tape arena

- **256 MiB 是這個 worker 全部 tape 記憶體的硬上限。** 訊息 bytes、slot、每筆的固定開銷、chunk，以及每個 ring 的 chunk 指標陣列，都算在裡面。0 仍是 256 MiB（E04-D049）。修訂 E04-D038 的位元組算法，以及 E04-D049「位元組是留下的 `md.f` 訊息長度」。
- **記憶體以固定 64 KiB 的 chunk 為單位，不可設定。** 一塊 chunk 就是 64 KiB：前 128 bytes 是標頭，其餘是 slot 與 payload。slot 從前面往後長，payload 從後面往前長，8 bytes 對齊。一筆事件不跨 chunk。當前這塊的尾巴放不下時，尾巴留著不用，改拿下一塊。
- **耗盡時回收全 worker 最舊的 chunk，O(log n)。** 排序鍵是該 chunk 裡最新一筆的 `md_ts_ns`，相同則比 feed key，再比 chunk 序號。只回收某個 ring 最舊的那一塊，不在中間挖洞。每個 feed 的 10 分鐘與 100000 筆仍在，誰先到用誰。時間窗與筆數仍從最舊的事件砍；整塊 chunk 回到 arena，要等裡面每一筆都已經不在範圍裡。
- **每個 ring 至少留 1 個 chunk。** 最新、還在寫的那一塊不進回收堆，別的 feed 拿不走。沒有可設定的保留額。閒置的 ring 不先配 chunk。自己這一塊放不下、arena 也拿不出新的時，在這塊裡改寫，留下放得下的最新後綴。
- **ring 是固定長度的 chunk 指標陣列，本身也是一個環，不是鏈結串列。** 長度是 `min(tape_max_events, max(1, floor(budget / 64KiB)))`。預設 256 MiB / 64 KiB = 4096 個指標。閒置的 ring 是這份指標陣列，加上對齊用的 ring 本體，小於一個 chunk，而不是預先配好的 100000 格。
- **對齊用的 ring 本體在配出來時就計入上限。** 即使因此超過一個很小的 budget，也照計。chunk 用完回到 free list，不還給 GC，所以上限不依賴回收行程。ring 被移除時，指標陣列的帳先扣掉，那塊記憶體等 GC；在途的讀取仍拿得到這個 ring。

### E04-D059 讀取，與放不下的事件

- **建頁先二分搜尋 chunk，再二分搜尋 chunk 裡的 slot。** `before` 與 `since_ns` 都這樣定位，然後往回讀最多 `limit` 筆（E04-D051）。成本仍是 O(limit)。讀取不拿鎖，也不 trim。
- **無鎖讀取比的是 chunk 的 generation。** 寫入端要重用一塊之前，先發布新的尾端，再把 generation 改成奇數；寫完改回偶數。讀取端讀 generation、以 atomic 字組複製 slot 與 payload、再讀一次。不一致就重讀。指標槽重用時，chunk 序號對不上同樣重讀（E04-D050）。
- **大於 32 KiB 的事件不錄，改記一筆 gap。** 比的是整則 `md.f` 訊息（envelope 加 payload）。剛好 32 KiB 照錄。這筆 gap 的排序鍵是該事件的 `(conn_epoch, seq)`，不是重連的 `(epoch, 0)`。`start_ns` 與 `end_ns` 都是該事件的 `md_ts_ns`。`last` 是前一筆還留著的事件游標；沒有就不填。`first` 照 E04-D040，讀取時從下一筆事件得出。它計入 `continuous_since_ns`。`head` 推進到這個游標，live 不會再套用這筆。`md.f` 仍照常發布。
- **一筆紀錄完全寫不進去時，不推進 `head`。** 連 gap 都沒有 chunk 可放就是這種。ring 是空的話，`head` 維持 `(conn_epoch, 0)`（E04-D037），live 仍能把這筆送出去。
- **修訂 E04-D050 的覆蓋確認，以及 E04-D051 的二分搜尋。** 時間窗與筆數仍在寫入端從最舊的事件拿；worker 上限拿的是最舊的 chunk（E04-D058）。

### E04-D060 定案前刪掉 `reserved`；`mdctl`、`md`、`read` 定案（E4-12，#152）

- **系統尚未上線。** `mdctl.proto`、`md.proto` 裡已刪欄位留下的 `reserved` 直接刪除，不保留。現有欄位號碼不重新編號。`read.proto` 沒有 `reserved`。
- **刪掉的是：**
  - `InitPayload` 的 5 `tape_url`（E04-D002）、8 `book_depth`（E04-D053）、9 `book_boundary_bp`（E04-D026）
  - `Report` 的 5 `atoms`（E04-D026）
  - `Meta` 的 1 `seq`、2 `conn_epoch`、4 `md_ts_ns`（移到 envelope，E08-D003、E04-D009）
  - `FeedEvent` 的 4 `order_book`、100 `feed_end`（E04-D053）
- **這三份定案。** 從 `buf.yaml` 的 `breaking.ignore` 移出，`buf breaking` 開始檢查。PROTOCOL §1 的 `mdctl.proto` 列，以及 `md.proto`、`read.proto`、envelope（§7.1）那一列，改為定案。envelope 的位元組已由 E04-D005–E04-D011 定死。
- **`AtomAssignment.record` 這張不改。** 仍忽略（E04-D047）。
- **修訂 E04-D053 裡「reserved」的說法。**

### E08-D065 每一則行情的 payload 帶 UID，不帶 ticker 字串（E8-01，#222）

- **subject 屬於協議，不屬於訊息。** subject 決定訊息送到哪裡、是哪一種（E04-D055）；從 subject 送來的 payload 才是訊息。SDK 把 NATS 訊息剝開之後，策略拿到的只有 payload：一個策略訂了多個 feed，看不出一則行情是哪個商品。所以商品身分是 payload 的資訊，每一則行情都要帶。
- **用 UID，不用字串。** 系統內部的商品身分是 UID（E02-D003）。行情的 payload 裡沒有 ticker 字串，也不帶 `exch_ticker`。
  - `FeedEvent` 在 oneof 外加 `uint32 uid = 13`。每一種行情都有；`TapeEvent.event` 是 `FeedEvent`，tape 跟著帶。
  - `BookSnapshot` 加 `uint32 uid = 7`。`FetchReply` 加 `uint32 uid = 3`，是請求的商品。
  - `md.fetch` 的請求以 UID 指定商品：`FetchKlines` 的 `string ticker = 1` 換成 `uint32 uid = 4`，`FetchOpenInterest` 的換成 `uint32 uid = 2`。舊號碼不改型別重用。
- **不放進 envelope。** envelope 是給 prehook 只讀前 40 bytes 用的（E08-D003）；加欄位要升 v2（E04-D005），所有讀取端跟著改。prehook 用不到商品。
- **subject 不變。** 仍是 `md.v1.f.{inst}.{topic}.{ticker}` 與 `md.v1.view.…`、`md.v1.fetch.{venue}`，用人讀的名字（E04-D055、E04-D056、P14）。MD 的需求項目與 feed key 仍是 `topic.UniversalTicker`，MD agent 不查 SYM（E02-D002）：那是控制面。
- **誰填：** conn worker 建立 feed 時從活躍表取得 UID，發佈 `md.f`、回答 `md.view` 時填進去。它本來就以活躍表決定訂不訂（E02-D002），UID 在同一列上。UID 由 SYM 分配，冗餘的兩個 conn worker 對同一個商品填同一個值，所以 `digest`（對整個 payload 算，E04-D010）仍一致。md.fetch 以 UID 查活躍表，取得 venue 與交易所寫法。
- **md.proto 的變更是純新增，維持定案。** read.proto 刪了兩個欄位，`buf breaking` 的 FILE 規則會擋；系統尚未上線，比照 E06-D029、E06-D033：放回 `breaking.ignore`，直接刪，不留 `reserved`。移出清單的時點見 E08-D066。
- **控制面不在這一條。** `stsctl.FeedBinding.feed` 是 spec 的宣告名，`Universe.members` 是 selector 成員，都還是字串；SDK 在套用 binding 時以活躍表解成 UID（E02-D003）。成員改成 UID 要由有 SYM 的一方填，是後續。
- 修訂 E02-D002（`md.f` 維持 ticker）、E02-D003（subject 不用 UID 那一條補上 payload）、E04-D022（ticker 的 venue）。
- 不採用：SDK 在訂閱時把 UID 綁在 handler 上、payload 不帶。那是拿 subject 補訊息缺的資訊；tape、回放、其他語言的 SDK 與之後的 wildcard 訂閱都得各自再補一次。

### E08-D066 `read.proto` 分兩個 PR 定案（E8-01，#222）

- **比照 E6-15。** #241 刪 `FetchKlines` 與 `FetchOpenInterest` 的 `string ticker = 1`，照 E08-D065 不留 `reserved`，不把舊號碼改型別重用。那一版 `read.proto` 留在 `buf.yaml` 的 `breaking.ignore`，PROTOCOL §1 維持草案。
- **#241 合併後的小 PR 定案。** `read.proto` 從 `breaking.ignore` 移出，`buf breaking` 開始檢查。PROTOCOL §1 改回定案。這個 PR 不改欄位。
- 修訂 E08-D065「E8-01 改完再移出」的時點：移出清單不是 #241 的完成標準，是 #241 合併之後的後續。

### E09-D006 session worker 的 NATS 憑證：每個 session 一份（`log.sts` 由 E10-D015、E10-D030 拿掉）

- **簽發：** STS agent 持有 NATS account 的簽章金鑰，spawn 時為每個 session 簽一份 user JWT（NKeys）。和其他憑證一樣經 `Init` 的 socketpair 送進去，不放環境變數、不落檔。簽發是本機的密碼學運算，不需要連 server。
- **權限從 spec 推：**

  | | 允許 |
  |---|---|
  | publish | `td.order.{綁定的 api_id}`、`td.view.{綁定的 api_id}`（每個 api_id 一條，E08-D018）、`md.v1.view.{inst}.{topic}.{ticker}`（每個 feed 一條，E08-D018）、`md.v1.fetch.>`（E04-D056）、`sym`（E02-D002）。沒有 `log.sts.{session_id}`（E10-D015、E10-D030） |
  | subscribe | `md.v1.f.{inst}.{topic}.{ticker}`（每個 feed 一條，E08-D018）、`td.evt.{綁定的 api_id}`、`_INBOX_{session_id}.>`、`sym.v1.changed.>`（E2-09 的字串已由 E04-D057 取代） |

  - 行情權限精確到每個宣告的 feed（E08-D018 修改上面的表），不再用 `{inst}.>`。沒有帳號不給 `td.*`，沒有 feed 不給 `md.v1.f`／`md.v1.view`。selector 一期不展開：spec 含 selectors 回 error（E08-D022）。
  - 簽發時檢查 CONNECT 引數（E08-D027）。超過 server 的 `max_control_line`（262144）就拒絕這份 spec，錯誤寫明 feed 過多。檢查在簽發處，不在列出 subject 的函數。
  - reply inbox 每個 session 一個獨立的前綴 `_INBOX_{session_id}`。預設的 `_INBOX.>` 會讓任何連線都能讀到別人的回覆，包括 `td.view` 查回來的帳務資料。
- **分層：** 跨帳號由 broker 保證，策略只能對自己綁定的帳號發 `td.order`；同一個帳號內的 algo 由 TD 檢查（E06-D002，防呆）。
- **不設過期、不做撤銷：** 設過期的話，到期時 server 會斷開連線，重連瞬間 core NATS 的 `td.evt` 可能漏。憑證只在進程記憶體裡，session 結束、進程一死就沒了。
- **不影響下單延遲：** 授權只在建立連線時做一次（`CONNECT` 帶 JWT 與對 nonce 的簽章）。之後每次發佈，server 只拿 subject 比對這條連線的權限快取，訊息本身不帶任何 token。
- MD 的 worker 是平台程式，用所屬 instance 的憑證。TD 的 account worker 每個帳號一份，由 TD agent 簽（E06-D011）。

### E02-D002 UniversalTicker 是佔位，解析之後才知道要訂閱什麼

照 mftik 的 symbol plane（E02-D001）。

- **給人寫的是 UniversalTicker，系統內部是 UID（E02-D003）。** spec 與設定寫 ticker 字串。STS 的 session worker、TD 的 account worker 載入時解成 UID，這兩個 worker 傳這個整數，再用 UID 向 SYM 拿欄位。TD agent 不查 SYM。歷史補正服務反查交易所寫法（E06-D001）。MD 的需求與 `md.f` 的 subject 維持 ticker，conn worker 用 ticker 查活躍表；`md.f` 的 payload 帶那一列的 UID，不帶 ticker 字串（E08-D065）。需求項目、`td.order` 今天仍帶 ticker，換成 UID 是後續。NATS 的 subject 用人讀的名字，不用 UID。確切拼法已由 E04-D055 取代，是 `md.v1.f.{inst}.{topic}.{ticker}`。SYM 表的主鍵是 UID。交易所的寫法（`exch_ticker`）只出現在 adapter 和 SYM。
- **翻譯是查表，經 SYM 解析，不是字串轉換。** `BTCUSDT` 不帶 base／quote 的切法，交易所的寫法也不一定是 base＋分隔＋quote。
- **ticker 的 base／quote 是 SYM 正規化之後的資產（E02-D003）。** 資產代碼一律大寫，再套別名表（例如 `XBT` → `BTC`），只在 SYM。先正規化，再做 quanto 判斷，再推 ticker；strike 先依 `spell_strike` 正規化，`6400.0` 與 `6400` 是同一個 ticker。`USD`、`USDC`、`USDT` 不合併。quanto 不入庫。
- **MD agent 不查 SYM。** 它從 ticker 的 venue，以及 topic 決定的 route，選 `md/<venue>/<route>/<n>`，把 `topic.UniversalTicker` 送進那個 worker。問 route 時只用 ticker 組出的 Info，channel 丟掉，不拿來訂閱。
  - conn worker 在 Init 載入該 venue 的活躍表（E02-D003：不讀終結表），用表上的 `symbol.Info` 呼叫 `Market.Atom`，跟目前的訂閱做 diff。不在活躍表，或不是 `TRADABLE`（quanto 不入庫），這筆就不訂。`EXPIRED` 與 `DELISTED` 不在活躍表。worker 把每一筆 feed 放進 `Report.feeds`：不在活躍表，或這條 route 訂不了，是 `REJECTED`，agent 照寫；在表上但不是 `TRADABLE` 是 `PENDING`。channel 被交易所 ack 之後，掛在上面的 feed 是 `SUBSCRIBED`，worker 在跑時 agent 寫 `READY`。
  - adapter 仍是純函數：不查 SYM、不做 I/O，只用資料就能測。
  - mftik 的 adapter 自己帶 SymbolResolver，在裡面查；mftik refactor 的 B7-01 改成先解析再 `atoms_for`。這裡由 conn worker 先從表拿出 Info，再呼叫 `Atom`。
- **回程帶交易所那一側的 key。** 私有事件、REST 拿回的成交與訂單帶 `(venue, category, exch_ticker)`（venue 見 E03-D001），account worker 與歷史補正服務以 SYM 反查換回 UniversalTicker。TD agent 不反查。category 是 key 的一部分：統一帳戶的 venue 上，同一個寫法可能是兩個商品（Bybit 現貨與 linear 的 `BTCUSDT`）。
- **發布路徑不查 SYM。** conn worker 在 reconcile 時讀記憶體裡的活躍表，表跟著 `sym.v1.changed` 重載。事件上的 UniversalTicker 來自 feed。account worker 每個 ticker 第一次下單時解析一次，之後查記憶體。
- **atom 的身分是解析後的 `(venue, route, channel)`，id 為 `<venue>:<route>:<channel>`。** channel 仍逐字照抄交易所參數；route 從解析後的商品推得（Bybit 依 category），所以同一個 channel 字串在不同 route 上是不同的 atom。
  - 不拿 `topic.UniversalTicker` 當 atom 的身分。一個 channel 常同時服務多個 topic：Bybit linear 的 `tickers.*` 帶 ticker、funding、OI、mark price；Binance UM 的 `markPrice` 帶 mark price 與 funding。以 (topic, ticker) 當身分，同一條連線上會重複訂閱同一個 channel，退訂一個就把其他的斷掉。這是 mftik `MdVenueSubscriptions` 處理過的問題：產品的 refcount 和 wire 上的訂閱是兩本帳。
  - 佔位留在 MD 外面：`md.f` 的 subject 是 feed（`topic.UniversalTicker`），atom 不出現在資料面（PROTOCOL §7）。
- **topic 名稱沿用 mftik：** `bestquote`、`trade`、`aggtrade`、`ticker`、`liquidation`、`funding_rate`、`open_interest`、`greeks`、`kline_<interval>`，加上這裡自己的 `orderbook_delta`、`mark_price`。`orderbook` 已刪除（E04-D053）。表在 PROTOCOL §2。
- **SYM 照 mftik：**
  - 刷新以 `(venue, category)` 為單位，各自刷新、各自下架：現貨的刷新不會讓 perp 下架。
  - 下架與到期不改 UniversalTicker（E02-D003）。狀態還會變的留在活躍表；進了終結狀態就搬到終結表，ticker 字串不變。訂單與 session 仍指向同一個 ticker。
  - 契約見 PROTOCOL §7.3，表見 `migrations/0002_symbol`。
- **誰用 SYM：STS 的 session worker、TD 的 account worker、歷史補正服務、MD 的 conn worker，以及 md.fetch。MD agent、TD agent 與 API 不用。** STS 這端是 session worker（經 SDK），所以它的 NATS 權限有 `sym`（E09-D006）。conn worker 用同一份 MD instance 憑證載入自己的 venue 表。歷史補正服務與 md.fetch 各自有自己的 user，publish 恰好 subject `sym`，subscribe `sym.v1.changed.>`（E04-D015、E06-D001）。
- **啟動時把用得到的 venue 的活躍表載入，之後跟著變更重載（E02-D003）。** 進程啟動時，依 `(venue, category)` 分頁把未終結的列讀進記憶體（每頁最多 500 筆）。Resolver 與熱路徑只讀這張表；終結列不進這份快取。盤中會有新上市、到期與下架，selector 也要挑得到新合約，所以 SYM 在 revision commit 之後發 `sym.v1.changed.{venue}.{category}`（內容 `Changed{venue, category, revision}`，用人讀的名字），收到的進程重載那個 `(venue, category)` 的活躍列。已定，見 PROTOCOL §7.3。subject 字串已由 E04-D057 取代。這一條取代原本的「單一商品精確查詢、用戶端快取 10 分鐘」。
- venue 名稱照 mftik 用 CamelCase：OKX 的 token 是 `Okx`。

### E03-D001 帳號種類和 venue 是兩個 enum

- **venue 是市場，帳號種類是 api_id 是什麼。** 兩個獨立的 enum，名稱可能相同，不可互查。
  - venue：UniversalTicker 的第一段，一條連線的公開行情、一組商品（E04-D003）。`Binance`、`BinanceUM`、`BinanceCM`、`BinanceEO`、`Okx`、`Bybit`。
  - 帳號種類：收哪種憑證、可以交易哪些 venue 的商品，由各交易所的套件在程式裡宣告。`Binance`、`BinanceUM`、`BinanceCM`、`BinanceEO`、`BinancePM`、`Okx`、`Bybit`。
  - 大多數帳號種類只交易同名的那個 venue。
- **BinancePM 是帳號種類，不是 venue。** 它可以交易 `Binance_Spot_BTCUSDT`、`BinanceUM_Perp_BTCUSDT`、`BinanceCM_*`；請求依帳號種類走 papi，再依 ticker 的 venue 選 margin、um、cm 的端點。沒有 `BinancePM_Spot_BTCUSDT` 這種 ticker：保證金模式改的是帳號怎麼算錢、單送到哪，不是商品本身，同一本訂單簿只有一個身分。
  - 帳號宣告（`state.v1.Account`）記的是帳號種類（`kind`），不是 venue。
  - account worker 在 `Submit` 前檢查 ticker 的 venue 在帳號種類的清單裡，不在就以 1xx 拒絕。
  - 交易所回來的事件帶 `(venue, category, exch_ticker)`：一條 PM 的私有連線同時有 margin、UM、CM 的事件，所以 E02-D002 的反查 key 加上 venue。
  - E06-D001 的 backfill 範圍 `(api_id, uid)`、E06-D002 的 algo_id 都不假設帳號只有一個 venue，不用改。
- **BinanceCM、BinanceEO 維持獨立的 venue。** 2026-06 的 CM-UM 整合之後，CM 仍然是 `dapi`、`dstream`，錢包也沒合併；EO 的行情雖然在 `fstream` 上，REST 與帳戶都是自己的。E04-D003 的規則不變。
  - CM 的 `dstream` 沒有路徑分流，只有 `default` 一個 route。
- **UM 和 CM 不共用連線。** 整合之後部分 stream 可以跨 host 訂，但連線的身分仍是 `(venue, route, n)`。全市場陣列在兩個 host 上都會送 UM＋CM 合併的內容，以 `st` 區分，adapter 解碼時丟掉另一個 venue 的列。
- **UM、CM 之間的耦合不寫進 adapter 契約。** 整合之後兩者共用 IP 的 weight 與下單數上限、共用持倉模式（`dualSidePosition`），CM 的 STP 跟著 UM。只記在 Binance adapter 的實作說明。

### E03-D002 adapter 是 sans-I/O 的 codec，技術棧與資料邊界

E3 開工前先定的介面、設計模式與技術棧。各交易所的實作照這一條寫，不各自發明。

- **adapter 不做 I/O。** 它只把意圖變成位元組（訂閱的 frame、簽好名的 `HTTPRequest`），把位元組變成事件（`Session.Decode`、`PrivateStream.Decode`、`Call.Parse`）。不連線、不讀時鐘、不 sleep、不起 goroutine、不重試。擁有連線的 worker 送出 adapter 做好的東西、交回收到的東西。
  - WS 本來就是這個形狀。REST 也改成這樣：每個方法回傳 `Call[T]`，也就是一個 `HTTPRequest` 加上解析它的回應的 `Parse`；分頁的讀取是 `Page[E]`，`Next` 以送出當下的時間產生下一頁的請求，讓簽名的時間戳是新的。`Run`、`Collect` 經 `Doer` 執行。要先打 REST 才能連的 WS（Binance 的 listenKey）拆成 `PrivateStream.Prepare` 回傳的 `Call`，加上 `Dial`。
  - 需要時間的地方（簽名）由呼叫者傳入 `now`。public 的請求不簽名，不帶 `now`。
  - 不採用注入 `Doer` 讓 adapter 自己送：`exchange` 是公開套件（E00-D006），`Doer` 的形狀會把傳輸函式庫的型別帶進介面；而且 adapter 拿得到連線，就擋不住它自己重試。
  - 好處：測試只需要錄下來的 fixture，`Decode` 與 `Parse` 都能 fuzz；重試、重連、限速只在 worker；傳輸可以換，adapter 不動。代價：有狀態的流程要寫成顯式的步驟。
- **技術棧：** WS 用 `github.com/coder/websocket`，REST 用 `github.com/valyala/fasthttp`，JSON 用 `github.com/bytedance/sonic`。coder/websocket 與 fasthttp 只出現在 worker 與 `exchange/transport/*`；sonic 只出現在 `exchange/internal/codec`（adapter 共用的兩份凍結設定）、worker 與 transport。`exchange` 的介面裡沒有它們的型別。
  - sonic 的兩份凍結設定：WS 借用（不複製字串），REST 複製（`CopyString`），欄位名稱都區分大小寫。它碰 runtime 內部，Go 每次升版都要先確認 sonic 跟上了。
  - fasthttp 沒有 context：以 ctx 的 deadline 呼叫 `DoDeadline`，送出後無法中途取消；`MaxIdemponentCallAttempts` 設 1，重試由 worker 決定。HTTP/1.1。
  - coder/websocket 的讀取上限預設 32 KiB，每個 venue 依自己的訊息大小設 `SetReadLimit`；pong 只在有 `Read` 進行時處理，讀的 goroutine 不停。握手走 net/http，proxy、TLS、DNS 與 fasthttp 由同一份設定產生。
- **緩衝區是 worker 的，借給 adapter。** WS 用 `Conn.Reader` 讀進每條連線自己的緩衝區，穩態零分配；交給 `Decode` 的 frame、交給 `Parse` 的 response 只在呼叫期間有效。sonic 不複製字串，解出來的字串可能指向緩衝區：Sink 收到的指標與其中的字串只在回呼期間有效，要留的自己複製。REST 的 `Parse` 一律以複製字串的設定解碼（fasthttp 會重用 response），不值得為它借用。conformance 驗證：回呼返回後把緩衝區塗掉，下游拿到的不變。數字是 float64（E00-D008），不受這條影響。
- **資料邊界：**
  - 商品：進去的是解析好的 `*symbol.Info`，出來的是 `symbol.ExchKey`（E02-D002、E02-D003），adapter 不碰 SYM。
  - 時間：`int64` 奈秒。adapter 只填交易所給的時間（`ExchTsNs`）；收到的時間由 worker 蓋。
  - enum：`exchange` 保留自己的 `Side`、`OrderType`、`TimeInForce`、`OrderStatus`（adapter 不依賴產生碼），數值與 `common.v1`、`td.v1` 逐一相同，worker 以型別轉換映射，`enums_test.go` 鎖住。
  - 錯誤：`Parse` 判讀交易所的回答：`*Reject`（明確拒絕）、`*RateLimited`、`ErrAuth`（簽名、權限、IP 白名單；不重試，告警）。沒有回答的由傳輸判定：請求可能已送出的是 `ErrUnknownOutcome`；`HTTPRequest.Mutates` 的請求（下單、撤單）遇到它，worker 查單或 recon，不當成被拒。`ErrUnsupported`、`ErrNotImplemented` 不變。
  - 限速：每個 `HTTPRequest` 宣告它的 `Cost`；交易所在回應裡報的用量由 `Usage(resp, now)` 讀出，`now` 是收到回應的時間。`Used.ResetAt` 是該計數器歸零的絕對時間（E04-D020）。節流只在 worker。
  - `Account.Leverage` 回 float64（E00-D008 的遺漏）。`Account.Snapshot` 是多個可分頁的 `Call`，worker 全部跑完再合併；`AsOfNs` 由 worker 在第一個請求送出前蓋。
- SYM 讀的是 `exchange.Lister`（整個 category 讀到最後一頁）：真的 venue 是 `ListingOver(PublicREST, Doer, now)`，測試是 `exchangetest.Source`。`exchangetest.Source.REST()` 是它的 sans-I/O 面，`Source` 本身就是回應它的 `Doer`。`mftiko-sym` 以 `exchange/transport/fasthttp` 的 Doer 執行它；proxy 與 TLS roots 是這個行程的旗標。沒有 Doer 時 refresh 回 `ErrNoTransport`。

### E03-D003 WS 下單：TradeStream

Binance 一律以 ed25519 經 WS API 收發單（2026-10-10 定）。E03-D002 的 `Account.Submit`、`Cancel` 回傳 HTTP 的 `Call`，裝不下「在一條 WS 上送請求、以 id 對回答」，所以加一個 codec。

- **`TradeStream` 是 sans-I/O 的 codec（E03-D002）。** `Dial(now)` 產生 URL 與連上之後要送的 frame（logon；連線也帶私有事件時，再加上訂閱）。`Submit(now, id, req)`、`Cancel(now, id, ref)` 把一個請求編成 frame。`Decode(frame, sink)` 把回答以 id 交給 `TradeSink.Submitted`、`Canceled`，交易所回報的用量交給 `Used`。logon 被拒是 `ErrAuth`。
- **id 屬於 worker。** 從 1 起算，同一條連線上不重複；codec 自己的 frame（logon、訂閱）用不會撞號的 id。
- **`Decode` 不靠記憶。** codec 把讀回答需要的東西寫進交易所的請求 id（Binance 收字串：`o17` 是下單、`c18` 是撤單；Bybit 的 `reqId` 是 worker id 的十進位，回答同時帶回 `op`）。這樣斷線重連之後讀得懂遲到的回答，錄下的 session 也能重播（E3-04）。
- **同一條連線也帶私有事件時（`TradeLimits.Events`），worker 不另開 `PrivateStream`。** `TradeSink` 本身就是 `AccountSink`：`Ready` 表示連線可用（已 logon；帶事件時也已訂閱），`Order`、`Fill`、`Balances`、`Positions` 從這裡進來。Binance 現貨是這種；UM 的 WS API 不帶私有事件，另開 `PrivateStream`。Bybit 的 trade socket 也不帶，訂單狀態仍走 `/v5/private`。
- **逾時、重連在 worker。** `exchange.Inflight` 記下送出的請求與期限，純邏輯、時間由 worker 傳入。過期或斷線時還沒回答的請求是 `ErrUnknownOutcome`：訂單可能在、也可能不在，由私有串流或下一次 recon 判定，不當成被拒。過期之後才到的回答不會被當成即時回答，worker 把它當成一則訂單消息處理。
- **`Account.NewTradeStream()` 回傳 codec**；交易所或這個 adapter 不走 WS 下單時回 `ErrUnsupported`，worker 改送 REST 的 `Submit`、`Cancel`。一期 Binance 與 Bybit 的 `Submit`、`Cancel` 走 WS；Okx 用 REST。Bybit 的單筆下單與撤單在 `wss://stream.bybit.com/v5/trade`，連線 auth 與私有串流相同，訂單 frame 不再簽名。`CancelBatch`、`Snapshot`、`Leverage`、`History` 都留在 REST，Binance 以 ed25519 簽名。
- `exchangetest.TradeVenue` 是給 worker 測試用的假交易所：回答順序由測試決定，可以丟掉、拒絕、拒絕 logon。conformance 加 `trade` session、`trade_dial`（logon 與請求 frame 的 golden）、`trade_answers`（每個 id 至多回答一次）。

### E09-D007 控制平面：API server 與 controller

控制平面持有所有進程的即時狀態，並依 MD、TD 的狀態管理 STS。它分成兩個進程：

| 進程 | 寫什麼 | 實例數 |
|---|---|---|
| API server | 使用者的宣告：session spec、帳號。讀狀態給 console | 無狀態，可以多個 |
| controller | 需求項目（E09-D001）、STS 的指派 | 選主，同時只有一個有效 |

- **controller 的職責：**
  1. 從 session spec 推導 MD、TD 的需求項目，寫進 provider 所在 region 的 cluster（E00-D005）。
  2. 為每個非 terminal 的 session 寫 STS 的指派 `/sts/{inst}/demand/{session_id}`：要不要跑、能不能 `Start`、目前的綁定（PROTOCOL §3.3）。session 一開始就發，讓 spawn 與 `OnInit` 和需求的建立並行（E01-D007）。
  3. watch 所有平面的 ack、`SessionStatus`、`AgentRecord`、`WorkerStatus`，作為全域的即時狀態。
  4. 依這些狀態管理 session：
     - 所有 MD、TD 需求都 `READY`，才在指派上放行 `Start`。這就是原本 STS agent 的 Gate。
     - 收到 nack（`REJECTED`、`DEAD`）就結束 session，以 `FAILED` 記下 ack 的原因。
     - 沉默時維持現狀：還沒有 ack，或 provider 的 agent 不在、ack 是最後已知的狀態（E09-D001）。暫時性的降級照 E06-D003 留給策略處理，不停 session。
     - `Start` 之前等超過 `deps_timeout_s`，以 `FAILED` 結束。
     - session 結束時依 E09-D001 的順序撤指派、刪需求。
  5. 每次當選、每次全量 resync 都比對並回收孤兒（E09-D001）。
- **controller 只到資源這一層，不碰進程。** 它寫「session S 需要 feed F」「session S 跑在 STS instance X」，不寫「起哪個 worker」。進程的生命週期只有 agent 碰（E00-D001、E01-D001）。它也不 import exchange、不查 SYM：商品是否存在由 MD、TD 解析後以 `REJECTED` 回報。
- **controller 要 fencing，和 agent 一樣（PROTOCOL §3.2 W2）。** 在 home region 的 cluster 參選，當選後 epoch 加 1，在每一個要寫入的 cluster 放 writer key，之後每次寫入都以它為條件。被取代的舊 controller 寫不進去。home region 是部署設定。
  - instance 名稱固定是 `controller`。名稱可以設定的話，兩個名稱不同的 controller 各有各的 writer key，會同時寫同一批需求；名稱固定，兩個進程即使對 home region 的設定不一致，也會在每個 cluster 搶同一把 writer key。
- **controller 不在交易的路徑上。** 它停機或輪換時，需求與指派原樣保留，跑著的 session 照跑，只有新的起停要等它回來。
- API server 不參選。它的寫入怎麼滿足 W1（例如 spec 只用 create-only 的 txn、帳號以 `mod_revision` 做 CAS），在 E10 定（PROTOCOL §10）。已定，見 E10-D006。

### E09-D008 一期不做跨主機的故障轉移

- session 固定在它的 STS instance 上，帳號固定在它的 TD instance 上，都寫在 key 的路徑裡（PROTOCOL §3.1）。controller 不搬動它們。
- **所以不需要帳號級的 fencing lease。** 同一個帳號只有一個 account worker，由這幾條保證，而且全在同一台主機上：
  - incarnation 先取號再 spawn（E01-D006、PROTOCOL §3.2 W6）；
  - 換 incarnation 一律 delete-before-create（E01-D006）；
  - shim 消失，worker 必死（E01-D004）。
- **之後要做故障轉移，前提是 account worker 先有帳號級的 fencing lease。** 交易所檢查不了 epoch 這類 token，只能靠時間：
  1. 先以 txn 取得帳號的 owner key（綁 etcd lease），才連私有 websocket、才訂閱 `td.order.{api_id}`。NATS 的一般訂閱會讓兩個訂閱者都收到同一筆下單，「誰在訂閱」本身就是 fence 的一部分。
  2. 以本機單調時鐘算「上次續約成功 ＋ TTL − margin」，過了就退訂、停止送單、結束。
  3. 新的持有者要等舊的 etcd lease 過期才取得得到。
  - session worker 不需要自己的 lease：它的單都經過 account worker，而 account worker 檢查得了 token，只接受目前需求裡那個 session incarnation 的單。
- Strategon 的 `sdk/lease` 就是這種 lease：它支援把策略搬到另一台機器，所以需要。

### E09-D009 OCI 函式庫是 oras-go v2（E9-01，#279）

- **推、拉、解析 manifest 都用 `oras.land/oras-go/v2`。** 策略 image 只拿 OCI 當打包格式（E09-D002），資料 artifact 有自己的 `artifactType` 與 mediaType（PROTOCOL §8.4）。oras 是為任意 artifact 的推拉設計的，依賴也小。
- 只在 `internal/platform/artifact` 與 `cmd/mftiko-pack` 裡 import。agent 與 sdk 不直接用它。
- 不採用：`go-containerregistry`。它以容器 image 為中心，自訂 mediaType 與 artifactType 要繞路。

### E09-D010 registry 的位置與設定（E9-01，#279）；策略 image 由 E10-D122 修改

- **一期只用一個 registry，所有 region 共用。** 這條對 MD、TD 的 worker image 仍成立（E10-D126）。策略 image 與 STS 的資料 artifact 改由完整路徑帶 host（E10-D122、E10-D125）。帳密檔的路徑仍寫在部署設定。
- 帳密檔放在主機上，agent 讀它，不進 etcd、不進 spec。
- **dev：** `deploy/dev` 的 compose 加 `registry:2`，不設認證。
- 跨 region 的鏡像與快取等到真的跨 region 部署再定（同 P16）。

### E09-D011 策略只依 digest 拉（E9-01，#279）

- `StrategyRef` 是 `repository` 加 `code`（image manifest 的 digest）。拉取一律用 digest，不用 tag；同一個 session 重拉仍是這個 digest（PROTOCOL §8.1）。
- 拿到之後再驗一次：config 的 mediaType 是 `application/vnd.mftiko.worker.config.v1+json`，manifest 的 `kind` 是 `strategy`。不符就以 `INIT_FAILED` 結束 session，不 spawn。

### E09-D012 `mftiko-pack` 的 `contracts`（E9-01，#279；已由 E09-D035 取代）

- **已由 E09-D035 取代。** 下面留作當時的紀錄。
- **pack 不執行策略 binary（E09-D002）。** 以 `debug/buildinfo` 讀出 binary 連結的 `github.com/BullionBear/mftiko` 版本。
- 版本與 pack 自己相同：`contracts` 填 pack 內建的那一份。這份與 session worker 在 `Hello` 送的是同一個來源：sdk 匯出一個常數，`sdk/plane.go` 與 pack 都用它。
- 版本不同：拒絕打包，除非明確給 `-contracts`。混版時要人決定，pack 不猜。
- `sdk` 欄位的 `version` 也取自 buildinfo。

### E09-D013 artifact 的大小上限（E9-01，#279）

- PROTOCOL §8.3 第 1 步檢查的總大小（所有層加上 config），上限放在部署設定，預設 1 GiB。
- 超過就不下載、不 spawn：策略以 `INIT_FAILED` 結束 session，reason 是 `artifact_too_large`。

### E09-D014 MD、TD 的 artifact 在 E9 只做到 CI 打包（E9-01，#279）

- **E9 做：** CI 在 release 時把 conn worker、account worker 打包成 `md.conn`、`td.account` 的 artifact，推上 registry。
- **E9 不做：** md-agent、td-agent 改成從 registry 拉最新版（PROTOCOL §8.1、§8.3）。移到 E12。E9 的完成標準只看策略，那兩個 agent 已經穩定，改動要另外驗收。
- 在那之前，md-agent、td-agent 照舊執行這個 repo 建出的 binary（E09-D002）。

### E09-D015 GC 在 reconcile 裡做（E9-01，#279）

- 磁碟上的層快取與 rootfs 由 agent 自己清，Go 與作業系統不會替我們刪。
- **不另外排時間。** 每一輪 reconcile 都知道哪些 session 不是 terminal、各自用哪個 digest，就照 PROTOCOL §8.3 第 5 條刪：沒被釘住、也不是目錄裡最新版的 rootfs；沒有 rootfs 引用的層。session 進入 terminal 會觸發 reconcile；agent 啟動後的第一輪也會比對磁碟，上次 crash 留下的殘檔一起清掉。
- reconcile 在一個 agent 裡是單執行緒，GC 不會和同時進行的 spawn 搶同一份層快取，不加鎖。

### E09-D016 STS agent 只照指派 spawn（E9-01，#279）

- 有指派（`/sts/{inst}/demand/{session_id}`）而且 `desired` 是 `RUNNING` 才 spawn。只有 spec、沒有指派的 session 不碰。
- spec（`/sts/{inst}/spec/{session_id}`）只用來組 `InitPayload` 與檢查 algo 重疊（E09-D017）。
- 指派消失一律當成 `ENDED`（PROTOCOL §3.3 N4）。

### E09-D017 algo 重疊：較晚的 session 不 spawn（E9-01，#279）

- STS agent 檢查自己 instance 上非 terminal 的 session（E06-D016）：同一個 `api_id` 上 `AccountDecl.algo_ids` 有交集，就是重疊。
- `issued_at_ns` 較晚的那個不 spawn，直接寫 terminal 的 `SessionStatus`：phase `FAILED`，reason `algo_conflict`。這是設定錯誤，不重啟。`issued_at_ns` 相同時，以 `session_id` 字串較大的那個為較晚。
- 跨 STS instance 的重疊仍由使用者保證（E06-D016）。

### E09-D018 STS 的預設值（E9-01，#279）

- 放在 STS instance 的部署設定。spec 寫 0 時，agent 換成實值再交給 worker（E08-D003、E8-02）。

  | 值 | 預設 |
  |---|---|
  | `recv_window_ms` | 1000 |
  | `Limits.hook_timeout_s` | 10 |
  | `start_timeout_s` | 60，換算成 `Init`、`Start` 的 `deadline_ms` |
  | `Limits.volume_mb` | 1024 |
  | `Stop.grace_ms` | 10 秒，與 shim 自己收到 SIGTERM 時相同（PROTOCOL §4.3） |

### E09-D019 `RestartPolicy`（E9-01，#279）

- `RESTART_MODE_UNSPECIFIED`（spec 沒寫）視為 `ON_FAILURE`。
- `ON_FAILURE` 重啟 `CRASHED`、`HUNG`、不是 agent 要求的 signal；不重啟 `DONE`、`FAILED`、`INIT_FAILED`（E08-D003）。`NEVER` 一律不重啟。
- 退避從 1 秒起、上限 60 秒（E08-D003），用 agent 骨架的 `Backoff`。
- `window_s` 內重啟超過 `max_restarts` 次：寫 terminal 的 `SessionStatus`，phase `FAILED`，reason `restart_limit`。`max_restarts` 或 `window_s` 是 0 時取部署設定的預設，5 次、300 秒。

### E09-D020 no_beat 判 HUNG（E9-01，#279）

- 門檻照 PROTOCOL §9.7：`Init`、`Start` 期間是 `deadline_ms` 加 10 秒；之後是 `hook_timeout_s` 加 10 秒。用 agent 骨架的 `BeatTimeout` 與 `Silent`。
- STS 的 Policy 收到 `Silent` 就經 shim 送 SIGKILL。`ExitInfo` 記 outcome `HUNG`、reason `no_beat`，之後照 E09-D019 決定要不要重啟。
- 不先送 `Stop`：worker 已經不回應，`OnStop` 本來就不跑（PROTOCOL §5.3）。

### E09-D021 `SessionStatus` 的 phase（E9-01，#279）

| 狀態 | phase |
|---|---|
| 有指派，還沒 spawn | `PENDING` |
| 已送 `Init`，`start` 還是 false | `WAITING` |
| 已送 `Start`，還沒收到 `Done(START, ok)` | `STARTING` |
| worker 回報 `Stage(running)` | `RUNNING` |
| 已送 `Stop` | `STOPPING` |
| 退避中 | `RESTARTING` |
| worker 的 outcome 是 `DONE`；或指派是 `ENDED` 而 worker 正常結束 | `DONE`（terminal） |
| 其他 terminal | `FAILED`（terminal），`reason` 寫原因 |

- terminal 的 `reason`：worker 的 outcome 與原因、`algo_conflict`（E09-D017）、`restart_limit`（E09-D019）、`volume_over_limit`（E09-D004）、`artifact_too_large`（E09-D013）、指派的 `end_reason`。
- `observed_generation` 照 W3，是處理過的 spec 的 `mod_revision`。只在 phase 或 `reason` 改變時寫（W7）。

### E09-D022 `Bindings.Version`（E9-01，#279）

- `Version(epoch, seq)`：`epoch` 是 STS agent 的 epoch，`seq` 是這個 session 在這個 epoch 裡的遞增號，從 1 起。
- agent 輪換後 epoch 變大，新 agent 推的快照必然比舊的大；worker 只套用比手上大的（PROTOCOL §6.1），不會倒退。

### E09-D023 session 結束後的清理（E9-01，#279；清理時機由 E10-D016 修改）

- `state_out` 推成功之後（volume 是空的則立刻）只刪 volume 與 inputs。shim 的 log 檔（`${WORK_DIR}/log/sts/{h}.*`）、`terminal.json`、incarnation key 與 `sessions/<id>/` 留到 spec 被刪（E10-D016；第二段順序見 E10-D020）。STS 的 worker key 不會重用。
- `WorkerStatus` 照 agent 骨架的規則，worker 消失就刪，controller 才能照 N4 往下走。清理不擋這一步。
- rootfs 與層由 GC 處理（E09-D015）。

### E09-D024 session 的 JWT 在 `internal/sts/issuer` 簽（E9-01，#279）

- 維持 E8-11 的位置（`internal/sts/issuer`，含 E08-D027 的長度檢查）。epic #11 原本寫的 `internal/platform/natsx` 改掉。
- 簽章金鑰照 E04-D014，與 TD agent 同一把。

### E09-D025 `state_out` 的 tag（E9-01，#279；保留期由 E10-D017 修改）

- 推送與引用都用 digest；`InputDecl` 只認 digest。
- 另外打一個 tag `<session_id>`，給人查找。保留期由 API 管（PROTOCOL §8.4），STS 不刪。

### E09-D026 `state_out` 推送失敗（E9-01，#279）

- 推不成功就保留 volume，以退避重試（1 秒起、上限 60 秒）。
- 不擋 terminal 的 `SessionStatus`：phase 先寫，`state_out` 推成功之後再補寫。controller 的 N4 不等 `state_out`。
- agent 重啟後，從磁碟上還在的 volume 找出未推送的 session，繼續推。

### E09-D027 E9 結束時定案的範圍（E9-01，#279）

- 移出草案清單：`session.proto`、`stsctl.proto`、`state.proto` 的需求、指派、ack 與 `SessionStatus`，以及 PROTOCOL §8 的 artifact manifest。
- 比照 E06-D029、E04-D060：先刪掉定案前的 `reserved`，再從 `buf.yaml` 的 `breaking.ignore` 移出，分兩個 PR。

### E09-D028 錯誤分永久與暫時（E9-02，#280）

- 拉取回傳的錯誤帶 reason，並標明永久或暫時。套件本身不重試；要不要重試、何時以 `INIT_FAILED` 結束，由呼叫端決定。
- 永久的 reason：`artifact_too_large`、`artifact_wrong_kind`、`artifact_platform_mismatch`、`artifact_contracts_mismatch`、`artifact_digest_mismatch`、`artifact_not_found`、`artifact_unauthorized`。401 與 403 是 `artifact_unauthorized`。
- 暫時：registry 連不上，或回應 5xx。

### E09-D029 contracts 比對（E9-02，#280）

- 呼叫端傳入這次需要的契約清單。manifest 的 `contracts` 必須全部包含，多出來的不算錯。缺任何一個就是 `artifact_contracts_mismatch`。

### E09-D030 下載不留半檔（E9-02，#280）

- 層先下載到 `${DATA}/layers/` 下的暫存檔，邊下載邊算 sha256。驗過才 rename 成 `<digest>`；驗不過就刪暫存檔。
- 啟動時清掉殘留的暫存檔。
- 同一個進程裡，同一層的並行請求只下載一次。

### E09-D031 快取不重算 sha256（E9-02，#280）

- `<digest>` 這個檔名只在驗證通過之後才出現。重用時只比對大小，不重算 sha256。

### E09-D032 總大小（E9-02，#280）

- 下載前把 manifest 宣告的各層與 config 的大小加總。超過上限就是 `artifact_too_large`，不下載。
- 下載中實際讀到的位元組不得超過該描述子宣告的大小。

### E09-D033 帳密與 dev registry（E9-02，#280）

- 帳密檔用 Docker config.json 的格式，以 oras-go 內建的 credentials 讀取。basic 與 token 交換都支援。
- 帳密不進 etcd、不進 spec（E09-D010）。
- 部署設定加 `plain_http`，給 dev 的 registry:2。API 與 STS 改為 host 清單（E10-D124）。MD、TD 與 `mftiko-pack` 仍是布林。

### E09-D034 只接受單一 image manifest（E9-02，#280）

- digest 指向 OCI index（多平台清單）時拒絕。不從 index 裡挑一個平台。
- 另記（不在本票，留給 #282／E10）：tag 仍可供人查找。API／CLI 收到 tag 時先解析成 digest，再寫入 `StrategyRef.code`。

### E09-D035 `contracts` 的來源（E9-04，#282；取代 E09-D012）

- **sdk 把 contracts major 清單以可辨識字串嵌入 binary**（例如 `mftiko.contracts=worker.v1,stsctl.v1,md.v1,td.v1`），與 `Hello.contracts` 同源（`helloContracts`）。
- **`mftiko-pack` 從 binary 讀出嵌入清單填入 manifest，不比對 sdk 版本。**
- **Go 策略不提供 `-contracts`。** 讀不到嵌入清單即拒絕打包。`-contracts` 只保留給未來非 Go runtime。
- 理由：手動宣告無法驗證，寫錯時部署檢查放行、要到 `Hello` 才報錯，失去部署前檢查的意義。
- E09-D012 標記為被本條取代。PROTOCOL §8 `contracts` 列同步改寫。

### E09-D036 `mftiko-pack` 的命令列（E9-04，#282）

- `mftiko-pack -bin <binary> -name <name> -repo <registry/repository> [-tag <tag>] [-env K=V ...] [-plain-http]`。
- 推送成功後在 stdout 印出 manifest digest。
- pack 不 build（E09-D002）。

### E09-D037 os／arch 取自 ELF（E9-04，#282）

- 從 binary 的 ELF header 讀取，不靠旗標。

### E09-D038 打包可重現（E9-04，#282）

- gzip header 不寫檔名與時間、壓縮等級固定、檔案 mode 0755、不加 `org.opencontainers.image.created` annotation。
- 同一 binary 與參數兩次打包 digest 相同（要有測試）。

### E09-D039 `sdk.Contracts()`（E9-04，#282）

- 回傳複本，與嵌入 binary 的清單同源，呼叫端改動不影響內部。

### E09-D040 憑證（E9-04，#282）

- Docker `config.json`（oras-go credentials store），預設 `~/.docker/config.json`，與 E09-D033 一致。

### E09-D041 推送前驗證（E9-04，#282）

- push 前先以 manifest 驗證（同 `CheckLaunchFields` 規則），不合格不推。

### E09-D042 與 #284 的分界（E9-05，#283）

- **#283 把 `Plan.Start` 接到指派的 `start`。** 送 `Start`、移出計畫時送 `Stop` 由 agent 骨架負責。`WAITING`、`STARTING`、`RUNNING`、`STOPPING` 在這張可測。
- **#284 才把 spawn 換成 artifact 與完整 `InitPayload`，** 並加 bindings 推送與 `grace_ms`。

### E09-D043 這張 spawn 的執行檔（E9-05，#283；已由 E09-D060 取代）

- **已由 E09-D060 取代。** 下面留作當時的紀錄。
- **照 td-agent 的 `-account`，加 `-worker` 旗標指定執行檔。** 測試用 fakeworker。
- STS 的 `Program.Launch` 回這個路徑。#284 改為 artifact 解出的 `Manifest.Launch`。骨架與 shim 不改。

### E09-D044 有指派、沒有 spec（E9-05，#283）

- **有指派，但沒有 spec，或 spec 解不開：不進計畫、不寫 status，只記 log，等 spec 出現。**
- 多半是 API 與 controller 寫入的先後。

### E09-D045 terminal 不回頭（E9-05，#283）

- **agent 也 mirror 自己的 `/sts/{inst}/status/`。**
- 已經是 `DONE` 或 `FAILED` 的 session 不再進計畫，即使指派還在。`algo_conflict` 同樣不回頭。
- 避免 agent 重啟後重起已結束的 session。

### E09-D046 phase 對應骨架狀態（E9-05，#283）

- `Initializing`、`Initialized` 對到 `WAITING`。
- `Starting` 對到 `STARTING`。
- `RUNNING` 照 E09-D021：收到 worker 的 `Stage(running)` 才寫，不看 `Done(START, ok)`。
- shim 消失（`Lost`）在這張寫 `FAILED`、reason `shim_lost`。是否重啟留給 #285。

### E09-D047 spawn 的 hash（E9-05，#283；由 E09-D097 修訂）

- **以整份 spec 的編碼計算。** `observed_generation` 是 spec 的 `mod_revision`。算法不改：拿掉 `desired_state` 會讓升級後的 agent 在 adopt 時比對不到 hash，把在跑的 worker 換掉。
- **`desired_state` 以外的欄位不應改**（E13-D003、E09-D004）。API 已經擋下。agent 不另特別處理：那些 bytes 仍在 hash 裡，真的不同就照骨架換 incarnation，並記 warn log。
- **`desired_state` 變成 `ENDED` 不是 spec 變更**（E13-D003 允許的那一項，E09-D097）。不換 incarnation。

### E09-D048 部署設定用旗標（E9-05，#283）

- **照 td-agent 用旗標。** registry 位址、帳密檔路徑、資料 repository、`plain_http`（E09-D010）與 artifact 大小上限（E09-D013）先解析，這張不用。
- E09-D018、E09-D019 的預設值也以旗標給，各有預設。

### E09-D049 測試（E9-05，#283）

- **testplane 加行程內 etcd。** 測三條完成標準，另加 agent 重啟後 terminal session 不被重起的測試。

### E09-D050 不用 hardlink（E9-03，#281）

- 層快取存的是 tar.gz，內部檔案無法 hardlink；直接解壓寫檔。
- `artifacts/<digest>/` 內記一份所用層的清單，供 GC 判斷層是否被引用。
- PROTOCOL §8.3 第 3 步「可以用 hardlink 指向層快取」一句同步改寫。

### E09-D051 接受的 tar 項目（E9-03，#281）

- 只接受一般檔、目錄、指向 rootfs 內的相對 symlink、指向 rootfs 內前面已出現檔案的 hardlink。
- 拒絕：絕對路徑、`..`、指出 rootfs 的 symlink（含絕對 target）、裝置檔、FIFO、setuid／setgid、OCI whiteout（`.wh.`）。
- uid／gid 一律忽略，檔案屬 agent 使用者。

### E09-D052 不經過 symlink 寫檔（E9-03，#281）

- 以 `os.Root` 寫入 rootfs，從根本擋住逃逸（例如先 `a → /etc` 再 `a/passwd`），不自己逐段檢查路徑。

### E09-D053 解壓炸彈（E9-03，#281）

- 解壓後總大小上限預設 4 GiB、檔案數上限預設 100,000，皆為部署設定。
- 超過與其他拒絕同樣處理。

### E09-D054 錯誤（E9-03，#281）

- 照 E09-D028 加永久 reason `artifact_unsafe_layer`（STS 以 `INIT_FAILED` 結束）。
- 層的 sha256 正確，不刪快取，交給 GC。

### E09-D055 並行與重用（E9-03，#281）

- 同一 image digest 在同一進程只解一次（同 E09-D030 single-flight）。
- 解到暫存目錄再 rename，rootfs 目錄存在即完整、直接重用不重驗（同 E09-D031）。
- 啟動時清殘留暫存目錄。
- 任一拒絕都整個作廢，不留半個 rootfs。

### E09-D056 GC 介面（E9-03，#281）

- `GC(keep)` 收要保留的 image digest，其餘 rootfs 刪除，再刪沒有 rootfs 引用的層。
- 函式不判斷最新版。「目錄裡的最新版」待 E10 有 API 再定，E9 的 STS 只傳釘住的 digest。
- 正在拉取或解開的 digest 由呼叫端放進 `keep`。
- 暫存檔與暫存目錄 GC 不碰。

### E09-D057 共用解開函式（E9-03，#281）

- 解開函式收一組層與目標目錄，#286 解 inputs 到 `sessions/<id>/inputs/<name>/` 直接呼叫，拒絕規則相同。

背景（E9-06，#284）：骨架的 reconcile 依序執行動作（runner `act`），`fleet.Program` 明訂不做 I/O，所以拉取不能放在 `Launch` 或 spawn 裡。

### E09-D058 背景拉取（E9-06，#284）

- **拉取與解開在 reconcile 之外以背景 goroutine 進行，最多同時 4 個。** 計畫只放 rootfs 已就緒的 session；拉取中 phase 維持 PENDING。永久錯誤以 INIT_FAILED 結束，reason 用 E09-D028／E09-D054 的 reason；暫時錯誤用骨架 Backoff（1–60 秒）持續重試，直到指派消失。

### E09-D059 GC（E9-06，#284）

- **每一輪 reconcile 呼叫 GC(keep)。** keep 是非 terminal session 用到的 digest 加上正在拉取／解開的 digest（E09-D015、E09-D056）。

### E09-D060 拿掉 `-worker`（E9-06，#284）

- **正式路徑一律從 artifact 起（取代 E09-D043 的 `-worker`）。** 測試把 fakeworker 或 hello 打包推到行程內 registry。

### E09-D061 bindings 與版本（E9-06，#284）

- **Start 帶第一份 bindings，seq 為 1；之後 bindings 內容一變 seq 加 1，以 Command 推送。** Start 之前的變化不推送，只更新 Start 要帶的內容。seq 只在記憶體，agent 輪換後 epoch 變大、seq 從 1 重來（E09-D022）。

### E09-D062 每個 session 的 grace_ms（E9-06，#284）

- **骨架 StopGrace 目前是 Fleet 共用值；照 CarriesLive 的做法加可選介面，讓 STS 依 spec 給值，0 用 E09-D018 的 10 秒。** MD、TD 行為不變。

### E09-D063 ENDED 後的 terminal phase（E9-06，#284）

- **end_reason 為 ended_by_spec 且 worker 正常結束 → DONE；deps_failed、deps_timeout、invalid_spec → FAILED，reason 即 end_reason。** 尚未 spawn（例如拉取中）就 ENDED 套同一規則。

### E09-D064 worker 自己結束（E9-06，#284）

- **outcome DONE → DONE；FAILED、INIT_FAILED → FAILED，reason 用 worker 給的；CRASHED、HUNG、signal 本票一律 FAILED、不重啟，重啟留給 #285。**

### E09-D065 測試（E9-06，#284）

- **沿用 #292 測試的假 registry，不加新依賴。** 輪換測試：停掉 agent、起新 agent（epoch 加 1）、改 bindings，確認 worker 套用的版本比舊的大。

### E09-D066 沒心跳直接 SIGKILL（E9-07，#285）

- 骨架的 `Silent` 現在走一般 Stop（`Stop`、grace、SIGTERM、5 秒後 SIGKILL）。照 E09-D062 `grace_ms` 的做法，在骨架加可選介面。STS 因沒心跳停掉 worker 時，直接經 shim 送 SIGKILL（`c3.Conn.Signal`），不先送 `Stop`（E09-D020）。
- MD、TD 不實作這個介面，行為不變。

### E09-D067 `HUNG`／`no_beat` 記在 STS agent（E9-07，#285）

- shim 的 `ExitInfo` 照實記（signal 9，沒有 `Exiting`），shim 不改。
- STS agent 記住因沒心跳殺掉的 incarnation。判斷重啟與寫 `SessionStatus` 時把它當成 outcome `HUNG`、reason `no_beat`。

### E09-D068 分階段的門檻（E9-07，#285）

- STS 的 Policy 依 session 目前狀態算 `BeatTimeout`：收到 `Done(START, ok)` 之前是 `deadline_ms` 加 10 秒；之後（含 Init 完成、等 `start` 的 `WAITING`）是 `hook_timeout_s` 加 10 秒。
- 值都是 agent 解析後的實值（E08-D003、E09-D018）。

### E09-D069 `window_s` 的計數（E9-07，#285）

- STS 的 Policy 在記憶體記每個 session 每次重啟的時間。`window_s` 內超過 `max_restarts` 就以 `FAILED`、`restart_limit` 結束（E09-D019）。
- 退避指數照用骨架的 `Ended.Restarts`（跑滿 `StableAfter` 歸零）。
- agent 輪換後計數從 0 開始（比照 E09-D061 的 seq）。

### E09-D070 shim 掉了不重啟（E9-07，#285）

- 維持 E09-D046：`FAILED`、`shim_lost`。任何 `RestartPolicy` 都不重啟。
- 理由：agent 無法確認舊 worker 進程已經結束，重啟可能變成兩個實例同時下單。

### E09-D071 退避中與重啟後（E9-07，#285）

- 退避期間 phase 是 `RESTARTING`，reason 寫上次結束的原因。
- 新 incarnation 照 W6 先取號再 spawn、走 Init。指派的 `start` 已是 true 就直接送 `Start`。`Start` 帶目前的 bindings。`seq` 在同一 epoch 裡接著加 1，不回到 1（E09-D061）。
- Policy 的退避時間可設定，測試用毫秒級。

### E09-D072 拉資料 artifact（E9-08，#286）

- `internal/platform/artifact` 加拉資料用的函式，檢查 §8.4 的 `artifactType`（`application/vnd.mftiko.data.v1`）、config（`application/vnd.mftiko.data.config.v1+json`）與單一層（`application/vnd.mftiko.data.layer.v1.tar+gzip`）的 mediaType。下載、驗證、層快取、單一下載（single-flight）共用現有程式。大小與解開上限沿用策略 artifact 的設定（E09-D053：4 GiB、10 萬個檔案）。

### E09-D073 GC 保留拉取中的層（E9-08，#286）

- GC 另收一組要保留的層 digest。STS 把正在拉或正在解的 input 層放進去；解開後不再傳入，之後的 incarnation 沿用解開的目錄。
- 除了 input 層，STS 的 puller 在策略 artifact 的 `Pull` 回傳後立即以 `HoldLayers` hold 住 `Fetched.Layers`，`UnpackImage` 結束（成功或失敗）後 release。下載期間的空窗不另處理。

### E09-D074 解開的時機與完整性（E9-08，#286）

- inputs 與 rootfs 在同一個背景拉取裡（E09-D058），共用最多 4 個的上限。rootfs 與所有 inputs 就緒、volume 已建立，session 才進計畫；期間 phase 是 `PENDING`。每個 session 的 inputs 只解一次，用 E09-D057 的共用解開函式先解到暫存目錄再 rename；agent 重啟時目錄不存在就重解。暫時錯誤照 E09-D058 退避重試；永久錯誤以 `INIT_FAILED` 結束、不 spawn，reason 用 artifact 的 reason。

### E09-D075 inputs 唯讀（E9-08，#286；時機由 E09-D096 修改）

- 解完後檔案改 0444、目錄 0555。清理（E09-D023）時先改回可寫再刪。

### E09-D076 量 volume（E9-08，#286）

- 背景每 10 秒量一次（間隔可設定），用實際佔用的磁碟空間（`st_blocks`，同 `du`）。超過 `Limits.volume_mb`（0 取 E09-D018 預設 1024）就用一般 Stop 停掉（有 grace、`OnStop` 照跑），phase `FAILED`、reason `volume_over_limit`，不重啟。

### E09-D077 volume 建立與測試（E9-08，#286）

- volume 在第一次 spawn 前建立，權限 0700；worker crash 或 agent 重啟後沿用磁碟上原有的目錄。測試：fakeworker 寫入 volume 後 crash，重啟的 incarnation 讀得到；input 驗不過以 `INIT_FAILED` 結束且沒有 spawn；volume 超量以 `volume_over_limit` 結束；GC 不會刪掉拉取中的 input 層。

### E09-D078 打包時機（E9-09，#287）

- session 已 terminal 且 worker 進程確定結束才打包，避免打包時 volume 還在被寫。從未 spawn 的 session（例如 `INIT_FAILED`）沒有 volume 或 volume 是空的，直接清理。

### E09-D079 推資料 artifact（E9-09，#287）

- 現有 `artifact.Push` 只給策略 image（整層在記憶體、檢查啟動欄位、憑證固定 `~/.docker/config.json`）。另寫推資料用的函式：層先打包成 `${DATA}` 下的暫存檔再串流上傳；manifest、config、層的 mediaType 照 §8.4；憑證與 `plain_http` 用 STS 拉 artifact 的同一份設定（E09-D033）。tag `<session_id>`（E09-D025）。

### E09-D080 打包內容（E9-09，#287）

- 依路徑排序、mtime 歸零、uid／gid 為 0（§8.4）。只收 #286 解開時接受的項目（E09-D051）：一般檔、目錄、指向 volume 內的相對 symlink。權限只保留執行位元（檔案 0644／0755，目錄 0755）。其他項目（指向外面的 symlink、socket、device）跳過並寫 log，不讓推送失敗。hardlink 當各自獨立的檔案。打包可重現，重推得到同一個 digest。

### E09-D081 空 volume（E9-09，#287）

- 沒有任何一般檔或 symlink 就算空（只有空目錄也算）。空的不推、不寫 `state_out`，直接清理。

### E09-D082 agent 重啟後接著推（E9-09，#287）

- session 進入 terminal 時在 volume 旁寫 `sessions/<id>/terminal.json`（session_id、incarnation、clean）。重啟時掃 `sessions/`，有這個檔且 volume 還在就接著推。`clean` 取最後一個 incarnation 的 `on_stop_completed`；被 SIGKILL 或沒有 `Stopped` 一律 false。

### E09-D083 `state_out` 欄位與重試（E9-09，#287）

- `size_bytes` 是推上去的層的壓縮後大小；`at_ns` 是推送成功的時間；`digest`、`repository`、`clean`、`incarnation` 照 §8.4。推送失敗照 E09-D026 以 1–60 秒退避，一直重試到成功，volume 保留，不擋 terminal phase。

### E09-D084 清理順序與測試（E9-09，#287；由 E10-D016 修改）

- 推送成功（或空 volume）且 worker 已結束後，第一段依序刪 volume、inputs（先改回可寫，E09-D075）。第二段在 spec 不在之後，順序見 E10-D020：shim 的 log 檔（`${WORK_DIR}/log/sts/{h}.*`）、以 `Incarnations.Forget` 刪 incarnation key、`terminal.json`，最後刪掉整個 `sessions/<id>/`。測試：下一個 session 以 `state_out` 當 `InputDecl` 讀到同樣的檔案；registry 停掉時 volume 保留、恢復後推上並補寫 `state_out`；agent 重啟後接著推；spec 還在時 log、`terminal.json`、incarnation key 與 session 目錄留著，spec 刪掉之後主機上沒有該 session 的殘留。

### E09-D085 release 觸發（E9-10，#288）

- 推 `v*` tag 觸發 release workflow，另外加 `workflow_dispatch` 供手動補推。

### E09-D086 registry（E9-10，#288；2026-10-20 就地改寫）

- registry 是 `ghcr.io/bullionbear/`。workflow 只用 `GITHUB_TOKEN` 的 `packages: write` 推送，不負責設定可見度。新 package 預設 private，API 也改不了 org package 的可見度，所以推第一個 `v*` tag 之前，由 org owner 在 BullionBear org 設定把 Package creation 預設改成 Public（只需設一次）。這個前置步驟寫在 [release.md](release.md)。

### E09-D087 repository 與 tag（E9-10，#288）

- repository 是 `mftiko-md-conn`、`mftiko-td-account`。tag 用 git tag（例如 `v0.1.0`），另外再加 `latest`。E12 的 agent 以 `latest` 解析成 digest（這一期不改 agent）。

### E09-D088 mftiko-pack 加 kind（E9-10，#288）

- 產物是 OCI artifact：單層裝靜態 `bin/worker`，沒有 base OS，不是可 `docker run` 的容器鏡像，格式和策略一樣（PROTOCOL §8）。
- `-kind strategy|md.conn|td.account`，預設 `strategy`，預設行為不變。`md.conn`／`td.account` 用單層 `bin/worker`，不經過 SDK 嵌入路徑。

### E09-D089 contracts 與 sdk（E9-10，#288）

- 非策略 kind 的 `contracts` 由 pack 依 kind 填固定清單。conn worker 填 `worker.v1`、`mdctl.v1`，account worker 填 `worker.v1`、`tdctl.v1`，以實際實作的契約為準。`sdk` 留空。

### E09-D090 平台與 PR 驗證（E9-10，#288）

- 一期只打包 `linux/amd64`（E09-D034 不收 OCI index）。一般 PR 只打包、不推正式 registry。CI 起 `registry:2` service container，推上去後依 digest 拉回來驗證。

### E09-D091 selector 的型別留在草案，到 E5（E9-12，#290）

- **E9 定案 `session.proto`、`state.proto`、`stsctl.proto`（E09-D027），但其中 selector 的型別還是暫定的。** `SelectorDecl`、`OptionChain`、`RollingFuture` 的形狀等 E5-01（#208）的解析方式；`Universe.current` 尚未定；`Universe.members` 從 UniversalTicker 改成商品 UID 是已排定的後續（E02-D003），改型別是 breaking。
- **搬到各自 package 的 `selector.proto`，三份新檔留在 `buf.yaml` 的 `breaking.ignore`，到 E5 定案：**
  - `mftiko/session/v1/selector.proto`：`SelectorDecl`、`OptionChain`、`RollingFuture`。
  - `mftiko/state/v1/selector.proto`：`Universe`、`SelectorState`。
  - `mftiko/stsctl/v1/selector.proto`：`SelectorBinding`、`Universe`。
- package 與型別名不變，產生的 Go 型別只換了檔案，程式不用改。
- 定案檔裡引用這些型別的欄位（`SessionSpec.selectors`、`DemandEntry.selector`、`EntryStatus.universe`、`Bindings.selectors`）照常受 `buf breaking` 檢查：欄位不能改號、改型別名；型別的內容在草案檔裡，E5 可以改。
- 不採用：整份照原樣定案，E5 時再照 E06-D033 放回草案。E5 要改這些型別是已排定的，不是假設，那樣一定要多走一輪。

### E09-D092 定案前刪掉 `session.proto` 的 `reserved`（E9-12，#290）

- 比照 E06-D029、E04-D060：系統尚未上線，定案前的 `reserved` 直接刪掉，欄位號碼不重新編號。
- 刪掉的是：`SessionSpec` 的 15 `algo_id`（E06-D002），`FeedDecl` 的 3 `delivery`（E08-D003）。`state.proto`、`stsctl.proto` 沒有 `reserved`。
- 移出草案清單是下一個 PR（E09-D027）。

### E09-D093 artifact manifest 的相容規則（E9-12，#290）

- manifest（`mftiko.worker.v1`）與資料 artifact 的 config（`mftiko.data.v1`）是 JSON，`buf breaking` 檢查不到，規則寫在 PROTOCOL §8.2。
- 同一個 `schema` 之內只能新增欄位，缺席要保持舊的意思；讀的一方忽略不認得的欄位（`encoding/json` 的預設，`internal/platform/artifact` 不開 `DisallowUnknownFields`）。
- 不相容的變更換 `schema` 字串；讀的一方只認得自己的 `schema`，其他一律 `artifact_bad_manifest`。不在同一個 `schema` 裡做版本協商。
- 不另做機械檢查：manifest 只有 `mftiko-pack` 一個寫的人，靠 review 與 `internal/platform/artifact` 的測試守住。

### E09-D094 C3、C4 定案（#302）

- `shim.proto`（C3）與 `worker.proto`、`common.proto`（C4）移出 `breaking.ignore`，PROTOCOL §1 標為定案。原本寫「E1 定案」，E1（#3）關閉時漏了。
- 三份沒有定案前的 `reserved`，所以不需要 E06-D029、E04-D060 的第一個 PR，一個 PR 移出清單。
- 定案前檢查過：沒有標示暫定、留給其他 epic 的欄位。`Hello.pid` 由 `contract/wire/c4` 填入，平台目前不讀，留著供除錯，不刪。
- `common.proto` 的 `Side`、`OrderType`、`TimeInForce`、`ExitInfo`、`Condition` 早已被定案的 `td.proto`、`md.proto`、`state.proto` 引用；從這裡起，改它們也受 `buf breaking` 檢查。

### E09-D095 結束在重啟策略判斷之前不寫 terminal（#327）

- **worker 結束之後，`SessionStatus` 要等重啟策略判斷過這個 incarnation 的結束，才寫 `RESTARTING` 或 terminal。** 還沒判斷的寫 `STOPPING`。
- 原因：runner 每一輪以一份觀測做 Step（策略在這裡判斷），執行完動作之後，再取一份新的觀測交給 Sync 寫 status。worker 在兩次觀測之間結束，Sync 就看到策略還沒判斷的結束；原本照結束本身寫成 `FAILED`，而 terminal 不回頭（E09-D045），第一次 crash 就不會重啟，違反 E09-D019。agent 重啟後的第一次 Sync 在任何 Step 之前，在 agent 停機期間 crash 的 worker 必定如此。
- 策略記下它判斷的是哪個 incarnation（`Restart` 時從 fleet 的觀測取得），Sync 比對觀測到的 incarnation。下一輪 Step 一定會問策略，之後照 E09-D019、E09-D067、E09-D069 寫。
- 只在 STS 自己的策略生效時適用。測試換掉策略時，phase 照結束本身寫（原本的行為）。
- runner 不改：MD、TD 也用它，Sync 拿到動作之後的觀測是刻意的。

### E09-D096 inputs 在 rename 之前就唯讀（#327）

- **`artifact.UnpackReadOnly` 在暫存目錄解完之後、rename 成 `inputs/<name>/` 之前，就把檔案改成 0444、目錄改成 0555。** 正式位置一出現就是最終的權限，沒有 0644 的空檔。修改 E09-D075 的時機，權限不變。
- 原本是 rename 之後才改。spawn 等改完才開始，正式行為沒錯，但測試在空檔裡讀到 0644（#327）。
- 暫存目錄與正式位置在同一個父目錄，rename 一個 0555 的目錄不需要它本身可寫（Linux 只在換父目錄時要求）。
- 失敗時清掉暫存目錄改用 `RemoveWritable`：唯讀的樹不能直接 `RemoveAll`。`MakeReadOnly`、`RemoveWritable` 從 STS 搬到 `internal/platform/artifact`，STS 改用它們。
- rootfs 不受影響，照舊用 `Unpack`。agent 重啟時目錄已在就再 `MakeReadOnly` 一次，涵蓋舊版程式在 rename 之後、改權限之前掛掉的情形。

### E09-D097 spec 已是 `ENDED` 時不再 spawn（#433）

- **`decide` 以 spec 的 `desired_state` 為準。** 它已是 `ENDED`，而 demand 仍是 `RUNNING`：當作 ended，`kindEnded`，`EndReason` 是 `ended_by_spec`，不出 plan。incarnation 走一般 Stop（`not in demand`），不 replace、不再 spawn。terminal 之前仍佔著它的 algo，跟 demand 已經是 `ENDED` 一樣。
- 起因：session end 只 CAS spec 的 `desired_state`，demand 稍後才跟上。hash 含 `desired_state`（E09-D047），reconcile 看成 spec 變了就換一個 incarnation；demand 變成 `ENDED` 之後再以 `not in demand` 停掉。終態寫在 release 之後，觀測已經是空的。
- demand 已經是 `ENDED` 的，仍用它自己的 `end_reason`。`deps_failed`、`deps_timeout`、`invalid_spec` 不因 spec 也是 `ENDED` 改成 `ended_by_spec`。
- 修改 E09-D047：`desired_state` 變成 `ENDED` 不是 spec 變更。hash 仍是整份 bytes，不改算法。`desired_state` 以外的欄位不應改，API 已擋，agent 不另特別處理。
- PROTOCOL 不變。

### E09-D098 終態保留最後一個 incarnation 的 exit（#433）

- **`noteExitLocked` 一併存 `ExitInfo`。** 寫 `SessionStatus` 時 worker 已經不在觀測裡（release 之後 `obs == nil`），用這份紀錄補 `incarnation` 與 `last_exit`，包含 `on_stop_completed`。觀測還在時仍用觀測，不用這份紀錄蓋過。
- 沒有紀錄時不補：尚未 spawn 的終態 `incarnation` 是 0、沒有 `last_exit`（E09-D063）。
- `terminal.json` 的 `clean` 仍是這份 exit 的 `on_stop_completed`（E09-D082）。seal 之後不改 `clean`，也不丟掉已存的 `ExitInfo`。

### E10-D002 API 的契約：OpenAPI，`api.v1` 自成一體（E10-01，#306）

- **console 與 CLI 只認 API 自己的契約。** `api.v1` 不 import `session.v1`、`state.v1`：API server 把自己的請求型別轉成 `SessionSpec`、`Account` 寫進 etcd，讀取時反向轉換，轉換配對照測試，逐欄位比對。內部的 proto 可以演進，對外不動。
- **以 OpenAPI 3.0.3 寫在 `contract/openapi/api/v1.yaml`，先寫規格再實作。** 3.1 的 Go 產碼支援還不完整，所以用 3.0.3。
- **編碼是 JSON，是 PROTOCOL §1「編碼一律是 protobuf」的例外。** 相容性改由 `oasdiff breaking` 對 main 檢查，規則比照 §1：路徑帶 major（`/api/v1`），同一個 major 內只做相容擴充，讀取端忽略不認得的欄位。lint 用 Redocly。
- **不受 `selector.proto` 的草案狀態牽制（E09-D091）。** 一期 API 不收 selectors（E08-D022），E10 結束時定案（E10-D018）。
- 不採用：proto 加 Connect。前端拿到的會是平台的 proto 型別，`api.v1` 也得 import `session.v1`，要等 E5 才能定案。
- 不採用：import `session.v1`。少一層轉換，但內部的宣告一改，對外的 API 就跟著改。

### E10-D003 產碼範圍（E10-01，#306）

- **Go：** `oapi-codegen` 產 strict server 的介面與型別，以及 client，放在 `contract/gen/api/v1`。`internal/api` 實作 server 介面；client 給 E11 的 CLI（#13）與端到端測試。
- **TypeScript：** `openapi-typescript` 產型別，`openapi-fetch` 當 client，放在 `web/src/lib/gen/api`。
- **前端不再產 proto。** `buf.gen.yaml` 拿掉 `protoc-gen-es`，`web/package.json` 移除 `@bufbuild/buf`、`@bufbuild/protoc-gen-es`、`@bufbuild/protobuf`，刪掉 `web/src/lib/gen/mftiko/`。proto 只剩 Go 的平台內部契約，`buf` 照舊管。
- `make` 一次產出 Go 與 TypeScript，產物提交；CI 檢查漏產與 `oasdiff breaking`。工具版本釘在 `bin/tools`。

### E10-D004 錯誤模型與分頁（E10-01，#306）

- **錯誤一律是 `application/problem+json`（RFC 9457）。** 欄位 `type`、`title`、`status`、`detail`；驗證錯誤另帶 `errors: [{path, reason}]`，`path` 是請求 JSON 的欄位路徑，console 據此標在對應的欄位上。
- **狀態碼：** 400 驗證錯誤；401 未認證；404 不存在；409 並行衝突（create-only 或 CAS 不成立，E10-D006）以及狀態不允許（刪非 terminal 的 session、刪仍被引用的帳號）；503 該資源所在的 region 不可用（E10-D014）。
- **列表以游標分頁：** 請求帶 `limit`（預設 100，上限 1000）與 `cursor`，回應帶 `next_cursor`，沒有下一頁就省略。游標對呼叫端不透明。

### E10-D005 串流用 SSE（E10-01，#306）

- **狀態的即時更新走 SSE（`text/event-stream`）。** 單向、低頻、要能續傳，SSE 是普通的 `GET`，寫得進 OpenAPI，走同一套認證與稽核中介層。
- **event id 是 etcd revision。** 跨 region 的串流（例如列出所有 session）帶每個 region 各自的 revision，格式 `<region>=<rev>[;<region>=<rev>…]`。重連時瀏覽器帶 `Last-Event-ID`，API 從各 region 的 revision 接著送（W4 的規則），連到任何一個副本都接得上。某個 region 已經 compacted，就送一則 `reset`，client 重新 list。
- 每 15 秒送一行註解當心跳。
- **部署要求 HTTP/2。** HTTP/1.1 下每個網域最多 6 條連線，開幾個分頁就會卡住。
- 認證用 cookie（E10-D009）：瀏覽器的 `EventSource` 不能帶 header。
- 事件的 schema 寫在 OpenAPI 的 `components`，前端標註型別，測試確認一致。
- 不採用：WebSocket。OpenAPI 只描述得了握手，連線後的訊息格式要另外定義；續傳、心跳要自己設計。之後 market 頁若要在同一條連線上切換訂閱，或推高頻資料，再另外決定。

### E10-D006 API server 的寫入（P8）（E10-01，#306）

- **API server 不參選，可以多個副本，全部用 etcd 使用者 `api`。** RBAC 只讓 `api` 寫 `spec/`、`account/`，並依 W5 刪 `status/`（PROTOCOL §3.2 W1），所以 W1 以角色為單位成立。副本之間的競態由 txn 的條件擋：
  - **建立 spec、建立帳號：** create-only，`create_revision == 0`。
  - **結束 session：** 讀出 spec，只改 `desired_state`（E13-D003），以 `mod_revision` CAS 寫回。
  - **更新帳號：** 呼叫端帶讀取時拿到的 `revision`，以 `mod_revision` CAS 寫回。
  - **刪除 session（W5）：** 一個 txn 刪 spec 與 status，條件是兩者的 `mod_revision` 仍等於讀到的值，而讀到的 status 是 terminal。terminal 是吸收態，所以條件成立就安全。
  - **刪除帳號：** 以 `mod_revision` 為條件刪，前提見 E10-D011。
- 條件不成立回 409（E10-D004），由呼叫端重新讀取。
- 不採用：API server 也參選。API 的寫入都是使用者觸發的一次性動作，不像 controller 要持續推導，txn 的條件就夠了。
- PROTOCOL §3.2 加上 W10，§10 的 P8 標為已定。修改 E09-D007 最後一條。

### E10-D007 `session_id` 隨機配號（E10-01，#306）

- **API 以 `crypto/rand` 產生 6 個小寫 hex。** 寫入前先查 API 的副本（E10-D014），所有 STS instance 都沒有這個 id 的 spec；再以 create-only 寫入自己的 key。撞到就換一個，最多重試 5 次。
  - 副本的檢查不是原子的。兩個請求同時抽到同一個 id、又寫進不同的 STS instance，才會漏掉；機率約 1/1,677 萬，接受。
- **不再要求對所有歷史唯一，只保證存活與保留期內唯一。** W5 刪掉 spec 之後，舊 id 可能被新 session 抽到。依據：預期每年 10,000 個以內、同時存活 200 個以下；隨機配號第一年預期重用約 3 次。
- **重用只影響歷史，不影響運作。** 刪 spec 之前 controller 已依 N4 刪完需求，STS 主機上的東西在 spec 刪除後清掉（E10-D016），所以新 session 對 controller、STS、MD、TD 都是全新的。仍以 `session_id` 為鍵的歷史有三處：
  - registry 上 `state_out` 的 tag（E09-D025）：重用會蓋掉舊 tag，舊資料只剩 digest 找得到。tag 要不要改成帶時間另議，E09-D025 不變。
  - algo 的持有紀錄（E06-D002）：同一個 id 出現兩段，以時間區間區分。
  - 稽核紀錄（E10-D010）：同上。
- 不採用：etcd 計數器。歷史上永遠一對一，但計數器只能放在一座 cluster，多一個 key 和 CAS 迴圈；以上面的量，重用的代價不值得。
- 不採用：tombstone。key 只增不減，建立時要跨 region 檢查。
- PROTOCOL §2 的 `session_id` 一列改寫。

### E10-D008 `api_id` 的配號（E10-01，#306）

- **API 從 1 到 2⁵³−1 隨機抽一個整數。** 上限讓 JavaScript 的 number 表示得精確，console 不必把它當字串。寫入前先查副本，所有 TD instance 都沒有這個 `api_id` 的帳號；再以 create-only 寫入自己的 key。
- 帳號數量是幾十到幾百，2⁵³ 的空間下，連刪除後重用都可以忽略。
- 由 API 配發，使用者不能指定：`td.order.{api_id}` 等 subject 要全域唯一，人工指定容易撞。
- 不採用：計數器。理由同 E10-D007。

### E10-D009 認證：每人一個靜態 token，單一角色（E10-01，#306；CSRF 的 Content-Type 由 E10-D110 修改）

- **每個使用者一個 token。** API 的部署設定列出 `name` 與 token 的 SHA-256，設定檔裡沒有明文 token。換 token 就改設定、重啟 API。
- **只有一個角色：通過認證就能使用所有資源。** API 內部不再分唯讀、操作、管理。
- **CLI 與自動化：** `Authorization: Bearer <token>`。
- **console：** `POST /api/v1/auth/login` 帶 token，API 回一個 cookie：`HttpOnly`、`Secure`、`SameSite=Strict`，內容是 `{name, exp}` 以 HMAC-SHA256 簽章；金鑰在部署設定，所有副本共用，所以哪個副本簽的都能驗。預設 12 小時到期。`POST /api/v1/auth/logout` 清掉。SSE 用同一個 cookie（E10-D005）。
- **CSRF：** console 由 API 同源服務。cookie 是 `SameSite=Strict`，寫入另要求 `Content-Type: application/json`，HTML 表單送不出這種請求。
- 部署一律走 TLS；dev 例外。
- 不採用：OIDC。之後要接公司的身分系統再換，中介層的介面不變。
- 不採用：mTLS。瀏覽器的使用體驗差。
- 不採用：角色分級。目前的使用者都需要完整權限。

### E10-D010 稽核：中介層寫 Postgres 的 `audit` 表（E10-01，#306）

- **每個請求在回覆時，由中介層寫一列到 Postgres 的 `audit` 表。** health 與 readiness 不記。
- **欄位：** 時間、使用者（未通過認證時為空）、method、path、query、狀態碼、耗時、request id、來源位址、目標（`session_id` 或 `api_id`，有的話）、寫入請求的 body、錯誤的 `detail`。
- **body 去掉憑證。** 帳號的憑證欄位在寫入 `audit` 之前移除，其他欄位照記（E06-D013）。回應的 body 不記。
- **SSE 在串流結束時記一列**，耗時就是連線時間。
- **寫不進去不改變回應。** 動作已經做完，中介層記 error log 與計數器，照常回覆。
- 用 SYM 同一個 Postgres，表由 `migrations/` 新增的 `0003_audit` 建立，`mftiko-migrate` 套用。API 的部署設定加 DSN。
- 一期不刪 `audit` 的列。
- `SessionSpec` 不加建立者欄位：誰建的從 `audit` 查，已定案的 proto 不動。

### E10-D011 帳號（E10-01，#306）

- **封裝改由 API 做（E06-D013）。** 每個 TD instance 的公鑰寫在 API 的部署設定，格式與 `mftiko-seal pubkey` 的輸出相同。API 以 `internal/platform/sealbox` 封裝，`api_id` 與 `kind` 一起封進去；明文只在處理那個請求的記憶體裡。
- **憑證只寫不讀。** 讀取帳號回傳 `api_id`、`kind`、TD instance、`AccountSettings` 與密文的指紋，不回傳憑證；回應、log、錯誤訊息都不含明文或密文。
- E10-D063 修改「讀取帳號回傳密文的指紋」：讀取不回傳指紋、明文或密文。
- **一期接受的帳號種類：** E03-D001 的種類裡，只交易一個 venue 的那些。交易不只一個 venue 的（一期只有 BinancePM）回 400，與 account worker 的 `kind_unsupported` 同一條規則（E06-D014）。
- **刪除仍被引用的帳號回 409。** 依 API 的副本，任何非 terminal 的 spec 引用這個 `api_id` 就拒絕。檢查不是原子的：漏掉時，TD 對那個需求寫 `REJECTED`（帳號不存在），session 以 `deps_failed` 結束，接受。
- 更新用 CAS（E10-D006）。換憑證就是重新封裝、整份寫回。

### E10-D012 API 的驗證範圍（E10-01，#306）

- 建立 session 時檢查：
  - `algo_id` 在 1–1023，同一個 spec 內同一個帳號不重複。
  - **跨 session 的 `algo_id` 重疊做 best-effort 檢查：** 依 API 的副本，同一個 `api_id` 上已有非 terminal 的 session 用了同一個 `algo_id`，回 409。檢查不是原子的，漏掉的仍照 E06-D016 由使用者保證，同一個 STS instance 上的由 E09-D017 擋。修改 E06-D016。
  - `md_instance`、`td_instance` 在 topology 裡，而且平面正確。API 讀 `mftiko-etcd-roles` 同一份部署 JSON，和 controller 一樣（E13-D007、E13-D012）。controller 的 E13-D004 照舊保留，因為 topology 可能在 spec 寫入之後才改。
  - 含 selectors 回 400（E08-D022）。
  - 同一個 spec 重複的 feed、帳號回 400。
  - 策略：tag 先解析成 digest（E09-D034），digest 在 registry 存在而且 `kind` 是 `strategy`。`contracts` 的相容由 STS 在 spawn 前檢查（E09-D029），API 不重複做。
  - `inputs` 的 digest 在 registry 存在，而且是資料 artifact（PROTOCOL §8.4）。
- **不查 SYM（E02-D002）。** ticker 寫錯由 MD、TD 解析後以 `REJECTED` 回報，session 以 `deps_failed` 結束；console 顯示 ack 的原因（E10-D014）。

### E10-D013 策略目錄（E10-01，#306）；由 E10-D118、E10-D119、E10-D122 修改

- **以 registry 為準，API 不另外存。** 修改 E09-D002「API 的策略目錄記錄 `name → digest`」。
- 策略的 repository 清單寫在 API 的部署設定：registry 不一定支援列出 repository（ghcr 不支援 `/v2/_catalog`）。版本是各 repository 的 tags list；manifest 與 config 照 PROTOCOL §8.2。
- **「目錄裡的最新版」是 `latest` tag 指到的 digest，** 和 MD、TD 的 artifact 一致（E09-D087）。
- STS 的 GC 依此保留最新版（E09-D056），不在 E10 的範圍，和 agent 以 `latest` 解析（E09-D087）一起移到 E12。在那之前 STS 照舊只保留釘住的 digest。
- **E10-D118、E10-D119 修改目錄的形狀。** 不再有 repository 清單。`allow` 是前綴或單獨的 `*`。列出是 `GET /strategies?repository=`。建立時讀 manifest。GC 仍用 `latest` tag 的 digest，上一條不變。E10-D122 起 `*` 是任何 host，repository 含 host。

### E10-D014 讀取端（E10-01，#306）

- **每個 cluster 一個 `Mirror`（W4），比照 controller 的 E13-D017–E13-D021。** 讀取一律從副本回答，不在請求裡直接讀 etcd。
- **region 不可用：** 尚未完成首次同步，或和該 cluster 斷線超過 15 秒（同 E13-D018 的門檻，可設定）。列表回傳其他 region 的結果，並以 `unavailable_regions` 標明；只在該 region 的單一資源回 503。
- **controller 不在：** `/ctl/controller/agent` 不存在時，回應帶 `controller_absent`，console 顯示「狀態可能過期」的橫幅。
- **session 的檢視合併：** spec、`SessionStatus`、`SessionDemand` 的 `end_reason`，以及每個需求項目 ack 的狀態與 `REJECTED` 的原因。
- **失敗的原因要留下來。** controller 依 N4 刪掉需求之後，ack 就不在了，`SessionStatus.reason` 只剩 `deps_failed`（E09-D063）。所以 STS 寫 terminal 的 `SessionStatus` 時，把結束那一刻 `SessionDemand.bindings` 裡 `REJECTED`、`DEAD` 的項目各寫成一個 `Condition`：`type` 是 `dependency`，`status` 是 false，`reason` 是 ack 的 reason，`message` 是 feed key 或 `api_id` 加上 provider instance。打錯 ticker 的 session 在 spec 刪除之前都看得到原因。
- **log 的位置：** STS instance、主機、`${WORK_DIR}/log/sts/{h}.stdout` 與 `.stderr`（`h = WorkerHash("sts/<session_id>")`）。`AgentRecord` 加兩個欄位 `host`（`os.Hostname()`）與 `work_dir`，由 agent 當選時寫入；API 從它算出路徑。`state.proto` 已定案，加欄位是相容擴充（PROTOCOL §1）。
- **`td.evt`（訂單與成交）一期不給 console。** API 不連 NATS。

### E10-D015 log：一期不接 `log.*`，運維上機器讀（E10-01，#306）；由 E14-D010 修改

- **一期沒有 log 的資料流。** worker 的 stdout、stderr 照 PROTOCOL §4.5 S4 由 shim 寫檔，運維到 console 顯示的主機與路徑讀（E10-D014）。
- **拿掉 `log.*`。** session JWT 的 `log.sts.{session_id}` 從 publish 清單刪除（修改 E09-D006、E08-D018），PROTOCOL §7 刪掉 `log.{plane}.{id}` 一列。目前沒有發佈者：SDK 的 log 寫 stderr。之後要做即時 log 再加回來。實作在 E10-14。
- **log 檔讓運維不必 sudo 就能讀：** shim 寫的 stdout、stderr 與 agent 建立的 `{h}.shim.log`（PROTOCOL §4.6）改成 0640，目錄 0750。group 由部署決定：`${WORK_DIR}/log` 設 setgid 並指定 group，新檔沿用。修改 PROTOCOL §4.5 S4 的 0600、0700。
- **MD、TD 的 log 不變**，照 S4 依 worker key 累積、輪替。不再出現的 worker key 留下的檔案，以及不輪替的 `{h}.shim.log`，記為 E12 的磁碟風險。
- 之後要接即時 log，候選是每台主機一個 shipper 讀 shim 的檔案、送到 JetStream：來源涵蓋所有平面、所有語言與 crash 輸出，stream 的序號也能當 SSE 的 event id。
- **E14-D010 修改（#436）：** 檔案仍是唯一來源，權限與「沒有 `log.*`」不變。STS agent 另開 HTTP 端點，API 依 `AgentRecord.log_addr` 讀 session worker 的 stdout、stderr（契約 L）。不是把 `log.*` 加回來。上面的 JetStream shipper 不是這條路。

### E10-D016 STS 的 log 保留到 spec 被刪（E10-01，#306）

- **清理拆成兩段，比照 `docker rm`。** 修改 E09-D023、E09-D084：
  1. **terminal 之後：** 照舊推 `state_out`（E09-D025、E09-D026）。推成功（空 volume 則立刻）只刪 volume 與 inputs。
  2. **spec 不在之後：** 第 1 段完成，而且 `/sts/{inst}/spec/{session_id}` 不存在，才刪 `terminal.json`、shim 的 log 檔（`${WORK_DIR}/log/sts/{h}.*`，含 `{h}.shim.log`）、incarnation key 與 `sessions/<session_id>/`。
- **level-triggered。** 每輪 reconcile 判斷；agent 重啟後 list 一次，磁碟上有、etcd 裡沒有 spec 的 terminal session 照樣清得掉。只清本機確認過 terminal 的 session，和 E09-D044（有指派、spec 還沒出現）區分開。
- spec 在 terminal 之前被刪（N3 `ended_by_spec`，E13-D003）：照常結束、推完 `state_out` 再清。
- **保留期只有一處：API 依 W5 刪 spec 的時機（E10-D017）。** STS 端一期不另設上限。
- 實作在 E10-02。

### E10-D017 W5 的保留期（E10-01，#306）

- **預設 7 天，寫在 API 的部署設定。** 從 terminal 的 `SessionStatus.updated_at_ns` 起算。
- 每個副本都會掃，用 E10-D006 的 txn 刪 spec 與 status，重複刪除無害。
- **使用者也可以從 API 立即刪除一個 terminal 的 session。** 非 terminal 的回 409：要結束 session 就把 `desired_state` 改成 `ENDED`。
- 刪除寫進 `audit`（E10-D010）。
- **registry 上的資料 artifact 一期不刪。** 修改 E09-D025 與 PROTOCOL §8.4「保留期由 API 管」。新 session 可以在保留期之後以 digest 引用舊的 `state_out` 接手，隨 spec 一起刪會讓接手失敗。registry 的容量由運維管理。

### E10-D018 E10 結束時定案的範圍與拆票（E10-01，#306）

- **定案：** `contract/openapi/api/v1.yaml`。E10-20 以 main 上的版本設為 `oasdiff` 的基準，之後同一個 major 只做相容擴充。在那之前是草案，CI 只跑 lint，不擋不相容的改動。
- **拆票：** E10-02 到 E10-20，即 #307 到 #325，依賴已連好。E11（#13）只依賴 E10-04（#309）的 Go client。

### E10-D019 判斷 spec 不在（E10-02，#307）

- 只看 reconcile 手上的 spec 檢視，不另外對每個 session 打 etcd Get。agent 啟動後要等第一次完整 list 同步完成，才開始第二段清理，避免剛重啟、快取還是空的時候誤刪 log。

### E10-D020 第二段刪除順序（E10-02，#307）

- 依序刪 shim log 檔（`${WORK_DIR}/log/sts/{h}.*`）、incarnation key、`terminal.json`，最後刪 `sessions/<id>/`。`terminal.json` 是「本機確認過 terminal」的記號，留到最後刪，中途 crash 下一輪可以接著清，也不會跟 E09-D044 那種從未 spawn 的目錄混淆。

### E10-D021 權限不受 umask 影響（E10-02，#307）

- log 檔和目錄建立後，再明確 `Chmod` 成 0640/0750。三種 agent 都適用（shim 與 `internal/platform/agent/fleet`）。

### E10-D022 舊目錄（E10-02，#307）

- shim 每次啟動都把 `${WORK_DIR}/log/{plane}` `Chmod` 成 0750；已經存在的舊 log 檔不追溯改。

### E10-D023 設定分三類（E10-05，#310）；由 E10-D119、E10-D122 修改

- `-in` 讀 `mftiko-etcd-roles` 同一份部署 JSON（E10-D012、E13-D012）。
- `-config` 讀 API 專用 JSON：使用者 `name` 與 token SHA-256（E10-D009）、策略 allow 清單（E10-D119，修改 E10-D013）、`registry_creds` 與 `plain_http_hosts`（E10-D123、E10-D124；不再有 `registry`）、各 TD instance 公鑰（E10-D011）、W5 保留期（E10-D017，預設 7 天）、region 不可用門檻（E10-D014，預設 15 秒）、監聽位址。
- 秘密值走環境變數，比照 controller：etcd `api` 密碼、cookie HMAC 金鑰、Postgres DSN。

### E10-D024 範圍（E10-05，#310）

- 這張票讀進並驗證全部設定，但不連 Postgres、不連 registry；endpoint、認證、稽核、W5 刪除留給後續票。readiness 只看 etcd。
- E10-D048 修改「不連 Postgres」。API 啟動時建立連線池，但不等它連上。readiness 仍只看 etcd。Postgres 不可用只影響稽核。不連 registry 與「W5 刪除留給後續」不變。
- E10-D119 的 `allow` 在 Load 檢查形狀與前綴，仍不連線。

### E10-D025 每個 cluster 一個 Mirror（E10-05，#310）

- 每個 cluster 一個 `Mirror`，監看整個 keyspace（`api` 角色可讀全部），不拆前綴。

### E10-D026 readiness（E10-05，#310）

- 任一 region 可用，`/readyz` 回 200，body 列出每個 region 的狀態與 `unavailable_regions`；全部不可用才回 503。`/healthz` 只看進程存活。

### E10-D027 優雅關閉（E10-05，#310）

- SIGTERM 後 readiness 先轉 503、停止接新請求、等進行中請求，最多 10 秒（可設定），以 0 結束。

### E10-D028 靜態檔（E10-05，#310）

- `-web` 指向 `web/build`，從磁碟讀、不嵌入 binary；非 `/api/` 且找不到檔案的路徑回 `index.html`。未設 `-web` 只提供 API。

### E10-D029 TLS（E10-05，#310）

- 有 `-tls-cert`、`-tls-key` 跑 HTTPS 加 HTTP/2（SSE 需要，E10-D005）；沒有則明文 HTTP，只給 dev。監聽預設 `:8080`，與 `make web-dev` 的 proxy 一致。

### E10-D030 `log.*` 善後（E10-14，#319）

- session JWT 刪除 `log.sts.{session_id}`（E10-D015，修改 E09-D006、E08-D018）。`internal/sts/permissions.go` 與權限測試同步，conformance bus 不再用的 `logSubject` 一併刪除。
- conformance `-violate subject` 改把單送到 `td.view.{api_id}`：session 有權限送、但不是下單 subject，單不會出現在 `td.order`，檢查仍判不合格。不再依賴權限擋下。
- PROTOCOL §9 `-violate subject` 那一列改為 `td.view.{api_id}`。

### E10-D031 session 的歷史表：Postgres 與 etcd 一致（E10-03，#308）

- **Postgres 加一張 `session_records`，存每個 session 的 spec 與結局。** etcd 仍是控制面的權威來源（E00-D004）：controller、STS 只看 etcd。`session_records` 是持久的紀錄，範圍比 etcd 大：W5 刪掉的 session 還在。
  - 理由：W5 之後 spec 與 status 都從 etcd 刪除，「哪一次執行、用哪個版本與參數、怎麼結束」就查不到了；`orders`、`fills` 經 `algo_ownership` 只對得到一個 `session_id`，而它可能重用（E10-D007）。
- **不變式：**
  1. etcd 裡每個 spec，Postgres 都有一列。
  2. 內容相同：紀錄仍存 etcd 那份 spec 的原始 bytes 與 SHA-256。比對以 `internal/api` 的 `specContentEqual` 為準（排除 `desired_state`），與程式一致。只有其餘欄位不同才算內容不同。
  3. 結束、terminal、W5 刪除最後都會反映到 Postgres。
- E10-D072 修改不變式 1：改為「經 API 建立的 spec，Postgres 都有一列」。etcd 有 spec 而 Postgres 沒有列時只告警，不補列，建立者不記為 null。
- **寫入順序**（PROTOCOL §3.2 W10）：
  - 建立：先寫 Postgres（`pending`，含建立者、建立時間、解析後的 spec），再以 create-only 寫 etcd，再標 `created`。中途失敗留下的是沒有 session 的 `pending`，不會是沒有紀錄的 session。
  - 結束：先 CAS 改 etcd，再更新 Postgres。只改了 etcd 時由 reconcile 補。
  - W5 刪除：先寫 Postgres（最後的 status、結束時間、`state_out` digest，並關閉這列），再以 txn 刪 etcd。只寫了 Postgres 時由 reconcile 重刪。
- **reconcile：** 每個副本跑一個 level-triggered 的迴圈比對 mirror 與 Postgres：etcd 有、Postgres 沒有就補一列，建立者記為 null；內容不同（`specContentEqual` 為假，排除 `desired_state`）就告警；`desired_state` 與 terminal 狀態補同步；`pending` 逾時標 `abandoned`；已關閉而 key 還在就重刪。寫入都是有條件的 upsert，冪等，不需要選主。
- E10-D072 拿掉「etcd 有、Postgres 沒有就補一列，建立者記為 null」。reconcile 只記 error log 與計數器，不補列。
- **主鍵是代理鍵（UUID）。** `session_id` 在未關閉（`pending`、`created`）的列之中唯一，以部分唯一索引保證：同時存在的 id 不重複（E10-D007），刪除時關閉，之後再抽到同一個 id 是新的一列。
- **Postgres 停機時：** 建立回 503（`storage_unavailable`），不寫 etcd；正在跑的 session 不受影響。結束照常（etcd 先）。W5 刪除暫停，主機上的 log 跟著多留一段（E10-D016）。
  - 不採用：Postgres 停機時照樣建立、事後補紀錄。那段期間的建立者會遺失。
- **讀取：** 即時狀態來自 etcd 的副本；`created_by`、`created_at` 與已刪除的 session 來自 `session_records`（`GET /session-records`）。
- migration、建立流程與 reconcile 在 E10-11（#316），刪除流程在 E10-12（#317），讀取在 E10-13（#318）。
- 不採用：寫進 spec 的保留 label（例如 `mftiko/created-by`）。只解決建立者與時間，解決不了 W5 之後的歷史。
- 不採用：只靠 `audit`（E10-D010）。`audit` 記的是請求原樣，不是解析後的 spec，也沒有結局。

### E10-D032 instance 由使用者指定（E10-03，#308）

- **`sts_instance` 與每個 feed 的 `md_instance` 由使用者在請求裡指定，必填。** API 只驗證它在 topology 裡、平面正確（E10-D012）。
- 理由：任何 STS instance 都能跑任何策略，任何 MD instance 都能連 exchange registry 裡的任何 venue（`mftiko-md-agent` 只有覆寫網址的選項，沒有 venue 清單）。選哪一台是部署位置的取捨：延遲，以及交易所依地區封鎖連線。topology 只有 region 與平面，API 沒有東西可以「解析」。選到連不上的 MD instance，錯誤在 conn worker 連線時以 ack 回報，console 從 session 的依賴看得到。
- **`td_instance` 不必填**，由帳號紀錄帶入：`api_id` 全域唯一（E10-D008）。
- `session.proto` 的 `FeedDecl.md_instance` 註解「API 解析後一律填」改成「API 驗證後填入」。它會改變產生的程式碼，在 E10-04 重新產碼時一起改，PROTOCOL 的 proto 區塊同時改。
- 之後要自動選，在部署 JSON 加明確的規則（例如每個 STS instance 預設用哪個 MD instance），不讓 API 猜。

### E10-D033 `params` 是 JSON object（E10-03，#308）

- `params` 是策略自己的參數，平台不解讀（`SessionSpec.params`）。它不是 MD、TD、STS 的設定：那些是 spec 的其他欄位。
- **API 收巢狀的 JSON object**，存成緊湊的 JSON bytes 交給策略（`ctx.Params()`）。不收單一的數字或字串，方便日後讓策略的 manifest 附 JSON Schema。這是對策略的一條約定：params 必須是 JSON。
- YAML 只用在人編輯的那一端：CLI 的 session 檔、console 的編輯器，送出前轉成 JSON。策略不收 YAML：每種語言的 SDK 都要內建 parser，YAML 1.1 的 `no`、`on`、`0755` 也有陷阱。
- **精度：** Go 端以 `json.RawMessage` 保留原始 bytes（規格的 `x-go-type`），不經過 float；console 以文字處理 params，不經過 JS 物件來回轉換。範例與文件建議價格、數量寫成字串。
- 不採用：base64 字串。在 YAML 檔與 console 裡都無法編輯。

### E10-D034 帳號的 `enabled` 與更新（E10-03，#308）

- **`enabled` 一期不公開，API 一律寫 true。** proto 的預設是 false，不填 TD 會以 `account_disabled` 拒絕所有需求。TD 端早已支援停用：account worker 停掉、需求 `REJECTED`、session 以 `deps_failed` 結束；之後要公開不必改程式。
- **更新帳號會重啟 account worker。** TD 沒有執行中推送設定的通道（`Live` 是空的，E01-D011）：憑證、`cancel_on_disconnect_s` 只在 `Init` 送進去。spawn spec 的 hash 一變，骨架就停掉舊的、以新的 spec 起新的 incarnation。新連線與 recon 完成前這個帳號的下單路徑是斷的；session 不會被結束，但期間的單會因沒有 responder 或逾時失敗（E08-D043）。規格與 console 都要標明。
- 內容沒變（hash 相同）時 API 不寫 etcd，避免無謂的重啟。
- `kind`、`td_instance` 不能改：密文封了 `kind`，`td_instance` 在 key 的路徑上。
- `AccountSettings` 的補正欄位（`backfill_interval_s`、`backfill_requested_at_ns`）一期不公開：誰執行還沒定（PROTOCOL §10 P18）。
- `GET /account-kinds` 從 exchange registry 列出帳號種類、`key_types`、是否要 passphrase，console 依此產生表單；只交易一個 venue 的才是 `supported`（E06-D014、E10-D011）。

### E10-D035 策略的識別碼（E10-03，#308）；由 E10-D118 修改

- **舊說法，已由 E10-D118 取代，不是現況：** `/strategies/{id}` 的 `id` 曾是 repository 路徑的最後一段，例如 `ghcr.io/bullionbear/strategy-chase` 的 `strategy-chase`。當時 E10-05 的設定（`repositories` 字串清單，E10-D023）不變，載入時檢查最後一段不重複。
- manifest 的 `name`（`mftiko-pack -name`，PROTOCOL §8.2）只在回應中顯示。
- 不採用：設定改成 `{name, repository}` 的配對。要改已合併的設定格式。
- 不採用：以 manifest 的 `name` 當識別碼。要先拉 manifest 才知道，各版本也可能不同。
- **E10-D118 拿掉以 `id` 選策略的路由與請求欄位。** 回應的 `id` 仍是存下來的路徑的最後一段，只供顯示。不再檢查最後一段不重複。`repositories` 字串清單由 E10-D119 的 `allow` 取代。

### E10-D036 規格的慣例（E10-03，#308）

- 時間是 RFC 3339 字串，不用 `*_ns` 整數。
- enum 是 API 自己的小寫字串，轉換寫在對照測試裡。兩個值沒有 proto 的直接對應：`Outcome` 的 `unknown` 是 `OUTCOME_UNSPECIFIED`（沒有宣告結束方式）；`EntryState` 的 `silent` 是 provider 還沒有 ack，或 provider 的 agent 不在（E09-D001）。
- problem+json 的 `type` 一律是 `about:blank`，另帶穩定的 `code`：`validation_failed`、`unauthenticated`、`not_found`、`conflict`、`not_terminal`、`referenced`、`algo_overlap`、`region_unavailable`、`storage_unavailable`、`registry_unavailable`、`internal`。驗證錯誤的 `path` 是 JSON Pointer（RFC 6901）。
- 整數用 JSON number：`api_id` 在 2⁵³ 內（E10-D008），etcd revision 實務上也到不了。
- **labels：** 最多 32 個，key 是 `^[A-Za-z0-9][A-Za-z0-9._/-]{0,62}$`，value 最長 256。平台不解讀，也沒有保留前綴。原本沒有任何規則，etcd 的值不能無限長。
- 規格是單一檔案 `contract/openapi/api/v1.yaml`，不拆外部 `$ref`。lint 設定在 `contract/openapi/redocly.yaml`（Redocly 的 `recommended`）：關掉 `no-unused-components`（SSE 的事件 schema 從 `x-mftiko-events` 引用，lint 不追）與 `info-license`（repo 沒有授權檔）。

### E10-D037 並行（E10-03，#308）

- **帳號的更新與刪除帶讀到的 `revision`**（`PUT` 在 body，`DELETE` 在 query），條件不成立回 409 `conflict`（E10-D004、W10）。
  - 不採用：`If-Match` 加 ETag。依 HTTP 的語意不成立應回 412，與 E10-D004 不一致；CLI 也比較好處理 body 的欄位。
- **結束 session 不帶 revision。** E13-D003 之後 spec 只有 `desired_state` 會變，伺服器端 CAS 重試即可；動作冪等，已經 `ended` 或 terminal 的照樣回 200。

### E10-D038 單一 `/events` 端點（E10-03，#308）

- `GET /api/v1/events?topics=sessions&topics=instances[&session_id=…]`，SSE（E10-D005）。
- **事件帶單一資源的完整最新狀態，不送差異：** `session`（`SessionDetail`）、`session_removed`、`instance`（`InstanceDetail`）、`reset`（重新 list 這些 topic）。用戶端整筆取代，和 mirror 的 level-triggered 一致（W4）。
- 連上時先送每個符合條件資源的目前狀態，再送後續的變化。
- event id、`Last-Event-ID`、心跳、HTTP/2、cookie 照 E10-D005。
- 一期的 topic 只有 `sessions`、`instances`。帳號變化少，console 重新整理即可。
- 不採用：每種資源各一條串流。一個頁面要開好幾條，而且各自處理重連。

### E10-D039 E10-03 的端點與一期不做的（E10-03，#308）

- 端點：`/auth/login`、`/auth/logout`、`/auth/me`；`/strategies`、`/strategies/{id}/versions`、`/strategies/{id}/versions/{ref}`；`/sessions`（建立、列出）、`/sessions/{id}`（取得、刪除）、`/sessions/{id}/end`；`/session-records`、`/session-records/{record_id}`；`/account-kinds`；`/accounts`、`/accounts/{api_id}`（取得、`PUT`、刪除）；`/instances`、`/instances/{name}`；`/events`。`/healthz`、`/readyz` 在 `/api/v1` 外，不在規格裡（E10-D026）。
- 「MD instance」是 `plane=md` 的 instance，不另開資源。
- 一期不做：補正的觸發（P18）、`td.evt` 的訂單與成交（E10-D014）、log 的內容（E10-D015）、帳號的 `enabled`（E10-D034）、selectors（E08-D022）。
- 試產碼：oapi-codegen v2.8.0（models、std-http-server、strict-server、client）產出的 Go 可以編譯；openapi-typescript 7.13.0 的輸出通過 `tsc --strict`。工具版本與 `make` 目標由 E10-04 釘住。

### E10-D040 `status/` 的寫入權是刻意的缺口（E10-06，#311）

- etcd RBAC 不分寫入與刪除。W5 要刪 status，`api` 只能拿 `status/` 前綴的整個寫入權，所以 etcd 擋不住 `api` 寫 status。
- W1「status 只有 STS 寫」在 status 上只靠 API 程式守。
- 接受。PROTOCOL §3.1 寫明，並以正向測試記錄「`api` 寫得進 status」是刻意的。
- 不改成由 STS 清 status。那會推翻 E10-D017。

### E10-D041 測試位置（E10-06，#311）

- 補在現有 `TestRBACEnforced`（`internal/platform/state/rbac_test.go`），用 etcdtest 開 auth 的真 etcd 跑。
- 正向：讀全部、寫 spec、寫 account、刪 status、寫 status（E10-D040）。
- 負向：寫 `demand/`、`ack/`、`agent`、`worker/`、`writer`、其他 instance 的前綴，都要被 etcd 拒絕。
- PROTOCOL §3.1 的表與角色表對齊。

### E10-D042 API 的寫入路徑與配號的細節（E10-08，#313）

- **API 另有一條寫入路徑 `state.APIWriter`，不經 `Term`。** `Term` 的正確性靠 fencing（writer key 與 epoch，W2），前提是一個 instance 同時只有一個 leader。API 不參選、多個副本同時寫（E10-D006），沒有 epoch 可比，每個寫入由自己的 txn 條件擋（create-only、`mod_revision` CAS）。
  - 兩條路徑碰的 key 不重疊，只有 status 例外：STS 經 `Term` 寫，API 經 `APIWriter` 刪（W5）。
- **`APIWriter` 的範圍：put 只限 STS instance 的 `spec/` 與 TD instance 的 `account/`；delete 另外可以碰 STS instance 的 `status/`。** 送出之前檢查，不經 I/O，回 `ErrNotWriter`。etcd RBAC 擋不住 `api` put status（E10-D040），擋的是這一層。txn 的 key 必須都在同一座 cluster（`ErrCrossCluster`），與 `Term` 相同。
- **錯誤分三類：**
  - txn 條件不成立：409 `conflict`。
  - etcd 連不上或逾時（gRPC `Unavailable`、`DeadlineExceeded`、`ResourceExhausted`，context 逾時，進程正在關）：503 `region_unavailable`。
  - 其他（範圍外的 key、跨 cluster、權限不足）是程式或設定錯誤：500 `internal`。
  - 503 時寫入可能已經生效：請求送出之後逾時，不代表沒有 commit。建立的情況由 E10-D031 的 reconcile 收尾（`pending` → `created`）。呼叫端重送建立會得到另一個 session；冪等的建立（例如 idempotency key）不在這張票。
- **配號只查可用 region 的副本**（E10-D014 的可用）。不可用的 region 跳過，一個 region 斷線不會擋住所有建立。
  - 只存在於不可用 region 的 id 可能被重抽，與 E10-D007 接受的兩副本競態同級：那個 region 有 200 個 session 時，每次建立約 1/84,000。
  - **要寫入的 region 必須可用，否則直接回 503，不寫。** 那個 region 的副本就是這次查重的依據，不可用時查不準。
- **任何 key 帶著這個 `session_id`，就算佔用**（`state.SessionOf`）：spec，以及 status、`SessionDemand`、MD 與 TD 的需求項目與 ack。不只看 spec，因為 W5 刪掉 spec 與 status 之後，STS 仍可能把推 `state_out` 的結果寫回 status，留下沒有 spec 的 status；新 session 抽到這個 id，就會接手不屬於自己的 status。`api_id` 仍只看所有 TD instance 的帳號（E10-D008）。
- **抽到佔用的 id 與 create-only 失敗都算一次，共 5 次**（E10-D007）。用完回 500 `internal`：以預期的量不會碰上，碰上表示亂數來源或 keyspace 有問題，重試也沒用。
- **reconcile（E10-D031，#316、#317）補兩條：**
  1. 只刪沒有 spec 的 status。有 spec 的 status 屬於那個 spec 的 session。
  2. `session_records` 的列以 spec 的 `create_revision` 對應 etcd，不只看 `session_id`。id 會重用（E10-D007），「已關閉、key 還在」要確認 key 就是那一列建立的那一個，才重刪。
- **結束 session 的伺服器端重試（E10-D037）用同一組原語：** 讀、改、以 `mod_revision` CAS；失敗時由 txn 的 else 讀回目前的值，再改一次，次數有上限；內容沒變不寫。
- 不採用：API 也用 `Term`。要參選、要 fencing，多副本變成主備，與 E10-D006 衝突。
- 不採用：查重時要求所有 region 都可用。一個 region 斷線就停掉所有 region 的建立。
- PROTOCOL §3.2 W10 補上寫入路徑、錯誤、配號與 reconcile 的規則（v1.36）。

### E10-D043 buf 的來源（E10-04，#309）

- **不再從 `web/node_modules` 取 buf。** 改以 `go install` 裝進 `bin/tools`，版本釘死。`buf.gen.yaml` 只留 `protoc-gen-go`。
- 釘 **v1.72.0**。原本 `web/package.json` 的 `@bufbuild/buf` 是 1.73.0，但 v1.73.0 要求 Go 1.26.7。這個 repo 是 Go 1.26.0，CI 的 actions/setup-go 設 `GOTOOLCHAIN=local`，不會再下載 toolchain，`go install` v1.73.0 失敗。v1.72.0 是裝得起來的最新版；對目前的 proto，`buf lint` 與 `protoc-gen-go` 的產物和 v1.73.0 相同。

### E10-D044 工具的位置（E10-04，#309）

- **Go 工具由 `make tools` 以 `go install` 裝進 `bin/tools`，版本釘死：** buf v1.72.0（E10-D043）、`oapi-codegen` v2.8.0（E10-D039）、`oasdiff` v1.34.1。
- **Node 工具放在 `web/package.json` 的 devDependencies，精確版本、不加 `^`，從 `node_modules/.bin` 執行：** Redocly CLI 2.60.0、`openapi-typescript` 7.13.0（E10-D039）。7.13.0 的 peer 是 TypeScript 5，console 是 TypeScript 6。CLI 不載入專案的 TypeScript，所以 `web/.npmrc` 保留 `engine-strict`，並設 `legacy-peer-deps=true`，`npm ci` 才裝得起來。產物仍由 svelte-check 用專案的 TypeScript 檢查。
- **`openapi-fetch` 0.17.0 放在 dependencies**，精確版本、不加 `^`。它是 client 程式庫，不是產物。

### E10-D045 Go 產碼（E10-04，#309）

- **產成一個 package，放在 `contract/gen/api/v1`，package 名 `apiv1`。**
- 型別、strict server、client 各一個 `.gen.go`：`types.gen.go`（models）、`server.gen.go`（`std-http-server` 與 `strict-server`）、`client.gen.go`。
- **server 用標準庫 `net/http`，不引入其他 router。**
- `types.gen.go` 設 `skip-prune`。SSE 的 `SessionRemovedEvent` 與 `ResetEvent` 只從 `x-mftiko-events` 引用，lint 不追（E10-D036），oapi-codegen 預設會剪掉。
- 這個 package 會 import `github.com/oapi-codegen/runtime`。archtest 只對 `contract/gen/api` 放開這個 import；其餘 `contract` 仍只靠 protobuf 與標準庫（E00-D006）。

### E10-D046 草案期間的 oasdiff（E10-04，#309；E10-20 起照 E10-D116 會擋）

- **E10-20（E10-D018）之前，CI 照跑 `oasdiff breaking` 對 base，印出報告但不擋。** 不帶 `--fail-on`。E10-20 加上 `--fail-on ERR`，改成會擋。
- **base 沒有 spec 時略過。** base 不是一個 commit 則失敗，不當成略過。

### E10-D047 make 目標（E10-04，#309）

- **新增 `make openapi`：** Redocly lint，然後產 Go 與 TypeScript。
- **`make all` 與 `make gen-check` 同時涵蓋 proto 與 openapi。**
- **TypeScript 產物放在 `web/src/lib/gen/api/v1.ts`。**
- `make check` 與 CI 在 proto 的 lint、breaking 之後跑 `openapi-lint`、`openapi-breaking`，再跑 `gen-check`。

### E10-D048 Postgres 的連線（E10-07，#312）

- **啟動時建立連線池，但不等它連上。** 修改 E10-D024 的「不連 Postgres」。
- **readiness 仍只看 etcd。** Postgres 不可用只影響稽核。
- 不連 registry 與「W5 刪除留給後續」不變。

### E10-D049 稽核非同步寫入（E10-07，#312）

- **中介層把稽核列放進有上限的佇列，由背景寫入，不拖慢回應。** 佇列滿了丟棄，記 error log 與計數器。寫不進去也不改變回應（E10-D010）。
- **SIGTERM 時在 E10-D027 的收尾時限內把佇列寫完。** 時限就是 `shutdown_timeout`（預設 10 秒），與進行中的請求同一個窗口。逾時還沒寫完的列丟棄，記 error log 與計數器。

### E10-D050 cookie 一律 Secure（E10-07，#312）

- **cookie 一律 `Secure`，dev 的明文 HTTP 也不例外。** 瀏覽器把 localhost 當成安全來源。不另設開關。

### E10-D051 Bearer 優先（E10-07，#312）

- **Bearer 和 cookie 同時存在時看 Bearer。** Bearer 無效就回 401，不退回用 cookie。

### E10-D052 稽核 body（E10-07，#312）

- **存成 jsonb，上限 64 KiB，超過就截斷並標記。** 不是 JSON 的 body 不記內容，只記長度。
- **帳號憑證欄位照 E10-D010 先移除。** 名為 `token` 的欄位也先移除，稽核列與 log 不留下 token。

### E10-D053 request id（E10-07，#312）

- **一律由伺服器產生，用 `X-Request-Id` 回給呼叫端，不採用外面帶進來的。**

### E10-D054 來源位址（E10-07，#312）

- **只記 `RemoteAddr`（含 port），不信任 `X-Forwarded-For`。**

### E10-D055 登入不限速（E10-07，#312）

- **一期不做登入限速。**
- **token 須是至少 32 bytes 的隨機值**，寫進部署文件與產生 token 的說明。設定只存 SHA-256，API 無法檢查長度。

### E10-D056 沿用 artifact 的 client（E10-10，#315）

- **沿用 `internal/platform/artifact` 的 oras-go client 與 manifest 解析。** 新增的唯讀操作是列出 tag，以及把 tag 解析成 digest。既有函式的行為不變，STS 不受影響。
- 目錄讀 manifest 時重用同一個解析器，不下載 layer，也不檢查 os／arch：目錄不是 spawn。這不改變 `Pull`。不需要資料目錄。

### E10-D057 不設快取（E10-10，#315）

- **目錄不設快取。** registry 是準的。每次請求都問 registry。
- 列出版本時，解析 tag 同時最多 8 個請求。收集同一個 digest 的 tag 也用這個上限。

### E10-D058 tag 清單與分頁（E10-10，#315）

- **一次取回完整的 tag 清單**（client 跟著 registry 的分頁），依 tag 名稱排序，`latest` 是普通的一項。
- API 用既有的 `limit`／`cursor` 分頁（E10-D004）。cursor 是上一頁的最後一個 tag，下一頁從它之後開始。

### E10-D059 錯誤（E10-10，#315）

- 連不上，或 registry 5xx，回 HTTP 503，`code` 是 `registry_unavailable`。
- manifest 解析不了，或不是策略 artifact（含 OCI index、錯誤的 media type、`kind` 不是 strategy），回 HTTP 502，`code` 是 `invalid_artifact`。這個 code 加進契約的 `ErrorCode`。
- 找不到回 404 `not_found`，包括未知的 strategy id 與未知的 tag 或 digest。
- 呼叫端取消或行程關閉不改寫成 503。
- **E10-D120、E10-D121 修改建立與列出。** 未知的 strategy id 與版本路由已刪。列出時 repository 不存在回 404，detail 是 `repository not found`。建立時找不到 ref 是 `validation_failed`（`was not found`），kind 不是 strategy 也是 `validation_failed`（`is not a strategy artifact`），不是 502。列出不讀 manifest。解析 tag 時不是 not-found 的錯誤仍用上面的對應。

### E10-D060 registry 設定（E10-10，#315）；由 E10-D119、E10-D122 修改

- **與 STS 相同的是那一份 Docker `config.json`：** 以 oras-go 的 credentials 依 host 讀取（E10-D123）。`plain_http` 布林改成 host 清單（E10-D124。布林的決定是 E09-D033，註記曾寫成 E09-D034）。
- 放在 API 的 `-config` JSON：`registry_creds`（路徑，空則匿名）、`plain_http_hosts`。沒有 `registry`。秘密值不進 JSON。
- 載入時檢查格式與檔案，不連線（E10-D024 的載入不連線仍然成立）。`allow` 取代 `repositories`（E10-D119）。請求、spec 與回應的 `repository` 都是含 host 的完整路徑（E10-D122）。回應的 `id` 是該路徑最後一段，只供顯示（E10-D118，修改 E10-D035）。

### E10-D061 測試（E10-10，#315）

- **用 CI 同一套 `registry:2`。** 推一個測試 artifact，再用目錄 API 查，比對 tag、digest 與 manifest。找不到回 404。

### E10-D062 建立帳號必帶憑證（E10-09，#314）

- `AccountCreate.credentials` 必填。缺少回 400。
- 更新時選填。沒帶就保留原密文。

### E10-D063 拿掉 `credentials_fingerprint`（E10-09，#314）

- 從 OpenAPI 規格的 `Account` 刪除 `credentials_fingerprint`，並重新產碼。`make gen-check` 必須通過。
- 讀取帳號完全不回傳任何與憑證相關的值：明文、密文、指紋都沒有。
- 修改 E10-D011「讀取帳號回傳密文的指紋」。

### E10-D064 憑證格式驗證（E10-09，#314）

- `key_type` 必須在該種類的 `key_types` 內。
- passphrase 依種類必須有，或不能有。
- key、secret 不可為空。
- 違反回 400。錯誤只說哪個欄位，不帶值。
- 不向交易所驗證 key。

### E10-D065 `td_instance` 檢查（E10-09，#314）

- 必須在 topology 內，而且是 TD 平面。
- `td_keys` 必須有它的公鑰。
- 否則回 400。缺公鑰要明確說明。

### E10-D066 `name` 不重複（E10-09，#314）

- 依副本 best-effort 檢查。同名回 409。
- 比對的是這個副本在可用 mirror 上看得到的每一個帳號，精確字串，不限同一個 TD instance。更新時排除自己。不可用的 region 略過。

### E10-D067 驗收測試（E10-09，#314）

- Go 測試串起 API 封裝 → 寫 etcd → TD agent 以私鑰解開並比對 `api_id`／`kind` → 接 fake exchange，讓 account worker 到 READY。
- 另一測試掃 etcd、API 回應、log、`audit`，確認找不到憑證明文與密文。
- 註記（E06-D013，不另編號）：etcd 的 `sealed_credentials` 就是密文。掃 etcd 只確認沒有明文。掃 API 回應、log 與 `audit` 確認沒有明文，也沒有密文（原始位元組、base64 的 Std／Raw／URL／RawURL，或 hex）。

### E10-D068 冪等建立（E10-11，#316）

- 建立 session 可選帶 `Idempotency-Key` header，存在 `session_records`，以部分唯一索引保證，保留 24 小時。同 key 重送回同一個 session；同 key 不同 body 回 409。

### E10-D069 `pending` 逾時 2 分鐘（E10-11，#316）

- 超過 2 分鐘且 etcd 無對應 spec 就標 `abandoned`。之後 etcd 出現該 spec 且 `create_revision` 與雜湊對得上，改回 `created`，不另開一列。

### E10-D070 reconcile 頻率（E10-11，#316）

- 每 30 秒全量比對，mirror 有變動時也觸發；每個副本都跑，寫入一律有條件的 upsert，不選主。

### E10-D071 雜湊不同（E10-11，#316）

- 比對以 `specContentEqual` 為準，排除 `desired_state`（E10-D031）。只記 error log 與計數器，不覆蓋任何一邊；只同步 `desired_state` 與 terminal 結局。

### E10-D072 etcd 有 spec、Postgres 沒有紀錄：只告警，不補列（E10-11，#316）

- 正常經 API 建立不會發生（先寫 Postgres）。reconcile 只記 error log 與計數器。
- 修改 E10-D031：不變式 1 改為「經 API 建立的 spec，Postgres 都有一列」，拿掉「etcd 有、Postgres 沒有就補一列，建立者記為 null」。
- 本票完成標準「reconcile 補得回直接寫進 etcd 的 spec」取消，改為「直接寫進 etcd 的 spec 會告警」。

### E10-D073 `session_records` 欄位（E10-11，#316）

- migration `0004_session_records`。代理鍵 UUID、session_id、sts_instance、create_revision、spec 原始 bytes 與 SHA-256、state（pending／created／abandoned／closed）、desired_state、end_reason、created_by、idempotency_key，以及建立、結束、關閉時間；state_out digest 欄位先建好，由 #317 寫入。
- 驗收「controller 推得出需求、STS 起得了 worker」用 testplane 端到端測試。
### E10-D074 樣式（E10-15，#320）

- **不加 UI 框架。** console 用 Svelte scoped CSS，加上一份全域 CSS 變數。
- 不採用 Tailwind，也不採用元件庫。

### E10-D075 SSE（E10-15，#320）

- **用瀏覽器原生 `EventSource`。** cookie 由瀏覽器自動帶上（E10-D005、E10-D009）。斷線由瀏覽器重連，並送 `Last-Event-ID`。
- **收到 `reset` 就重新 list。**
- 不自己實作重連，也不自己記 event id。

### E10-D076 未登入（E10-15，#320）

- **任何 API 回 401 就導向 `/login`，登入後回到原頁。**
- **登入頁只有 token 欄位。** token 不存 localStorage，只靠 cookie。

### E10-D077 狀態管理（E10-15，#320）

- **Svelte 5 runes。** 在 `$lib` 放一個全域 store，內容是登入使用者、region 可用性、`controller_absent`。
- 不加其他狀態管理套件。

### E10-D078 dev（E10-15，#320）

- **`vite dev` 把 `/api` proxy 到 `localhost:8080`。** cookie 同源，與正式環境由 API 同源提供 console（E10-D009、E10-D028）一致。

### E10-D079 測試（E10-15，#320）

- **照完成標準跑 `svelte-check` 與 build，並加進 CI。**
- 不加 vitest，不加 Playwright。端到端驗收留給 #324。
- PROTOCOL 為 v1.43。#314（v1.41）與 #316（v1.42）平行進行；後合併者 merge main，不 rebase，並維持版本單調。

### E10-D080 Postgres 不可用（E10-12，#317）

- E10-D031 要求先寫 Postgres，所以 Postgres 連不上時不刪。保留期掃描跳過該輪；`DELETE /sessions/{id}` 回 503 `unavailable`。
- 回應的 `code` 用既有的 `storage_unavailable`（E10-D031、E10-D036）。OpenAPI 的 503 是 `Unavailable`，沒有單獨的 `unavailable` 這個 `code`。

### E10-D081 etcd 有 session、Postgres 無列（E10-12，#317）

- E10-D072 的情境：不刪、不補列，只記 error log 加計數器（告警）。依原則：不會發生的情況不特別處理。立即刪除走同一條：不刪。

### E10-D082 只有 status 沒有 spec 的 key（E10-12，#317）

- STS 推完 `state_out` 寫回的 status：過保留期就以該 status 的 `mod_revision` 為條件用 `deleteIf` 刪。
- 若對應列尚未 closed，先寫最後 status、`state_out_digest` 並關閉，再刪。

### E10-D083 掃描頻率（E10-12，#317）

- 沿用 #316 的 reconcile 迴圈（每 30 秒加 mirror 變動），不另開迴圈。

### E10-D084 立即刪除 API（E10-12，#317）

- 照現有 OpenAPI：非 terminal 回 409 `not_terminal`；成功 204，重複刪除也 204。
- 稽核動作 `session.delete`：既有中介層寫一列 `audit`（E10-D010），`method` 是 DELETE、`path` 是 `/api/v1/sessions/{session_id}`、`session_id` 是該 id、成功的 `status` 是 204。不另加欄位。

### E10-D085 驗收（E10-12，#317）

- 以 issue 完成標準為準：過期的刪、未過期與非 terminal 的不刪；兩個 replica 同時跑；寫 Postgres 之後、刪 etcd 之前被殺，重啟後 reconcile 完成刪除；刪掉 spec 之後 E10-02 的 STS 清理接得上（spec 不在才清，E10-D016、E10-D019）。
- 加上 E10-D080–E10-D082 各自的測試。
- PROTOCOL 為 v1.46。

### E10-D086 params（E10-17，#322）

- **JSON 文字框。** console 只檢查是合法 JSON，原樣送出。
- manifest 有預設 params 時預填。
- `StrategyVersion` 沒有預設 params，所以文字框一開始是空的。

### E10-D087 feeds（E10-17，#322）

- **可增刪的列，自由輸入。** console 不比對清單，由 API 驗證。

### E10-D088 帳號與 algo_id（E10-17，#322）

- **帳號從 `/accounts` 下拉。** algo_id 由使用者在本 session 自行宣告（1–1023），換帳號時清空。
- 帳號不存 `algo_ids`（E06-D002）。不新增每帳號的 algo 清單。

### E10-D089 Idempotency-Key（E10-17，#322）

- **每次打開精靈產生一個 UUID。** 送出失敗重試沿用同一個（E10-D068）。

### E10-D090 欄位錯誤（E10-17，#322）

- **依 API 錯誤的 JSON Pointer 對到欄位顯示。** 對不到的放表單頂端。

### E10-D091 session 列表與流程（E10-17，#322）

- **列 `/sessions` 現存 session**（狀態、策略、帳號、建立者、時間），SSE 即時更新。
- `/session-records` 歷史不在本票。建立成功後導向 session 詳細頁。
- PROTOCOL 為 v1.47。與 #317 的 v1.46 平行；後合併者 merge main 並維持版本單調。

### E10-D092 `AgentRecord` 加 `host`、`work_dir`（E10-13，#318）

- **在共用 agent 骨架當選時寫入。** 所有平面（MD、TD、STS、controller）都寫。
- `state.proto` 相容擴充，PROTOCOL proto 區塊同步。
- 除多寫這兩欄外，MD、TD、controller 行為不變。

### E10-D093 terminal Condition（E10-13，#318）

- STS 寫 terminal `SessionStatus` 時，依當下 `SessionDemand.bindings` 的 `REJECTED`、`DEAD` 項目各寫一個 `dependency` Condition。
- 若當下 `SessionDemand` 已不在則不寫，不特別處理。

### E10-D094 SSE 接續（E10-13，#318）

- 每條 SSE 連線對每個 region 各開一個 etcd watch，從 `Last-Event-ID` 該 region 的 revision+1 開始。
- compacted 送 `reset`。
- 不經 #316 的 mirror（mirror 無歷史）。

### E10-D095 慢用戶端（E10-13，#318）

- 每條連線的送出緩衝有上限，滿了就斷線，由瀏覽器帶 `Last-Event-ID` 重連。

### E10-D096 `AgentRecord` 最後已知值（E10-13，#318）

- 每個副本在記憶體保留最後看到的 `AgentRecord`，回應標明是最後已知值。
- 副本重啟後沒有就只標示不在。

### E10-D097 `/session-records`（E10-13，#318）

- 分頁與篩選照 OpenAPI 現有參數，依 `created_at` 由新到舊。
- 驗收以 issue 完成標準為準，並為 E10-D093–E10-D096 各補測試。
- PROTOCOL 為 v1.50。

### E10-D098 不支援的種類（E10-16，#321）

- **`/account-kinds` 中 `supported` 為 false 的種類在選單顯示為灰色不可選，附原因。**

### E10-D099 TD instance（E10-16，#321）

- **自由輸入，由 API 依 E10-D065 驗證。**
- 不依賴 `/instances`（#318），兩票不互卡。

### E10-D100 更新帳號（E10-16，#321）

- **憑證區預設收起，勾「更換憑證」才展開。**
- 不勾則沿用原密文（E10-D062）。

### E10-D101 密鑰欄位（E10-16，#321）

- **ed25519 私鑰是 PEM PKCS#8，`pem.Decode` 需要保留換行。** key type 為 ed25519 時，secret 用保留換行的多行文字框，並可選 `.pem` 檔讀入。關閉自動填入與拼字檢查。送出後立即從頁面狀態清除，不存任何地方。成功或失敗都不回顯。送出的 bytes 是原樣 PEM 文字（含換行）。
- HMAC 的 key、secret、passphrase 維持 password 輸入框。

### E10-D102 刪除（E10-16，#321）

- **先確認。**
- 409 `referenced` 列出 `referenced_by` 的 session。
- 409 `conflict` 重新載入後再確認。

### E10-D103 列表（E10-16，#321）

- **顯示種類、TD instance、`referenced_by`（session 可點過去）。**
- PROTOCOL 為 v1.49。與 #318 的 v1.48 平行；後合併者 merge main 並維持版本單調。

### E10-D104 結束按鈕（E10-18，#323）

- **先確認。** session 已是 `ended` 或 terminal 時不顯示按鈕。
- 呼叫 `POST /sessions/{id}/end`（冪等）。

### E10-D105 log 位置（E10-18，#323）

- **顯示 `LogLocation` 的主機與完整路徑，附複製按鈕。**
- `AgentRecord` 不在、用最後已知值時加註（E10-D096）。

### E10-D106 `state_out`（E10-18，#323）

- **只顯示 digest 加複製，不做下載。**

### E10-D107 SSE（E10-18，#323）

- **每頁一條 EventSource，只訂該頁需要的 topic。**
- 收到 `reset` 重新 list。沿用 `web/src/lib/live.svelte.ts`。

### E10-D108 agents 頁（E10-18，#323）

- **依平面分組。** 最後已知的 `AgentRecord` 加標記。
- `controller_absent`、`unavailable_regions` 以頁頂橫幅顯示。

### E10-D109 market 頁（E10-18，#323）

- **唯讀列出 MD instance 與各 feed 狀態，不加搜尋或篩選。**
- PROTOCOL 為 v1.51。

### E10-D110 Content-Type 只對 cookie 要求（E10-19，#324；修改 E10-D009）

- **以 cookie 認證的寫入（POST、PUT、PATCH、DELETE，沒有 body 的也算）與 `POST /auth/login` 才要求 `Content-Type: application/json`。** Bearer 的請求不檢查。
- 原因：這條規則是 CSRF 的防線，CSRF 只對瀏覽器自動帶上的憑證有效；bearer token 不會被自動帶上。原本一律要求，沒有 body 的寫入（`POST /sessions/{id}/end`、`DELETE /sessions/{id}`、`DELETE /accounts/{api_id}`、`POST /auth/logout`）照 OpenAPI 生成的 Go client 不會帶這個 header，被回 400。
- **console 一律帶。** openapi-fetch 只在有 body 時設 Content-Type，所以 console 原本的結束、刪除、登出也是 400。`web/src/lib/api.ts` 加一個 middleware，GET、HEAD 以外的請求沒有 Content-Type 就補上。
- 不採用：沒有 body 就免檢查。HTML 表單一定會帶 Content-Type，擋得住表單，但同站的 no-cors fetch 可以不帶，防線變弱。
- 不採用：只改 client。契約寫的是沒有 request body，照契約生成的 client（E11 的 CLI）都要自己記得補。
- OpenAPI 的認證說明與 `cookieAuth` 補上這一條；不影響生成的程式碼。
- PROTOCOL 為 v1.52。

### E10-D111 端到端驗收的環境（E10-19，#324）

- **擴充 `deploy/accept`，不另開 compose 專案。** 沿用 runner、testnet 憑證與 `accept.sh`；加上 registry、第二個 etcd（region `accept2`，只放 `sts2`），controller、`sts1`、`sts2` 與兩個 API 副本都是 runner 裡的行程。
- **只有 API 副本 a 對外：** `127.0.0.1:18080`（`ACCEPT_API_PORT`），給瀏覽器與 Playwright。避開 dev 的 8080。
- **驅動程式是 `cmd/mftiko-api-accept`**，比照 `mftiko-md-accept`、`mftiko-td-accept`。只透過 API，用 `contract/gen/api/v1` 的 Go client，同時打兩個副本。
- **主流程用 `examples/strategies/hello`**：一個 book feed 加一個帳號，掛一張遠離市價的買單再撤掉，結果印在 stdout，所以主機上的 log 檔就是 MD、TD、STS 都通的證據。
- **每個流程都比對 `audit`：** 流程送出的每個請求，以回應的 `X-Request-Id` 找 audit 的列，method、path、status 與使用者都要相同。
- **驗收的保留期是 1 分鐘，`unavailable_after` 5 秒**，主流程才看得到 W5 刪 spec 與 STS 清 log。

### E10-D112 異常流程在驗收腳本裡跑，不進 CI（E10-19，#324）

- 未知 ticker 要真的 MD 與 SYM；controller 不在、region 斷線要停掉行程或容器。都在 `accept.sh` 的同一組環境裡跑，`accept.sh e2e-all` 一次跑完。
- 各元件的單元測試（`internal/api` 的併發副本、region 不可用等）照舊在 CI。

### E10-D113 Playwright smoke 在本機打驗收環境（E10-19，#324）

- `web/e2e/smoke.spec.ts`，`@playwright/test` 固定 1.64.1，只用 Chromium。不起 server，預設打 `http://127.0.0.1:18080`。
- 需要 testnet 才走得到 `ready` 與 `running`，所以不進 CI、不在 `make check` 裡；`accept.sh console-smoke` 執行。
- smoke 的檔案不在 `svelte-check` 的範圍：要納入得多加 `@types/node`。Playwright 執行時自己轉譯。

### E10-D114 dev 環境的 API（E10-19，#324）

- **`deploy/dev/compose.yaml` 加 `api`：** alpine 跑 `bin/linux/mftiko-api`，`-web` 是 `web/build`，`127.0.0.1:8080`，plain HTTP（E10-D029）。`make dev-up` 先 `make web`、建 Linux 的 `mftiko-api`。
- **設定由 `deploy/dev/api-init.sh` 產生到 `deploy/dev/data/api`（gitignore）**，比照 NATS 憑證：一個 region `dev`，controller、`md1`、`td1`、`sts1`；使用者 `dev` 與 token、cookie 金鑰、td1 的 seal 金鑰只產生一次。dev 不跑 agent，session 停在 pending。
- dev 的 etcd 沒有 auth，etcd client 遇到 auth 沒開時照樣連，所以 API 的密碼只是佔位。
- API 啟動時以 `os.Root` 打開 web 目錄並一直持有（E10-D028）。`make web` 會換掉整個 `web/build`，之後要重啟 API 才看得到新的 console。

### E10-D115 驗收環境的 migration（E10-19，#324）

- `mftiko-migrate` 還沒實作。`accept.sh up` 逐檔套用 `migrations/*.up.sql`，套過的記在 `accept_migrations`。
- E4、E6 時建的資料庫沒有這張表，只套過 0001、0002：遇到 `already exists` 的檔案補記為已套用，其他錯誤照樣失敗。
- `mftiko-migrate` 做出來之後改用它。
- E12-D031 取代上面的暫行做法：`accept.sh up` 改呼叫 `mftiko-migrate up`，從空資料庫開始，不再讀 `accept_migrations`。

### E10-D116 `api.v1` 定案（E10-20，#325）

- **`contract/openapi/api/v1.yaml` 定案，`info.version` 從 `1.0.0-draft` 改成 `1.0.0`。** 實作 E10-D018：之後同一個 major 只做相容擴充。
- **`OASDIFF_FAIL_ON` 預設 `ERR`。** `make openapi-breaking`（CI 的 `oasdiff breaking` 那一步）以 PR 的 base 為基準，遇到 ERR 等級的不相容改動就失敗；WARN 只印出。實作 E10-D046 說的「E10-20 加上 `--fail-on ERR`」。
- **`info.version` 的規則：** 相容擴充加 minor，只改說明加 patch，和 git tag 無關。不相容的改動另開 `v2.yaml` 與 `/api/v2`，與 v1 並存。寫在 `docs/release.md`。
- **E10-D118 是唯一的原地例外（#429）。** `info.version` 改成 `2.0.0`，路徑仍是 `/api/v1`。`make openapi-breaking` 帶 `contract/openapi/api-v1-err-ignore.txt`。下一次不相容仍另開 `v2.yaml`。
- **PROTOCOL §10 移出 P8**（v1.29 已定，§3.2 W10 與 E10-D006）。§1 的契約 A 標為定案。PROTOCOL 為 v1.53。
- `cmd/mftiko-api`、`internal/api` 的 doc 補上 E10-D110、E10-D114，拿掉「W5 是之後的票」。
- epic #12 的完成標準照 E10-01（#306）分成契約、API server、端到端、console 四層，各自對到驗證過的票與檢查。

### E10-D117 `session_records` 存最後的 status（#371）

- **新欄位 `final_status BYTEA`（migration 0005），存 etcd 裡那份 `statev1.SessionStatus` 的 proto bytes**，和 `spec_bytes` 存 spec 的方式相同。`GET /session-records` 與 `GET /session-records/{id}` 以 `statusView` 轉成 `final`。conditions、`last_exit`、`state_out` 都保留，跟 `GET /sessions/{id}` 的 `status` 同一個樣子。
- **兩條關閉路徑都寫：** W5（`retainSpec`、`retainStatusOnly`）與立即 `DELETE`（E10-D084），寫入關閉前讀到的那份 status。已關閉的列之後收到較新的 status（E10-D082）時，取代成較新的那份；沒有讀到 status 就保留原值。
- **舊列不回填。** 0005 之前關閉的列 `final_status` 是 null，API 省略 `final`。存的 bytes 解不開時，記 error log 並省略 `final`，這筆紀錄仍可讀。
- **契約不變：** `final` 本來就是選填（`api.v1` 仍是 `1.0.1`）。E10-D031 寫的「W5 先寫最後的 status」到這裡才實作。
- 不採用：只存 phase、reason、`last_exit` 幾個欄位（之後 `SessionStatus` 加欄位就得改 schema）；存 jsonb（要另寫一份轉換，而且跟 etcd 的 bytes 不一致）。
- PROTOCOL 為 v1.62。

### E10-D118 策略選擇改為 repository 與 ref（#429）

- **`StrategySelector` 是 `{repository, ref}`，兩者都必填。** `repository` 是完整路徑，例如 `ghcr.io/acme/md_watch`。拿掉 `id`。沒有相容期。
- **在原地打破 `api.v1`，不另開 `v2.yaml`。** 這張是 E10-D116 的唯一例外。路徑仍是 `/api/v1`，檔案仍是 `contract/openapi/api/v1.yaml`。`info.version` 改成 `2.0.0`，`mftiko version` 看得到這次打破。之後同一個 major 只加相容欄位；下一次不相容才開 `v2.yaml` 與 `/api/v2`。
- **`oasdiff breaking --fail-on ERR` 用 `contract/openapi/api-v1-err-ignore.txt` 放行這次 diff。** base 已經含這次打破之後，這些行對不到任何改動，留著也不擋。拿掉 `strategy/id` 是 WARN，印出來、不失敗。
- **回應的 `StrategyRef.id` 仍是選填：** 存下來的路徑的最後一段，只供顯示。路徑是空的或形狀不合法就省略。session 列表的 `strategy_id` 篩選比對這個顯示值。選擇器與請求不再有 `id`。
- 修改 E10-D013、E10-D035、E10-D116。PROTOCOL 為 v1.79。

### E10-D119 策略 allow 清單（#429）；由 E10-D122 修改

- **`-config` 的 `repositories` 改成 `allow`（`[]string`）。** 每一項是 registry host，或 host 加路徑前綴，例如 `registry.example/team`。單獨的 `*` 表示任何 host 上的任何路徑（E10-D122；原先寫的「只開這一個 registry」不再成立）。空的 `allow`（或省略）合法，全部拒絕。舊的 `repositories` 欄位 Load 不認得。
- **`*` 必須是唯一一項。** 和前綴混在一起，Load 失敗。去掉結尾 `/` 之後重複，Load 失敗。不是 `*` 的項目必須是 host（`host[:port]`）或 `host[:port]/path`（沒有空白，沒有空的、`.`、`..` 片段，沒有 tag 或 digest）。不再要求一個設定好的 registry 位址。存下來的形式去掉結尾 `/`。
- **比對是路徑邊界。** 去掉結尾 `/` 之後，repository 等於前綴，或以前綴加 `/` 開頭。`registry.example/team` 對上自己與 `registry.example/team/strategies/md_watch`，對不上 `registry.example/team-evil`。前綴等於 host 時，allow 比對涵蓋該 host 字串與其下的路徑。只有 host、沒有路徑，回 `validation_failed`，reason 是 `is not a repository`（E10-D122；原先是 `is not on the configured registry`）。
- **請求的 repository 就是完整路徑，含 host。** 這條完整路徑交給 Catalog，也是寫進 `sessionv1.StrategyRef.Repository` 的值。回應的 `repository` 就是存的值。STS 從這條路徑的 host 拉。proto 不變。沒有把 #431 去掉 host 的舊值補回來的相容期。
- **不在 allow：** `validation_failed`，建立是 `/strategy/repository`、列出是 `/repository`，reason 是 `is not allowed`。這項先查，registry 掛了也一樣。catalog 沒有接上時：`registry_unavailable`，detail 是 `registry is not configured`。Load 之後 catalog 一定接上，這條只留給沒有 catalog 的行程。
- 帳密見 E10-D123。Load 仍不連 registry（E10-D024）。GC 仍依 digest（E09-D056），不變。
- 修改 E10-D013、E10-D023、E10-D024、E10-D060。E10-D122 修改本節。

### E10-D120 建立 session 時讀 manifest（#429）

- **建立時讀 manifest，`kind` 必須是 strategy。** 否則 `validation_failed`，`/strategy/ref` 的 reason 是 `is not a strategy artifact`，不是 502。
- 缺少 `repository` 或 `ref`：`validation_failed`。形狀不對：`/strategy/repository` 的 reason 是 `is not a repository`。
- registry 沒有這個 repository 或 ref，或 manifest 讀不出來：`validation_failed`，`/strategy/ref` 的 reason 是 `was not found`。
- registry 連不上：`registry_unavailable`。
- 列出 tag 不讀 manifest，也不檢查 kind。

### E10-D121 列出一個 repository 的 tag（#429）

- **`GET /strategies?repository=<完整路徑>`。** `StrategyRepository`：必填 `repository`（去掉結尾 `/` 的請求值）與 `items`（`StrategyTag`）。選填 `latest`（名為 `latest` 的 tag；沒有就省略；不在這一頁也回）。選填 `next_cursor`。
- **分頁不變（E10-D058）：** `limit`／`cursor`，預設 100，最大 1000，cursor 是這一頁最後一個 tag 的 base64 raw-url。先解析該 repository 的全部 tag，並行上限 8（E10-D057）。不快取。
- 拿掉沒有參數的列表、`GET /strategies/{id}/versions`、`GET /strategies/{id}/versions/{ref}`，以及 schema `Strategy`、`StrategyList`、`StrategyVersion`、`StrategyTagList`。`InvalidArtifact` 與 `ErrorCode` 留著。解析 tag 時不是 not-found 的錯誤仍照 E10-D059。
- 不在 allow：400 `validation_failed`，path `/repository`。缺少 query：400（必填參數）。不是 `host/path`：400，reason 是 `is not a repository`（E10-D122）。registry 說 repository 不存在：404 `not_found`，detail 是 `repository not found`。catalog 沒接上、連不上，或 401、403：503 `registry_unavailable`（E10-D127）。
- **CLI：`mftiko strategy list <repository>`。** 拿掉 `strategy versions` 與 `strategy show`。欄是 TAG、DIGEST（完整）、LATEST（digest 等於回應的 `latest`，不是只看這一頁有沒有名為 latest 的 tag）。自動翻頁的 `-o json` 保留 `repository` 與 `latest`；其他命令的合併仍只留 `items` 與 `unavailable_regions`（E11-D011）。`--cursor` 仍原樣寫出那一頁。
- **console：** `/strategies` 是 repository 文字框加 tag 表，沒有全域目錄。拿掉 `/strategies/[id]`。建立 session 先填完整 repository，再從 `GET /strategies?repository=` 載入 tag。body 是 `{repository, ref}`。
- 修改 E11-D013。

### E10-D122 策略 image 的 host 來自 repository（#430）；由 E15-D007 修改

- **拿掉單一 registry 位址。** API 的 `-config` 不再有 `registry`。STS 不再有 `--registry`。策略 image 的 host 一律是完整 `repository` 在第一個 `/` 之前的那段。`registry` 與 `--registry` 不再用來找策略 image。
- **spec 存完整路徑。** `sessionv1.StrategyRef.Repository` 含 host。回應的 `repository` 就是這個值，不再把位址接在前面。`id` 仍是最後一段，只供顯示。輸入的資料 artifact 也是完整路徑，仍不查 `allow`。沒有相容改寫：位址已經不在，#431 寫進 spec 的不含 host 的值拉不到。
- **`*` 仍必須是唯一一項，改為任何 host。** 空的 `allow` 仍全部拒絕。dev 與 testnet 的範例改列具體 host，不用 `*`，避免打開每一台 registry。host 本身可以當 allow 前綴。請求只有 host、沒有路徑：`is not a repository`。
- **`mftiko_registry_up` 維持 0。** API 沒有單一地址可探。沿用 E12-D038「沒有設定是 0」，不新增依 host 標籤的探測。範例告警 `MftikoRegistryDown` 拿掉，否則它恆為真。某次策略請求連不上仍是 `registry_unavailable`。STS 拉不到仍是 `mftiko_artifact_pull_failing`。
- 修改 E10-D119、E10-D060、E10-D013、E09-D010、E12-D038。PROTOCOL 為 v1.80。

### E10-D123 一份 Docker config.json，依 host 對帳密（#430）

- **帳密仍是一份檔案。** API 是 `registry_creds`，STS 是 `--registry-creds`。多個 host 是這份 `config.json` 裡的多筆 `auths`。
- **對 host 用 oras-go `credentials.Credential` 的現狀。** 檔案裡有這個 host 就用那一筆。沒有就匿名，不是錯誤。公開 image 不必給帳密。不另寫 matcher。帳密不進 etcd、不進 spec。

### E10-D124 `plain_http` 改成 host 清單（#430）

- **拿掉布林。** API 的欄位是 `plain_http_hosts`（`[]string`）。STS 的旗標是 `--plain-http-hosts`，逗號分隔，空的表示沒有。只有列出來的 host 走 HTTP，給 dev 的 registry:2。其他 host 走 HTTPS。
- **重複的 host、或不是 `host[:port]`，Load 或啟動失敗。** 舊欄位 `registry`、`plain_http` Load 不認得。
- **MD、TD 與 `mftiko-pack` 的布林不變**（E10-D126）。`mftiko-agent-latest` 仍是一個 `--registry` 加 `--plain-http`。

### E10-D125 STS 的 data repository 是完整路徑（#430）

- **`--data-repository` 是 `host[:port]/path`。** host 從這條路徑來，不依賴已刪的 `--registry`。空的仍表示非空 volume 一直重試，直到能命名一個 repository。有值但拆不出 host 與路徑，啟動失敗。
- **`state_out.repository` 是這條完整路徑。** 憑證與 plain HTTP host 清單跟拉策略 image 是同一份設定。

### E10-D126 `mftiko-agent-latest` 這張不改（#430）

- MD、TD 仍從既有的 worker repository 取最新版。`--registry`、`--registry-creds`、`--plain-http` 留著。repository 仍是不含 host 的路徑，位址由 `--registry` 接上。
- 這張不改 worker image 的發布位置，也不改 `mftiko-pack` 的 `--plain-http`。

### E10-D127 帳密錯或拒絕存取仍是 `registry_unavailable`（#430）

- **401、403（artifact reason `artifact_unauthorized`）不加錯誤碼。** 列出與建立都回既有的 `registry_unavailable`。
- **列出的 detail 仍是 `registry unavailable`。** 建立的 detail 仍是 `registry is unavailable`。回應不帶 registry 的內文。
- 永久的 `artifact_unavailable`（不是 unauthorized）在列出仍是 500 `internal`。STS 拉到 401 仍是永久錯誤，session `FAILED`，這張不改拉取的分類。

### E10-D128 runbook 與範例設定（#430）

- **runbook 說明對多個 host 各做一次 `docker login`，寫進同一份 `config.json`。** 沒有列出的 host 匿名。API 用 `registry_creds` 指這份檔。STS 用 `MFTIKO_REGISTRY_CREDS`。
- **dev、accept、drill 一起改。** API JSON 用 `plain_http_hosts`，`allow` 列具體 host 或前綴。STS 用 `MFTIKO_PLAIN_HTTP_HOSTS` 與完整的 `MFTIKO_STS_DATA_REPOSITORY`。MD、TD 的 `MFTIKO_REGISTRY` 與 `MFTIKO_PLAIN_HTTP` 留著。

### E11-D001 `mftiko` 只對 API 說話（E11-02，#354）

- `cmd/mftiko` 是命令列客戶端，只用 `contract/gen/api/v1` 的 Go client（E10-D003）。實作在 `internal/cli`。
- `mftiko-pack`（E09-D036）與 `mftiko-seal`（E06-D013）維持獨立工具。
- `internal/archtest`：`cmd/mftiko` 與 `internal/cli` 的依賴閉包裡沒有 etcd、NATS、Postgres、exchange、其他平面，也不用 `contract` 裡 API 以外的產碼。

### E11-D002 標準庫 flag（E11-02，#354）

- 標準庫 `flag`。分派是 `mftiko <資源> <動作>`，另有單字命令 `login`、`whoami`、`help`。
- 不加 cobra。一期不做 shell 補全。

### E11-D003 設定檔與 token（E11-02，#354）

- 設定檔是 `~/.config/mftiko/config.yaml`。路徑就是這個，不看 `XDG_CONFIG_HOME`。
- token 來自 `MFTIKO_TOKEN` 或設定檔，環境變數優先。`login` 的 token 只從 stdin 讀（E11-D006）。
- 權限寬於 0600 就拒絕，而且不讀內容。窄於 0600（例如 0400）可以讀。
- token 不作為旗標，也不出現在 stdout、stderr 與錯誤訊息。

### E11-D004 具名 context（E11-02，#354）

- 設定檔可以有多組具名 context。
- `--context` 或 `MFTIKO_CONTEXT` 選擇。兩個都設時必須相同。都沒設就拒絕，不挑一個預設 context。

### E11-D005 TLS（E11-02，#354）

- TLS 預設驗證憑證。`ca_file` 的 PEM 是唯一信任根，不加入系統信任庫，主機名仍要核對。不關閉驗證。
- context 的 `url` 路徑是 `/api/v1`。明文 HTTP 要 `insecure_http: true`，否則拒絕。

### E11-D006 `login` 與 `whoami`（E11-02，#354）

- `login` 從 stdin 讀 token，`GET /auth/me` 成功才把 token 寫進所選 context。失敗不改設定檔。
- `whoami` 只呼叫 `GET /auth/me`。
- 只用 `Authorization: Bearer`。不使用 cookie，也不呼叫 `POST /auth/login`。

### E11-D007 exit code（E11-02，#354）

- 0 成功。1 是用法與設定錯誤。2 是網路錯誤。
- API problem 的 `code` 對到固定的 exit code：`validation_failed` 10、`unauthenticated` 11、`not_found` 12、`conflict` 13、`not_terminal` 14、`referenced` 15、`algo_overlap` 16、`region_unavailable` 17、`storage_unavailable` 18、`registry_unavailable` 19、`internal` 20、`invalid_artifact` 21。
- 2026-10-24 修訂（使用者確認）：`invalid_artifact` 對應 21。其餘不變。
- 錯誤輸出同一行寫名稱與號碼。exit code 看 body 的 `code`，不看 HTTP status。

### E11-D008 輸出與 HTTP（E11-02，#354）

- 預設是表格，時間用本地時區。`-o json` 原樣寫出 API 的 JSON，不重編碼，所以時間維持 API 的 RFC 3339。
- 錯誤逐行列出 `errors[].path` 與 reason，並印回應的 `X-Request-Id`。`-v` 在成功時也印。
- HTTP 逾時預設 30 秒，`--timeout` 可改。
- `User-Agent` 是 `mftiko-cli/<版本>`。
- 沒有 body 的請求不補 `Content-Type`（E10-D110）。

### E11-D009 確認（E11-02，#354）

- 刪除類命令在 TTY 上詢問，提示 `[y/N]`。只有 `y` 與 `Y` 繼續。`--yes` 在 TTY 上不跳過詢問。
- 非 TTY 沒有 `--yes` 就拒絕（exit 1），而且不讀 stdin。
- 共用函式是 `cli.Confirm`，給 E11-04、E11-06 用。
- PROTOCOL 為 v1.55。

### E11-D010 分頁（E11-03，#355）

- 預設自動翻完全部頁，每一頁請求的 `limit` 是 1000。
- `--limit N` 限制總筆數上限。
- `--cursor` 只取指定的那一頁，供手動翻頁。
- **E11-D010 補充（使用者確認，2026-10-24）：** `--limit` 與 `--cursor` 同時給時為用法錯誤（exit 1）。

### E11-D011 自動翻頁的 JSON（E11-03，#355）

- 自動翻頁時 `-o json` 合併輸出為單一 `{"items":[...]}`。
- `unavailable_regions` 取各頁聯集。
- 不帶 `next_cursor`。
- 指定 `--cursor` 時照 E11-D008 原樣輸出該頁。

### E11-D012 部分可用（E11-03，#355）

- 有 `unavailable_regions` 或 `controller_absent` 時，stderr 只印一次警告，並列出缺少的 region。exit code 不變。
- 單一資源 503 照 `region_unavailable`（exit 17）處理。
- `registry_unavailable`（exit 19）的訊息明寫為 registry 連線問題。

### E11-D013 `latest` 顯示（E11-03，#355）

- `strategy list` 顯示 latest 的 tag 與 digest 前 12 碼。
- `strategy versions` 在 digest 等於 latest 的那一行標 `latest`。
- `strategy show` 印完整 digest。
- PROTOCOL 為 v1.56。
- **E11-D013 修訂（使用者確認，2026-10-24）：** 短 digest 統一為前 7 碼（不含 `sha256:` 前綴），`strategy list` 由 12 碼改為 7 碼，與 E11-D024 一致；併入 #367。
- **E10-D121 修改本條（#429）。** `strategy list <repository>` 印 tag 與完整 digest，digest 等於回應 `latest` 的列標 `latest`。`strategy versions` 與 `strategy show` 刪除。2026-10-24 的 7 碼修訂適用於當時的目錄列與 `session show`；tag 表改印完整 digest。

### E11-D014 憑證輸入（E11-04，#356）

- 憑證只從 `--key-file`、`--secret-file`、`--passphrase-file` 讀。旗標值是路徑，不是憑證。`-` 代表 stdin，最多一個。
- TTY 上沒給的必要檔案改為不回顯的互動輸入。HMAC 的 key、secret、passphrase 各讀一行。ed25519 的 secret 讀到 EOF，這樣 PEM 的換行才留得住。
- ed25519 的 PEM 原樣送出（bytes 與換行都保留）。HMAC 的值只去掉結尾一個換行；結尾的 CR LF 算一個。檔案與 `-` 先讀完整份內容再套這條。
- 憑證不進旗標值，也不出現在 stdout、stderr、錯誤訊息與 `-v` 的請求記錄。

### E11-D015 送出前檢查（E11-04，#356）

- 送出前讀 `GET /account-kinds`。`supported` 為 false，或種類不在清單裡，client 不送出，exit 10，訊息與 API 的 400 相同（`/kind` 的 reason 是 `is not accepted`）。
- 必要欄位依 `key_types` 與 `passphrase`。`passphrase` 為 true 則必填，為 false 則不能帶。key 與 secret 不可為空。
- `key_types` 只有一種時用那一種。多於一種時 `-key-type` 必須是其中之一；client 不自己挑。這個旗標的值不是憑證。
- `account create` 的 `--name`、`--kind`、`--td-instance` 必填。`--td-instance` 先對上 instance 名稱文法，再讀 `GET /instances/{name}`。不是 td 平面、不在 topology，或名稱不合法，client 不送出，訊息與 API 的 400 相同（`is not a td instance`、`is not in the topology`、`is not an instance name`）。

### E11-D016 更新（E11-04，#356）

- `account update` 先讀 `revision`，再以 body 的 `revision` CAS 寫回（E10-D037）。
- 給了任一憑證檔就換成完整的一組。少一欄就擋下，不送出。
- 一組都沒給就不送 `credentials`，沿用原密文（E10-D062）。單獨的 `-key-type` 是用法錯誤，不算換憑證。

### E11-D017 衝突不重試（E11-04，#356）

- 409 `conflict` 不自動重試，exit 13。stderr 提示重新執行。

### E11-D018 刪除（E11-04，#356）

- `account delete` 先讀 `revision`，DELETE 帶這個 `revision`。
- 確認用 `cli.Confirm`（E11-D009）。
- 409 `referenced` 列出 `referenced_by` 的 session，exit 15。
- stdout、stderr、錯誤與 `-v` 的請求記錄都不印憑證欄位。
- `account list` 與 `show` 讀取不回傳憑證（E10-D063）。列表分頁沿用 E11-D010–E11-D012（與 #355 並行）。PROTOCOL 為 v1.57。

### E11-D019 解析（E11-05，#357）

- 用 `gopkg.in/yaml.v3` 在節點層級轉成 JSON，所有數字保留原文、不經 float；JSON 視為 YAML 子集，YAML、JSON 檔與 `-f -` 走同一個解析器；重複 key 報錯。

### E11-D020 不明確的數字（E11-05，#357）

- 不明確的數字（`0755` 這類前導零、`0x1F`、`0o17`、`.inf`、`.nan` 等會產生非法 JSON 或語意不清者）報錯並指出行號，要求加引號；不猜成八或十六進位。

### E11-D021 布林照 YAML 1.2（E11-05，#357）

- 布林照 YAML 1.2：只有 `true`/`false` 是布林，`no`、`on`、`yes`、`off` 等一律為字串，不警告。

### E11-D022 冪等（E11-05，#357）

- 每次執行產生 UUID 作 `Idempotency-Key`，`--idempotency-key` 可指定；只在連線失敗或逾時時重試，最多 3 次，間隔 1/2/4 秒，沿用同一個 key；同 key 不同 body 的 409 說明「此 key 已用於不同內容」並印出 key；錯誤輸出一律附上 key。

### E11-D023 欄位錯誤（E11-05，#357）

- API 的 JSON Pointer 依節點位置對回檔案，印 `檔名:行:欄 路徑 訊息`；對不到時只印路徑。建立成功印 `session_id` 與解析到的 digest；client 端不重複 API 的驗證（E10-D012）。
- PROTOCOL 為 v1.58。

### E11-D024 `session show` 版面（E11-06，#358）

- `session show` 版面：spec 摘要（策略 tag 與 digest、STS instance、`created_at`（本地時間，E11-D008）、`created_by`）、狀態（`phase`、`reason`、重啟次數）、需求項目（每項 ack 狀態，`REJECTED`／`DEAD` 標出並附原因）、結束資訊（`outcome`、exit code、signal），最後附 log 位置。`session list` 篩選旗標為 `--phase`、`--active`、`--sts-instance`。
- **E11-D024 修訂（使用者確認，2026-10-24）：** session 只存 digest，不保留 tag（E09-D034），所以 `session show` 的策略欄改為顯示 id、名稱與 digest 前 7 碼（不含 `sha256:` 前綴），不反查 registry。`-o json` 保留完整 digest。

### E11-D025 log 位置（E11-06，#358）；由 E14-D006 修改

- log 位置印 `<sts_instance> <host>:<完整路徑>`；使用最後已知值時註明「最後已知，agent 目前不在線」。一期不讀 log 內容（E10-D015）。
- **E14-D006 修改（#436）：** 位置仍照上面印。CLI 以 `session logs`、`session export-logs` 經 API 讀內容。`--tail` 預設全部，`--latest` 取最新建立的 session。

### E11-D026 `session end`（E11-06，#358）

- `session end` 需確認：TTY 詢問 y/N，非 TTY 須 `--yes`；已結束的照樣成功。

### E11-D027 `session delete`（E11-06，#358）

- `session delete` 使用共用確認（E11-D009）；非 terminal 回 `not_terminal` 時 exit 14，並提示先執行 `session end`。

### E11-D028 `record list` 時間篩選（E11-06，#358）

- `record list` 時間篩選接受 RFC 3339 或 `YYYY-MM-DD`（本地時區午夜），不收相對時間；分頁照 E11-D010。
- PROTOCOL v1.59（與 #357 的 E11-D019–D023／v1.58 並行；後合併者 merge main）。

### E11-D029 SSE client（E11-07，#359）

- SSE client 自寫，用標準庫逐行解析 `event`、`id`、`data`，不加套件。

### E11-D030 重連（E11-07，#359）

- 重連：指數退避 1–30 秒，帶最後的 `Last-Event-ID`；45 秒（3 次心跳）沒收到任何內容視為斷線；收到 `reset` 時重新 list 再續接。`watch` 無限重試，`--wait` 受逾時限制。

### E11-D031 `session watch`（E11-07，#359）

- `session watch`：不帶 id 時，TTY 重畫表格；`-o json` 或非 TTY 每個事件輸出一行 JSON（NDJSON）。帶 id 時重畫 `session show` 的畫面；收到 `session_removed` 印一行說明，exit 0。

### E11-D032 `--wait`（E11-07，#359）

- `create --wait <phase|terminal>` 等到指定 phase；`end --wait` 等到 terminal；`--wait-timeout` 預設 10 分鐘。未到目標 phase 就已 terminal 時，依結束原因回 exit code。

### E11-D033 exit code（E11-07，#359）

- exit code：到達目標、`outcome=done` 或 `end_reason=ended_by_spec` 回 0。其餘：30 `failed`、31 `init_failed`、32 `crashed`、33 `hung`、34 `unknown`、35 `deps_failed`、36 `deps_timeout`、37 `invalid_spec`、38 等待逾時、39 等待中 session 被刪除。文件與輸出號碼、名稱並列。

### E11-D034 region 不可用（E11-07，#359）

- 等待期間 session 的 region 不可用：stderr 印警告並持續重試，直到逾時（exit 38）。
- PROTOCOL 為 v1.60。

### E11-D035 CLI 隨 `v*` tag 發佈到 GitHub Release（E11-08，#360）；由 E15-D009 修改

- **在 E11 發佈，不等 E12。** `release.yml` 加一個 `cli` job，在同一個 `v*` tag（或該 tag 的 `workflow_dispatch`）上建 `mftiko`，上傳到該 tag 的 GitHub Release。Release 不存在時由 job 建立（`--verify-tag`），已存在時以 `--clobber` 換掉 assets。只有這個 job 有 `contents: write`。
- **平台：** `darwin/arm64`、`darwin/amd64`、`linux/amd64`、`linux/arm64`。`CGO_ENABLED=0`、`-trimpath`、`-s -w`。
- **產物：** 每個平台一個 `mftiko_<tag>_<os>_<arch>.tar.gz`，裡面只有 `mftiko`；外加 `SHA256SUMS`。
- **同一支腳本。** `.github/scripts/build-cli.sh` 給 release、`make cli-dist` 與 CI 用；CI 的 `check` 只建不傳。腳本會執行本機平台的 binary，確認版本與注入的一致。
- **另外兩條安裝路徑：** `go install github.com/BullionBear/mftiko/cmd/mftiko@<tag>` 與 `make build`。
- 不採用：goreleaser（多一個工具與設定，這裡只有一個 binary）；macOS 簽章與公證、Homebrew tap（一期不做）；移到 E12 的 release 打包（使用者要等 E12 才拿得到 binary）。

### E11-D036 `mftiko version`（E11-08，#360）

- **`mftiko version` 與 `-version` 印同樣兩行：** `mftiko <版本> (commit <短 hash>, built <UTC 時間>)` 與 `api.v1 <info.version>`。`-o json` 印 `{"version","commit","built","api_version"}`。不讀設定檔，也不呼叫 API。
- **CLI 版本**來自 `internal/platform/buildinfo`，由 Makefile 與 release 以 `-ldflags` 注入 `git describe`（E01-D003 的同一套）。沒有注入時（`go install`，版本仍是 `dev`），改用 `debug.ReadBuildInfo` 的 module 版本，以及 `vcs.revision` 的前 7 碼（有的話）。這個退路只在 CLI，其他 binary 照舊。`User-Agent` 的 `mftiko-cli/<版本>`（E11-D008）用同一個值。
- **`api.v1` 版本**是 `internal/cli` 的常數 `APIVersion`。它是編譯時對應的 `contract/openapi/api/v1.yaml` 的 `info.version`（E10-D116），測試會讀那份檔案比對。不在執行時問伺服器。
- 不採用：`version` 同時呼叫 API 取伺服器版本。API 目前沒有版本端點，`version` 也應該在沒有設定時可用。

### E11-D037 CLI 文件在 `docs/cli.md`（E11-08，#360）

- **`docs/cli.md`** 內容：安裝（Release 與驗證 `SHA256SUMS`、`go install`、`make`）、`mftiko version`、設定檔與 context、token 與 `login`、以 `examples/strategies/hello` 走一次 session 檔、exit code 一覽（E11-D007、E11-D033）、常見錯誤。每個命令的細節以 `mftiko help <命令>` 為準，文件不重抄。
- README 只放一段 CLI 說明與連結。`docs/release.md` 加「CLI」一節，寫發佈流程。
- PROTOCOL 為 v1.61。

### E11-D038 CLI 的端到端驗收（E11-09，#361）

- **驅動程式沿用 `cmd/mftiko-api-accept`，加上 `cli main|faults|region <step>`。** 在 `deploy/accept` 的同一組環境裡跑（E10-D111），`accept.sh e2e cli ...`，`e2e-all` 一起跑。`accept.sh up` 把 `cmd/mftiko` 建進 runner。
- **CLI 是唯一的 client。** 驅動程式執行 `mftiko` binary，給它新的 `HOME` 與一個 context，先 `login`（token 從 stdin），之後不設 `MFTIKO_TOKEN`。結果讀 `-o json` 與 exit code。
- **請求經過記錄用的 reverse proxy。** context 的 `url` 指到驅動程式開的 proxy，proxy 轉到副本 a，並記下每個回應的 `X-Request-Id`、method、path、status。CLI 自己的分頁、SSE（`/events`）與重試都算在內，不依賴 CLI 的 `-v` 輸出。最後照 E10-D111 比對 `audit`。401 的那列沒有使用者。
- **安全：** CLI 的 stdout、stderr 若出現 token 或帳號憑證，該步就失敗（E11-D003、E11-D014）。
- **異常項目**與 exit code：`validation_failed` 10（附檔案行號）、`unauthenticated` 11、`not_found` 12、`not_terminal` 14、`referenced` 15、`algo_overlap` 16、`region_unavailable` 17；本地拒絕的數字與非 TTY 沒有 `--yes` 是 1，不送請求。
- 不採用：shell 腳本解析 CLI 的文字輸出（比對 audit 需要 CLI 送出的每個請求的 id，文字輸出不保證涵蓋分頁、SSE 與重試）；在 CI 跑（需要 testnet，同 E10-D112）。

### E13-D001 controller 的套件結構與依賴規則（E13-01，#262）

- 比照 E01-D010，決策與 I/O 分開：
  - `internal/controller` 是純邏輯：推導（N1）、gate 與綁定（N2、N3）、生命週期的 diff（N4、N5）。輸入是全域的 view，輸出是要寫與要刪的 key，不需要 cluster 就能測。
  - `internal/controller/daemon` 是接線：`state.Campaign` 與 `Term`、每個 region 的 `Mirror`、寫入。
  - `cmd/mftiko-controller` 只做組裝。
- 不沿用 `internal/platform/agent/runner`：它只連自己的 region，也綁著 Fleet 與 worker。controller 沒有 worker，要連每一個 region。
- `internal/archtest`：`internal/controller` 是一個平面，不 import 其他平面。根套件比照 agent 平面的純度規則，依賴閉包裡沒有 etcd、NATS、`state`、`natsx`、`shim`。整個平面不 import exchange、不用 `sym.v1`（E09-D007：不碰進程、不查 SYM）。
- 一期只部署一個 controller 進程。選主與 fencing 照做：systemd 重啟、舊進程還沒退出時，新舊兩個會短暫同時在跑。備援進程的數量與位置在 E12 定（#14）。

### E13-D002 `deps_timeout_s` 從指派第一次寫入起算（E13-01，#262）

- `SessionDemand` 加 `issued_at_ns`：controller 第一次寫這個 session 的指派時填，之後每次改寫都照抄，不改。期限是 `issued_at_ns + deps_timeout_s`。
- 理由：controller 換手不能重新計時。否則一直輪換的 controller 會讓 session 永遠等下去。
- 用的是牆上時鐘，跨進程比較。時鐘偏差在秒級，相對於期限可以接受。
- `deps_timeout_s` 是 0 時取 60 秒。
- 不採用：leader 自己的單調時鐘（換手就歸零）；`updated_at_ns`（`bindings` 每變一次就更新）；spec 的寫入時間（etcd 的 revision 不帶時間，spec 也可能在 controller 看到之前就寫好很久）。

### E13-D003 spec 只有 `desired_state` 可以改（E13-01，#262）

- `SessionSpec.desired_state` 改成 `ENDED`：controller 以 `ended_by_spec` 結束 session（N3）。
- `desired_state` 以外的欄位不應改（E09-D004）。API 已經擋下。改了，controller 不重新推導，只記 log；`SessionDemand.spec_revision` 仍是最初推導用的那一版。agent 不另特別處理（E09-D047）。
- spec 在 session 進入 terminal 之前被刪掉，當成 `ended_by_spec`：照 N3 結束，再照 N4 刪除。spec 不在不代表 worker 已經停了，所以不能直接刪需求。

### E13-D004 不合法的 spec 以 `invalid_spec` 結束（E13-01，#262）

- 例如：`md_instance`、`td_instance` 不在 topology 裡；同一個 spec 有重複的 feed、selector 名稱或帳號。
- controller 寫一筆 `desired = ENDED` 的 `SessionDemand`，`end_reason` 是 `invalid_spec`，不寫任何需求項目。STS agent 照常寫入 terminal 的 `SessionStatus`，使用者在 console 看得到原因。
- API server 的驗證在 E10。之後 API 擋下大部分，controller 這一條仍保留，因為 topology 可能在 spec 寫入之後才改。
- 商品是否存在仍由 MD、TD 以 `REJECTED` 回報，controller 不檢查（E09-D007）。

### E13-D005 每個 region 在第一次寫入之前才 fence（E13-01，#262）

- 當選之後不等所有 cluster 都 fence 完才開始。每個 region 在 controller 第一次要寫那裡時，才放 writer key（W2 第 3 步，`Term.Fence` 原本就是每個 region 第一次用到時取得，結果快取）。
- 不變的條件：在一個 cluster 裡，fence 之前不寫入。
- 理由：要求全部 fence 完才開始的話，只要有一個 region 連不上，其他 region 的 session 也全部停擺。
- 某個 region 發現 writer key 已經是更大的 epoch（`ErrSuperseded`），term 就結束，controller 停下所有動作並退出，不只停那個 region。

### E13-D006 和某個 region 斷線時保持現狀（E13-01，#262）

controller 不和 agent 連線，只連每個 region 的 cluster（E00-D005），所以斷線的單位是 region。agent 自己和 etcd 斷線時，controller 看到的是它的 `AgentRecord` 消失，照 §3.5 處理，不是這一條。

- controller 對每個 region 的 Mirror 看 `LastContact`。超過門檻（預設 15 秒，部署設定，與 W8 相同）就是過舊。
- **寫入依 region 分開排隊。** 一個 region 寫不進去，不擋其他 region。
- **過舊的 region 不做減法：**
  - 不刪那裡的需求項目、不刪那裡的 `SessionDemand`；
  - 不因為那裡的 ack 結束 session，因為看到的可能是舊值；
  - STS 的 region 過舊時，那裡的 session 照 N4 繼續等。
- **`deps_timeout_s` 照常計時。** 還沒 `Start` 的 session 結束的代價小；session 所在的 STS region 寫不進去時，結束也跟著排隊。
- 恢復聯繫之後的下一輪 reconcile 再對齊，只改有差異的（N6）。
- 理由同 E01-D009：錯誤的代價不對稱，多留一筆需求最壞是浪費，誤刪會停掉正在交易的 session。

### E13-D007 topology 與 home region（E13-01，#262）

- controller 從部署設定讀 `state.Topology`，和 `mftiko-etcd-roles` 讀同一份。
- home region 是 `Topology.Instances[ControllerInstance]`。
- topology 決定每個 instance 的 key 寫進哪個 cluster（E00-D005）。不在 topology 裡的 instance 依 E13-D004 處理。

### E13-D008 只在值改變時寫（E13-01，#262）

- 推導出的 `SessionDemand`、`DemandEntry` 和 etcd 裡的比較，不看 `updated_at_ns`；相同就不寫。
- `bindings` 跟著 ack 變，一期不加 debounce。

### E13-D009 N4 以 `WorkerStatus` 判斷 session worker 已經消失（E13-01，#262）

- 看 `/sts/{inst}/worker/{h}`，`h = WorkerHash("sts/<session_id>")`。key 不存在，就表示 session worker 已經消失（W7.1：`WorkerStatus` 在 worker 確定不在之後才刪）。
- STS 的 `AgentRecord` 不在時照最後的值，也就是一直等。instance 永久下線見 E12-D033–E12-D037 與 PROTOCOL §3.7：沒有 `/retired/{inst}` 時仍是一直等。

### E13-D010 全量 resync 每 60 秒一次（E13-01，#262）

- Mirror 本來就是 level-triggered，變化會觸發 reconcile。另外定期對全部 session 做一次完整比對（N5），預設 60 秒，是部署設定。
- 每次當選，在第一次寫入之前也做一次。

### E13-D011 E13 的拆票（E13-01，#262）

| 票 | 內容 |
|---|---|
| E13-02 | 骨架：`cmd/mftiko-controller`、`internal/controller{,/daemon}`、讀 topology、參選、fence、`PutRecord`，term 結束就退出；archtest 規則；能起多個 cluster 的 etcdtest |
| E13-03 | Intake：每個 region 的 Mirror（spec、status、ack、agent、worker），組成全域 view；過舊的判斷（E13-D006） |
| E13-04 | 推導 N1 與寫入：只寫差異、依 region 排隊、`invalid_spec`（E13-D004） |
| E13-05 | Gate 與綁定 N2、N3：`bindings`、`start`、§3.5 的表、`deps_timeout`（E13-D002） |
| E13-06 | 生命週期 N4、N5：照順序刪除、比對回收孤兒、全量 resync |
| E13-07 | 故障演練的整合測試：controller 停機、region 斷線、雙 controller |

- 順序：02 → 03 → 04 → 05、06 可以平行 → 07。
- `state.proto` 的 `issued_at_ns` 與 `invalid_spec` 在這張票改（仍是草案，E9 定案）。

### E13-D012 controller 讀同一份部署 JSON（E13-02，#263）

- `mftiko-controller` 用 `-in` 讀 `mftiko-etcd-roles` 同一份部署 JSON，從裡面取 `state.Topology`。`-` 是標準輸入。JSON 裡的 `planes` 照單接受，這一期不用。
- etcd 的使用者是 `controller`。密碼來自 `MFTIKO_ETCD_PASSWORD`，`-etcd-password` 可以覆寫，和各 agent 一樣。
- 不另定設定格式。home region 仍是 `Topology.Instances[ControllerInstance]`（E13-D007）。

### E13-D013 骨架就有依 region 的寫入佇列（E13-02，#263）

- daemon 現在就實作每個 region 一條寫入佇列（E13-D006）。一個 region 正在寫或寫失敗，不擋其他 region。
- 每個 region 的 `Term.Fence` 只在那裡第一次寫入之前（E13-D005）。`Campaign` 在返回前只 fence home region（W2 第 3 步）；其他 region 在第一次寫到那裡時才 fence。
- `Controller.Put` 與 `Controller.Delete` 是測試用的寫入入口。正式的 `mftiko-controller` 不呼叫它們。E13-04 把推導接到同一條佇列。
- 非 home region 連不上時，controller 仍當選。那個 region 的寫入失敗，其他 region 照常寫，term 繼續。`ErrSuperseded` 仍結束整個 term。

### E13-D014 AgentRecord 當選後寫一次（E13-02，#263）

- 沿用 `state.v1.AgentRecord`：`plane`、`instance`、`epoch`、`release`、`started_at_ns`。
- 編碼與 `internal/platform/agent/runner` 相同：`proto.MarshalOptions{Deterministic: true}`。`plane` 是 `ctl`，`instance` 是 `controller`。
- 當選之後 `Term.PutRecord` 一次，不刷新。正常停下（SIGTERM）時 `Term.Close` revoke（W7）。

### E13-D015 term 結束與行程結束（E13-02，#263）

- `ErrSuperseded` 或選主 session 斷線：停下所有寫入，以非 0 結束。systemd 用 `Restart=on-failure` 再拉起。
- SIGTERM：revoke 後以 0 結束。SIGINT 走和其他 agent 相同的路徑。
- **unit 的重啟政策由 E12-D019 取代：** 全部 unit 是 `Restart=always`。結束碼不變。
- 還沒當選的行程停在 `Campaign`，直到當選或收到 SIGTERM。

### E13-D016 etcdtest 一次起多個 region（E13-02，#263）

- `etcdtest.StartRegions` 依給定的 region 名稱，依序各起一座 server。回傳的位址就是 topology 裡那個 region 的端點；instances 由呼叫端填。
- 這個 helper 不回傳 `state.Topology`：`state` 的測試 import `etcdtest`，回傳 topology 會形成 import cycle。
- `state` 既有的 `twoServers` 改成呼叫它。E13-03 起沿用同一個 helper。

### E13-D017 Mirror 的切法（E13-03，#264）

- 每個 instance 一個 `state.Mirror`，監看整個 instance 前綴（如 `/sts/{inst}/`），再依子路徑（`spec/`、`status/`、`demand/`、`worker/`、`ack/`、`agent`）分類進 view。不為每個子路徑各開 Mirror。
- STS 收 `spec/`、`status/`、`demand/`、`worker/`、`agent`。MD、TD 收 `demand/`、`ack/`、`agent`。election、epoch、writer、incarnation、selector、placement、account 不進 view。
- 前綴裡的 plane 用部署 JSON 的 `planes`，和 `mftiko-etcd-roles` 同一份（E13-D012）。topology 只決定 instance 在哪個 region（E13-D007），不記 plane。controller 自己的 instance 不開 Mirror。
- 要 watch 的 instance 是 topology 裡除了 controller 以外的每一個。

### E13-D018 未完成首次同步的 region（E13-03，#264）

- `Synced` 未關閉前，該 region 視為過舊：不做任何減法，且完全不往該 region 寫入，避免還沒讀到既有的 `SessionDemand` 就蓋掉 `issued_at_ns`（E13-D002）。
- 其他已同步的 region 照常 reconcile，不等它。
- 寫入佇列在該 region 每個 instance 的 Mirror 都 `Synced` 之前不呼叫 `Term.Put`／`Delete`，因此也不 fence（E13-D005）。
- 過舊門檻預設 15 秒，是 daemon 設定（`StaleAfter`）。0 取預設。和 W8 相同（E13-D006）。

### E13-D019 變化合併（E13-03，#264）

- 單一 reconcile 迴圈。任何 Mirror 的 `Notify` 設一個 dirty 旗標；一輪跑完若旗標已設就立即再跑，不設最短間隔。
- 60 秒全量（E13-D010）走同一迴圈。當選後的第一輪是全量，在推導的寫入之前。
- 全量間隔預設 60 秒，同過舊門檻放在 daemon 設定（`Resync`）。0 取預設。

### E13-D020 跨 region 不一致（E13-03，#264）

- 接受。view 帶每個 region 的 revision 與是否過舊。
- 純邏輯層不得依賴跨 region 原子性，前提同 E13-D006。

### E13-D021 解不開的值（E13-03，#264）

- 不進 view。記 warn log，並計數；view 另列這些 undecodable keys。
- controller 不寫也不刪這些 key，同過舊，不做減法。

### E13-D022 spec 被改（E13-04，#265）

- `SessionDemand` 已存在且 spec 的 revision 與其 `spec_revision` 不同（改動不只 `desired_state`）時，以 etcd 裡現有的需求項目為準：不新增、不刪除，只記一次 log。不以 `invalid_spec` 結束。
- `desired_state` 以外的欄位不應改，API 已經擋下（E13-D003）。這一條仍是 etcd 被直接改寫時的防守，不另特別處理。agent 側見 E09-D047。

### E13-D023 比較忽略的欄位（E13-04，#265）

- 比較推導值與 etcd 現值時，忽略 `SessionDemand.updated_at_ns` 與 `DemandEntry.spec_revision`（E13-D008 的延伸）。

### E13-D024 `bindings`（E13-04，#265）

- #265 就寫好 `bindings` 的目標（feed／selector／api_id 與提供的 instance），`status` 留空，`start` 固定 false。
- #266 只填 ack 狀態與 `start`。

### E13-D025 寫入順序（E13-04，#265）

- 不保證跨 region 先後。需求項目與 `SessionDemand` 同一輪各自進該 region 的佇列。
- 正確性靠 #266 的 `start` 關卡。

### E13-D026 一開始就 `ENDED` 的 spec（E13-04，#265）

- 尚無 `SessionDemand` 而 spec 的 `desired_state` 已是 `ENDED`：什麼都不寫。
- 已有指派者由 #266 以 `ended_by_spec` 結束。

### E13-D027 不合法 spec 多一項（E13-04，#265）

- `md_instance` 在 topology 裡但部署 JSON `planes` 標的不是 md（`td_instance` 不是 td 同理），也以 `invalid_spec` 結束（E13-D004 的延伸）。

### E13-D028 結束不可逆（E13-05，#266）

- `desired` 變成 `ENDED` 後不再改回 `RUNNING`。
- `end_reason` 保留第一次寫下的值，之後 ack 恢復或 spec 改回都不理會。

### E13-D029 結束原因優先序（E13-05，#266）

- 同一輪多個原因時 `ended_by_spec` > `deps_failed` > `deps_timeout`。

### E13-D030 結束後不更新 `bindings`（E13-05，#266）

- `ENDED` 之後不再抄 ack 進 `bindings`，省掉不必要的寫入。

### E13-D031 逾時計時器（E13-05，#266）

- reconcile 迴圈依最早到期的 `issued_at_ns + deps_timeout_s` 設計時器，到期即跑一輪，不等 60 秒全量。

### E13-D032 過舊 region 的 ack（E13-05，#266）

- 除了不因過舊的 ack 結束 session（E13-D006），也不因過舊的 `READY` 放行 `start`。
- `bindings` 照抄。

### E13-D033 刪除等確認（E13-06，#267）

- 刪完 MD、TD 需求項目後，要在 view 裡確認它們已不存在，才刪 `SessionDemand`。
- 任一 provider region 過舊時 `SessionDemand` 跟著等，讓 N4 順序跨 region 成立。
- 過舊的 region 本身仍不做減法（E13-D006）。

### E13-D034 孤兒的判定（E13-06，#267）

- 需求 key 只帶 `session_id`，須查所有 STS instance 的 `SessionDemand`。
- 只有所有 STS region 都已同步且不過舊、確定沒有任何 instance 有這個 session 時，才當孤兒刪除。

### E13-D035 controller 只刪自己寫的 key（E13-06，#267）

- 只刪需求項目與 `SessionDemand`。
- spec（API）、`SessionStatus`、ack、`WorkerStatus`（各 agent，W1）一律不刪。

### E13-D036 平行開工（E13-05，#266）

- #266 與 #267 同時做，後合併者負責 rebase 解衝突。

### E13-D037 寫入不擋 reconcile（E13-07，#268）

E13-07 的測試找到：region 同步之後才斷線，在過舊判定（E13-D006，預設 15 秒）之前送出的寫入會卡到恢復為止。原本一輪 reconcile 等所有 region 的寫入完成，於是整個迴圈停住，其他 region 不再推導，view 也不再更新，斷線的 region 永遠不會被標成過舊。

- 一輪 reconcile 把推導、gate、生命週期的寫入交給各 region 的佇列就繼續，不等結果。佇列仍依 region 分開、依序執行，同一 region 的 put 在 delete 之前（E13-D025 不變）。
- 佇列裡同一個 key 只留最新的值；和正在執行的寫入相同的值不再排。卡住的 region 每個 key 最多一筆。
- 這個 term 寫過、但 view 對該 region 讀到的 revision 還沒到那次寫入的值，再推導出相同的值時不寫（E13-D008 的延伸）。比較是位元組相等，所以 `SessionDemand` 和本 term 上次排入的值只差 `updated_at_ns` 時，沿用上次的 `updated_at_ns`（與 E13-D002 沿用 `issued_at_ns` 同理），否則 view 落後的那一輪會多寫一次。
- 寫入失敗只記 log。下一輪仍要這個值，就會再排。
- `Controller.Put`、`Delete`（測試入口，E13-D013）仍等結果。
- provider 的 region 尚未完成首次同步時，那裡的 ack 沒有讀過。`bindings` 保留 etcd 裡現有的 status，不清成空值，也不因此放行 `start`（E13-D018、N6）。新 leader 當選時 provider region 同步得比 STS region 慢，原本會把已放行 session 的 `bindings` 清空。

### E13-D038 home region 不一致（E13-07，#268）

兩個 controller 進程讀到不同的 `instances.controller`（E13-D007），例如搬 home region、新舊進程重疊時。

- 兩邊各在自己的 home 當選，都拿到 epoch。每座 cluster 的 writer key 由先 fence 的一方持有；同 epoch 不能取代（W2 第 3 步的條件是「小於」），另一方寫到那裡就 `ErrSuperseded`，退出後由 systemd 重啟（E13-D015）。
- 結果是每座 cluster 只由 home 在那裡的 controller 寫入。沒有要改的東西時兩邊都不寫，不會一直重啟；跨 region 的一次變更最多讓非 home 的一方重啟一兩次，需求最後會補齊。
- 一期只部署一個 controller（E13-D001），這是設定錯誤，由人處理。備援進程的 home 怎麼設，在 E12 定（#14）。

### E13-D039 舊 leader 的時間窗（E13-07，#268）

- leader 和 home 斷線後，server 端的 lease 先過期，新 leader 當選；舊 leader 的 term 要到 `concurrency.Session` 自己判定 lease 過期才結束。client 從收到 keepalive 回應起算 TTL，server 從收到請求起算，所以 client 一定比較晚。E13-07 量到 0.5 到 1 秒（TTL 2 秒）。
- 這段時間裡，新 leader 還沒寫過的 region（E13-D005）仍是舊的 writer key，舊 leader 寫得進去。寫入的都是舊 leader 從它的 view 推導的值，新 leader 下一輪照差異改回。結束 session 不可逆（E13-D028），但要寫在 STS 的 region，舊 leader 連得到那裡時才可能。
- 修法在平台層：term 以最後一次 keepalive 回應減去一個 keepalive 間隔為期限，提前結束（#277）。agent 共用 `Term`，一起修。修好之前接受這個時間窗；`TestScenarioZombieWindow` 斷言它小於一個 TTL。
- **已由 #277 修正（E01-D016）。** term 在 server 端選主 lease 過期之前結束；`TestScenarioZombieWindow` 斷言 term 結束早於選主 key 被刪。上面描述的是修正前的時間窗。

### E12-D001 演練用兩台真實 Linux VM、兩個 region（E12-01，#374）

- **testnet 演練跑在真實的 Linux VM 上**，不在 compose 裡跑 systemd。unit 檔就是正式用的那一份；`KillMode=process`、主機重開、拔網路（iptables）都照真實環境的方式做。`deploy/accept` 照舊給 PR 與單項驗收用。
- **兩台主機，一台一個 region：**

  | 主機 | region | 服務 |
  |---|---|---|
  | A | home | etcd、NATS、Postgres、SYM、md.fetch、API、console、controller、md-agent、td-agent、sts-agent |
  | B | 遠端 | etcd、sts-agent；之後要演練遠端的 MD、TD 時再加 md-agent、td-agent |

- 每個 region 的 etcd 一期是單一 member。E12-06 的 jwt 金鑰分發、E12-07 的備份與還原，程序照多 member 寫，演練用單一 member 走過。
- **NATS 一期只有 A 上的一台 server**，B 的 worker 跨 region 連過去。NATS 多 region 的拓撲仍是 P16，不在 E12。
- Postgres、SYM、md.fetch、API 都只在 home region。registry 是 `ghcr.io`，所有 region 共用（E09-D010）。
- 跨 region 斷線的演練，用防火牆擋 A 和 B 之間的流量，走的是真實網路。
- 不採用：一台主機跑兩個 region（碰不到真實的網路延遲與斷線）；三台（一期只有一個 controller，E12-D006，第三台用不到）。

### E12-D002 host binary 隨 `v*` tag 發佈成 GitHub Release 的 tarball（E12-01，#374）

- **產物：** `mftiko-server_<tag>_linux_amd64.tar.gz`，跟 CLI 的 tarball 傳到同一個 Release（E11-D035）。只有 `linux/amd64`，同 E09-D090。
- **內容：**
  - `bin/`：`mftiko-md-agent`、`mftiko-td-agent`、`mftiko-sts-agent`、`mftiko-shim`、`mftiko-controller`、`mftiko-api`、`mftiko-sym`、`mftiko-mdfetch`、`mftiko-migrate`，工具 `mftiko-etcd-roles`、`mftiko-retire-clean`、`mftiko-seal`，以及 `mftiko-release`（E12-D022）。
  - `web/`：console 的建置結果（`make web`），給 `mftiko-api -web` 用。
  - `systemd/`：E12-05 的 unit 檔。
- 不放：conn 與 account worker（agent 從 registry 拉，E12-02）；CLI（自己有 tarball）；`*-accept`、`mftiko-conformance`、`mftiko-nats-dev`、`mftiko-pack`（開發與 CI 用）。
- **一份 `SHA256SUMS`**，涵蓋 CLI 與 server 的 tarball。兩份同名的檔案會互相覆蓋，所以兩種 tarball 在同一個 job 裡建，再一起算。
- 建置照 E11-D035：`CGO_ENABLED=0`、`-trimpath`，`-ldflags` 注入 `internal/platform/buildinfo`。每個 binary 的 `-version` 都印這個 tag。腳本與 `make` 目標在 E12-04 做，CI 只建不傳。
- 部署機上不需要 Go 與 Node。
- 不採用：在部署機上 checkout tag 再建（每台機器都要有 Go 與 Node，同一個 tag 在不同機器上建出來的東西也不保證相同）；容器 image（agent 不跑在 container 裡，#14）。

### E12-D003 目錄配置：每版一個目錄，`current` 是 symlink（E12-01，#374）

- **binary：** `/opt/mftiko/releases/<tag>/` 放解開的 tarball（`bin/`、`web/`、`systemd/`），內容唯讀。`/opt/mftiko/current` 是指到其中一版的 symlink。unit 一律執行 `/opt/mftiko/current/bin/…`，agent 的 `-shim` 也指到 `current/bin/mftiko-shim`。
- **換版：** 解開到新的 `releases/<tag>/`，以 rename 換掉 `current`（先建暫時的 symlink 再 rename，所以是原子的），然後重啟 unit。不覆寫任何舊檔：已經在跑的 shim 與 worker 用的是舊目錄裡的 inode，繼續跑（E01-D003）。新 agent 之後 spawn 的 shim 才是新版。回退就是把 `current` 指回舊的目錄，再重啟。
- **清理：** 保留最近 3 版，加上 `current` 指的那一版。只要還有進程的 `/proc/<pid>/exe` 指在某個版本的目錄裡，那個版本就不刪；長時間跑著的 shim 可能讓很舊的版本一直留著，這是預期的。腳本在 E12-04。
- **狀態：** `/var/lib/mftiko/<服務>/` 是各服務的 `-work-dir` 與 `${DATA}`（socket、結束檔、log、artifact 的層與 rootfs、session）。換版不動它。`${WORK_DIR}/log` 的 group 依 docs/operations.md。
- 不採用：直接覆寫 `/usr/local/bin`（換版時跑著的 shim 會跟著被換掉，或者是 text file busy）；套件管理器的 deb、rpm（一期只有自己用，多一層打包）。

### E12-D004 設定與祕密放在 `/etc/mftiko/<服務>/`，不用 `LoadCredential=`（E12-01，#374）

- **位置：** 每個服務一個目錄 `/etc/mftiko/<服務>/`，擁有者 `root:<服務的使用者>`，0750。設定檔（topology JSON、API 的 `-config`、端點 JSON）0640；祕密檔（NATS creds、NATS signing key、seal key、registry 的 Docker config.json）0600，擁有者是服務的使用者。
- **etcd 密碼**以 `EnvironmentFile=/etc/mftiko/<服務>/env`（0600，root）帶進 `MFTIKO_ETCD_PASSWORD`，不寫在 unit 的 `ExecStart` 裡，`systemctl show` 看不到。
- **不用 `LoadCredential=`：** 它的目錄（`$CREDENTIALS_DIRECTORY`）在 unit 停下時就被刪掉。agent 把 creds 檔的路徑交給 worker（例如 conn worker 的 `MFTIKO_NATS_CREDS`），而 worker 在 agent 停下之後還會繼續跑，之後重連時就讀不到檔案了。
- 輪換：改檔案再重啟 unit。worker 讀的檔案在原地改寫，不會換路徑。各種祕密的輪換程序在 E12-06（etcd）與 runbook（E12-11）。
- **接受的風險：** worker 跟 agent 是同一個 Unix 使用者（E12-D005），所以策略程式讀得到同一個使用者的祕密檔，也讀得到 shim 從 agent 繼承的環境（`/proc/<pid>/environ`）；worker 自己的環境只有 `Launch` 給的那些。系統是單人使用，策略是自己寫的（E04-D016），一期接受。worker 改用另一個使用者是二期的事。

### E12-D005 每個服務一個 Unix 使用者（E12-01，#374）

- **使用者：** `mftiko-md`（md-agent）、`mftiko-td`（td-agent）、`mftiko-sts`（sts-agent）、`mftiko-controller`、`mftiko-api`、`mftiko-sym`（SYM 與 md.fetch）。都是 system user，沒有 login shell。shim 與 worker 沿用 agent 的使用者；artifact 解出來的檔案也屬於 agent 的使用者（E09-D051）。
- **好處：** TD 的 seal key 與帳號憑證只有 `mftiko-td` 讀得到，跑在 `mftiko-sts` 底下的策略讀不到；每個服務也只有自己的 etcd 密碼，對應 RBAC 的 user（PROTOCOL §3.2 W1）。
- **運維的 group `mftiko-ops`：** 照 docs/operations.md，`${WORK_DIR}/log` 設 `chgrp mftiko-ops` 加上 setgid 2750。運維的帳號加進這個 group，不必 sudo 就能讀 log。服務本身不在這個 group 裡。
- etcd、NATS、Postgres 用各自套件或官方 binary 的預設使用者，不在這份清單裡。
- 不採用：所有服務共用一個 `mftiko` 使用者（策略就讀得到 TD 的 seal key）。

### E12-D006 一期只有一個 controller 進程，放在 home region（E12-01，#374）

- 維持 E13-D001：一期只部署一個 controller，跑在主機 A 上，由 systemd 的 `Restart=always` 拉起來。選主與 fencing 照做；重啟時新舊兩個進程會短暫同時在跑，選主保證只有一個寫得進去。
- **home region** 是主機 A 的 region。部署 JSON 的 `instances` 裡，`controller` 對應到它（E13-D007、E13-D012）。
- controller 停機期間，跑著的 session 不受影響，需求與指派維持現狀（E09-D001）；新的 session 等它回來，API 回報 `controller_absent`（E10-D014）。演練 E12-12 會驗。
- 不採用：home region 兩個進程（主機 A 一倒兩個一起倒，要再多一台主機才有意義）；每個 region 各一個（home region 失聯時要另外處理 home 不一致，E13-D038）。兩者留到二期。

### E12-D007 E12 的其他開放問題各歸各的票（E12-01，#374）

- 主機上每個服務的 `-work-dir` 與資料路徑依 E12-D003。unit 檔的細節（`TimeoutStopSec`、`ExecStop`、cgroup）在 E12-05（#378），已由 E12-D016–E12-D021 定案。
- etcd 的 TLS、密碼與 jwt 金鑰的輪換在 E12-06（#379）。在那之前，兩台主機之間的 etcd 與 NATS 流量只走私有網路或 VPN。
- 監控在 E12-10（#383）。

### E12-D008 md-agent、td-agent 在背景追蹤 `latest`（E12-02，#375）

- **每個 agent 一個 goroutine**（`internal/platform/agent/latest`，三種 agent 共用 `internal/platform/artifact`）。它在 agent 啟動時，以及之後每 60 秒（`-artifact-poll`），把 `latest` tag 解析成 digest（E09-D087）。digest 沒拉過，就照 PROTOCOL §8.3 第 1–3 步拉取、驗證、解開。kind 必須是 `md.conn`／`td.account`，contracts 是 `worker.v1` 加上 `mdctl.v1`／`tdctl.v1`。
- **spawn 用最後一個通過驗證的 digest**，`Launch` 依 §4.6 從它的 manifest 換算出來：`dir` 是 rootfs，`path` 是 rootfs 底下的 `bin/worker`。reconcile 不呼叫 registry，不會因為 registry 慢而卡住；代價是 `latest` 換了之後，最多晚一個輪詢間隔才被 spawn 用到。
- **跑著的 worker 不換**：換版不排程換 incarnation，換 agent 也不重啟 worker（P9、E01-D006）。舊的 rootfs 留在磁碟上，由 GC 處理（E12-03，#376）。
- **看得到版本的地方：** `WorkerStatus.release` 是 worker 在 Hello 裡回報的版本，也就是 build 時戳進去的 git tag，不另外加 digest 欄位。digest 寫在 agent 的 log（`worker artifact ready`），也看得到進程的 `/proc/<pid>/exe`（`${DATA}/artifacts/<digest>/rootfs/bin/worker`）。
- 不採用：每次 spawn 都同步解析一次 `latest`（spawn 在 reconcile 裡，registry 慢就會拖住整個平面）。

### E12-D009 拉取失敗時保持原狀，ack 寫出原因（E12-02，#375）

- **暫時錯誤**（registry 連不上、5xx）：以 1–60 秒的退避重試，上限是輪詢間隔。
- **artifact 內容的永久錯誤**（kind、平台、contracts、manifest、大小、不安全的層）：記下這個 digest，不再拉它。digest 的內容不會變，要修就推一個新的 `latest`。401、403、404 是 registry 端的問題，下一輪照常重試。
- **已經有就緒的版本時**，新的 `latest` 失敗只寫 warn log，spawn 繼續用就緒的那一版。
- **一個就緒的版本都沒有時**，spawn 在 `Launch` 失敗，照 reconcile 的動作重試（1–30 秒，`DefaultRetry`），不算進 restart policy，也不會放棄。這個 worker 的需求項目維持 `PENDING`，`reason` 從 `starting` 換成 artifact 的 reason：還在拉第一版時是 `artifact_pulling`，其他是 §8.3 的 reason（`artifact_unavailable`、`artifact_wrong_kind` 等）。不寫 `REJECTED` 或 `DEAD`，因為錯的不是需求，推一個好的 `latest` 就會恢復。
- 不採用：拉不到就 `REJECTED`（需求方要刪掉再重放，而且原因跟需求本身無關）。

### E12-D010 正式環境用 `-registry`，`-conn`、`-account` 只給開發與測試（E12-02，#375）

- **flag：** `-registry`（host[:port]）、`-registry-creds`（Docker config.json）、`-plain-http`、`-data`（`${DATA}`，有 `-registry` 時必填）、`-repository`（預設 `bullionbear/mftiko-md-conn`、`bullionbear/mftiko-td-account`）、`-max-artifact-bytes`（預設 1 GiB）、`-artifact-poll`（預設 1 分鐘）。意思和 sts-agent 的同名 flag 相同（E09-D010、E09-D033）。E10-D124 之後這句只適用於 MD、TD：STS 不再有 `--registry` 與布林 `--plain-http`。
- **`-conn`、`-account` 保留**，給開發與 Go 測試直接執行本機的 binary，Launch 照舊是 `dir` `/`。跟 `-registry` 二選一，兩個都給或都不給，agent 都拒絕啟動。MD、TD 的 systemd unit（E12-05）用 `-registry`。STS 已拿掉（E10-D122）。
- **`deploy/accept` 改成從 registry 拉。** `accept.sh up` 用 `mftiko-pack -kind` 把剛建好的 conn、account 推到 compose 裡的 `registry:2`，tag 是 `latest`；兩個 agent 用 `-registry registry:5000 -plain-http`。accept 工具改以 worker 環境裡的 `MFTIKO_PLANE` 找 conn 與 account worker：從 artifact 起來的 worker 執行的是 `bin/worker`，不能再看 `comm`。

### E12-D011 STS 查哪些策略的 `latest`（E12-03，#376）

只查磁碟上有 rootfs 的 repository，不抄 API 的策略清單。解開 rootfs 時記下它的 repository。此改動前解開、沒有紀錄的 rootfs 照舊只保留被釘住的，不另外處理。

### E12-D012 STS 查詢頻率（E12-03，#376）

每個 repository 在背景解析 `latest`，啟動時與之後每 60 秒（`-artifact-poll`，同 E12-D008）。GC 只讀快取結果，reconcile 不呼叫 registry。

### E12-D013 解析失敗（E12-03，#376）

registry 不可用、`latest` 不存在、或尚未成功解析過，該 repository 在磁碟上的 rootfs 全部保留，直到下次解析成功。

### E12-D014 MD、TD 的 keep（E12-03，#376）

正在跑的 worker 用的 digest、正在拉或解開中的 digest，加上 `latest.Source` 最後一個通過驗證的 digest。最新一次拉取失敗、尚未解開完成的版本不在磁碟上，不需保留。

### E12-D015 MD、TD 的 GC 時機（E12-03，#376）

與 STS 相同，每輪 reconcile 呼叫一次（只做本機檔案操作）；失敗只寫 warn log。

### E12-D016 agent 的 cgroup：`KillMode=process`，shim 留在同一個 cgroup（E12-05，#378）

- md-agent、td-agent、sts-agent 的 unit 設 `KillMode=process`。shim 呼叫 `setsid` 之後仍在 agent 的 cgroup 裡。`systemctl stop` 與 `restart` 只殺掉 agent；新的 agent 在同一個 cgroup 裡起來並收養（E01-D002）。
- `OOMPolicy=continue`：這個 cgroup 裡有一個行程被 OOM killer 殺掉時，不把整個 unit 停掉。
- 不用 `systemd-run --scope` 把 shim 搬到自己的 cgroup。那要 system 使用者的 D-Bus，太重。
- **`systemctl kill` 不受 `KillMode=` 限制**，預設殺掉整個 cgroup，包括 shim 與 worker。見 [operations.md](operations.md)。
- controller、API、SYM、etcd、NATS 維持預設的 `KillMode=control-group`，不設 `OOMPolicy=`。

### E12-D017 不對每一次啟動做 namespace 隔離（E12-05，#378）

- 不開 `PrivateTmp=`、`ProtectSystem=` 這類設定。留下的 shim 會留在舊的 namespace，新的 agent 看到的是另一份。
- 不用 `RuntimeDirectory=`：unit 停下時那個目錄會被刪掉。
- 狀態目錄是 `StateDirectory=mftiko/<服務>`，也就是 E12-D003 的 `/var/lib/mftiko/<服務>`。agent 的 `-work-dir` 與 `-data` 都指這裡。
- 同一份 unit 用在兩台主機（E12-D001）。instance 名稱、registry、`plain-http`、NATS URL、etcd 密碼放在 `/etc/mftiko/<服務>/env`（E12-D004 的 `EnvironmentFile=`），unit 以 systemd 的 `${VAR}` 展開。不在程式裡加新的環境變數後備。正式環境 `MFTIKO_REGISTRY=ghcr.io`、`MFTIKO_PLAIN_HTTP=false`。unit 不帶 `-conn`、`-account`（E12-D010）。

### E12-D018 停下：沒有 `ExecStop=`，`TimeoutStopSec=30s`（E12-05，#378）

- agent 的 unit 沒有 `ExecStop=`。systemd 送 `SIGTERM`（`KillSignal=SIGTERM`）。agent 照現有的訊號處理收尾、revoke、以 0 結束。
- `TimeoutStopSec=30s`。逾時的 `SIGKILL` 只打到 agent，因為 `KillMode=process`。
- controller、API、SYM、etcd、NATS 不設 `TimeoutStopSec=`（systemd 預設 90 秒）。API 自己的 `shutdown_timeout` 預設 10 秒，90 秒蓋得住。

### E12-D019 重啟：全部 `Restart=always`（E12-05，#378）

- 所有 unit（三種 agent、controller、API、SYM、etcd、NATS）`Restart=always`、`RestartSec=1s`、`StartLimitIntervalSec=0`。連續失敗也不停止重啟。`StartLimitIntervalSec=` 寫在 `[Unit]`。
- `systemctl stop` 不會被 `Restart=` 拉起來。
- E13-D015 寫 controller 以非 0 結束、unit 用 `Restart=on-failure`。unit 改成 `Restart=always`；controller 的結束碼不變（`ErrSuperseded` 仍是非 0，SIGTERM 仍 revoke 後以 0 結束）。不改 controller 的程式。

### E12-D020 region 的 etcd 與 NATS 自己寫 unit，Postgres 用發行版的（E12-05，#378）

- etcd：`User=etcd`，`ExecStart=/usr/bin/etcd`，`StateDirectory=etcd`（`/var/lib/etcd`），`Environment=ETCD_DATA_DIR=/var/lib/etcd`。這一台的 cluster 設定放 `/etc/etcd/etcd.env`。不設 `Type=notify`（不依賴 sd_notify），不設 `LimitNOFILE=`，不設 `PrivateTmp=`、`ExecStop=`。
- NATS：`User=nats`，`ExecStart=/usr/bin/nats-server -c /etc/nats/nats-server.conf`。不設上游 unit 的 `PrivateTmp=`、`ExecStop=`、`LimitNOFILE=`。
- 兩者的重啟政策照 E12-D019，不照上游的 `RestartSec=` 或 `Restart=on-failure`。
- Postgres 用發行版套件的 unit，不另外寫。md.fetch 不在這張票的 unit 清單裡，不寫 unit。
- TLS 是 E12-06（#379）。在那之前跨主機流量只走私有網路或 VPN（E12-D007）。

### E12-D021 驗收（E12-05，#378）

- CI 對 `deploy/systemd/*.service` 跑 `systemd-analyze verify`。
- 另一個 GitHub Actions 的 Ubuntu job 用真正的 systemd 跑三種 agent。etcd、NATS、registry 用 `deploy/accept` 的 compose 起；worker 是假的，包成 OCI artifact。`-registry` 只剩 MD、TD 的 unit（E12-D010）；STS 已拿掉（E10-D122）。三種情況都要收養，shim 與 worker 的 pid 不變：`systemctl restart`、`systemctl stop` 再 `start`、對 MainPID 送 `SIGKILL` 之後由 `Restart=always` 拉起。
- controller 停機不影響跑著的 session，留到 #385 的 testnet 演練，這裡不測。
- unit 檔放在 `deploy/systemd/`。打進 tarball 是 E12-04（#377），這張票不打包。
- PROTOCOL 為 v1.68。
### E12-D022 安裝工具是 `bin/mftiko-release`（E12-04，#377）

- **bash 腳本，隨 tarball，root 執行。** 子指令 `fetch <tag>`、`install`、`switch <tag>`、`rollback`、`gc`、`list`。部署機沒有 repo，安裝不走 make。打包是 `.github/scripts/build-server.sh` 與 `make server-dist`（E12-D002：腳本與 make 目標在這張，CI 只建不傳）。E12-D002 的 `bin/` 加上 `mftiko-release`。
- **`fetch`** 從 GitHub Release 下載 `mftiko-server_<tag>_linux_amd64.tar.gz` 與 `SHA256SUMS`，核對檔名完全相符的那一行。repo 是 public，不帶認證。
- **`install`** 只解開剛剛 fetch 的那一版到 `/opt/mftiko/releases/<tag>/`。目錄已存在就拒絕，不覆寫。內容改成唯讀。成員若是絕對路徑或含 `..`，或解開後的 symlink 指到目錄外（realpath 解不開也一樣），這次 install 失敗，不留下目錄。
- **`list`** 由新到舊列出目錄還在的版本，標出 `current` 與 `previous`。

### E12-D023 `switch` 安裝該版的 unit（E12-04，#377）

- 把該版 `systemd/` 複製到 `/etc/systemd/system/` 並 `daemon-reload`。只複製 `*.service`、`*.socket`、`*.timer`、`*.target`、`*.path`、`*.mount`、`*.slice` 與 `*.service.d`、`*.socket.d`、`*.timer.d`。同名覆寫；這版沒有的 unit 不刪。unit 的內容是 #378，這張不寫。`rollback` 同樣安裝它指回去的那一版（E12-D003：unit 執行 `current/bin/…`，指回去之後檔案也要是那一版）。
- 重啟不靠檔名。`ExecStart`、`ExecStartPre`、`ExecStartPost` 含有該 binary 才算（註解不算）。#378 的檔名只要執行 `/opt/mftiko/current/bin/<binary>` 就對得上。

### E12-D024 換版先 migrate，再依序重啟有啟用的 unit（E12-04，#377）

- **`switch` 的順序：** (1) `releases/<tag>/bin/mftiko-migrate up`，失敗則 `current`、unit、行程都不動；(2) 安裝 unit 並 `daemon-reload`；(3) 原子換 `current`；(4) 只重啟 `systemctl is-enabled` 成功的 unit；(5) 都成功才 gc。重啟失敗不 gc，`current` 已經指到新版。
- **順序：** `mftiko-sym`、`mftiko-mdfetch`、`mftiko-api`、`mftiko-controller`、`mftiko-md-agent`、`mftiko-td-agent`、`mftiko-sts-agent`。etcd、NATS、Postgres 不執行這些 binary，不在這次重啟裡。
- **migrate** 讀繼承的 `DATABASE_URL`，腳本不傳 `-database`。兩台主機都跑；沒有本機 Postgres 的把 `DATABASE_URL` 指到 home。migration 只做向後相容的新增，可以重複執行。實作見 E12-D027–E12-D032：`up` 套用尚未紀錄的檔案，已套用檔案的 sha256 不同就停；沒有 `down`。
- **`rollback`** 把 `current` 指回上一版，安裝那一版的 unit 並依同一順序重啟（E12-D003）。不跑 migrate，不降 schema，不 gc。每次 `current` 改動都更新 `previous`，再 rollback 一次回到剛離開的版本。上一版目錄已經不在就拒絕，`current` 不動。

### E12-D025 `switch` 之後 gc，留下最近 3 版、`current`、還有人用的（E12-04，#377）

- 保留 `/opt/mftiko/installed` 裡還在的最近 3 版，再加上 `current`。這個檔是成功 install 追加的紀錄，最舊在前；同一個 tag 以最後一次為準。「最近」是這個順序，不是 mtime，也不是 semver。
- 任一進程的 `/proc/<pid>/exe`（去掉結尾的 ` (deleted)`）落在某版目錄裡，那一版不刪。`/proc` 讀不到則什麼都不刪並失敗。
- 不另外保留 `previous`。它若落在上面之外會被刪，之後 rollback 拒絕。往前換一版時上一版是次新的，留在最近 3 版裡。
- 刪之前 realpath 必須正好是 `/opt/mftiko/releases/<tag>`。內容是唯讀，刪之前先 `chmod u+w`。

### E12-D026 CI 檢查 tarball，腳本用假版本 A、B（E12-04，#377）

- PR 的 CI 在 `make web` 之後跑 `make server-dist`：檢查內容清單，並在 linux/amd64 上確認每個 Go binary 的 `-version` 以注入的版本開頭。`SHA256SUMS` 涵蓋 `OUT` 裡全部 tarball，與 CLI 同一份（E12-D002）。`release.yml` 的 `cli` job 上傳 server tarball。
- 腳本測試用假版本 A、B，gc 另用 C、D、E。涵蓋 install、switch、rollback、gc，以及一個從 A 的目錄啟動、還停在那裡的行程讓 gc 不刪 A。`DATABASE_URL` 的佔位 `postgres://test-token-a@localhost/mftiko?sslmode=disable` 由 E12-D032 取代：測試用真正的 `mftiko-migrate` 與 pgtest 的 DSN。
- 真實 A→B 換版與 agent 收養 shim 在 #385，不在這張。

### E12-D027 自己寫 migrator（E12-13，#391）

- 不引入 golang-migrate 或 goose。`migrations` 嵌入 `*.sql`，`cmd/mftiko-migrate` 套用 `*.up.sql`。MD、TD、controller 不 migrate，也不 import 這個套件。
- 依檔名編號由小到大。`NNNN_name.up.sql`：`NNNN` 是至少 1 的十進位整數（`0001` 的 version 是 1），`name` 是 `[a-z0-9_]+`。重複的 version 在連線之前拒絕。不是 `NNNN_name.up.sql` 或 `NNNN_name.down.sql` 的 `*.sql` 是載入錯誤。
- `schema_migrations` 由 `up` 建立，不是編號檔案。欄位：`version INTEGER PRIMARY KEY`、`name TEXT NOT NULL`（檔名的 snake 段，例如 `history`）、`sha256 TEXT NOT NULL`（該 `.up.sql` 原始位元組的 SHA-256，小寫十六進位）、`applied_at TIMESTAMPTZ NOT NULL`（該檔交易裡的 `now()`）。
- 已紀錄且 name、sha256 都相符的跳過。sha256 比對不分大小寫，手動插入可以用任一種十六進位。

### E12-D028 子指令只有 `up` 與 `status`（E12-13，#391）

- `mftiko-migrate [-database URL] up|status`。旗標在子指令前面。`-database` 預設 `$DATABASE_URL`，空的是用法錯誤（exit 2）。`-version` 印 `buildinfo.String()` 並以 0 結束。沒有 `down`（E12-D024 不降 schema）。`.down.sql` 只給測試執行。
- `up` 這次提交的檔案各印一行 `applied NNNN_name`。沒有新檔時 stdout 正好是 `up to date\n`。
- `status` 不拿鎖。每一列一行：`0001 history applied <RFC3339Nano UTC> <sha>`、`0002 symbol pending`、`0001 history changed recorded <sha> file <sha>`（紀錄的 name 不同則是 `changed recorded-name <name> recorded <sha> file <sha>`）、`0009 oldname missing`。有 drift 時印完各列再以 1 結束。連線錯誤是 1。用法錯誤是 2。stderr 以 `mftiko-migrate:` 開頭。
- `make migrate-down` 拿掉。`make migrate-new` 仍建立 up 與 down 成對的檔案。

### E12-D029 一個檔案一個交易，先拿 advisory lock（E12-13，#391）

- 每個檔案與它的 `INSERT` 在同一個交易。失敗則回滾該檔（表與列都不留），以非 0 結束。同一輪較早已提交的檔案留著。CLI 先印出這次已提交的 `applied` 行，再印錯誤。不是整次 `up` 一個交易。
- `up` 一開始 `SELECT pg_advisory_lock(hashtext('mftiko-migrate'), 1)`（session lock），結束時解鎖，包含中途失敗。第二個 `up` 等到第一個結束。不設 `lock_timeout`。`status` 不鎖。
- 檔案用 simple query protocol 包成 `BEGIN;`、檔案本文、`INSERT ... now();`、`COMMIT;`。失敗先 `ROLLBACK`，連線仍能解鎖。不支援不能放進交易的 SQL（`CREATE INDEX CONCURRENTLY`；目前沒有）。

### E12-D030 已套用檔案的 sha256 不同就停（E12-13，#391）

- 套用任何新檔之前，先核對每一列紀錄。sha256 與嵌入檔不同、name 不同、或紀錄的 version 沒有嵌入檔，都是 drift：`up` 印錯誤並停止，不套新檔。
- `status` 仍印出每一列，exit 1。

### E12-D031 驗收從空資料庫跑 `mftiko-migrate up`（E12-13，#391）

- 不讀 `accept_migrations`，不猜既有的表。取代 E10-D115 的暫行做法。
- `accept.sh up` 建出 `mftiko-migrate` 並執行 `mftiko-migrate up`。compose 不再掛 `migrations/`。
- 空的資料庫直接 `up`。開發庫若已被舊的 `accept.sh` 套過 0001–0005，要手動插入那五列一次（步驟在 `docs/operations.md`，#384 的 runbook 收錄）。填錯的 sha256 會讓下一次 `up` 停住。

### E12-D032 測試（E12-13，#391）

- pgtest：空資料庫套到最新、再跑一次是 no-op（`applied_at` 不變）、兩個行程同時只有一個在套、改過的已套用檔案報錯。
- E12-D026 的發佈腳本測試改用真正的 `mftiko-migrate`，不再用 bash 替身。`DATABASE_URL` 是 pgtest 的 DSN（使用者 `mftiko`、trust、沒有密碼）。E12-D026 的 `postgres://test-token-a@localhost/mftiko?sslmode=disable` 由這條取代。第二次 `switch` 的 stdout 含 `up to date`。改掉已套用檔案的 sha256 再 `switch`，`current` 不動，也不 `daemon-reload`。
- `docs/release.md` 拿掉「尚未實作」。
- PROTOCOL 為 v1.69。


### E12-D033 退役由 CLI 宣告（E12-09，#382）

- `mftiko instance retire <inst>` 經 API `POST /instances/{name}/retire`。不從部署設定宣告。
- API 在 `AgentRecord` 的 `version == 0` 時，同一個 txn put `/mftiko/v1/retired/{inst}`（PROTOCOL 簡寫 `/retired/{inst}`）。值是 instance 名稱的 UTF-8，沒有新 proto。key 在該 instance 的 home region，不在 `/{plane}/{inst}/` 底下，所以清前綴不會刪到它。
- `AgentRecord` 在：409 `conflict`，detail `agent record is present`。不寫 etcd、不寫 Postgres。
- 不在 topology、或 plane 不是 sts／md／td（含 controller）：404。region 不可用：503 `region_unavailable`。
- etcd 成功之後才 `INSERT INTO retired_instances (instance, plane, region, retired_by, retired_at) ... ON CONFLICT DO NOTHING`。`retired_by` 是認證過的 API 使用者。列不刪。agent 不在時重複呼叫回 204。
- etcd 已寫而 Postgres 失敗：503 `storage_unavailable`，再呼叫一次補列。marker 已經擋住註冊。
- 不新增 ErrorCode。409 用既有的 `conflict`。

### E12-D034 session 轉成 terminal（E12-09，#382）

- controller 看到 `/retired/{inst}` 之後，對該 STS 上尚未 terminal 的 session 寫 `SessionDemand`：`desired = ENDED`、`end_reason = instance_retired`。不寫 `SessionStatus`（W1；RBAC 測試禁止 controller 寫 status）。
- 指派已經是 `ENDED` 的留第一次的原因。沒有指派、而且 spec 不在或已經 `ENDED`，什麼都不寫。gate 把這筆 `ENDED` 留著，跟 `invalid_spec` 一樣。
- N4 的「等 WorkerStatus 消失」在這個 key 存在時已經滿足：status 不是 terminal、WorkerStatus 還在，也一樣。然後依原順序刪 MD、TD 需求，等 view 裡不見、而且每個 provider region 都新鮮，才刪 `SessionDemand`。STS 的 region 過舊仍整段都等。沒有這個 key 時，N4 與以前相同。
- `algo_id` 的釋放是兩件事：TD 需求被刪；API 的 `algoOverlap` 略過 STS instance 在同一 region mirror 裡有 `/retired/{inst}` 的 session。spec 與非 terminal 的 status 留著（W5 與 DELETE 都要 terminal status，死去的 agent 寫不了）。marker 在的時候它們不佔 `algo_id`。立即 DELETE 仍要 terminal status。
- MD、TD 留在 placement map，已經指派的 session 不改判 `invalid_spec`。provider 不在的既有行為照舊。尚無 `SessionDemand` 的新指派，只要 feed、selector 或帳號指到退役的 instance，就只寫 `invalid_spec`，不寫需求項目。
- 退役的 mirror 不放進 writable 的那組：沒同步完成的退役 watch 不擋寫入。沒同步時 `View.Retired` 是空的，行為與今天相同。只採納 watch 的 region 等於該 instance home region 的 key。
- 可以在已退役的 STS 上建立新 session；controller 立刻以 `instance_retired` 結束它，overlap 略過它。

### E12-D035 清前綴（E12-09，#382）

- `mftiko-retire-clean` 用該 instance 自己的 etcd 使用者刪 W1 屬於它的 key。條件是 `/retired/{inst}` 在且 `AgentRecord` 不在。沒清，controller 也不再等。
- 刪的是 `election/`、`epoch`、`writer`、`agent`、`worker/`、`incarnation/`，以及 STS 的 `status/`、MD 的 `ack/`、`selector/`、`placement/`、TD 的 `ack/`。不刪 spec、demand、account，也不刪 `/retired/{inst}`。一整個前綴的授權會讓活著的 agent 刪掉別人的 key，所以不做。
- 不實作 etcd 的 apply。`mftiko-etcd-roles -drop <inst>` 只印該 region 的 `user revoke-role`、`user delete`、`role delete`，不套用，也不把這個 instance 從完整角色表拿掉。套用是部署時以 root 執行印出的命令。
- 工具放進 server tarball，跟 `mftiko-etcd-roles` 一起。

### E12-D036 同名不能再用（E12-09，#382）

- 紀錄永久。agent 在 `runner.Connect`、`Campaign` 之前讀 `/retired/{inst}`。key 在而且讀得到，就拒絕註冊並退出，不參選、不寫 epoch、不寫 `AgentRecord`。
- 對這把 key permission denied 表示角色表還是舊的（`EnsureAuth` 只加不收回）：當成還沒退役，照常參選。否則升級之後、角色表還沒重套，全部 agent 都會起不來。controller 本來就讀得到 `/mftiko/v1/`。API 的新授權是這把精確 key 的寫入；put 被 etcd 拒絕是 500，不是 `region_unavailable`。
- systemd `Restart=always` 不改。runbook 要求先套上新的角色表，並 `systemctl disable --now`，再 retire。

### E12-D037 範圍（E12-09，#382）

- P11 從 PROTOCOL §10 移到 §3.7。
- 這一張不碰 testnet。演練是 #385。
- 既有 MD、TD、controller 的行為只在 `/retired/{inst}` 存在時改變。
- 測試涵蓋：STS 退役後 session 的 `end_reason` 是 `instance_retired`，需求與 `algo_id` 釋放；MD 與 TD 退役；同名被拒；`AgentRecord` 在則 409。
- runbook 寫在 `docs/operations.md`。
- PROTOCOL 為 v1.70。

### E12-D038 監控做在 API server（E12-10，#383）；由 E14-D010 修改

- `/metrics` 在 API server，跟 `/healthz`、`/readyz` 一樣是精確路徑，比靜態檔先對上。`GET` 與 `HEAD`。不認證、不記稽核、不進 OpenAPI。`Content-Type` 是 `text/plain; version=0.0.4; charset=utf-8`。不加 Prometheus client 套件。
- mftiko 只輸出自己的 metric。磁碟、CPU、記憶體交給 node_exporter，不在 mftiko 內實作。agent 不另外開端點。**E14-D010 修改（#436）：** 這句指 `/metrics`。STS agent 另開 session log 的 HTTP 端點（契約 L），不是 metrics 端點。MD、TD、controller 不開。
- 數字來自現有的各 region mirror（E10-D014），再加上 API 自己對 registry 與 Postgres 的觀察：
  - `mftiko_agent_up{instance,plane,region}`：該 region 第一次同步之後才有這個系列。之後 `AgentRecord` 的 key 在就是 1，即使後來 region 過期仍是 1。
  - `mftiko_controller_leader{instance,region}`：同樣的規則，controller 的 `AgentRecord`（`/ctl/controller/agent`）。instance 是 `controller`。
  - `mftiko_worker_phase{instance,phase,plane,region}`：`WorkerStatus.phase` 的數量。六個已知 phase（`STARTING`、`RUNNING`、`STOPPING`、`BACKOFF`、`EXITED`、`LOST`）連 0 都輸出，另外出現的非空 phase 也輸出。解不開或 phase 是空的就跳過。不把 `EntryStatus` 的 `PENDING`、`DEAD` 另算成系列：`WorkerStatus` 沒有這兩個 phase。對應是 `BACKOFF` 期間需求為 `PENDING`，政策放棄時的 `EXITED` 對到 `DEAD`，`LOST` 維持最後一次的項目狀態。行程內重連（MD 的 `conn_epoch`、TD 的 `DEGRADED`）不改 `WorkerStatus`，不是 `DEAD`。
  - `mftiko_region_available{region}`：每個設定的 region 都有，含尚未第一次同步（0）。判斷與 `/readyz` 相同。
  - `mftiko_postgres_up`：刮取當下對既有的稽核連線池 `Ping`。沒有池是 0。錯誤不寫 log（可能含 DSN）。
  - `mftiko_registry_up`：刮取當下 `GET {http|https}://address/v2/`。最終狀態 200 是 1，其餘（含 401）是 0。沒有設定是 0。刮取失敗不寫 log。E10-D122：API 不再設定單一地址，所以這個 gauge 維持 0。不另做依 host 的探測。
- 第一次同步之前不輸出該 region 的 agent、controller、worker 系列。
- API 不連 NATS（E10-D014），也不探測交易所。那兩種斷線不移動 `AgentRecord` 或 `WorkerStatus`。
- readiness 仍不看 Postgres（E10-D048）。

### E12-D039 告警交給部署端（E12-10，#383）

- mftiko 不打 webhook，也不自己判斷門檻。告警由部署端的 Prometheus 或 Alertmanager 負責。
- 範例規則在 `docs/operations.md`，標成範例。`for` 與門檻由部署端決定。
- API 本身抓不到是 Prometheus 的 `up{job="mftiko-api"} == 0`，不是 mftiko 行程裡的 metric。
- agent 或 controller 為 0 時，要同時該 region 的 `mftiko_region_available == 1`，避免 region 分割時每一台都叫。

### E12-D040 region 連不上沿用 15 秒（E12-10，#383）

- 沿用 E13-D018、E10-D014 的 `unavailable_after`，預設 15 秒。不是新設定。`mftiko_region_available` 與 `/readyz` 用同一個比較：`LastContact` 嚴格舊過門檻才算不可用。

### E12-D041 MD、TD 舊 log 清理（E12-10，#383）

- agent 在啟動時與每小時清一次。不再出現的 worker key 留下的 log，最後修改時間嚴格早於保留天數就刪。預設 7 天，`-log-retain-days`，小於 1 拒絕。不寫進 systemd unit。
- 還算出現：觀察到的 worker、上一輪計畫、仍持有的 `WorkerStatus`、接不上的 shim、`{h}.sock`。socket 目錄讀不了就整輪不刪。目錄不在不是錯誤。
- 只刪一般檔，名稱是 16 個十六進位字的 `.stdout`、`.stderr`、`.shim.log` 與沒有前導零的輪替。mtime 剛好等於界線的留下。
- 清理失敗只 warn，不結束 term。`LogRetain` 為 0 的呼叫端（STS、controller、testplane）不清理。MD、TD 把未設定的值當成 7 天。
- 正在跑的 worker 的舊輪替檔留下，份數由輪替管，不由 7 天管。

### E12-D042 `{h}.shim.log` 輪替（E12-10，#383）

- shim 依大小輪替，10 MiB，保留 3 份（含正在寫的）。agent 仍把 stdout、stderr 接到檔案，不改成 pipe。shim 讀完 `Launch` 之後重新開啟，並把 fd 1 與 fd 2 指過去。
- 檔案不存在時不建立、不改 fd，沿用繼承的 stderr。正式環境由 agent 先建立檔案。開既有檔失敗則在 spawn 之前以 1 結束。
- 三個平面共用 `mftiko-shim`，所以都輪替。STS 刪 spec 時既有的清理已涵蓋輪替檔名，那份邏輯不改。
- worker 結束後留下的檔案照 E12-D041 清理。

### E12-D043 範圍（E12-10，#383）

- 演練清單（#385）的每一種故障都對到一個 metric 或範例告警，寫在 `docs/operations.md`。testnet 實測留到 #385。
- MD、TD、controller 的既有控制行為不變，只新增 log 清理。STS 的清理不變。
- PROTOCOL 為 v1.71。

### E12-D044 數量與位置（E12-08，#381）

- 沿用 E12-D001、E12-D006。一期只在 home region 的主機 A 跑一個 controller。沒有熱備援，也不在每個 region 各放一個。
- 行程不在時由既有的 `mftiko-controller.service` 拉回。`Restart=always` 是 E12-D019，這裡不改 unit 的行為。

### E12-D045 跨 region 認證（E12-08，#381）

- 每個 region 的 etcd 使用者都是 `controller`。密碼可以不同。
- 先讀 `MFTIKO_ETCD_PASSWORD_<region>`，region 名稱原樣接在後面。沒有或是空字串，再用 `MFTIKO_ETCD_PASSWORD`。
- `-etcd-password` 只覆寫後面這個 fallback，不蓋過 region 的環境變數（延伸 E13-D012）。不另加旗標。
- 每個 region 都要得到非空密碼。每個 region 都有自己的密碼時，fallback 可以是空的。

### E12-D046 逾時與過舊門檻（E12-08，#381）

- 部署 JSON 的 region 可寫 `dial_timeout`、`request_timeout`、`stale_after`，都是 Go duration 字串。沒寫、空字串或 0 用現在的預設。不另加命令列旗標。
- `dial_timeout` 是 etcd client 的 `DialTimeout`（取 auth token 與版本檢查）。預設 5 秒。0 不能拿來表示不限時。
- `request_timeout` 限制 unary RPC，含撥號時的認證。沒寫就不加額外期限。watch 與 lease keepalive 不受這欄限制。逾時的 unary 不當成呼叫自己的 deadline，所以不會用全新的預算再試。認證失敗仍走原有的 token 更新。
- `stale_after` 是 W9 看該 region `LastContact` 的門檻，預設 15 秒。嚴格舊過才算過舊。
- controller 等一個正在撥的 region，上限是最長 `dial_timeout` 的兩倍，至少 10 秒。比 5 秒短的 dial 仍在自己的期限內失敗。
- agent、API、MD、TD 讀同一份檔案時收下這三欄，不改自己的 dial，也不改 W8。`Config.StaleAfter` 仍是 region 沒寫時的程式 fallback，給測試用，不是新的旗標。

### E12-D047 斷線（E12-08，#381）

- 沿用 E13-D006。遠端 region 斷線時維持現狀，不加邏輯。
- 告警用 #383 已有的 `mftiko_region_available`。那個 gauge 比的是 API 的 `unavailable_after`（E12-D040），不是 controller 的 `stale_after`。

### E12-D048 範圍（E12-08，#381）

- `docs/operations.md` 加 controller 一節：部署、換版，以及搬 home region 時的 E13-D038。
- CI 測 E12-D045 與 E12-D046。testnet 的重啟、換版、斷線恢復留給 #385。
- MD、TD 的行為不變。
- PROTOCOL 為 v1.72。

### E12-D049 角色表用 `mftiko-etcd-roles -apply` 套用（E12-06，#379）

- **`-apply`** 以 root 對每個 region 的 cluster 呼叫 `state.EnsureAuth`，跟測試開 RBAC 的方式相同。`-region` 只套一個 region。新 cluster 與 instance 增減都用它，重跑不改任何東西，auth revision 也不動。
- **只加不刪。** cluster 有、角色表沒有的 role、user、permission，列成 `# region <r>: … is not in the table`，不刪。刪只在退役時做，照 E12-D035 用 `-drop` 印出的命令。
- **auth 要先開。** 新 cluster 先用 etcdctl 建 root、`auth enable`（docs/operations.md），再 `-apply`。這時還沒有服務連上來，先開 auth 沒有影響。`-apply` 不負責開 auth。
- `-format etcdctl` 照舊，給沒有角色的 cluster 看與手動套用。
- 不採用：`-diff` 印出差異的 etcdctl 命令再手動貼上（`EnsureAuth` 本身就是比對，多一步貼錯的機會）；`-apply` 也刪多出來的（套表的時機會刪到還在用的 user，例如部署 JSON 寫錯時）。

### E12-D050 etcd 密碼的產生、存放與輪換（E12-06，#379）

- **產生：** `-apply` 替 cluster 沒有的 user 讀 `-secrets` 目錄裡同名的檔案；沒有檔案就產生 32 個隨機 byte，base64url 不帶 padding，先寫檔（0600，`O_EXCL`）再建 user。檔案寫不了就整個停下，不建沒人知道密碼的 user。已經有的 user 不改密碼。
- **主本：** 主機 A 的 `/etc/mftiko/etcd/`，`root:root` 0700，每個檔案 0600：`root` 與每個 user 一個檔案。所有 region 共用這個目錄，`-apply` 從主機 A 連各 region 的 cluster。
- **服務用的副本：** 依 E12-D004，放在該服務主機的 `/etc/mftiko/<服務>/env` 的 `MFTIKO_ETCD_PASSWORD`。主機 B 的服務由運維從主本複製過去。
- **`-apply` 每個 user 一個檔案，所以 `api` 與 `controller` 在每個 region 預設是同一個密碼。** API 只有一個 `MFTIKO_ETCD_PASSWORD`，一定相同，輪換時每個 region 都要改。controller 可以照 E12-D045 每個 region 不同（`MFTIKO_ETCD_PASSWORD_<region>`）：在 `-apply` 之前把該 region 的密碼放進主本、再以 `-region` 套用，或者事後以 `etcdctl user passwd` 改那個 region；輪換也就能一個 region 一個 region 做。root 是所有 region 共用一個密碼。
- **輪換有中斷。** etcd 一個 user 只有一個密碼，改了之後所有 token 都過舊（auth revision 變了），還拿著舊密碼的進程重新認證失敗，到它以新密碼重啟為止（`TestPasswordRotation`）。其他 user 只是重新認證一次，不受影響。
- **順序：先停再改。** 新密碼寫進 env → `systemctl stop` 該服務（API 是每個副本）→ 該 user 所在的每個 region 執行 `etcdctl user passwd` → `systemctl start`。中斷就是停到起。舊進程停下時以還有效的舊密碼 resign、revoke 選主的 session，新進程立刻選得上。agent 用 `KillMode=process`，worker 不中斷，新 agent 收養。controller 重新選主。API 停著的時候不回應。
- **testnet 實測（controller，兩個 region）：** 先停再改，停到新 term 寫好 record、兩個 region 同步約 0.5 秒。先改再 `restart` 約 14.5 秒：舊進程收到 SIGTERM 時 resign 與 revoke 都認證失敗，選主的 key 留到 lease 的 15 秒 TTL 過期，新進程一直等在 Campaign。
- 輪換 root 不影響服務：只有 `-apply` 與運維用 root。
- 不採用：每次輪換建一個新的 user 再切過去（user 名稱就是 instance 名稱，W1 的角色綁在名稱上）；`LoadCredential=`（E12-D004）。

### E12-D051 `--auth-token=jwt`：金鑰、分發與輪換（E12-06，#379）

- **設定：** `/etc/etcd/etcd.env` 的 `ETCD_AUTH_TOKEN=jwt,pub-key=/etc/etcd/jwt.pub,priv-key=/etc/etcd/jwt.key,sign-method=ES256,ttl=5m`。ES256 與 5 分鐘跟 etcdtest 相同（`etcdtest.DefaultTokenTTL`），測試跑的就是正式的設定。
- **金鑰：** `openssl ecparam -name prime256v1 -genkey -noout` 產生 P-256 私鑰，`openssl ec -pubout` 取出公鑰。`jwt.key` 是 `etcd:etcd` 0600，`jwt.pub` 0644。每個 region 一對，各 region 的 cluster 彼此獨立，不共用。
- **分發：** 同一個 region 的每個 member 放同一對金鑰。client 在一個 member 拿到的 token 會送到其他 member 驗證，金鑰不同就被拒。一期每個 region 只有一個 member（E12-D001），程序仍照多 member 寫。
- **輪換：** 產生新的一對，複製到該 region 每個 member，以 rename 換掉舊檔，再逐台 `systemctl restart etcd`。之後舊金鑰簽的 token 被拒（invalid auth token），client 自己重新認證，服務不用重啟：session 撐過 etcd 的重啟，watch 繼續送，寫入照常（`TestSigningKeyRotation`）。多 member 時，換到一半的 cluster 兩種金鑰並存，client 連到另一種金鑰的 member 時重新認證一次。
- **重啟要在 10 秒內完成。** etcd 停著的時候 keepalive 收不到回應，term 在最後一次回應加 2/3 TTL（預設 15 秒的 2/3）結束（E01-D013）。逾時的話 agent 重新參選，worker 不受影響。testnet 實測兩個 region 都約 7 秒：有 client 連著時 etcd 停下要 7 秒，啟動不到 1 秒；controller 沒有重啟，term 沒有結束。餘裕不大，一個 region 有多台 member 時要逐台、等健康再換下一台。
- auth 開了之後 `etcdctl endpoint health` 要帶 root 的帳密，否則一直失敗；runbook 的健康檢查照這樣寫。
- **TTL 與 E01-D013 對得上：** E01-D013 的補充要 2/3 session TTL 大於一次 token 重連。重連約是一個 keepalive 間隔（TTL/3）加上 client 的 500ms：預設 15 秒時，期限 10 秒，重連約 5.5 秒。jwt 的 `ttl` 只決定多久重連一次，不影響這個不等式。正式環境的 session TTL 都是 `DefaultSessionTTL`，沒有旗標可以調短。
- 不採用：simple token（每個 member 各自記 token，換到別的 member 就要重新認證，重啟也全部失效；accept 的 compose 仍用 simple token，只給單項驗收）；更短的 jwt `ttl`（重連更頻繁，沒有換到安全性：密碼才是長期的祕密）。

### E12-D052 etcd 的 TLS 一期不做（E12-06，#379）

- peer 與 client 都不開 TLS。跨主機的 etcd 流量照 E12-D007 只走私有網路或 VPN。NATS 跨主機的 TLS 一期同樣不做，也走同一個網路。
- 這表示密碼（認證時）與 token 在私有網路上是明文。能看到這段網路的人能冒用任何服務的 user。一期只有兩台自己管的主機，接受。
- 理由：TLS 要 CA、每台主機的憑證與到期輪換，每個服務的 client 也要加憑證設定與旗標；VPN 一次蓋住 etcd 與 NATS。
- 二期要開時，client 端改 `state.AuthDial` 的 `clientv3.Config.TLS`，順序是先開 client TLS，再開 peer TLS。

### E12-D053 範圍（E12-06，#379）

- 程式只有 `mftiko-etcd-roles -apply` 與 etcdtest 的 `RotateSigningKey`。agent、controller、API 不改：重新認證本來就由 clientv3 處理。
- 程序寫在 docs/operations.md「etcd 認證」。etcd 的 unit 不改，`ETCD_AUTH_TOKEN` 放在 `etcd.env`。
- **testnet（2026-10-25）：** 主機 cp 是 region `jp`（home，跑 controller），主機 sys 是 `jp2`，各一個 etcd 3.7.2 member，client 與 peer 只綁 tailscale 的位址（E12-D052）。照 operations.md 走完完成標準的三項：
  - 兩個 region 都開 auth、token 是 jwt。`sts-jp-1` 寫 demand、寫 controller 的 key，`api` 寫 demand，`sts-jp2-1` 寫 `sts-jp-1` 的 key，都是 permission denied；`sts-jp-1` 在 `jp2` 沒有 user，認證失敗。
  - 輪換 controller 的密碼：中斷時間見 E12-D050。
  - 兩個 region 各輪換一次 jwt 金鑰：舊 token 被拒，controller 沒有重啟、term 沒有結束，重新認證數次後恢復。
  - agent 與 API 沒有部署在 testnet 上（#381、#385）。它們的密碼輪換照同一份程序，`TestPasswordRotation` 涵蓋 etcd 這一端。
- MD、TD、controller 的行為不變。PROTOCOL 為 v1.73。

### E12-D054 etcd 備份：主機 A 每小時存每個 region 的快照（E12-07，#380）

- **誰存：** 主機 A 的 `mftiko-etcd-backup.timer`（`OnCalendar=hourly`、`Persistent=true`）啟動 `mftiko-etcd-backup.service`（oneshot，root）。它以 root 對部署 JSON 的每個 region 呼叫 etcd 的 snapshot（跟 `etcdctl snapshot save` 同一個 RPC），一個 region 失敗不擋其他 region，但 unit 以失敗結束。部署 JSON 用 controller 的 `/etc/mftiko/controller/deployment.json`。
- **為什麼在主機 A：** snapshot 要 root，root 的密碼只在主機 A 的 `/etc/mftiko/etcd/`（E12-D050），跟 `-apply` 一樣從主機 A 連各 region。遠端 region 的快照因此不在它自己的主機上。
- **放在哪裡：** `/var/backups/mftiko/etcd/<region>/<UTC 時間>.db`，目錄 0700、檔案 0600：快照裡有 user 的 bcrypt 雜湊與所有資料。先寫 `.part`，fsync 之後才改名，看得到 `.db` 就是完整的。
- **保留：** 每個 region 最新 72 份（三天）。只數 `mftiko-etcd-backup` 命名的檔案，其他檔案不刪。
- **異地：** home region 的快照跟 home 的 etcd 在同一台主機，主機 A 整台不見時一起不見。複製到另一台（例如主機 B）交給部署端，比照 E12-D039 不在 mftiko 裡做。
- **失敗：** unit 是 failed，`systemctl list-units --failed` 看得到。告警交給部署端（E12-D039），例如 node_exporter 的 systemd collector，或最新一份 `.db` 的 mtime 超過兩小時。
- 不採用：每台主機各自存自己 region 的快照（主機 B 要放 root 密碼）；在 etcd 的 data dir 直接複製 `member/snap/db`（執行中複製不保證一致）；保留更久（還原到三天前的狀態很少用得上；要留更久，由部署端在異地複本上管理）。

### E12-D055 還原一律 `--bump-revision <bump> --mark-compacted`，bump = R_old − R_s + 10 億（E12-07，#380）

- **指令：** `etcdutl snapshot restore <快照> --bump-revision <bump> --mark-compacted`，`--name`、`--initial-cluster`、`--initial-cluster-token`、`--initial-advertise-peer-urls` 照 `/etc/etcd/etcd.env`，所以 member、cluster ID 與 client URL 都跟還原前一樣。
- **為什麼要 bump：** 讀取端的 `Mirror` 停在還原前看過的 revision R_m，從那裡續 watch（W4）。還原後 cluster ID 不變，revision 從快照的 R_s 接著往下。R_s + bump 大於 R_m 時，revision 不會倒退，`--mark-compacted` 把 R_s + bump 以下都標成已 compact，續 watch 收到 compacted，`Mirror` 重新 list（`TestMirrorRestoredSnapshot`）。沒有 bump 時，cluster 一追過 R_m，新歷史的事件就疊在舊副本上，只有 probe 剛好看到 revision 或 raft term 倒退才會發現（`TestMirrorRestoreReusingRevisions`）。API 的 SSE 同樣從 compacted 送 `reset`（E10-D094）。
- **取值：** R_m 不超過還原前 cluster 到過的最大 revision R_old，所以 bump 要大於 R_old − R_s。**bump = R_old − R_s + 10 億**（`state.RestoreMargin`）。R_old 是還原前讀到的 revision；cluster 讀不到時，用該 region 最新一份快照的 revision。10 億的餘裕蓋過 R_old 沒算到的寫入：最新快照之後的一小時，或最後一次讀取之後、一個 probe 間隔（5 秒）內的寫入。revision 是 int64，加幾十億沒有溢位的問題；etcd 只把 bump 記成 revision 的起點，沒有額外成本。
- **不用固定的 10 億：** 原本寫的是固定 `--bump-revision 1000000000`，R_old − R_s 超過 10 億時才改。testnet 上第二次還原 jp 時照固定值做，cluster 已經被第一次還原 bump 到 10 億以上（R_old = 1000000046，R_s = 23），revision 倒退到 1000000023。觀察中的 `Mirror` 靠 probe 看到 revision 倒退才 relist，那時只差 22 個 revision 就追過去。一次還原過的 region 都在 10 億以上，條件一定會成立，所以改成一律計算。runbook 以 shell 算，不靠記得條件（`TestRepeatedRestore`）。
- 不採用：不 bump、只靠 probe 的 revision 與 raft term 檢查（mirror.go 的說明：一個 probe 間隔內追過就看不出來）；固定值（上一點）。

### E12-D056 還原之後、agent 起來之前把 epoch 加上同一個 bump（E12-07，#380）

- **問題：** epoch 存在 etcd（W2），快照裡是舊的值。快照之後選過主的 instance，shim 已經接受過更大的 epoch；新 agent 拿到「舊值 + 1」去 `Attach`，被回 `stale_epoch`，agent 結束（C3），systemd 拉起來再加 1，一直到追過為止。controller 也一樣：home 還原之後它的 epoch 變小，其他 region 的 writer key 仍是新的 epoch，Fence 失敗、term 結束。
- **做法：** `mftiko-etcd-backup -raise-epochs -region <r> -by <bump>` 以 root 把該 region cluster 裡每個 instance 的 epoch 加上 bump（`state.RaiseEpoch`）：該 region 的每個 agent，`r` 是 home 時加上 controller。key 不在也寫，當成 0：instance 可能在快照之後才第一次選主。以 txn 比對 mod revision，被改過就重讀。`-by` 沒有預設值。
- **取值就是 E12-D055 的 bump：** epoch 每加 1 都是一次寫入，所以從同一份快照算起，epoch 增加的量不超過 revision 增加的量。之前的還原若也是 revision 與 epoch 加同一個 bump，這個關係仍成立。所以 epoch 加上 R_old − R_s + 10 億之後，一定大於快照之後用過的任何 epoch，包括之前幾次還原。固定加 10 億會跟固定的 revision bump 犯同樣的錯：testnet 的 jp 第一次還原後 controller 是 1000000004、jp2 的 writer key 也是，同一份快照再還原一次，固定值只能給 1000000003（`TestRepeatedRestore`）。
- **順序：** 先停該 region 的 agent 與 controller，再還原、再加 epoch、最後啟動。agent 是 `KillMode=process`，停著的時候 worker 照跑，新 agent 收養。
- **不做的後果（testnet 實測）：** jp 還原後不加 epoch，controller 以 epoch 4 啟動，看起來正常（`NRestarts=0`），直到第一次要寫 jp2：`writer key in jp2 holds epoch 6, this term has 4`，退出，systemd 拉起來 5、6 再退出兩次，到 7 才過。差多少就重啟多少次，每次約 1.3 秒。
- 不採用：agent 收到 `stale_epoch` 時把 epoch 跳到 `details["epoch"]`（改 W2 與 C3 的行為；controller 沒有 shim，它要追的是別的 cluster 的 writer key）；還原前讀出每個 instance 現在的 epoch 再寫回（還原的原因常常就是那個 cluster 讀不到了）。

### E12-D057 還原的程序與影響（E12-07，#380）

- **停：** 該 region 主機上的 agent 與 controller（`systemctl stop`）。API 不用停：etcd 停著的時候它的 `/readyz` 與 `mftiko_region_available` 照 E12-D040 反映，還原之後 `Mirror` 重新 list。
- **auth：** 快照裡有 auth，角色表與 user 是快照時的。快照之後新增的 user 沒有了，重跑 `mftiko-etcd-roles -apply -region <r>` 補上。快照之後輪換過的密碼回到舊的，以 env 裡現在的密碼再 `etcdctl user passwd` 一次。jwt 金鑰是檔案，不受影響。
- **舊的 lease 還在：** 快照裡的選主 key 與 `AgentRecord` 綁在快照時的 lease 上。etcd 還原 lease 時重新計時，所以在一個 session TTL（15 秒）之內，舊的選主 key 擋住新的 Campaign，`AgentRecord` 也還在，`mftiko_agent_up` 看起來是 1。testnet 上 jp（home）每次還原後 controller 從啟動到寫好 record 都是約 15 秒，jp2 沒有選主 key，立刻寫好。驗證要在啟動 15 秒之後看。不另外 revoke lease：等 TTL 過期效果一樣，少一步可能做錯的命令。
- **驗證：** revision 大於 R_old、`auth status` 是 true；agent 啟動後沒有 `stale_epoch`、`NRestarts` 不再增加；15 秒之後 `mftiko_agent_up`、`mftiko_controller_leader`、`mftiko_region_available` 是 1；API 的 `/readyz` 是 200。
- **對 worker 的影響：** 收養不受影響（E12-D056）。etcd 的內容回到快照時：快照之後才建立的 session 沒有 spec，快照之後的 demand、ack、status 都是舊的。agent 收養之後照 reconcile 對齊快照裡的需求，預期沒有需求的 worker 被停掉、status 由 agent 重新寫。testnet 沒有 agent，這一點還沒實測（E12-D058）。影響是「快照到還原之間的變更」，不是還原本身；能用越新的快照越好。
- 舊的 data dir 改名留著（`/var/lib/etcd.before-restore.<時間>`），確認之後再刪。
- runbook 第 1 步 export 了 `ETCDCTL_USER`。在同一個 shell 以 `etcdctl --user` 測別的 user 會被 etcdctl 拒絕（環境變數與旗標衝突），要先 `unset ETCDCTL_USER ETCDCTL_PASSWORD`。

### E12-D058 範圍與 testnet（E12-07，#380）

- 程式：`mftiko-etcd-backup`（存快照與 `-raise-epochs`）、`state.RaiseEpoch`、`state.RestoreMargin`、etcdtest 的 `Snapshot` 與 `Restore`、`mftiko-etcd-backup.service` 與 `.timer`。agent、controller、API 與 `Mirror` 不改：`Mirror` 本來就把 compacted 與 revision、raft term 倒退當成重新 list。
- 測試：`TestMirrorRestoredSnapshot`、`TestMirrorRestoreReusingRevisions`；`mftiko-etcd-backup` 的存、留份數、從存下的檔案還原再加 epoch，以及同一份快照還原兩次（`TestRepeatedRestore`）。etcdtest 的 `Restore` 用 etcdutl，找 etcd 旁邊的，再找 PATH；CI 的 etcd tarball 裡就有。CI 對 `deploy/systemd/*.timer` 也跑 `systemd-analyze verify`。
- **testnet（2026-10-26）：** cp 是 `jp`（home，跑 controller），sys 是 `jp2`，etcd 3.7.2。兩台裝上同版本的 `etcdutl`（官方 tarball，SHA256 核過）。`mftiko-etcd-backup` 還不在 release 裡，放在 cp 的 `/usr/local/lib/mftiko-e12-07/`，unit 以 drop-in 改 `ExecStart`；release 帶上之後刪掉 drop-in。
  - 備份：timer 啟用，service 手動跑三次，每個 region 一份，目錄 0700、檔案 0600，`etcdutl snapshot status` 讀得到。
  - 測試資料：以 `api` 在 `sts-jp-1`、`sts-jp2-1` 各寫一個只有名稱的 spec，controller 寫下 `SessionDemand` 並 fence jp2。存快照之後再寫 marker、重啟 controller 兩次（epoch 3 → 5）、再加一個 spec。一個以 `api` 執行、跨過還原一直跑著的 `state.Mirror` 代替 API 觀察兩個 region。
  - jp2 還原（固定 bump）：資料回到快照，`sts-jp2-1` 的 epoch 0 → bump，controller 一次啟動（`NRestarts=0`），RBAC 照舊（`sts-jp2-1` 與 `api` 寫 demand 都是 permission denied）。觀察者 7 秒內 relist，這次是 probe 先看到 raft term 倒退（3 → 2），不是 compacted。
  - jp 還原不加 epoch：controller 重啟三次才過（E12-D056）。
  - jp 再以固定 bump 還原：revision 倒退（E12-D055）。
  - 改成計算 bump 之後，jp 與 jp2 各再還原一次：revision 都上去（jp 1000000030 → 2000000030，jp2 1000000020 → 2000000020），epoch 加上同一個 bump，之後強迫 controller 寫 jp2，`NRestarts` 仍是 0；觀察者兩邊都是 `watch compacted, relisting`，view 與 etcd 相同。
  - controller 的中斷（停到寫好 record）：jp2 約 11 秒，jp 約 23 秒，其中 15 秒是 E12-D057 的舊 lease。都是手動操作的速度。
  - 結束後刪掉測試的 spec、`SessionDemand` 與 marker，留下 `-raise-epochs` 寫的兩個 STS epoch。舊 data dir 留在兩台的 `/var/lib/etcd.before-restore.*`。
  - **沒做到的：** testnet 上沒有 agent、API、NATS（#381、#385），所以三種 agent 收養 worker、API 本身的重新同步沒有在 testnet 上走過；分別由 `TestRepeatedRestore`（epoch）與觀察者（`Mirror`，API 用同一個）涵蓋到 etcd 這一端。worker 的影響（E12-D057）留到 #385 的演練。
- MD、TD、controller 的行為不變。PROTOCOL 為 v1.74。

### E12-D059 runbook 是 `docs/runbook.md`，程序留在 operations.md（E12-11，#384）

- **入口：** `docs/runbook.md`。依告警與依操作各一個目錄，每一項是「告警、先看、做、確認」。運維從告警名稱或要做的事找起。
- **不重抄：** 已經寫在 `docs/operations.md`、`docs/release.md` 的長程序（etcd 認證、備份與還原、永久退役、`mftiko-release`）只連過去。兩份會漂移，程序只有一份。operations.md 留著當參考：檔案位置、unit、metric、保留期。
- **為什麼不併入 operations.md：** E12-01 沒有定位置。operations.md 是依元件寫的參考，已經四百多行；runbook 要依症狀找，放在同一份會兩種讀法互相打架。
- runbook 只寫一期有的東西：兩台主機、兩個 region、每個 region 一個 etcd member、一個 controller（E12-D001、E12-D006）。

### E12-D060 範圍（E12-11，#384）

- **有：** 換版、回退、重啟 agent 與 controller；etcd 的備份、還原、密碼與 jwt 金鑰輪換；其他放在檔案裡的祕密（NATS user creds、registry 帳密、cookie 金鑰、Postgres 密碼）；instance 增減與永久退役；殺 worker、`BACKOFF`／`EXITED`／`LOST`、agent 不在、controller 沒有 leader、主機重開、各種斷線、Postgres 停機、registry 不可用、磁碟、API 不在、etcd 壞掉。每一項附上 E12-10 的範例告警；不響的情形（NATS、交易所斷線）也寫出來。
- **祕密輪換的規則**照 E12-D004：同一路徑原地改寫、再重啟 unit。改寫是寫 `.new` 再 rename，不先刪再建。
- **沒有：** NATS 的 account signing key 與 TD 的 seal key 的輪換。前者要一起改 NATS server 的 account 設定，後者要以新公鑰重新 seal 每個帳號（E06-D013、`mftiko-seal`），都還沒有工具與程序，一期不輪換。舊驗收庫補記 `schema_migrations`（E12-D031）只給開發用的資料庫，留在 operations.md，不進 runbook。
- 程式不改。PROTOCOL 不變（v1.74）。

### E12-D061 演練之前沒有實測的條目（E12-11，#384）

- 完成標準是 #385 的演練全程只照 runbook 做。演練之前，下列條目是照設計與程式碼寫的，沒有在 testnet 上做過：換版與回退（E12-D026 只在 CI 用假版本）；agent 收養（E12-D021 在 CI 的 systemd）；殺 worker、主機重開、拔網路、Postgres、registry。
- 主機重開後 STS 的行為特別標出：還沒 terminal 的 session 沒有 shim 可收養，照計畫重新 spawn，已經 terminal 的不動（`internal/sts/agent/plan.go`）。這跟 E09-D070「shim 掉了不重啟」不衝突：那是 agent 還在、看到 shim 消失的情形，無法確認舊進程已結束；重開後舊進程確定不在。演練時確認。
- 已在 testnet 實測過的：etcd 認證的輪換（E12-D053）、備份與還原（E12-D058）。
- 演練發現 runbook 以外的步驟，補進 runbook，並在這裡記下。

### E12-D062 md conn worker 用 handler 上報 NATS，經 socketpair（#401）

- md conn worker 在 `natsx.Conn` 的 `DisconnectErrHandler` 與 `ReconnectHandler` 上感知。`OnDisconnect` 與既有的 `OnReconnect` 都在 client 的 callback goroutine，不得阻塞。
- 連通性不經 NATS 送。`mdctl.Report` 加 optional `bus_up`（欄位 8）。沒填表示這個 worker 不上報。有填時每次都是完整快照，只在 bus 狀態改變時送，走 shim 的 socketpair（C4 Report）。`cmd/mftiko-md-conn` 在 `Connect` 成功後以 `conn.BindBus` 接上，一開始是連著。
- Publish 失敗不當成斷線。斷線只看 handler。
- PROTOCOL 為 v1.75。

### E12-D063 md-agent 寫成 WorkerStatus 條件，不改 ack（#401）

- `WorkerStatus` 加 `repeated Condition conditions`（欄位 10），型別與 `SessionStatus.conditions` 相同。
- md-agent 在 `bus_up` 有填時寫一條 type `bus_disconnected`：`bus_up` false 則 status true（斷線），true 則 status false（連著）。不寫 `at_ns`，避免每輪 reconcile 都改 bytes。沒有 `bus_up` 的報告不寫這條。
- `EntryStatus` 與 feed ack 不變。`READY` 仍是 worker RUNNING 且 feed `SUBSCRIBED`。phase 不變。
- W7.2 擴充：階段或 conditions 改變才寫。`Beat` 與 Report 的其他欄位仍不進 etcd。TD、STS 的 hook 是空的，寫入的 bytes 與以前相同。
- controller 不讀這條 condition。這修改 E12-D038「NATS 斷線不改 WorkerStatus」裡 md conn worker 的部分：phase 仍不改，多的是 conditions。E12-D038 的條文留著當當時的決定。

### E12-D064 gauge，門檻在部署端（#401）

- API 輸出 `mftiko_worker_bus_up{instance,plane,region,worker}`。`worker` 標籤是 `WorkerStatus.worker.key`。key 是空的、或沒有 type `bus_disconnected` 的，不輸出系列，所以 TD、STS 不會被看成 1。
- status true 是 0，status false 是 1。
- 與其他 worker 系列相同：該 region 第一次同步之後才出現，後來過期仍維持。
- mftiko 不計時、不設門檻。範例告警 `MftikoWorkerBusDown`，`expr: mftiko_worker_bus_up == 0`，`for: 15s`，與 E12-D040 的 15 秒同一種寫法。`for` 由部署端改。

### E12-D065 重連立即回到 1（#401）

- Reconnect handler 一觸發就把 `bus_up` 設回 true，不等重連緩衝排空，也不等第一則訊息。
- 不改 client 的 `ReconnectBufSize`（維持 nats.go 的預設）。
- runbook 寫明：gauge 回到 1 時，訂單簿還要幾秒才追上。

### E12-D066 範圍與測試（#401）

- 只做 md conn worker。條件的格式是通用的，TD 與 STS 不實作。
- `docs/operations.md` 的告警表與演練列、`docs/runbook.md` §3.5 改成新告警。交易所斷線仍不響。
- 測試：嵌入的 NATS 停下之後 gauge 是 0，再啟動回到 1。輪詢看得到的報告，不 sleep。
- TD、STS、controller 的行為不變。feed ack 的意思不變。

### E12-D067 暫時錯誤不改重試（#403）

- STS agent 拉策略 image 或 session input 遇到暫時錯誤時，維持現在的行為：不放棄、不設上限，照既有的 1–60 秒退避重試。session 維持 `PENDING`。registry 恢復後自己接上。不加自動放棄。
- 退避用的 `fails` 與 `next` 不因這個訊號改寫。成功或永久錯誤只清掉訊號用的連續失敗紀錄，不把退避計數歸零。
- 永久錯誤（artifact 被拒絕）仍照現在把 session 寫成 `FAILED`，reason 是 artifact 的 reason。

### E12-D068 連續拉取失敗寫 `artifact_pull` condition（#403）

- 只記暫時錯誤。STS 的策略 image 與 session inputs 都算。STS 沒有對應的 `EntryStatus`（那是 MD、TD 的需求項目，E12-D070），condition 寫在 `SessionStatus`。
- `Condition.type` 是 `artifact_pull`，`status` 是 false，`reason` 是 artifact 的 reason，沒有 reason 時用 `artifact_unavailable`。`message` 是 `<十進位失敗次數> <digest>`，中間剛好一個空白；digest 是 `sha256:` 加 64 個小寫十六進位。`at_ns` 是這一段連續失敗的第一次，unix 奈秒。失敗次數從這一段的第一次算起。
- 同一個 digest 一條 condition，依 digest 排序。多個 job 同一個 digest 時取較早的 `at_ns`，相同時取較大的失敗次數。其中一個 job 已經就緒，這個 digest 的 condition 就清掉。
- 成功立刻清掉這一段。下一次 reconcile 把 condition 從 `SessionStatus` 拿掉。phase 與 reason 沒變也要寫（延伸 E09-D021：原本只在 phase 或 reason 改變時寫）。沒變就不重寫。
- 永久錯誤把 session 寫成 `FAILED`，不寫 `artifact_pull`。terminal 的 condition 仍只有 `dependency`（E10-D093）。
- agent 重啟後、這個進程還沒有這次拉取的結果之前，留下 status 上已經寫好的 `artifact_pull`，gauge 維持 1，部署端的 `for` 不會因為重啟歸零。這個進程拉成功就清掉。這個進程再次失敗就用新的失敗次數與 `at_ns` 覆寫，0／1 仍是 1。
- MD、TD 的 `EntryStatus` 不寫這個 condition。

### E12-D069 API 輸出 artifact 拉取失敗的 gauge（#403）

- `mftiko_artifact_pull_failing{instance,region,digest}` 是 0 或 1。標籤順序是 instance、region、digest。樣本依這三個排序。
- `mftiko_artifact_pull_fail_since_seconds` 用同一組標籤，值是第一次失敗的 unix 秒（`at_ns / 10^9`）。只在 failing 是 1 時輸出。同一個 digest 有多個 session 時取最早的 `at_ns`。
- 只看 STS instance，而且該 region 已經第一次同步（與 `mftiko_agent_up` 相同）。尚未同步不輸出系列。
- 非 terminal session 的 spec（策略 `code` 與 input digest）先記 0。status 上合規的 `artifact_pull` 把同一個 digest 記成 1。另一個 session 健康不會把已經是 1 的 digest 改回 0。任一非 terminal session 有這條 condition，gauge 就是 1。
- `DONE`、`FAILED` 的 session 略過：永久錯誤不輸出 1，也不為了那個 digest 輸出 0，除非別的非 terminal session 引用它。
- status 的 key 在但解不開：整個 session 略過。status 不在：當成非 terminal，依 spec 輸出 0。spec 解不開：略過該 session，不單靠 status 告警。
- 不是 `artifact_pull`、`status` 不是 false、`at_ns` 不大於 0、失敗次數不大於 0、或 digest 不是 sha256 的 64 個小寫十六進位，都忽略。`dependency` condition 不會變成這個 gauge。
- mftiko 不寫門檻。部署端的範例規則是 `mftiko_artifact_pull_failing == 1`、`for: 5m`。
- 兩個 family 沒有樣本也輸出 HELP 與 TYPE。

### E12-D070 範圍只有 STS agent（#403）

- MD、TD 拉 conn、account worker 的重試、`EntryStatus.reason`、`PENDING` 都不改。
- controller 不改。CLI 仍只把 `dependency` condition 印成依賴，不改。
- MD、TD、controller 的行為不變。PROTOCOL 為 v1.76。

### E12-D071 告警與 runbook（#403）

- `docs/operations.md` 的範例告警加 `MftikoArtifactPullFailing`，`expr: mftiko_artifact_pull_failing == 1`，`for: 5m`。E10-D122 之後 `mftiko_registry_up` 維持 0（API 不再有單一地址可做 `GET /v2/`），範例告警 `MftikoRegistryDown` 拿掉，拉不到 artifact 只看 `MftikoArtifactPullFailing`。
- 演練對照加一列：只有 STS agent 連不到 registry。
- `docs/runbook.md` §3.7 加上這個情形。先看該 STS agent 主機到 registry 的網路，以及該服務的 registry 帳密檔（`MFTIKO_REGISTRY_CREDS` 指到的 `config.json`，mode 0600）。恢復後 agent 繼續拉，condition 清除，gauge 回到 0。被拒絕的 artifact 仍是 `FAILED`，沒有這個 gauge。
- 測試：失敗時有訊號、成功後清除、永久錯誤不發這個訊號。

### E12-D072 STS 背景解析 `latest` 只在狀態改變時記 log（#404）

- STS agent 背景解析 `latest`（E12-D011、E12-D012）的結果，每個 repository 只在第一次失敗與狀態改變時記 log。狀態不變的重複結果不再記。等級維持現狀：失敗是 warn（`latest not resolved`），成功是 info（`latest resolved`）。
- **狀態改變：** resolved 轉 not resolved、錯誤 reason 改變、not resolved 轉回 resolved，以及解析成功後 digest 改變。reason 是 artifact 的 reason（`artifact_not_found`、`artifact_unavailable` 等）；沒有 reason 的錯誤用錯誤文字。沒有 `latest` tag 時 reason 一直是 `artifact_not_found`，所以不會每分鐘 warn。
- 狀態在這個 agent 行程裡。重啟之後，下一次失敗會再記。列出 repository 失敗（`latest repositories`）不是單一 repository 的解析結果，維持每輪記一次。
- GC 不變（E12-D013）：registry 不可用、`latest` 不存在、或尚未成功解析過，該 repository 的 rootfs 全部保留。MD、TD、controller 的行為不變。PROTOCOL 不變。

### E12-D073 沒有活躍列的 feed 啟動時立刻 refresh 一次（#405）

- SYM 啟動時，每個 feed 看自己在 `symbol_active` 有沒有列。沒有就立刻 refresh 一次，這一次之後回到原本的間隔加抖動。已有列的 feed 不改：第一次仍只等 `[0, DefaultJitter)` 的抖動，之後是間隔加新的抖動。
- 只算活躍表。終結表有列、活躍表沒有，仍立刻 refresh。查詢失敗時不把這張表當成空的，仍走原本的抖動間隔。
- 立刻那一次失敗，就回到間隔加抖動，不連續重試。
- 不另做運維用的 refresh 指令，也不加旗標。`sym` 的 `RefreshRequest` 與驗收工具維持原樣。
- 範圍只有 SYM。MD、TD、STS、controller 的行為不變。PROTOCOL 不變（v1.76）。

### E12-D074 runbook 改確認活躍表，不手動 refresh（#405）

- `docs/runbook.md` 的部署確認不要求手動強制 refresh。演練用的 `mftiko-md-accept refresh` 不是部署步驟。
- 改為確認 SYM 啟動後一分鐘內 `symbol_active` 有列。已有列時重啟不會立刻再刷，確認看到的是原本的列。

### E12-D075 演練環境：共用主機、本機建的 release、自己的 registry（E12-12，#385）

- **主機：** cp（region `jp`，home）與 sys（`jp2`），就是 E12-D053 的兩台。兩台上還有別的服務（traefik、別的 Postgres 與 NATS、jitsi 等）。mftiko 的元件都用自己的 port 與容器：NATS :4223、Postgres 16 容器 :15432、registry:2 容器 :5005、API :18080，只綁 tailscale 的位址與 127.0.0.1。共用的 Postgres 與 NATS 不碰。
- **斷網：** `OUTPUT` 裡比對 mftiko 服務使用者 UID 的 REJECT 規則，帶 `mftiko-drill` 註解，`drill.sh uncut` 全部移除。agent 與它的 worker 是同一個 UID（E12-D005），所以「agent 與 NATS」實際上是整個平面與 NATS。主機上其他程式不受影響。主機重開不在演練清單裡，不做：會停掉別人的服務。
- **release：** 不發佈 `v*` tag。本機以 `build-server.sh` 建 tarball，版本是 commit hash（A `dd73a2d`、B `b32b0de`、C `2326586`），經 `mftiko-release` 的 fetch、install、switch 安裝。唯一跟正式部署不同的是 `MFTIKO_RELEASE_BASE` 指向主機上 127.0.0.1 的檔案伺服器；SHA256SUMS 照樣核對。
- **artifact：** conn、account worker 與策略推到主機 A 自己的 registry（plain HTTP，E09-D010）。「registry 不可用」就是停這個容器，或擋 agent 到它的連線。
- **交易所：** BinanceUM testnet，ed25519 金鑰（E03-D003）。憑證只以 0600 的暫存檔交給 CLI，用完 `shred`。
- **策略：** `examples/strategies/drill`，一直維持一張遠離市價的掛單，每個 hook 一行 JSON，`drill.sh orders` 算出每一步的下單統計與最長空窗。
- 不採用：另開乾淨的 VM（要新的主機）；真的發佈 tag 與推到 ghcr（演練用的版本會變成公開的 release）。

### E12-D076 演練中修掉的問題（E12-12，#385）

- **`mftiko-release install` 的權限：** 解開用的 `mktemp -d` 是 0700，整個目錄改名過去之後 release 只有 root 進得去，服務的使用者都 `Permission denied`；testnet 上 `switch` 之後 controller 停了約 3 分鐘。改成唯讀之前先 `chmod 0755`。`assertReadOnly` 也檢查其他使用者讀得到。CI 原本沒抓到：release 的測試在沒有 Postgres 時跳過，以 root 執行時又讀得到 0700 的目錄。
- **Binance 帳號種類的 key type：** adapter 只用 ed25519 簽（E03-D003），但每個 Binance 種類仍宣稱接受 hmac（那個決定之前留下的）。API 收下 HMAC 的 BinanceUM 帳號，worker 每次連線都 `signs with ed25519: unsupported`，session `deps_timeout`。改成只宣稱 ed25519，`TestKindsAdvertiseWhatTheySign` 把宣稱與簽章器綁在一起。
- **`SessionStatus.restart_count`：** STS agent 從來沒寫，一直是 0；testnet 上被殺的策略是 incarnation 2、`WorkerStatus` restarts 1，`restart_count` 0。policy 每允許一次重啟就加一，status 帶著它。定義是這個 agent 進程的累計次數，輪換後從 0 起，跟 E09-D069 的窗口計數一致；被 `restart_limit` 擋下的不算。
- release C 帶著這三個修正，在 testnet 上換版一次並再殺一次策略確認。

### E12-D077 runbook 的補充（E12-12，#385）

演練中 runbook 以外的步驟，補回 runbook：

- §2.1：從 `dd73a2d` 或更早的版本換版，install 用新版 tarball 裡的 `mftiko-release`，或裝好之後 `chmod 0555`。
- §2.3：agent 被 SIGKILL 或 OOM 時 lease 沒有 revoke，新 agent 等它過期，最多 15 秒才收養（實測 9–15 秒）。
- §3.5：md 平面斷 NATS 時 feed 的 ack 仍是 READY，平台沒有訊號（#401）。
- §3.6：Postgres 停機時 `session create`、`session delete`、`record list` 是 503；`session list` 沒有建立時間與建立者；`session end` 照常。
- §3.7：registry 不可用時建不了 session，照 E10-D012、E10-D057 的設計（#402 以此關閉）。agent 連不到 registry 的訊號是 #403 的 `mftiko_artifact_pull_failing`。E10-D122：`mftiko_registry_up` 維持 0，不再用它表示 registry 可達。

E12-D061 列的「演練之前沒有實測」，除了主機重開，都在 testnet 上走過了。主機重開不做（E12-D075），STS 重開後的行為仍照 E12-D061 是待確認。

### E12-D078 範圍與結果（E12-12，#385）

- 程式：`deploy/drill`（`drill.sh`、`orders.py`、兩個 session 檔）、`examples/strategies/drill`，以及 E12-D076 的三個修正。
- 演練清單的每一項都通過，紀錄在 `deploy/drill/README.md`：輪換四種行程、兩次換版、殺三個平面的 worker、`systemctl restart` 與 SIGKILL agent、四種斷網、Postgres 停機、registry 不可用（停容器、只擋 agent）。
- 沒有在這張修的問題各開一張票：#401（feed 在 NATS 斷線時仍是 READY）、#403（agent 拉不到 artifact 沒有訊號）、#404（沒有 `latest` 的 repository 每分鐘 warn，已由 E12-D072 修好）、#405（SYM 第一次 refresh 要等抖動的間隔）。
- 策略在 `OnStop` 撤不了單（SDK 在 session 結束中拒絕），被殺的 incarnation 的掛單也留在交易所；演練結束用 `drill.sh sweep` 撤。
- 演練環境留在兩台主機上，session 都已結束，帳號沒有掛單。拆除步驟在 README。
- PROTOCOL 為 v1.77。

### E12-D079 SYM 的 NATS user 由 mftiko-nats-dev 簽發（#413）

- **來源：** 正式部署準備。`natsdev` 只發 MD 與 md.fetch 的 user（E04-D015），TD agent 沒有 user（E06-D039），SYM 沒有規定。accept 讓 SYM 借用觀察端那個可收發所有 subject 的 user；#385 演練的 `creds/sym.creds` 不是任何工具產生的。
- **權限：** publish `sym.v1.changed.>`；subscribe `sym`（queue group）；回答 `sym` 帶 response permission，一則、兩分鐘，與 MD、md.fetch 相同。以 account 的身分金鑰簽，`IssuerAccount` 留空（E04-D014）。
- **沒有 inbox。** SYM 只回答 `sym`、只發佈變更通知，不發請求，所以不給 `_INBOX_sym`，manifest 的 `inbox_prefix` 是空字串。它不訂 `sym.v1.changed.>`，也不能 publish `sym`。
- **產生：** `mftiko-nats-dev -sym-inst <inst>`，預設 `dev`。檔名 `creds/sym-{inst}.creds`，種子 `keys/user-sym-{inst}.nk`，已有就沿用。manifest 加 `sym`。沒有 SYM user 的舊 manifest 由 `Load` 拒絕並請重跑；重跑 `Generate` 補上，不換其他金鑰。
- **檢查：** `-check` 驗證 SYM 的變更通知送得到 session、session 的 `sym` 請求在 `sym` queue group 得到回答，以及 publish `sym`、subscribe `sym.v1.changed.>`、publish `md.v1.f`、碰任何 inbox 都被拒。檢查用的 session 照 PROTOCOL 加上 publish `sym` 與 subscribe `sym.v1.changed.>`。
- **部署：** creds 放 `/etc/mftiko/sym/nats.creds`（0600，`mftiko-sym`），SYM 的 unit 已經固定這個路徑。accept 的 SYM 改用 `/nats/creds/sym-dev.creds`。
- 範圍只有 `natsdev`、`mftiko-nats-dev`、accept 與文件。MD、TD、STS、controller 與 SYM 本身的行為不變。PROTOCOL 為 v1.78。

### E14-D001 認證（E14-01，#436）

- 一份共用的 bearer token。API 的設定存 token 本身，STS agent 只存它的 SHA-256。
- token 至少 32 bytes 的隨機值，比照 E10-D055。
- 一期不做 mTLS。NATS 與 etcd 一期也不開 TLS（E04-D012、E12-D052）。

### E14-D002 位址（E14-01，#436）

- `AgentRecord` 加 `log_addr`。寫入時機比照 E10-D092：STS agent 當選時寫，和 `host`、`work_dir` 同一個寫入點。
- 值取 `-log-advertise`。沒有給這個旗標時是 `<hostname>:7442`。
- 7442 與現有元件的 port 沒有衝突（`docs/research/session-logs.md`）。
- 開這個端點的只有 STS agent（E14-D010）。MD、TD、controller 沒有 session log 端點，`log_addr` 留空（E14-D009）。
- `state.proto` 已定案，加欄位是相容擴充（PROTOCOL §1）。這一欄先寫在 PROTOCOL §3.6；`contract/proto` 由 #437 對齊。

### E14-D003 agent 回給 API 的格式（E14-01，#436）

- varint 長度前綴的 `mftiko.log.v1.Frame`。編碼是 protobuf（PROTOCOL §1）。
- 不用 NDJSON，也不用 base64。
- `Frame` 的欄位由 #437 寫進 `log.proto`：`data`、`next`、`gap`、`eof`、`final`、`removed`。定義在 PROTOCOL §11。

### E14-D004 時間戳（E14-01，#436）

- 一期不做 `-t`、`--since`、`--until`。
- S4 不改。每行加時間留給選做 #444。

### E14-D005 匯出格式（E14-01，#436）

- 呼叫端拿到的是 tar.gz。
- `manifest.json` 放在最後。總長與 sha256 要等串流寫完才有。
- **#437 補上 tar 的內容（E14-D014）：** 依序是 `stdout.log`、`stderr.log`、`manifest.json`。`manifest.json` 的 `stdout`、`stderr` 各有 `size` 與 `sha256`。

### E14-D006 CLI（E14-01，#436）

- 命令是 `session logs` 與 `session export-logs`。
- `--tail` 照 docker，預設全部，不固定行數。
- 加 `--latest`：最新建立的 session，取代 `drill.sh` 的 `ls -t`。
- CLI 仍只對 API 說話（E11-D001）。
- 修改 E11-D025。

### E14-D007 上限（E14-01，#436）

- 同時串流數 16、讀取速率 16 MiB/s，做成 flag。
- 一則 frame 256 KiB、follow 心跳 15 秒，寫死，不是 flag。
- 旗標的名字，除了 E14-D002 的 `-log-advertise`，由實作票寫。

### E14-D008 錯誤（E14-01，#436）

- 新的 problem code `logs_unavailable`，HTTP 503。
- CLI exit 22。`internal/cli/exit.go` 目前沒有用 22，對照由後面的票補。
- `ErrorCode` 是 enum。這一張不改 OpenAPI。若 #437 加上這個值時 `oasdiff` 判成 ERR，比照 E10-D118 把那一次寫進 `contract/openapi/api-v1-err-ignore.txt`。
- **#437 把 `logs_unavailable` 加進 `ErrorCode`。** `info.version` 是 `2.1.0`。`oasdiff` 對共用的 `ErrorCode` 報了 60 筆 ERR（`response-property-enum-value-added`），寫進 `contract/openapi/api-v1-err-ignore.txt`。新的 log 端點本身不是 breaking。

### E14-D009 範圍（E14-01，#436）

- 只有 STS session worker 的 stdout、stderr。
- `{h}.shim.log`、平台元件、MD 與 TD 的 worker、spec 刪除之後的封存，都不在 E14。

### E14-D010 傳輸（E14-01，#436）

- 傳輸照 kubelet：STS agent 讀 shim 的檔案、開 HTTP 端點，API 依 `AgentRecord.log_addr` 連過去。這是 2026-10-27 已定的做法（本票「原本就定的」），不是另開的題。
- log 不經 NATS。API 仍不連 NATS（E10-D014）。沒有 `log.*`，也沒有 `log.sts.{session_id}`（E10-D015、E10-D030）。
- 檔案仍是唯一來源。shim 的 S4、策略 SDK 與 PROTOCOL §9 不改。
- STS 的 log 留到 spec 被刪（E10-D016）。
- 一期這條 HTTP 不開 TLS。
- 修改 E10-D015、E12-D038。
- 契約 L 是草案，寫在 PROTOCOL §11。HTTP 路徑與 OpenAPI 在 #437。
- PROTOCOL 為 v1.81。`api.v1` 的 `info.version` 仍是 `2.0.0`。
- **#437 已寫入路徑、狀態碼與 OpenAPI**（E14-D011–E14-D015）。PROTOCOL 為 v1.83。`info.version` 是 `2.1.0`。

### E14-D011 兩條 stream 合併（E14-02，#437）

- API 對 agent 並行開 stdout、stderr 兩條 `GET /v1/sessions/{session_id}/logs/{stream}`。哪條的 frame 先到，就先送哪條。
- 公開的 SSE 是 `GET /sessions/{session_id}/logs`。事件 `log`、`gap` 帶 `stream`（`stdout` 或 `stderr`）。`end` 不帶 `stream`，它結束兩邊。
- 不保證跨 stream 的順序。沒有時間戳（E14-D004）。同一條 stream 的順序保留。
- 兩邊的第一則 `next` 都到了，API 才送第一則 SSE。`id` 是 `stdout=<cursor>;stderr=<cursor>`。cursor 是該 stream 最後一則 frame 的 `next`，不含分號與空白。
- 有 `Last-Event-ID` 就用這兩個 cursor 向 agent 續接，`tail` 不生效。沒有就看 `tail`：省略表示全部，API 送 `from=start`；有 `tail` 就兩邊送同一個行數。`follow` 照傳。
- 任一條 frame 的 `removed` 為真，SSE 送 `end`，`reason` 是 `removed`。`follow` 時兩邊都 `final`，`reason` 是 `terminal`。沒有 `follow` 時兩邊都 `eof`，`reason` 是 `eof`。
- `gap` 為真就送事件 `gap`。同一則 frame 又有 `data` 時，先 `gap` 再 `log`。
- worker key 是 `sts/<session_id>`。同一個 session 的所有 incarnation 寫同一組檔，契約不帶 incarnation。

### E14-D012 `tail` 單位（E14-02，#437）

- `tail` 的單位是行，不是位元組。
- 最後一行沒有換行也算一行，同 docker。
- `tail=0` 是零行。負數是 400。
- agent 的讀取恰好帶一個起點：`from=start`、`tail`、`cursor`。多給或一個都不給是 400。`from` 只接受 `start`。

### E14-D013 agent 狀態碼（E14-02，#437）

- 兩條 agent 路徑都要 `Authorization: Bearer`，值是 E14-D001 的那份 token。沒帶或對不上：401，沒有 body。
- 此 agent 的 view 沒有這個 session 的 spec：404，沒有 body。
- 同時進行的 `GET .../logs/{stream}` 與 `GET .../export` 超過上限：503，帶 `Retry-After`（delay-seconds，正整數），沒有 body。每條連線算一個，直到 body 結束。
- API 把 agent 的 401 與 503 轉成 `logs_unavailable`（HTTP 503）。404 轉成一般的 `not_found`。
- 讀取速率的上限只放慢送出，不回 503（E14-D007）。

### E14-D014 匯出（E14-02，#437）

- agent 的 `GET /v1/sessions/{session_id}/logs/export` 直接產出 tar.gz。`Content-Type` 是 `application/gzip`，帶 `Content-Length`。
- tar 裡依序是一般檔 `stdout.log`、`stderr.log`、`manifest.json`，沒有目錄前綴。前兩個是該 stream 由舊到新接起來的位元組。
- `manifest.json` 在最後（E14-D005）。UTF-8 JSON，`stdout` 與 `stderr` 各有 `size`（位元組數）與 `sha256`（該檔位元組的 SHA-256，64 個小寫 hex）。
- API 的 `GET /sessions/{session_id}/logs/export` 把 agent 的 200 body、`Content-Type` 與 `Content-Length` 原樣轉送，不把內容寫到暫存。
- 公開端點的 `Content-Type` 是 `application/gzip`。

### E14-D015 找 agent（E14-02，#437）

- API 依 spec 的 `sts_instance`，讀該 instance 當選 leader 的 `AgentRecord.log_addr`。leader 就是選主 lease 上的那一筆（PROTOCOL §3.2 W7）。
- `log_addr` 是空的、`AgentRecord` 不在，或連不上，回 `logs_unavailable`。
- 該 STS 的 region 讀不到，仍是 `region_unavailable`（E10-D014）。
- URL 是 `http://<log_addr>/v1/...`。一期這條不開 TLS（E14-D001）。
- session 在 API 的副本裡不存在，回 `not_found`，不去連 agent。
- `log.proto` 是草案，列在 `buf.yaml` 的 `breaking.ignore`。`state.proto` 的 `AgentRecord.log_addr` 是欄位 8，相容擴充。
- PROTOCOL 為 v1.83。`api.v1` 的 `info.version` 是 `2.1.0`。

### E15-D001 上傳工具是 `mftiko-pack data`（E15-01，#446）

- `mftiko-pack data -dir DIR -repo HOST/PATH [-tag TAG] [-plain-http] [-max-bytes N] [-L]`。第一個參數是 `data` 時走這條；原本打包 worker 的 `-bin …` 不變（E09-D036）。
- `mftiko` 仍只對 API 說話（E11-D001），不加上傳命令，API 也不代傳檔案。
- 帳密與 plain HTTP 跟打包策略相同：`~/.docker/config.json`（E09-D040），`-plain-http` 是布林（E10-D126）。
- 不採用：`mftiko data push`（要修改 E11-D001，或讓 API 收大檔）；另一個 binary。

### E15-D002 輸出（E15-01，#446）

- 推送成功後 stdout 只印一行 manifest digest，同 E09-D036，方便接腳本。
- 摘要（壓縮後大小、解開大小、entry 數）印 stderr。不另印 session 檔的 `inputs` 片段。
- `-tag` 選填。沒有 tag 時 manifest 以 digest 推送。tag 只供人查找，digest 與它無關。

### E15-D003 運維上傳沒有 `source`（E15-01，#446）

- config 是 `{"schema":"mftiko.data.v1"}`，沒有 `source`（PROTOCOL §8.4）。
- `DataPush.Source` 改成 `*DataSource`。STS 推 `state_out` 照舊寫 `source`，config 的位元組不變。
- 原因：`PushData` 原本一律寫 `Source: &req.Source`，零值也序列化成 `"source":{}`（main `7a8c644` 實測）。

### E15-D004 不支援的項目預設報錯，`-L` 跟著 symlink 走（E15-01，#446）

- 層裡只放解開時會接受的項目（E09-D051）：一般檔、目錄、不含 `..` 的相對 symlink。hardlink 當成各自獨立的檔案，權限只留執行位元（E09-D080）。
- 其他項目讓打包失敗，錯誤列出每個路徑與原因，不推任何 blob：絕對路徑的 symlink、target 含 `..` 的 symlink（例如 HuggingFace 快取的 `snapshots/<rev>/x -> ../../blobs/<sha>`）、socket、device、fifo、名稱以 `.wh.` 開頭的項目。
- `-L`：symlink 一律跟著走，把 target 當下的內容當成一般檔或目錄打包（同 `cp -L`、`tar -h`）。target 不存在、形成迴圈，或是 socket、device、fifo，仍然報錯。
- `state_out` 不變：跳過、寫 log、照樣推送（E09-D080）。volume 是策略在執行中寫的，推送時沒有人在旁邊。
- 不採用：照 E09-D080 跳過、只警告（缺的檔案要到 `OnInit` 讀檔才發現）；預設就跟著 symlink 走（目錄內的相對 symlink 會無聲地變成副本）。

### E15-D005 推送前檢查上限（E15-01，#446）

- 解開後：總大小 4 GiB、entry 數 100,000，算法與解開相同（E09-D053）：大小是每個 entry 宣告大小的總和，目錄與 symlink 也算 entry。
- 壓縮後：config 加層 1 GiB（E09-D013，`artifact.DefaultMaxBytes`）。`-max-bytes` 可調，要與 STS 的 `-max-artifact-bytes`、API 的 `max_input_bytes`（E15-D006）一起調。
- 先打包到暫存檔，檢查完才推。超過就不推任何 blob，錯誤寫出實際值與上限。
- 權重檔幾乎壓不動，實際的上限是壓縮後的 1 GiB，不是 4 GiB。
- STS 解開時仍用預設上限（`UnpackLimits{}`），沒有旗標，與 E09-D053「部署設定」不符。補旗標是選做 #453；E15 只寫進文件。

### E15-D006 API 在建立時檢查 input 的大小（E15-01，#446）

- `-config` 加 `max_input_bytes`。省略或 0 時是 1 GiB，與 STS 的 `-max-artifact-bytes` 預設相同。
- 每個 input 以 manifest 宣告的 config 加層大小比對，各自比、不加總（STS 也是每個 input 各自比，E09-D013）。超過回 `validation_failed`，path `/inputs/<i>/digest`，reason `is too large`。API 不下載層。
- 這個值要與 STS 的 `-max-artifact-bytes` 一致。不一致時以 STS 為準：建立通過的仍可能 `INIT_FAILED`（`artifact_too_large`）。
- 原因：`Catalog.ReadData` 原本不比大小，太大的 input 建立回 201，要到 STS 才失敗。

### E15-D007 inputs 受 `allow` 管（E15-01，#446）

- 建立 session 時，`/inputs/<i>/repository` 與 `/strategy/repository` 用同一份 `allow` 比對（E10-D119 的路徑邊界）。不在 `allow`：`validation_failed`，reason `is not allowed`。在 registry 檢查之前做，registry 掛了也一樣。
- STS 的 `--data-repository` 必須落在 `allow` 內，否則上一個 session 的 `state_out` 不能當 input。API 讀不到 STS 的設定，不在 Load 時檢查；dev、accept、drill 的設定一起改。
- 原因：不查時，持 token 的人可以讓 API（建立時讀 manifest）與 STS（下載）對任意 host 發 registry 請求。帳密依 host 對應，不會外洩（E10-D123），但 API 與 STS 主機能連到的內網可以被探測。
- 已經寫進 etcd 的 spec 不受影響，檢查只在建立時做。
- 不採用：另一份 `inputs_allow`（多一份要同步的清單）；維持 E10-D122。
- 修改 E10-D122「輸入的資料 artifact 也是完整路徑，仍不查 `allow`」。

### E15-D008 重推（E15-01，#446）

- 同一棵樹重推得到同一個 digest：打包可重現（E09-D080），config 不含 `source`。
- 推 blob 之前先 `Exists`，registry 已有就不傳。大檔推到一半失敗，重跑只補缺的。
- `state_out` 的推送走同一段，結果不變，只是少一次重傳。

### E15-D009 `mftiko-pack` 隨 CLI 發佈（E15-01，#446）

- CLI 的四個平台 tarball 都放 `mftiko` 與 `mftiko-pack`。`SHA256SUMS` 仍是每個 tarball 一列。
- 原因：準備模型的人多半用 macOS，Python 使用者不一定有 Go。`mftiko-pack` 在 darwin 編得過（`7a8c644`）。
- `go install github.com/BullionBear/mftiko/cmd/mftiko-pack@<tag>` 照樣可用。伺服器 tarball 仍不放（`docs/release.md`）。
- 修改 E11-D035「裡面只有 `mftiko`」。

### E15-D010 Python 的端到端在 lynxlinkage/mftik（E15-01，#446）

- mftik 把 pin 改到含 E15 的 mftiko commit，用釘住的 `mftiko-pack data` 推，用釘住的 artifact 套件照 STS 的方式唯讀解開，再以 `MFTIKO_INPUT_DIR` 跑打包後的 rootfs（同 mftik E05-D003）。追蹤在 #451。
- 完整的 API → controller → STS 路徑由 Go 的端到端守（#450）。STS 與語言無關（PROTOCOL §9）。
- 不採用：mftiko CI 從 PyPI 裝 `mftik`（mftiko CI 多一個 Python 依賴，版本也要另外釘）。

### E15-D011 不在 E15（E15-01，#446）

- inputs 用 tag 引用：spec 仍只收 digest（E09-D011）。
- `python -m mftik.pack data`。
- 刪除 registry 上的資料 artifact（E10-D017）。
- 不同 session 共用同一份解開的 input：每個 session 仍各解一份（E09-D074）。
- STS 解開上限的旗標（選做 #453）；console 與 `session show` 顯示 inputs（選做 #454）。
- 「唯讀」仍是權限位元（E09-D075）。shim 不切換 uid，worker 與 agent 是同一個使用者，自己 `chmod` 就改得回來；以 root 跑時不擋。文件照實寫，E15 不加隔離。
- PROTOCOL 為 v1.82。

### E00-D006 專案分三塊，加上獨立的 exchange

| 塊 | 內容 | 位置 | 可見性 |
|---|---|---|---|
| ① 平面 | 控制平面（API server、controller）、平台 runtime（agent 骨架、shim、state、natsx）、STS／MD／TD 的 agent 與 worker、SYM、md.fetch | `internal/{api, platform, sts, md, td, sym}` | 私有 |
| ② Go 策略 SDK | session worker 的那一端 | `sdk/` | 公開 |
| ③ 契約 | proto 與產生碼、protobuf 表達不了的編碼（frame、envelope、cloid）、UniversalTicker 的文法 | `contract/{proto, gen, wire, symbol}` | 公開 |
| exchange | 各交易所的 adaptor | `exchange/` | 公開 |

- **exchange 是獨立的套件，每個交易所也各自獨立。**
  - 根目錄依平面定義 adaptor 介面（E3-17 定案）：MD 用的 `Market`、`Session`、`BookSync`、`PublicREST`；TD 用的 `Account`、`PrivateStream`、`TradeStream`、`History`；SYM 讀 `Lister`，由 `PublicREST.Listing` 經 `ListingOver` 組成。`AccountKind` 是註冊用的 struct（憑證、可交易的 venue、兩座 factory），不是收發單的介面。
  - `binance/`、`bybit/`、`okx/` 各自實作。cmd 明確地把它們註冊進 MD、TD、SYM 各自的 factory，不靠 `init()` 的副作用。平面只看介面，不 import 具體的交易所。
  - 介面不放進各平面：放進去的話 exchange 要 import `internal/md`、`internal/td`，就不能獨立。
  - exchange 只依賴 contract，之後要拆成自己的 Go module 也不用改結構。一期留在同一個 module。
- **共用的交易 enum 定義在契約裡。** `Side`、`OrderType`、`TimeInForce` 放進 `common.v1`。SDK 與其他語言用這一份；SDK 不能 import exchange。exchange 另有同名型別，再加上 `OrderStatus`，數值與 `common.v1`、`td.v1` 相同，worker 以型別轉換映射，`enums_test.go` 鎖住（E03-D002，E3-17 定案）。exchange 根目錄還放憑證、錯誤分類、限速。
- **依賴規則由 `internal/archtest` 的測試檢查**，`make check` 與 CI 都會跑：
  - 平面之間互不 import；平面只用 exchange 的介面；API 不 import exchange、不用 `sym.v1`。
  - `internal/platform` 不 import 任何平面或 exchange。
  - exchange 只依賴 contract；contract 只依賴 protobuf 與標準庫。
  - sdk 只依賴 contract，整個依賴閉包不得出現 etcd、Postgres driver、exchange 或 internal：策略不碰 etcd，策略的 binary 裡也不該有簽交易所請求的程式。
  - examples 只用 sdk 與 contract，模擬外部的策略作者。
- 搬家在 E1-07 之前做，那時大部分套件還是空殼。

### E00-D007 契約分成草案與定案

- 每份契約都有狀態與定案的 EPIC，表在 PROTOCOL §1。`sym.proto` 在 E2-10 定案。`mdctl.proto`、`md.proto` 在 E4-12 定案（E04-D060）。`read.proto` 在 E4-12 定案後依 E08-D065 回到草案，E08-D066 的後續再次定案。`tdctl.proto` 在 E6-15 定案（E06-D029）。`td.proto` 在 E6-15 定案後依 E06-D033 回到草案，E06-D034 的後續再次定案。exchange 的 adaptor 介面在 E3-17 定案。其餘仍是草案。
- `buf.yaml` 的 `breaking.ignore` 列出草案的 proto：草案可以自由改，CI 不擋。定案的 EPIC 把自己的檔案移出清單，寫進完成標準。`sym.proto` 已移出。`mdctl.proto`、`md.proto` 已移出（E04-D060）。`read.proto` 依 E08-D065 放回，E08-D066 的後續再次移出。`tdctl.proto` 已移出（E06-D029）。`td.proto` 依 E06-D033 放回，E06-D034 的後續再次移出。`log.proto` 在 #437 加入清單，E14 定案時移出。adaptor 介面是 Go，沒有 proto，不進這份清單；定案之後靠 review 維持相容。C3、C4 在 E1 結束時移出。
- 代價：定案之前，不小心改壞草案不會被 CI 抓到，只能靠 review。還沒有任何東西部署，沒有相容性要保護，可以接受。
- **趁還是草案，拿掉兩條跨契約的 import：**
  - `stsctl.v1` 不再 import `state.v1`。策略的契約版本偏差最久（PROTOCOL §1），etcd 的 schema 卻是不相容就換 key 根，兩者不該綁在一起。session 的宣告（`SessionSpec` 與它的組成）抽成獨立的套件 `mftiko.session.v1`，歸 STS；`state.v1`、`stsctl.v1` 與 E10 的 API 契約都 import 它。`EntryState`、`Universe` 在 `stsctl` 裡另定綁定自己的型別。
  - `td.v1` 不再為了 `Side` import `md.v1`：`Side` 搬到 `common.v1`（E00-D006）。

### E00-D008 十進位數一律是 float64

- **價格、數量、金額、手續費、費率、greeks、IV、tick 與 step、`contract_size`、strike、槓桿，在 Go 裡是 `float64`，在契約裡是 `double`。** 範圍包括 `md.v1`、`td.v1`、`sym.v1`、SDK 與 `exchange` 的型別。這條推翻 PROTOCOL §2 原本的「十進位數一律是字串」。
- 理由：熱路徑（WS 解碼、策略）不必再 parse 字串；值不帶指標，E3 零分配解碼時不受緩衝區重用的影響；策略本來就用浮點運算，字串只是把轉換推給每個使用者。代價是不精確，用下面的規則收住。
- **不變的：** `client_order_id` 仍是十進位字串（它是整數，不是數量）；UniversalTicker 裡 strike 的拼法是身分，由 float 的最短十進位表示推出（`symbol.StrikeSpelling`，E02-D003）；Postgres 的欄位仍是 `NUMERIC`，SYM 在寫入前把 float 轉成最短的十進位文字，`"1.10"` 與 `1.1` 因此是同一個值。
- **規則**（`contract/wire/decimal`）：
  1. 入站用 `decimal.Parse`，正確捨入；NaN、Inf、十六進位、底線一律拒收，正負號由各欄位自己檢查。有效位數不超過 15 位的文字，Parse 之後再以同樣的小數位 Format，得回原文字。超過的（例如千萬級數量加 8 位小數）會被捨入，E3 的 conformance 對每個 venue 的 tick 與 step 範圍驗證。
  2. 出站用 `decimal.Append`：先對齊 tick 或 step，再以 step 的小數位、不帶指數寫出；交易所拒收 `1e-05`。有方向的對齊（買價往下、賣價往上）用 `SnapDown`、`SnapUp`，帶 1e-9 個 step 的容差：`0.3/0.1` 是 `2.9999999999999996`。
  3. 不用 `==` 比較。換成整數個 step 再比（`Steps`、`Equal`）：累計成交量是否等於下單量、持倉是否歸零都照這條。
  4. 張數換算（E02-D003）：交易所的張數是 `Snap(qty / contract_size, 張數的 step)`，下單前驗證殘差。
  5. 累加值（餘額、PnL）以交易所回報為準，本地累加只是估計，對帳帶容差。
  6. 缺值在程序裡是 `decimal.Absent()`（NaN），在契約上是不設的 optional 欄位；NaN 不上線。`Filters` 的「有這個限制但沒有界限」也是 NaN，JSON 裡是 `null`。
- 簽名的 request body 只 marshal 一次：簽那份 bytes，送那份 bytes（E3）。

### E01-D008 agent 以快照恢復，shim 的事件只是通知

- **agent 做任何決策所依據的事實，都必須在 `Attached`（或結束檔）裡有最新值。** agent 不在時（輪換、crash、斷線），shim 發出的事件可能沒人收；agent 回來 attach，拿快照和 etcd、需求比對，有差異就改。這是 level-triggered，和 W4、`Mirror` 同一套思路。
- **事件是降低延遲的通知，不是正確性的來源。** agent 不在時遺失的事件，只少了歷史（互相抵銷的轉換、轉換的時間點），不影響決策。
- **shim 不需要知道 agent 做完了沒有。** `Ack` 只讓 shim 提早釋放記憶體，不是提交；環一律最多保留 1024 則，`gap` 只說明歷史不完整。
- 不能等 agent 回來的事都在 shim：`Stop` 的計時、自己收到 SIGTERM 時送 `Stop`、結束的記錄、對 worker 的 `Relay` 回 `Undeliverable`。其餘的判斷（例如要不要重啟）本來就要等 agent，事件保留得再完整也不會更快。
- 不採用的做法：`Ack` 當成提交（agent 把效果寫進 etcd 之後才 Ack，未 Ack 的事件等於 etcd 還沒反映的部分）。它讓 shim 的保留語意綁上 agent 何時持久化，agent 還得處理 at-least-once 的冪等；而只要快照完整，這些都不需要。
- 因此 `Attached` 帶 `exiting`（worker 正在跑 OnStop 時，原本打算怎麼結束）與 `exit`（記錄完的 `ExitInfo`）。

### E02-D003 幣與數量的單位

三個欄位描述一個商品的數量、價格、損益各用什麼幣。都由 adapter 從交易所的原生欄位填（例如 OKX 的 `ctValCcy`、Deribit 期權的 `quote_currency`），下游不推導。不沿用 mftik 把 linear／inverse 寫在 `reservations.is_linear_margin` 的做法，也不為 Deribit 開特例。

這一票只定語意。三個欄位還沒進 `Listing` 與 `SymbolInfo`。契約裡既有的結算欄位仍叫 `settle`，不另開一個 `settlement_asset`。

- **`contract_value_ccy` 是 qty 的單位。** qty = 張數 × `contract_size`，以這個幣計。linear 是 base，inverse 是 quote，期權是 base（標的）。inverse 的 qty 因此是 quote 的金額，不是張數。平台上的 qty、`filled_qty`、成交 qty、持倉 size 都已經是這個乘積；下游不再乘除。現貨沒有張數，qty 是 base。
- **`price_ccy` 是 price、`price_tick`、權利金的單位。** 現貨與期貨（含 inverse）是 quote。幣本位期權是 base。U 本位期權是 quote（那個穩定幣）。
- **`settlement_asset` 是損益結算幣，不是保證金幣。** mftik 在這裡語意不一致：Binance 的 listing 填 `marginAsset`，Bitget 直接寫成 quote。這裡不沿用那兩種填法。`unrealized_pnl` 以 `settlement_asset` 計；契約欄位仍是 `settle`。現貨沒有這個欄位。
- **幣本位／U 本位是推導出來的，只給顯示與分組。** 入庫的合約與期權：`settlement_asset == base` 是幣本位，否則是 U 本位。交易路徑不看這個標籤，只看上面三個欄位。
- **quanto 一期不支援。** `settlement_asset` 既不是 base 也不是 quote（例如報價 USD、結算 USDC）就不入庫，需求項目 `REJECTED`。不留一列、也不用非 `TRADABLE` 狀態來表示它。

| 類 | 例子 | contract_value_ccy | price_ccy | settlement_asset | qty |
|---|---|---|---|---|---|
| linear | `BinanceUM_Perp_BTCUSDT`、`Bybit_Perp_BTCUSDC` | base | quote | quote | base（張數 × `contract_size`） |
| inverse | `BinanceCM_Inverse_BTCUSD` | quote | quote | base | quote（張數 × `contract_size`），不是張數 |
| U 本位期權 | `BinanceEO_Option_BTCUSDT-261226-100000-C` | base（標的） | quote（穩定幣，權利金） | quote | base（張數 × `contract_size`） |
| 幣本位期權 | `Okx_Option_BTCUSD-261225-100000-C`（settle 是 BTC；ticker 的 quote 是 strike 的幣，這裡是 USD） | base（標的） | base（權利金） | base | base（張數 × `contract_size`） |
| quanto | 報價 USD、結算 USDC 的 BTC 合約 | 不入庫 | 不入庫 | 既不是 base 也不是 quote | 不入庫。需求 `REJECTED` |

`Bybit_Perp_BTCUSDC` 與 `Bybit_Perp_BTCUSDT` 是兩個商品。表裡的三個幣是 adapter 填進去的值，不是下游拿類別回推的公式。

- **資產代碼一律大寫，別名只在 SYM 套。** 別名表例如 `XBT` → `BTC`，放在 `contract/symbol`，各 adapter 共用這一張，不各自留表。adapter 傳交易所原生代碼，不自行改名。各 venue 的別名在做 venue integration 時補進這張表。順序固定：SYM 在 `Validate` 之前先大寫並套別名，再做 quanto 判斷，再推 ticker。交易所拼法留在 `exch_ticker`（Bitmex 的 `XBTUSDT` 入庫為 `Bitmex_Spot_BTCUSDT`）。先比再收會把同一個幣看成兩種資產。
- **`USD`、`USDC`、`USDT` 是三種資產，別名表不合併。** 報價 USD、結算 USDC 依 quanto 那條，一期不入庫、需求 `REJECTED`。這是刻意的。
- **strike 先依 `spell_strike` 正規化，再推 ticker。** 整數去掉尾端的小數零，`6400.0` 與 `6400` 是同一個 ticker。有小數的 strike 把小數點寫成 `D`（PROTOCOL §2，`6.4` 是 `6D4`）。
- **filter。** `price_tick` 用 `price_ccy`。`qty_step`、`min_qty`、`max_qty` 與 qty 同一個單位（`contract_value_ccy`）。
- **交易狀態，暫定。** 契約用 status enum，不再是 `bool active`：`PRELISTING`、`TRADABLE`、`HALTED`、`SETTLING`、`EXPIRED`、`DELISTED`、`UNKNOWN`，再加上 proto 的零值 `UNSPECIFIED`（非法、不可交易、不入庫）。完整列舉還沒定案，這一票標暫定。交易路徑只接受 `TRADABLE`，其他狀態拒單。之後新增 enum 值不會讓原本不可交易的變成可交易。交易所的原生狀態對不上，記成 `UNKNOWN`，不可交易。各 venue 的原生狀態對照在做 venue integration 時補。活躍表的 CHECK 是 `PRELISTING`、`TRADABLE`、`HALTED`、`SETTLING`、`UNKNOWN`；終結表是 `EXPIRED`、`DELISTED`。
- **SYM 的 listing 分兩張表，看狀態還會不會變。** 期權與到期合約會讓 ticker 變多，熱路徑不該掃已終結的列。enum 仍是暫定，分表跟著這個暫定的切法。
  - **活躍表：** `PRELISTING`、`TRADABLE`、`HALTED`、`SETTLING`、`UNKNOWN`。狀態還可能變。Resolver 與熱路徑只讀這張表。裡面只有 `TRADABLE` 可以交易。
  - **終結表：** `EXPIRED`、`DELISTED`。一到這裡，狀態就不再變。留給歷史查詢：舊的成交與持倉，以及依 UID 反查。不再刷新，可以歸檔。
  - 列進了終結狀態，就從活躍表搬到終結表。UniversalTicker 的字串不變，UID 也不變。`symbol_filter` 同樣分活躍表與終結表，跟列一起搬。
  - E2-03 拆了表。`migrations/0002_symbol` 是活躍表 `symbol_active` 與終結表 `symbol_terminal`，欄位相同，主鍵都是 `uid`。`symbol_filter_active` 參照活躍表、`symbol_filter_terminal` 參照終結表，終結時跟列一起搬。活躍表用 generated column 從 `universal_ticker` 拆出 `venue`、`category`，並有 `UNIQUE (venue, category, exch_ticker)` 與 `universal_ticker` 唯一；終結表沒有這兩個唯一，同一個 ticker 可以留多列。`symbol_revision (venue, category, revision, refreshed_at_ns)` 給變更通知比對 revision（PROTOCOL §7.3，subject 是 `sym.v1.changed.{venue}.{category}`，E04-D057）。另有 `symbol_uid`（主鍵 `uid`，列不刪），兩張 listing 與兩張 filter 參照它，所以歸檔之後也不會再抽到同一個 uid。結算幣欄位名是 `settle`，與契約一致，另外有 `contract_value_ccy`、`price_ccy`。strike 入庫時去掉尾端的零。
- **UID 是系統內部的商品身分。** UniversalTicker 是給人看的字串，只用在寫設定與 spec。系統裡面傳商品用 UID（整數）。各平面要欄位時用 UID 向 SYM 查。STS、TD、MD 之間用 UID，不用 ticker 字串。
  - UID 由 SYM 分配，是隨機整數：唯一、永不重用，不是單調序號，也不是 Postgres sequence。活躍表與終結表合在一起唯一，同一個 UID 不會同時在兩張表。UID 跟列一起持久化，所以重啟與重載都不改它。搬到終結表時 UID 跟著列走。碰撞時 SYM 重試，再抽一個。
  - 同一個 ticker 下架之後再上市，是新的一列，拿新的 UID。舊的成交與持倉仍指向舊 UID。
  - ticker 解成 UID 只發生在設定與 spec 載入的邊界。`SessionSpec` 的 feed 仍寫 UniversalTicker，載入時用 `GetRequest` 換成 UID。
  - `symbol_filter` 也拆成活躍表與終結表。列進了終結狀態時，filter 跟 listing 列一起搬。
  - SYM 的 `List` 以 offset＋limit 分頁。翻頁期間列可能搬表或新上市，跨頁不一致可以接受。
  - `Universe.current` 是 selector，尚未定，不在這一條。selector 沒有 UID。selector 的成員今天仍是 ticker 字串；成員是商品，改成商品 UID 是後續。
  - 這一版只在 `SymbolInfo` 加上 `uid`，並加 `GetByUidRequest`。`td.order`、需求項目、selector 成員今天仍是 ticker 字串。SQL 的兩張表以 `uid` 為主鍵。
  - NATS 的 subject 用人讀的名字，不用 UID。確切拼法已由 E04-D055 取代：`md.v1.f.{inst}.{topic}.{ticker}`，不改成 UID。行情的 payload 帶 UID，不帶 ticker 字串（E08-D065）。策略 API 以 UID 為商品身分，ticker 只在 `Resolve`（E08-D004，P15）。
  - 契約型別是 `uint32`，範圍 `1..4294967295`（`1..2^32-1`），0 非法（PROTOCOL §10 P13）。Postgres 欄位是 `bigint`，`CHECK (uid BETWEEN 1 AND 4294967295)`。E2-03 的 store 照這個範圍抽號，先插入 `symbol_uid`，撞到 unique violation 就換一個再插。`symbol_reject_shared_uid` 仍擋兩張 listing 同時持有同一個 uid；搬表在同一個交易裡先刪活躍列再插入終結列。
  - **進程內 Resolver（E2-05）只給測試與開發。** 它對設定的每個 `(venue, category)` 直接呼叫 `PublicREST.Listing`，來源必須實作 `exchange.InProcessUIDSource`（`exchangetest`）。載入前先呼叫與 SYM refresh 相同的 `normalizeAssets`（大寫並套別名），再 `Listing.Validate`。只快取活躍表狀態：`PRELISTING`、`TRADABLE`、`HALTED`、`SETTLING`、`UNKNOWN`。`EXPIRED` 與 `DELISTED` 不進這個進程的快取（沒有終結表），`UNSPECIFIED` 非法。只有 `TRADABLE` 可交易。`Validate` 過了但 `Ticker()` 失敗的列略過：記 log、計數，不讓整次 venue 重載失敗。不經過 SYM 服務，也不寫 Postgres。UID 由這個進程自己發；測試注入固定順序，沒注入就用與 SYM 相同的隨機抽號，但抽到的號碼不寫進 `symbol_uid`。這種 UID 不是正式 UID：不落庫，不跨進程、不跨平面傳。真實 venue 的進程等 SYM 分配的 UID。重載在這個進程裡整份換上；還在清單上的 ticker 沿用同一個進程內 UID，從清單消失的不重用，同一個 ticker 再上市拿新的。

- **不採用：** adapter 自行改名，或在 adapter 裡收別名；下游用類別回推三個幣欄；沿用 `reservations.is_linear_margin`，或為 Deribit 開特例；把 `settlement_asset` 當成保證金幣（Binance 的 `marginAsset`、Bitget 寫成 quote）；inverse 的 qty 用張數；quanto 入庫並用非 `TRADABLE` 狀態表示；把 `USD`、`USDC`、`USDT` 收成同一個；已終結的列留在熱路徑讀的那張表；搬表時改寫 UniversalTicker 或重發 UID；下架再上市沿用舊 UID；用單調序號或 Postgres sequence（`symbol_uid_seq`）當 UID；重啟或重載時重發 UID；給 selector 一個 UID；系統內部繼續用 ticker 字串當商品身分；NATS subject 用 UID；進程內 Resolver 接真實 venue，或把進程內發的 UID 落庫、傳到別的進程或別的平面。

### E01-D009 需求過舊時保持現狀

agent 依需求做的改變，前提是需求是新的。Mirror 的 `LastContact` 超過門檻（預設 15 秒，大約是 probe 間隔的 3 倍；各平面的部署設定）就算過舊。這時 view 停在最後一次聯繫的 revision：它曾經為真，只是之後的變化看不到。

- **過舊時不做依據需求的減法，也不改變活著的 worker。** 不停 worker、不因為換 spec 而替換、不推 live config（推的可能是退訂）。恢復聯繫之後的下一輪 reconcile 才做。
  - 看不到不等於不要：不在舊 view 裡的 worker，可能正是失聯之後才加的需求。
  - 錯誤的代價不對稱：多跑一個 worker，聯繫恢復後就停掉，會自己修正；停掉一個還需要的 worker，影響傳到系統外（MD 的缺口補不回來，TD 要撤單、對帳，STS 丟掉策略的狀態）。
  - 故障是相關的：etcd 分區時所有 agent 一起過舊，過舊就減法等於一次 etcd 故障就讓交易全停。控制面的故障不擴散到資料面（fail static）。
- **維持最後一份需求由 Policy 決定。** 只有兩件事：重啟結束了的 worker、對已初始化的 worker 送 `Start`。`Policy.KeepWhileStale` 說可以才做。MD 可以（唯讀，多一份沒有副作用）；TD 由 Policy 判斷（失聯期間帳號可能被撤銷）；STS 的 `Start` 要等 controller 設 `start`（§3.3 N2），自然最多走到 `Init`。
- **過舊時不寫 `WorkerStatus`。** 寫入會卡在連不上的 cluster；狀態只在值改變時寫，恢復後一次補上（W7.2）。
- 骨架只提供「是否過舊」與上面兩類動作的區分（`internal/platform/agent` 的 `Reconciler`），平面的判斷放在 Policy。
- 不採用：過舊時照常 reconcile；過舊時只禁止減法、加法照常（新的 spawn 也是依據需求的改變，而且 TD 的加法不一定安全）；過舊時停掉一切（就是上面說的故障擴散）。

### E01-D010 平面的決策與 I/O 分開

- 平面的根套件（`internal/sts`、`md`、`td`）放 Planner、Policy 與 domain 型別，是純的邏輯，不需要 cluster 就能測。接線（Intake 的 Mirror、Fleet 的 shim、NATS）放在子套件 `internal/{plane}/daemon`。cmd 只組裝 daemon。
- 骨架同樣分開：`internal/platform/agent` 是型別與 `Reconciler`（diff 與重啟的記帳，不做 I/O）；`agent/runner` 是一個 term 的 loop（連自己 region、等需求、收養、比對 `WorkerStatus`、寫 `AgentRecord`、reconcile）；`agent/fleet` 是以 shim 為底的 Fleet，平面只提供 `Program`（跑什麼、送什麼）；`agent/c3` 是 shim 的 client；`agent/testplane` 是測試用平面（E1-19），以假 worker 跑完整個骨架。
- `internal/archtest` 檢查：平面的根套件與 `internal/platform/agent` 的依賴閉包裡沒有 etcd、NATS、`state`、`natsx`、`shim`、`agent/c3`、`agent/fleet`、`agent/runner`、`agent/testplane`。
- 子套件叫 `daemon`：它是常駐的 agent 進程。不用 `runtime`（遮住標準庫）、`agent`（與 `internal/platform/agent` 同名，平面的接線一定 import 它）、`wire`（與 `contract/wire` 同名）、`adapter`（與 exchange 的 adaptor 混淆）。

### E01-D011 live config 以 etcd 為唯一來源

- **live config 是完整的期望狀態，不是增量**：例如 MD 的 atom 集合、STS 的依賴綁定、TD 的 drain。worker 收到之後和手上的版本 diff 再套用，所以收到同一份就什麼都不做。
- **唯一的來源是 etcd，加上 SYM。** agent 每次都從 etcd 推導，不信任自己記得推過什麼，也不在 shim 另外留一份副本。
  - 推導當中依賴當下狀態的部分，例如 MD selector 的成員（價格決定置中、時間決定換月）、placement，**先寫進 etcd，再推給 worker**。否則 agent 推完、還沒寫就掛了，新 agent 依當下的狀態重算，會得到另一份。
  - 因此 Planner 的輸入是需求加上平面自己持久化的推導狀態，例如 MD 的 `selector/`、`placement/`，不只是 `demand/`。selector 的評估是一個先寫 etcd、再影響計畫的動作，不放在純的 Planner 裡（E01-D010）。這兩點在 E4、E5 實作。
- **worker 手上是哪一份，以 worker 的回報為準。** agent 推的時候附上 digest，worker 套用後回報 `Applied{digest}`，shim 把最新一則記在 `Attached`（E01-D008）。agent 拿它和 etcd 推導出的 digest 比對，不同才推。digest 防的是漂移（推到一半、套用失敗、輪換期間需求變了），不是竄改，用 `Hash()` 就夠。
- **推送走 C3／C4 的 `Live`，不走 C5 的 `Relay`**（E1-22）：digest 是骨架的事，不該寫進各平面的 C5 契約。`Init` 帶 `live_digest`，worker 套用第一份之後也回 `Applied`。送出去還沒收到 `Applied` 的，隔一段時間（預設 5 秒）再送；這也補上了套用失敗時 agent 不會再推的漏洞。E1-17 的暫行做法（收養時一律重推）就此拿掉。
- **不採用：**
  - 收養時不推：輪換期間的需求變化會被永久漏掉。
  - shim 記下 agent 交出的 `live_hash`：多一份副本，而且只代表交了，不代表套用了。
  - live config 寫成增量：重推會重複套用，漂移也無法用 digest 比出來。

### E01-D012 回應時間來源（E1-24，#277）

同一個 lease 另外呼叫一次 `cli.KeepAlive`，只取回應時間；不換掉 `concurrency.Session`，選主不變。

### E01-D013 期限（E1-24，#277）

期限 = 最後一次 keepalive 回應時間 + TTL − TTL/3（2/3 TTL）。TTL 用 server 回應裡的值，時間用單調時鐘。第一次回應前以 Grant 回應時間起算。

修訂（2026-10-24）：維持公式（最後 keepalive 回應 + TTL − TTL/3）。auth token 過期會拆掉 keepalive stream 再重連，所以 2/3 TTL 必須大於一次 token 重連所需的時間；TTL 設太短，term 會在 server 端 lease 仍有效時提前結束，這是設定的下限，不另外處理。`TestTokenExpiry` 調大 session TTL，讓 2/3 TTL 蓋得住一次重連。不採「重連中放寬」。

### E01-D014 結束方式（E1-24，#277）

期限到即以 `ErrSessionLost` 結束 term（訊息註明 keepalive 期限），與 `Session.Done()` 同一條路；term 結束時原有的行為不變，不另外 Revoke。

### E01-D015 範圍（E1-24，#277）

只改 `internal/platform/state` 的 `Term`；MD、TD、STS、controller 程式碼不另改。

### E01-D016 測試（E1-24，#277）

`TestScenarioZombieWindow` 改斷言 term 結束早於 server 端選主 key 被刪，重複多次成立；加「正常續約數個 TTL 不結束」測試；E13-D039 補註已由 #277 修正。

## 待討論

目前沒有。協議層還沒定的見 PROTOCOL §10。

已解決：舊的 Q2（綁定變化由 controller 從 ack 整理進 STS 的指派，STS agent 轉送給 worker，見 E09-D007）、Q4（durable store 是 etcd，訂單歷史另放 Postgres）、Q12（每個 region 一個 etcd，見 E00-D005）、Q1（每個策略一個 artifact，見 E09-D002、E08-D002）、Q13（生命週期兩段，見 E01-D007）、Q11（TD 也依 refcount 起停，見 E09-D001）、Q16（歷史補正的交易所讀取比照 md.fetch，TD agent 不查 SYM；成交發在 `td.evt`，由誰落庫見 PROTOCOL §10，不會是 TD agent，見 E06-D001、E06-D038）、Q9（tape 與 book 的權威在 conn worker，以 `md.view` 讀取；md.fetch 是無狀態服務，見 E04-D002）、Q3（一期沒有執行中加減訂閱的介面，見 E09-D003）、Q6（spec 不可變維持；策略狀態經 volume 與資料 artifact 延續，見 E09-D004）、Q5（單一 dispatcher、TD／MD 兩個 buffer、prehook 依 `recv_window_ms` 丟舊行情，見 E08-D003）、Q8（平台不清場；策略的 SDK 照 CancelBatch 拆批自己清，否則人工撤，見 E06-D002）、Q7（連線以 `(venue, route, n)` 共用，滿了才開下一條，不整併，見 E04-D003）、Q10（線上編碼一律 protobuf，`buf breaking` 機械檢查相容性；`md.f` 在 protobuf 前面加固定長度的二進位 envelope，見 E08-D003、PROTOCOL §1、§7.1）、Q14（行情過舊由策略自己驗時間戳；TD 降級時拒新單、放行撤單，重連後以 `OnRecon` 重置狀態，見 E06-D003）、Q15（只有一層 linger，5 秒；需求不綁 TTL，見 E09-D001、E09-D005）、Q17（backfill 的範圍從下單決定：worker 以 Report 回報每個商品第一次下單的時間；誰建 cursor 見 PROTOCOL §10，不會是 TD agent，見 E06-D001、E06-D038）、Q18（atom 的身分加上 route，商品先經 SYM 解析，見 E02-D002）、Q19（topic 名稱沿用 mftik，見 E02-D002、PROTOCOL §2）、Q20（BinanceCM、BinanceEO 維持獨立 venue、不共用連線；BinancePM 是帳號種類，見 E03-D001）。
