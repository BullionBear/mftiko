# 運維：主機、log 與保留期

這份是參考。要處理告警或做常見的操作，從 [runbook.md](runbook.md) 開始（E12-D059）。

依 [DECISIONS.md](DECISIONS.md) E10-D014–E10-D017、E10-D021、E10-D022、E10-D080–E10-D083 與 [PROTOCOL.md](PROTOCOL.md) §4.5 S4、§3.2 W5（E10-19，#324）。STS session 的 stdout、stderr 用 `mftiko session logs` 與 `mftiko session export-logs` 讀（E14-D031–E14-D035）。shim log、MD、TD 仍在主機上讀檔（E10-D015）。

## log 在哪裡

每個 worker 的 stdout、stderr 由它的 shim 寫檔，放在該 agent 的 `-work-dir`（以下寫作 `${WORK_DIR}`）底下：

| 檔案 | 內容 |
|---|---|
| `${WORK_DIR}/log/{plane}/{h}.stdout` | worker 的 stdout |
| `${WORK_DIR}/log/{plane}/{h}.stderr` | worker 的 stderr，Go SDK 的 log 也在這裡 |
| `${WORK_DIR}/log/{plane}/{h}.shim.log` | shim 自己的 log（PROTOCOL §4.6） |

- `{plane}` 是 `sts`、`md`、`td`。`h` 是 worker key 的雜湊（`state.WorkerHash`），STS 的 worker key 是 `sts/<session_id>`。
- 檔案依 worker key 累積，不依 incarnation：重啟後的 worker 接著寫同一組檔案，開頭有一行 `[mftiko-shim] <時間> worker <key> incarnation <n>`。
- **session 的 log 不用自己算。** console 的 session 詳細頁與 `GET /api/v1/sessions/{id}` 的 `logs` 列出主機（agent 的 `os.Hostname()`）與 stdout、stderr 的完整路徑，來自該 STS agent 的 `AgentRecord` 裡的 `host`、`work_dir`（E10-D014）。agent 不在時顯示最後已知的值並加註；`AgentRecord` 沒有 `host`（E10-13 之前的 agent）時不顯示。STS 的 stdout、stderr 也可直接用 `mftiko session logs <id>` 與 `mftiko session export-logs <id>` 讀（旗標見 `mftiko help session`）。
- shim 寫檔跟不上（暫存超過 2 MiB）或寫入失敗（磁碟滿）時，丟掉的輸出會留一行 `[mftiko-shim] N bytes of stdout lost here: …`。worker 不會因為輸出而阻塞。

## 權限

| | 模式 | 說明 |
|---|---|---|
| `${WORK_DIR}/log/{plane}/` | `0750` | agent 與 shim 啟動時，不是 0750 才 chmod 成 0750；目錄上已有的 setgid 保留（E10-D015、E10-D022、E16-D036） |
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
- `/etc/mftiko/<服務>/` 是 `root:<使用者>` 0750；設定檔 0640；祕密檔（NATS creds、signing key、seal key、registry 的 config.json、API 的 `log-token`）0600，屬服務的使用者。STS 主機上只有 log token 的 SHA-256，放在 env（[session log 端點](#session-log-端點)）。
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
| `mftiko-etcd-backup.service`、`.timer` | root | 只在主機 A。oneshot，timer 每小時啟動一次，見「etcd 備份與還原」 |

- Postgres 用發行版套件的 unit，不在這份清單。md.fetch 沒有 unit：E12-05 的清單沒有它。
- etcd、NATS 的使用者是各自套件的預設使用者，要先存在。資料目錄 `StateDirectory=etcd` 由 systemd 建立；NATS 的儲存看它自己的設定檔。
- 不設 `LimitNOFILE=`、`Type=notify`、`PrivateTmp=`、`ExecStop=`。重啟是 `Restart=always`、`RestartSec=1s`、`StartLimitIntervalSec=0`（寫在 `[Unit]`），不照上游 unit 的重啟政策。`mftiko-etcd-backup.service` 例外：`Type=oneshot`，由 timer 啟動，不重啟。
- etcd 與 NATS 一期不開 TLS（E12-D052），跨主機流量只走私有網路或 VPN。etcd 的認證見下方「etcd 認證」。

**`systemctl kill` 會殺掉整個 cgroup。** `KillMode=process` 管的是 `systemctl stop` 與 `restart`：只對 agent 送訊號，shim 與 worker 留在同一個 cgroup，新的 agent 起來收養。`systemctl kill` 不看 `KillMode=`，預設對 cgroup 裡每一個行程送訊號，worker 會一起死。只停 agent 時用 `stop`、`restart`，或對 MainPID 送訊號。

agent 的 unit 另外：

- 沒有 `ExecStop=`。systemd 送 SIGTERM；`TimeoutStopSec=30s`，逾時的 SIGKILL 只打到 agent。
- 不開 `PrivateTmp=`、`ProtectSystem=`，也不用 `RuntimeDirectory=`。狀態目錄是 `StateDirectory=mftiko/<服務>`，即 `/var/lib/mftiko/<服務>`。`-work-dir` 與 `-data` 都是這個目錄。`${WORK_DIR}/log` 的 `mftiko-ops` 與 setgid 仍是安裝時做的，不是 unit 指令。
- binary 是 `/opt/mftiko/current/bin/…`，`-shim` 是 `/opt/mftiko/current/bin/mftiko-shim`。不帶 `-conn`、`-account`。
- `After=etcd.service` 與 `After=nats.service` 只排順序，沒有 `Requires=`。etcd 不是這個 unit 時（例如驗收用容器），agent 還是起得來。

同一份 unit 用在兩台主機。部署值放在 `/etc/mftiko/<服務>/env`（0600，root），unit 用 `${VAR}` 展開。沒有寫進 unit 的旗標（etcd 密碼、`NATS_URL`、`DATABASE_URL`、`MFTIKO_MAX_UNPACKED_BYTES`、`MFTIKO_MAX_UNPACKED_FILES`）由行程讀環境變數，變數名就是旗標原本的預設來源。

| 服務 | env 裡要有的 | 正式環境的值 |
|---|---|---|
| 三種 agent、controller、API | `MFTIKO_ETCD_PASSWORD` | 該服務 etcd user 的密碼 |
| md-agent、td-agent | `MFTIKO_INSTANCE`、`MFTIKO_REGISTRY`、`MFTIKO_PLAIN_HTTP` | 單一 registry 位址。`MFTIKO_PLAIN_HTTP` 是 `true` 或 `false` |
| sts-agent | `MFTIKO_INSTANCE`、`MFTIKO_PLAIN_HTTP_HOSTS`（可省略） | 逗號分隔的 plain-HTTP host。空的表示沒有。策略與資料的 host 來自路徑 |
| 三種 agent | `MFTIKO_REGISTRY_CREDS`（可省略） | 空的就是匿名；有帳密時是一份 Docker `config.json`，多個 host 各一筆 `auths`，0600，屬該使用者 |
| md-agent | `NATS_URL`、`MFTIKO_NATS_CREDS` | creds 檔 `/etc/mftiko/md-agent/nats.creds` |
| md-agent | `MFTIKO_MD_ENDPOINTS`、`MFTIKO_MD_REST_ENDPOINTS`（可省略） | 空的就是交易所預設 URL；有覆寫時是 JSON 檔的路徑 |
| td-agent、sts-agent | `NATS_URL`、`MFTIKO_NATS_ACCOUNT` | account 的公鑰。簽章種子是固定路徑 `/etc/mftiko/<服務>/nats-signing.nk` |
| td-agent | `MFTIKO_TD_ENDPOINTS`、`MFTIKO_TD_REST_ENDPOINTS`（可省略） | 同 MD，空的就是預設 |
| sts-agent | `MFTIKO_STS_DATA_REPOSITORY`（可省略） | 資料 artifact 的完整路徑 `host/path`。空的表示非空 volume 一直重試 |
| sts-agent | `MFTIKO_MAX_UNPACKED_BYTES`、`MFTIKO_MAX_UNPACKED_FILES`（可省略） | 一個 rootfs 或一個 input 解開後的大小與 entry 數。省略或 0 是 4 GiB、100,000。負值讓 agent 不啟動。unit 不展開這兩個變數，行程把它們當 `-max-unpacked-bytes`、`-max-unpacked-files` 的預設。要與 `mftiko-pack data` 的同名旗標一起調。rootfs 與每個 input 各自比 |
| sts-agent | `MFTIKO_LOG_TOKEN_SHA256`；`MFTIKO_LOG_LISTEN`、`MFTIKO_LOG_ADVERTISE`、`MFTIKO_LOG_MAX_STREAMS`、`MFTIKO_LOG_MAX_RATE`、`MFTIKO_LOG_MAX_BYTES`、`MFTIKO_LOG_KEEP`（可省略，unit 有預設） | log token 的 SHA-256，64 個小寫 hex。其餘見 [session log 端點](#session-log-端點) |
| API | `MFTIKO_COOKIE_KEY`、`MFTIKO_POSTGRES_DSN` | cookie 的 HMAC 金鑰、Postgres DSN。log token 不在 env，是檔案 `/etc/mftiko/api/log-token` |
| SYM | `DATABASE_URL`、`NATS_URL` | `-nats-creds` 固定 `/etc/mftiko/sym/nats.creds`，即 `mftiko-nats-dev` 的 `creds/sym-{inst}.creds`（E12-D079）。`MFTIKO_SYM_FEEDS` 可省略，省略就是全部 feed |
| etcd | `/etc/etcd/etcd.env` 的 `ETCD_NAME`、`ETCD_LISTEN_CLIENT_URLS`、`ETCD_ADVERTISE_CLIENT_URLS`、`ETCD_LISTEN_PEER_URLS`、`ETCD_INITIAL_ADVERTISE_PEER_URLS`、`ETCD_INITIAL_CLUSTER`、`ETCD_INITIAL_CLUSTER_STATE`、`ETCD_AUTH_TOKEN` | 資料目錄由 unit 設成 `/var/lib/etcd`，env 不要改 `ETCD_DATA_DIR` |

MD、TD 的 `MFTIKO_PLAIN_HTTP` 一定要是 `true` 或 `false`。unit 把它展開成 `-plain-http=${MFTIKO_PLAIN_HTTP}`，空字串這個旗標不收。STS 的 `MFTIKO_PLAIN_HTTP_HOSTS` 可以是空的（E10-D124）。

controller 連每個 region 時，密碼先讀 `MFTIKO_ETCD_PASSWORD_<region>`，沒有再用 `MFTIKO_ETCD_PASSWORD`。見 [controller](#controller)。

## controller

依 [DECISIONS.md](DECISIONS.md) E12-D044–E12-D048（E12-08，#381）。

一期在 home region 的主機跑一個 `mftiko-controller`。Unix 使用者是 `mftiko-controller`，unit 是 `mftiko-controller.service`（`Restart=always`，E12-D019、E12-D044）。設定是 `/etc/mftiko/controller/deployment.json`，祕密在 `/etc/mftiko/controller/env`。沒有熱備援，也不在每個 region 各放一個。行程退出後由 systemd 拉回。

每個 region 的 etcd 使用者都是 `controller`，角色由 `mftiko-etcd-roles` 產生。密碼可以不同：先讀 `MFTIKO_ETCD_PASSWORD_<region>`，region 名稱原樣接在後面（例如 `MFTIKO_ETCD_PASSWORD_tw`）。沒有或是空字串就用 `MFTIKO_ETCD_PASSWORD`。`-etcd-password` 只覆寫這個 fallback。每個 region 都要能得到非空密碼。

deployment JSON 的每個 region 可以加三個 Go duration 字串。沒寫、空字串或 `0s` 用預設。不另加命令列旗標。

| 欄位 | 預設 | 用途 |
|---|---|---|
| `dial_timeout` | 5 秒 | etcd client 的 `DialTimeout`：建立連線時取 auth token 與版本檢查的上限 |
| `request_timeout` | 不加額外期限 | unary RPC 的上限，含撥號時的認證。watch 與 lease keepalive 不受這欄限制。逾時的 unary 不會用全新的預算再試 |
| `stale_after` | 15 秒 | W9 看該 region `LastContact` 的門檻。嚴格舊過才算過舊 |

controller 等一個 region 撥號完成的上限，是最長 `dial_timeout` 的兩倍，至少 10 秒。比 5 秒短的 dial 仍在 dial 自己的期限內失敗。

agent、API、MD、TD 讀同一份 JSON 會收下這三欄，不改自己的 dial，也不改 W8 的過舊門檻。API 的 `unavailable_after`（`mftiko_region_available`、`/readyz`）是另一個設定，預設也是 15 秒。

換版：`mftiko-release switch` 會重啟本機已啟用的 controller（順序見 [release.md](release.md)）。需求留在 etcd，session worker 在 agent 的 cgroup。controller 重啟不停止它們。testnet 上的重啟、換版與斷線恢復留給 #385。

搬 home region（E13-D038）：兩個 `instances.controller` 不同的 controller 會各自只寫自己的 home cluster，另一個在那邊寫入時被取代。一期把重疊當成設定錯誤。先停掉舊的（`systemctl disable --now mftiko-controller`，否則 `Restart=always` 會再拉起來），把部署 JSON 的 `instances.controller` 改成同一個 region，再在新的 home 主機啟動。不要讓兩個同時跑。

和遠端 region 斷線時維持現狀（E13-D006、E12-D047）：不刪那裡的需求，也不另外寫斷線邏輯。告警用既有的 `mftiko_region_available`，見下方監控。只斷 controller、API 仍看得到該 region 時，這個 gauge 維持 1。

## etcd 認證

依 [DECISIONS.md](DECISIONS.md) E12-D049–E12-D053（E12-06，#379）。每個 region 的 etcd 都開 auth、用 jwt，每個服務只用自己的 user（PROTOCOL §3.2 W1）。etcd 一期不開 TLS（E12-D052），跨主機流量只走私有網路或 VPN。

密碼的主本在主機 A 的 `/etc/mftiko/etcd/`（`root:root` 0700，檔案 0600）：`root` 與每個 user 一個同名檔案。服務讀的是 `/etc/mftiko/<服務>/env` 裡的副本（見上方「主機上的使用者與檔案」）。`-apply` 每個 user 讀一個檔案，所以 `api`、`controller`、`root` 在每個 region 預設是同一個密碼。controller 每個 region 的密碼也可以不同（`MFTIKO_ETCD_PASSWORD_<region>`，E12-D045），見 [controller](#controller)。以下命令都在主機 A 以 root 執行，`R` 是 region 的 client URL。

### jwt 金鑰

每個 region 一對，放到該 region 的每個 member：

```sh
umask 077
openssl ecparam -name prime256v1 -genkey -noout -out jwt.key
openssl ec -in jwt.key -pubout -out jwt.pub
install -o etcd -g etcd -m 0600 jwt.key /etc/etcd/jwt.key
install -o root -g root -m 0644 jwt.pub /etc/etcd/jwt.pub
```

`/etc/etcd/etcd.env` 加上：

```sh
ETCD_AUTH_TOKEN=jwt,pub-key=/etc/etcd/jwt.pub,priv-key=/etc/etcd/jwt.key,sign-method=ES256,ttl=5m
```

### 新 cluster：開 auth、套角色表

1. 每個 member 照上一節放好金鑰與 `ETCD_AUTH_TOKEN`，啟動 etcd。
2. 建 root、開 auth（`/etc/mftiko/etcd/root` 已有就沿用，所有 region 同一個）：

   ```sh
   install -d -o root -g root -m 0700 /etc/mftiko/etcd
   test -s /etc/mftiko/etcd/root || (umask 077; openssl rand -base64 32 | tr -d '\n=+/' >/etc/mftiko/etcd/root; echo >>/etc/mftiko/etcd/root)
   export ETCDCTL_ENDPOINTS=R
   etcdctl user add root --interactive=false </etc/mftiko/etcd/root
   etcdctl user grant-role root root
   etcdctl auth enable
   ```

3. 套角色表。`-region` 省略就是部署 JSON 裡的每個 region：

   ```sh
   mftiko-etcd-roles -in <部署 JSON> -apply -secrets /etc/mftiko/etcd -region <region>
   ```

   新建的每個 user 印一行 `add user <u>, password generated into /etc/mftiko/etcd/<u>`。把該檔案的內容寫進對應服務的 env（`MFTIKO_ETCD_PASSWORD=…`），再啟動服務。

4. 檢查：`ETCDCTL_USER=root ETCDCTL_PASSWORD="$(cat /etc/mftiko/etcd/root)" etcdctl auth status` 是 `true`；以某個 agent 的 user 寫另一個 instance 的 key（例如 `etcdctl --user "<inst>:$(cat /etc/mftiko/etcd/<inst>)" put /mftiko/v1/md/<別的 inst>/epoch x`）要回 `permission denied`。

### instance 增減

- **新增：** 部署 JSON 加上 instance，再跑一次第 3 步。已有的 user 不動、不改密碼，auth revision 不變；只新增缺的 role、permission 與 user。
- **輸出裡的 `is not in the table`：** cluster 有、角色表沒有的東西。`-apply` 不刪。退役的 instance 照「永久退役一個 instance」用 `-drop`；其他的確認之後用 etcdctl 手動移除。
- **移除：** 只有退役（上一節）。

### 輪換服務的密碼

etcd 一個 user 只有一個密碼，所以服務要停一下（E12-D050）。**先停服務，再改密碼，再啟動。** 舊進程停下時要以自己的密碼 resign、revoke 選主的 session；先改密碼的話它認證不了，session 留到 TTL（15 秒）過期，新進程要等那麼久才選得上。testnet 實測：先停再改，controller 中斷約 0.5 秒；先改再重啟約 14.5 秒。agent 停的時候 worker 不中斷，新 agent 收養；controller 重新選主；API 在停著的時候不回應。四步連著做：

```sh
u=<user>   # instance 名稱、controller 或 api
(umask 077; openssl rand -base64 32 | tr -d '\n=+/' >/etc/mftiko/etcd/$u.new; echo >>/etc/mftiko/etcd/$u.new)
# 1. 新密碼寫進服務的 env（主機 B 的服務就改主機 B 上的檔案）。跑著的進程不受影響。
#    controller 若有 MFTIKO_ETCD_PASSWORD_<region>，改的是那個 region 的變數，第 3 步也只改那個 region。
sed -i "s|^MFTIKO_ETCD_PASSWORD=.*|MFTIKO_ETCD_PASSWORD=$(cat /etc/mftiko/etcd/$u.new)|" /etc/mftiko/<服務>/env
# 2. 停服務（API 是每個副本；主機 B 的服務在主機 B 上停）。
systemctl stop <unit>
# 3. 該 user 所在的每個 region 改密碼（api、controller 是每個 region）。
for R in <該 user 所在 region 的 client URL>; do
  ETCDCTL_ENDPOINTS=$R ETCDCTL_USER=root ETCDCTL_PASSWORD="$(cat /etc/mftiko/etcd/root)" \
    etcdctl user passwd "$u" --interactive=false </etc/mftiko/etcd/$u.new
done
mv /etc/mftiko/etcd/$u.new /etc/mftiko/etcd/$u
# 4. 啟動。
systemctl start <unit>
```

第 3 步失敗時，env 已經是新密碼，服務起不來：改好密碼再 start，或把 env 改回舊的（`/etc/mftiko/etcd/$u`）再 start。

輪換 root：`etcdctl user passwd root`，再更新 `/etc/mftiko/etcd/root`。不用重啟任何服務。

### 輪換 jwt 金鑰

服務不用重啟：舊金鑰簽的 token 被拒之後，client 自己重新認證（E12-D051）。

1. 照「jwt 金鑰」產生新的一對，複製到該 region 每個 member，在每台上以 rename 換掉：`install … /etc/etcd/jwt.key.new` 再 `mv /etc/etcd/jwt.key.new /etc/etcd/jwt.key`，`jwt.pub` 同樣。
2. 逐台 `systemctl restart etcd`，等健康再換下一台。auth 開了之後 `endpoint health` 要帶 root：`ETCDCTL_USER=root ETCDCTL_PASSWORD="$(cat /etc/mftiko/etcd/root)" etcdctl endpoint health`，不帶會一直失敗。
3. 每台的重啟要在 10 秒內完成，否則連著這台的服務會因 keepalive 期限結束 term 再重新參選（agent 的 worker 不受影響）。testnet 實測約 7 秒：有 client 連著時 etcd 停下要 7 秒，啟動不到 1 秒。
4. 檢查：服務沒有重啟（`systemctl show -p NRestarts <unit>` 不變），log 有幾次 `invalid auth token` 之後恢復；舊的 token（換之前用 `curl -X POST R/v3/auth/authenticate` 拿到的）現在回 `invalid auth token`。

## etcd 備份與還原

依 [DECISIONS.md](DECISIONS.md) E12-D054–E12-D058（E12-07，#380）。

### 備份

主機 A 每小時以 root 存每個 region 的快照（E12-D054）：

```sh
systemctl enable --now mftiko-etcd-backup.timer
systemctl start mftiko-etcd-backup.service   # 立刻存一次，確認能用
ls -l /var/backups/mftiko/etcd/*/
```

- 檔案是 `/var/backups/mftiko/etcd/<region>/<UTC 時間>.db`（例如 `20261026T030000Z.db`），目錄 0700、檔案 0600。快照裡有所有 key 與 user 的密碼雜湊。
- 每個 region 留最新 72 份（三天）。目錄裡別的檔案不刪。
- 部署 JSON 是 `/etc/mftiko/controller/deployment.json`，root 的密碼是 `/etc/mftiko/etcd/root`。手動執行：`mftiko-etcd-backup -in /etc/mftiko/controller/deployment.json -secrets /etc/mftiko/etcd -dir /var/backups/mftiko/etcd [-region <r>] [-keep 72]`。
- home region 的快照跟它的 etcd 在同一台。把 `/var/backups/mftiko/etcd/` 複製到另一台（例如主機 B）由部署端安排。
- 失敗時 `mftiko-etcd-backup.service` 是 failed（`systemctl list-units --failed`），log 在 `journalctl -u mftiko-etcd-backup`。告警由部署端設，例如最新一份 `.db` 超過兩小時。
- 看一份快照：`etcdutl snapshot status <檔案> -w table`，`REVISION` 是快照的 revision。

### 還原

一次還原一個 region。`r` 是 region，`R` 是它的 client URL。第 3 步與 `systemctl start etcd` 在該 region 的 etcd 主機上做，其他都在主機 A 以 root 做（root 的密碼只在主機 A）。兩台都要有 `etcdutl`：跟 etcd 在同一個官方 tarball，裝在 `/usr/bin/etcdutl`。testnet 的實測見 E12-D058。

1. **選快照、算 bump**（主機 A）。用最新的一份：快照之後的變更都會沒有（E12-D057）。bump 一律計算，不用固定值：一次還原過的 region 已經在 10 億以上（E12-D055）。

   ```sh
   export ETCDCTL_ENDPOINTS=$R ETCDCTL_USER=root ETCDCTL_PASSWORD="$(cat /etc/mftiko/etcd/root)"
   snap=/var/backups/mftiko/etcd/$r/<時間>.db
   newest=$(ls -1 /var/backups/mftiko/etcd/$r/*.db | tail -1)
   rev() { grep -o '"revision":[0-9]*' | head -1 | cut -d: -f2; }
   R_s=$(etcdutl snapshot status "$snap" -w json | rev)
   R_old=$(etcdctl get /mftiko/v1/ --limit=1 -w json | rev)                     # cluster 不回應時是空的
   R_old=${R_old:-$(etcdutl snapshot status "$newest" -w json | rev)}
   bump=$(( R_old - R_s + 1000000000 )); echo "R_s=$R_s R_old=$R_old bump=$bump"
   ```

   之後每一步都用這個 `$bump`。把快照複製到該 region 的 etcd 主機。

2. **停寫入者。** 停 controller（主機 A），以及該 region 主機上的 agent。worker 不停（`KillMode=process`），稍後由新 agent 收養。API 不用停。

   ```sh
   systemctl stop mftiko-controller                                              # 主機 A
   systemctl stop mftiko-md-agent mftiko-td-agent mftiko-sts-agent              # 該 region 的主機，有哪些停哪些
   ```

3. **還原 data dir**（該 region 的 etcd 主機）。`--name` 等四個值照 `/etc/etcd/etcd.env`，member 與 cluster ID 才不變；沒設 `ETCD_INITIAL_CLUSTER_TOKEN` 時是 etcd 的預設 `etcd-cluster`。`<bump>` 是第 1 步的值：

   ```sh
   systemctl stop etcd
   mv /var/lib/etcd /var/lib/etcd.before-restore.$(date -u +%Y%m%dT%H%M%SZ)
   set -a; . /etc/etcd/etcd.env; set +a
   etcdutl snapshot restore <快照> --data-dir /var/lib/etcd \
     --name "$ETCD_NAME" --initial-cluster "$ETCD_INITIAL_CLUSTER" \
     --initial-cluster-token "${ETCD_INITIAL_CLUSTER_TOKEN:-etcd-cluster}" \
     --initial-advertise-peer-urls "$ETCD_INITIAL_ADVERTISE_PEER_URLS" \
     --bump-revision <bump> --mark-compacted
   chown -R etcd:etcd /var/lib/etcd
   systemctl start etcd
   ```

4. **檢查**（主機 A）。`endpoint health` 要帶 root（E12-D051），第 1 步的 export 已經帶了。revision 要大於 R_old：

   ```sh
   etcdctl endpoint health
   new=$(etcdctl get /mftiko/v1/ --limit=1 -w json | rev); [ "$new" -gt "$R_old" ] && echo "ok: $new > $R_old"
   etcdctl auth status                                                         # true
   ```

5. **auth。** 角色表與 user 是快照時的。補上快照之後新增的：`mftiko-etcd-roles -in /etc/mftiko/controller/deployment.json -apply -secrets /etc/mftiko/etcd -region $r`。快照之後輪換過密碼的 user，以服務 env 裡現在的密碼再設一次：`etcdctl user passwd <user> --interactive=false`，stdin 給密碼（「輪換服務的密碼」第 3 步）。要以別的 user 測權限（`etcdctl --user …`），先 `unset ETCDCTL_USER ETCDCTL_PASSWORD`，否則 etcdctl 因為環境變數與旗標衝突而退出。

6. **加 epoch**（E12-D056），`-by` 是同一個 `$bump`。沒做這步，agent 會被 shim 以 `stale_epoch` 拒絕、一直重啟；controller 在第一次寫別的 region 時被那裡的 writer key 取代、一直重啟：

   ```sh
   mftiko-etcd-backup -in /etc/mftiko/controller/deployment.json -secrets /etc/mftiko/etcd -raise-epochs -region $r -by $bump
   ```

   每個 epoch key 印一行 `<key>: <舊> -> <新>`。

7. **啟動**，先 controller 再 agent：`systemctl start mftiko-controller`，再在該 region 的主機 `systemctl start` 第 2 步停掉的 agent。還原的是 home region 時，快照裡的選主 key 要等它的 lease 過期，controller 約 15 秒後才當選；agent 在自己的 region 也一樣（E12-D057）。

8. **驗證**，啟動 15 秒之後：
   - agent 的 log 有 `shims adopted`，沒有 `stale_epoch`；controller 的 log 沒有 `superseded`；`systemctl show -p NRestarts <unit>` 不再增加。
   - `mftiko instance show <inst>` 的 agent 在；`mftiko_agent_up`、`mftiko_controller_leader`、`mftiko_region_available` 都是 1；API `/readyz` 是 200。SSE 的 client 收到 `reset`（E10-D094）。
   - 快照之後才建立的 session 沒有了，對應的 worker 由 agent 停掉（E12-D057）。要的話重新建立。
   - 確認之後刪掉 `/var/lib/etcd.before-restore.*`。

## 輪替

- 每個 stream（stdout、stderr 各自）寫到 64 MiB，寫完那一行就輪替：`{h}.stdout` 改名成 `{h}.stdout.1`，原來的 `.1` 改成 `.2`，依此類推。一行不會被切到兩個檔，所以 Loki 收到的也是整行（E16-D037）。一個檔最多是上限加 64 KiB；比這還長的行在那裡切開。
- 每個 stream 保留 5 個檔案，包含正在寫的那個；最舊的刪掉。所以一個 worker 的 stdout 最多約 320 MiB，stderr 也一樣。
- 這是預設值（`Launch.output.max_bytes`、`keep`）。下限 64 KiB，`keep` 上限 100。
- `{h}.shim.log` 寫到 10 MiB、寫完那一行就輪替，保留 3 個檔案，含正在寫的（E12-D042）。agent 仍把這個檔接到 shim 的 stdout、stderr，不接 pipe；shim 自己重新開啟並輪替，所以 agent 不在之後還寫得進去。三個平面的 shim 都一樣。
- MD、TD 在啟動時，以及之後每小時，刪掉不再出現的 worker key 的 stdout、stderr、`.shim.log` 與輪替檔。最後修改時間要嚴格早於 `-log-retain-days`（預設 7，小於 1 不接受）。mtime 剛好在界線上的留下。這個旗標不寫進 systemd unit。
- 還算「出現」的不刪：agent 看得到的 worker、這一輪計畫裡的、仍留著 `WorkerStatus` 的、接不上的 shim、以及 `{h}.sock` 還在的。正在跑的 worker 的舊輪替檔因此留下，份數由上面的保留數管，不由 7 天管。
- 只刪一般檔。名稱是 16 個十六進位字的 `.stdout`、`.stderr`、`.shim.log`，輪替的數字沒有前導零。其他檔（例如 `notes.txt`）、目錄、symlink 留下。log 目錄不在不是錯誤。socket 目錄讀不了就這一輪完全不刪。
- 清理失敗只寫 warn，agent 不因此退出。STS 不走這條：仍是 spec 被刪之後才清，輪替出來的 `.shim.log.N` 也在那一次刪掉。

## session log 端點

依 [DECISIONS.md](DECISIONS.md) E14-D001、E14-D002、E14-D019–E14-D030（E14-08，#443）與 [PROTOCOL.md](PROTOCOL.md) §11。`mftiko session logs`、`export-logs` 與 console 的 Logs 區塊經 API 讀 STS worker 的 stdout、stderr：API 依 spec 的 `sts_instance`，連到該 instance 當選 leader 的 `AgentRecord.log_addr`。檔案仍是 shim 寫的那一份，SSH 讀檔照舊可用。

### port 與防火牆

- STS agent 聽 `MFTIKO_LOG_LISTEN`（unit 預設 `:7442`）。`log_addr` 是 `MFTIKO_LOG_ADVERTISE`；空的就是主機名加實際的 port（E14-D019）。API 主機要解析得到這個名字，名字不對就給 `MFTIKO_LOG_ADVERTISE=<私有 IP>:7442`。
- **只開給 API 主機。** 這條是明文 HTTP，一期不開 TLS（E14-D001），與 etcd、NATS 一樣只走私有網路或 VPN（E12-D052）。主機 B 的 7442 只允許主機 A（API 所在）連入。
- 聽不起來（port 被占用）時，agent 照常選主、跑 session，每 5 秒重試，log 有 `log listen`。聽上之後才寫 `log_addr`（E14-D021）。
- MD、TD、controller 沒有這個端點。

### token

- 一份共用的 bearer。API 存 token 本身：`/etc/mftiko/api/log-token`（0600，`mftiko-api`），unit 以 `-log-token` 傳入，啟動時讀一次（E14-D026）。STS 只存它的 SHA-256：`/etc/mftiko/sts-agent/env` 的 `MFTIKO_LOG_TOKEN_SHA256`（E14-D020）。
- 每個 API 副本是同一份 token，每台 STS 主機是同一個 SHA-256。
- 產生（在主機 A，以 root）：

  ```sh
  (umask 077 && openssl rand -base64 32 | tr -d '\n' >/etc/mftiko/api/log-token)
  chown mftiko-api /etc/mftiko/api/log-token
  tr -d '\n' </etc/mftiko/api/log-token | sha256sum | cut -d' ' -f1   # 這個值寫進每台 STS 的 MFTIKO_LOG_TOKEN_SHA256
  ```

  token 去掉前後空白之後至少 32 bytes，不得含空白、控制字元或非 ASCII。不合格時 API 起不來，錯誤不印 token。
- **檔案不在，API 起不來。** unit 一定帶 `-log-token`。只有不帶這個旗標的行程（不是 systemd 的部署）會照常起來，兩個 log 端點回 503 `logs_unavailable`。
- STS 的 `MFTIKO_LOG_TOKEN_SHA256` 是空的：端點不開，`log_addr` 留空，log 一行 `log endpoint disabled: -log-token-sha256 is empty`，agent 照常跑。不是 64 個小寫 hex：agent 在選主前以 exit 2 結束。

### 輪換 token

沒有新舊並存，換的時候讀 log 會短暫回 `logs_unavailable`。

1. 照上面產生新的 token 與 SHA-256。
2. 每台 STS 主機改 `MFTIKO_LOG_TOKEN_SHA256`，`systemctl restart mftiko-sts-agent`。只停 agent，shim 與 worker 繼續跑（[systemd](#systemd)）。
3. 每個 API 副本換 `/etc/mftiko/api/log-token`，`systemctl restart mftiko-api`。
4. 確認：`mftiko session logs <跑著的 session> --tail 1` exit 0。

第 2 步到第 3 步之間，API 帶舊 token，agent 回 401，API 轉成 `logs_unavailable`（E14-D013）。

### 上限

| env | unit 預設 | 意思 |
|---|---|---|
| `MFTIKO_LOG_MAX_STREAMS` | 16 | 同時進行的讀取與匯出。超過回 503 `Retry-After: 1`，API 重試後仍滿就是 `logs_unavailable`（E14-D022） |
| `MFTIKO_LOG_MAX_RATE` | 16777216 | 讀取速率，bytes/s。只放慢，不拒絕 |
| `MFTIKO_LOG_MAX_BYTES`、`MFTIKO_LOG_KEEP` | 0、0 | worker 輸出的輪替。0 是 shim 的預設 64 MiB、5 份（[輪替](#輪替)，E14-D023）。改了只影響之後啟動的 worker |

讀到已經被輪替掉的位置時，CLI 在 stderr 印 `mftiko: some output was rotated out`，console 顯示一行 `some output was rotated out`，然後從最舊還在的檔接著讀（E14-D017）。

## 監控與告警

依 [DECISIONS.md](DECISIONS.md) E12-D038–E12-D043（E12-10，#383）、E12-D062–E12-D066（#401）與 E12-D069–E12-D071（#403）。mftiko 只輸出自己的狀態。磁碟、CPU、記憶體用 node_exporter，不在 mftiko 裡。mftiko 不打 webhook，也不自己判斷門檻。規則是**範例**：`for` 與門檻由部署端決定。

`GET /metrics` 與 `HEAD /metrics` 在 API server，跟 `/healthz` 一樣不認證、不記稽核、不在 OpenAPI。靜態目錄裡若有一個叫 `metrics` 的檔，不會被這個路徑送出去。刮取不帶 bearer。

```yaml
scrape_configs:
  - job_name: mftiko-api
    honor_labels: true
    static_configs:
      - targets: ["api.example.invalid:8080"]
```

`honor_labels: true` 不能省：系列自己帶 `instance`（`md1`、`sts1` 這種 mftiko 的 instance）。省略時 Prometheus 把它改名成 `exported_instance`，`instance` 變成刮取目標的位址，告警與抑制就對不上 instance（E16-D031）。沒有 `instance` 的系列（`mftiko_region_available`、`mftiko_postgres_up` 等）照樣拿到目標的位址。

| metric | 標籤 | 意義 |
|---|---|---|
| `mftiko_agent_up` | `instance`、`plane`、`region` | 該 region 第一次同步之後，`AgentRecord` 的 key 在就是 1。後來 region 過期仍維持，系列不會消失 |
| `mftiko_controller_leader` | `instance`、`region` | 同上，controller 的 `AgentRecord`（instance 是 `controller`） |
| `mftiko_worker_phase` | `instance`、`phase`、`plane`、`region` | `WorkerStatus.phase` 的數量。`STARTING`、`RUNNING`、`STOPPING`、`BACKOFF`、`EXITED`、`LOST` 連 0 都有；另外的非空 phase 也有 |
| `mftiko_worker_bus_up` | `instance`、`plane`、`region`、`worker` | md conn worker 的 NATS。`WorkerStatus` 有 type `bus_disconnected` 才有系列：status false 是 1，true 是 0。`worker` 是 `worker.key`。沒有這條的 worker（TD、STS）不出現。mftiko 不計時 |
| `mftiko_region_available` | `region` | 與 `/readyz` 同一個判斷：第一次同步完成，且 `LastContact` 沒有嚴格舊過 `unavailable_after`（預設 15 秒，E12-D040）。尚未同步是 0 |
| `mftiko_postgres_up` | 無 | 刮取當下 ping 稽核用的 Postgres 連線池。沒有池是 0。`/readyz` 仍不看 Postgres |
| `mftiko_registry_up` | 無 | API 沒有單一 registry 位址，這個 gauge 維持 0（E10-D122）。策略請求連不上仍是 `registry_unavailable` |
| `mftiko_artifact_pull_failing` | `instance`、`region`、`digest` | STS 的非 terminal session。這個 digest 有 `artifact_pull` condition 就是 1；有 session 引用、但沒有人失敗是 0。region 還沒第一次同步時不出現。MD、TD 不在這裡 |
| `mftiko_artifact_pull_fail_since_seconds` | `instance`、`region`、`digest` | 這個 digest 最早一筆 `artifact_pull` 的首次失敗時間，unix 秒。只在 failing 是 1 時出現 |

第一次同步之前，那個 region 的 agent、controller、worker 系列不出現（不是 0）。API 行程自己抓不到，看 Prometheus 的 `up`，不是 mftiko 的 gauge。

`WorkerStatus` 沒有 `PENDING`、`DEAD` 這兩個 phase。需求項目的狀態是另一個東西：`BACKOFF` 期間需求是 `PENDING`；政策放棄之後的 `EXITED` 對到 `DEAD`；`LOST` 維持最後一次的項目狀態。MD 行程內換 `conn_epoch`、TD 行程內 `DEGRADED`，都不改 `WorkerStatus` 的 phase，也不是 `DEAD`。md conn worker 的 NATS 斷線改的是 `conditions`，不是 phase，也不是 feed 的 `READY`。TD、STS 沒有 `mftiko_worker_bus_up`。API 不連 NATS，也不探測交易所。

範例規則在 [`deploy/dev/prometheus/mftiko.rules.yml`](../deploy/dev/prometheus/mftiko.rules.yml)，`make dev-up` 的 Prometheus 載入同一份，這裡不另外抄（E16-D022）。抓取設定在同一個目錄的 `prometheus.yml`；`job_name` 要是 `mftiko-api`，`MftikoAPIDown` 看的是它。檔裡的 `for` 一律 30 秒，好在 dev 上走通（E16-D024）。正式環境從下表的 `for` 開始調：

| 告警 | 正式環境的 `for` 起點 |
|---|---|
| `MftikoAPIDown`、`MftikoRegionUnavailable`、`MftikoControllerNoLeader` | 1m |
| `MftikoAgentDown`、`MftikoPostgresDown` | 2m |
| `MftikoWorkerLost`、`MftikoArtifactPullFailing` | 5m |
| `MftikoWorkerBackoff`、`MftikoDiskLow` | 10m |
| `MftikoWorkerExited` | 15m |
| `MftikoWorkerBusDown` | 15s |

Prometheus 與 Loki 的 ruler 送同一個 Alertmanager（E16-D008）。策略 log 的告警（R1、R2、R5）、Alertmanager 與 Discord 的設定見[策略 log 與告警](#策略-log-與告警)。

`MftikoArtifactPullFailing` 看的是 STS 拉 artifact 的結果（E12-D069）。`mftiko_registry_up` 維持 0，API 沒有單一 registry 位址可探（E10-D122）。`MftikoAgentDown` 與 `MftikoControllerNoLeader` 要求同一個 `region` 的 `mftiko_region_available == 1`，避免 region 分割時每一台都叫。`MftikoWorkerExited` 的 `for` 放長：短暫的 `EXITED` 是正常的，status 刪掉之後這個數回到 0；一直留著才對應需求 `DEAD`。`MftikoWorkerBackoff` 對應需求 `PENDING`。`MftikoWorkerBusDown` 的 `for` 起點 15 秒，與 `unavailable_after` 同一種寫法（E12-D040、E12-D064）。重連的 handler 一觸發，gauge 就回到 1，不等緩衝排空；訂單簿要幾秒才追上（E12-D065）。TD、STS 沒有這個系列。`MftikoDiskLow` 用的是 node_exporter，不是 mftiko 的 metric。

#385 的演練清單（實測留到那張票）對到的訊號：

| 演練 | 看哪裡 | 範例告警 |
|---|---|---|
| 輪換 md、td、sts 與 controller，含換版。worker 不斷，新 agent 收養 | 空窗時 `mftiko_agent_up` 或 `mftiko_controller_leader` 變 0，回來變 1。worker 的 phase 維持 `RUNNING` | `MftikoAgentDown`、`MftikoControllerNoLeader`（空窗長過 `for` 才響） |
| 殺 worker。照平面規則重啟，策略端的影響見 §9.5 | phase 離開 `RUNNING`。重啟退避是 `BACKOFF`（需求 `PENDING`）；政策放棄是 `EXITED`（需求 `DEAD`）；shim 不在、最後狀態留著是 `LOST` | `MftikoWorkerBackoff`、`MftikoWorkerExited`、`MftikoWorkerLost` |
| 重啟 agent 的 systemd unit。shim 與 worker 還在，新 agent 收養 | `WorkerStatus` 不綁 lease，phase 維持 `RUNNING`。`AgentRecord` 隨 term 消失，`mftiko_agent_up` 在空窗是 0 | `MftikoAgentDown` |
| 拔掉 agent 與 etcd | API 仍看得到該 region 時，lease 到期後 `mftiko_agent_up` 變 0，`mftiko_region_available` 維持 1。API 也看不見時，`mftiko_region_available` 變 0，agent 的 0 不另外叫 | `MftikoAgentDown` 或 `MftikoRegionUnavailable` |
| 拔掉 md conn worker 與 NATS | `mftiko_agent_up` 維持 1，phase 維持 `RUNNING`，feed ack 維持 `READY`。`mftiko_worker_bus_up` 變 0，恢復後回到 1。TD、STS 沒有這個系列。重連當下 gauge 就是 1，訂單簿要幾秒才追上 | `MftikoWorkerBusDown` |
| 拔掉交易所連線 | `mftiko_agent_up` 維持 1，phase 維持 `RUNNING`。行程內重連不改 `WorkerStatus` 的 phase，不是 `DEAD` | 不響 |
| 拔掉 controller 與遠端 region | controller 的 `AgentRecord` 在 home，`mftiko_controller_leader` 維持 1。API 到該 region 的 mirror 一起過期時 `mftiko_region_available` 變 0。只斷 controller、API 仍連得上時，這個 gauge 維持 1（W9：controller 維持現狀、不寫該 region） | `MftikoRegionUnavailable`（API 也看不到時）。只斷 controller 時不響 |
| Postgres 停機。API 照 E10-D080 降級，恢復後補上 | `mftiko_postgres_up` 變 0。`/readyz` 不變 | `MftikoPostgresDown` |
| registry 不可用時 spawn 新的 worker | 需求項目維持 `PENDING`（那是項目狀態，不是 `worker_phase`）。API 建立或列出策略回 `registry_unavailable`。`mftiko_registry_up` 維持 0 | 不另設 registry 告警 |
| 只有 STS agent 連不到 registry | 非 terminal 的 `SessionStatus` 有 `artifact_pull`，`mftiko_artifact_pull_failing` 是 1。`mftiko_registry_up` 維持 0 | `MftikoArtifactPullFailing` |
| 磁碟用量 | node_exporter 的檔案系統剩餘比例。mftiko 不輸出這項 | `MftikoDiskLow` |

## 策略 log 與告警

依 [DECISIONS.md](DECISIONS.md) E16-D001–E16-D005、E16-D008–E16-D011、E16-D020–E16-D035（E16，#476）。phase 1 只收 STS session worker 的 stderr，文件稱「策略 log」（E16-D001）。Loki、Alloy、Alertmanager 由部署端安裝與維運，mftiko 不打包、不發佈。範例設定就是 `make dev-up` 跑的那幾個檔，這裡不另外抄（E16-D002、E16-D022）。告警響了怎麼處理見 [runbook §3.13](runbook.md#313-策略-log-的告警)。

### 架構

1. shim 把 session worker 的 stderr 寫到 `${WORK_DIR}/log/sts/{h}.stderr`。同一個 session 的每個 incarnation 都寫同一個檔，一個檔就是一個 session（[log 在哪裡](#log-在哪裡)）。
2. 每台 STS 主機的 Alloy 以 glob `/var/lib/mftiko/sts-agent/log/sts/*.stderr` 找檔，新的 session 自動接上，推到 Loki（E16-D003）。只收 `*.stderr`：輪替出來的 `*.stderr.N` 是同樣的行換了檔名，收了會重複。Alloy 每 `sync_period`（範例 1s）才發現新的檔；在那之前就輪替掉的第一個檔不進 Loki（E16-D038）。在追的檔輪替時，Alloy 先讀完舊檔才換到新檔，不漏。stdout 是策略自己的資料，不收。不是 JSON 的行照收，例如 shim 的 `[mftiko-shim] …` 與 Go 的 panic。
3. Loki 的 ruler 評估 R1、R2、R5，成立的送到 Alertmanager。
4. Prometheus 的 `mftiko_*` 告警（[監控與告警](#監控與告警)）送到同一個 Alertmanager（E16-D008）。
5. Alertmanager 分組、抑制，送到 Discord（[Discord](#discord)）。dev 送到假接收端（E16-D021）。

stream label 是 `host`、`region`、`plane`（`sts`）、`instance`（STS instance），加上 Alloy 給的 `filename`（E16-D004）。`region`、`instance` 要和 `mftiko_*` 的同名標籤一樣，Alertmanager 才對得上。

| 規則 | 告警 | 響的時候 |
|---|---|---|
| R1 | `MftikoStrategyLogError` | 有 warn 以上的行（`warn`、`error`、`fatal`、`panic`） |
| R2 | `MftikoStrategyLogCrash` | 有不是 JSON 的 `panic: `、`fatal error: `，或 shim 丟掉輸出時寫的 `[mftiko-shim] N bytes of … lost here`。shim 在每個 incarnation 開頭寫的那一行不算 |
| R5 | `MftikoStrategyOverloaded` | 有 SDK 以 info 寫的 `hook exceeded 100ms` 或 `td buffer full`。R1 看不到這兩種 |

- 三條都是 `count_over_time(… [5m]) > 0`：最後一行之後，條件還成立 5 分鐘。告警依 `region`、`instance`、`filename` 分；R1、R5 的行有 `session_id` 時也帶上，R2 的行不是 JSON，沒有（E16-D029）。
- annotation `logs` 是查 log 的方法：有 `session_id` 時是 `mftiko session logs <session_id> --stream stderr --tail 50`，沒有時是[從 filename 查回 session](#從-filename-查回-session)的 Loki 查詢。通知不帶 log 的內文，也不帶數值（E16-D009、E16-D010）。
- `MftikoAgentDown` 響的期間，同一個 `region`、`instance` 的 `MftikoStrategy*` 不送（`inhibit_rules`，E16-D030）。agent 不在時 worker 照跑、照寫 log；這段期間的策略錯誤，要等 agent 回來之後規則仍成立才送。
- 策略卡住或死掉就不再寫 log，這三條不會響。那一段由 `MftikoWorkerBackoff`、`MftikoWorkerExited`、`MftikoWorkerLost` 負責（E16-D008）。

### 主機

- 每台跑 STS agent 的主機一個 Alloy（主機 A、B，E16-D003）。
- Loki 與 Alertmanager 在主機 A（home region）。主機 B 的 Alloy 經私有網路推到主機 A 的 Loki（E16-D011）。
- Loki 的 HTTP 埠（範例是 3100）只開給跑 Alloy 的主機。推送與查詢是同一個埠：運維在主機 A 上查，或經 SSH 轉發。Alertmanager（範例是 9093）沒有認證，同樣只走私有網路。
- 主機沒有雲端防火牆時，在主機 A 以 nftables 自己的 table 放行（E16-D038）。Loki 的 `server.http_listen_address` 設成私有位址；gRPC（9096）設 `127.0.0.1`。tailscale 會重排自己的 iptables chain，規則不要插在 `INPUT` 裡；自己的 table 掛在 input hook、priority 比 filter 早，drop 就是最終結果。以 systemd 的 oneshot unit 在開機時 `nft -f` 載入：

  ```
  table inet mftiko_loki
  delete table inet mftiko_loki
  table inet mftiko_loki {
  	set alloy {
  		type ipv4_addr
  		elements = { 100.64.0.1, 100.64.0.2 }   # 主機 A 自己與每台跑 Alloy 的主機
  	}
  	chain input {
  		type filter hook input priority -10; policy accept;
  		tcp dport 3100 ip saddr @alloy accept
  		tcp dport 3100 drop
  	}
  }
  ```

  開頭兩行讓同一個檔可以重複載入：`nft -f` 在一個交易裡換掉整個 table。加一台 Alloy 主機就改 `elements` 再 `nft -f` 一次。
- 一期不開 TLS，與 etcd、NATS、session log 端點相同（E12-D052、E14-D001）。

### Alloy 的權限

Alloy 以自己的使用者（以下寫作 `alloy`）經 group `mftiko-ops` 讀 log，不用 root（E16-D003）。前提是部署時已照[權限](#權限)把 `${WORK_DIR}/log` 改成 `mftiko-ops` 並加上 setgid；`mftiko-release` 不做這一步。每台 STS 主機，以 root：

```sh
chgrp mftiko-ops /var/lib/mftiko/sts-agent/log
chmod 2750 /var/lib/mftiko/sts-agent/log
usermod -aG mftiko-ops alloy
systemctl restart alloy                                # 行程重啟才拿到新的 group
sudo -u alloy ls -l /var/lib/mftiko/sts-agent/log/sts/  # 讀得到，檔的 group 是 mftiko-ops
```

- setgid 只管之後建立的。chgrp 之前就有的 `log/sts/` 與檔仍是原本的 group，Alloy 讀不到：`chgrp -R mftiko-ops /var/lib/mftiko/sts-agent/log && chmod g+s /var/lib/mftiko/sts-agent/log/sts`。
- v0.4.0 與更早的 agent 與 shim 每次啟動都 chmod `log/sts/`，agent 不在 `mftiko-ops`，Linux 因此清掉 setgid，之後的檔是 agent 的 group（E16-D036）。升級之後照上一條補一次。
- worker 與 agent 不用加進 `mftiko-ops`。
- Grafana 官方的 `.deb` 安裝時會把 `alloy` 加進 `adm` 與 `systemd-journal`，讀得到 `/var/log` 與 journal。只要讀策略 log 就拿掉：`gpasswd -d alloy adm && gpasswd -d alloy systemd-journal`，再確認 `id alloy` 只剩 `alloy`、`mftiko-ops`（E16-D038）。`.deb` 第一次安裝不 enable 也不啟動 `alloy`，設定與 group 都好了再 `systemctl enable --now alloy`。
- dev 的 sts1 以 nobody 跑，Alloy 以 `473:65534`（image 裡的 `alloy` 使用者加上 nobody 的 group）跑，一樣以 group 讀（E16-D027）。

### 範例設定

檔在 [`deploy/dev`](../deploy/dev)（E16-D022）。正式環境要改的：

| 檔 | 正式環境要改的 |
|---|---|
| [`alloy/config.alloy`](../deploy/dev/alloy/config.alloy) | `__path__` 不用改：unit 的 `-work-dir` 與 dev 都是 `/var/lib/mftiko/sts-agent`。`host` 改成主機名（可用 `constants.hostname`）。`region`、`instance` 改成這台 STS agent 的：`instance` 是 env 的 `MFTIKO_INSTANCE`，`region` 是 `mftiko instance list` 的 `REGION`。`loki.write` 的 `url` 改成主機 A 的私有位址，例如 `http://loki.example.invalid:3100/loki/api/v1/push`。`sync_period` 保持 1s：比它長，一開始就寫很快的 session 可能在被發現之前就輪替掉第一個檔 |
| [`loki/mftiko.rules.yml`](../deploy/dev/loki/mftiko.rules.yml) | `for`（dev 30s）、窗口 `[5m]` 與門檻 `> 0` 由部署端定（E12-D039）。一行讓條件成立一個窗口那麼久，所以 `for` 要短於窗口，否則只有一行時永遠不響。要多幾行才叫，就提高門檻 |
| [`loki/loki.yaml`](../deploy/dev/loki/loki.yaml) | 儲存的目錄（`common.path_prefix` 以下）放到主機 A 的資料磁碟。`limits_config.retention_period`（dev 24h，[Loki 的保留期](#loki-的保留期)）。`ruler.alertmanager_url` 指到主機 A 的 Alertmanager，`ruler.evaluation_interval`（dev 15s，Loki 預設 1m）。規則檔放在 `ruler.storage.local.directory` 底下租戶的目錄；auth 關著時租戶是 `fake`，dev 掛在 `/etc/loki/rules/fake/`。`discover_service_name: []` 保留，stream label 才只有上面那幾個。`ruler.external_url` 設成 Grafana 的網址、`ruler.datasource_uid` 設成 Grafana 裡 Loki datasource 的 uid：通知的 `Source` 連結才打得開 Explore 並選好 Loki。沒設時連結是 `/explore?…` 的相對路徑（E16-D038） |
| [`alertmanager/alertmanager.yml`](../deploy/dev/alertmanager/alertmanager.yml) | receiver 換成 Discord（[下面](#discord)）。`group_wait`（dev 10s，Alertmanager 預設 30s）。`group_interval` 5m、`repeat_interval` 4h 是放寬過的起點，一個策略一直寫 error 時不洗版（E16-D009）。`inhibit_rules` 照抄 |
| [`prometheus/`](../deploy/dev/prometheus) | `mftiko_*` 的規則與刮取，見[監控與告警](#監控與告警)。`alerting` 指到主機 A 的 Alertmanager |

### 在已經有 log 的主機加上 Alloy

Alloy 第一次看到一個檔，從頭讀。檔裡的舊行以讀到的時間進 Loki，所以 R1、R2、R5 會為很久以前的行響，Discord 跟著洗版。STS 主機已經跑過 session 時，先把現有的 `*.stderr` 記成讀到檔尾，再啟動 Alloy（E16-D038）：

```sh
# 以 root，alloy 還沒啟動；HOST、INSTANCE、REGION 與 config.alloy 的 host、instance、region 相同
POS=/var/lib/alloy/data/loki.source.file.sts/positions.yml
install -d -o alloy -g alloy -m 0750 "$(dirname $POS)"
{
  echo "positions:"
  for f in /var/lib/mftiko/sts-agent/log/sts/*.stderr; do
    [ -e "$f" ] || continue
    printf '  ? path: %s\n    labels: '"'"'{host="%s", instance="%s", plane="sts", region="%s"}'"'"'\n  : "%s"\n' \
      "$f" "$HOST" "$INSTANCE" "$REGION" "$(stat -c %s "$f")"
  done
} > $POS && chown alloy:alloy $POS
systemctl enable --now alloy
```

- 目錄名是 `loki.source.file` 元件的 id（`loki.source.file "sts"`）。`labels` 要和 `path_targets` 去掉 `__path__` 之後的 label 一字不差，依 key 排序，否則 Alloy 當成新的檔、從頭讀。
- 記下之後才寫的行不會漏：Alloy 從記下的位置讀起。之後新的 session 的檔照樣從頭讀。
- 不用 `tail_from_end = true`：它也套用在之後新的檔上。Alloy 每 `sync_period` 才發現新檔，session 一開頭的幾行（例如 `init failed`）會被跳過。

### 從 filename 查回 session

檔名的 `{h}` 是 `WorkerHash("sts/<session_id>")`，反推不回 session（E16-D004）。告警帶 `session_id` 時，直接跑 `logs` 那一條命令。只有 `filename` 時（R2，或行裡沒有 `session_id`）：

- **Loki**（spec 刪掉之後也查得到，直到 Loki 的保留期）：`{filename="<filename>"} |= "[mftiko-shim]"`，就是 annotation `logs`。shim 在每個 incarnation 開頭寫一行 `[mftiko-shim] <時間> worker sts/<session_id> incarnation <n>`，所以查詢的時間範圍要涵蓋 session 開始或重啟的時候。在 Grafana 的 Explore 貼上這個查詢，或在主機 A：

  ```sh
  curl -sG http://127.0.0.1:3100/loki/api/v1/query_range --data-urlencode since=168h \
    --data-urlencode 'query={filename="<filename>"} |= "[mftiko-shim]"' | grep -o 'worker sts/[^ ]*' | sort -u
  ```

- **主機上**（檔還在時）：`grep -h '^\[mftiko-shim\] .* worker ' <filename>*`，含輪替出來的檔。
- **API**（spec 還在時）：`mftiko session list -o json` 裡 `logs.stderr` 等於這個 `filename` 的那一筆，例如 `mftiko session list -o json | jq -r --arg f '<filename>' '.items[] | select(.logs.stderr == $f) | .id'`。

### Discord

用 Alertmanager 內建的 `discord_configs`，不另做轉接（E16-D009）。dev 沒有 Discord，`make dev-alert-smoke` 只送假接收端（E16-D034）。

1. 在 Discord 頻道設定的「整合」建立一個 webhook，複製它的 URL。URL 是祕密：拿到就能往頻道發訊息。
2. 在主機 A 以 root 寫進檔：0600，屬 Alertmanager 的使用者（以下寫作 `alertmanager`），不進 repo。從 stdin 讀，URL 不進 shell 的歷史。換 URL 也照這三行：

   ```sh
   (umask 077 && tr -d '\n' >/etc/alertmanager/discord-webhook.new)   # 貼上 URL，Enter，Ctrl-D
   chown alertmanager /etc/alertmanager/discord-webhook.new
   mv /etc/alertmanager/discord-webhook.new /etc/alertmanager/discord-webhook
   ```

3. Alertmanager 的設定。route 與 `inhibit_rules` 同 dev 的檔，只換 receiver，`group_wait` 用 Alertmanager 的預設：

   ```yaml
   route:
     receiver: discord
     group_by: [alertname, instance, filename]
     group_interval: 5m
     repeat_interval: 4h
   receivers:
     - name: discord
       discord_configs:
         - webhook_url_file: /etc/alertmanager/discord-webhook
   inhibit_rules:
     - source_matchers: ['alertname="MftikoAgentDown"']
       target_matchers: ['alertname=~"MftikoStrategy.*"']
       equal: [region, instance]
   ```

4. 檢查設定，再 reload。改了設定或 webhook 的檔之後都要 reload：

   ```sh
   amtool check-config /etc/alertmanager/alertmanager.yml
   curl -fsS -X POST http://127.0.0.1:9093/-/reload
   ```

   reload 失敗時 Alertmanager 繼續用原本的設定，`curl` 以非 0 結束，Alertmanager 的 log 有原因。
5. 確認：送一則測試告警，頻道收到 `[FIRING:1] MftikoDiscordTest`。它沒有後續就自己解除。

   ```sh
   amtool --alertmanager.url=http://127.0.0.1:9093 alert add alertname=MftikoDiscordTest instance=test region=test
   ```

Discord 的訊息用 Alertmanager 預設的範本：告警名稱、label 與 annotation（`summary`、`logs`）。規則不放 log 的內文，訊息裡就沒有（E16-D010）。

### Loki 的保留期

- 由部署端設：`limits_config.retention_period`，並開 `compactor.retention_enabled`，否則 Loki 不刪（dev 24h，E16-D011、E16-D024）。
- 和 session 的[保留期](#保留期)是兩回事。spec 刪掉時，STS 只刪主機上的檔（E10-D016），Loki 裡的副本留到 Loki 自己的保留期。反過來，Loki 已經刪掉的行，spec 還在時仍可以 `mftiko session logs` 讀，只是主機上的檔有[輪替](#輪替)的上限。
- 策略的 stderr 集中存進 Loki，可能留得比主機上的檔久。策略不要把祕密寫進 log，例如 `InitPayload.nats` 的 seed（PROTOCOL §6.1，E16-D011）。

### dev 與 smoke

- `make dev-up` 跑 Alloy、Loki、Prometheus、Alertmanager 與假接收端 `mftiko-webhook-dev`。每則通知（含 resolved）是 `deploy/dev/data/webhook/received.jsonl` 的一行（E16-D021）。Loki 在 `http://127.0.0.1:3100`，Alertmanager 在 `http://127.0.0.1:9093`。dev 的 `md1`、`td1` 沒有跑，`MftikoAgentDown` 對它們一直響（E16-D029）。
- `make dev-alert-smoke`（先 `make dev-up`、`make migrate-up`）以 repo 的 `examples/strategies/conform` 開三個 session，讓 R1、R2、R5 各響一則：確認假接收端收到、照 `logs` 查得回 session。R2 的那一行是 smoke 附加到 session 的 stderr 的，不是真的 panic。接著停掉 sts1，確認 Alertmanager 有 `MftikoAgentDown{instance="sts1"}`、三則策略告警被它抑制、假接收端收到它，最後把 sts1 啟動回來（E16-D032–E16-D035）。約兩分鐘。
- 它會停 sts1 約一分鐘，sts1 有跑著的 session 時不跑。上一次停 sts1 的 `MftikoAgentDown` 還沒送出 resolved 時，先等它，最多 6 分鐘。通過時刪掉自己的 session，失敗時留著並印出 id。

## 上傳資料 artifact

依 [PROTOCOL.md](PROTOCOL.md) §8.4 與 [DECISIONS.md](DECISIONS.md) E15-D001–E15-D011（E15-07，#452）。解開上限見 E15-D012–E15-D016（E15-08，#453）。預訓練模型這類檔案（`.pt`、`.pth`、`.pkl`）以資料 artifact 交給 session，唯讀，不用重 build 策略 image。故障處理在 [runbook §3.11](runbook.md#311-input-拉取失敗)。

### 流程

1. 準備一個目錄，例如 `./model/`。目錄本身不進路徑：`./model/model.pt` 解開後是 `$MFTIKO_INPUT_DIR/<name>/model.pt`。
2. `mftiko-pack data` 推上去。stdout 只有 manifest digest，stderr 是 entry 數與解開前後的大小：

   ```sh
   mftiko-pack data -dir ./model -repo registry.example/data/model -tag v1
   ```

   `-tag` 選填，digest 不受它影響。同一個目錄重推得到同一個 digest，registry 上已有的 blob 不重傳。`mftiko-pack` 隨 CLI 的 release tarball 發佈（[cli.md](cli.md)）。
3. session 檔加上 `inputs`。`digest` 只收 digest，不收 tag：

   ```yaml
   inputs:
     - name: model                          # ^[a-z0-9][a-z0-9_-]{0,63}$；策略以這個名字找目錄
       repository: registry.example/data/model
       digest: sha256:…                     # mftiko-pack data 印出的
   ```

4. `mftiko session create -f session.yaml --wait running`。API 建立時讀 manifest：repository 不在 `allow`、不是資料 artifact、超過 `max_input_bytes`，都是 400 `validation_failed`（CLI exit 10），`errors[].path` 指到 `/inputs/<i>/repository` 或 `/inputs/<i>/digest`。
5. 策略在 `OnInit` 讀：Go 是 `ctx.InputDir("model")`，Python 是 `ctx.input_dir("model")`。

### 格式與目錄

- 格式見 PROTOCOL §8.4：manifest 的 `artifactType` 是 `application/vnd.mftiko.data.v1`，一層 tar+gzip，config 沒有 `source`。用 `mftiko-pack data` 推，不要手組 manifest。
- STS 在 worker 啟動之前拉下並解到 `${DATA}/sessions/<session_id>/inputs/<name>/`，也就是 worker 的 `$MFTIKO_INPUT_DIR/<name>/`。每個 input 都就緒才啟動 worker。
- 檔案 0444、目錄 0555，屬 STS agent 的使用者（E09-D075）。**唯讀只是權限位元**：worker 與 agent 是同一個使用者，自己 `chmod` 就改得回來，以 root 跑時完全不擋。要寫的東西寫 `$MFTIKO_VOLUME_DIR`。
- symlink：相對且留在目錄內的照原樣存，解開後仍是 symlink。絕對路徑或含 `..` 的 symlink、socket、device、fifo、`.wh.*` 讓 `mftiko-pack data` 在送出任何請求前失敗；`-L` 改存 symlink 指到的內容（E15-D004）。
- session 結束後 STS 刪掉 inputs 目錄（[保留期](#保留期)）。registry 上的 artifact 不動。

### 上限

| | 預設 | 誰檢查 | 要調整 |
|---|---|---|---|
| 壓縮後（config 加層） | 1 GiB | `mftiko-pack data -max-bytes`、API 的 `max_input_bytes`、STS 的 `-max-artifact-bytes` | 三個一起調 |
| 解開後 | 4 GiB | `mftiko-pack data -max-unpacked-bytes`、STS 的 `-max-unpacked-bytes`（`MFTIKO_MAX_UNPACKED_BYTES`） | 兩個一起調。省略或 0 是 4 GiB |
| entry 數 | 100,000，目錄也算 | `mftiko-pack data -max-unpacked-files`、STS 的 `-max-unpacked-files`（`MFTIKO_MAX_UNPACKED_FILES`） | 兩個一起調。省略或 0 是 100,000 |

- 權重檔幾乎壓不動，實際上以壓縮後的 1 GiB 為準。
- 解開上限跟著部署走。`mftiko-pack data` 與 STS 要一起調，否則大於 4 GiB 的資料推不出去（E15-D015）。rootfs 與每個 input 各自比、不加總。負值讓 STS 不啟動，也讓打包失敗。超過是 `INIT_FAILED`、reason `artifact_unsafe_layer`。API 不下載層，也不檢查解開後大小（E15-D016）。
- STS 的 `-max-artifact-bytes` 與策略 image 共用（E09-D013）。systemd unit 沒有帶這個旗標，要調就用 drop-in 覆寫 `ExecStart`。API 的 `max_input_bytes` 比 STS 的大時，太大的 input 建立會成功，到 STS 才以 `artifact_too_large` 失敗。

### 帳密與 HTTP

- **推的人：** `mftiko-pack` 讀 `~/.docker/config.json`（`DOCKER_CONFIG` 可換目錄）。先 `docker login <host>`。
- **讀的人有兩個：** API 建立時讀 manifest（API 設定的 `registry_creds`），STS 拉層（`MFTIKO_REGISTRY_CREDS`）。兩份 `config.json` 都要能讀這個 host（E10-D123、E10-D128）。API 讀得到、STS 讀不到時，建立成功，session 以 `artifact_unauthorized` 失敗。
- **走 HTTP 的 host：** API 的 `plain_http_hosts`、STS 的 `MFTIKO_PLAIN_HTTP_HOSTS`、`mftiko-pack data -plain-http`，三處都要有（E10-D124）。

### allow

- input 的 repository 與策略一樣，必須在 API 的 `allow` 內（E15-D007，修改 E10-D122）。
- STS 的 `MFTIKO_STS_DATA_REPOSITORY`（`state_out` 推到這裡）也要在 `allow` 內，否則下一個 session 以 `state_out` 接手時建立會被擋。慣例是 `allow` 同時列 `<host>/strategies/` 與 `<host>/data/`，`deploy/accept` 就是這樣設。

### dev

`make dev-up` 跑 controller 與 `sts1`（E15-10），沒有 feeds、沒有 accounts 的 session 會真的跑起來。`md1`、`td1` 不跑，帶 feeds 或 accounts 的 session 停在等依賴；那種用 `deploy/accept`。

- registry 在主機上是 `127.0.0.1:5000`，在 compose 裡（API 與 `sts1` 看到的）是 `registry:5000`。從主機推用前者，session 檔寫後者。內容相同，digest 相同。dev 的 `allow` 是整個 `registry:5000`。
- `sts1` 在 Linux 容器裡跑 worker，所以策略要是 Linux 的 binary。`make dev-up` 把 `examples/strategies/` 建成 Linux 版放在 `bin/linux/examples/`。
- `sts1` 以 `nobody` 跑，input 的 0444、0555 擋得住寫入。
- `mftiko session logs` 在 dev 可用：API 與 `sts1` 共用 `deploy/dev/data/api/log-token`。

以 `examples/strategies/inputs` 為例。它在 `OnInit` 比對 params 裡的 sha256，並確認寫入被拒：

```sh
make dev-up
make migrate-up   # 第一次，或有新的 migration 時
make build        # bin/mftiko、bin/mftiko-pack
bin/mftiko-pack -bin bin/linux/examples/inputs -name inputs -repo 127.0.0.1:5000/strategies/inputs -tag dev -plain-http
bin/mftiko-pack data -dir ./model -repo 127.0.0.1:5000/data/model -tag v1 -plain-http   # 印出 sha256:…
shasum -a 256 model/model.pt                                                               # 給 params
```

`inputs.yaml`，CLI 的 context 與 `mftiko login` 見 [cli.md](cli.md)（dev 的 token 在 `deploy/dev/data/api/token`）：

```yaml
name: inputs
sts_instance: sts1
strategy:
  repository: registry:5000/strategies/inputs
  ref: dev
params:
  input: model
  sha256:
    model.pt: "<shasum 的結果>"
inputs:
  - name: model
    repository: registry:5000/data/model
    digest: sha256:…                       # mftiko-pack data 印出的
restart:
  mode: never
```

```sh
bin/mftiko session create -f inputs.yaml --wait running
bin/mftiko session logs <session_id> --stream stderr
# {"level":"info","component":"strategy","hook":"OnInit","input":"model","files":1,"message":"inputs"}
bin/mftiko session end <session_id> --wait --yes
```

sha256 寫錯時，session 以 `failed` 結束，last exit 是 `init_failed`，reason 指出哪個檔案不符。`make dev-down` 保留資料，`make dev-up` 之後可以照樣重跑。完整路徑另有 `accept.sh e2e cli inputs`（[deploy/accept/README.md](../deploy/accept/README.md)）。

### 容量

資料 artifact 一期不刪（E10-D017）：上傳的模型與 `state_out` 都留在 registry，每個版本都累積。要清理在 registry 端做；先確認沒有還在的 session 引用那個 digest（`mftiko session show <id>` 的 spec 列出 `inputs`）。

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

- registry 上的資料 artifact（`state_out` 與[上傳的 input](#上傳資料-artifact)）一期不刪，新的 session 可以用 digest 接手；容量由運維管理（E10-D017）。
- `audit` 的列一期不刪（E10-D010）。
- Loki 裡策略 stderr 的副本，留到 Loki 自己的保留期（[Loki 的保留期](#loki-的保留期)，E16-D011）。

## 舊驗收庫補記 schema_migrations

依 [DECISIONS.md](DECISIONS.md) E12-D031（E12-13，#391）。`accept.sh up` 從空資料庫呼叫 `mftiko-migrate up`，不再讀 `accept_migrations`。只給開發用的資料庫，不在 [runbook](runbook.md) 裡（E12-D060）。

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

`deploy/accept/accept.sh e2e main` 會走完一次：session RUNNING 時在主機上找到 log，檢查目錄 0750、檔案 0640；結束之後 log 還在；保留期（驗收設成 1 分鐘）過後 spec 被刪，log 也被清掉。見 [deploy/accept/README.md](../deploy/accept/README.md)。`accept.sh e2e cli logs` 經 CLI 讀、追蹤、匯出 log，跨過 STS agent 重啟，並讓輸出輪替出 gap（E14-08）。
