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

同一份 unit 用在兩台主機。部署值放在 `/etc/mftiko/<服務>/env`（0600，root），unit 用 `${VAR}` 展開。沒有寫進 unit 的旗標（etcd 密碼、`NATS_URL`、`DATABASE_URL`）由行程讀環境變數，變數名就是旗標原本的預設來源。

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
| API | `MFTIKO_COOKIE_KEY`、`MFTIKO_POSTGRES_DSN` | cookie 的 HMAC 金鑰、Postgres DSN |
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

- 每個 stream（stdout、stderr 各自）寫到 64 MiB 就輪替：`{h}.stdout` 改名成 `{h}.stdout.1`，原來的 `.1` 改成 `.2`，依此類推。
- 每個 stream 保留 5 個檔案，包含正在寫的那個；最舊的刪掉。所以一個 worker 的 stdout 最多約 320 MiB，stderr 也一樣。
- 這是預設值（`Launch.output.max_bytes`、`keep`）。下限 64 KiB，`keep` 上限 100。
- `{h}.shim.log` 寫到 10 MiB 就輪替，保留 3 個檔案，含正在寫的（E12-D042）。agent 仍把這個檔接到 shim 的 stdout、stderr，不接 pipe；shim 自己重新開啟並輪替，所以 agent 不在之後還寫得進去。三個平面的 shim 都一樣。
- MD、TD 在啟動時，以及之後每小時，刪掉不再出現的 worker key 的 stdout、stderr、`.shim.log` 與輪替檔。最後修改時間要嚴格早於 `-log-retain-days`（預設 7，小於 1 不接受）。mtime 剛好在界線上的留下。這個旗標不寫進 systemd unit。
- 還算「出現」的不刪：agent 看得到的 worker、這一輪計畫裡的、仍留著 `WorkerStatus` 的、接不上的 shim、以及 `{h}.sock` 還在的。正在跑的 worker 的舊輪替檔因此留下，份數由上面的保留數管，不由 7 天管。
- 只刪一般檔。名稱是 16 個十六進位字的 `.stdout`、`.stderr`、`.shim.log`，輪替的數字沒有前導零。其他檔（例如 `notes.txt`）、目錄、symlink 留下。log 目錄不在不是錯誤。socket 目錄讀不了就這一輪完全不刪。
- 清理失敗只寫 warn，agent 不因此退出。STS 不走這條：仍是 spec 被刪之後才清，輪替出來的 `.shim.log.N` 也在那一次刪掉。

## 監控與告警

依 [DECISIONS.md](DECISIONS.md) E12-D038–E12-D043（E12-10，#383）、E12-D062–E12-D066（#401）與 E12-D069–E12-D071（#403）。mftiko 只輸出自己的狀態。磁碟、CPU、記憶體用 node_exporter，不在 mftiko 裡。mftiko 不打 webhook，也不自己判斷門檻。下面的規則是**範例**：`for` 與門檻由部署端決定。

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
| `mftiko_worker_bus_up` | `instance`、`plane`、`region`、`worker` | md conn worker 的 NATS。`WorkerStatus` 有 type `bus_disconnected` 才有系列：status false 是 1，true 是 0。`worker` 是 `worker.key`。沒有這條的 worker（TD、STS）不出現。mftiko 不計時 |
| `mftiko_region_available` | `region` | 與 `/readyz` 同一個判斷：第一次同步完成，且 `LastContact` 沒有嚴格舊過 `unavailable_after`（預設 15 秒，E12-D040）。尚未同步是 0 |
| `mftiko_postgres_up` | 無 | 刮取當下 ping 稽核用的 Postgres 連線池。沒有池是 0。`/readyz` 仍不看 Postgres |
| `mftiko_registry_up` | 無 | API 沒有單一 registry 位址，這個 gauge 維持 0（E10-D122）。策略請求連不上仍是 `registry_unavailable` |
| `mftiko_artifact_pull_failing` | `instance`、`region`、`digest` | STS 的非 terminal session。這個 digest 有 `artifact_pull` condition 就是 1；有 session 引用、但沒有人失敗是 0。region 還沒第一次同步時不出現。MD、TD 不在這裡 |
| `mftiko_artifact_pull_fail_since_seconds` | `instance`、`region`、`digest` | 這個 digest 最早一筆 `artifact_pull` 的首次失敗時間，unix 秒。只在 failing 是 1 時出現 |

第一次同步之前，那個 region 的 agent、controller、worker 系列不出現（不是 0）。API 行程自己抓不到，看 Prometheus 的 `up`，不是 mftiko 的 gauge。

`WorkerStatus` 沒有 `PENDING`、`DEAD` 這兩個 phase。需求項目的狀態是另一個東西：`BACKOFF` 期間需求是 `PENDING`；政策放棄之後的 `EXITED` 對到 `DEAD`；`LOST` 維持最後一次的項目狀態。MD 行程內換 `conn_epoch`、TD 行程內 `DEGRADED`，都不改 `WorkerStatus` 的 phase，也不是 `DEAD`。md conn worker 的 NATS 斷線改的是 `conditions`，不是 phase，也不是 feed 的 `READY`。TD、STS 沒有 `mftiko_worker_bus_up`。API 不連 NATS，也不探測交易所。

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
      - alert: MftikoWorkerBusDown
        expr: mftiko_worker_bus_up == 0
        for: 15s
      - alert: MftikoPostgresDown
        expr: mftiko_postgres_up == 0
        for: 2m
      - alert: MftikoArtifactPullFailing
        expr: mftiko_artifact_pull_failing == 1
        for: 5m
      - alert: MftikoDiskLow
        expr: node_filesystem_avail_bytes{fstype!~"tmpfs|overlay"} / node_filesystem_size_bytes{fstype!~"tmpfs|overlay"} < 0.1
        for: 10m
```

`MftikoArtifactPullFailing` 看的是 STS 拉 artifact 的結果（E12-D069）。`mftiko_registry_up` 維持 0，API 沒有單一 registry 位址可探（E10-D122）。`MftikoAgentDown` 與 `MftikoControllerNoLeader` 要求同一個 `region` 的 `mftiko_region_available == 1`，避免 region 分割時每一台都叫。`MftikoWorkerExited` 的 `for` 放長：短暫的 `EXITED` 是正常的，status 刪掉之後這個數回到 0；一直留著才對應需求 `DEAD`。`MftikoWorkerBackoff` 對應需求 `PENDING`。`MftikoWorkerBusDown` 的 `for: 15s` 是範例，與 `unavailable_after` 同一種寫法（E12-D040、E12-D064）。重連的 handler 一觸發，gauge 就回到 1，不等緩衝排空；訂單簿要幾秒才追上（E12-D065）。TD、STS 沒有這個系列。`MftikoDiskLow` 用的是 node_exporter，不是 mftiko 的 metric。

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

`deploy/accept/accept.sh e2e main` 會走完一次：session RUNNING 時在主機上找到 log，檢查目錄 0750、檔案 0640；結束之後 log 還在；保留期（驗收設成 1 分鐘）過後 spec 被刪，log 也被清掉。見 [deploy/accept/README.md](../deploy/accept/README.md)。
