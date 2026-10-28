# E12-12 testnet 演練

#385 的紀錄。依 [DECISIONS.md](../../docs/DECISIONS.md) E12-D075–E12-D078。步驟照 [runbook.md](../../docs/runbook.md) 做，runbook 以外多出來的步驟記在最後一節，並補回 runbook。

`drill.sh` 從這台機器以 ssh 操作兩台主機。可以重跑的部分都在裡面：

```sh
D=deploy/drill/drill.sh
$D infra                       # 使用者、nats-server、Postgres 與 registry 的容器
$D release <tag> <dist 目錄>    # 兩台都 mftiko-release fetch、install
$D configure <nats 目錄> <tag>  # 兩台的 /etc/mftiko/*（etcd 使用者由 mftiko-etcd-roles -apply 補上）
$D session <api_id>            # 兩個演練 session
$D status                      # unit、NRestarts、readyz、gauge、shim 與 worker 的 PID
$D orders <RFC 3339>           # 兩個 session 從那個時間起的下單統計與最長空窗
$D cut <項目> ; $D uncut        # 斷網
$D sweep <api_id>              # 結束演練 session，撤掉它們的 algo 留在交易所的單
```

## 環境（2026-10-26）

| | 主機 A（cp，tailscale 100.108.10.2） | 主機 B（sys，100.127.116.76） |
|---|---|---|
| region | `jp`（home） | `jp2` |
| etcd | 3.7.2，auth、jwt（E12-06） | 同左 |
| 其他 | NATS 2.15.0（:4223）、Postgres 16 容器（:15432）、registry:2 容器（:5005）、SYM、API（:18080）、controller、md-jp-1、td-jp-1、sts-jp-1 | sts-jp2-1 |

- 兩台主機上還有別的服務（traefik、別的 Postgres 與 NATS 等）。mftiko 的每個元件都用自己的 port 與容器；斷網只用 `OUTPUT` 裡比對 mftiko 使用者 UID 的 REJECT 規則（E12-D075）。
- release 在本機建（`build-server.sh`），經 `mftiko-release` 安裝；A 是 `dd73a2d`（main），B 是 `b32b0de`，C 是 `2326586`。worker 與策略推到主機 A 的 registry。不發佈 tag。
- 交易所：BinanceUM testnet。帳號是 ed25519 金鑰（`api_id` 8585405279489955，td-jp-1）。
- 策略：`examples/strategies/drill`。在最佳買價下方 3% 掛一張買單，交易所掛上之後過 10 秒撤掉，再掛下一張；每個 hook 在 stderr 一行 JSON（E15-D022）。兩個 session：`drill-jp`（sts-jp-1，algo 1）與 `drill-jp2`（sts-jp2-1，algo 2），同一個帳號、同一個 feed `orderbook_delta.BinanceUM_Perp_BTCUSDT`（md-jp-1）。
- 基準：全部 gauge 是 1，worker 都 RUNNING；兩個 session 的單每 10 秒一輪，`orders` 的最長空窗約 10.5 秒。

## 結果

全部通過。下表的「最長空窗」是 `drill.sh orders` 對這一步前後的統計，10 秒左右表示單子沒有停。

| # | 項目 | 做法 | 期望 | 實際 | |
|---|---|---|---|---|---|
| 1 | 輪換 md、td、sts（兩台）與 controller | 每個 `systemctl stop`，20 秒，`start` | worker 不斷，新 agent 收養 | shim 與 worker 的 PID 都沒變；每個新 agent `adopted=1 unreachable=0`；`NRestarts` 0；最長空窗 10.8 秒 | 通過 |
| 1 | 換版 A → B、B → C | 兩台依序 `mftiko-release switch`（runbook §2.1） | 同上；依 E12-D024 的順序重啟 | 依序重啟 sym、api、controller、md、td、sts；worker 的 PID 不變，繼續跑舊的 binary（E01-D003）；全部收養；`NRestarts` 0；最長空窗 10.6、10.7 秒 | 通過 |
| 2 | 殺策略 worker | `kill -9` STS 的 worker | `on_failure` 重啟 | 1 秒內以 incarnation 2 重來，`OnInit`、`OnStart` 再跑一次；recon 看到上一個 incarnation 掛著的單（`open=1`），平台不替策略撤（E06-D002）。`restart_count` 原本一直是 0，修好（E12-D076）；換到 C 之後殺一次是 1 | 通過 |
| 2 | 殺 conn worker | `kill -9` MD 的 worker | 照政策重拉；策略重新同步 book | 1 秒內重拉；`conn_epoch` 1 → 2 重新連上；feed 的 ack 維持 READY；最長空窗 11.3 秒 | 通過 |
| 2 | 殺 account worker | `kill -9` TD 的 worker | 照政策重拉、recon | 1 秒內重拉，recon 之後撤單、下單照常；ack 維持 READY；最長空窗 10.5 秒 | 通過 |
| 3 | 重啟 agent 的 unit | 每個 agent `systemctl restart` | shim 與 worker 還在，收養 | 全部收養，`NRestarts` 0 | 通過 |
| 3 | agent 被 SIGKILL | 每個 agent `kill -9` MainPID | systemd 拉起來，收養 | shim 與 worker 不變；`NRestarts` 0 → 1；9–15 秒後收養：SIGKILL 不能 revoke 選主的 lease，新 agent 等它過期（E12-D057 同理）；最長空窗 11.1 秒 | 通過 |
| 4 | md-agent 與 etcd | `cut etcd-md`，49 秒 | term 在 lease 過期前結束（#277）；worker 照跑；`MftikoAgentDown` 的條件成立 | 斷線 10 秒後 term 結束（`lost the shim`），15 秒時行程退出，比 server 端的 lease 早。之後 systemd 一直重啟（連不上 etcd），`NRestarts` 到 7；`mftiko_agent_up` 0、`region_available` 1；`WorkerStatus` RUNNING；conn worker 的 PID 不變，book 照常（約 120 則／30 秒）；接回 1 秒內 `agent_up` 回到 1、收養；最長空窗 10.6 秒 | 通過 |
| 4 | md 平面與 NATS | `cut nats-md`，43 秒 | 不響（E12-10）；接回後恢復 | `agent_up` 1、RUNNING、ack READY，什麼都不響。策略的 book 停住，依 book 驅動的單停了 49 秒；接回後 1 秒恢復。沒有任何平台訊號 → #401 | 通過（照設計） |
| 4 | TD 與交易所 | `cut exchange-td`，43 秒 | 行程內重連，不是 DEAD；不響 | `agent_up`、RUNNING、ack READY 都不變。斷線中送出的撤單，接回後 recon，回 `CANCEL_REJECTED` 120「cancel was not placed」；策略 10 秒後重送，`CANCELLED`；之後照常 | 通過 |
| 4 | controller 與遠端 region | `cut ctl-jp2`，84 秒；中間在 sts-jp2-1 建一個 session | W9：維持現狀、不刪；`region_available` 維持 1 | controller 把 jp2 標成過舊，leader 1；jp2 上的 session 照常下單，什麼都沒刪；新 session 沒有 status，接回後 6 秒 `running` | 通過 |
| 5 | Postgres 停機 | `docker stop` 演練的 Postgres，16 秒 | E10-D080：降級，恢復後補上 | `postgres_up` 0、`/readyz` 200；`session end` 成功（etcd）；`session delete`、`session create`、`record list` 回 503 `storage_unavailable`；`session list` 照常但沒有建立時間與建立者；跑著的 session 不受影響。恢復後 `postgres_up` 1，delete 成功，停機中結束的 session 的 record 是 closed | 通過 |
| 6 | registry 不可用，spawn 新的 worker | `docker stop` registry；殺 conn worker 與策略 | 已就緒的版本照用（E12-D009） | `registry_up` 0；conn worker 與策略都從快取的 artifact 重拉。API 拒絕所有 session create（以 tag、已快取的 digest、新的 digest）：`registry is unavailable`，這是設計（E10-D012、E10-D057；#402） | 通過 |
| 6 | agent 拉不到新版 | `cut registry-sts`；以沒拉過的 digest 建 session | 停在 pending，接回後自己接上 | `pending`，agent 每幾秒 `artifact pull will retry: artifact_unavailable`；接回 1 秒內拉到，5 秒後 `running`。`registry_up` 一直是 1（那是 API 的視角）→ #403 | 通過 |

## 發現的問題

| | 問題 | 處理 |
|---|---|---|
| 1 | `mftiko-release install` 把 release 目錄留成 0700（`mktemp -d`），服務的使用者都執行不了：`switch` 之後每個 unit `Permission denied`，controller 停了約 3 分鐘 | 修好（E12-D076）。CI 的測試是 root 執行的，而且沒有 Postgres 時跳過，所以沒看到 |
| 2 | Binance 的帳號種類宣稱接受 hmac，但 adapter 只用 ed25519 簽：API 收下 HMAC 帳號，worker 每次連線都失敗，session `deps_timeout` | 修好（E12-D076） |
| 3 | `SessionStatus.restart_count` 從來沒寫，一直是 0 | 修好（E12-D076） |
| 4 | feed 在 conn worker 發不出 NATS 時仍是 READY | #401 |
| 5 | registry 不可用時 API 拒絕所有 session create | 照設計：建立時以 registry 驗證策略、不設快取（E10-D012、E10-D057）。#402 關閉，寫進 runbook §3.7 |
| 6 | agent 拉不到 artifact 時沒有訊號 | #403 |
| 7 | 沒有 `latest` tag 的策略 repository，STS 每分鐘 warn | #404，已由 E12-D072 修好 |
| 8 | SYM 啟動後第一次 refresh 要等抖動後的間隔，新部署一開始解析不到商品 | #405 |

## runbook 以外的步驟

演練中多做、已經補進 runbook 的（E12-D077）：

- 從有問題 1 的版本換版：用新版 tarball 裡的 `mftiko-release` 安裝，或安裝後 `chmod 0555 /opt/mftiko/releases/<tag>`（§2.1）。
- agent 被 SIGKILL 或 OOM 後，等到 15 秒才收養（§2.3）。
- Postgres 停機時 create、delete、record list 都是 503（§3.6）。
- registry 不可用時建不了 session；agent 端拉不到時 `registry_up` 仍是 1（§3.7）。

不屬於 runbook、只為演練做的：

- SYM 的第一次 refresh 用驗收工具 `mftiko-md-accept refresh` 強制（#405）。
- 策略留在交易所的單用 `drill.sh sweep` 撤掉。策略在 `OnStop` 撤不了（SDK 在結束中拒絕下單），被殺的 incarnation 的單也一樣留著。
- 主機 A 上 Docker 對發布的 port 先做 DNAT，`filter OUTPUT` 看到的是容器的位址，所以 `cut registry-sts` 在主機 A 比對 conntrack 的原始目的地。

## 收尾

- 演練環境留在兩台主機上，所有 session 都已結束，帳號沒有掛單。
- 主機 A 的 `/usr/local/lib/mftiko-e12-12/` 放著不在 release 裡的工具（`mftiko` CLI、`mftiko-pack`、worker 與策略的 binary、`mftiko-md-accept`）。
- 拆掉：兩台 `systemctl disable --now` mftiko 的 unit 與 `nats`，`docker rm -f mftiko-drill-pg mftiko-drill-registry`，刪 `/opt/mftiko/releases/*`、`/etc/mftiko/{md-agent,td-agent,sts-agent,api,sym}`、`/root/e12-12`。etcd、controller 與 E12-07 的備份 timer 是之前就有的，不在這張的範圍。
