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

- shim 與 worker 沿用 agent 的使用者。TD 的 seal key 只有 `mftiko-td` 讀得到。
- `/etc/mftiko/<服務>/` 是 `root:<使用者>` 0750；設定檔 0640；祕密檔（NATS creds、signing key、seal key、registry 的 config.json）0600，屬服務的使用者。
- etcd 密碼放在 `/etc/mftiko/<服務>/env`（0600，root），以 `EnvironmentFile=` 帶進 `MFTIKO_ETCD_PASSWORD`。不用 `LoadCredential=`：unit 停下時那個目錄會被刪，agent 停下後還在跑的 worker 就讀不到 creds 了。
- 改祕密就改檔案，路徑不變，再重啟 unit。
- 同一個使用者底下，策略讀得到 sts-agent 的祕密檔與 shim 的環境，一期接受（E12-D004）。

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

## 驗收

`deploy/accept/accept.sh e2e main` 會走完一次：session RUNNING 時在主機上找到 log，檢查目錄 0750、檔案 0640；結束之後 log 還在；保留期（驗收設成 1 分鐘）過後 spec 被刪，log 也被清掉。見 [deploy/accept/README.md](../deploy/accept/README.md)。
