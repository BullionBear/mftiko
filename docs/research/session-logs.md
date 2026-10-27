# session log

這一頁記 E14 已經定的事（E14-D001–E14-D010，#436，2026-10-27）。HTTP 路徑與 `Frame` 的欄位已寫在 PROTOCOL §11（#437，E14-D011–E14-D015）。讀檔的 cursor、CLI 的輸出細節、console 的版面仍由 #438–#444 寫，這裡不定。

檔案仍是唯一來源。shim 的 S4、策略 SDK、PROTOCOL §9 不改。STS 的 log 留到 spec 被刪（E10-D016）。

## 3. 傳輸

傳輸照 kubelet，2026-10-27 已定（E14-D010）：

- STS agent 讀 shim 寫下的檔案，自己開一個 HTTP 端點。
- API 依該 STS 的 `AgentRecord.log_addr` 連過去。
- log 不經 NATS。沒有 `log.*`，也沒有 `log.sts.{session_id}`（E10-D015、E10-D030）。API 仍不連 NATS（E10-D014）。
- 一期這條 HTTP 不開 TLS，也不做 mTLS。NATS 與 etcd 一期也不開 TLS（E04-D012、E12-D052）。

這條路改寫 E10-D015 的「一期沒有 log 的資料流」與 E12-D038 的「agent 不另外開端點」。檔案、權限、不接 `log.*` 仍在。`/metrics` 仍只在 API server。STS agent 開的是 session log 端點，不是 metrics 端點。

## 4. 介面

### 4.2 agent 端點

認證是一份共用的 bearer token（E14-D001）。API 的設定存 token 本身。STS agent 只存它的 SHA-256。token 至少 32 bytes 的隨機值，產生方式比照 E10-D055。

位址是 `AgentRecord.log_addr`（E14-D002）。STS agent 當選時寫入，與 E10-D092 的 `host`、`work_dir` 同一個寫入點。值取 `-log-advertise`。沒有給這個旗標時是 `<hostname>:7442`。開端點的只有 STS agent，所以 MD、TD、controller 的 `log_addr` 留空（E14-D009、E14-D010）。`state.proto` 已定案，加欄位是相容擴充（PROTOCOL §1）。這一欄先寫在 PROTOCOL §3.6；`contract/proto` 由 #437 對齊。

agent 回給 API 的是 varint 長度前綴的 `mftiko.log.v1.Frame`（E14-D003）。編碼是 protobuf（PROTOCOL §1）。不用 NDJSON，也不用 base64。

上限（E14-D007）：同時串流數 16、讀取速率 16 MiB/s，做成 flag。一則 frame 256 KiB、follow 的心跳 15 秒，寫死，不是 flag。旗標的名字除了 `-log-advertise` 以外，這裡不定。

HTTP 路徑、狀態碼、`Authorization` 與 `Frame` 的欄位寫在 PROTOCOL §11（#437）。聽哪個位址、token 怎麼比、端點失敗時 term 怎麼辦，由 #439 寫。

### 4.3 讀取

讀的是 S4 的檔：`${WORK_DIR}/log/sts/{h}.stdout` 與 `{h}.stderr`，`h = WorkerHash("sts/<session_id>")`。權限 0640，目錄 0750（E10-D015、PROTOCOL §4.5）。輪替規則不在這一張改。

讀取速率與同時讀取數的上限是 E14-D007 的那兩個 flag。cursor 怎麼組成、tail 怎麼跨輪替檔、follow 怎麼握 fd，由 #438 寫，這裡不定。

### 4.4 API

呼叫端仍只對 API 說話（E11-D001）。即時讀取用 SSE，以 `Last-Event-ID` 續接（E10-D005、E11-D030）。

匯出給呼叫端的是 tar.gz。`manifest.json` 放在最後，因為總長與 sha256 要等串流寫完才有（E14-D005）。誰壓縮、tar 裡的檔名與順序，由 #440 寫。

兩個 stream 的合併、`tail` 的單位、狀態碼怎麼轉、匯出由 agent 產出、怎麼找 `log_addr`，是 E14-D011–E14-D015，寫在 PROTOCOL §11 與 `contract/openapi/api/v1.yaml`。`api.v1` 的 `info.version` 是 `2.1.0`。API 怎麼保存那份 token、連線中斷怎麼重試，由 #440 寫。

### 4.5 CLI

命令是 `session logs` 與 `session export-logs`（E14-D006）。`--tail` 照 docker，預設全部，不固定行數。`--latest` 是最新建立的 session，用來取代 `drill.sh` 的 `ls -t`。

`session show` 仍印 log 位置：`<sts_instance> <host>:<完整路徑>`；最後已知值仍註明「最後已知，agent 目前不在線」（E11-D025）。E14-D006 改的是「一期不讀 log 內容」：內容改由這兩個命令經 API 讀。

stdout 與 stderr 怎麼分流、gap 怎麼印、匯出寫到哪個檔名，由 #441 寫。`docs/cli.md` 也在那張改。

### 4.6 console

session 詳細頁現在顯示 `LogLocation` 的主機與完整路徑，`AgentRecord.last_known` 為真時加註（E10-D105）。這一頁要能看 log，是 E14 的目標。分頁、追蹤、下載的版面由 #442 寫。這裡不定最後顯示幾行，也不定要不要搜尋。

### 4.8 錯誤

端點不可用是新的 problem code `logs_unavailable`，HTTP 503（E14-D008）。CLI exit 22。`internal/cli/exit.go` 目前沒有用 22，對照表由後面的票補。

`ErrorCode` 是 enum。這一張不改 OpenAPI。#437 加上這個值時，若 `oasdiff` 判成 ERR，比照 E10-D118 把那一次寫進 `contract/openapi/api-v1-err-ignore.txt`。

哪幾種失敗收成這個 code、要不要重試，由 #440 寫。`logs_unavailable` 的告警與 runbook 由 #443 寫。

## 5. 時間戳

一期不做 `-t`、`--since`、`--until`（E14-D004）。S4 不改。shim 不在每一行加時間。

要做的話改 S4，只對新的 incarnation 生效，那是選做的 #444。這一期不排它。

## 6. 範圍

只有 STS session worker 的 stdout 與 stderr（E14-D009）。

不在 E14：

- `{h}.shim.log`
- 平台元件自己的 log
- MD 與 TD worker 的 log
- spec 刪除之後的封存

運維文件（port、防火牆、token 的產生與輪換、runbook）由 #443 寫。`docs/operations.md` 在那張之前仍是上機器讀檔。

## 7. port 7442

E14-D002 的預設 port 是 7442。寫這份決定之前先搜過設定、文件、程式與 compose：`.go`、`.yml`、`.yaml`、`.md`、`.json`、`.toml`、`.conf`、`.service`、`.env`、`.sh`、`.proto` 裡沒有獨立的 `7442`。含這四個數字的地方只在交易所 testdata 的時間戳，以及 `exchange/internal/codec/sign_test.go` 的一組測試向量，都不是監聽 port。7442 與現有元件沒有衝突。
