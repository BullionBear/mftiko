# PROTOCOL — 協議與契約

> **狀態：草案 v0.42（2026-10-09）。** 依據 [DECISIONS.md](DECISIONS.md)。v0.3 把控制面改到 etcd（D004），依賴改成 agent 層級的 lease（D901），shim 與 worker 之間加入生命週期動詞（D107），資料面拿掉給策略看的可用性廣播（D801）。v0.4 依 D005 改成每個 region 一個 etcd cluster：路由函數、每個 cluster 各自的 writer key、版本號不依賴 etcd revision；並依 D902 加入策略 artifact 的格式（§8）。v0.5 依 D601 加入歷史補正：`AccountSettings` 加 interval 與手動觸發，TD agent 訂閱 `td.evt` 落庫，`Fill.fill_id` 規定為交易所的 trade id。v0.6 依 D402：md.proto 加 `OrderBookDelta`，新增 `read.proto`（`md.view`、`md.fetch`），tape 不再用 Redis（拿掉 `tape_url` 與 `StandingSubscription.record`）。v0.7 依 D106：行情的 subject 一律由 MD agent 給，`SelectorBinding` 加 `subject_prefix`。v0.8 依 D903 拿掉 `stsctl.SubscribeRequest`：一期策略完全宣告式。v0.9 依 D904 加入 session volume 與資料 artifact：`SessionSpec.inputs`、`Limits.volume_mb`、`SessionStatus.state_out`、兩個環境變數、§8.4。v0.10 依 D803：`md.f` 加 envelope（`seq`、`conn_epoch`、`md_ts_ns` 從 `Meta` 移過去），拿掉 `Delivery`，加 `SessionSpec.recv_window_ms`、`Limits.hook_timeout_s`、`QueueStat.max_age_ms`。v0.11 依 D602：加 `algo_id`（由 spec 在每個帳號上宣告，可以多個，TD agent 以 lease 檢查衝突），`client_order_id` 的 session 欄位換成 algo，`CancelSession` 換成策略自己發的 `BatchCancel`，STS agent 不再有 `td.order` 的權限。v0.12：`client_order_id` 改成 `ver(4) | algo(10) | ts_ms(40) | seq(10)`。v0.13 依 D403：MD 連線的身分改成 `(venue, route, n)`，endpoint 變成 `mdctl.InitPayload.url` 這個參數，atom id 拿掉 endpoint。v0.14 依 D603：`td.evt` 的 `AccountReset` 換成帶快照的 `Recon`，TD 降級時拒新單、放行撤單。v0.15 依 D905：只有一層 linger，連線在最後一個 atom 退訂時就停；lease 的 TTL 統一 60 秒。v0.16 依 D404 拿掉常駐訂閱（`StandingSubscription` 與 `/md/{inst}/standing/`）：MD 的 owner 只有 session 的 lease 項目。v0.17 依 D601：`tdctl.Report` 加 `first_submit_ns`，backfill 的範圍從下單決定。v0.18 依 D906：session worker 的 NATS 憑證每個 session 一份（`stsctl.InitPayload.nats`），權限見 §7。v0.19 依 D802：一期只做 Go SDK，拿掉 Python 綁 Go 的設計；新增 §9，寫明其他語言的 SDK 要遵守的契約，以及 STS agent 以 `Beat` 逾時判定 `HUNG` 的保險。v0.20 依 D202：UniversalTicker 是佔位，經 SYM 解析之後才決定訂閱什麼；atom id 改成 `<venue>:<route>:<channel>`；加入 topic 表與 SYM 的契約（§7.3，`mftiko.sym.v1`）。v0.21 依 D301：帳號種類和 venue 分成兩個 enum，`state.v1.Account.venue`、`tdctl.InitPayload.venue` 改為 `kind`；BinancePM 是帳號種類，不是 venue。v0.22 依 D106：incarnation 存在不綁 lease 的 `/{plane}/{inst}/incarnation/{h}`，spawn 前以 txn 取號（§3.2 W6）。v0.23：W4 的 watch 改從 list 的 revision（含）開始，不再加 1。v0.24 依 D907–D908、D006–D007：控制平面分成 API server 與 controller；跨平面的依賴改由 controller 以宣告式的需求表達，不綁 TTL（D901、D905），lease 項目改名需求項目，key 拿掉 `{sts_inst}`；新增 STS 的指派 `/sts/{inst}/demand/{session_id}`，Gate 與綁定的整理移到 controller；ack 由 controller watch；SYM 改成啟動時整表載入、跟著變更重載，使用者是 STS、MD、TD；三種 worker 都從 artifact 起（§8）；契約分成草案與定案（§1）；session 的宣告抽成 `mftiko.session.v1`，`stsctl.v1` 不再 import `state.v1`，改用自己的 `BindingState`、`Universe`；`Side`、`OrderType`、`TimeInForce` 移到 `common.v1`，`td.v1` 不再 import `md.v1`。v0.25 依 D004：etcd lease 只有選主 session 一種；`AgentRecord` 綁在它上面，agent 在不在就看它；ack、`WorkerStatus` 不綁 lease，由 agent 自己維護（§3.2 W7）。v0.26：W1 由 etcd 的 auth 與 RBAC 強制，角色表由 instance 清單產生（§3.2 W1）。v0.27：shim 的啟動參數 `Launch` 由 agent 經 shim 的 stdin 送入，欄位由 artifact 的 manifest 換算而來（§4.6、§8.3）。v0.28：C3 的 server（E1-09）：`Launch` 加 `work_dir`，socket 的權限、只收同一個 uid 的連線、殘留與重複的處理（§4.1）；attach 的檢查順序、shim 記住接受過的最大 epoch、錯誤碼表、major 沒有交集時只能 `Signal` 與跨 major 不變的欄位（§4.2）；worker 被 reap 之後 `Signal` 回 `exited`（§4.3）；新增 §10 P12。v0.29：生命週期指令（E1-11）：shim 記下的階段 `Phase` 放進 `Attached`，`Init`、`Start` 只在對應的階段接受，`Stop` 的計時與重複的 `Stop`、`Release` 的條件、shim 自己收到 SIGTERM 時的 grace，錯誤碼加 `worker_busy`（§4.2、§4.3）；`cred` 的 uid 和 shim 不同而 shim 沒有 `CAP_KILL` 時不 spawn（§4.6）。v0.30：worker 的輸出（E1-14）：`Launch.output`，檔案放在 `${WORK_DIR}/log/{plane}/{h}.stdout`、`{h}.stderr`，依大小輪替，寫不及時丟棄並在檔案裡註明，不讓 worker 阻塞（§4.5 S4、§4.6）。v0.31：事件緩衝與補送（E1-12）：只有進環狀緩衝的事件有 seq；`Ack` 釋放已確認的事件，環的 1024 則只算未確認的；`gap` 只標在 agent 要的事件被溢出丟掉時；attach 之後先送最新的 `Report` 再補送；控制者換人或離開時，shim 對 worker 送 `Controller`；agent 送完指令就關閉連線時，已送出的指令照樣執行（§4.1、§4.2、§4.4）。v0.32：結束檔與 `Exited`（E1-13）：新增 `ExitRecord`；`ExitInfo` 的合成規則（code、signal 一律依 wait status，outcome、reason 有 `Exiting` 就依它，沒有就依 exit code）；reap 之後讀完 fd 3 才記錄，記錄完才是 `EXITED`、才接受 `Release`；有前一個 incarnation 沒收走的結束檔時 shim 不啟動（§4.3、§4.5 S3、§4.6、§5.3）。v0.33 依 D108：agent 以快照恢復，事件只是通知；`Attached` 加 `exiting`、`exit`，且 `next_seq` 是快照的時間點；`Ack` 不是提交（§4.4）。v0.34 依 D203：幣與數量的單位。qty 的單位是 `contract_value_ccy`（張數 × `contract_size`）：linear 與期權是 base，inverse 是 quote，不是張數；`price_ccy` 是 price、`price_tick` 與權利金的幣，現貨與期貨（含 inverse）是 quote，幣本位期權是 base，U 本位期權是 quote；`settlement_asset` 是損益結算幣，不是保證金幣，三個欄位由 adapter 從原生欄位填，下游不推導。別名只在 SYM 收（一律大寫，例如 XBT→BTC），先正規化再判定 quanto 再推 ticker；strike 先依 `spell_strike`（`6400.0` 與 `6400` 是同一個 ticker）。`USD`、`USDC`、`USDT` 不合併，報價 USD 而結算 USDC 是 quanto，一期不入庫、需求 `REJECTED`。狀態 enum 暫定（`PRELISTING`、`TRADABLE`、`HALTED`、`SETTLING`、`EXPIRED`、`DELISTED`、`UNKNOWN`），契約仍是 `bool active`，交易路徑只接受 `TRADABLE`。SYM 的 listing 預定分兩張表：活躍表是狀態還會變的（`PRELISTING`、`TRADABLE`、`HALTED`、`SETTLING`、`UNKNOWN`），Resolver 與熱路徑只讀它；終結表是 `EXPIRED`、`DELISTED`，狀態不再變，不再刷新，給歷史查詢，可以歸檔；搬過去時 UniversalTicker 字串不變。這一版不拆表。UniversalTicker 只給人寫 spec；系統內部用 UID，是兩張表的主鍵，跨表唯一，搬表時不變，下架再上市拿新的 UID。`SymbolInfo` 加 `uid`（寬度暫定，契約暫用 `uint64`，0 不分配）與 `GetByUidRequest`。寬度、分配方式、重啟是否沿用還沒定；建議方向是一條 Postgres sequence（例如 `symbol_uid_seq`）餵兩張表的 `uid` 主鍵，`symbol_filter` 以 uid 為鍵，終結時整列連 uid 搬走（§10 P13），這一版不實作。NATS subject 用人讀的名字，不用 UID；確切拼法還沒定（§10 P14），這一版仍是 `md.f.{inst}.{topic}.{ticker}`。策略 hook 看 ticker 還是 UID 還沒定（§10 P15）。`td.order` 等仍是 ticker。`td.proto` 的數量欄位註上單位（§7.2、§7.3）。v0.35 依 D109：需求過舊（`LastContact` 超過門檻，預設 15 秒）時，agent 不依需求停 worker、替換、推 live config，也不寫 `WorkerStatus`；重啟結束的 worker 與送 `Start` 由各平面的 Policy 決定（§3.2 W8）。v0.36 改寫 §10 P13：UID 由 SYM 分配，是隨機、唯一、永不重用的整數，不是單調序號，也不是 Postgres sequence；跟列一起持久化，重啟與重載不變，下架再上市拿新的，碰撞就重試。契約型別是 `uint64`，0 非法。寬度與範圍仍開放，Postgres BIGINT 有號，建議 `1..2^63-1`。`symbol_filter` 也分活躍表與終結表，終結時跟列一起搬。SYM `List` 以 offset＋limit 分頁，跨頁不一致可以接受。`Universe.current` 是 selector，尚未定，不在 D203；selector 沒有 UID。P14、P15 仍開放。v0.37：agent 起 shim 的方式（E1-15）：開新 session、shim 的 log 接到 `${WORK_DIR}/log/{plane}/{h}.shim.log`、`Launch.env` 最後由 agent 補上身分變數（§4.6）。v0.38：同一個 worker key 至多一個存活的 incarnation（E1-16）：上一個沒有結束紀錄（shim 不見了）時，agent 以 pid 加 start_time 確認它已經不在，才起下一個；pid 被重用或成了 zombie 都算不在（§4.6）。v0.39：Beat 逾時（E1-18）：agent 記下每個 worker 最後一次 `Beat`，超過平面 Policy 的逾時就通知 Policy，每段沉默一次，由 Policy 決定要不要停掉它（§4.4）。v0.40：啟動時收養（E1-17）：新的 term 在第一次 reconcile 之前掃 `run/{plane}/` 的 socket 與結束檔，逐一 attach；attach 不成的保留 `WorkerStatus`、不起新的 incarnation、定期重試；shim 已不在的結束檔當成已結束的 worker；收養後依 etcd 把 live config 再推一次，暫行到 E1-22 以 worker 回報的 digest 比對（D111，§4.6）。v0.41 定案 symbol UID 的寬度與範圍（§10 P13、D203）：契約型別 `uint32`，範圍 `1..4294967295`（`1..2^32-1`），0 非法。Postgres 欄位是 `bigint`，`CHECK (uid BETWEEN 1 AND 4294967295)`。v0.42 依 E2-03：`migrations/0002_symbol` 改成活躍表 `symbol_active` 與終結表 `symbol_terminal`，主鍵是 uid；`symbol_filter_active` 與 `symbol_filter_terminal` 跟列一起搬。活躍表以 generated column 拆出 venue、category，`UNIQUE (venue, category, exch_ticker)` 與 `universal_ticker` 唯一只在活躍表。加 `contract_value_ccy`、`price_ccy`，結算欄位仍叫 `settle`。strike 入庫前去掉尾端的零。`symbol_revision` 記下每個 `(venue, category)` 的 revision，通知的訊息仍見 P7。契約的 `active` 仍是 bool。另加 symbol_uid（主鍵 uid，列不刪）：兩張 listing 與兩張 filter 的 uid 參照它，分配先插入 symbol_uid，撞 unique 就重抽，歸檔或刪列之後也不會再抽到同一個 uid。symbol_reject_shared_uid 仍在，因為登記表擋不住兩張 listing 同時持有同一個 uid；搬表在同一個交易裡先刪活躍列再插入終結列。uid 是 BIGINT，CHECK (uid BETWEEN 1 AND 4294967295)。契約型別見 P13。還沒定的寫在 [§10](#10-待定)。
>
> proto 區塊開頭的 `// file:` 是它在 `contract/proto/` 下的路徑。全部區塊以 `protoc` 編譯驗證過，`contract/proto/` 的檔案與它們逐字相同（第一行除外）。

## 0. 總覽

```
  API server ──寫宣告──▶ etcd（控制面，每個 region 一個）◀──watch／寫──▶ controller
                              ▲ watch 需求與指派；寫 ack、狀態
               ┌──────────────┼──────────────┐
           STS agent       MD agent       TD agent
               │ C3            │ C3            │ C3
             shim            shim            shim
               │ C4            │ C4            │ C4
        session worker ◀── C6（NATS）──▶ conn worker、account worker
```

控制流只走垂直方向：API server 寫宣告，controller 推導需求與指派，agent 照著起停 worker（DECISIONS D901、D907）。agent 之間沒有控制流，平面之間只有 C6 的資料流。

| 契約 | 兩端 | 傳輸 | 內容 | 擁有者 |
|---|---|---|---|---|
| E | API server、controller、agent ⇄ etcd | etcd key ＋ watch | 宣告、需求項目、STS 的指派、ack／nack、agent 狀態（`mftiko.state.v1`；session 的宣告在 `mftiko.session.v1`） | 依前綴，見 §3.1 |
| C3 | agent ⇄ shim | unix socket | attach、生命週期、事件、信號（`mftiko.shim.v1`） | 平台 |
| C4 | shim ⇄ worker | socketpair（worker 的 fd 3） | 生命週期、心跳、回報、結束方式（`mftiko.worker.v1`） | 平台 |
| C5 | agent ⇄ worker（包在 C3、C4 裡） | — | 各平面的 payload（`stsctl`、`mdctl`、`tdctl`） | 各平面 |
| C6 | worker ⇄ worker、服務 | NATS | 行情、行情讀取、下單、帳號事件、商品資料（`mftiko.md.v1`、`mftiko.td.v1`、`mftiko.sym.v1`） | `md.v1` MD、`td.v1` TD、`sym.v1` SYM |

每份契約目前的狀態與定案的時點見 §1。

## 1. 版本與相容

shim 與 worker 會比 agent 活得久（D103），所以**版本偏差是常態**，沒有全域的 `pv`。

- 每份契約是一個 protobuf package：`mftiko.<contract>.v<major>`。
- **同一個 major 內只做相容擴充**：加欄位、加 oneof 分支、加 enum 值。讀取端必須容忍未知欄位與未知 enum 值。CI 對每個 release 跑 `buf breaking`。
- 不相容的改動開新 major，仍有對端使用舊 major 的期間兩者並存。

| 契約 | 協商 | 必須支援的範圍 |
|---|---|---|
| E | 不協商。etcd 的值只做相容擴充；不相容時換 key 根（`/mftiko/v2/`） | — |
| C3 | agent 在 `Attach` 列出支援的 shim major；沒有交集時只剩 `Signal`（§4.2） | 仍在跑的所有 shim major |
| C4 | worker 在 `Hello` 列出支援的 C5 契約 | 三種 worker 都從 artifact 起（§8），版本可能和 agent 不同：部署時以 manifest 的 `contracts` 檢查，執行時以 `Hello` 協商。偏差最久的是 STS：策略以某一版 SDK 編譯，會跑很久 |
| C5 | agent 依 `Hello` 選擇；都不支援時只做信號級的管理 | 目前與前一個 major |
| C6 | 不協商。只做相容擴充；不相容時換 subject 根 | — |

編碼一律是 protobuf，理由是 `buf breaking` 能機械地檢查相容性。唯一的例外是 `md.f`：protobuf 前面加一段固定長度的二進位 envelope，給 SDK 的 prehook 不解碼就能讀（§7.1、DECISIONS D803）。

**草案與定案（DECISIONS D007）。** 上面的相容規則只約束定案的契約。草案可以自由改：`buf.yaml` 的 `breaking.ignore` 列出草案的檔案，CI 不擋；定案的 EPIC 把自己的檔案移出清單，寫進完成標準。目前全部是草案。

| 契約 | 檔案 | 兩端 | 狀態 | 定案 |
|---|---|---|---|---|
| E | `state.proto` | API server、controller、三種 agent | 草案 | agent 紀錄在 E1；需求、指派、ack 在 E9 |
| E、C5 | `session.proto` | API server、controller、STS agent、session worker | 草案 | E9 |
| C3 | `shim.proto` | agent ⇄ shim | 草案 | E1 |
| C4 | `worker.proto`、`common.proto` | shim ⇄ worker | 草案 | E1 |
| C5 | `mdctl.proto` | MD agent ⇄ conn worker | 草案 | E4 |
| C5 | `tdctl.proto` | TD agent ⇄ account worker | 草案 | E6 |
| C5 | `stsctl.proto` | STS agent ⇄ session worker | 草案 | E8、E9 |
| C6 | `md.proto`、`read.proto`、envelope（§7.1） | conn worker、md.fetch ⇄ session worker | 草案 | E4 |
| C6 | `td.proto`、`client_order_id`（§2） | account worker ⇄ session worker、TD agent | 草案 | E6 |
| C6 | `sym.proto` | SYM ⇄ STS、MD、TD | 草案 | E2 |
| §8 | artifact manifest | `mftiko-pack`、CI ⇄ 三種 agent | 草案 | E9 |
| — | exchange 的 adaptor 介面（DECISIONS D006） | exchange ⇄ MD、TD、SYM | 草案 | E3 |

## 2. 識別碼與慣例

| 名稱 | 形式 | 由誰分配 |
|---|---|---|
| instance 名稱 | `^[a-z][a-z0-9-]{0,63}$`，慣例是 `{plane}-{region}-{n}`（`md-tw-1`），拆 region 後仍然全域唯一 | 部署設定 |
| `session_id` | 6 個小寫 hex，對所有歷史 session 唯一 | API |
| `api_id` | `int64`，大於 0 | API |
| worker key | `sts/<session_id>`、`td/<api_id>`、`md/<venue>/<route>/<n>`（DECISIONS D403） | 各 agent |
| incarnation | `uint32`，每個 worker key 從 1 起各自遞增；只保證遞增，可以跳號。存在 `/{plane}/{inst}/incarnation/{h}`，spawn 前取號（§3.2 W6） | 各 agent |
| agent epoch | `int64`，持久化在 `/{plane}/{inst}/epoch`，agent 每次當選加 1（§3.2）。不用 etcd revision：不同 cluster 的 revision 不能比較，cluster 重建時 revision 也會從頭開始 | agent |
| `Version` | `(epoch, seq)`，字典序比較。agent 推給 worker 的指令都帶它；`seq` 在同一個 agent 進程內遞增 | agent |
| generation | session spec key 的 `mod_revision`（§3.2），只在該 key 所在的 cluster 內比較 | etcd |
| UniversalTicker | `<Venue>_<Category>_<SYMBOL>`，照抄 mftik（D201）。給人寫設定與 spec 的字串（D203）。系統內部不拿它當商品身分，載入 spec 時解成 UID。Venue 是 CamelCase（`BinanceUM`、`Okx`）；Category 是 `Spot`、`Perp`、`Inverse`、`Future`、`Option`；SYMBOL 是 base＋quote 不加分隔，到期合約加 `-YYMMDD`，期權再加 `-STRIKE-C`／`-P`。strike 先依 `spell_strike` 正規化再拼（D203）：`6400.0` 與 `6400` 是同一個 ticker，小數點寫成 `D`（`6D4`）。不含 `_` 以外的分隔、不含 `.` | SYM（由 listing 推得） |
| symbol UID | SYM 分配的隨機整數，唯一且永不重用，不是單調序號（D203）。活躍表與終結表的主鍵，兩張表合在一起唯一，跟列一起持久化：重啟與重載都不改它。搬到終結表時不變；同一個 ticker 下架再上市是新的 UID；碰撞時 SYM 重試。契約型別 `uint32`，範圍 `1..4294967295`（`1..2^32-1`），0 非法。Postgres 欄位是 `bigint`，`CHECK (uid BETWEEN 1 AND 4294967295)`（§10 P13） | SYM |
| venue | 市場，UniversalTicker 的第一段，CamelCase：`Binance`、`BinanceUM`、`BinanceCM`、`BinanceEO`、`Okx`、`Bybit`。一條連線的公開行情、一組商品（D403） | 程式（`internal/exchange`） |
| 帳號種類 | api_id 是什麼：收哪種憑證、可以交易哪些 venue 的商品：`Binance`、`BinanceUM`、`BinanceCM`、`BinanceEO`、`BinancePM`、`Okx`、`Bybit`。和 venue 是兩個獨立的 enum，名稱可能相同，不可互查。`BinancePM` 只是帳號種類，交易 `Binance`、`BinanceUM`、`BinanceCM` 的商品，依 ticker 的 venue 路由，沒有 `BinancePM_*` 這種 ticker（D301） | 程式（`internal/exchange`） |
| exch_ticker | 交易所自己的寫法，只出現在 adapter 與 SYM。反查要帶 venue 與 category：一個帳號種類可能交易多個 venue（BinancePM），統一帳戶的 venue 上同一個寫法可能是兩個商品（Bybit 現貨與 linear 的 `BTCUSDT`） | 交易所 |
| topic | `bestquote`、`trade`、`aggtrade`、`orderbook`、`orderbook_delta`、`ticker`、`liquidation`、`funding_rate`、`open_interest`、`greeks`、`mark_price`、`kline_<interval>`（interval 是 `1m`、`4h`、`1mo` 這類正規寫法）。前面幾個沿用 mftik 的名稱（D202） | — |
| feed key | `<topic>.<UniversalTicker>`。spec 裡寫這個字串；載入時 ticker 才解成 UID（D203）。NATS subject 用人讀的名字，不用 UID；確切拼法還沒定（§10 P14），這一版的 `md.f` 仍是 `md.f.{inst}.{topic}.{ticker}` | — |
| atom id | `<venue>:<route>:<channel>`，只出現在 MD 內部與 `mdctl`。channel 是交易所的訂閱參數，逐字照抄，可能含 `:` 與 `.`，所以只切前兩個 `:`。route 是身分的一部分：同一個 channel 字串可能出現在不同 route（Bybit 現貨與 linear 的 `publicTrade.BTCUSDT`）（D202） | MD adapter（由解析後的商品推得） |
| route | 同一個 venue 裡 atom 所屬的 WS 路徑，一個小寫單字，例如 OKX 的 `public`、`business`，Bybit 的 `spot`、`linear`；只有一條路徑的 venue 是 `default`（D403） | MD adapter |
| `algo_id` | `uint32`，1–1023（10 bits），0 保留。訂單的命名空間，跨 session 不變；一個 session 可以有多個。只需在同一個帳號內唯一；同一個帳號上同時只屬於一個非 terminal 的 session，由 TD agent 依需求項目檢查（DECISIONS D602） | spec（`AccountDecl.algo_ids`） |
| `client_order_id` | uint64 `ver(4) \| algo(10) \| ts_ms(40) \| seq(10)`，以十進位字串傳。`ts_ms` 自 2026-01-01 UTC 起算；`seq` 每個進程每毫秒 1024 個；`ver` 只用 1–7，最高位為 0（D602） | session worker |

- 十進位數一律是字串，不用浮點。時間一律是 `int64` unix 奈秒，欄位名以 `_ns` 結尾。
- worker 的本機檔名與 etcd key 用 `h = hex(sha256(worker key))[:16]`。

```proto
// file: mftiko/common/v1/common.proto
syntax = "proto3";

package mftiko.common.v1;

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1;commonv1";

message WorkerRef {
  string key = 1;
  uint32 incarnation = 2;
}

// agent 推給 worker 的指令版本，以 (epoch, seq) 的字典序比較。
// epoch 是持久化的計數器，不是 etcd revision，所以換 cluster 之後仍然單調。
message Version {
  int64 epoch = 1;
  uint64 seq = 2;
}

message Condition {
  string type = 1;
  bool status = 2;
  string reason = 3;
  string message = 4;
  int64 at_ns = 5;
}

message Error {
  string code = 1;
  string message = 2;
  bool retryable = 3;
  map<string, string> details = 4;
}

// worker 宣告的結束方式（D107）。
enum Outcome {
  OUTCOME_UNSPECIFIED = 0; // 沒有宣告：被殺，或在 Hello 之前就死了
  OUTCOME_DONE = 1;        // 正常結束：Stop，或策略呼叫 Exit
  OUTCOME_INIT_FAILED = 2; // Init 或 Start 失敗、逾時
  OUTCOME_FAILED = 3;      // 主動 fail
  OUTCOME_CRASHED = 4;     // hook panic 或回傳 error；OnStop 已跑
  OUTCOME_HUNG = 5;        // hook 卡太久，watchdog 結束；OnStop 沒跑
}

message ExitInfo {
  int32 code = 1;   // 被 signal 結束時為 -1
  int32 signal = 2; // 0 表示正常 exit
  int64 at_ns = 3;
  Outcome outcome = 4;
  string reason = 5;
  bool on_stop_completed = 6;
}

// 交易共用的 enum：行情的成交與強平、TD 的訂單、exchange 的 adaptor、SDK 都用這一份（DECISIONS D006）。
enum Side {
  SIDE_UNSPECIFIED = 0;
  SIDE_BUY = 1;
  SIDE_SELL = 2;
}

enum OrderType {
  ORDER_TYPE_UNSPECIFIED = 0;
  ORDER_TYPE_LIMIT = 1;
  ORDER_TYPE_MARKET = 2;
}

enum TimeInForce {
  TIME_IN_FORCE_UNSPECIFIED = 0;
  TIME_IN_FORCE_GTC = 1;
  TIME_IN_FORCE_IOC = 2;
  TIME_IN_FORCE_FOK = 3;
  TIME_IN_FORCE_POST_ONLY = 4;
}
```

## 3. 控制面：etcd

**cluster 與路由（D005）。** 每個 region 一個 etcd cluster。所有存取經過路由函數 `clusterFor(instance)`，依 key 歸屬的 instance 所在的 region 決定連哪個 cluster。歸屬的規則是「會依據這個 key 行動的那個 agent」：

| key | 歸屬 |
|---|---|
| session spec、STS 的指派、status | STS 所在的 region |
| 需求項目、ack／nack | provider（MD／TD）所在的 region。TW 的 session 要 JP 的行情，controller 就寫進 JP 的 cluster |
| controller 的 election、epoch、agent | controller 的 home region（部署設定，DECISIONS D907） |
| controller 的 writer | 每一個 controller 會寫入的 cluster 各一份 |
| 其他（election、epoch、writer、agent、worker、incarnation、selector、placement、帳號） | 該 instance 所在的 region |

只有 controller 和 API server 會連多個 cluster；三種 agent 都只連自己 region 的。下表的 key 路徑在每個 cluster 裡都一樣，差別只在存在哪個 cluster。

### 3.1 key 結構

根是 `/mftiko/v1`。表中「綁 lease」指 key 綁在哪個 etcd lease 上：該 lease 過期，key 由 etcd 自動刪除。只有一種 lease，就是 W2 參選用的選主 session（W7）。

| key | 值 | 寫入者 | watch 者 | 綁 lease |
|---|---|---|---|---|
| `/sts/{inst}/spec/{session_id}` | `SessionSpec` | API server | controller、STS agent | — |
| `/sts/{inst}/demand/{session_id}` | `SessionDemand`（STS 的指派） | controller | STS agent | — |
| `/sts/{inst}/status/{session_id}` | `SessionStatus` | STS agent | controller、API server | — |
| `/td/{inst}/account/{api_id}` | `Account` | API server | TD agent | — |
| `/md/{inst}/demand/{session_id}/{entry}` | `DemandEntry` | controller | MD agent | — |
| `/td/{inst}/demand/{session_id}/{api_id}` | `DemandEntry` | controller | TD agent | — |
| `/md/{inst}/ack/{session_id}/{entry}` | `EntryStatus` | MD agent | controller | — |
| `/td/{inst}/ack/{session_id}/{api_id}` | `EntryStatus` | TD agent | controller | — |
| `/{plane}/{inst}/election` | etcd election | agent | — | 選主 session |
| `/{plane}/{inst}/epoch` | 20 位補零的十進位 epoch | agent | — | — |
| `/{plane}/{inst}/writer` | 20 位補零的十進位 epoch；在這個 agent 會寫入的**每一個** cluster 各有一份 | agent | — | — |
| `/{plane}/{inst}/agent` | `AgentRecord` | agent | controller、API server | 選主 session |
| `/{plane}/{inst}/worker/{h}` | `WorkerStatus` | agent | controller、API server | — |
| `/{plane}/{inst}/incarnation/{h}` | 十進位，這個 worker key 最近取到的 incarnation（§3.2 W6） | agent | — | — |
| `/md/{inst}/selector/{spec_hash}` | `SelectorState` | MD agent | — | — |
| `/md/{inst}/placement/{atom_hash}` | `Placement` | MD agent | — | — |
| `/ctl/controller/election` | etcd election | controller | — | 選主 session |
| `/ctl/controller/epoch` | 20 位補零的十進位 epoch | controller | — | — |
| `/ctl/controller/writer` | 同 agent 的 writer；在 controller 會寫入的每一個 cluster 各有一份 | controller | — | — |
| `/ctl/controller/agent` | `AgentRecord`（`plane` 是 `ctl`） | controller | API server | 選主 session |

`{plane}` 是 `sts`、`md`、`td`。controller 的 instance 名稱固定是 `controller`：每個 controller 進程都用它，即使兩個進程對 home region 的設定不一致，也會在每個 cluster 搶同一把 writer key，互相 fence（W2）。MD 的 `{entry}` 是 `feed/{feed key}` 或 `select/{selector 名稱}`。需求與 ack 的 key 不再帶 `{sts_inst}`：寫入者是 controller，`session_id` 對所有歷史唯一，足以當 owner 的身分。

### 3.2 寫入規則

- **W1 單一寫入者。** 每個前綴只有表中的那一個寫入者。API server 不參選，它的寫入怎麼滿足 W1 見 §10。
  - etcd 開 auth 與 RBAC，由 etcd 自己強制這條：每個進程在它連的每個 cluster 有自己的使用者，agent 用 instance 名稱，controller 用 `controller`，API server 用 `api`。角色只給讀寫上表寫入者欄裡屬於它的 key：agent 只在自己 region 的 cluster 有使用者，讀自己 instance 的前綴、寫自己的 key，不寫任何 `demand/`；controller 讀全部，寫 `/ctl/controller/` 與該 region 每個 agent instance 的 `demand/`；API server 讀全部，寫 `spec/`、`account/`，並依 W5 在保留期之後刪除 `status/`（W1 唯一的例外）。
  - 角色表由部署的 instance 清單產生（`mftiko-etcd-roles`，實作是 `internal/platform/state` 的 `AuthTables`），帳密的存放與輪換、套用角色表在部署時做。
  - token 是 JWT。過期，或使用者與角色變更之後（auth revision 變了），client 自動重新認證，進行中的 watch 與 lease keepalive 不受影響。
- **W2 寫入以 writer key 為條件。** agent 與 controller 都照這個流程；controller 的 key 在 `/ctl/controller/` 底下，在 home region 參選。
  1. agent 在自己 region 的 cluster 參選 `/{plane}/{inst}/election`（etcd `concurrency.Election`）。
  2. 當選後以 txn 把 `/{plane}/{inst}/epoch` 加 1，得到 epoch E。這也是 C3 `Attach` 帶的 epoch。
  3. 在每一個要寫入的 cluster，以 txn 把 `/{plane}/{inst}/writer` 改成 E，條件是原值小於 E 或不存在（值是 20 位補零的十進位，位元組比較就等於數值比較），並記下這次寫入的 `mod_revision` R。
  4. 之後在該 cluster 的每一次寫入都是 txn，條件是 writer key 的 `mod_revision` 仍然等於 R。被取代的舊 agent 寫不進去。

  fencing 因此只依賴「被寫入的那個 cluster」裡的 key，跨 cluster 寫入時不需要跨 cluster 的 txn。
- **W3 generation 就是 revision。** session spec 的 generation 是該 key 的 `mod_revision`。`SessionStatus.observed_generation` 是 STS agent 處理過的那個 `mod_revision`。
- **W4 list ＋ watch。** 讀取端先 list 前綴，再從該 revision（含）開始 watch，revision 不大於副本的事件直接丟掉。效果等同從加 1 開始，但 watch 的起點不會超過 cluster 目前的 revision；起點超過的話，etcd 會停發整條連線的 progress notification，cluster 安靜時就一直停著。收到 compacted 錯誤就重新 list。處理邏輯是 level-triggered 的。實作是 `internal/platform/state` 的 `Mirror`。
- **W5 terminal 是吸收態。** session 進入 `DONE` 或 `FAILED` 之後，spec 再怎麼改都沒有效果。API 可以在保留期之後刪掉 spec 和 status。
- **W6 incarnation 先取號再 spawn（DECISIONS D106）。**
  1. agent 當選後 list `/{plane}/{inst}/incarnation/`，把每個 worker key 目前的號碼記在記憶體裡。這個前綴只有它寫，不需要 watch。
  2. spawn 前以一個 txn 取號。條件有兩個：writer key 的 `mod_revision` 仍是 R（W2）；incarnation key 的值仍是記憶體裡的 n，第一次 spawn 則是 `create_revision == 0`。成立就寫入 n＋1。
  3. txn 成功才 spawn，以 n＋1 當這個進程的 incarnation。writer 的條件不成立表示 agent 已被取代，不 spawn。incarnation key 的條件不成立表示記憶體裡的值錯了，重讀後重試。
  4. 取號之後 spawn 失敗，這個號碼就作廢，下次取新的。
  5. key 不刪：worker 停掉之後，同一個 worker key 還會再起來（MD 重新開到同一個連線編號、refcount 又回到 1 的 `api_id`）。刪掉的話它會從 1 重來，下游記得的號碼比它大，新進程的訊息會被當成遲到的舊訊息丟掉。唯一的例外是 STS：`session_id` 對所有歷史唯一，session 進入 terminal 之後同一個 key 不會再起來，所以最後一個 incarnation 確認消失之後，由 STS agent 刪掉。
  6. `SessionStatus.incarnation`、`WorkerStatus.worker.incarnation` 只是顯示，值從這裡來。
- **W7 agent 在不在，看 `AgentRecord`（DECISIONS D004）。** agent 與 controller 都只有一個 etcd lease，就是 W2 參選用的選主 session。
  1. ack 與 `WorkerStatus` 不綁 lease，只有它的寫入者會刪它，所以「不存在」只有一種意思。
     - `WorkerStatus` 在 worker 確定不在之後才刪：`Release` 完成，或 shim 的 socket 與進程都已不存在。attach 失敗、逾時都不算，key 保留。
     - ack 在需求項目消失之後才刪。
  2. 只在值改變時寫：`WorkerStatus` 在階段改變時才寫，`Beat`、`Report` 不進 etcd。
  3. agent 每次當選，在第一次 reconcile 之前 list 自己的 `worker/` 與 `ack/`，和收養得到的 worker（§4.2）、目前的需求比對：有差異的重寫，多出來的照 1 的條件刪掉。和 controller 的 N5 是同一套做法。
  4. 比對完才寫 `AgentRecord`，綁在選主 session 的 lease 上。只有這一個寫入點，agent 的其他寫入碰不到它（實作是 `internal/platform/state` 的 `Term.PutRecord`）。term 一結束（session 斷了、被取代），session 就不再續約，`AgentRecord` 最晚在一個 TTL 之後消失；agent 正常停下時 revoke 這個 lease，立刻消失。`epoch` 說明是哪一個 term。
  5. 讀的一方把 ack、`WorkerStatus` 和寫入者的 `AgentRecord` 一起看：`AgentRecord` 在，值是新的（已經比對過）；不在，值是最後一次已知的狀態，可能已經過時。controller 對兩種情況的處理相同（§3.5）。instance 永久下線的程序見 §10 P11。
- **W8 需求過舊時保持現狀（DECISIONS D109）。** agent 的 `demand/` 副本的 `LastContact` 超過門檻（預設 15 秒，部署設定）就是過舊。過舊時：
  1. 不依需求停 worker、不因為 spawn spec 變了而替換、不推 live config。恢復聯繫之後的下一輪才做。
  2. 重啟結束的 worker、對已初始化的 worker 送 `Start`，只在該平面的 Policy 允許時做（`KeepWhileStale`）。
  3. 不寫 `WorkerStatus`；恢復之後只寫有改變的（W7.2）。
  4. 收養（§4.2）、Release 已經結束且不再需要的 worker 照常。

### 3.3 需求與 STS 的指派

需求項目與 STS 的指派都由 controller 寫，是宣告式的：不綁 etcd lease，存在的理由是「controller 決定它應該存在」，和 controller 活著與否無關（DECISIONS D901、D907）。

- **N1 由 session spec 推導。** 每個非 terminal 的 session，controller 寫：
  - 一個 `SessionDemand`，在 STS 的 cluster；
  - 每個 feed、每個 selector 一個 MD 的 `DemandEntry`，在 `FeedDecl.md_instance`、`SelectorDecl.md_instance` 的 cluster；
  - 每個帳號一個 TD 的 `DemandEntry`，在 `AccountDecl.td_instance` 的 cluster。

  需求只來自 spec，一期沒有執行中加減訂閱的介面（DECISIONS D903）。
- **N2 STS 的指派一開始就發。** `desired` 是 `RUNNING`，`start` 是 false：STS agent 立刻 spawn 並送 `Init`，`OnInit` 和需求的建立並行（DECISIONS D107）。controller 把每個需求項目目前的 ack 抄進 `bindings`；全部 `READY` 時把 `start` 設成 true，STS agent 才送 `Start`。之後的 ack 變化照樣更新 `bindings`，STS agent 轉成 `stsctl.Bindings` 送給 worker（§6.1）。
- **N3 controller 結束 session 的方式是把 `desired` 改成 `ENDED`，並寫明 `end_reason`：** spec 要求結束、`deps_failed`（收到 nack，§3.5）、`deps_timeout`（`start` 之前超過 `deps_timeout_s`）。STS agent 停掉 worker，寫入 terminal 的 `SessionStatus`。`SessionStatus` 只有 STS agent 寫（W1）：worker 自己結束、重啟次數用完、volume 超量（DECISIONS D904、D803）這些 terminal 也由它判定。
- **N4 刪除的順序。** controller 看到 `SessionStatus` 進入 terminal，且 `WorkerStatus` 確認 session worker 已經消失之後，才刪掉這個 session 的 MD、TD 需求項目，最後刪 `SessionDemand`。平台不替策略撤單（DECISIONS D602）。`SessionDemand` 不見了，STS agent 一律當成 `ENDED`。
- **N5 孤兒靠比對回收。** controller 每次當選、每次全量 resync，都比對「應該存在的」與「實際存在的」。需求的 key 帶 `session_id`，掃前綴就知道哪些項目的 session 不存在或已經 terminal，依 N4 刪掉。
- **N6 controller 停機或輪換時，需求與指派原樣保留。** 新 leader 先 relist 再比對，只改有差異的項目，不先全部刪掉再重建。萬一刪掉又馬上加回，MD、TD 的 linger 會吸收（DECISIONS D905）。
- **N7 provider 只讀。** MD、TD agent 不寫需求項目；STS agent 不寫 `SessionDemand`。

### 3.4 MD 的推導與 refcount

- **owner** 只有 `{session_id}/{entry}`（需求項目）。沒有常駐訂閱；要保持行情或 tape，跑一個 tape keeper 策略（DECISIONS D404）。
- **推導：** feed → 一個 atom（D401）；selector → 成員 → 每個成員、每個 topic 一個 atom。先解析再推導（D202）：MD agent 在已載入的商品表裡解析 feed 的 UniversalTicker（§7.3），找不到、已下架、已到期，或不是可交易（D203：熱路徑只讀活躍表，不是 `TRADABLE` 就拒；`EXPIRED` 與 `DELISTED` 在終結表；quanto 一期不入庫，解析不到）就是 `REJECTED`；再把解析結果交給 adapter，由它決定 route 與 channel。adapter 是純函數，不查 SYM。selector 的成員從已載入的表裡挑。selector 的狀態（universe、epoch、置中點）存在 `/md/{inst}/selector/{spec_hash}`，規格相同的 selector 只算一份。
- **atom 的 refcount 是 owner 集合的大小。**
  - 0→1：加入 desired，放進同一個 `(venue, route)` 下還有空間、n 最小的連線；都滿了才開 n+1（容量由 adapter 宣告，DECISIONS D403）。有黏性，記在 `/md/{inst}/placement/`，推給 conn worker。不整併。
  - 1→0：linger 之後才從 desired 移除。
- **連線在最後一個 atom 退訂時就送 `Stop`**，沒有另外的 linger：最後一個 atom 的 linger 就是連線的 linger（DECISIONS D905）。linger 預設 5 秒，是 MD instance 的部署設定。
- **重啟：** list ＋ watch 讀回全部 owner、selector 狀態與 placement，再從 shim 取回每條連線實際訂了哪些 atom，然後才開始 reconcile。

TD 的對應（DECISIONS D901）：每個 `api_id` 一個 owner 集合，refcount 是它的大小。0→1 時 spawn account worker；1→0 時 linger 之後才送 `Stop`，並排一次歷史補正（D601）。account worker 存在與否就等於「這個帳號可以交易」，沒有另外的交易層開關。

### 3.5 ack／nack

每個需求項目，provider 寫一個對應的 `EntryStatus`，不綁 lease（W7）。key 的狀態對應 ack／nack／沉默：

| key | 意思 | controller 怎麼處理 |
|---|---|---|
| 存在，`READY` | ack：資源正常 | 全部 `READY` 時放行 `start`（§3.3 N2） |
| 存在，`PENDING` | 還在建立 | 繼續等；`start` 之前超過 `deps_timeout_s` 就以 `deps_timeout` 結束 session |
| 存在，`REJECTED` | 這個項目不可能成立（venue 沒有這個 topic、商品不存在、帳號不存在、algo_id 與同帳號其他 session 衝突） | 以 `deps_failed` 結束 session（§3.3 N3） |
| 存在，`DEAD` | nack：provider 透過 shim 明確知道資源死了（worker 結束、連線斷） | 以 `deps_failed` 結束 session。DEAD 的確切時機見 §10 |
| 不存在 | 不知道：provider 還沒處理這個項目 | 維持現狀；`start` 之前照常計 `deps_timeout_s` |

provider 的 `AgentRecord` 不在時，上表的值是它最後寫下的狀態：agent 可能只是在輪換，worker 照跑。controller 照值處理，不把「agent 不在」當成 nack，也不因此改變 session。沒有對應需求項目的 ack（N4 刪掉需求之後，provider 不在、還沒刪的）controller 不理會。

- feed 的 `READY`：它的 atom 在 RUNNING 的 conn worker 上已被交易所 ack。
- selector 的 `READY`：已算出 universe，且每個成員的 atom 都已 ack。
- 帳號的 `READY`：account worker RUNNING、私有連線已建立、recon 完成。

controller watch 這些 key，依上表決定 session 的去留，並把每個項目的 `EntryStatus` 抄進 `SessionDemand.bindings`。STS agent 不讀 ack，只讀自己的 `SessionDemand`：`start` 變成 true 時送 `Start`，`bindings` 變化時以 `stsctl.Bindings` 轉送給 session worker（§6.1）。

### 3.6 值

session 的宣告在 `mftiko.session.v1`，三份契約共用（DECISIONS D007）；其餘 etcd 的值在 `mftiko.state.v1`。

```proto
// file: mftiko/session/v1/session.proto
syntax = "proto3";

package mftiko.session.v1;

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1;sessionv1";

// session 的宣告：使用者經 API server 寫進 etcd（§3.1）；controller 依它推導需求，STS agent 依它起 worker，策略在
// InitPayload 拿到自己宣告的那一份（§6.1）。歸 STS 擁有；state.v1、stsctl.v1 與 API 的契約都 import 它（DECISIONS D007）。
// 商品在這裡寫 UniversalTicker，不寫 UID（D203）。ticker 解成 UID 發生在這份 spec 載入時，不在這份宣告裡。

message SessionSpec {
  string name = 1;
  map<string, string> labels = 2;
  DesiredState desired_state = 3;
  StrategyRef strategy = 4;
  bytes params = 5; // 策略自己的參數，平台不解讀
  repeated FeedDecl feeds = 6;
  repeated SelectorDecl selectors = 7;
  repeated AccountDecl accounts = 8;
  RestartPolicy restart = 9; // 語意見 DECISIONS D803
  uint32 deps_timeout_s = 10;
  uint32 start_timeout_s = 11;
  Limits limits = 12;
  repeated InputDecl inputs = 13; // 資料 artifact（DECISIONS D904）
  // MD 事件在 buffer 裡的最長壽命，以 envelope 的 md_ts_ns 計（D803）。0 = STS 的預設值。
  // 跨 region 的 session 要比 WAN 延遲大。
  uint32 recv_window_ms = 14;
  reserved 15; // 草案 v0.11 的 algo_id：改到 AccountDecl.algo_ids（D602）
  reserved "algo_id";
}

// 唯讀注入的資料 artifact，解到 $MFTIKO_INPUT_DIR/<name>/（§8.4）。
message InputDecl {
  string name = 1;       // 目錄名，^[a-z0-9][a-z0-9_-]{0,63}$，同一個 spec 內唯一
  string digest = 2;     // 資料 artifact 的 OCI manifest digest（"sha256:…"）
  string repository = 3; // 拉取用的 OCI repository
}

enum DesiredState {
  DESIRED_STATE_UNSPECIFIED = 0;
  DESIRED_STATE_RUNNING = 1;
  DESIRED_STATE_ENDED = 2;
}

message StrategyRef {
  string name = 1;
  string code = 2;       // 策略 artifact 的 OCI image digest（"sha256:…"），session 生命週期內不變
  string repository = 3; // 拉取用的 OCI repository
}

// 人寫的規格。feed key 裡是 UniversalTicker，不是 UID（D203）。
// ticker 解成 UID 發生在 spec 載入時（sym.GetRequest），不在這份宣告裡。
message FeedDecl {
  string feed = 1;        // 正規 feed key（<topic>.<UniversalTicker>）。載入邊界才解成 UID
  string md_instance = 2; // API 解析後一律填
  reserved 3;             // 原本的 delivery：背壓改由 recv_window_ms 處理（D803）
  reserved "delivery";
}

message SelectorDecl {
  string name = 1;
  string md_instance = 2;
  string venue = 3;
  string underlying = 4;
  repeated string topics = 5;
  oneof kind {
    OptionChain option_chain = 10;
    RollingFuture rolling_future = 11;
  }
}

message OptionChain {
  string ref_feed = 1;
  uint32 nearest_expiries = 2;
  uint32 min_tte_s = 3;
  uint32 atm_strikes = 4;
  repeated string sides = 5; // "C"、"P"
  uint32 recenter_strikes = 6;
  uint32 recenter_min_dwell_s = 7;
}

message RollingFuture {
  string tenor = 1; // weekly | monthly | quarterly
  uint32 roll_before_s = 2;
}

message AccountDecl {
  string name = 1;
  int64 api_id = 2;
  string td_instance = 3;
  // 這個 session 在這個帳號上可以使用的 algo_id，每個 1–1023（D602）。
  // 同一個帳號上和其他非 terminal 的 session 重疊時，TD 的 ack 是 REJECTED（algo_conflict）。
  repeated uint32 algo_ids = 4;
}

message RestartPolicy {
  RestartMode mode = 1;
  uint32 max_restarts = 2;
  uint32 window_s = 3;
}

enum RestartMode {
  RESTART_MODE_UNSPECIFIED = 0;
  RESTART_MODE_NEVER = 1;
  RESTART_MODE_ON_FAILURE = 2;
}

message Limits {
  uint32 memory_mb = 1;
  uint32 gomaxprocs = 2;
  uint32 volume_mb = 3; // session volume 的上限（D904）；0 = STS 的預設值。超過就以 FAILED 結束（volume_over_limit）
  uint32 hook_timeout_s = 4; // 單一 hook 的硬上限（D803）；0 = STS 的預設值。超過判定 HUNG
}
```

```proto
// file: mftiko/state/v1/state.proto
syntax = "proto3";

package mftiko.state.v1;

import "mftiko/common/v1/common.proto";
import "mftiko/session/v1/session.proto";

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1;statev1";

// ---------- 帳號的宣告（API server 寫）----------

message Account {
  string name = 1;
  // 帳號種類（DECISIONS D301），不是 venue：BinancePM 可以交易 Binance、BinanceUM、BinanceCM 的商品。
  // 兩個 enum 的名稱可能相同（帳號種類 BinanceUM 交易 venue BinanceUM），不可互查。
  string kind = 2;
  bytes sealed_credentials = 3; // 以 TD instance 的公鑰封裝；etcd 裡不放明文
  AccountSettings settings = 4;
  bool enabled = 5;
}

message AccountSettings {
  uint32 cancel_on_disconnect_s = 1; // 0 = 關閉
  // 歷史補正（DECISIONS D601）。
  uint32 backfill_interval_s = 2; // 另加 jitter；0 = TD agent 的預設值。必須短於交易所成交歷史的保留期
  // 手動觸發：比 TD agent 上次處理過的值新，就提前跑一輪。agent 重啟後可能多跑一次，無害。
  int64 backfill_requested_at_ns = 3;
}

// ---------- STS agent 寫 ----------

enum Phase {
  PHASE_UNSPECIFIED = 0;
  PHASE_PENDING = 1;
  PHASE_WAITING = 2; // 等 ack
  PHASE_STARTING = 3;
  PHASE_RUNNING = 4;
  PHASE_STOPPING = 5;
  PHASE_RESTARTING = 6;
  PHASE_DONE = 7;
  PHASE_FAILED = 8;
}

message SessionStatus {
  int64 observed_generation = 1;
  Phase phase = 2;
  repeated mftiko.common.v1.Condition conditions = 3;
  uint32 incarnation = 4; // 顯示用；權威值在 /sts/{inst}/incarnation/{h}（PROTOCOL §3.2 W6）
  uint32 restart_count = 5;
  string reason = 6;
  mftiko.common.v1.ExitInfo last_exit = 7;
  int64 updated_at_ns = 8;
  StateSnapshot state_out = 9; // session terminal 之後 volume 的快照（D904）；推送完成前不填
}

message StateSnapshot {
  string digest = 1;     // 資料 artifact 的 OCI manifest digest；新 session 以 InputDecl 引用
  string repository = 2;
  bool clean = 3;        // 最後一個 incarnation 的 ExitInfo.on_stop_completed
  uint32 incarnation = 4;
  int64 at_ns = 5;
  uint64 size_bytes = 6;
}

// ---------- controller 寫（不綁 lease，DECISIONS D901、D907）----------

// MD、TD 的需求項目（§3.3）。
message DemandEntry {
  oneof target {
    string feed = 1;
    mftiko.session.v1.SelectorDecl selector = 2;
    int64 api_id = 3;
  }
  int64 spec_revision = 10; // 推導自哪一版 session spec；只供除錯，可能來自另一個 cluster，不做比較
  repeated uint32 algo_ids = 11; // 只有帳號項目：同 AccountDecl.algo_ids，TD agent 用來檢查衝突（D602）
}

// STS 的指派（§3.3）。STS agent 只跑有指派的 session。
message SessionDemand {
  mftiko.session.v1.DesiredState desired = 1; // RUNNING：spawn 並送 Init；ENDED：停掉 worker
  bool start = 2;           // 所有 MD、TD 需求都 READY，STS agent 可以送 Start（D107）
  string end_reason = 3;    // desired 是 ENDED 時：ended_by_spec、deps_failed、deps_timeout
  repeated Binding bindings = 4;
  int64 spec_revision = 5;  // 推導自哪一版 session spec；只供除錯
  int64 updated_at_ns = 6;
}

// 一個需求項目目前的 ack，由 controller 從 provider 的 EntryStatus 抄來。
message Binding {
  oneof target {
    string feed = 1; // feed key。spec 載入時才把其中的 ticker 解成 UID（D203）；這個欄位今天仍是字串
    string selector = 2; // SelectorDecl.name
    int64 api_id = 3;
  }
  string instance = 4;    // 提供這個資源的 MD 或 TD instance
  EntryStatus status = 5; // provider 沒有 ack（沉默）時不填
}

// ---------- provider agent 寫 ----------

enum EntryState {
  ENTRY_STATE_UNSPECIFIED = 0;
  ENTRY_STATE_PENDING = 1;
  ENTRY_STATE_READY = 2;
  ENTRY_STATE_REJECTED = 3;
  ENTRY_STATE_DEAD = 4;
}

// 不綁 lease（§3.2 W7）。provider 的 AgentRecord 不在時，是它最後寫下的狀態。
message EntryStatus {
  EntryState state = 1;
  string reason = 2;
  // feed：完整的 subject（今天是 md.f.{inst}.{topic}.{ticker}）。NATS topic 用人讀的名字，不用 UID；確切拼法還沒定（§10 P14）。selector：成員 subject 的前綴（今天是 md.f.{inst}）；
  // 帳號：td.order.{api_id}。由 provider 決定，訂閱端不自己組（PROTOCOL §7、DECISIONS D106）。
  string subject = 3;
  mftiko.common.v1.WorkerRef worker = 4;
  Universe universe = 5; // 只有 selector
  int64 updated_at_ns = 6;
}

message Universe {
  uint64 epoch = 1;
  repeated string members = 2; // 成員是商品。今天是 UniversalTicker；改成商品 UID 是後續（D203）。selector 本身沒有 UID
  string current = 3;          // 只有 rolling_future。這是 selector，尚未定，不在 D203 的範圍；selector 沒有 UID
  repeated string topics = 4;
}

// ---------- agent 自己 ----------

// 綁在選主 session 的 lease 上（§3.2 W7）：存在就表示這個 instance 有活著的 agent。
message AgentRecord {
  string plane = 1;
  string instance = 2;
  int64 epoch = 3;
  string release = 4;
  int64 started_at_ns = 5;
}

// 不綁 lease（§3.2 W7），只在階段改變時寫；worker 確定不在之後由 agent 刪除。
message WorkerStatus {
  mftiko.common.v1.WorkerRef worker = 1;
  string phase = 2; // STARTING | RUNNING | STOPPING | BACKOFF | EXITED | LOST
  string stage = 3; // worker 回報的生命週期階段
  string release = 4;
  string shim_release = 5;
  uint32 restarts = 6;
  int64 rss_bytes = 7; // 寫入當下的值，只是快照：WorkerStatus 只在階段改變時寫（§3.2 W7）
  int64 started_at_ns = 8;
  mftiko.common.v1.ExitInfo last_exit = 9;
}

// ---------- MD agent 不能重新推導的狀態 ----------

message SelectorState {
  mftiko.session.v1.SelectorDecl spec = 1;
  uint64 epoch = 2;
  repeated string members = 3; // 同 Universe.members：商品。今天是 UniversalTicker
  string current = 4;           // 同 Universe.current：selector，尚未定，不在 D203；selector 沒有 UID
  string center = 5; // 置中的 strike
  int64 center_at_ns = 6;
}

message Placement {
  string conn = 1; // atom 所在的 conn worker key
}
```

## 4. C3：agent ⇄ shim

### 4.1 傳輸

unix socket `${WORK_DIR}/run/{plane}/{h}.sock`，`WORK_DIR` 是 `Launch.work_dir`（§4.6）。frame 是 4 bytes big-endian 長度加一則 protobuf，上限 1 MiB。agent → shim 是 `ShimRequest`，shim → agent 是 `ShimMessage`。

- shim 讀到 `Launch` 之後、spawn 之前開始 listen（§4.6 第 4 步），listen 不成就以 1 結束、不 spawn。agent 以重試 connect 等 socket 出現。
- 權限只給 agent 的 uid。shim 由 agent exec，沿用 agent 的 uid，所以 agent 的 uid 就是 shim 自己的 uid：
  - `run/{plane}` 不存在就以 0700 建立；已經存在的必須是 shim 的 uid 擁有、group 與 other 沒有任何權限，否則以 1 結束。socket 是 0600。
  - 每條連線以 `SO_PEERCRED` 檢查，uid 不同就直接關掉。root 也一樣，除非 shim 自己是 root。
- 路徑上已經有 socket 時先 connect：連得上表示另一個 shim 還在服務同一個 worker key，以 1 結束。換 incarnation 是 delete-before-create（D106），舊的 shim 要先 `Release`。連不上就是殘留（shim 被 SIGKILL），刪掉重建。路徑上是別種檔案時以 1 結束，不刪。
- shim 結束時刪掉自己的 socket。
- 同時可以掛多條連線，只有一條是控制者（§4.2）。每條連線的送出有自己的佇列，寫入有逾時；agent 不讀，佇列滿了或逾時，shim 就關掉這條連線，不會因此卡住其他連線。寫入因為 agent 已經關閉（例如送完 `Stop` 就退出）而失敗時，shim 不再寫，但照樣讀完並執行它已經送出的指令，直到讀到 EOF。

### 4.2 attach 與 fencing（D105）

1. agent 送 `Attach{plane, agent_instance, epoch, shim_majors, resume_from}`，`epoch` 是 §3.2 W2 的值，從 1 起。
2. 依序檢查。不通過就回錯誤，什麼都不改，原來的控制者照舊：
   - `plane`、`agent_instance` 與 `Launch` 不同 → `wrong_agent`。
   - `epoch` 小於 1 → `bad_request`。
   - `epoch` 小於 shim 接受過的最大 epoch → `stale_epoch`，`details["epoch"]` 是那個最大值。這個值在控制者斷線之後仍然保留：新 agent 斷線時，被它取代的舊 agent 重連也拿不回控制權。
3. 通過時（`epoch` 大於或等於接受過的最大值），對舊的控制者連線送 `Preempted{by_epoch}`，送完就關閉它，這條連線成為控制者。相等也接受：同一個 agent 重連時，舊的連線可能還沒斷。控制者自己再送 `Attach` 不會收到 `Preempted`。
4. 協商 major：`shim_majors` 與 shim 支援的 major 的交集裡最大的那個，放在 `Attached.shim_major`。沒有交集時 `shim_major` 是 0：attach 照樣成功，fencing 照樣生效，但這條連線只能送 `Signal`，其他指令回 `no_common_major`，也不串流事件。
5. shim 回 `Attached`，接著送最新的 `Report`，然後從 `resume_from` 開始串流事件。太舊時從最舊的開始送；中間有在確認之前就被丟掉的，第一則標 `gap`（§4.4）。
6. 只有目前控制者的指令會被執行，其他連線收到 `not_controller`，`Signal` 也一樣。

| `Response.error.code` | 意思 |
|---|---|
| `wrong_agent` | `Attach` 的 plane 或 instance 不是 spawn 這個 shim 的 agent |
| `stale_epoch` | `Attach` 的 epoch 小於接受過的最大值 |
| `not_controller` | 這條連線不是控制者 |
| `no_common_major` | 控制者和 shim 沒有共同的 major，只能 `Signal` |
| `bad_request` | 欄位不合法：epoch 小於 1、`signo` 不在 1–64、`Ack` 的 seq 還沒產生 |
| `unimplemented` | shim 不認得或沒有實作的指令。同一個 major 內新加的指令，舊的 shim 就回這個 |
| `exited` | worker 已經被 reap（§4.3） |
| `signal_failed` | `kill` 失敗，`message` 是原因 |
| `running` | worker 還沒被 reap 時送了 `Release`（§4.3） |
| `bad_stage` | `Init`、`Start` 不在它要的階段（§4.3）；`details["phase"]` 是目前的階段，例如 `initializing` |
| `worker_busy` | 往 worker 的佇列滿了：worker 沒在讀 fd 3。指令沒有送出，階段不變 |

**跨 major 不變的部分。** 下一個 major 可以改任何東西，除了這些：沒有共同 major 的 agent 靠它們 attach、被 fencing、送 `Signal`、看出 worker 是否還在。它們的欄位編號、型別與意思在所有 major 都相同。

- frame（§4.1），以及 `common.v1` 的 `WorkerRef`、`Error.code`。
- `ShimRequest` 的 `id`（1）、`attach`（10）、`signal`（12）；`Attach` 的 `plane`、`agent_instance`、`epoch`、`shim_majors`（1、2、3、5）；`Signal.signo`（1）。
- `ShimMessage` 的 `response`（1）、`preempted`（3）；`Response` 的 `id`、`ok`、`error`、`attached`（1–4）；`Attached` 的 `shim_release`、`shim_major`、`worker`、`state`、`pid`、`pgid`、`start_time`（1–3、5、7–9）；`Preempted.by_epoch`（1）。

### 4.3 生命週期（D107）

agent 經 shim 送 `Init`、`Start`、`Stop`。shim 把 `Init`、`Start` 原樣轉給 worker，自己只記下 worker 目前的階段。生命週期是兩段（DECISIONS D107）：`Init` 在 spawn 後立刻送，不等任何需求；`Start` 在依賴都就緒之後才送（STS 等 controller 把指派的 `start` 設成 true，§3.3 N2；MD、TD 的 worker 沒有跨平面依賴，`Init` 完成就送）。

shim 記下的階段是 `Attached.phase`。新的 agent 收養 worker 時以它決定接著送什麼，不必從事件推回來：

| 階段 | 進入 |
|---|---|
| `NEW` | spawn 之後 |
| `INITIALIZING` | `Init` 轉給 worker |
| `INITIALIZED` | worker 回 `Done(INIT, ok)` |
| `STARTING` | `Start` 轉給 worker |
| `STARTED` | worker 回 `Done(START, ok)` |
| `STOPPING` | 收到 `Stop`、shim 收到 SIGTERM、worker 回 `Done(…, !ok)`，或 worker 送了 `Exiting` |

worker 被 reap 之後 `phase` 停在最後一個，`state` 是 `EXITED`。

| 指令 | shim 做什麼 |
|---|---|
| `Init{payload}` | 階段是 `NEW` 時轉給 worker，否則 `bad_stage`。payload 是該平面 C5 的 `InitPayload` |
| `Start{payload}` | 階段是 `INITIALIZED` 時轉給 worker，否則 `bad_stage`：要等 worker 回 `Done(INIT, ok)`。payload 是該平面 C5 的 `StartPayload` |
| `Stop{reason, grace_ms, kill_after_ms}` | 對 worker 送 C4 `Stop{reason, grace_ms}`；`grace_ms` 到了還沒結束就對 process group 送 SIGTERM，再過 `kill_after_ms` 就 SIGKILL。0 表示不等。計時在 shim，從 shim 收到 `Stop` 起算，agent 中途消失也照樣執行。worker 已經送了 `Exiting` 時不再送 C4 `Stop`，計時照樣開始（`OnStop` 可能卡住）。只有第一個 `Stop` 設定計時，之後的 `Stop` 回 ok、不改計時；要更快就送 `Signal` |
| `Signal{signo}` | 對 worker 的 process group 送信號，`signo` 是 1–64。任何 shim major 都必須支援（§4.2）。worker 被 reap 之後回 `exited`，不再送：pid 釋出之後，同一個號碼可能成為別人的 process group。shim 先以 `waitid(WNOWAIT)` 得知 worker 結束，在與 `Signal` 互斥的情況下才 reap，所以 reap 之前送出的 `Signal` 一定送到這個 group |
| `Relay{contract, corr_id, payload}` | 轉給 worker（C5 指令），任何階段都可以 |
| `Release{}` | worker 的結束已記錄（S3）時才接受，否則 `running`。回應送出後，shim 刪除結束檔與 socket 後退出 |

worker 被 reap 之後，`Init`、`Start`、`Stop`、`Relay` 都回 `exited`。轉給 worker 的指令依收到的順序寫進 fd 3；worker 不讀時，寫入排在 shim 的佇列裡，佇列滿了就回 `worker_busy`，不會卡住其他連線。`Stop` 的 C4 `Stop` 排不進去時，計時照樣開始：SIGTERM 會在 grace 之後到，SDK 把 SIGTERM 當成 `Stop`（§9.2）。

shim 收到 SIGTERM（例如主機關機）時，自己對 worker 送 `Stop`，不先退出：`reason` 是 `shim: terminated`，grace 10 秒，再過 5 秒 SIGKILL。worker 已經在 `Stop` 的計時裡時沿用原來的計時。worker 被 reap 之後 shim 才退出，不等 `Release`。收到 SIGTERM 時 worker 已經結束的話，shim 立刻退出。SIGINT 一樣。

### 4.4 事件與緩衝

| 事件 | 緩衝 |
|---|---|
| `Started`、`Hello`、`Done`、`Stage`、`Exiting`、`Stopped` | 環狀緩衝（1024 則），滿了丟最舊的 |
| `Report` | 只保留最新一份；attach 時先送一次 |
| `Beat` | 不緩衝；最多每秒轉發一則。`Attached` 帶最後一次的時間 |
| `Relay`（worker 的請求） | 不緩衝；沒有控制者時，shim 立刻對 worker 回 `Undeliverable` |
| `Exited` | 環狀緩衝，另外落檔 `run/{plane}/{h}.exit.pb`，直到 `Release`（S3） |

- **快照為準（DECISIONS D108）。** agent 做任何決策所依據的事實，都必須在 `Attached`（或結束檔）裡有最新值：state、phase、最後一個 stage、`Hello`、最後一次 `Beat` 的時間、`exiting`（worker 送的第一則 `Exiting`）、`exit`（記錄完的 `ExitInfo`），加上 attach 時先送的最新 `Report`。事件是降低延遲的通知；agent 不在時遺失的事件只少了歷史，不影響它回來之後的判斷。
- **seq。** 進環狀緩衝的事件依序編號，從 1 起，一個 shim 一個序列。`Started` 是 1。`Report`、`Beat`、`Relay` 不進緩衝，`seq` 是 0。`at_ns` 是 shim 收到的時間。
- **Ack。** `Ack{seq}` 表示 agent 不再需要 seq 以前（含）的事件，shim 提早把它們從環裡釋放；環最多保留 1024 則還沒釋放的。它不是提交：shim 不需要知道 agent 處理完了沒有，agent 也不必等持久化才送，收到就可以 Ack，或者完全不送。確認過的 seq 再送一次不改變什麼；還沒產生的 seq 回 `bad_request`。只有控制者能送，且要有共同的 major。
- **補送。** attach 成功之後，shim 先回 `Attached`（`oldest_seq` 是環裡最舊的 seq，`next_seq` 是下一則會拿到的 seq；環是空的時兩者相同。`Attached` 是同一個時間點的快照：它反映了 seq < `next_seq` 的每一則事件，不含 seq ≥ `next_seq` 的任何一則，所以 agent 拿到快照之後只需要 seq ≥ `next_seq` 的事件，更早的可以略過），接著送最新的 `Report`（有的話），再從 `resume_from` 開始送環裡的事件（0 等於 1），之後新的事件一產生就送。補送與即時的事件走同一個游標，不會漏也不會重複。控制者自己再 `Attach` 也一樣，從新的 `resume_from` 重送。沒有共同 major 的控制者不收事件。
- **gap。** agent 要的事件（從 `resume_from` 起）有一部分在 Ack 之前就因為環滿了被丟掉，補送的第一則標 `gap`。被 `Ack` 釋放的不算。`gap` 只說明歷史不完整；要知道現在的狀態，看 `Attached`。
- **Beat 逾時（E1-18）。** agent 記下每個 worker 最後一次 `Beat` 的時間（收養時取 `Attached.last_beat_at_ns`），第一次之前從 spawn 算起。超過平面 Policy 的 `BeatTimeout` 就通知 Policy（`Silent`），每段沉默只通知一次，worker 再送 `Beat` 之後重新計算；Policy 可以要求以 `beat timeout` 為理由停掉它，判定 `HUNG` 的規則屬於各平面（E9）。shim 每秒最多轉發一則 `Beat`，所以逾時至少要幾秒。這個停止不是依據需求，需求過舊（§3.2 W8）時照樣進行。
- **`Controller`（C4）。** 有控制者、而且有共同的 major 時，worker 的 `Relay` 才有人收。這件事改變時（控制者出現、離開、換了 epoch），shim 對 worker 送 `Controller{attached, epoch}`；同一個控制者再 attach 不送。worker 沒在讀 fd 3 時這則可能丟失，shim 仍然照常回 `Undeliverable`。

### 4.5 不變式

- **S1** shim 是 worker 唯一的父進程；Linux 上以 `PR_SET_CHILD_SUBREAPER` 收孫進程；shim 以 `setsid` 開新 session，由 init 收養。
- **S2** shim 消失時 worker 自行 graceful stop：socketpair 讀到 EOF；Linux 上另有 `Pdeathsig` 當備援（spawn worker 的 goroutine 必須 `LockOSThread`）。
- **S3** shim reap 之後記錄 worker 怎麼結束，等 `Release` 才退出：
  - reap 之後先把 fd 3 讀到 EOF（最多 1 秒：worker 把 fd 3 漏給孫進程時等不到），`Exiting`、`Stopped` 才不會被漏掉；再依 §5.3 合成 `ExitInfo`。
  - 寫結束檔 `${WORK_DIR}/run/{plane}/{h}.exit.pb`（`ExitRecord`：worker、`spec_hash`、pid、start time、`ExitInfo`），寫暫存檔、fsync、rename，讀的人只會看到完整的一份。權限 0600。
  - 然後在環狀緩衝加一則 `Exited`。到這裡才算記錄完：`Attached.state` 才是 `EXITED`，`Release` 才接受。
  - `Release` 刪除結束檔再退出。shim 收到 SIGTERM 而在 `Release` 之前退出時（主機關機），結束檔留著給下一個 agent。
  - shim 啟動時，同一個 worker key 還有沒收走的結束檔，就以 1 結束、不 spawn（§4.6 第 4 步）：否則新的 incarnation 會蓋掉它。agent 讀完結束檔後刪掉它（E1-17）。
- **S4** shim 持有 worker 的 stdout／stderr，寫成檔並輪替：
  - 檔案是 `${WORK_DIR}/log/{plane}/{h}.stdout` 與 `{h}.stderr`，`h` 同 §4.1 的 socket。目錄不存在就以 0700 建立，檔案是 0600。
  - 依 worker key、不依 incarnation：新的 incarnation 接在同一組檔案後面寫，開頭一行 `[mftiko-shim] <時間> worker <key> incarnation <n>`。同一個 worker key 同時只有一個 shim（§4.1），所以每個檔案只有一個寫入者。
  - 檔案到了 `Launch.output.max_bytes` 就輪替：`{h}.stdout` 改名成 `.1`，原來的 `.1` 改成 `.2`，依此類推；每個 stream 只留 `keep` 個檔案（含目前寫的那個），最舊的刪掉。沒有任何檔案超過 `max_bytes`。預設 64 MiB、5 個。
  - worker 絕不因為輸出而阻塞：shim 一直讀 pipe，寫檔在另一個執行緒。寫檔跟不上（最多暫存 2 MiB）或寫入失敗（磁碟滿）時丟棄，並在恢復寫入的位置插入一行 `[mftiko-shim] N bytes of stdout lost here: …`。
  - worker 被 reap 之後，shim 退出前最多等 2 秒把剩下的輸出寫完；worker 留下的孫進程若還拿著 pipe，不等它。
  - 檔案不隨 `Release` 刪除。STS 的 worker key 不會重用，session 結束後由 STS agent 清掉。
- **S5** shim 不認識任何平面，不解讀 C5，不連 NATS，不連 etcd，不做重啟決策。

```proto
// file: mftiko/shim/v1/shim.proto
syntax = "proto3";

package mftiko.shim.v1;

import "mftiko/common/v1/common.proto";
import "mftiko/worker/v1/worker.proto";

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1;shimv1";

// ---------- agent 啟動 shim（§4.6） ----------

// agent exec shim 之後寫進它 stdin 的唯一一個 frame。
message Launch {
  string plane = 1;
  string agent_instance = 2;
  mftiko.common.v1.WorkerRef worker = 3;
  string spec_hash = 4;     // agent 給的，shim 不解讀，Attached 原樣帶回
  // 以下由 artifact 的 manifest 換算（§4.6 的對應表）
  string path = 5;          // rootfs ＋ entrypoint[0]，絕對路徑
  repeated string args = 6; // entrypoint[1:]；argv[0] 是 path
  repeated string env = 7;  // manifest 的 env，之後是 §5.4 的平台變數；KEY=VALUE。shim 自己加上 MFTIKO_CTL_FD=3
  string dir = 8;           // rootfs，絕對路徑；path 必須在它底下
  Credential cred = 9;      // 平面決定，不來自 manifest；沒有就沿用 shim 的身分
  string work_dir = 10;     // ${WORK_DIR}，絕對路徑；C3 的 socket 在 run/{plane}/{h}.sock（§4.1）
  Output output = 11;       // worker 的 stdout、stderr 怎麼輪替（§4.5 S4）；沒有就用預設
}

message Credential {
  uint32 uid = 1;
  uint32 gid = 2;
  repeated uint32 groups = 3; // 附加群組；空的表示沒有
}

// worker 的輸出寫在 log/{plane}/{h}.stdout、{h}.stderr，各自輪替（§4.5 S4）。0 表示用預設。
message Output {
  uint64 max_bytes = 1; // 一個檔案的上限；預設 64 MiB，不得小於 64 KiB
  uint32 keep = 2;      // 每個 stream 保留幾個檔案，含目前寫的那個；預設 5，最多 100
}

// ---------- agent → shim ----------

message ShimRequest {
  uint64 id = 1;
  oneof op {
    Attach attach = 10;
    Ack ack = 11;
    Signal signal = 12;
    mftiko.worker.v1.Init init = 13;
    mftiko.worker.v1.Start start = 14;
    Stop stop = 15;
    mftiko.worker.v1.Relay relay = 16;
    Release release = 17;
  }
}

message Attach {
  string plane = 1;
  string agent_instance = 2;
  int64 epoch = 3;
  string agent_release = 4;
  repeated uint32 shim_majors = 5;
  uint64 resume_from = 6;
}

message Ack {
  uint64 seq = 1;
}

message Signal {
  int32 signo = 1;
}

message Stop {
  string reason = 1;
  uint32 grace_ms = 2;
  uint32 kill_after_ms = 3;
}

message Release {}

// ---------- shim → agent ----------

message ShimMessage {
  oneof msg {
    Response response = 1;
    Event event = 2;
    Preempted preempted = 3;
  }
}

message Response {
  uint64 id = 1;
  bool ok = 2;
  mftiko.common.v1.Error error = 3; // code 見 §4.2 的表
  Attached attached = 4;
}

message Attached {
  string shim_release = 1;
  uint32 shim_major = 2; // 協商出的 major；0 表示沒有交集，只能 Signal（§4.2）
  mftiko.common.v1.WorkerRef worker = 3;
  string spec_hash = 4;
  WorkerState state = 5;
  string stage = 6; // 最後一個 Stage
  int32 pid = 7;
  int32 pgid = 8;
  uint64 start_time = 9; // /proc/<pid>/stat 的 starttime，防 pid 重用
  mftiko.worker.v1.Hello hello = 10;
  uint64 oldest_seq = 11;
  uint64 next_seq = 12; // 這份快照的時間點：反映了 seq < next_seq 的每一則事件，不含 seq ≥ next_seq 的任何一則（§4.4）
  int64 last_beat_at_ns = 13;
  Phase phase = 14; // 生命週期的階段（§4.3）；worker 結束後是結束前的最後一個
  mftiko.worker.v1.Exiting exiting = 15; // worker 送的第一則 Exiting；沒送過就沒有（§4.4）
  mftiko.common.v1.ExitInfo exit = 16;   // state 是 EXITED 時的 ExitInfo，與結束檔相同（§4.5 S3）
}

// shim 記下的生命週期階段（§4.3）。
enum Phase {
  PHASE_UNSPECIFIED = 0;
  PHASE_NEW = 1;          // 還沒 Init
  PHASE_INITIALIZING = 2; // Init 已轉給 worker，還沒 Done
  PHASE_INITIALIZED = 3;  // Done(INIT, ok)
  PHASE_STARTING = 4;     // Start 已轉給 worker，還沒 Done
  PHASE_STARTED = 5;      // Done(START, ok)
  PHASE_STOPPING = 6;     // 收到 Stop、shim 收到 SIGTERM、Done 失敗，或 worker 送了 Exiting
}

enum WorkerState {
  WORKER_STATE_UNSPECIFIED = 0;
  WORKER_STATE_STARTING = 1; // 已 exec，還沒 Hello
  WORKER_STATE_RUNNING = 2;
  WORKER_STATE_EXITED = 3;
}

message Event {
  uint64 seq = 1;
  int64 at_ns = 2;
  bool gap = 3;
  oneof kind {
    Started started = 10;
    mftiko.worker.v1.Hello hello = 11;
    mftiko.worker.v1.Done done = 12;
    mftiko.worker.v1.Stage stage = 13;
    mftiko.worker.v1.Beat beat = 14;
    mftiko.worker.v1.Report report = 15;
    mftiko.worker.v1.Relay relay = 16;
    mftiko.worker.v1.Exiting exiting = 17;
    mftiko.worker.v1.Stopped stopped = 18;
    Exited exited = 19;
  }
}

message Started {
  int32 pid = 1;
  int32 pgid = 2;
  uint64 start_time = 3;
}

message Exited {
  mftiko.common.v1.ExitInfo exit = 1;
}

// 結束檔 run/{plane}/{h}.exit.pb 的內容（§4.5 S3）：worker 被 reap、ExitInfo 合成之後寫入，Release 時刪除。
message ExitRecord {
  mftiko.common.v1.WorkerRef worker = 1; // 哪一個 incarnation
  string spec_hash = 2;
  int32 pid = 3;
  uint64 start_time = 4;
  mftiko.common.v1.ExitInfo exit = 5;
}

message Preempted {
  int64 by_epoch = 1;
}
```

### 4.6 啟動

agent exec `mftiko-shim`，在它的 stdin 寫一個 `Launch` frame 後關掉。`Launch` 不經環境變數、不落檔，`ps` 也看不到。

三種 worker 都從 artifact 起（§8）：agent 先照 §8.3 第 1–3 步取得、檢查並解開 artifact，再把 manifest（§8.2）換算成 `Launch`。shim 只執行換算的結果，不認識 OCI，也不連 registry（S5）。

| `Launch` | 來源 |
|---|---|
| `plane`、`agent_instance` | agent 自己 |
| `worker` | worker key，與 §3.2 W6 取得的 incarnation |
| `spec_hash` | agent 對 worker 的 spec 算的雜湊，平面自訂 |
| `dir` | `${DATA}/artifacts/<image digest>/rootfs` |
| `path` | `dir` ＋ `/` ＋ `entrypoint[0]` |
| `args` | `entrypoint[1:]` |
| `env` | manifest 的 `env`（依 key 排序），之後是 §5.4 平台注入的變數。manifest 設了 `MFTIKO_` 開頭的變數就不 spawn |
| `cred` | 平面決定，不來自 manifest |
| `work_dir` | agent 的設定（`${WORK_DIR}`）；C3 的 socket 與 worker 的輸出都在它底下（§4.1、§4.5 S4） |
| `output` | agent 的設定；不設就用預設（§4.5 S4） |

shim：

1. `setsid`（已經是 session leader 就略過），再設 `PR_SET_CHILD_SUBREAPER`（S1）。agent 起 shim 時要嘛讓它開新 session（Go 是 `SysProcAttr.Setsid`），要嘛什麼都不設；只設 `Setpgid` 會讓 shim 成為 process group leader，`setsid` 就會失敗。fd 3 以上全部設成 close-on-exec，agent 讓 shim 繼承的 fd 不會傳到 worker。
2. 從 stdin 讀一個 frame，之後 stdin 換成 `/dev/null`。10 秒內沒讀到就以 1 結束。
3. 檢查 `Launch`：`dir` 是絕對路徑，`path` 是 `dir` 底下的絕對路徑（逐字比對，不解 symlink），`env` 每項都是 `KEY=VALUE`、同一個 key 不出現兩次，且不自己設 `MFTIKO_CTL_FD`。`plane`、`agent_instance` 符合 instance 名稱的語法（§2），`worker.key` 不是空的，`work_dir` 是乾淨的絕對路徑，socket 的路徑不超過 107 bytes（`sun_path` 的上限）。`output.max_bytes` 是 0 或至少 64 KiB，`output.keep` 不超過 100。不符就以 1 結束，不 spawn。
4. 開 C3 的 socket（§4.1）；確認沒有前一個 incarnation 沒收走的結束檔（S3）；再開 worker 的兩個輸出檔、寫入這個 incarnation 的標頭（§4.5 S4）。任何一步不成就以 1 結束，不 spawn。
5. 建 socketpair，exec worker：
   - fd 0 是 `/dev/null`，fd 1、2 各是一條 pipe，讀端在 shim（S4），fd 3 是 socketpair 的一端；其他 fd 一律不繼承。
   - 環境只有 `env` 加上 `MFTIKO_CTL_FD=3`，不繼承 shim 自己的。rlimit、umask 與被忽略的信號則沿用 shim 的，也就是 agent 起 shim 時的設定。
   - worker 自成一個 process group，`Signal` 送給這個 group（§4.3）；它留在 shim 的 session 裡。
   - `Pdeathsig` 是 SIGTERM（S2）。fork 它的執行緒一直活到 worker 被 reap，否則 kernel 會把那個執行緒結束當成 parent 死了。
   - 有 `cred` 就降到該 uid、gid 與附加群組，shim 需要 `CAP_SETUID`、`CAP_SETGID`；`cred` 和 shim 自己的身分相同時不需要。沒有 `cred` 就沿用 shim 的身分。uid 和 shim 不同時，shim 對 worker 送 `Signal`（以及 `Stop` 的 SIGTERM、SIGKILL）還需要 `CAP_KILL`，沒有就以 1 結束、不 spawn：否則要停 worker 時才發現殺不掉。「不同」照 kill(2) 的規則：`cred.uid` 既不是 shim 的 real uid 也不是 effective uid。
6. 開始接受 agent 的連線（§4.2）。之後 reap worker 與每個被收養的孫進程，直到一個都不剩。

exec 失敗（找不到執行檔、沒有權限）時 shim 以 1 結束，agent 從 shim 的結束得知。

shim 自己的 log 寫 stderr，由 agent 決定接到哪裡；接檔案比接 pipe 好，pipe 的讀端隨 agent 消失。讀端不在時 shim 的寫入失敗，但 shim 不會因此結束。

agent 這一端（`internal/platform/agent/fleet`）：

- 以 `SysProcAttr.Setsid` 起 shim，stdout、stderr 都接到 `${WORK_DIR}/log/{plane}/{h}.shim.log`（附加寫入，跨 incarnation 同一個檔），不接 pipe。
- `Launch.env` 是平面給的（manifest 的 `env`，再加平面自己的 §5.4 變數），最後由 agent 補上 `MFTIKO_PLANE`、`MFTIKO_INSTANCE`、`MFTIKO_WORKER_KEY`、`MFTIKO_INCARNATION`。
- 寫完 `Launch` 就 connect 並 `Attach`（§4.1 的重試）；shim 在 listen 之前就結束時立刻放棄，不等逾時。接著送 `Init`。
- agent 活著時由它 reap 自己起的 shim；agent 不在之後由 init 接手。
- 同一個 worker key 至多一個存活的 incarnation（D106）。起下一個之前，上一個要確定不在：
  - 有結束紀錄（`Exited`，或 shim 不見之前寫下的結束檔，S3）就是不在；經 shim `Release`，或 shim 已不在時由 agent 刪掉結束檔。
  - 沒有紀錄（shim 被 SIGKILL）時，以 `Attached` 的 `pid` 與 `start_time` 查 `/proc/<pid>/stat`：pid 不存在、start_time 不同（pid 被重用）、或是 zombie，才算不在；agent 起的那個 shim 也要已經結束。讀不到 `/proc` 不算不在。
  - 還在就不起，等退避之後再試。worker 應該隨 shim 結束（D104：Pdeathsig、fd 3 的 EOF），這只是保險。
- 啟動時收養（D102，E1-17）：新的 term 在第一次 reconcile 之前（§3.2 W7.3）掃 `run/{plane}/` 的 `{h}.sock` 與 `{h}.exit.pb`：
  - socket 有回應：以這個 term 的 epoch `Attach`（舊 term 若還連著就被 `Preempted`），worker 的狀態取自 `Attached`（D108）；`next_seq` 之前的事件已經反映在快照裡，略過。被拒為 `stale_epoch` 表示有更新的 term，agent 結束。
  - socket 有回應但 attach 不成（逾時）：保留它的 `WorkerStatus`（W7.1），不為這個 worker key 起新的 incarnation（它可能還在跑），每隔幾秒重試，直到 attach 成功或 shim 不在。
  - socket 沒有回應而有結束檔：worker 在沒有 agent 時結束，當成已結束的 worker 交給 Policy；`Release` 時由 agent 刪掉結束檔。
  - socket 沒有回應也沒有結束檔：shim 在記錄結束之前被殺，worker key 不明，無從收養；它的 `WorkerStatus` 依 W7.1 處理（需求還要它就保留）。下一個 shim 會清掉殘留的 socket（§4.1）。
  - live config 以 etcd 為唯一來源（DECISIONS D111）。`Attached` 還沒有 worker 已套用的版本（E1-22 的 `Applied`），所以收養後 agent 把從 etcd 推導出的 live config 一律再推一次（暫行）。live config 是完整的期望狀態，worker 以 diff 套用，收到相同的就是無事。

部署時 shim 不能和 agent 放在會一起被殺的 cgroup 或 systemd unit 裡，否則 agent 重啟時 shim 和 worker 會跟著死，D102–D104 就不成立。

## 5. C4：shim ⇄ worker

### 5.1 傳輸

shim 在 exec 前建立 `socketpair(AF_UNIX, SOCK_STREAM)`，worker 的一端是 fd 3（`MFTIKO_CTL_FD=3`）。frame 與 C3 相同。worker → shim 是 `WorkerUp`，shim → worker 是 `WorkerDown`。兩端死掉都以 EOF 呈現。

### 5.2 流程

```
worker 啟動 ──▶ Hello
          ◀── Init(InitPayload)          STS：OnInit
          ──▶ Done(INIT, ok)
          ◀── Start(StartPayload)        STS：訂閱 feed → OnStart → 開始交付
          ──▶ Done(START, ok)、Stage(running)
              （之後：Beat 每 1 秒、Report、Relay）
          ◀── Stop                       或策略自己 Exit／Fail，或 crash
          ──▶ Exiting(outcome, reason)   STS：OnStop
          ──▶ Stopped(on_stop_completed)
              exit(code)
```

- `Exiting` 一定在 `OnStop` 之前送。`OnStop` 卡住被殺時，agent 仍然知道這個 worker 原本打算怎麼結束。
- `Beat` 由不會被 hook 阻塞的執行緒送（Go SDK 是獨立的 goroutine）。做不到的時候，由 STS agent 的 `Beat` 逾時接手（§9.7）。
- 收到 `Controller` 時更新「目前有沒有 agent」。沒有時，worker 對 agent 的請求會立刻收到 `Undeliverable`。

### 5.3 結束方式與 exit code

| outcome | 觸發 | `OnStop` | exit code |
|---|---|---|---|
| `DONE` | `Stop`，或策略 `Exit()` | 跑 | 0 |
| `INIT_FAILED` | `Init`／`Start` 失敗或逾時 | `Start` 失敗時跑 | 10 |
| `CRASHED` | hook panic 或回傳 error | 跑 | 20 |
| `HUNG` | hook 卡太久 | 不跑 | 21 |
| `FAILED` | 策略 `Fail()`，或 agent 要求 fail | 跑 | 30 |
| （沒有 `Exiting`） | 被殺：OOM、SIGKILL、沒接住的 panic | 不跑 | signal，或 1–9（Go 的 runtime 錯誤是 2） |

範圍保留給之後細分：10–19 初始化類、20–29 執行中失敗類、30–39 依要求結束類。

shim 是 parent，wait status 一定拿得到；`Exiting` 和 wait status 一起放進 `ExitInfo`：

| 欄位 | 來源 |
|---|---|
| `code`、`signal` | 一律依 wait status。被 signal 結束時一律以 signal 為準：`code` 是 -1，`signal` 是那個 signal，不管 worker 說了什麼 |
| `outcome`、`reason` | 有 `Exiting` 就依它：它在 `OnStop` 之前送，`OnStop` 卡住被殺時仍然說得出原本打算怎麼結束。沒有 `Exiting`：正常結束的依 exit code（0 `DONE`、10–19 `INIT_FAILED`、21 `HUNG`、20 與 22–29 `CRASHED`、30–39 `FAILED`，其餘沒有 outcome），`reason` 是 `exit code N, no Exiting`；被 signal 結束的就是被殺，沒有 outcome，`reason` 是 `signal N (名稱), no Exiting` |
| `on_stop_completed` | 依 `Stopped`；沒有就是 false |
| `at_ns` | 記錄的時間 |

### 5.4 worker 的環境合約

| env | 內容 |
|---|---|
| `MFTIKO_WORKER_KEY`、`MFTIKO_INCARNATION` | 身分 |
| `MFTIKO_PLANE`、`MFTIKO_INSTANCE` | 所屬 agent |
| `MFTIKO_CTL_FD` | `3` |
| `MFTIKO_NATS_URL` | NATS 的位址。worker 沒有 etcd 的存取權 |
| `MFTIKO_NATS_CREDS` | 只有 MD、TD：所屬 instance 的 NATS 憑證。STS 的 session worker 沒有這個變數，它的憑證每個 session 一份，經 `Init` 送進去（DECISIONS D906） |
| `GOMEMLIMIT`、`GOMAXPROCS` | Go runtime |
| `MFTIKO_VOLUME_DIR` | 只有 STS：session 的可寫目錄，跨 incarnation 保留（D904） |
| `MFTIKO_INPUT_DIR` | 只有 STS：`inputs` 解開的位置，每個 input 一個子目錄（D904）。worker 不應寫入 |

spawn spec 和憑證不走環境變數，也不落檔：都在 `Init` 的 payload 裡，經 socketpair 送進去。

```proto
// file: mftiko/worker/v1/worker.proto
syntax = "proto3";

package mftiko.worker.v1;

import "mftiko/common/v1/common.proto";

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1;workerv1";

// ---------- worker → shim ----------

message WorkerUp {
  oneof msg {
    Hello hello = 1;
    Done done = 2;
    Stage stage = 3;
    Beat beat = 4;
    Report report = 5;
    Relay relay = 6;
    Exiting exiting = 7;
    Stopped stopped = 8;
  }
}

message Hello {
  string kind = 1;               // session | account | conn
  string release = 2;
  repeated string contracts = 3; // 支援的 C5 契約，例如 "mdctl.v1"
  int32 pid = 4;
}

enum Verb {
  VERB_UNSPECIFIED = 0;
  VERB_INIT = 1;
  VERB_START = 2;
}

message Done {
  Verb verb = 1;
  bool ok = 2;
  string error = 3;
}

message Stage {
  string name = 1;
}

message Beat {
  uint64 seq = 1;
  bytes progress = 2;
}

message Report {
  string contract = 1;
  bytes payload = 2; // 該平面 C5 的 Report，最新一份取代舊的
}

message Relay {
  string contract = 1;
  bytes corr_id = 2;
  bytes payload = 3;
}

message Exiting {
  mftiko.common.v1.Outcome outcome = 1;
  string reason = 2;
  string detail = 3;
}

message Stopped {
  bool on_stop_completed = 1;
}

// ---------- shim → worker ----------

message WorkerDown {
  oneof msg {
    Init init = 1;
    Start start = 2;
    Stop stop = 3;
    Relay relay = 4;
    Undeliverable undeliverable = 5;
    Controller controller = 6;
  }
}

message Init {
  string contract = 1;
  bytes payload = 2; // 該平面 C5 的 InitPayload
  uint32 deadline_ms = 3;
}

message Start {
  string contract = 1;
  bytes payload = 2; // 該平面 C5 的 StartPayload
  uint32 deadline_ms = 3;
}

message Stop {
  string reason = 1;
  uint32 grace_ms = 2;
}

message Undeliverable {
  bytes corr_id = 1;
}

message Controller {
  bool attached = 1;
  int64 epoch = 2;
}
```

## 6. C5：各平面的 payload

下行的 `Command` 一律是完整快照，帶 `Version(epoch, seq)`；worker 只套用比手上大的。上行的 `Report` 也是完整快照，最新一份取代舊的。agent 輪換之後，新 agent 再推一次完整快照即可。

### 6.1 STS：`stsctl.v1`

```proto
// file: mftiko/stsctl/v1/stsctl.proto
syntax = "proto3";

package mftiko.stsctl.v1;

import "mftiko/common/v1/common.proto";
import "mftiko/session/v1/session.proto";

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1;stsctlv1";

message InitPayload {
  string session_id = 1;
  uint32 incarnation = 2;
  string sts_instance = 3;
  string name = 4;
  mftiko.session.v1.StrategyRef strategy = 5;
  bytes params = 6;
  repeated mftiko.session.v1.FeedDecl feeds = 7;
  repeated mftiko.session.v1.AccountDecl accounts = 8;
  NatsCredentials nats = 9; // 這個 session 專屬的 NATS 憑證，只經 socketpair 送進去（DECISIONS D906）
}

// STS agent 為這個 session 簽的 user JWT。權限見 PROTOCOL §7。
message NatsCredentials {
  string user_jwt = 1;
  string seed = 2;         // NKey seed，只在進程記憶體裡
  string inbox_prefix = 3; // _INBOX_{session_id}；SDK 以它設定 CustomInboxPrefix
}

message StartPayload {
  Bindings bindings = 1;
}

message Command {
  oneof cmd {
    Bindings bindings = 1; // 綁定變化（selector 換成員、資源 DEAD 等）
    Fail fail = 2;
  }
}

message Fail {
  string reason = 1;
}

// ack 的完整快照：controller 整理進 SessionDemand.bindings（§3.3），STS agent 轉成這個格式送給 worker。
message Bindings {
  mftiko.common.v1.Version version = 1; // STS agent 為每個 session 產生，不是 etcd revision
  repeated FeedBinding feeds = 2;
  repeated SelectorBinding selectors = 3;
  repeated AccountBinding accounts = 4;
}

message FeedBinding {
  string feed = 1;
  string md_instance = 2;
  string subject = 3;
  BindingState state = 4;
}

message SelectorBinding {
  string name = 1;
  string md_instance = 2;
  Universe universe = 3;
  string subject_prefix = 4; // 成員的 subject 是 {subject_prefix}.{topic}.{ticker}。NATS topic 用人讀的名字，不用 UID；確切拼法還沒定（D203、§10 P14）
}

message AccountBinding {
  string name = 1;
  int64 api_id = 2;
  BindingState state = 3;
  mftiko.common.v1.WorkerRef worker = 4;
}

message Report {
  mftiko.common.v1.Version bindings_applied = 1;
  string hook = 2;
  uint64 hook_blocked_ms = 3;
  uint64 hook_slow = 4;
  repeated QueueStat queues = 5;
}

// name 是 td 或 md（D803）。
message QueueStat {
  string name = 1;
  uint32 depth = 2;
  uint64 dropped = 3;    // md：因超過 recv_window_ms 或 buffer 滿而丟掉的則數
  uint32 max_age_ms = 4; // 上一個回報週期內，交給 hook 的事件中最老的那則的年齡
}

// 綁定的狀態，對應 provider 的 ack（§3.5）。值與 state.v1.EntryState 相同；stsctl 自己定義，策略的契約才不綁在 etcd 的
// schema 上（DECISIONS D007）。
enum BindingState {
  BINDING_STATE_UNSPECIFIED = 0; // provider 沒有 ack（沉默）
  BINDING_STATE_PENDING = 1;
  BINDING_STATE_READY = 2;
  BINDING_STATE_REJECTED = 3;
  BINDING_STATE_DEAD = 4;
}

// selector 目前的成員。欄位與 state.v1.Universe 相同。
message Universe {
  uint64 epoch = 1;
  repeated string members = 2; // 成員是商品。今天是 UniversalTicker；改成商品 UID 是後續（D203）。selector 本身沒有 UID
  string current = 3;          // 只有 rolling_future。這是 selector，尚未定，不在 D203 的範圍；selector 沒有 UID
  repeated string topics = 4;
}
```

### 6.2 MD：`mdctl.v1`

```proto
// file: mftiko/mdctl/v1/mdctl.proto
syntax = "proto3";

package mftiko.mdctl.v1;

import "mftiko/common/v1/common.proto";

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1;mdctlv1";

message InitPayload {
  string venue = 1;
  string route = 2; // 由 adapter 從解析後的商品推得（D403、D202）
  uint32 n = 3;
  string md_instance = 4;
  reserved 5; // 原本的 tape_url：tape 在 worker 記憶體（D402）
  reserved "tape_url";
  // 實際連線的 URL。host 由 MD instance 的設定選：預設，或交易所提供的替代 endpoint（D403）。換 URL 要換 incarnation。
  string url = 6;
}

// Start 沒有 payload：連上 websocket，等第一份 Desired。
message StartPayload {}

message Command {
  oneof cmd {
    Desired desired = 1;
  }
}

// 這條連線上該有的 atom，完整清單。
message Desired {
  mftiko.common.v1.Version version = 1;
  repeated AtomAssignment atoms = 2;
}

message AtomAssignment {
  string atom_id = 1;
  repeated string feeds = 2; // 這個 atom 要發佈的 feed（atom → 事件 1:N）
  bool record = 3;           // 錄 tape；MD agent 依 topic 推導（D402）
}

message Report {
  mftiko.common.v1.Version applied = 1;
  uint64 conn_epoch = 2; // 每次重連 +1
  ConnState state = 3;
  string reason = 4;
  repeated AtomStatus atoms = 5;
  double msg_rate = 6;
}

enum ConnState {
  CONN_STATE_UNSPECIFIED = 0;
  CONN_STATE_CONNECTING = 1;
  CONN_STATE_LIVE = 2;
  CONN_STATE_DOWN = 3;
}

message AtomStatus {
  string atom_id = 1;
  AtomState state = 2;
  int64 first_msg_at_ns = 3;
  int64 last_msg_at_ns = 4;
  string error = 5;
}

enum AtomState {
  ATOM_STATE_UNSPECIFIED = 0;
  ATOM_STATE_PENDING = 1;
  ATOM_STATE_SUBSCRIBED = 2; // 交易所已 ack
  ATOM_STATE_ERROR = 3;
}
```

conn worker 只照 `Desired` 對交易所做 subscribe／unsubscribe 的 diff：observed 以 `conn_epoch` 為鍵，舊 epoch 晚到的 ack 丟棄；重連後 observed 歸零，下一輪補齊。refcount 與 linger 都在 MD agent（§3.4）。

### 6.3 TD：`tdctl.v1`

```proto
// file: mftiko/tdctl/v1/tdctl.proto
syntax = "proto3";

package mftiko.tdctl.v1;

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/tdctl/v1;tdctlv1";

message InitPayload {
  int64 api_id = 1;
  string kind = 2; // 帳號種類（D301），同 state.v1.Account.kind
  string td_instance = 3;
  uint32 cancel_on_disconnect_s = 4;
  Credentials credentials = 5; // 只經 socketpair 送進去
}

message Credentials {
  string key_type = 1; // hmac | ed25519
  string key = 2;
  string secret = 3;
  string passphrase = 4;
}

// Start：建 HTTP 連線池、連私有 websocket、recon。全部完成才回 Done(START, ok)。
message StartPayload {}

message Command {
  oneof cmd {
    Drain drain = 1;
  }
}

// drain-replace 的第一步：新單以可重試的錯誤拒絕，等在途 ack 收齊。
message Drain {
  uint32 deadline_ms = 1;
}

message Report {
  AccountState state = 1;
  string reason = 2;
  int64 reconciled_at_ns = 3;
  uint32 open_orders = 4;
  uint32 inflight = 5;
  bool draining = 6;
  // 這個 incarnation 下過單的商品 → 第一次 Submit 的時間（DECISIONS D601）。TD agent 據此建 backfill 的 scope。
  // 新商品的第一筆單：worker 先送出含它的 Report，再送單。
  map<string, int64> first_submit_ns = 7;
}

enum AccountState {
  ACCOUNT_STATE_UNSPECIFIED = 0;
  ACCOUNT_STATE_READY = 1;
  ACCOUNT_STATE_DEGRADED = 2; // 私有 websocket 斷線：新單以 1xx 拒絕，撤單照常（D603）
  ACCOUNT_STATE_UNAVAILABLE = 3;
}
```

## 7. C6：資料面（NATS）

| subject | 型態 | 發佈者／服務者 | 訂閱者／呼叫者 |
|---|---|---|---|
| `md.f.{inst}.{topic}.{ticker}` | pub | MD conn worker | session worker |
| `md.view.{inst}.{topic}.{ticker}` | req | 持有該 atom 的 MD conn worker | session worker |
| `md.fetch` | req | md.fetch 服務（queue group） | session worker |
| `td.order.{api_id}` | req | TD account worker | session worker |
| `td.view.{api_id}` | req | TD account worker | session worker |
| `td.evt.{api_id}` | pub | TD account worker | session worker、API、TD agent（落庫） |
| `log.{plane}.{id}` | pub | 全部 | API |
| `sym` | req | SYM（queue group） | session worker（經 SDK）、MD agent、TD agent、account worker、md.fetch（§7.3）。API 不查 |

- 資料面不用 NATS header：subject 決定訊息種類，payload 是該種類的 wrapper。`md.f` 的 payload 前面多一段固定長度的 envelope（§7.1），給 SDK 的 prehook 不解碼就能讀（D803）。
- 沒有給策略看的可用性廣播（D801）。存活由 controller 依 ack／nack 處理（§3.5）。
- `md.f` 的 subject 直接是 feed：feed 對 atom 1:1（D401），所以 atom 不出現在資料面上。UniversalTicker 不含 `.`，可以直接當 token。
- **行情的 subject 由 MD agent 給，SDK 不自己組（D106）。** feed 用 `FeedBinding.subject`；selector 成員用 `SelectorBinding.subject_prefix` 加上 `.{topic}.{ticker}`；`md.view` 是把開頭的 `md.f.` 換成 `md.view.`。之後要讓新舊版 conn worker 並存，MD agent 在 subject 裡加版號即可，SDK 不用改。
- **`md.view` 由持有 atom 的 conn worker 服務（D402）**，它在 atom 加入時訂閱、移除時退訂。沒有 responder 就是沒有 worker 持有這個 atom。
- **`md.fetch` 是無狀態服務（D402）**，和 SYM 一樣不歸任何 agent 管。conn worker 自己要快照時在進程內呼叫 REST reader，不經過 `md.fetch`。
- **TD 壞掉時下單失敗並帶原因（D801）：** `td.order.{api_id}` 沒有 responder（account worker 不在）或逾時，SDK 轉成一筆 `OrderReject`，code 落在 1xx（TD 本地拒絕）。account worker 自己拒絕的（drain 中、帳號不可用）也走同一條路。
- **TD 降級（D603）。** 私有 websocket 斷線期間，`Submit` 以 1xx 拒絕（`account_degraded`），`Cancel`、`BatchCancel` 照常走 HTTP。重連並 recon 之後，在 `td.evt` 發一則 `Recon`，之後才恢復接受新單。account worker 換 incarnation 之後也發一則 `Recon`。TD 的 ack 維持 `READY`。
- **第一份 `Recon` 由 SDK 自己取得（D603）：** `OnStart` 之後，SDK 以 `td.view`（ledger 加上依 algo 篩選的 OMS）組出一份，當作第一次 `OnRecon` 交給策略。
- **session worker 的 NATS 權限（D906）。** 每個 session 一份 user JWT，由 STS agent 依 spec 簽發：
  - publish：`td.order.{綁定的 api_id}`、`td.view.{綁定的 api_id}`、`md.view.{綁定的 MD instance}.>`、`md.fetch`、`sym`、`log.sts.{session_id}`。
  - subscribe：`md.f.{綁定的 MD instance}.>`、`td.evt.{綁定的 api_id}`、`_INBOX_{session_id}.>`。
  - 授權只在連線時做一次，訊息不帶 token。對沒綁定的帳號發 `td.order`，broker 直接拒絕。
- **帳號只交易它的種類可交易的 venue（D301）。** `Submit` 的 ticker 所屬的 venue 不在帳號種類的清單裡，account worker 以 1xx 拒絕（`venue_not_tradable`），不送出。
- **數量與價格的單位（D203）。** `qty`、`filled_qty`、`Fill.qty`、`Position.size` 的單位是 `contract_value_ccy`（qty = 張數 × `contract_size`）：linear 與期權是 base，inverse 是 quote，不是張數。`quote_qty` 的單位是 quote。`price`、`avg_price`、持倉的 `entry_price` 與 `mark_price`，以及 `price_tick`，用 `price_ccy`：現貨與期貨（含 inverse）是 quote，幣本位期權是 base，U 本位期權是 quote（穩定幣）。`unrealized_pnl` 用 `settlement_asset`（契約欄位 `settle`），是損益結算幣，不是保證金幣。不是 `TRADABLE` 的商品，交易路徑拒單。見 §7.3。
- **策略只能動自己的單（D602）。** TD 檢查 `Submit`、`Cancel`、`BatchCancel` 帶的 `algo_id` 與 `client_order_id` 的 algo 欄位一致，不符就以 1xx 拒絕。沒有 `client_order_id` 的單（人工下的）不屬於任何策略。SDK 只允許使用 spec 裡宣告給該帳號的 algo_id；這是防呆，不是防惡意。平台不替策略撤單，STS agent 沒有 `td.order` 的權限。
- **TD agent 訂閱 `td.evt` 落庫（D601）。** 所有 `Fill`，以及終態且 `filled_qty > 0` 的 `Order`，以 provisional 進 agent 的 queue。走 core NATS：漏收的由歷史補正讀回，不用 JetStream。

### 7.1 MD

**`md.f` 的訊息 = envelope ＋ protobuf `FeedEvent`（D803）。** envelope 是固定位置的 little-endian 欄位，每個欄位對齊 8 bytes，可以直接 unsafe 讀：

| offset | 長度 | 欄位 |
|---|---|---|
| 0 | 1 | envelope 版本，目前是 `1` |
| 1 | 1 | envelope 長度（bytes），v1 是 `32`。讀取端一律跳到這個 offset 讀 payload，之後加欄位不會破壞舊的讀取端 |
| 2 | 6 | 保留，填 0 |
| 8 | 8 | `md_ts_ns`（int64）：conn worker 發佈的時間，MD 主機的時鐘 |
| 16 | 8 | `conn_epoch`（uint64） |
| 24 | 8 | `seq`（uint64）：以 `(feed, conn_epoch)` 起算，同一條連線上連續；重連後從 1 重新起算 |
| 32 | — | protobuf `FeedEvent` |

- SDK 的 prehook 只讀 envelope：`now − md_ts_ns > recv_window_ms` 就丟，不解碼 payload。
- 交給策略的事件帶 envelope 的三個欄位，加上 `Meta.exch_ts_ns`。
- tape（§`md.view`）存的也是這整段 bytes。

```proto
// file: mftiko/md/v1/md.proto
syntax = "proto3";

package mftiko.md.v1;

import "mftiko/common/v1/common.proto";

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1;mdv1";

// seq、conn_epoch、md_ts_ns 移到 envelope（上表）。
message Meta {
  reserved 1, 2, 4;
  reserved "seq", "conn_epoch", "md_ts_ns";
  int64 exch_ts_ns = 3;
}

message FeedEvent {
  oneof event {
    BestQuote best_quote = 1;
    Trade trade = 2;
    AggTrade agg_trade = 3;
    OrderBook order_book = 4;
    Kline kline = 5;
    Ticker ticker = 6;
    Liquidation liquidation = 7;
    FundingRate funding_rate = 8;
    OpenInterest open_interest = 9;
    Greeks greeks = 10;
    MarkPrice mark_price = 11;
    OrderBookDelta order_book_delta = 12;
    FeedEnd feed_end = 100;
  }
}

message BestQuote {
  Meta meta = 1;
  string bid = 2;
  string bid_qty = 3;
  string ask = 4;
  string ask_qty = 5;
}

message Trade {
  Meta meta = 1;
  string trade_id = 2;
  string price = 3;
  string qty = 4;
  mftiko.common.v1.Side side = 5; // taker
}

message AggTrade {
  Meta meta = 1;
  string trade_id = 2;
  string price = 3;
  string qty = 4;
  mftiko.common.v1.Side side = 5;
  string first_trade_id = 6;
  string last_trade_id = 7;
  uint32 match_count = 8;
}

message Level {
  string price = 1;
  string qty = 2;
}

// orderbook：conn worker 本地全量 book 的 top-N，top-N 有變才推（D402）。
message OrderBook {
  Meta meta = 1;
  repeated Level bids = 2;
  repeated Level asks = 3;
  uint32 depth = 4;
}

// orderbook_delta：本地全量 book 的增量（D402）。qty 為 "0" 表示刪除該價位。
// reset 為 true 時，bids／asks 是完整的 book，取代本地狀態（conn worker 對交易所重新同步之後）。
// 晚加入的接法見 read.proto 的 md.view。
message OrderBookDelta {
  Meta meta = 1;
  repeated Level bids = 2;
  repeated Level asks = 3;
  bool reset = 4;
}

message Kline {
  Meta meta = 1;
  string interval = 2;
  int64 open_time_ns = 3;
  string open = 4;
  string high = 5;
  string low = 6;
  string close = 7;
  string volume = 8;
  optional string quote_volume = 9;
  bool closed = 10;
}

// 只有 24h 統計（D401）；venue 不推的欄位不填。
message Ticker {
  Meta meta = 1;
  optional string last = 2;
  optional string open_24h = 3;
  optional string high_24h = 4;
  optional string low_24h = 5;
  optional string volume_24h = 6;
  optional string quote_volume_24h = 7;
  optional string change_pct_24h = 8;
}

message Liquidation {
  Meta meta = 1;
  mftiko.common.v1.Side side = 2; // 被平倉部位的方向
  string price = 3;
  string qty = 4;
}

message FundingRate {
  Meta meta = 1;
  string rate = 2;
  int64 next_funding_ns = 3;
  optional uint32 interval_s = 4;
}

message OpenInterest {
  Meta meta = 1;
  string open_interest = 2;
  optional string open_interest_value = 3;
}

message Greeks {
  Meta meta = 1;
  optional string delta = 2;
  optional string gamma = 3;
  optional string vega = 4;
  optional string theta = 5;
  optional string rho = 6;
  optional string mark_iv = 7;
  optional string bid_iv = 8;
  optional string ask_iv = 9;
  optional string mark_price = 10;
  optional string underlying_price = 11;
}

message MarkPrice {
  Meta meta = 1;
  string mark_price = 2;
  optional string index_price = 3;
}

message FeedEnd {
  string reason = 1; // expired | delisted
  int64 at_ns = 2;
}
```

**行情讀取（D402）。** `md.view` 問持有 atom 的 conn worker，回答它記憶體裡的狀態；`md.fetch` 問無狀態服務，轉發交易所的公開 REST。

- **tape 的接法：** 先訂 `md.f`，記下第一筆 live 的 `(conn_epoch, seq)`；再從 `md.view` 由新到舊讀 tape，丟掉不小於它的部分。
- **`orderbook_delta` 的接法（SDK 的可選小工具）：** 先訂 `md.f` 並緩衝；向 `md.view` 要 book，得到 seq S；丟掉緩衝裡 seq ≤ S 的增量，之後依序套用。seq 跳號（包括被 prehook 丟掉，D803）或 `conn_epoch` 改變時，重新要一次。收到 `reset` 就整本取代。
- **K 線的接法：** `md.fetch` 拿最近 N 根已收盤的，再接 live 的 kline feed，以 `open_time_ns` 對齊。

```proto
// file: mftiko/md/v1/read.proto
syntax = "proto3";

package mftiko.md.v1;

import "mftiko/md/v1/md.proto";

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1;mdv1";

// ---------- md.view.{inst}.{topic}.{ticker} ----------
// subject 最後一段今天是 UniversalTicker。NATS topic 用人讀的名字，不用 UID；確切拼法還沒定（D203、§10 P14）。
// 由持有該 atom 的 conn worker 回答。回什麼由 subject 的 topic 決定：
// trade、aggtrade、liquidation 回 tape；orderbook_delta 回 book。其他 topic 回 error = "unsupported"。

message ViewRequest {
  uint32 limit = 1;            // 只用於 tape：這一頁最多幾筆
  optional Cursor before = 2;  // 只用於 tape：第一頁不填
}

// 以 (conn_epoch, seq) 的字典序比較。
message Cursor {
  uint64 conn_epoch = 1;
  uint64 seq = 2;
}

message ViewReply {
  string error = 1; // 空字串 = 成功；unsupported | not_held | invalid
  oneof result {
    TapePage tape = 10;
    BookSnapshot book = 11;
  }
}

message TapePage {
  // 由新到舊。每則是 md.f 上發佈過的原始訊息（envelope ＋ FeedEvent），worker 直接從 ring 拷貝，不重新編碼。
  repeated bytes events = 1;
  bool more = 2;
  optional Cursor next = 3;      // 下一頁的 before
  int64 continuous_since_ns = 4; // 從這個時間起沒有 gap
  repeated Gap gaps = 5;         // 這一頁涵蓋範圍內的 gap
}

message Gap {
  int64 start_ns = 1;
  int64 end_ns = 2;
}

// orderbook_delta 這個 feed 在 (conn_epoch, seq) 當下的完整 book。
message BookSnapshot {
  Meta meta = 1;
  repeated Level bids = 2;
  repeated Level asks = 3;
  uint64 conn_epoch = 4;
  uint64 seq = 5;
  int64 md_ts_ns = 6;
}

// ---------- md.fetch ----------
// 無狀態服務，轉發交易所的公開 REST。queue group，任何 replica 都能回答。

message FetchRequest {
  oneof req {
    FetchKlines klines = 1;
    FetchOpenInterest open_interest = 2; // 交易所不推送、只能查詢的值（D401）
  }
}

message FetchKlines {
  string ticker = 1;   // 今天是 UniversalTicker。改成 UID 是後續（D203）。subject 用人讀的名字，拼法還沒定（§10 P14）
  string interval = 2; // 正規寫法：1m、4h、1mo
  uint32 limit = 3;    // 最近 N 根已收盤的；超過交易所單次上限時由服務分頁
}

message FetchOpenInterest {
  string ticker = 1;
}

message FetchReply {
  string error = 1; // 空字串 = 成功；invalid | unsupported | rate_limited | venue_error
  string reason = 2;
  oneof result {
    Klines klines = 10;
    OpenInterest open_interest = 11;
  }
}

message Klines {
  repeated Kline klines = 1; // 由舊到新，只有已收盤的
}
```

### 7.2 TD

reject code 照抄 mftik：`0` 無；`100–199` TD 本地拒絕，什麼都沒送出；`200–299` 已對照的交易所拒絕；其他值是交易所原生代碼。

```proto
// file: mftiko/td/v1/td.proto
syntax = "proto3";

package mftiko.td.v1;

import "mftiko/common/v1/common.proto";

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1;tdv1";

enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;
  ORDER_STATUS_PENDING_NEW = 1;
  ORDER_STATUS_NEW = 2;
  ORDER_STATUS_PARTIALLY_FILLED = 3;
  ORDER_STATUS_FILLED = 4;
  ORDER_STATUS_PENDING_CANCEL = 5;
  ORDER_STATUS_CANCELLED = 6;
  ORDER_STATUS_REJECTED = 7;
  ORDER_STATUS_EXPIRED = 8;
  ORDER_STATUS_UNKNOWN = 9;
}

// ---------- td.order.{api_id} ----------

message OrderRequest {
  oneof req {
    Submit submit = 1;
    Cancel cancel = 2;
    BatchCancel batch_cancel = 4;
  }
  reserved 3; // 原本的 cancel_session：平台不替策略撤單（D602）
  reserved "cancel_session";
}

message Submit {
  string session_id = 1;
  string ticker = 2; // 今天是 UniversalTicker。平面之間改傳 UID 是後續（D203），這一版不改 td 的欄位
  mftiko.common.v1.Side side = 3;
  mftiko.common.v1.OrderType order_type = 4;
  optional string qty = 5; // 單位是 contract_value_ccy（D203）：qty = 張數 × contract_size。現貨、linear、期權是 base，inverse 是 quote，不是張數
  optional string quote_qty = 6; // quote 資產的金額，單位是 quote，不是 contract_value_ccy（D203）
  optional string price = 7; // 單位是 price_ccy（D203）：現貨與期貨（含 inverse）是 quote；幣本位期權的權利金是 base；U 本位期權的權利金是 quote（穩定幣）
  mftiko.common.v1.TimeInForce tif = 8;
  bool reduce_only = 9;
  string client_order_id = 10;
  map<string, string> params = 11;
  uint32 algo_id = 12; // 必須等於 client_order_id 的 algo 欄位（D602）
}

message Cancel {
  string session_id = 1;
  string ticker = 2;
  string client_order_id = 3;
  string order_id = 4;
  uint32 algo_id = 5; // 目標單的 client_order_id 的 algo 欄位必須等於它；沒有 client_order_id 的單一律拒絕（D602）
}

// 撤掉這個帳號上 client_order_id 的 algo 欄位等於 algo_id 的所有掛單（D602）。
// 只看 algo 欄位，不分 session、incarnation：上一個進程或上一個 session 留下的也算。
message BatchCancel {
  uint32 algo_id = 1;
  repeated string tickers = 2; // 空 = 全部商品
  uint32 timeout_ms = 3;
}

message OrderReply {
  oneof reply {
    OrderAck ack = 1;
    mftiko.common.v1.Error error = 3;
    BatchCancelResult batch_cancel = 4;
  }
  reserved 2; // 原本的 cancel_session
  reserved "cancel_session";
}

message OrderAck {
  string client_order_id = 1;
  bool accepted = 2; // TD 收下了請求，不代表交易所接受
  int32 code = 3;
  string reason = 4;
}

message BatchCancelResult {
  uint32 algo_id = 1;
  bool confirmed = 2;               // 全部撤掉並確認
  repeated string cancelled = 3;    // client_order_id
  repeated string unconfirmed = 4;  // 逾時前沒確認的 client_order_id
}

// ---------- td.view.{api_id} ----------

message ViewRequest {
  oneof req {
    OmsViewRequest oms = 1;
    LedgerViewRequest ledger = 2;
    EnsureLeverageRequest leverage = 3;
  }
}

message OmsViewRequest {
  string session_id = 1;
  bool settled = 2;
  uint32 algo_id = 3; // 非 0 時改以 algo 篩選，涵蓋上一個進程、上一個 session 留下的單（D602）
}

message LedgerViewRequest {}

message EnsureLeverageRequest {
  string ticker = 1;
}

message ViewReply {
  oneof reply {
    OmsView oms = 1;
    LedgerView ledger = 2;
    LeverageInfo leverage = 3;
    mftiko.common.v1.Error error = 4;
  }
}

message OmsView {
  repeated Order orders = 1;
  uint32 incarnation = 2;
  int64 as_of_ns = 3;
}

message LedgerView {
  repeated Balance balances = 1;
  repeated Position positions = 2;
  uint32 incarnation = 3;
  int64 as_of_ns = 4;
}

message LeverageInfo {
  string ticker = 1;
  string leverage = 2;
}

// ---------- td.evt.{api_id} ----------

message AccountEvent {
  uint64 seq = 1; // 每個 incarnation 從 1 起算
  mftiko.common.v1.WorkerRef source = 2;
  oneof event {
    Order order_update = 10;
    Fill fill = 11;
    OrderReject order_reject = 12;
    CancelReject cancel_reject = 13;
    BalanceUpdate balance_update = 14;
    PositionUpdate position_update = 15;
    Recon recon = 17;
  }
  reserved 16; // 原本的 account_reset：由 Recon 取代（D603）
  reserved "account_reset";
}

message Order {
  string ticker = 1;
  string order_id = 2;
  string client_order_id = 3;
  mftiko.common.v1.Side side = 4;
  mftiko.common.v1.OrderType order_type = 5;
  OrderStatus status = 6;
  string qty = 7; // 同 Submit.qty，單位是 contract_value_ccy（D203）
  optional string quote_qty = 8; // 同 Submit.quote_qty（D203）
  optional string price = 9; // 同 Submit.price，單位是 price_ccy（D203）
  string filled_qty = 10; // 已成交數量，單位同 qty（contract_value_ccy，D203）
  optional string avg_price = 11; // 同 Submit.price，單位是 price_ccy（D203）
  string reject_reason = 12;
  int64 exch_ts_ns = 13;
  int64 td_ts_ns = 14;
}

message Fill {
  string ticker = 1;
  // 交易所的 trade id。WS 與 REST 必須給同一個值：落庫以 (api_id, fill_id) 去重（D601）。
  string fill_id = 2;
  string order_id = 3;
  string client_order_id = 4;
  mftiko.common.v1.Side side = 5;
  string price = 6; // 同 Submit.price，單位是 price_ccy（D203）
  string qty = 7; // 成交數量，單位同 Submit.qty（contract_value_ccy，D203）
  string fee = 8;
  string fee_asset = 9;
  string liquidity = 10;
  int64 exch_ts_ns = 11;
}

message OrderReject {
  string client_order_id = 1;
  string order_id = 2;
  string ticker = 3;
  int32 code = 4;
  string reason = 5;
}

message CancelReject {
  string client_order_id = 1;
  string order_id = 2;
  int32 code = 3;
  string reason = 4;
}

message Balance {
  string asset = 1;
  string free = 2;
  string locked = 3;
  string prelock = 4;
}

message Position {
  string ticker = 1;
  string size = 2; // 持倉數量，單位同 Submit.qty（contract_value_ccy，D203）
  optional string entry_price = 3; // 同 Submit.price，單位是 price_ccy（D203）
  optional string mark_price = 4; // 同 Submit.price，單位是 price_ccy（D203）
  optional string unrealized_pnl = 5; // 損益結算幣 settlement_asset（欄位 settle），不是保證金幣（D203）
  optional string leverage = 6;
}

message BalanceUpdate {
  repeated Balance balances = 1;
}

message PositionUpdate {
  repeated Position positions = 1;
}

// 帳號狀態的整份快照（DECISIONS D603）。SDK 把它交給策略的 OnRecon：
// open_orders 先篩成自己宣告的 algo_id，positions、balances 原樣給。
// 和其他事件共用 seq；策略整份換掉自己的狀態，之後的事件照常疊上去。
message Recon {
  uint32 incarnation = 1;
  ReconReason reason = 2;
  repeated Order open_orders = 3;
  repeated Position positions = 4;
  repeated Balance balances = 5;
  int64 as_of_ns = 6;
}

enum ReconReason {
  RECON_REASON_UNSPECIFIED = 0;
  RECON_REASON_RECONNECT = 1;   // 私有 websocket 重連並 recon 之後
  RECON_REASON_INCARNATION = 2; // account worker 換了 incarnation
}
```

### 7.3 SYM

照 mftik 的 symbol plane（DECISIONS D201、D202）。SYM 是商品資料的權威：交易所怎麼寫一個商品、它的交易限制是什麼。翻譯是查表，不是字串轉換。

- **不是 agent。** 每個 region 部署，NATS queue group，任何 replica 都能回答；沒有 lease、fencing。只有 SYM 寫。listing 預定分兩張表，看狀態還會不會變（D203，enum 仍暫定）：活躍表是 `PRELISTING`、`TRADABLE`、`HALTED`、`SETTLING`、`UNKNOWN`，Resolver 與熱路徑只讀它，只有 `TRADABLE` 可交易；終結表是 `EXPIRED`、`DELISTED`，狀態不再變，給歷史查詢（舊成交、持倉、依 UID 或 ticker 反查），不再刷新，可以歸檔。兩張表的主鍵都是 UID，合在一起唯一。UID 是 SYM 分配的隨機整數，永不重用，不是單調序號，跟列一起持久化。列進了終結狀態就從活躍表搬到終結表，UniversalTicker 與 UID 都不變。`symbol_filter` 也分活躍表與終結表，跟列一起搬。同一個 ticker 下架再上市是新的 UID。E2-03 的 `migrations/0002_symbol` 已拆成 `symbol_active` 與 `symbol_terminal`，主鍵是 uid；`symbol_filter_active`、`symbol_filter_terminal` 跟列一起搬。活躍表的 `venue`、`category` 是從 `universal_ticker` 生成的欄位，`UNIQUE (venue, category, exch_ticker)` 與 `universal_ticker` 唯一只在活躍表。
- **刷新的單位是 `(venue, category)`。** 每個單位一個來源（adapter 的 `PublicREST.Listing`），各自刷新，一個 venue 掛了不影響其他。這次沒出現的列標成 `DELISTED`，只在同一個 `(venue, category)` 裡判斷：現貨的刷新不會讓 perp 下架。`EXPIRED` 或 `DELISTED` 都搬進終結表，之後不再刷新。ticker 字串與 UID 都不變。分頁的 listing 要讀完才算數，讀一半就當失敗。預設每小時一次，也可以 `RefreshRequest`。
- **使用者是 STS、MD、TD，API 不查（D202）。**
- **啟動時載入活躍表，之後跟著變更重載（D203）。** 進程啟動時，以 `ListRequest` 把用得到的每個 `(venue, category)` 的未終結列分頁讀進記憶體，每頁最多 500 筆，壓在 NATS 的 1 MiB 以下。分頁是 offset＋limit。翻頁之間列可能搬表或新上市，跨頁不一致可以接受（§10 P13）。Resolver 與熱路徑只查這份記憶體，不打 SYM，也不讀終結表。歷史查詢（舊成交、持倉、依 UID 反查，以及 MD 的到期監看）才讀終結表。SYM 刷新出差異時發變更通知，帶 revision；收到的進程重載那個 `(venue, category)` 的活躍列。通知的 subject 與訊息見 §10。`GetRequest` 是 spec 載入時把 ticker 解成 UID 的那一次；`GetByUidRequest` 是之後各平面用 UID 拿欄位；`ReverseRequest` 把交易所寫法換回 listing（回覆帶 UID）。三者都留著，給載入、熱路徑找不到、或要查終結列時用。
- **誰載入什麼（D202）：**
  - session worker：SDK 在啟動時載入 spec 裡用到的 venue，讓策略拿到 tick、qty step 這類 filter 做下單取整。權限見 §7 的 `sym`。
  - MD agent：載入 instance 服務的 venue。需求項目進來時在表裡解析 feed；selector 的成員從表裡挑。
  - account worker：載入帳號種類可交易的 venue。私有事件帶回交易所的寫法，反查得到 UID。
  - TD agent 的 backfill：同 account worker；REST 拿回的成交同樣反查成 UID。
  - md.fetch：載入它服務的 venue。
  - conn worker 不解析：它依 `AtomAssignment` 的 feed 在事件蓋上身分。今天蓋的是 ticker；事件改蓋 UID 是後續。`md.f` 的 subject 用人讀的名字，不用 UID；拼法還沒定（§10 P14），這一版仍是 ticker。

**幣與數量（D203）。** `contract_value_ccy`、`price_ccy`、`settlement_asset` 由 adapter 從交易所的原生欄位填（例如 OKX 的 `ctValCcy`、Deribit 期權的 `quote_currency`），下游不推導。不沿用 mftik 的 `reservations.is_linear_margin`，也不為 Deribit 開特例。這一節的三個欄位還沒進 `SymbolInfo`；結算欄位仍叫 `settle`。

`contract_value_ccy` 是 qty 的單位。qty = 張數 × `contract_size`，以它計：linear 是 base，inverse 是 quote，期權是 base（標的）。inverse 的 qty 是 quote，不是張數。平台上的 qty 已經是這個乘積。現貨沒有張數，qty 是 base。

`price_ccy` 是 price、`price_tick`、權利金的單位。現貨與期貨（含 inverse）是 quote；幣本位期權是 base；U 本位期權是 quote（穩定幣）。

`settlement_asset` 是損益結算幣，不是保證金幣。mftik 在這裡語意不一致：Binance 的 listing 填 `marginAsset`，Bitget 直接寫成 quote，這裡不沿用。`unrealized_pnl` 以它計。

幣本位／U 本位只給顯示與分組：入庫的合約與期權，`settlement_asset == base` 是幣本位，否則是 U 本位。交易路徑不依賴這個標籤。

| 類 | contract_value_ccy | price_ccy | settlement_asset | qty |
|---|---|---|---|---|
| linear | base | quote | quote | base（張數 × `contract_size`） |
| inverse | quote | quote | base | quote（張數 × `contract_size`），不是張數 |
| U 本位期權 | base（標的） | quote（穩定幣，權利金） | quote | base（張數 × `contract_size`） |
| 幣本位期權 | base（標的） | base（權利金） | base | base（張數 × `contract_size`） |
| quanto | 不入庫 | 不入庫 | 既不是 base 也不是 quote | 不入庫。需求 `REJECTED` |

資產代碼一律大寫。別名表（例如 `XBT` → `BTC`）只在 SYM 套：adapter 傳原生代碼，不自行改名。各 venue 的別名在做 venue integration 時補。順序固定：先大寫並套別名，再做 quanto 判斷，再推 ticker。`USD`、`USDC`、`USDT` 不合併，不進別名表。報價 USD、結算 USDC 是 quanto，一期不入庫，需求 `REJECTED`，這是刻意的。

strike 先依 `spell_strike` 正規化，再推 ticker。整數去掉尾端的小數零，`6400.0` 與 `6400` 是同一個 ticker；有小數的把小數點寫成 `D`（§2）。

`qty_step`、`min_qty`、`max_qty` 與 qty 同一個單位（`contract_value_ccy`）。`price_tick` 用 `price_ccy`。

quanto（`settlement_asset` 既不是 base 也不是 quote）不入庫。解析不到就是 `REJECTED`（§3.4）。

**狀態（暫定）。** `active` 會換成 status enum：`PRELISTING`、`TRADABLE`、`HALTED`、`SETTLING`、`EXPIRED`、`DELISTED`、`UNKNOWN`。完整列舉還沒定案。交易路徑只接受 `TRADABLE`，其他狀態拒單。之後新增 enum 值不會讓原本不可交易的變成可交易。交易所的原生狀態對不上，記成 `UNKNOWN`，不可交易；各 venue 的對照在做 venue integration 時補。enum 定案前契約仍是 `bool active`。

分表跟著這個暫定的切法：活躍表是 `PRELISTING`、`TRADABLE`、`HALTED`、`SETTLING`、`UNKNOWN`；終結表是 `EXPIRED`、`DELISTED`。Resolver 與熱路徑只讀活躍表。終結列狀態不再變，不再刷新，可以歸檔；搬過去時 ticker 字串與 UID 都不變。表已拆（E2-03）。

**UID（D203）。** UniversalTicker 只出現在人寫的設定與 spec。載入 spec 時，`GetRequest` 把 feed key 裡的 ticker 解成 UID；這是唯一的翻譯邊界。之後 STS、TD、MD 傳 UID，要 base、filter、狀態時用 `GetByUidRequest` 查，不傳 ticker。UID 由 SYM 分配，是隨機整數：唯一、永不重用，不是單調序號，也不是 Postgres sequence。它是兩張表的主鍵，跨表唯一，跟列一起持久化，所以重啟與重載都不改它。搬到終結表不換 UID。同一個 ticker 下架再上市拿新的 UID，舊資料仍指著舊的。碰撞時 SYM 重試。`symbol_filter` 也分活躍表與終結表，終結時跟列一起搬。`List` 以 offset＋limit 分頁，跨頁不一致可以接受。selector 沒有 UID；`Universe.current` 是 selector，尚未定，不在這一條。契約型別是 `uint32`，範圍 `1..4294967295`（`1..2^32-1`），0 非法（§10 P13）。Postgres 欄位是 `bigint`，`CHECK (uid BETWEEN 1 AND 4294967295)`。E2-03 已落表並抽號。`sym` 這一節仍不提供服務。NATS subject 用人讀的名字，不用 UID；確切拼法見 §10 P14，這一版的 `md.f` 仍是 `md.f.{inst}.{topic}.{ticker}`。策略 hook 看 ticker 還是 UID 見 §10 P15。`td.order`、需求項目、selector 成員這一版仍是 ticker 字串。

```proto
// file: mftiko/sym/v1/sym.proto
syntax = "proto3";

package mftiko.sym.v1;

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1;symv1";

// ---------- sym ----------
// Served by SYM (queue group, any replica answers). Read-only except Refresh.

message Request {
  oneof req {
    GetRequest get = 1;
    ListRequest list = 2;
    ReverseRequest reverse = 3;
    RefreshRequest refresh = 4;
    GetByUidRequest get_by_uid = 5;
  }
}

// Spec 與設定載入的邊界（D203）：人寫的 UniversalTicker 在這裡解成 UID。
// 平面之間不走這條，它們已經持有 UID，用 GetByUidRequest。
// One instrument by exact ticker: a single row, never a venue table.
message GetRequest {
  string universal_ticker = 1;
  bool include_inactive = 2; // D203：預設只讀活躍表。設了才讀終結列（EXPIRED、DELISTED），給歷史查詢，含 MD 的到期監看。Resolver 與熱路徑不設。表是 symbol_active 與 symbol_terminal
}

// 系統內部的查詢（D203）。STS、TD、MD 持有 UID，用它拿這一列的欄位。
// 預設只讀活躍表；include_inactive 才含終結表。0 不是合法 UID。
message GetByUidRequest {
  // 契約型別 uint32，範圍 1..4294967295（1..2^32-1），0 非法（PROTOCOL §10 P13）。
  // Postgres 欄位是 bigint，CHECK (uid BETWEEN 1 AND 4294967295)。
  uint32 uid = 1;
  bool include_inactive = 2; // 同 GetRequest.include_inactive（D203）
}

// One (venue, category) table, paged by offset and limit. A page stays under NATS's 1 MiB
// with every filter. A row may move or appear between pages; that inconsistency is accepted
// (D203, PROTOCOL §10 P13).
message ListRequest {
  string venue = 1;
  string category = 2;
  uint32 limit = 3; // at most 500
  uint32 offset = 4;
  bool include_inactive = 5; // 同 GetRequest.include_inactive（D203）
}

// The venue's spelling -> the listing. Callers keep SymbolInfo.uid (D203). Category is required:
// on a unified-account venue the spelling alone is ambiguous (Bybit's spot and linear "BTCUSDT").
message ReverseRequest {
  string venue = 1;
  string category = 2;
  string exch_ticker = 3;
}

// Re-pull one venue's listings now, or every venue when venue is empty.
message RefreshRequest {
  string venue = 1;
}

message Reply {
  string error = 1; // empty = success; invalid | not_found | unavailable
  string reason = 2;
  oneof result {
    SymbolInfo info = 10;     // get, get_by_uid, reverse
    SymbolList list = 11;     // list
    RefreshResult refresh = 12;
  }
}

message SymbolInfo {
  string universal_ticker = 1; // 給人看的字串。系統內部用 uid（D203）
  string exch_ticker = 2;
  string base = 3;
  string quote = 4;
  optional string contract_size = 5; // 一張的面值。qty = 張數 × 此值，單位是 contract_value_ccy：linear 與期權是 base，inverse 是 quote（D203）
  optional string settle = 6; // 損益結算幣（settlement_asset），不是保證金幣。空 = 現貨。quanto（既不是 base 也不是 quote）一期不入庫（D203）
  int64 expiry_ns = 7; // 0 = does not expire
  optional string strike = 8; // SYM 先依 spell_strike 正規化再推 ticker：6400.0 與 6400 相同（D203）
  string option_type = 9; // C | P | ""
  bool active = 10; // 暫定（D203）。將改成 PRELISTING、TRADABLE、HALTED、SETTLING、EXPIRED、DELISTED、UNKNOWN。只接受 TRADABLE。對不上的是 UNKNOWN，不可交易。之後加 enum 值不會讓原本不可交易的變成可交易。活躍表是 PRELISTING、TRADABLE、HALTED、SETTLING、UNKNOWN；終結表是 EXPIRED、DELISTED，搬過去時 ticker 與 uid 都不變。定案前契約仍是 bool。表已拆成 symbol_active 與 symbol_terminal
  // name -> bound. A key with an absent value: the venue publishes the restriction, no bound.
  // price_tick is price_ccy; qty_step, min_qty, max_qty are contract_value_ccy (D203).
  map<string, Bound> filters = 11;
  int64 updated_at_ns = 12;
  // SYM 分配的隨機整數：跨活躍表與終結表唯一、永不重用，跟列持久化，不是單調序號（D203）。
  // 重啟與重載不改它。下架再上市是新的 UID。碰撞時 SYM 重試。
  // 契約型別 uint32，範圍 1..4294967295（1..2^32-1），0 非法（PROTOCOL §10 P13）。
  // Postgres 欄位是 bigint，CHECK (uid BETWEEN 1 AND 4294967295)。
  // SQL 的兩張表以 uid 為主鍵（migrations/0002_symbol）。
  uint32 uid = 13;
}

message Bound {
  optional string value = 1;
}

message SymbolList {
  repeated SymbolInfo symbols = 1;
  uint32 total = 2;
}

message RefreshResult {
  map<string, uint32> refreshed = 1;   // venue -> instruments seen
  map<string, uint32> deactivated = 2; // venue -> rows marked inactive
  map<string, string> failed = 3;      // venue -> reason, per category
}
```

## 8. artifact（D902、D904）

三種 worker 都從 artifact 起（DECISIONS D902）：策略 artifact（STS 的 session worker），以及這個 repo 的 conn worker、account worker 由 CI 打包的 artifact。它們是同一個格式的 OCI image，只當打包格式用，不跑容器，manifest 的 `kind` 區分種類。資料 artifact（§8.4）放在同一個 registry，用另一種 mediaType。

### 8.1 結構

| OCI 物件 | 內容 |
|---|---|
| image manifest | 各層的 digest；`config.mediaType` 是 `application/vnd.mftiko.worker.config.v1+json`，三種 worker 共用 |
| config blob | 下面的 mftiko manifest（JSON） |
| 層 | Go 策略：一層，`bin/strategy`。其他語言由該語言的 SDK 決定，例如 Python：runtime 層（解譯器與 SDK）、依賴層（`site-packages/`）、策略層（`app/`）。MD、TD：一層，`bin/worker` |

策略的 image digest 就是 `StrategyRef.code`。MD、TD 的 artifact 由 controller 的配置指定給各 instance（§10）。

### 8.2 mftiko manifest

```json
{
  "schema": "mftiko.worker.v1",
  "kind": "strategy",
  "name": "chase",
  "runtime": "go",
  "os": "linux",
  "arch": "amd64",
  "entrypoint": ["bin/strategy"],
  "env": {},
  "sdk": { "lang": "go", "version": "1.0.0" },
  "contracts": ["worker.v1", "stsctl.v1", "md.v1", "td.v1"]
}
```

| 欄位 | 意思 |
|---|---|
| `schema` | manifest 自己的版本 |
| `kind` | `strategy`、`md.conn` 或 `td.account`。agent 只接受自己那一種 |
| `runtime` | `go`，或其他語言 SDK 自訂的名稱（例如 `python`）。只用於顯示與統計，平台不依它改變行為 |
| `os`、`arch` | 目標平台；與主機不符就 `INIT_FAILED` |
| `entrypoint` | 相對於 rootfs 的路徑，不得以 `/` 開頭、不得含 `..`；agent 換算成 `Launch` 的 `path`、`args`（§4.6）。Go 是 `["bin/strategy"]`；Python SDK 例如 `["python/bin/python3", "-m", "<sdk>.run", "<module>:<Class>"]` |
| `env` | 放在平台注入的環境變數之前（§4.6）；不得設 `MFTIKO_` 開頭的變數 |
| `sdk` | SDK 的語言與版本 |
| `contracts` | 支援的 C4、C5、C6 契約 major；STS 在 spawn 前比對 |

### 8.3 主機上的處理

三種 agent 都照這個流程，程式共用（`internal/platform/artifact`，DECISIONS D006）。

1. 取得 image manifest 與 config，檢查 `schema`、`kind`、`os`／`arch`、`contracts`、總大小。不符就不 spawn；策略以 `INIT_FAILED` 結束 session。
2. 缺少的層下載到 `${DATA}/layers/<digest>`，逐層驗證 sha256。
3. 依序解開到 `${DATA}/artifacts/<image digest>/rootfs`，可以用 hardlink 指向層快取。拒絕絕對路徑、`..`、指出 rootfs 的 symlink、裝置檔、setuid／setgid。
4. 依 §4.6 的對應表換算成 `Launch` 交給 shim；shim 以 rootfs 為工作目錄執行 `entrypoint`。
5. GC：策略的 rootfs 沒有被非 terminal session 釘住、也不是目錄裡的最新版，就刪；MD、TD 的 rootfs 沒有進程在用、也不是 controller 指定的版本，就刪；層沒有任何 rootfs 引用，就刪。

### 8.4 資料 artifact（D904）

| OCI 物件 | 內容 |
|---|---|
| manifest | `artifactType` 是 `application/vnd.mftiko.data.v1`；一層 |
| config blob | `application/vnd.mftiko.data.config.v1+json`：`{"schema": "mftiko.data.v1", "source": {"session_id": …, "incarnation": …, "clean": …}}`。運維上傳的沒有 `source` |
| 層 | `application/vnd.mftiko.data.layer.v1.tar+gzip`：檔案樹，路徑相對於 input 的根 |

STS 主機上：

1. **spawn 前：** 每個 `InputDecl` 的層下載到 `${DATA}/layers/<digest>`（與策略 artifact 共用快取），驗 sha256，解到 `${DATA}/sessions/<session_id>/inputs/<name>/`。拒絕的項目與 §8.3 相同。拉不到或驗不過就以 `INIT_FAILED` 結束 session，不 spawn。
2. **volume：** `${DATA}/sessions/<session_id>/volume`，第一個 incarnation spawn 前建立，之後的 incarnation 沿用。STS agent 定期量大小，超過 `Limits.volume_mb` 就以 `FAILED` 結束 session（`volume_over_limit`）。
3. **session terminal 之後：** volume 不是空的，就打包成一層（檔案依路徑排序、mtime 歸零、uid／gid 為 0），推到設定的 data repository，寫入 `SessionStatus.state_out`。推成功之後刪 volume 與 inputs；推不成功就保留並重試。
4. **GC：** 層快取的規則同 §8.3。registry 上資料 artifact 的保留期由 API 管，不在 STS。

## 9. session worker 的實作契約（給其他語言的 SDK）

平台只出 Go SDK，它是參考實作（DECISIONS D802）。其他語言（例如另外實作的 Python SDK）只要做到本節的每一條，就能在平台上跑，平台不需要為它改任何東西。

- **必須**：契約。違反時平台會判定失敗、拒絕請求，或行為與其他策略不一致。
- **建議**：讓行為和 Go SDK 一致，或避開已知的坑。

### 9.1 打包與環境

1. **必須**以 §8 的 OCI 格式打包。manifest 填 `runtime`、`entrypoint`、`sdk`、`contracts`；`contracts` 列出實作的 major（`worker.v1`、`stsctl.v1`、`md.v1`、`td.v1`），STS 在 spawn 前比對。
2. **必須**把解譯器、SDK、第三方套件全部放進 artifact。不得假設主機上有任何語言 runtime 或套件。
3. **必須**從 §5.4 的環境變數取得身分與位址，不得覆蓋 `MFTIKO_*`。憑證不在環境變數裡，在 `Init` 的 payload 裡。
4. **必須**只寫 `MFTIKO_VOLUME_DIR`（與系統暫存目錄）；`MFTIKO_INPUT_DIR` 唯讀（DECISIONS D904）。**建議**寫 volume 一律用原子寫入（暫存檔加 rename）。
5. **必須**讓自己開的子進程跟著 worker 一起結束。shim 對 worker 的 process group 送信號（§4.3）。

### 9.2 C4：fd 3

1. **frame**：4 bytes big-endian 長度加一則 protobuf，上限 1 MiB。上行 `WorkerUp`，下行 `WorkerDown`（§5.1）。拿到 fd 3 先設 close-on-exec：worker 起的子進程不能拿著它，否則 worker 死了 shim 也讀不到 EOF，子進程還能冒充 worker 送訊息。
2. **流程照 §5.2：**
   - 啟動後先送 `Hello`，`contracts` 列出支援的 C5 契約（`stsctl.v1`）。
   - 收到 `Init`：解析 payload → 呼叫策略的 `OnInit` → 回 `Done(INIT, ok)`。
   - 收到 `Start`：套用 `Bindings`、訂閱 subject → `OnStart` → 回 `Done(START, ok)` → `Stage(running)`。
   - `deadline_ms` 內沒完成，算 `INIT_FAILED`。
3. **`Beat` 每 1 秒一則**，必須由不會被 hook 阻塞的執行緒送。做不到時，由 §9.7 的保險接手。
4. **`Report`**：`stsctl.Report` 有變化時送，至少每 5 秒一次；最新一份取代舊的。`QueueStat` 的 `td`、`md` 兩個 buffer 都要回報（§9.5）。
5. **結束的順序必須是**：送 `Exiting(outcome, reason)` → 該跑的才跑 `OnStop`（§5.3）→ 送 `Stopped(on_stop_completed)` → 以 §5.3 的 exit code 結束。
6. **收到 `Stop`**：在 `grace_ms` 內走完上面的流程。逾時 shim 會 SIGTERM，再逾時 SIGKILL。SIGTERM 當成 `Stop` 處理。
7. **fd 3 讀到 EOF（shim 消失）**：必須自行 graceful stop（§4.5 S2）。
8. **`Controller`**：記下目前有沒有 agent。沒有時，對 agent 的 `Relay` 會收到 `Undeliverable`。

### 9.3 C5：`stsctl.v1`

1. `InitPayload`：`session_id`、`incarnation`、策略參數（`params`，平台不解讀）、`feeds`、`accounts`（含 `algo_ids`）、`nats`（§9.4）。
2. `StartPayload.bindings` 與 `Command.bindings`：完整快照，帶 `Version`，**只套用比手上大的**（§6）。selector 換成員時，依新的成員訂閱、退訂。
3. `Command.fail`：以 `FAILED` 結束（跑 `OnStop`，exit 30）。
4. protobuf 的讀取端**必須**容忍未知欄位與未知 enum 值（§1）。

### 9.4 C6：NATS

1. **連線**：用 `InitPayload.nats` 的 `user_jwt` 與 `seed`；reply inbox 前綴設成 `inbox_prefix`。不得使用其他憑證（DECISIONS D906）。
2. **subject 一律用 binding 給的，不自己組**（DECISIONS D106）：
   - feed：`FeedBinding.subject`。
   - selector 成員：`SelectorBinding.subject_prefix` 加上 `.{topic}.{ticker}`。
   - `md.view`：把 feed subject 開頭的 `md.f.` 換成 `md.view.`。
3. **`md.f` 的訊息**：先讀 envelope（§7.1）。payload 一律從 envelope 長度欄位指的 offset 開始讀，容忍比 32 bytes 長的 envelope。
4. **下單**：`Submit`、`Cancel`、`BatchCancel` 必須帶 `session_id` 與 `algo_id`；`algo_id` 只能用 `AccountDecl.algo_ids` 裡宣告給該帳號的（DECISIONS D602）。
5. **`client_order_id`**：照 §2 的版面 `ver(4) | algo(10) | ts_ms(40) | seq(10)`，`ver` 填 1，以十進位字串傳。同一個進程內 `ts_ms` 單調；同一毫秒的 `seq` 用完時，`ts_ms` 往前借一毫秒。
6. **`td.order` 沒有 responder 或逾時**：轉成本地的 `OrderReject`，code 落在 1xx，交給策略（§7、DECISIONS D801）。TD 回的 1xx 拒單（例如 `account_degraded`）原樣交給策略。
7. **商品資料**：要提供 tick、qty step 這類 filter 時，在啟動時以 `sym` 的 `ListRequest` 載入 spec 用到的 venue，收到變更通知時重載（§7.3、§10 P7）。不要每次下單都問 SYM。

### 9.5 交付語意（DECISIONS D803）

1. **單一 dispatcher**：hook 一次一個、依序執行。策略程式碼不需要鎖。跨執行緒只有 `Post` 一個入口。
2. **兩個 buffer：**
   - TD：`td.evt`、下單與撤單的回覆、timer、`Post`、`md.view` 與 `md.fetch` 的回覆。FIFO，不丟。有上限，滿了以 `FAILED` 結束。同一個 timer 積了多次只觸發一次。
   - MD：所有行情。FIFO ring，滿了丟最舊的，跳過正在被讀的那一格。
3. **每跑完一個 hook**：TD 有東西就先取 TD；TD 空了才取 MD。
4. **prehook**：MD 事件交給 hook 之前只看 envelope：`now − md_ts_ns > recv_window_ms` 就丟，不解碼 payload。通過的才解析。
5. **hook 的時間預算：**
   - 超過 `Limits.hook_timeout_s`：送 `Exiting(HUNG)`，不跑 `OnStop`，exit 21。
   - hook 拋出例外：送 `Exiting(CRASHED)`，跑 `OnStop`，exit 20。
6. `QueueStat` 的 `depth`、`dropped`、`max_age_ms` 照實回報。

### 9.6 帳號狀態（DECISIONS D603）

1. `OnStart` 之後，以 `td.view`（ledger，加上依自己 `algo_ids` 篩選的 OMS）組出第一份 `Recon`，交給策略的 `OnRecon`。
2. `td.evt` 的 `Recon`：`open_orders` 篩成自己的 `algo_ids`；`positions`、`balances` 原樣交給 `OnRecon`。
3. 不提供可用性 hook（DECISIONS D801）。策略只會拿到拒單與新的帳號狀態。

### 9.7 平台的保險：不依賴 SDK

- STS agent 以 `Beat` 判斷 worker 是否卡住：
  - `Init`、`Start` 期間：超過該階段的 `deadline_ms` 再加 10 秒沒有 `Beat`。
  - 之後：超過 `hook_timeout_s` 再加 10 秒沒有 `Beat`。
  - 符合任一條件，就經 shim 送 SIGKILL，`ExitInfo.outcome` 記為 `HUNG`，`reason` 是 `no_beat`。
- 這條保險讓「heartbeat 可能被 hook 卡住」的語言也能安全上線，但 hook 卡住期間的狀態不會被即時發現，延遲最多到上面的門檻。

### 9.8 conformance 測試

- 平台提供一套黑箱測試：假的 shim（走 fd 3）、真的 nats-server（驗 JWT 權限），加上假的 MD、TD 對端（發 `md.f`、`td.evt`，回應 `td.order`、`td.view`、`md.view`）。配合一個標準的測試策略，逐條驗本節：流程與 exit code、`Beat`、buffer 的優先順序、prehook 丟棄、buffer 溢出、`client_order_id` 版面、subject 與權限、`Recon`。
- Go SDK 也跑同一套。SDK 通過之後，manifest 的 `contracts` 才能宣稱支援。

### 9.9 給 Python 實作的建議

- `Beat`、NATS 收訊放在獨立的 thread，hook 在主 thread。hook 卡在不放 GIL 的 C 擴充裡時，`Beat` 會停，§9.7 會接手。
- 不要讓 NATS 收訊和 dispatcher 共用同一個 asyncio loop：hook 一卡，收訊也跟著停（mftik 的經驗）。收訊放在獨立 thread，經由 thread-safe 的佇列交給主 thread。
- `multiprocessing` 用 `spawn` 或 `forkserver`：有背景 thread 時 fork 不安全。
- 沒有 `GOMEMLIMIT` 這種機制。`Limits.memory_mb` 由 SDK 自己遵守（例如監控 RSS），或交給主機的資源限制。

## 10. 待定

P1–P5 已依 DECISIONS 定案：P1 見 D803 與 §1，P2 見 D402，P3 見 D903，P4 見 D906，P5 見 D803。v0.24 新增 P6–P10，v0.25 新增 P11，v0.28 新增 P12，v0.34 新增 P13–P15，v0.36 改寫 P13，v0.41 定案 P13 的寬度與範圍：

- **P6 `DEAD` 的確切時機（E4、E6 之前）。** 現在的定義是 worker 一結束、連線一斷就 `DEAD`，而 controller 收到 `DEAD` 就結束 session（§3.5）。MD、TD 會自動重拉 worker，照這個定義，一條 conn worker crash 就會結束這條連線上所有的 session。另一種定義是 provider 自動重拉期間維持 `PENDING`，放棄時才 `DEAD`。
- **P7 SYM 的變更通知（E2）。** subject（例如 `sym.changed.{venue}`）、訊息內容（revision、哪些 category 變了），以及進程在兩次通知之間漏掉一次時怎麼發現（例如定期比對 revision）。
- **P8 API server 的寫入怎麼滿足 W1（E10）。** API server 不參選、可以多個。候選：spec 只用 create-only 的 txn（`create_revision == 0`），帳號以 `mod_revision` 做 CAS。
- **P9 MD、TD 的 artifact 版本放在哪裡（E4、E6）。** controller 的部署設定，或 etcd 裡一個由 controller 讀的 key；以及換版時 agent 怎麼排程換 incarnation（DECISIONS D106）。
- **P10** 已在 v0.24 完成：session 的宣告抽成 `mftiko.session.v1`，`Side`、`OrderType`、`TimeInForce` 在 `common.v1`。
- **P11 instance 永久下線的程序（E12）。** ack、`WorkerStatus`、`SessionStatus` 不綁 lease，instance 不再回來時會一直留著：非 terminal 的 `SessionStatus` 加上 `RUNNING` 的 `WorkerStatus`，會讓 controller 的 N4 一直等，MD、TD 的需求與 `algo_id` 的佔用都不會釋放。這些 key 的寫入者是那個 agent（W1），維運也不該直接改。候選：API 宣告某個 instance 已退役，controller 據此把它的 session 當成 terminal，再由一次性的工具以該 instance 的身分清掉它的前綴。
- **P12 沒有共同 major 時怎麼收尾（E1-13）。** agent 只能 `Signal`（§4.2）。worker 結束之後，shim 依 S3 等 `Release` 才退出；在那之前，同一個 worker key 的下一個 shim 會因為 socket 還有人服務而起不來（§4.1）。候選：`Release` 也列入跨 major 不變的部分。
- **P13 symbol UID 的寬度與範圍（D203，2026-10-09）。** 已定，寫在 D203 與 §7.3。
  - **已定。** UID 是 SYM 分配的隨機整數：唯一、永不重用，不是單調序號，也不是 Postgres sequence。活躍表與終結表合在一起唯一。UID 跟列一起持久化，重啟與重載都不改它。同一個 ticker 下架再上市拿新的 UID。碰撞時 SYM 重試。契約型別是 `uint32`，範圍 `1..4294967295`（`1..2^32-1`），0 非法。Postgres 欄位是 `bigint`，`CHECK (uid BETWEEN 1 AND 4294967295)`。`symbol_filter` 也拆成活躍表與終結表，列進終結狀態時跟列一起搬。SYM 的 `List` 用 offset＋limit 分頁，跨頁不一致可以接受。`Universe.current` 是 selector，尚未定，不在 D203 的範圍；selector 沒有 UID。NATS subject 不用 UID，見 P14。策略 hook 見 P15。
- **P14 NATS topic 的拼法（D203）。** 已經定的是：NATS 的 topic 用人讀的名字，不用 UID。`md.f`、`md.view` 都不改成 UID。確切的拼法還沒定。這一版維持 `md.f.{inst}.{topic}.{ticker}` 與 `md.view.{inst}.{topic}.{ticker}`。
- **P15 策略 hook 看 ticker 還是 UID（D203）。** spec 已定用 UniversalTicker。平面之間已定用 UID。策略回呼算人機介面還是系統內部，還沒定。這一票不改 SDK。
