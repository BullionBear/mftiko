# DECISIONS

> 決策紀錄（2026-10-15 拿掉 `OmsViewRequest.settled`（欄位 2），改成 `reserved 2` 與 `reserved "settled"`，`algo_id` 仍是 3，D632。同日定案前刪掉 `td.proto` 的 `reserved`，欄位號碼不重排；`tdctl.proto` 沒有 `reserved`；兩份仍在草案清單，移出清單與 PROTOCOL §1 定案是這份合併後的後續，D629。同日保留歷史補正欄位（`AccountSettings` 的 interval 與手動觸發、`td.backfill` 相關欄位），D630。同日每一則 `td.view` 回覆帶 `cancel_batch`，給策略拆批，D631。2026-10-15 修訂 D610（E6-14，#174）：撤單沒有回答、過了界線查到單仍在簿上時，清掉 `cancel_sent_ns`，發 `CANCEL_REJECTED`、code 120，策略可以再撤。2026-10-15 定案兩把鑰匙：OMS 與交易所之間（事件、recon、查單）以 order_id 對單，沒有 client id 的單仍以 order_id 追蹤；系統裡面（策略、td.order、Cancel、BatchCancel、td.view、向外的 OrderUpdate）以 cloid 為鍵。Cancel 與 BatchCancel 只收 client_order_id，order_id 欄位保留，OMS 送出時換成那張單的 order_id。解不開成平台 cloid 的不屬於任何 algo。BatchCancel 不檢查歸屬。D602、D605、D615、D617、D623、D625、D626 改寫，不改編號。2026-10-15 修訂批次撤單（E6-09，#169）：BatchCancel 是明確的訂單清單，TD 一對一轉成一筆交易所請求，不挑單、不分批；CancelBatch 為 1 時本地拒絕；超過上限仍送出並原樣回報交易所的拒絕；一批算 OrderRate 的一筆；商品解不出來只影響那一筆。一次撤掉某 algo 的全部掛單改由 STS SDK 做。D602、D625–D628 改寫，不改編號。2026-10-14 定案下單與撤單（E6-08，#168）：記憶體 cloid 冪等、同 cloid 內容不同回 106、每筆 `recv_window_ms`、`OrderRate` 用到 90% 回 108、查單失敗逾 5 分鐘 `DEGRADED`、`UNKNOWN` 可撤、`TradeStream` 不可用改走 REST，D618–D624（修改 D605、D617）。2026-10-14 定案以 cloid 查單（`Account.QueryOrder`，不存在是 `ErrNotFound`）、請求有效期 `AccountConfig.Validity`，以及三家讀持倉模式；Bybit 以 `positionIdx` 判斷，D617。2026-10-13 定案 td-agent：兩個前綴、ack、spec、endpoint 經 Init；執行中不檢查持倉模式，拿掉 `UNAVAILABLE`；algo 由 STS agent 檢查，D616（修改 D602、D607、D612、D614）。2026-10-13 定案 `mdctl.proto`、`md.proto`、`read.proto`（E4-12，#152）：三份移出草案清單，PROTOCOL §1 改為定案，D460。2026-10-13 刪掉 `mdctl.proto`、`md.proto` 的 `reserved`，現有欄位號碼不重排；三份仍在草案清單，移出清單與 PROTOCOL §1 定案是 #204 合併後的後續，D460。2026-10-13 定案 account worker 的 OMS 與帳本：OMS 只放活著的單，`settled` 不再有作用；recon 期間事件先排著再重放；斷線期間消失的掛單記成 UNKNOWN；槓桿取自持倉，D615（修改 D605、D606）。同日定案 tape 的 worker 級 arena（#189）：64 KiB chunk、硬上限含 slot 與索引、回收全 worker 最舊的 chunk，D458–D459。2026-10-13 定案 P14（#181）：行情 subject `md.v1.f.{inst}.{topic}.{ticker}`、`md.v1.view.{inst}.{topic}.{ticker}`，`md.v1.fetch.{venue}`，`sym.v1.changed.{venue}.{category}`，D455–D457。實作在 #194。2026-10-12 定案 account worker 的連線角色與狀態：只有事件連線決定 DEGRADED，計畫性換線先接後斷；一期不做 BinancePM，D614。同日拿掉 top-N 與 FeedEnd（#180）：book topic 只剩 `orderbook_delta`，寫 `orderbook` 回 `invalid`，D453–D454。2026-10-12 定案 tape ring 與 `md.view`（E4-06）：一個 feed 一個 ring，讀取無鎖，以 `head` 接 live，D435–D452。同日定案帳號憑證以 TD instance 的 X25519 公鑰封裝：NaCl sealed box 加版本與指紋，`api_id`、`kind` 一起封、解開後比對，封裝工具 `mftiko-seal`，D613。同日定案持倉模式由 `AccountSnapshot.HedgeMode` 帶回，以及給 account worker 測試用的假帳號 `exchangetest.AccountVenue`，D612。2026-10-11 定案 TD 的契約與 NATS 身分（E6-02）：`td.order` 只回 ack 或本地拒絕，單的每一次變化都是 `OrderUpdate`，1xx 只列一期會碰到的；訂單狀態加 `ACCEPTED`（上簿），撤單在途改成旗標 `cancel_sent_ns`，拿掉 `PENDING_CANCEL`；account worker 每個帳號一份 NATS 憑證，由 TD agent 簽，D610–D611。同日修訂 D601：TD agent 不查 SYM，歷史補正的交易所讀取比照 md.fetch。同日修訂 D202、D401：MD agent 不查 SYM，conn worker 用活躍表推出 atom 並 diff 訂閱，並在 Report 以 FeedStatus 回報每一筆 feed。同日更新 D415、D426：MD instance 訂閱 `sym.changed.>`，conn worker 在 Init 載入該 venue 的活躍表。同日定案本地訂單簿：一張快照加增量、seq、top-N、序號權威與 fixture，D424–D429。同日再定本地簿為刻度環，D430–D433。同日再定 SnapshotStream 斷號或 checksum 失敗時只重訂該 channel，D434。同日修訂 D426：SnapshotREST 不設價格邊界、不因距離重抓。同日定案 TD 的幾件事（E6-01）：連線與重建是 TD 的事、STS 是最後一道防線；結果未知分兩層，STS 交出 UNKNOWN、TD 主動以 cloid 查單；`td.view` 只讀本地快照；一期只支援單向持倉；TD 的 Policy 與限速，D604–D609。同日更新 D420：`Used.ResetAt` 是計數器歸零的絕對時間，用量到點作廢；429 與 418 依 `Retry-After` 停送，沒有才等到 `ResetAt`。同日定案 md.fetch：subject 分 venue、K 線分頁、限速、不快取、活躍表解析、平台執行檔，D418–D423。同日定案 dev NATS：operator／account JWT、full resolver、MD 與 md.fetch 的權限與 inbox，D412–D417。同日更新：`md.f` 的 v1 envelope 固定 40 bytes，拿掉 `len`，加 `digest`（payload 的 xxHash64），見 D405–D411。2026-10-11 定案 P6 與 P9：`DEAD` 的時機，以及 worker 版本只在下次 spawn 取用，見 D106、D902 與 PROTOCOL §3.5、§8。2026-10-09 更新：幣與數量的單位、SYM listing 分活躍表與終結表，以及 symbol UID，D203。同日再定 UID 的分配：SYM 發隨機、唯一、永不重用的整數，不是單調序號；跟列持久化，重啟與重載不改。寬度與範圍已定：契約型別 `uint32`，`1..4294967295`（`1..2^32-1`），0 非法；Postgres 欄位是 `bigint`，`CHECK (uid BETWEEN 1 AND 4294967295)`，見 PROTOCOL §10 P13）。只收已經講定的事；還在討論的放在 [§待討論](#待討論)。協議與契約見 [PROTOCOL.md](PROTOCOL.md)。
>
> 不照抄 mftik `refactor/process-planes`，也不照抄 Strategon 的 agent。兩者都只當參考。
>
> **編號。** 決策屬於哪個 epic，號碼就落在那個 epic 的段。每個 epic 自己分配下一個號碼，不跟其他 epic 共用一條流水號：E1 是 D101、D102、…，E2 是 D201、…，E3 是 D301、…，以此類推。epic 編號是兩位數時，後面仍接兩位序號（E10 是 D1001、D1002、…），才不會和 E1 的 D101 撞號。不屬於單一 epic 的跨平面、架構層決策用 D0XX（D001、D002、…）。

## 已定

### D001 三個平面是三種專門的 agent

STS（策略）、MD（行情）、TD（執行）各是一種 agent，角色接近 Strategon 的 agent：收 desired、reconcile、拉起並看管進程。但每種 agent 只管**一種已知形式**的 worker，不需要 Strategon 為了跑任意程式而有的 OCI driver 與 download／verify／switch 部署階段。

- desired 來自 controller（D907），agent 之間沒有控制流。
- 三種 worker 都從 artifact 起（D902）：STS 的是各個策略；MD、TD 各只有一種，都在這個 repo 裡實作，由 CI 打包。

| agent | worker 形式 | worker 身分 |
|---|---|---|
| STS | Strategy Actor 的宿主：runtime ＋ 一個繼承 Strategy Actor 的策略 | `session_id` |
| TD | 一個帳號的私有 websocket ＋ HTTP 連線池 | `api_id` |
| MD | 一條公用 websocket | `(venue, route, n)`（D403） |

### D101 agent 的骨架

| 抽象 | 職責 | 共用或平面自有 |
|---|---|---|
| Intake | 收 desired（watch etcd）：需求項目與 STS 的指派都來自 controller（D907） | 共用 |
| Planner | desired → 每個 worker 的 spawn spec ＋ live config，純函數 | 平面自有 |
| Fleet | 以 shim 為底的 worker 集合：spawn、attach、推 live config、觀測、release。supervisor 在這裡，只管 incarnation 與 backoff | 共用 |
| Reconciler | 單一 loop，diff 計畫與觀測，產生動作 | 共用 |
| Policy | 失敗語意與重啟規則 | 平面自有 |
| Provider | MD、TD 依需求項目推導資源並回 ack／nack。等依賴就緒的 Gate 在 controller（D907），STS agent 只照指派送 `Start` | 平面自有 |

各平面最重要的設計決定，是哪些欄位放在 **spawn spec**（變了只能換 incarnation），哪些放在 **live config**（推給活著的 worker）。

| | spawn spec | live config |
|---|---|---|
| STS | 策略、參數、依賴綁定的初始值 | stop／fail、依賴綁定的變化 |
| TD | 帳號、venue、憑證 | drain |
| MD | venue、route、連線 URL（endpoint 參數，D403） | atom 集合（帶 generation） |

### D102 shim 保留，是平面輪換時收養 worker 的地方

不採用 Strategon agent 的 adopt（pid ＋ start_time ＋ pidfd）。shim 是 worker 唯一的父進程，agent 輪換時 worker 留在 shim 底下。

### D103 shim 與 worker 同壽命，輪換只換 agent

- shim 不原地升級、不重啟。shim 的版本就是 spawn 它的 release。
- 新 agent 必須能和舊版 shim 溝通：shim 協定獨立版本、只做向後相容的擴充。
- shim 的修正只在 worker 換 incarnation 時生效。

### D104 shim 消失，worker 必須死

- shim 消失時，worker 自行 graceful stop。
- 理由是 at-most-one：agent 必須能把「shim 不在」當成「worker 不在」。parent 關係也轉不走，新 shim 拿不到 wait status，也收不到孫進程。
- agent spawn 新 incarnation 之前，仍以 pid ＋ start_time 確認舊 worker 已消失。

### D105 控制權的 fencing 在 shim

agent 每次啟動帶 epoch。shim 只接受 epoch 最大的 agent 的指令；更大的 epoch attach 時，舊連線被踢掉。新舊 agent 可以短暫重疊。

### D002 控制走 shim，資料走 NATS

- 控制：agent ⇄ shim ⇄ worker。shim 只轉送、不解讀。live config 的推送因此天然受 epoch fencing 保護，控制面不依賴 NATS。
- agent 不在時，shim 把 worker 的事件帶 seq 緩衝；新 agent attach 時從上次 ack 的 seq 補讀。exit 另外落檔。
- 資料面（行情、下單、回報）走 NATS。
- controller 與 agent 之間的控制走 etcd（D004），不走 NATS。agent 之間沒有控制。

### D106 incarnation

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
- **MD 連線不做 make-before-break。** 新舊版並存時，新版 worker 發在新的 subject，平台內不做遷移。SDK 一律用 binding 給的 subject，不自己組（PROTOCOL §7）。版號放哪裡已由 D455 取代：前段現在就是 `v1`（`md.v1.f.{inst}.{topic}.{ticker}`），不是之後才加。
- 和 agent 的 epoch 是兩個獨立的計數：輪換 agent 不動 worker，換 incarnation 只動一個 worker。

### D901 跨平面依賴由 controller 以宣告式的需求表達

```
API server ──Spec、Account──▶ etcd
controller ──STS 指派──────▶ STS agent
controller ──需求項目──────▶ MD agent、TD agent
MD agent、TD agent ──ack／nack──▶ controller
STS agent ──SessionStatus──▶ controller、API server
STS、MD、TD ─────▶ SYM
```

所有箭頭都是「寫 etcd、對方 watch」（D004）。agent 之間沒有控制流；平面之間只有資料流，走 NATS。2026-10-07 以前的版本由 STS agent 發綁 TTL 的 lease，改寫的理由見本條最後。

- **controller 從 session spec 推導需求，不是 STS agent。** 每個非 terminal 的 session：每個 feed、每個 selector 一個 MD 需求項目；每個帳號一個 TD 需求項目。controller 同時持有所有平面的狀態（ack、SessionStatus、AgentRecord、WorkerStatus），跨平面的判斷都在它那裡做（D907）。
- **需求是宣告式的，不綁 TTL。** 需求項目是一般的 key，存在的理由是「controller 決定它應該存在」，和 controller 活著與否無關。controller 停機或輪換時，所有需求原樣保留，MD、TD 照常服務。
- **只有 controller 明確刪除時，需求才消失：**
  - session 結束：controller 先把 STS 的指派改成結束，等 `SessionStatus` 進入 terminal、從 WorkerStatus 確認 session worker 已經消失，再刪這個 session 在 MD、TD 的需求項目，最後刪指派（PROTOCOL §3.3）。
  - 孤兒：controller 每次當選、每次全量 resync，都比對「應該存在的」與「實際存在的」。key 裡有 `session_id`，掃前綴就知道哪些項目的 session 不存在或已經 terminal，刪掉。回收靠比對，不靠計時器。
  - 新 leader 接手時先 relist 再比對，只改有差異的項目，不先全部刪掉再重建。萬一刪掉又馬上加回，linger（D905）會吸收掉。
- **需求的粒度是行情原子，或能推導成原子的規格**（feed、selector）。controller 不自己展開：需求項目送到 MD agent，由它的 orchestrator 推導成 atom，再 reconcile → shim → conn worker。期權鏈、滾動合約這類動態訂閱全在 MD 內部消化，需求不必跟著變。controller 只管到資源這一層，不碰進程：要起哪些 worker 由各 agent 決定。
- **兩層 refcount：**
  - atom：0→1 訂閱，1→0 退訂。
  - 連線（conn worker）：refcount = 0 若且唯若這條連線上所有 atom 的 refcount 都是 0。
- **refcount 是 owner 集合的大小，不是加減計數器。** 每個 atom 記 `{(session, feed 或 selector)}`。需求項目會重寫、會重試，集合是 level-triggered，計數器會漂移。多個 feed 共用一個 atom 也自然處理。agent 重啟時 list 一次需求前綴，就能重建所有 owner 集合，不需要保存計數。
- **只有一層 linger（D905）。** atom 的 owner 集合歸零後，等 linger 才退訂；連線在最後一個 atom 退訂時就停，最後一個 atom 的 linger 就是連線的 linger，不另設參數。
- **TD 和 MD 一樣依 refcount 起停 worker。** 需求的單位是帳號，每個 `api_id` 一個 owner 集合 `{(session, api_id)}`。0→1 時拉起 account worker（私有 websocket ＋ HTTP 連線池，Start 時 recon）；1→0 時 linger 之後才停。worker 存在就等於這個帳號可以交易，沒有另外的交易層開關，也不保留 mftik 的常駐層。閒置帳號的歷史不為此拉起 worker。排程與落庫在 TD agent，交易所讀取是無狀態服務（D601）。
  - 代價：帳號的第一個 session 要冷啟動（建連線、recon）。`OnInit` 可以和它並行，影響有限。
  - session 進入 terminal 時，TD 需求項目依上面的順序刪除。平台不替策略撤單（D602）。
  - `restarting` 期間需求項目不刪，策略重啟不會讓 account worker 跟著起停。
- **ack／nack 是三種狀態，粒度是資源：**
  - ack：資源正常。
  - nack：provider agent 明確知道某個資源不成立或死了（它透過 shim 管 worker，所以知道）。controller 收到 nack 就結束 session（D907）。
  - 沉默：不知道。ack 不存在表示 provider 還沒處理這個項目。provider 的 agent 不在（`AgentRecord` 不在，D004）時，ack 是它最後寫下的狀態；可能只是在輪換，worker 照跑。兩種情況 controller 都維持現狀，不能當成 nack。
- 資料面的依賴（session worker ⇄ conn worker、account worker）走 NATS，不經過 agent。
- **為什麼不再由 STS agent 發綁 TTL 的 lease：**
  - TTL 把「需求還在不在」綁在「發送者還活不活著」上。發送者只是斷線，TTL 一到，正在交易的資源就被收掉。controller 只有一個、橫跨所有 region（D005），綁 TTL 的話，跨 region 的網路一抖就會停掉遠端 region 的交易。
  - 需求留太久，最壞是多開一條連線，浪費但不危險。所以「不確定就保留」，回收交給 controller 的比對。
  - 這和 Strategon 的 fencing lease 不同：那個保護的是「同一個策略只有一份在下單」，交易所無法檢查 token，只能靠時間，所以必須綁 TTL。mftiko 一期不做跨主機的故障轉移，不需要它（D908）。

### D003 Strategon 那一側的前提

「輪換時 shim 不動」依賴以下四條。任何一條破了，就等於 shim 被動了，worker 跟著死。

1. 停 agent 的信號只送到 agent 的 process group。shim 以 `setsid` 開自己的 session。
2. 開 `oci_host_pid`，不建 PID namespace。
3. 不以 `cgroup.kill` 整個 slot 停止。要有測試鎖住。
4. 舊 release 的 rootfs 仍被 shim 或 worker 當作 root 時不 GC。

### D201 Symbol 照抄 mftik

UniversalTicker（`<Venue>_<Category>_<SYMBOL>`）、`Parse` 嚴格／`Resolve` 寬鬆的分工、SymbolResolver（翻譯是查表不是字串轉換）、listing 與 filters、feed key `topic.UniversalTicker`。SYM 是普通服務，不是 agent。

### D401 feed 與 atom

- **atom** 是交易所那一側的一個訂閱項：在一條 websocket 上 subscribe 一次、交易所 ack 一次的最小單位，身分是 `(venue, route, channel)`，channel 逐字照抄交易所參數；route 是 WS 路徑，由 adapter 從商品推得（D403、D202）。conn worker 交給 adapter 的是活躍表上的那一列。
- **feed** 是策略宣告的行情：`topic.UniversalTicker`。
- **在同一個 venue 上，feed → atom 一律 1:1。** 一個平台 topic 在一個 venue 上只對應一個 channel。
- **atom → 事件可以 1:N。** 一個 frame 解碼出多種事件（Deribit `ticker.*` → quote、greeks、OI），是解碼端的扇出，不需要 join。
- topic 的模型依「交易所原生同時推送的欄位」定義。某 venue 不推的欄位是 optional，並提供能力表給策略查詢。`ticker` 只有 24h 統計；最佳買賣是 `bestquote`。
- 多個 feed 可以共用一個 atom。owner 是 feed 的集合，由 MD agent 算（D901），沒有組合語意。conn worker 用 feed 清單與活躍表推出 atom，再對已訂閱的 channel 做 subscribe／unsubscribe 的 diff。
- 沒有 projector、沒有多 atom 組合出來的 down 狀態、沒有多 atom 的 late-joiner 等待。feed 的狀態就是它那一個 atom 的狀態。
- 交易所沒有推送的東西不做成 feed，只能查詢（例如 Binance UM 的 open interest，走 md.fetch，D402），不以 REST 輪詢假裝成 feed。
- 策略需要兩個 feed 的最新值時，SDK 提供 opt-in 的小工具，那是函式庫，不是平台語意。

### D1001 API 的角色

- API 層以宣告式設定操作平面，只負責開始與結束（start／end）。
- 控制平面分兩個進程（D907）：API server 只寫使用者的宣告（session spec、帳號）到 etcd（D004）；controller 依宣告與各平面的狀態推導需求、決定 session 起停。兩者都不直接呼叫 agent。
- API 不查 SYM（D202）。

### D004 控制面在 etcd，agent 無狀態

- 宣告、需求項目、STS 的指派、ack／nack、不能重新推導的 agent 狀態，全部放 etcd。agent 和 controller 在本機都不留狀態：desired 從 etcd 讀，worker 的實際狀態從 shim 取回。
- API server → etcd、controller → agent、agent → controller 都是「寫 etcd、對方 watch」，沒有 NATS request-reply。控制面的契約就是 etcd 的 key 結構，加上每個前綴的寫入者。
- 每個前綴的寫入者由 etcd 的 RBAC 強制，不只靠程式自律：每個進程有自己的使用者，角色從 instance 清單產生（PROTOCOL §3.2 W1）。被取代的舊 agent 靠 writer key 擋（W2），寫錯前綴的程式靠 RBAC 擋，兩道各自獨立。
- **etcd lease 只有一種：選主 session（PROTOCOL §3.2 W2、W7）。** agent 與 controller 的 `AgentRecord` 綁在自己的選主 session 上：它在，就表示這個 instance 有活著的 agent，以及是哪一個 term。需求項目（D901）、ack／nack、`WorkerStatus` 都不綁 lease，只會被它的寫入者刪掉。讀的一方把 ack、`WorkerStatus` 和寫入者的 `AgentRecord` 一起看：`AgentRecord` 不在時，那些值是最後一次已知的狀態。
- **2026-10-08 拿掉 agent 自己的 lease（E1-07 的第一版，PR #51）。** 那個設計讓 AgentRecord、WorkerStatus、ack 綁在一個 TTL 60 秒的 lease 上，agent 消失就由 etcd 刪掉。拿掉的理由：
  - controller 對沉默維持現狀，TTL 不影響它的任何決定，只讓它晚一點看到沉默。
  - 「key 不存在」因此有兩種意思：寫入者刪了，或 lease 到期。STS agent 掛掉超過 TTL，`WorkerStatus` 消失，controller 依 PROTOCOL §3.3 N4 可能誤以為 session worker 已經消失而刪掉需求；shim 底下的 worker 其實還在跑。
  - agent 在不在，選主 session 本來就知道，不需要第二個 lease。
- **也不改成由 API server 輪詢 agent。** 那要讓 agent 開 server、做服務發現與認證，違反本條；量到的是 API 到 agent 的網路，不是 agent 自己能不能工作（TW 的 API 輪詢 JP 的 agent，WAN 一斷就把健康的 agent 當成死了，正是 D005 要避免的）；API server 有多個副本，結果不一致，也沒有地方放（PROTOCOL §3.2 W1）。
- agent 重啟時 list 加 watch，owner 集合立刻完整恢復，不需要寬限期。
- etcd 本身故障不會造成選主 session 大量過期：沒有 leader 時 lease 不會過期，新 leader 會把所有 lease 延長一個 TTL。
- etcd 不在時，controller 和所有 agent 都不能改狀態，但資料面照跑。
- NATS 只負責資料面。

### D107 生命週期由 shim 送給 worker，worker 回報結束方式

- agent 決定時機，經 shim 對 worker 送 `Init`、`Start`、`Stop`。shim 認得這組動詞與 worker 目前的階段，但不解讀內容。agent 不在時（例如主機關機），shim 可以自己送 `Stop`。
- worker 結束前告訴 shim 它要怎麼結束（`Exiting`：正常結束、初始化失敗、主動 fail、crash、卡死），`OnStop` 跑完再送 `Stopped`。exit code 是備援：shim 是 parent，一定拿得到 wait status。
- 沒有送 `Exiting` 又被 signal 結束的，就是被殺。
- **生命週期分兩段（`Init`、`Start`），加上 `Stop`。**

  | 階段 | agent 何時送 | 策略做什麼 |
  |---|---|---|
  | `Init` → `OnInit` | spawn 後立刻送，不等任何需求 | 解析參數、載模型、讀檔。沒有行情，不能下單 |
  | `Start` → `OnStart` | controller 判定所有 MD、TD 需求都 READY，在 STS 的指派上放行之後（D907） | 訂閱行情、需要行情的熱身（tape、歷史資料）。可以下單；`OnStart` 回傳之後才開始交付 hook |
  | `Stop` → `OnStop` | — | 收尾 |

  controller 在 session 一開始就發 STS 的指派，`OnInit` 因此可以和需求的建立並行，慢的準備工作不會拉長啟動時間。mftik 的三段制（多一個 `Ready`：拿行情熱身但平台禁止下單，帳號就緒後才開放）先不做；之後需要時以新的契約版本加入，只對在 `Hello` 宣告支援的 worker 送。

### D801 策略不處理可用性

- 策略只寫交易邏輯，不處理容錯。SDK 沒有 `OnMdUpdate`、`OnTdUpdate`。
- 行情斷了：策略收不到事件，自然不會反應。
- TD 壞了：下單失敗，策略拿到原因（帶原因的拒單）。
- 存活判斷由 controller 依 ack／nack 處理（D901、D907），不經過策略。

### D005 每個 region 一個 etcd cluster

一開始就每個 region 各架一個 cluster，不先用全域的。理由是故障範圍要和實際的依賴對齊：全域 etcd 在 TW、JP 斷線超過 TTL 時，少數方 region 的 agent 連不到 etcd，選主 session 被健康的 leader 判定過期，整個 region 的控制面停擺，連只用本地資源的 session 也起停不了；每個 region 一個，則只有真的跨 region 的依賴受影響，而那些依賴的資料面（NATS）在同一次斷線裡本來就斷了。

1. **key 放在「會依據它行動」的 agent 所在的 region。** session spec、STS 的指派與 status 在 STS 的 region；需求項目與 ack／nack 在 provider（MD／TD）的 region；其他 key 在該 instance 的 region。所有存取經過路由函數 `clusterFor(instance)`。
2. **只有 controller 和 API server 連多個 cluster。** 三種 agent 都只連自己 region 的 cluster。
3. **fencing 不跨 cluster。** agent 與 controller 在每個要寫入的 cluster 各放一個 writer key（記錄 epoch），寫入時只比對那個 cluster 裡的 writer key。
4. **版本號不用 etcd revision。** agent 的 epoch 是持久化的計數器；推給 worker 的指令帶 `(epoch, seq)`。不同 cluster 的 revision 不能比較，cluster 重建時 revision 也會從頭開始，計數器不受影響。
5. **instance 名稱帶 region**（`md-tw-1`），在所有 cluster 之間唯一。
6. **跨 region 的需求**（TW 的 session 要 JP 的行情，controller 寫進 JP 的 cluster）是一般的寫入，不綁 lease。controller 連不到某個 region 時，那裡既有的需求原樣保留、照常服務，只是改不了；恢復連線後 controller 先 relist 再比對。監控跨 region 的 watch 中斷次數。
7. 只部署一個 region 時，就只有一個 cluster，程式碼不變。

### D902 每個 worker 都從 artifact 起，以 OCI image 格式打包

- **三種 worker 都是 artifact，manifest 帶 `kind`：** `strategy`（STS 的 session worker）、`md.conn`（conn worker）、`td.account`（account worker）。agent 只接受自己那一種。agent 依指派找到要起的 worker，加上 manifest 的 entrypoint 與參數，起動進程（PROTOCOL §8）。策略的 digest 在 session spec；MD、TD 在 spawn 時取當時的最新。
- **MD、TD 的 artifact 各只有一種，都在這個 repo 裡實作，** 由 CI 在 release 時建好推上 registry。不按 instance 釘版本：每次 spawn 取當時的最新，跑著的進程保持原 binary，換 agent 不因此換 incarnation（D106、PROTOCOL §8）。E9 的 artifact 管線做好之前，md-agent 與 td-agent 執行這個 repo 建出的 binary。做好之後，主機上的拉取與驗證和 STS 用同一套程式（`internal/platform/artifact`，D006）。
- 以下各條原本只針對策略 artifact，現在三種都適用，只有標明「策略」的除外。
- 每個策略各自編譯、各自發版，與平台的 release 無關。session 釘住策略的 image digest，重新掛起也沿用同一個 digest。
- **只拿 OCI 當打包格式，不跑容器。** STS 把 image 解開成 rootfs，shim 以 rootfs 為工作目錄直接 exec。之後若要隔離，可以換成 Strategon 的 rootless OCI driver，格式不變。
- **分層讓策略之間共用：**

  | 層 | Go 策略 | 其他語言（由該語言的 SDK 決定，D802） | 共用 |
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

### D802 一期只做 Go SDK；其他語言照契約自行實作

- **不做 Python 綁 Go。** Python 呼叫 Go 的生態還不成熟：沒有像 PyO3 那樣官方支援的方案，只能 `-buildmode=c-shared` 編成 `.so` 再用 ctypes 匯入，還得處理一個進程只能有一個 Go runtime、cgo 的指標規則、GIL 與 Go thread 的互動、manylinux 編譯等問題。成本和風險都高，一期不做。
- **平台只出 Go SDK，它是參考實作。**
- **平台契約與語言無關。** session worker 只要做到 PROTOCOL §9 的每一條，就能用任何語言實作（例如另外做一個純 Python 的 SDK），以策略 artifact（D902）打包，平台不需要為它改任何東西。
- **一致性由 conformance 測試保證。** 平台提供一套黑箱測試：假的 shim（走 fd 3）加上假的 NATS 對端，逐條驗 §9 的契約。Go SDK 也跑同一套。SDK 通過之後，manifest 的 `contracts` 才能宣稱支援。
- **平台另有一道不依賴 SDK 的保險：** 不是每種語言都能保證 heartbeat 不被 hook 卡住（例如 Python 的 hook 卡在不放 GIL 的 C 擴充裡）。STS agent 以 `Beat` 判斷：超過 `hook_timeout_s` 再加 10 秒沒有 `Beat`，就經 shim 送 SIGKILL，outcome 記為 `HUNG`（PROTOCOL §9.7）。

### D601 歷史補正：排程與落庫在 TD agent，交易所讀取比照 md.fetch

account worker 依 refcount 起停，沒有常駐層（D901），所以閒置帳號的歷史不為此拉起 worker。交易所的 REST 讀取與商品反查是無狀態服務，比照 md.fetch（D402、D418–D423）。TD agent 不查 SYM。它排程、訂 `td.evt`、把結果落庫。

- **Postgres 的 cursor 是成交進度的權威。** live 路徑寫進去的都是暫定（provisional）；哪一段已經確認，只看 cursor。
- **TD agent 專有一個 scheduler**（不在 D101 的共用骨架裡）：
  - 啟動時從自己 region 的 etcd 讀帳號、從 Postgres 讀 cursor，為每個帳號載入 backfill job。
  - 觸發：
    - 固定 interval 加 jitter（`AccountSettings.backfill_interval_s`，必須短於各交易所成交歷史的保留期）。
    - 帳號 refcount 1→0 之後，隔 SAFETY_LAG 跑一次（取代 mftik 的 detach 觸發）。
    - `td.evt` 的 seq 斷號時提前。
    - API 改 `backfill_requested_at_ns` 時手動觸發。
  - 只有持有 writer key 的 agent 跑 scheduler 和 drain（PROTOCOL §3.2 W2）。被取代時停掉所有 job，盡力把 queue 清完再退。
- **兩條路徑進同一個 queue：**
  - provisional：worker 發在 `td.evt.{api_id}` 的 `Fill`、`Order`，由 agent 訂閱。
  - confirmed：向 `td.backfill.{venue}` 要來的。服務已經把交易所寫法反查成 ticker。
  - queue 在 agent 的記憶體裡；一個每 5 秒的 cron 把它清進 Postgres。
- **cursor 的推進也是 queue 的項目**，排在它涵蓋的那一頁後面。drain 依序寫入，同一批在同一個交易裡 commit。agent 掛掉時資料和 cursor 一起消失，下一輪從原處重讀，確認線不會越過沒寫進去的成交。queue 滿時 backfill 擋住上游，provisional 可以丟。
- **provisional 丟了補得回來，所以不用 JetStream。** 每一輪都重讀 `[確認線, now − SAFETY_LAG]`，agent 輪換時漏收的 `td.evt` 一定落在之後某一輪的範圍裡。前提是 refcount 歸零後的那一輪不能省。
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
  - 輪換重疊時兩個 agent 都會收到 `td.evt`，重複寫入沒有影響。
- **範圍從下單決定，不從成交決定。** 一旦下單，就建立那個商品的 backfill cursor：
  - 平台的每一筆單都經過 account worker 的 `Submit`，所以在任何成交之前，worker 就知道帳號動了哪個商品。`tdctl.Report` 帶 `first_submit_ns`（這個 incarnation 每個商品第一次下單的時間）。
  - 走 Report 是因為它不會丟：shim 保留最新一份，worker 死了也留著，agent attach 時一定拿得到。`td.evt` 在輪換、Postgres 不在、私有連線斷線（D603）時都可能漏掉成交，若剛好是某個商品的第一批，就永遠進不了範圍。
  - 新商品的第一筆單：worker 先把 Report 寫給 shim（一次本機寫入，幾 µs），再送單，不留「單已送出、Report 還沒出去」的窗口。之後的單只查一次記憶體裡的 map，成本可忽略。每個商品每個 incarnation 只發生一次。
  - TD agent 收到後，經由同一個 queue 把 scope `(api_id, uid, since)` 寫進 Postgres，`since` 取 `LEAST`（重啟、換 incarnation 都不會把起點往後推）。key 是 `Report.first_submit_ns` 的 UID（D610）。cursor 的建立完全是非同步的，不影響下單。
  - 起點是第一次下單的時間，一定早於任何成交，所以第一批成交即使 provisional 丟了，也讀得到。
  - 不涵蓋：人工在新商品上的交易。人工單不屬於平台（D602）；之後需要時，可以由 adapter 用交易所的帳戶層級介面發現商品。
  - 資金流水（出入金）之後以同樣的方式加入，另一條 cursor。
- **交易所讀取是無狀態服務，比照 md.fetch。**
  - 每個 region 部署，NATS queue group，任何 replica 都能回答。不歸 TD agent 管，沒有 lease、fencing，不落庫。
  - subject 是 `td.backfill.{venue}`。venue 是人讀的名字，與 `md.v1.fetch.{venue}` 相同。每個 replica 只訂它連得到的 venue，queue group 的名稱是 subject。哪些 venue 由部署設定決定。程式不選 region：本地有 responder 就由本地回答。恰好 subject `td.backfill` 不能訂。
  - 啟動時以 `List` 載入這些 venue 的活躍表，訂閱 `sym.v1.changed.>`，漏收或 NATS 重連時比 revision，落後就重載（D203、D422）。反查只讀這份記憶體。
  - REST 拿回 `(venue, category, exch_ticker)`，在表裡換成 UniversalTicker。category 是 key 的一部分（D202）。不在活躍表的這一筆，以 `include_inactive` 直接問 SYM 的 Reverse，拿終結列原本的 UID，不進快取。兩張表都沒有，這筆這輪跳過，確認線不越過它。熱路徑不走 Reverse。
  - 限速比照 D420：429、418、`ResetAt`，不排隊、不重試。一個帳號失敗不影響其他帳號。
  - 憑證仍由 TD agent 解開。封裝用的是 TD instance 的公鑰。agent 把這一輪的憑證放進請求，服務用完不留，也沒有這把私鑰。md.fetch 沒有這一步，因為它讀的是公開 REST。
  - 平台服務，不放進 td-agent，不是 worker artifact。自己的 NATS user：publish 恰好 subject `sym`，subscribe `td.backfill.>`、`sym.v1.changed.>`、`_INBOX_backfill.>`。回答時一則、兩分鐘。
- **排程仍是固定 interval 加 jitter。** 交易所那一側的限速在服務上，比照 D420。
- **TD agent 因此多了：**
  - 解開憑證，放進給 `td.backfill.{venue}` 的那一次請求。憑證本來就以 TD instance 的公鑰封裝，由 agent 解開後交給 account worker；給服務的是同一種解開，服務不留。
  - Postgres 連線。Postgres 不在時 queue 會滿、backfill 停住，交易不受影響。
  - 不讀 venue 的 REST，不查 SYM。
- agent 掛掉時 worker 照跑（D102–D104），只影響補正與落庫。

### D402 行情狀態的權威在 conn worker；md.fetch 是無狀態服務

行情狀態（book、tape）的權威在持有該 atom 的 conn worker 記憶體裡，跟 TD 的 ledger 在 account worker 裡是同一個模式：快照打底，fold 串流的增量，有變化就推出去，晚加入的人向持有者要目前的狀態。

- **book：**
  - conn worker 維護一本本地全量 book，只從 `orderbook_delta` 推出。topic 對到交易所的增量 channel；沒有增量 channel 的交易所，用它最完整的 book channel。`orderbook` 已刪除，寫這個名字是 `invalid`，不換成 delta（D453）。
  - 跟交易所同步的邏輯全在 conn worker 裡：快照與增量對齊、驗序號、斷號後重抓。快照用 adapter 的 REST reader 在進程內抓（Binance 類），或用訂閱時 WS 推來的那一張（OKX、Bybit 類）。不經過 md.fetch 服務：正確性不依賴另一個服務，而且對齊有時序要求，多一跳 NATS 只會讓快照變慢。
  - `orderbook_delta`：正規化後的增量原樣轉發（`OrderBookDelta`：價位 → 數量，`0` 為刪除），worker 不預先過濾（D432）。重新同步之後推一則 `reset`，內容是窗內的完整 book（D430）。第一次成功的快照也是一則 `reset`（D425）。增量被 prehook 丟掉（D803）時，seq 會跳號，SDK 的小工具就向 `md.view` 重拿。
  - 環長與 `price_tick` 見 D430、D433。`exchTicker` 見 D426。重抓只在斷號或 checksum 失敗。重抓節奏見 D424。SnapshotREST 抓 `BookSync.Depth()`（D453）。
  - 晚加入：SDK 先訂 `md.f`，再向持有者要 `md.view`，拿到 seq S 時的 book，之後只套用 seq > S 的增量。seq 斷號或 `conn_epoch` 改變時，重新要一次。這是 SDK 提供的可選小工具，fold 跟交易所無關。
- **tape：**
  - 只錄 `trade`、`aggtrade`、`liquidation`，book 類不錄。錄不錄由 conn worker 依 topic 決定。`AtomAssignment.record` 忽略（D447）。一個 feed 一個 ring（D449）。
  - ring 存已經發佈的訊息 bytes（envelope ＋ `FeedEvent`，D803）。`md.view` 去掉 envelope，不重新編碼（D435）。上限見 D438、D449、D458。
  - WS 重連時在 ring 裡記一個 gap（D440）。ring 跨 `conn_epoch` 保留。
  - 接 live：SDK 先訂 `md.f` 並緩衝，同時讀 tape，以 `TapePage.head` 對齊。`(conn_epoch, seq)` 大於 head 就套用，小於等於就丟。不必等第一筆 live（D437）。沒有 id 的 liquidation 也適用。
  - 不用 Redis。tape 跟著 worker 進程和 atom 一起活：只有進程重新掛起（crash，或停掉後 spawn 新的 incarnation）、或 atom 的 linger 結束，tape 才會沒了。
  - worker 不隨版本遷移（D103、D106）：發新版只影響之後 spawn 的 worker，跑著的進程繼續用原本的 binary，tape 不受影響。tape 在這條連線的進程換掉時才沒：crash 後重拉，或連線停掉之後再 spawn。
  - tape 只是熱身用，回覆會說明涵蓋多少。要一直熱的，跑一個 tape keeper：不下單、不綁帳號的策略，跟 mftik 一樣，tape 跟著它的生命週期走（D404）。
- **md.view：** 持有該 feed 的 conn worker 服務 `md.v1.view.{inst}.{topic}.{ticker}`（D455、D448）。D444 已由 D455 取代。形狀跟 `md.v1.f` 一樣，對應 `td.view.{api_id}`。回覆依 topic：trade 類回 tape 分頁（由新到舊，以 `(conn_epoch, seq)` 當游標，每頁約 900 KiB，D439），book 類回現在的窗內 book，未同步是 `not_synced`（D441、D442）。沒有 responder，就是沒有 worker 持有這個 feed。
- **md.fetch：**
  - 無狀態的服務，跟 SYM 一樣：每個 region 部署，NATS queue group，任何 replica 都能回答。不需要 agent、lease、fencing。不落庫。subject、分頁、限速、解析與部署見 D418–D423。
  - 只轉發交易所的公開 REST：K 線，以及交易所不推送、只能查詢的值（D401，例如 Binance UM 的 open interest）。
  - K 線接 live 的 kline feed，以 `open_time` 對齊。
  - book 快照不走 md.fetch，走 `md.view`。best quote 這類交易所有推送的值，訂閱 feed。
- **共用的是 adapter 的 REST reader 程式碼，不是服務：** conn worker 在進程內呼叫它，md.fetch 服務拿它轉發。SDK 那一層可以都叫 fetch（`fetch_book`、`fetch_klines`），線上依回答者不同走兩個 subject。

### D903 一期的策略完全宣告式

- 策略要的行情全部寫在 session spec 裡，需求項目由 controller 只從 spec 推導。
- 沒有讓策略在執行中加減訂閱的介面。動態的需求（期權鏈重新置中、滾動合約換月）由 selector 在 MD 內部處理（D901）。
- 一次性的讀取（K 線、只能查詢的值）走 md.fetch（D402），不算訂閱。

### D904 策略狀態：session volume 與資料 artifact

spec 維持不可變，平台不解讀策略狀態，重啟仍是新進程從零開始。不同的是，新進程有兩個地方可以讀：

- **volume**：每個 session 一個可寫目錄，在 STS 主機上（`MFTIKO_VOLUME_DIR`）。跨 incarnation 保留，所以同一個 session 內 crash 後重拉，讀得到 crash 前最後寫入的內容。容量上限是 `Limits.volume_mb`。
- **資料 artifact**：不可變、以 digest 定址的檔案樹，存在 D902 的 OCI registry，只是 mediaType 不同（PROTOCOL §8.4）。新 session 的 spec 以 `inputs` 引用（名稱 → digest），STS 主機在 spawn 前拉下、驗 sha256，解到 `MFTIKO_INPUT_DIR/<名稱>/`。上一個 session 的狀態，以及運維上傳的模型、參數表，都走這條路。
- **為什麼要兩個：** spec 不可變，注入的 inputs 在整個 session 期間固定。session 內重啟如果只讀 inputs，會回到 session 開始時的狀態；最新的狀態只會在 volume 裡。
- **spec 只放引用，不放內容。** etcd 的值預設上限是 1.5 MiB，狀態檔很容易超過。
- **session 進入 terminal 後，STS agent 把 volume 打包成資料 artifact 推到 registry。**
  - digest 記在 `SessionStatus.state_out`，下一個 session 拿它注入。
  - 標上 `clean`，取最後一個 incarnation 的 `ExitInfo.on_stop_completed`，讓接手的人知道這份是不是正常結束時寫的。
  - 推成功之後才刪 volume；registry 不在就保留 volume 並重試。volume 是空的就不推。
- **讀取順序由 SDK 的小工具處理，不是平台語意：** volume 有就用 volume，否則用 inputs，兩者都沒有就從零開始。
- **寫入：** SDK 提供原子寫入（暫存檔加 rename），OnStop 逾時被殺也不會留下寫到一半的檔案。
- **只在 OnStop 寫，crash 時就沒有狀態可用。** crash、HUNG、被 kill 都不跑 OnStop。要讓 crash 後也能 rebuild，策略得在執行中定期寫；寫不寫、多久寫一次，由策略自己決定。
- **讀檔放 `OnInit`**（D107 的分工）。狀態檔是策略自己的認知，帳上的部位與掛單以 TD 為準，對帳在第一次 `OnRecon`（D603）。
- **volume 在主機本地。** session 綁在一個 STS instance 上，主機掛了 volume 就沒了，只能退回 inputs 那份較舊的狀態。一期接受。
- **容量：** STS agent 定期量 volume 的大小，超過 `Limits.volume_mb` 就以 `FAILED` 結束 session（原因 `volume_over_limit`），因為磁碟滿會拖累同一台主機上所有 session。
- 名稱上區分「策略 artifact」（D902，程式）與「資料 artifact」（本條，狀態與輸入）。

### D803 session worker 的執行模型、背壓與重啟

**執行模型**

- **單一 dispatcher（Actor）。** hook 一次一個、依序執行，策略程式碼不需要鎖。下單這類呼叫不阻塞，結果以事件回來。
- 跨 thread 只有一個入口 `Post`：策略自己開的 thread 或 goroutine 用它把結果放回 TD buffer。其他 SDK 呼叫只能在 hook 裡做。
- NATS 收訊、心跳、watchdog 跑在 Go 核心的 goroutine 上（D802），hook 卡住不影響它們。mftik 的雙 thread、offload、breathe 都不需要。

**兩個 buffer**

| buffer | 放什麼 | 規則 |
|---|---|---|
| TD | `td.evt`、下單／撤單的回覆、timer、`Post`、`md.view`／`md.fetch` 的回覆 | FIFO，不丟。有上限，滿了以 `FAILED` 結束：跟不上自己的訂單回報是不安全的。多個 goroutine 寫入，量小，用 mutex 即可。同一個 timer 積了多次只觸發一次 |
| MD | 所有行情 | 一個 FIFO ring，每格存收到的訊息 bytes。滿了丟最舊的，跳過正在讀的那一格 |

- dispatcher 每跑完一個 hook：TD 有東西就先取 TD；TD 空了才取 MD。
- TD、MD 之間沒有順序保證：兩邊來自不同進程、不同 subject，本來就沒有，分開只是不假裝有。

**prehook：系統只做簡單的抽濾**

- 事件交給 hook 之前，prehook 只看 envelope 做過濾，通過的才把 payload 解析成資料交給 hook。
- MD 的規則只有一條：`now − envelope.md_ts_ns > recv_window_ms` 就丟。`recv_window_ms` 是 session 的設定，有預設值。
- envelope 是 `md.f` 訊息開頭、由版本決定長度的欄位（PROTOCOL §7.1、D405–D411），不用 NATS header：header 是文字、nats.go 每則都要解析成 map。欄位用 little-endian 的 `encoding/binary` 讀，不用 unsafe，因為 `msg.Data` 不保證 8-byte 對齊（D407）。
- envelope 的時間是 MD 主機的時鐘，所以 window 包含傳輸延遲與本地排隊。跨 region 的 session 要設得比 WAN 延遲大；主機之間需要時鐘同步。
- 交易所的時間戳在資料裡。策略對舊資料有自己的判斷，就在 hook 裡 early return，系統不管。
- 丟掉之後，策略用 `(conn_epoch, seq)` 判斷連續性自己處理。SDK 提供可選的小工具：`orderbook_delta` 跳號就向 `md.view` 重拿快照（D402）；已收盤的 kline 被丟了就用 `md.fetch` 補。
- 持續過載時，送達的事件延遲會穩定在接近 `recv_window_ms`，這是 FIFO 加過期丟棄的性質。以 `Report` 的 `QueueStat`（`depth`、`dropped`、`max_age_ms`）讓它看得見。

**slot 的壽命**

- 正在讀的那一格被釘住，直到 hook 結束。
- Go SDK 的 buffer 是預先配好的 `[]byte`：不含指標的記憶體，GC 不掃描內容。
- 交給 Go 策略的資料預設是複製出來的。要 zero-copy 的另外提供只在 hook 內有效的 view；debug build 在 slot 釋放時填 `0xFF`，誤用會立刻現形。

**hook 的時間預算**

- 軟上限：超過只計數、記 log，反映在 `Report` 的 `hook_slow`、`hook_blocked_ms`。
- 硬上限 `Limits.hook_timeout_s`：超過判定 `HUNG`，watchdog 結束進程（exit 21），`OnStop` 不跑。
- `OnInit`、`OnStart` 用 `start_timeout_s`；`OnStop` 有自己的 deadline。

**重啟（`RestartPolicy`）**

- `ON_FAILURE` 時重啟：`CRASHED`、`HUNG`、不是 agent 要求的 signal。不重啟：`DONE`、`FAILED`（策略自己宣告的失敗）、`INIT_FAILED`（設定或環境的問題，重來也一樣）。
- 指數退避，1 秒起、上限 60 秒；`window_s` 內重啟超過 `max_restarts` 次就轉成 `FAILED`。
- **平台不清場（D602）。** 新的 incarnation 從零開始；上一個進程留下的掛單，由策略的 SDK 自己清：照 `CancelBatch` 拆成 `BatchCancel`，沒有批次撤單就改走單筆 `Cancel`（D625），或接手。第一次 `OnRecon` 會帶上自己 algo 的掛單與帳號的部位，對帳在那裡做（D603）。
- 重啟期間 MD、TD 的需求項目不刪（D901），account worker 與行情不受影響。

### D602 策略的單由策略自己管：algo_id

- **平台不替策略撤單。** crash、`FAILED`、STS agent 不在時留下的掛單，平台都不自動處理：要嘛由宣告同一個 algo_id 的 session 下一次啟動時自己清，要嘛人工撤。帳號層級的 cancel-on-disconnect（`AccountSettings`）是另一回事，照帳號的設定。
- **algo_id 是訂單的命名空間，由設定宣告，不由 API 分配。**
  - 一個 session 可以宣告多個，例如同一個帳號上 BTCUSDT、ETHUSDT、BNBUSDT 各一個。宣告在 spec 的帳號綁定上（`AccountDecl.algo_ids`），策略下單時自己選用哪一個。
  - 1–1023（10 bits），0 保留，讓人工單或格式不對的 id 不會被當成某個 algo。只需要在同一個帳號內唯一：`client_order_id` 與 `batch_cancel` 都以帳號為範圍。
  - 跨 session、跨 incarnation 不變：新 session 宣告同樣的 algo_id，就能接手上一個 session 留下的單。
- **同一個帳號上，一個 algo_id 同時只屬於一個非 terminal 的 session**，否則兩邊會撤到對方的單。由 STS agent 檢查它自己 instance 上的 session；跨 instance 的重疊由使用者保證，TD 不檢查（D616 修改）。
- **編進 `client_order_id`：** uint64 `ver(4) | algo(10) | ts_ms(40) | seq(10)`，以十進位字串傳。沿用 mftik 的 uint64 形式，欄位重新分配：
  - `ts_ms`：自 2026-01-01 UTC 起算的毫秒，40 bits 可用約 34 年（到 2060 年）。
  - `seq`：每個進程每毫秒 1024 個。超過時 `ts_ms` 往前借一毫秒，實際上不會發生。
  - `ver` 只用 1–7，最高位維持 0，用 int64 解析也不會壞。十進位最多 19 位，各家交易所的長度限制都放得下。
  - 新 incarnation 的 seq 從 0 起算也不會撞號：重啟至少退避 1 秒，`ts_ms` 不會和上一個進程重疊。
  - `Submit` 本身仍帶 `session_id`。
- **從 `(api_id, algo, ts_ms)` 對到 session。** 同一個帳號上一個 algo 同時只有一個 session，所以靠持有紀錄就能對：TD agent 在 ack READY 時寫入取得時間、需求項目消失時寫入釋放時間，存成 Postgres 的 `(api_id, algo_id, session_id, acquired_at, released_at)`。需求項目在 session 結束時就從 etcd 刪了，不能當歷史用。
  - 交接時，舊 session 的進程結束、需求被刪、TD agent ack 新 session、新 session spawn 到 Start，這段實際間隔至少幾百 ms，遠大於主機之間 ms 級的時鐘誤差，所以不需要冷卻期。
  - 落庫的歸屬（D601）就用這條規則。
- **只能動自己的單。** SDK 只允許使用 spec 裡宣告給該帳號的 algo_id。TD 檢查 `Submit`、`Cancel` 帶的 algo_id 與 `client_order_id` 的 algo 欄位一致，不符就以 104 拒絕；`Cancel` 的 `client_order_id` 解不開是 106。`BatchCancel` 不帶 algo_id，也不檢查清單裡的單屬於哪個 algo（D625）。解不開成平台 cloid 的 `client_order_id`（空的、交易所自己編的）不屬於任何 algo：`td.view` 以 algo 過濾時看不到，OMS 仍以 `order_id` 追蹤沒有 client id 的單。這是防呆，不是防惡意。
- **SDK 只提供能力，怎麼用是策略的事：**
  - `ctx.batch_cancel(api_id, algo_id, …)`：SDK 從自己的活單挑出這個 algo，依 `td.view` 回覆的 `cancel_batch` 拆成多筆 `BatchCancel`；值是 1 就改走單筆 `Cancel`（D631）。TD 不挑單、不拆批（D625）。SDK 這一段是後續，不在 E6-09。
  - 以 algo 查自己的掛單（`td.view` 的 OMS 查詢）。
  - 策略重啟或新 session 接手時，在 `OnStart` 用它們清場或接手。
- STS agent 不再需要 `td.order` 的權限。session 進入 terminal 時，controller 依 D901 的順序刪除 TD 需求項目。

### D403 MD 連線的身分、endpoint 與放置

- **venue 以「一條連線、一組商品」為單位**，照 mftik 的 venue registry（D201）。host 或產品線不同就是不同 venue，例如 `Binance`（現貨）、`BinanceUM`、`BinanceCM`、`BinanceEO`。憑證屬於帳號種類，不屬於 venue；`BinancePM` 是帳號種類，不是 venue（D301）。
- **endpoint 是連線的可選參數，不是身分。** 同一家交易所提供的替代 host（例如針對某個雲端優化的 endpoint）視為同一個 venue，由 MD instance 的設定選擇，進 conn worker 的 spawn spec。換 endpoint 要換 incarnation（D106）。
- **route：同一個 venue 裡，商品和 topic 決定要連哪條 WS 路徑。** 例如 OKX 的 K 線在 `/ws/v5/business`，其他公開行情在 `/ws/v5/public`；Bybit 的公開行情依類別分成 `/v5/public/spot`、`linear`、`inverse`、`option`。不同 route 不能共用連線。OKX、Bybit 是一份憑證的統一帳戶，拆成不同 venue 會破壞 UniversalTicker 與 TD 的帳號，所以由 adapter 從商品推出 route（D202）。conn worker 用活躍表上的商品。MD agent 只為了選 worker，用 ticker 問 route，不訂那個 channel。大多數 venue 只有一個 route。
- **連線的身分是 `(venue, route, n)`。** 之後加版號（D106）就是 `(version, venue, route, n)`。同一組 atom 一律共用連線，兩個 session 要同一個 atom 就是 refcount=2，不會多開連線。
- **只有連線達到交易所的上限才開 n+1。** 上限由 adapter 宣告，例如 Binance 一條連線 1024 個 stream。新 atom 放進同一個 `(venue, route)` 下還有空間、n 最小的那條。共用同一個 channel 的 feed 是同一個 atom，只佔一份額度，放在同一條連線。agent 用 ticker 問出的 channel 只做這次放置，不拿來訂閱；conn worker 再用活躍表推出真正要訂的 channel。不整併：1024 連期權鏈也用不完，實際上幾乎只會有 n=0。

### D603 行情過舊與 TD 降級

- **行情過舊是策略的風險，平台不兜底。** timer 驅動的策略、用多個 feed 算價差的策略，下單前自己驗事件裡的 `md_ts_ns`、`exch_ts_ns`。系統不在下單時擋：SDK 不知道一筆單依賴哪個 feed。掛著的單要不要撤，也是策略的事（D602）。
- **TD 私有 websocket 斷線（`DEGRADED`）時：**
  - 新單以 1xx 拒絕（`account_degraded`）。收不到回報時送出去的單，策略只會拿到 accepted、看不到後續，很容易重複送單。這也就是 D801 的「TD 壞了 → 下單失敗，拿到原因」。
  - `Cancel`、`BatchCancel` 照常走 HTTP：降低風險的動作不擋。
  - account worker 自己重連。重連後 recon，在 `td.evt` 發一則 `Recon`，之後才恢復接受新單。
  - TD 的 ack 維持 `READY`：worker 還活著，只是暫時降級，改成 `DEAD` 會讓 controller 結束 session（D907），太重。
- **`OnRecon(reconState)`：帳號狀態重置只有這一條路徑。**
  - 內容：自己 algo 的 open orders，帳號的 positions、balances。
  - 觸發：私有連線重連並 recon 之後；account worker 換 incarnation 之後（取代原本的 `AccountReset`）；`OnStart` 之後 SDK 以 `td.view` 取得的第一份。D904 說的對帳、D602 說的接手或清場，都在這裡做。
  - 和其他 TD 事件共用 seq，順序由 TD buffer 的 FIFO 保證。策略在 `OnRecon` 裡整份換掉自己的狀態，之後的 `Order`、`Fill` 照常疊上去。
  - 快照是整個帳號的。SDK 把 `open_orders` 篩成自己宣告的 algo_id；`positions`、`balances` 是帳號層級、所有 algo 共用的，原樣給。
  - 斷線期間的個別成交不會補發給策略，策略拿到的是重置後的狀態。DB 的完整紀錄由 D601 的 backfill 補。
  - 這不是 D801 說的可用性 hook：策略不會被告知「TD 斷了」，只會拿到拒單，之後拿到一份新的狀態。

### D604 連線與重建是 TD 的事，STS 是最後一道防線

- **TD 自己處理連線與重建。** account worker 的私有連線斷了就自己重連（D603）。worker 被重新拉起（換 incarnation，D106）之後，第一件事是 recon：重建倉位、餘額與活訂單，建完就在 `td.evt` 發一則 `Recon(INCARNATION)`。STS 以最快的方式拿到新的帳號狀態，不必自己去要。
- **SDK 沒有任何讓 TD 重建或重連的請求。** `td.order` 只有下單與撤單，`td.view` 只讀本地快照（D606）。
- **TD 自己也處理不了時，STS 自保。** `DEGRADED`、沒有 responder，策略拿到的是 1xx 拒單，例如不再送單；平台不替策略做別的事（D602、D801）。
- incarnation 只在 worker 的實體進程換掉時加一：crash 後重拉、linger 停掉之後又被需要、刻意換版、主機重開機。換 td-agent 的行程（收養，換的是 epoch）與私有連線重連（`DEGRADED` → `Recon(RECONNECT)`）都不換 incarnation。

### D605 結果未知：STS 交出 UNKNOWN，TD 主動查單

結果未知有兩層，是兩件獨立的事，不混在一起。

- **STS：`td.order` 逾時是結果未知，不是 1xx。** 1xx 的意思是「什麼都沒送出」（PROTOCOL §7.2）。逾時的單可能已經到了交易所，當成被拒再送一次，部位就加倍。
  - 沒有 responder（account worker 不在）與 TD 的本地拒絕才是 1xx。
  - 逾時：SDK 把這張單以 `ORDER_STATUS_UNKNOWN` 交給策略。不查 `td.view`、不重送、不推論。
  - 之後這個 `client_order_id` 的 `OrderUpdate` 來了，SDK 照常更新。一直沒有下文的單怎麼處理是策略的事，例如以 `BatchCancel` 清掉，或等 `OnRecon`。
- **TD：交易所沒有回答的單，由 worker 主動查交易所。** 單已經有 `order_id` 時以它查，還沒有才用 `client_order_id`（D617）。
  - 下單或撤單送出之後沒有讀得到的回答（`ErrUnknownOutcome`，D303），OMS 把這張單記成 `UNKNOWN`，在 `td.evt` 發出。撤單沒有回答時不記成 `UNKNOWN`，見 D610。
  - 等到「送出時間 ＋ 請求有效期 ＋ 時鐘誤差」之後查單（已有 `order_id` 用它，否則用 cloid；`Account.QueryOrder`，REST，E6-16）。有效期是 Binance、Bybit 的 `recvWindow`、OKX 的 `expTime`，下單與撤單一律帶上。過了這條界線，請求就算才到交易所也會被拒，「查無此單」才等於「沒下成」。預設有效期 5 秒、時鐘誤差 1 秒，是部署設定。
  - 查到：照交易所的狀態更新 OMS，發 `OrderUpdate`。查無此單：`REJECTED`，reason `not_placed`。查詢本身失敗：退避重試，直到有結果。不重送原單。
  - 只靠私有事件與 recon 不夠：單沒到交易所時兩者都不會出現；單在私有連線斷線期間就終結時，事件漏了，recon 只讀 open orders 也看不到。
  - 打交易所查單的只有 worker。
- **以 `client_order_id` 冪等。** 同一個 incarnation 裡，account worker 收到還活著的單的 cloid，回原本的結果，不再送交易所。已經終結的單不在 OMS 裡，不保證認得（D615 修改）。
- 不採用：SDK 逾時之後查 `td.view` 並以同一個 cloid 重送。worker 換了 incarnation 之後，新的 worker 不認得上一個 incarnation 收過的 cloid，重送可能讓已經成交的單再下一次。

### D606 `td.view` 只讀本地快照

- **任何查詢都只讀 account worker 的記憶體，不打交易所，不消耗交易所的 REST 額度。** OMS、帳本（持倉與餘額）、槓桿都是。帳本與交易所的差異只由 recon 與 `Recon` 處理（D603、D604）。不提供「向交易所重新查詢」的選項。
- **槓桿讀本地快取。** 原本的 `EnsureLeverage` 對應 `Account.Leverage`，是一次 REST 呼叫，違反這條。改名 `Leverage`：快取取自持倉的 leverage（D615）；快取裡沒有的商品回未知，不去查。設定槓桿是寫入，一期不做。
- **快照還沒好也照樣回，不擋、不等。** 還沒完成第一次 recon，或正在 `DEGRADED` 時，回當下的快照，附上 `AccountState` 與 `reconciled_at_ns`，由呼叫端判斷新舊。
- **回覆帶 incarnation 與當下的 `td.evt` seq。** 快照加上 seq 之後的事件，能接成同一份狀態。STS 自己這一端漏收 `td.evt`（NATS 斷線、被當成 slow consumer）時，TD 不會知道；SDK 怎麼用它接回屬於 E8。
- **帳本可以依商品篩選。** `LedgerViewRequest` 帶 UID 清單，空的是全部；持倉與餘額照舊一起回。
- **OMS 只回活著的單（D615 修改，D632）。** 終結的單從 `td.evt` 與落庫的歷史拿（D601），熱路徑上不查資料庫。`OmsViewRequest.settled`（欄位 2）已拿掉，改成 `reserved`。

### D607 一期只支援單向持倉

- **雙向持倉（hedge mode）一期不支援。** 雙向持倉下 `Submit` 的 side 分不出開多或平空，交易所要求帶持倉方向（Binance 的 `positionSide`、OKX 的 `posSide`、Bybit 的 `positionIdx`）。E3 的 adapter 把持倉方向折進帶號的 `Size`，同一商品的多空兩列會互相蓋掉。支援它要動契約（`Position` 以 `(UID, 方向)` 為鍵、`Submit` 與事件帶方向）、adapter 與 ledger，一期不做。
- **檢查：**
  - account worker `Start` 時確認持倉模式。Binance UM 是帳號層級的 `dualSidePosition`（與 CM 共用），OKX 是帳號的 `posMode`，Bybit 依商品或結算幣。是雙向就 `INIT_FAILED`，reason `hedge_mode`；TD 的 ack 是 `REJECTED`，不重拉（D608）。
  - 執行中不檢查：假設 worker 執行期間沒有人改持倉模式（D616 修改）。
  - `Submit.params` 帶 `positionSide`、`posSide`、`positionIdx` 一律以 1xx 拒絕，不讓這個逃生口繞過。
- 不採用：跟著帳號的設定走、不指定模式。持倉方向必須進契約與帳本，成本見上。

### D608 TD 的 Policy

- **與 MD 相同：結束了的 account worker 一律重拉，需求過舊時也重拉（`KeepWhileStale` 為真，D109）。** 需求過舊時本來就不停正在跑的 worker，重拉一顆 crash 的 worker 是同一個立場：策略還在跑，帳號應該維持可以交易。代價是失聯期間有人停用了帳號，要等聯繫恢復、下一輪 reconcile 才生效。
- **設定錯誤不重拉：** `ErrAuth`（簽章錯誤、金鑰沒有權限或被撤銷）與 `hedge_mode`（D607）。重拉也一樣會失敗。ack 是 `REJECTED`，reason 是 `auth` 或 `hedge_mode`：這個帳號在現在的設定下不可能成立（PROTOCOL §3.5）。
- **退避與放棄：** 退避 1 秒起，每次加倍，上限 60 秒。連續重拉 10 次（其間沒有穩定跑過 `StableAfter`，預設 1 分鐘）就放棄，依 P6 寫 `DEAD`（PROTOCOL §3.5）。
- **Beat 逾時 10 秒。** 逾時就停掉，照上面的規則重拉。
- 以上數字是部署設定。`DEGRADED` 不是 `DEAD`（D603）。

### D609 TD 的限速，以及一期不做的

- **每個 account worker 自己限速。** 依 `AccountLimits.OrderRate` 與交易所回報的 `Usage`；429、418 照 D420 停送。
- **同一個出口 IP 上多個帳號共用的額度一期不處理。** Binance 的 request weight 依 IP 計，UM 與 CM 共用下單額度（D301）。多個 account worker 在同一台主機上會一起吃同一份額度，各自限速看不到彼此。記在 PROTOCOL §10 P17。
- **一期不做：** 改單（Amend）與條件單。之後在 `OrderRequest` 加 oneof 分支，是相容擴充。

### D610 `td.order` 只回 ack 或本地拒絕；單的每一次變化都是 `OrderUpdate`

- **`td.order` 的回覆只有兩種。** `accepted` 為真：TD 收下了，接著送往交易所，不代表交易所接受。`accepted` 為假：本地拒絕，code 是 1xx，什麼都沒送出。回覆裡沒有 `Order`，之後的狀態一律從 `td.evt` 拿。
  - `BatchCancel` 也一樣：回覆只說收下了，每一張單撤成或撤不成，各自是一則 `OrderUpdate`。拿掉 `BatchCancelResult` 與 `timeout_ms`。
  - account worker 完成 `Start` 之後才服務 `td.order`；在那之前送單和 worker 不在一樣，是 101。`td.view` 從連上 NATS 起就回答（D606）。
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
  - 撤單沒有回答時，`status` 不變、旗標保留，worker 照 D605 的界線查單。修改 D605：撤單沒有回答不把單記成 `UNKNOWN`，單還在交易所上，不確定的只有撤單本身。
  - 查到單已終結就照常更新。查到單還在簿上：過了界線撤單已經不會生效，清掉旗標，發 cause `CANCEL_REJECTED`、code 120（`NOT_PLACED`），reason `cancel was not placed`，策略可以再撤。不清的話，之後的撤單都只回 `accepted` 不送出，這張單再也撤不掉。
  - 進終態時一律清掉旗標；之後才到的撤單結果不再發 `OrderUpdate`。目標單已經有撤單在途時再撤，回 `accepted`，不再送交易所。
  - 交易所自己的 pending-cancel 狀態（Binance `PENDING_CANCEL`、Bybit `PendingCancel`）是還在簿上的單：`ACCEPTED`，有成交就是 `PARTIALLY_FILLED`（`exchange.OnBook`）。
  - 拿掉 `ORDER_STATUS_PENDING_CANCEL` 與 `exchange.OrderStatusPendingCancel`。`OrderCause` 加 `ACKNOWLEDGED`（→ `NEW`），`ACCEPTED` 改指上簿。
  - 不採用：保留 `PENDING_CANCEL` 狀態，另加 `prev_status` 供還原。`prev_status` 要對，撤單途中就得跟著事件更新，那它就是底層狀態本身；快照裡一張 `PENDING_CANCEL` 的單也看不出底下是 `NEW` 還是 `ACCEPTED`。這和 FIX、Binance 把 pending cancel 當成單的狀態不同，是刻意的。
- **同一次變化只發一則。** 同一張單的同一次變化，交易所的回答與私有事件常常都會報（WS 下單的回答、REST 的回應、user data stream）。worker 依 `client_order_id` 與變化後的狀態去重再發佈，所以每個交易所都是同一個語意。
- **1xx 以 code 區分，`reason` 只給人讀。** `RejectCode` 替 1xx 取名，名字要讓寫策略的人看得懂：沒有 responder 叫 `ACCOUNT_OFFLINE`。
  - 只列一期會碰到、有票驗收的，之後碰到新的情況再加。表在 PROTOCOL §7.2。
  - 101–119 是本地拒絕，只出現在 `td.order` 的回覆；120 起是送出之後確定沒下成，只出現在 `OrderUpdate`。
  - D301、D603、D605、D607 寫的 `venue_not_tradable`、`account_degraded`、`not_placed`、`hedge_mode`，分別是 105、102、120、103。etcd ack 的 reason `hedge_mode`（D608）不是拒單碼，不變。
  - 撤單的目標不是掛單（107）與限速（108，D609 照 D420 不排隊）都是一期就會碰到的，所以列入。drain 中的新單沒有拒單碼：一期沒有人送 `Drain`，等 drain-replace 落地時再加。
- **商品以 UID 指定（D203）。** `Submit`、`Cancel`、`BatchCancel`、`Order`、`Fill`、`Position`、`LedgerViewRequest`、`LeverageRequest` 與 `tdctl.Report.first_submit_ns` 的 key 都換成 `uint32` UID。subject 仍是 `td.*.{api_id}`。
- **`td.view` 的狀態欄位放在 `ViewReply` 上**：`state`、`reconciled_at_ns`、`incarnation`、`seq`，三種回覆共用（D606）。`OmsView`、`LedgerView` 原本各自的 `incarnation`、`as_of_ns` 拿掉。`AccountState` 從 `tdctl.v1` 移到 `td.v1`，`tdctl.Report` 改用它。`LeverageInfo.leverage` 是 optional：不設表示快取裡沒有。
- **`OmsViewRequest` 拿掉 `session_id`。** 單上只帶 algo（D602），TD 沒有 session 可以篩。`Submit`、`Cancel` 的 `session_id` 只給 log。
- 不採用：`td.order` 等交易所回答才回覆，或回覆帶 `Order`。結果會在回覆與 `td.evt` 各報一次，策略要對兩個來源排序、去重；回覆的逾時也得蓋過交易所的延遲。
- 不採用：保留 `OrderReject`、`CancelReject` 當獨立事件。同一張單的狀態散在三種事件裡，策略要自己拼。

### D611 TD 的 NATS 身分：agent 每個 instance 一份，account worker 每個帳號一份

- **account worker 每個 `api_id` 一份 user JWT，由 TD agent 簽。** 和 session 的憑證一樣（D906）：spawn 時在本機簽，經 `Init` 的 socketpair 送進去（`tdctl.InitPayload.nats`），不放環境變數、不落檔，不設過期。
- **為什麼每個帳號一份。** 一顆 account worker 只能服務自己的 `td.order.{api_id}`。用平面共用的憑證時，一顆 worker 訂得到別的帳號的 `td.order`；同一個帳號被兩顆 worker 同時服務，同一筆請求會送兩次。交易所只在掛單之間檢查 `client_order_id` 唯一（例如 Binance），已經終結的單擋不住第二次，不能靠交易所去重。
- **權限：**

  | | TD agent（`td-{inst}`） | account worker（`td-account-{api_id}`） |
  |---|---|---|
  | publish | `td.backfill.>` | `td.evt.{api_id}`、`sym` |
  | subscribe | `td.evt.>`、`_INBOX_td.>` | `td.order.{api_id}`、`td.view.{api_id}`、`sym.v1.changed.>`、`_INBOX_td_{api_id}.>` |
  | 回覆 | 不回答 request | 一則、兩分鐘（D415） |

  - TD agent 訂 `td.evt.>` 落庫（D601）。帳號會在 instance 之間搬，不逐一列出。它呼叫 `td.backfill.{venue}`，不 publish `sym`，也不訂 `sym.v1.changed.>`。
  - account worker 的 inbox 前綴是 `_INBOX_td_{api_id}`，不是平面共用的 `_INBOX_td`：一份憑證一個前綴，和 session 一樣。`session_id` 是六個 hex，不會和它撞。
  - account worker 的 `sym` 是 publish 恰好這個 subject，不是 `sym.>`（D415）。TD agent 沒有這條。
- **TD agent 自己的憑證**和 MD 一樣，每個 instance 一份（`MFTIKO_NATS_CREDS`），由 account 的身分金鑰簽。dev 由 `cmd/mftiko-nats-dev` 產生（`-td-inst`）。
- **TD agent 持有 `MFTIKO` 的簽章金鑰**，和 STS agent 同一把（D414）。系統是單人使用，兩把只多了輪換的工作。之後要限制 TD agent 能簽出的權限，可以改用 NATS 的 scoped signing key。
- **檢查（D417）：** 嵌入式 nats-server 上，在記憶體裡簽一顆 account worker，確認它服務得了自己帳號的 `td.order`、`td.view`，發得出自己的 `td.evt`；別的帳號的 `td.evt`、`td.order`、`td.view`、別人的 inbox 一律被拒。TD agent 收得到 `td.evt`，發不出 `td.evt`、`td.order`，也訂不到 `td.order`、`td.view`。
- 修改 D906 最後一條與 D414 的簽章金鑰。D415 留給 TD 的權限由這一條補上；SYM 仍未定。

### D612 持倉模式由 Snapshot 帶回；假帳號 `AccountVenue`

- **`exchange.AccountSnapshot` 加 `HedgeMode`。** 帳號在任何地方是雙向持倉就是真，任何一頁說是就是。worker 只在 `Start` 的第一次 recon 檢查（`INIT_FAILED`，reason `hedge_mode`），之後的 recon 不看它（D616 修改）。
  - 不另加方法：模式和持倉是同一份帳號狀態，recon 時一起讀；`Start` 的檢查也只是第一次 recon。
  - 真實 adapter 還沒讀持倉模式，一律是假：Binance UM 要多一個 `GET /fapi/v1/positionSide/dual`，OKX 讀 `GET /api/v5/account/config` 的 `posMode`，Bybit 看持倉的 `positionIdx`。在 E6-16 做完之前，這個檢查對真實交易所不生效。這一條只定介面。
- **`exchangetest.AccountVenue` 是一個帳號的假交易所**，給 account worker 的測試用（E6-05）。
  - 狀態在記憶體：掛單、持倉、餘額、槓桿、持倉模式。測試經由它改狀態（`PutOrder`、`Fill`、`SetBalance`、`SetPosition`）時，同時推出交易所會發的事件，跑完之後可以拿 worker 的 `td.view` 與它比對。不改狀態的重複或亂序事件用 `EmitOrder`、`EmitFill`。
  - REST：它自己就是 `exchange.Doer`。`Snapshot` 可分頁，可在指定頁失敗；`Submit`、`Cancel`、`CancelBatch` 依 cloid 指定接受、拒絕或不回答（`ErrUnknownOutcome`，可選交易所是否照做）；可以回 429（`Retry-After`）與回報用量（`ResetAt`）；換 key 之後一律 `ErrAuth`。`Requests` 列出收到的每個請求。
  - 私有串流：`Handler` 是真的 websocket，以 httptest 跑，worker 照正式路徑用 `exchange/transport/ws` 連。可以拒絕登入、讓握手失敗、一次斷掉所有連線、暫扣事件以決定先到的是回答還是事件，以及 Binance 式的 listenKey 與到期。
  - 不撮合：單一直掛著，直到測試成交或撤掉。不驗簽章，只比對 key。查單與「請求晚到」在 E6-16 加。
  - 不採用：私有串流用行程內的假連線。那樣 worker 的連線路徑就少測了 ws 傳輸；MD conn worker 的測試也用 httptest。

### D613 帳號憑證以 TD instance 的公鑰封裝

- **每個 TD instance 一對 X25519 金鑰，不是每個帳號、也不是整個平面一把。** 一個帳號只會被一個 TD instance 跑（D908，不跨主機接手），封給它就夠；一台主機被拿走，損失只限於封給它的帳號。私鑰是該主機上的檔案，0600，路徑是部署設定；權限比 0600 寬就拒絕載入。
- **格式是 `version(1) | fingerprint(8) | NaCl sealed box`**（`golang.org/x/crypto/nacl/box` 的 `SealAnonymous`：X25519、XSalsa20-Poly1305，每次封用新的暫時金鑰）。fingerprint 是收件公鑰 SHA-256 的前 8 bytes，讓 TD agent 不必試解就分得出「封給別的 instance」和「壞掉」。換演算法就換 version。
  - 不採用：自己組 X25519＋HKDF＋AES-GCM。sealed box 是現成的、介面小，不用自己決定 nonce 與 KDF。
  - 不採用：age。功能多於需要（多收件人、passphrase、armor），格式也不是我們控制。
- **明文是 `state.v1.AccountCredentials`，`api_id` 與 `kind` 一起封進去。** sealed box 沒有 associated data，密文從一個帳號搬到另一個帳號底下照樣解得開；TD agent 解開之後比對 etcd key 的 `api_id` 與 `Account.kind`，不符就不用。
- **解不開時，需求 `REJECTED`，reason 分三種：** `credentials_other_key`（指紋不是本 instance 的，用它的公鑰重封）、`credentials_unreadable`（空、版本不認得、長度不足或損壞；竄改指紋也算這個）、`credentials_other_account`。由 `sealbox.Reason` 產生；TD agent 寫 ack 在 E6-10。
- **公鑰的交付，一期是檔案或 CLI。** `cmd/mftiko-seal keygen` 寫私鑰並印出公鑰，`pubkey` 從私鑰檔印公鑰，`seal` 從 stdin 讀憑證 JSON、印出 base64 的密文。憑證只走 stdin，不進 flag，免得留在 shell history 與行程列表。TD instance 把公鑰發布到 etcd、API server 寫帳號時自己封，留到 E10。
- **明文只在記憶體裡。** `sealbox.Credentials` 與私鑰對任何 fmt 動詞與 JSON 都印 `REDACTED`；錯誤只說哪一步失敗，不帶 bytes。解開後經 `tdctl.InitPayload.credentials` 由 socketpair 送進 account worker（§5.4），歷史補正放進該次請求（D601）。
- **換 key：** 先把舊檔移開才能寫新的（`keygen` 不覆蓋），之後封給舊 key 的帳號全部要重封；在重封之前它們是 `credentials_other_key`。

### D614 account worker：連線角色、狀態與一期的範圍

- **一期不做 BinancePM。** PM 依 ticker 的 venue 把單送到 margin、UM、CM 不同的 WS API，一個 api_id 要有多條 trade stream；`Account.NewTradeStream()` 只回一個 codec。account worker 遇到交易不只一個 venue 的帳號種類就 `INIT_FAILED`，reason `kind_unsupported`；TD agent 對這種帳號的需求寫 `REJECTED`，同一個 reason（E6-10）。D301 不變：PM 仍是合法的帳號種類，E3 的 papi 碼留著。之後要做，從 `NewTradeStream` 依 venue 取 codec 改起。
- **連線依角色區分，不寫死條數。**
  - 事件連線：帶私有事件的那一條（帶事件的 trade stream，否則 `PrivateStream`）。
  - 下單連線：不帶事件的 trade stream。
  - 一期每個 api_id 至多各一條；換線期間會暫時多一條。
- **只有事件連線決定 `DEGRADED`（D603）。** 事件斷了，OMS 就看不到單的變化，所以拒新單、放行撤單，重連並 recon 之後才回到 `READY`。下單連線斷了，下單改走 REST（D303 的 `Submit`、`Cancel`），狀態不變。
  - 不採用：任何一條斷線都拒新單。這樣比較保守，但 OKX、Bybit 的 trade stream 一抖就會停單，而 REST 明明可以下。
- **計畫性換線先接後斷。** 連線到 `MaxLifetime` 減兩個 margin（`ws.MaxLifetimeMargin`）時撥新的，新的 Ready 之後才關舊的。期間不是 `DEGRADED`，也不重新 recon。兩條連線重疊的那一小段，同一則事件可能到兩次，OMS 要能吃重複（E6-07）。新的撥不上時，舊的照常在 lifetime 前一個 margin 關掉，之後依斷線處理。
  - 不採用：先斷後接。每條連線每天會有一次固定的 `DEGRADED` 和一次 recon（Binance 是 24 小時）。
- **reason：**
  - `auth`：交易所拒絕憑證。Start 時是 `INIT_FAILED`，執行中重撥被拒是 `CRASHED`（exit 20）。
  - `hedge_mode`：Start 時是 `INIT_FAILED`；執行中不檢查（D616）。
  - `kind_unsupported`。
  - `stream_lost`：`DEGRADED` 的 reason。

  D608 的 Policy 不重拉 `auth` 與 `hedge_mode`。
- **listenKey：**
  - 每 `RefreshEvery` 續約一次。續約失敗只記 log，不斷線：key 真的到期時，交易所會送到期事件（`Expired`），worker 收到才重連並重新 `Prepare`。
  - 續約被拒絕憑證時，依 `auth` 處理。
- **endpoint 由 Init 帶入**（`tdctl.InitPayload` 的 `rest_url`、`stream_url`、`simulated`），值是 td-agent 依帳號種類的設定（D616 修改；原本是 worker 的旗標）。

### D615 OMS 只放活著的單；recon 期間的事件先排著

- **OMS 只放活著的單。** 一張單送出終態的 `OrderUpdate`（FILLED、CANCELLED、REJECTED、EXPIRED）之後，就從 OMS 刪掉，記憶體裡不保留已終結的單。熱路徑上不查資料庫：`td.view` 只回活著的單。`settled` 沒有作用，欄位已拿掉（D632）；終結的單從 `td.evt` 拿，歷史由 TD agent 落庫（D601）。
  - 撤單指到不在 OMS 裡的單，以 107（`ORDER_NOT_OPEN`）本地拒絕，什麼都不送（E6-08）。
  - 冪等只涵蓋活著的單：已終結單的 cloid 再送一次，worker 不認得，照新單送出，交易所是否拒絕重複的 cloid 由它決定。同一個 cloid 重送本來就是 SDK 不做的事（D605）。
  - 記得最近終結的單的 key（有上限，與 fill_id 同量），只用來丟掉同一個終態的重複（交易所的回答與私有事件都報了、換線時兩條連線都送了），不回答查詢。
- **單一 goroutine 持有帳號狀態。** OMS、持倉、餘額、槓桿快取與 `td.evt` 的 seq 由一個 goroutine 持有；連線讀到的事件先複製（frame 是借來的，D302）再交給它，`td.view` 也經它回答，所以回覆的 seq 與內容一致（D606）。
- **與交易所之間以 `order_id` 對單。** 事件與 recon 都以 `order_id` 找到活單。交易所還沒給號的單，第一則帶著同一個 `client_order_id` 的訊息先對上，再記下 `order_id`。沒有 client id 的單只以 `order_id` 追蹤；查單表以 client id 為鍵，所以不替它們發起查單。斷線期間從快照消失的，留在 `UNKNOWN`，下一份帶著同一個 `order_id` 的快照或事件再對上。系統裡面（策略、`td.order`、`Cancel`、`BatchCancel`、`td.view`、向外的 `OrderUpdate`）以 cloid 為鍵。
- **recon 期間的事件先排著。** recon 開始時，之後到的事件不套用、不發出。快照回來之後：整份取代 OMS 的活著的單、持倉、餘額 → 發 `Recon`（新 incarnation 的第一次是 `INCARNATION`，之後是 `RECONNECT`）→ 依到達順序重放排著的事件，有變化的才發 `td.evt`。
  - 為什麼重放就對：同一條連線上交易所依發生順序送事件，比快照舊的那則之後的變化也一定排在後面，重放完就是交易所的現況。
  - 防護：單的 `filled_qty` 不倒退，狀態依 `PENDING_NEW < NEW < ACCEPTED < PARTIALLY_FILLED < 終態` 不倒退，終態不復活。與現有紀錄相同的事件不發（只差時間戳也算相同）。
  - 排著的事件有上限（十萬則），超過就丟掉並記 log：之後的快照有狀態，成交由歷史補正落庫（D601）。
  - recon 跑完時事件連線已經又斷了：這份快照不套用，維持 `DEGRADED`，事件繼續排著，等下一次 recon（補上 E6-06 的縫）。
- **斷線期間消失的掛單記成 `UNKNOWN`。** OMS 裡活著、快照裡沒有的單，是在看不到的時候終結了（或從沒下成），結果不知道。recon 之後它在 OMS 與 `Recon.open_orders` 裡是 `UNKNOWN`，等 E6-08 查單（已有 `order_id` 用它，否則用 cloid；`Account.QueryOrder`，E6-16）收斂。沒有 client id 的不排查單。還是 `PENDING_NEW` 的單（交易所還沒回答）照舊。交易所之後對它的任何消息都算新的。
  - 不採用：E6-07 直接依賴 E6-16 查單。E6-08 本來就要接 `QueryOrder`，收斂邏輯放在一起。
- **商品換成 UID，數量換成平台單位（D203）。** 交易所的寫法先以活躍表的記憶體索引 `Reverse`，沒有再以 `includeInactive` 問 SYM（剛下架的商品，有逾時）。SYM 從沒見過的商品記 log、丟掉，不進 OMS 與帳本，也不發 `td.evt`。數量（`qty`、`filled_qty`、`Fill.qty`、`Position.size`）乘以 `ContractSize`：adapter 回報交易所的單位（OKX 合約是張數）。價格不換。
- **槓桿快取取自持倉的 leverage。** recon 與持倉事件帶的 `Position.leverage` 寫進快取；持倉平掉之後仍保留（槓桿是商品的設定）。沒有持倉過的商品回未知。不另外打 `Account.Leverage`。
  - 不採用：recon 時對每個商品打 `Leverage`。對哪些商品查沒有好答案，額度也不划算；策略需要「還沒下單就知道槓桿」時另開票。
- **account worker 從 Init 起連 NATS。** 用 `InitPayload.nats` 的 user JWT 與 seed（不落檔，`natsx.Options.UserJWT`、`Seed`），inbox `_INBOX_td_{api_id}`；載入帳號種類那個 venue 的活躍表並訂閱變更；訂閱 `td.view.{api_id}`，第一次 recon 之前就回答（D606）。連不上或載不到是 `INIT_FAILED`。

### D616 td-agent；執行中不檢查持倉模式；algo 由 STS agent 檢查

- **td-agent 照 MD agent 的寫法，在 `internal/td/agent`。** 讀兩個前綴：controller 寫的需求 `/td/{inst}/demand/{session}/{api_id}` 與 API server 寫的帳號 `/td/{inst}/account/{api_id}`。一個 `api_id` 的 owner 是它的需求集合；有 owner 就跑一顆 account worker（worker key `account/{api_id}`），最後一個 owner 離開後等 linger 才停（D901、D905）。需求離開時 ack 立刻刪掉，linger 留的是 worker。
- **不跑 worker、ack 寫 `REJECTED` 的帳號：** 不存在（`account_not_found`）、停用（`account_disabled`）、種類不認得（`unknown_kind`）、一期不跑的種類（`kind_unsupported`，D614）、封裝的憑證打不開（`sealbox.Reason`，D613）。帳號在 linger 期間被停用，worker 立刻停。
- **ack：** worker 回報 `READY` 或 `DEGRADED` 時是 `READY`（D603）；worker 以 `auth`、`hedge_mode`、`kind_unsupported` 結束時是 `REJECTED`，不重拉（D608）；重拉 10 次放棄時是 `DEAD`（P6）；其他是 `PENDING`。
- **spec：** 帳號 id、種類、封裝憑證的 digest、`cancel_on_disconnect_s`、這個種類的 endpoint。任何一項變了就換 incarnation：重封的憑證、改了 host。TD 沒有 live config，`Init` 帶全部。
- **spawn 時才解封**，明文只在組 `Init` 的那一刻。每次 spawn 以 MFTIKO account 的簽章金鑰簽一個新的 NATS user（`natsdev.Signer`，D611），JWT 不設到期：worker 重拉就換一份。
- **endpoint 比照 MD（D403）：** td-agent 的 `-endpoints`（帳號種類 → 私有 websocket URL）、`-rest-endpoints`（帳號種類 → REST URL）、`-simulated`，經 `InitPayload` 交給 worker，算進 spec。
- **持倉模式只在 Start 檢查。** 假設 worker 執行期間沒有人改持倉模式；改了也不偵測。`AccountState` 拿掉 `UNAVAILABLE`，103 只剩 `Submit.params` 帶持倉方向鍵的拒絕。修改 D607、D612。
- **algo_id 重疊不由 TD 檢查。** STS agent 檢查它自己 instance 上的 session；跨 STS instance 的重疊由使用者保證不發生。`DemandEntry.algo_ids` 留著，TD 不讀。修改 D602。
  - 不採用：TD 以需求的建立順序拒絕較晚的那筆（`algo_conflict`）。使用者行為受限，保持簡單。

### D617 以 order_id 查單、請求有效期、讀持倉模式（E6-16）

- **`exchange.Account` 加 `QueryOrder(now, ref CancelRef)`，回 `Call[*OrderUpdate]`。** E3 定案的介面（D006）在這裡擴充。一律走 REST：Bybit、OKX 的 WS 下單通道沒有查單。單已經有 `order_id` 時只帶它查，不帶 `client_order_id`；還沒有 `order_id` 才用 `client_order_id`。找得到已終結的單（成交、撤單），不只掛單。
  - 交易所說沒有這張單時，`Parse` 回 `exchange.ErrNotFound`，不是 `*Reject`，也不是一般錯誤：Binance 的 `-2013`、OKX 的 `51603`、Bybit `GET /v5/order/realtime` 帶 `orderLinkId` 回空清單。worker 只在過了有效期之後才把它讀成「沒下成」（D605）。
  - Bybit 以 `orderLinkId` 查時不看 `openOnly`，已終結的單只保留每個 category 最近 500 張；Binance UM 沒有成交、已撤或過期的單，建立三天後查不到。兩者都遠長於 worker 等待的時間。
- **有效期是 `AccountConfig.Validity`，由 worker 給，adapter 寫進請求。** 0 是 `exchange.DefaultValidity`（5 秒）；上限 `exchange.MaxValidity`（60 秒，Binance `recvWindow` 的上限），須為整毫秒，factory 建帳號時檢查。
  - Binance：每個簽名的 REST 請求，以及 WS API 的 `session.logon`、`order.place`、`order.cancel`，都帶 `recvWindow`。
  - Bybit：每個 REST 請求與 trade socket 每個下單撤單 frame 的 `X-BAPI-RECV-WINDOW`。
  - OKX：`Submit`、`Cancel`、`CancelBatch` 的 `expTime` header，值是簽名時間加 `Validity`（Unix ms），不在 prehash 裡。讀取不帶。
  - 放在 `AccountConfig`，不放在每個請求：有效期是部署設定（D605），一個帳號只有一個值，trade stream 的 codec 也從同一處拿。worker 把它接到旗標，和用它算查單時點，屬於 E6-08；在那之前一律是預設 5 秒，與原本寫死的 `recvWindow` 相同。
- **持倉模式由 `Snapshot` 讀（D612）：**
  - Binance UM：多一個 `GET /fapi/v1/positionSide/dual`（weight 30），`dualSidePosition` 為真即是。現貨沒有持倉，恆為假。
  - OKX：多一個 `GET /api/v5/account/config`，`posMode` 是 `long_short_mode` 即是；不認得的值是錯誤。
  - Bybit：v5 沒有讀持倉模式的 API（只有 `switch-mode`，是寫入），帶 `settleCoin` 的持倉清單也不列空倉。所以以掛單與持倉的 `positionIdx` 判斷：1 或 2 只會出現在雙向持倉。空帳號讀起來是單向，`Start` 的檢查會放行；持倉模式只在 `Start` 檢查（D616），之後也不會再發現。在雙向模式下不帶 `positionIdx` 的單會被交易所拒絕，所以不會在錯的假設下成交，只是這個帳號下不了單。
  - 不採用：Bybit 以 `switch-mode` 試探（已是該模式時回錯誤碼）。那是寫入，帳號有倉位或掛單時還可能真的切換失敗或改到設定。
  - 不採用：Bybit 對每個要交易的商品帶 `symbol` 查持倉（帶 symbol 時空倉也會回 `positionIdx`）。`Snapshot` 不知道要交易哪些商品，請求數也隨商品數長。
- **假帳號（D612）：** `AccountVenue` 保留已終結的單，`QueryOrder` 查得到；`Outcome.Late` 讓 `Submit` 或 `Cancel` 在路上耽擱：`Do` 回 `ErrUnknownOutcome`，交易所什麼都不做，直到測試呼叫 `Arrive(cloid, at)`。`at` 不晚於「簽名時間＋`Validity`」就照常生效並推事件，晚了就被拒、什麼都不變。只有 REST 的請求能晚到；`TradeVenue` 沒有。
- **conformance：** 四家都有查單的資料（掛單、部分成交、成交、撤單、不存在）與持倉模式的資料（單向、雙向；Bybit 是帶 `positionIdx` 1、2 的掛單與持倉）。這些回應是依交易所文件手寫的，不是 testnet 錄下來的；在 E6-12 到 E6-14 的 testnet 驗收時以真實回應核對。

### D618 cloid 冪等（E6-08）

- **D618 cloid 冪等：** TD 只在記憶體記得本次執行見過的 cloid（最近 24 小時或 10 萬筆，取先到者），不持久化。重啟後靠 recon 取回仍在交易所的單，已結束的單交由交易所自身的重複 cloid 檢查。
- 修改 D605：冪等不是「同一個 incarnation 裡還活著的單」。這次執行結束就忘掉；重啟之後不認得已結束的單。

### D619 同 cloid 內容不同（E6-08）

- **D619 同 cloid 內容不同：** 同一 cloid 重送但內容不同，回 106 `INVALID_ORDER`，不送交易所。內容相同則回原結果。
- 比的是 `uid`、`side`、`order_type`、`qty`、`quote_qty`、`price`、`tif`、`reduce_only`、`params`、`algo_id`，不含 `session_id` 與 `recv_window_ms`。數量與價格用刻度相等（D008）。本地拒絕也記下來，重送回原本那一次的結果，不送出。recon 取回、記憶體裡還沒有的活單：重疊的欄位相同回原結果（`accepted`），不同回 106，而且這次不對的嘗試不記進記憶體。撤單不是同一筆 cloid 的重送。

### D620 有效期（E6-08）

- **D620 有效期：** `Submit`/`Cancel` 加有效期欄位（即 Binance `recvWindow`、Bybit `recv_window`、OKX `expTime`），由策略或 SDK 填（SDK 預設 5 秒，對應 E8 D803）。0 表示用 TD instance 預設值，超過交易所上限就壓到上限。時鐘誤差由 TD instance 設定（預設 1 秒）。此欄位一併納入 E6-15 td proto 定案。
- 修改 D617 的「不放在每個請求」：`AccountConfig.Validity` 仍是 TD instance 的預設（0 是 5 秒，上限 60 秒，須為整毫秒），每一筆 `Submit`、`Cancel` 的 `recv_window_ms` 可以蓋過它。0 用預設，負值當 0，超過上限壓到上限，不拒絕。worker 接到 `-validity`（0 即 5 秒）與 `-clock-skew`（預設 1 秒）。

### D621 本地限速（E6-08）

- **D621 本地限速：** 用到 `OrderRate` 的 90% 就回 108 `RATE_LIMITED`。
- 送出前檢查，`used * 10 >= limit * 9` 就拒絕。`Submit` 與 `Cancel` 都算；本地拒絕與冪等重放不扣額度。429 與 418 仍照 D420 停送，不排隊、不重試。

### D622 查單失敗（E6-08）

- **D622 查單失敗：** 指數退避 1–30 秒並持續重試；超過 5 分鐘仍查不到結果，帳號標成 `DEGRADED`。
- 用既有的 `ACCOUNT_STATE_DEGRADED`，不另開狀態。reason 是 `query_failed`；已經是 `stream_lost` 時不改寫。事件連線斷線會把 `query_failed` 改成 `stream_lost`。新單仍是 102，撤單照常，查單繼續重試。5 分鐘從該筆查單的第一次失敗起算。每一筆失敗中的查單都有結論（查到或 `ErrNotFound`），而且事件連線在，才回到 `READY`。重連後的 recon 若先回到 `READY`，還沒查到的單仍可能再標成 `DEGRADED`。限速停送不算查單失敗。

### D623 撤 UNKNOWN 單（E6-08）

- **D623 撤 UNKNOWN 單：** 狀態為 `UNKNOWN` 的單可以用 cloid 撤，不回 107。`Cancel` 只收 `client_order_id`（欄位 4 已刪，號碼不重排，D629）。OMS 以 cloid 找到活單，送出時帶上那張單的 `order_id`；還沒有就用 `client_order_id`。

### D624 WS 斷線（E6-08）

- **D624 WS 斷線：** `TradeStream` 不可用時改走 REST，不拒單。
- 請求還沒寫出去才改走 REST。寫出去之後的錯誤是 `ErrUnknownOutcome`，不重送、不改走 REST。`DEGRADED` 時撤單走 HTTP（D603）。

### D625 範圍（E6-09）

- **D625 範圍：** E6-09 只做 `BatchCancel`。cancel-on-disconnect（Binance `countdownCancelAll`、OKX `cancel-all-after`、Bybit DCP）不做：`AccountSettings.cancel_on_disconnect_s` 與 adapter 的 `CancelOnDisconnect` 不接，完成標準第 3 條拿掉。
- `BatchCancel` 帶明確的 `client_order_id` 清單（`order_id` 欄位已刪，號碼不重排，D629），對應一筆交易所批次撤單。TD 以 client id 找到 OMS 裡的活單，送出時改用那張單的 `order_id`（還沒有才用 client id）。不依 `algo_id` 或 `uids` 挑單，也不檢查這些單屬於哪個 algo。解不開成平台 cloid 的 client id 不屬於任何 algo，但仍能指名那張單。TD 一對一轉發，不分批、不排隊、不重試。
- 交易所沒有批次撤單（`Limits.CancelBatch` 為 1，例如 Binance 現貨）時，TD 本地拒絕：這個方法不能用，不改走單筆 `Cancel`。
- 清單長過交易所的批次上限時仍送出，交易所的拒絕原樣回報。
- 清單裡任何一張對不到活單，或商品解不出來，只拒絕這一筆，什麼都不送；別的 `BatchCancel` 不受影響。
- 「一次撤掉這個 algo 的全部掛單」改由 STS SDK 做：SDK 知道自己的活單，讀 `td.view` 回覆的 `cancel_batch`（這個帳號的 `Limits.CancelBatch`，D631）自己拆批，沒有批次撤單就改走單筆 `Cancel`。這一期的 TD 不做。

### D626 批次結果不明（E6-09）

- **D626 批次結果不明：** 這筆批次撤單的結果不明（`ErrUnknownOutcome`）時，批內每張單照 D622 查單收斂（已有 `order_id` 用它，否則用 cloid），不重送。

### D627 DEGRADED（E6-09）

- **D627 DEGRADED：** 帳號 `DEGRADED` 時 `BatchCancel` 照常，與 `Cancel` 相同（D603）。

### D628 撤單限速（E6-09）

- **D628 撤單限速：** `Cancel` 與 `BatchCancel` 不受 D621 的 90% 本地餘量限制，可用到 `OrderRate` 的 100%；交易所回 429/418 時仍依 `Retry-After` 停送（D420）。
- 修改 D621：90% 只適用 `Submit`。`Cancel` 算一張。`BatchCancel` 算一筆，不問清單裡有幾張；不再因為張數放不進窗口而整筆回 108。放不進這一筆才回 108，什麼都不送、不排隊。

### D629 定案前刪掉 `reserved`（E6-15，#175）

- **系統尚未上線。** `td.proto` 裡已刪欄位留下的 `reserved` 直接刪除，不保留。現有欄位號碼不重新編號。`tdctl.proto` 沒有 `reserved`。
- **刪掉的是：**
  - `AccountState` 的 3 `ACCOUNT_STATE_UNAVAILABLE`（D616）
  - `OrderRequest` 的 3 `cancel_session`（D602）
  - `Cancel` 的 4 `order_id`
  - `BatchCancel` 的 1 `algo_id`、2 `uids`、3 `timeout_ms`
  - `BatchCancelOrder` 的 2 `order_id`
  - `OmsViewRequest` 的 1 `session_id`（D602、D610）
  - `OmsView` 的 2 `incarnation`、3 `as_of_ns`（D606）
  - `LedgerView` 的 3 `incarnation`、4 `as_of_ns`（D606）
  - `AccountEvent` 的 12 `order_reject`、13 `cancel_reject`（D610）、16 `account_reset`（D603）
  - `Order` 的 12 `reject_reason`（D610）
- **這兩份仍是草案。** 留在 `buf.yaml` 的 `breaking.ignore`。PROTOCOL §1 仍是草案。移出草案清單、把 §1 改成定案，是這份合併之後的後續。`buf breaking` 的 `RESERVED_MESSAGE_NO_DELETE` 與 `RESERVED_ENUM_NO_DELETE` 會擋先移出清單再刪 `reserved`，所以順序是先刪、合併、再移出。
- **`OmsViewRequest.settled` 不在 D629 的刪除裡。** 它當時不是 `reserved`。拿掉並改成 `reserved` 是 D632。
- **修訂 D623、D625 裡「order_id 欄位保留」的說法。** 那兩個訊息上的 `order_id` 欄位已刪，號碼不重排。OMS 送出時仍帶那張單的 `order_id`，那是交易所那一側的身分，不是訊息上的欄位。

### D630 歷史補正欄位保留（E6-15，#175）

- **`AccountSettings` 的補正 interval 與手動觸發照現狀保留。** `backfill_interval_s`、`backfill_requested_at_ns` 屬暫緩的 E7（#9）。定案 `tdctl.proto`、`td.proto` 時不拿掉。它們在 `state.proto`，`state.proto` 仍是草案。
- **`td.backfill` 相關欄位照現狀保留。** `tdctl.Report.first_submit_ns`（backfill 的範圍從下單決定，D601）留下。PROTOCOL 裡 `td.backfill.{venue}` 的契約留下。不另刪欄位。
- 後續把這兩份 proto 移出草案清單時，這些欄位一起留下。

### D631 `cancel_batch` 給策略（E6-15，#175）

- **td proto 原本沒有把帳號的 `CancelBatch` 給策略。** `Limits.CancelBatch` 只在 account worker 裡。策略經 `td.order`、`td.view`、`td.evt` 看不到。
- **每一則 `td.view` 回覆都帶 `ViewReply.cancel_batch`。** 值是這個帳號的 `Limits.CancelBatch`。1 表示交易所沒有批次撤單（D625）。問 OMS、帳本、槓桿，或請求是空的，回覆都帶。從連上 NATS 起就有，不必等 `Start`。
- **SDK 照這個數拆批（#211）。** 修訂 D602、D625：SDK 讀的是 `cancel_batch`。值是 1 就改走單筆 `Cancel`。TD 仍不挑單、不拆批。
- 欄位號碼 14，接在 `seq` 後面。不重用已刪的號碼。

### D632 拿掉 `OmsViewRequest.settled`（E6-15，#175）

- **`settled`（欄位 2）拿掉。** 它沒有作用（D615）：OMS 只放活著的單，終結的單從 `td.evt` 與落庫的歷史拿。
- **比照 D629，改成 `reserved`，不重新編號。** `OmsViewRequest` 寫 `reserved 2;` 與 `reserved "settled";`。`algo_id` 仍是欄位 3。
- **這條 `reserved` 留下。** D629 刪的是定案前已經存在、系統尚未上線所以不保留的舊 `reserved`。這條是這次刪欄位時加上的。後續把 `td.proto` 移出草案清單時不刪它：那一版若再刪，`buf breaking` 的 `RESERVED_MESSAGE_NO_DELETE` 會擋。

### D905 linger；需求沒有 TTL

- **需求項目沒有 TTL（D901）。** 原本「STS agent 消失超過 TTL 就回收」的路徑整個拿掉，需求只在 controller 明確刪除時消失。
- **linger 只有一層，MD、TD 同一個概念：** owner 集合歸零後，等 linger 才釋放資源。
  - MD：atom 的 owner 歸零 → linger → 退訂。連線在最後一個 atom 退訂時就停 worker，不另設連線的 linger。
  - TD：帳號的 owner 歸零 → linger → 停 account worker。最後一個需求項目的 linger 就是帳號的 linger。
- **linger 吸收的是 controller 的短暫反覆：** 例如比對時刪掉又馬上加回同一個項目。其他情況都不靠 linger：
  - 期權鏈重新置中：由 orchestrator 先訂新的、再退訂舊的，置中時一直聽 perp 的行情，不會反覆進出。
  - 換參數重開 session：中間的空白無法控制，照規則走，該退訂就退訂。
  - tape 保溫：跑一個 tape keeper 策略（D404），tape 跟著它的生命週期走。
- **預設值：**
  - linger：5 秒。
  - 選主 session 的 TTL：15 秒。`AgentRecord` 綁在它上面（D004），所以它也決定 controller、API 多久之後看到 agent 不在。ack、`WorkerStatus` 不綁 lease，agent 不在時照樣留著，controller 維持現狀（D901），不會因此回收資源。
- 這些值是 MD、TD、STS instance 的部署設定，不放進 session spec。

### D404 沒有常駐訂閱，行情只由策略持有

- MD 的 owner 只有 session 的需求項目。拿掉常駐訂閱（`StandingSubscription`）這個 API，少一個特殊的入口。
- 要讓行情或 tape 一直保持熱的，就跑一個 **tape keeper**：不下單、不綁帳號的策略，跟 mftik 一樣。它和其他策略一樣經 API 開始、結束。
- 使用者的認知只有一條：行情由策略持有，要全部關掉行情，就是把策略全部關掉。
- tape keeper 的需求和其他 session 一樣由 controller 發、不綁 TTL：controller 或 STS agent 停機時，它持有的 atom 不會被釋放。

### D405 `version` 是大版本

- `version`（uint8）是大版本。只有行情 schema 出現 breaking change 時才升。envelope 加任何欄位也算 breaking，要升版本。
- 讀到不認識的版本，讀取端拒收。不靠長度去跳過後代的尾巴：加欄位就是新版本，舊的讀取端不讀。

### D406 v1 envelope 固定 40 bytes

- 長度由版本決定。v1 是 40 bytes，payload 緊接在後面。比 40 bytes 短的訊息拒收。
- **拿掉 `len`。** offset 1 到 7 全部是保留欄位：寫入填 0，讀取端看到不是 0 就拒收。不留一個必須等於 40 的 byte。保留欄位只有一種合法值，讀取端只檢查是不是 0。
- 不採用：用 `len` 跳到 payload，並容忍比 32 bytes 長的後代 envelope。那是加欄位仍相容的做法；D405 把加欄位定義成 breaking。

### D407 little-endian，用 `encoding/binary`

- 欄位是 little-endian，用 `encoding/binary` 讀寫。
- 不用 `unsafe`。NATS 的 `msg.Data` 不保證 8-byte 對齊。
- 編碼與解碼要 0 alloc。`contract` 只依賴標準庫與 protobuf，所以 xxHash64 寫在 `contract/wire/envelope` 裡，不另加函式庫。

### D408 `Append`、`Parse`、`MDTsNs`

- 實作在 `contract/wire/envelope`。`Append` 寫一則 v1（envelope 接 payload），`Parse` 讀一則，`MDTsNs` 只讀 `md_ts_ns`，給 prehook。三個都做 D406 的拒收檢查。
- 附 golden bytes。`Parse` 把 payload 指回輸入的那段，不複製。

### D409 `seq` 與 `conn_epoch` 留在 envelope

- `seq`（uint64）和 `conn_epoch`（uint64）做兩件事。一是傳輸層的連續性：conn worker 到策略之間的缺口，包括 prehook 丟棄和 NATS 掉訊息。二是 tape 和 `md.view` 的對齊（D402）。不必解開 payload 就能檢查連續性。
- 行情本身的連續性另外由交易所在 payload 裡的序號負責（Binance `U`／`u`／`pu`、Bybit `seq`）。兩層並存。
- `seq` 以 `(feed, conn_epoch)` 起算，重連後從 1 重新起算。conn worker 重啟後 `conn_epoch` 要繼續單調遞增。遞增是 conn worker 的事，不在 envelope 的讀寫裡：下一個號先寫進 C4 的 `Report`，寫完才撥，`md.f` 才用它（PROTOCOL §6.2）。

### D410 `digest`

- `digest`（uint64）是只對正規化後的 payload（不含 envelope）算的 xxHash64，seed 0。
- 用途是以後跨連線去重。去重這次不設計也不實作，讀取端不核對 digest，照讀出。
- 寫入的一端一律要算出這個值。`Append` 自己算。

### D411 v1 的位元組

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
### D412 dev NATS 用 operator／account JWT 與 full resolver

- **dev 與之後的部署用同一種授權。** operator JWT、account JWT、full resolver。不用 MEMORY resolver：account 變更不必重載設定，多台 server 也不必改設定。
- **account JWT 在 server 啟動前放進 resolver 目錄。** 檔名是 account 的公鑰加 `.jwt`。不走 `$SYS.REQ.CLAIMS.UPDATE`。這是兩種寫法裡較簡單的一種。
- **resolver 目錄可寫。** compose 把該目錄掛成 volume。種子與 user creds 不掛進容器。
- **要有 system account。** 名稱 `SYS`，只給 resolver 與 server 監控，沒有 user。
- **CI 的嵌入式 nats-server 用同一份設定。** 同一個 formatter 產生 compose 的設定與測試的設定：resolver 種類、operator、`SYS`、`MFTIKO` 相同，listen 位址用測試的自由 port。
- **不開 TLS。** 這一票只有一台 dev server。多 region 的拓撲見 PROTOCOL §10 P16。

### D413 金鑰不進 repo

- **小工具 `cmd/mftiko-nats-dev` 產生金鑰，不用 `nsc`。** `make dev-up` 先跑它，再起 compose。
- **輸出在 `deploy/dev/data/nats/`，這個目錄被 gitignore。** 裡面有 operator、`MFTIKO`、`SYS`、簽章金鑰，以及 MD 與 md.fetch 的 user creds。沒有 session 的 user JWT：那是 STS 在 spawn 時簽的（D906）。
- **金鑰檔已存在就沿用，JWT 與 creds 重簽。** 再跑 `make dev-up` 不換 account 身分。種子檔壞了或是空的就失敗，不靜默換一把新的。
- **repo 裡沒有種子。** 公鑰、設定、account JWT 可以出現在本機輸出目錄，不提交（GitGuardian，見 #122）。

### D414 一個 MFTIKO account

- **所有平面共用一個 account `MFTIKO`。** 同一個 subject 空間，沒有 import／export。
- **`MFTIKO` 另有一把簽章金鑰，給 STS 簽 session user JWT，TD agent 簽 account worker 的 user JWT（D611）。** account 的身分金鑰不外流，只留在 gitignore 的目錄。MD 與 md.fetch 的 user JWT 由 account 身分金鑰簽，不由這把簽章金鑰簽。
- **`SYS` 是 system account**，給 resolver 與監控，沒有業務 user。

### D415 MD 與 md.fetch 兩個 user

寫進 PROTOCOL §7。TD 與 SYM 的權限留到它們自己的票，這一票不加（TD 見 D611）。subject 拼法已由 D455–D457 取代；下面是新的字串。請求 subject 仍是恰好 `sym`。

- **MD instance（`MFTIKO_NATS_CREDS`）：** publish `md.v1.f.{inst}.>`、`sym`；subscribe `md.v1.view.{inst}.>`、`sym.v1.changed.>`。conn worker 只訂自己的 venue：`sym.v1.changed.{venue}.>`（D457）；JWT 仍是上面的萬用字元，一個 instance 可以有多個 venue。Init 載入自己那個 venue 的活躍表，變更通知到了就重載（D203、D426）。inbox 見 D416。
- **md.fetch（D402，無狀態，不是 MD worker）：** queue group subscribe `md.v1.fetch.>`（每個 replica 只訂閱部署設定裡它連得到的 `md.v1.fetch.{venue}`，D418）；subscribe `sym.v1.changed.>`（NATS Resolver，D422）；publish `sym`。inbox 見 D416。可 request `sym` 是 publish 恰好 subject `sym`，不是 `sym.>`。恰好 subject `md.v1.fetch` 不能訂。
- **回覆權限。** D416 不給 `_INBOX.>` 的 publish，服務者仍要能回一則。兩個 user 的 JWT 帶 NATS 的 response permission，上限沿用 server `allow_responses` 的預設：一則、兩分鐘。這不是新的決策編號。session 的呼叫者沒有這條，它只訂自己的 inbox。

### D416 inbox 前綴

- **session 維持 D906 的 `_INBOX_{session_id}`。**
- **平台行程用 `_INBOX_{plane}`，不含 instance。** MD 的 plane token 是 `md`，所以是 `_INBOX_md`。md.fetch 的 plane token 是 `fetch`，所以是 `_INBOX_fetch`。歷史補正服務的 plane token 是 `backfill`，所以是 `_INBOX_backfill`（D601）。同一個平面的每個 instance 共用這個前綴。`fetch` 與 `backfill` 是讓前綴不撞，記在這一條，不另編號。
- **權限只開自己平面的前綴**（`_INBOX_md.>`、`_INBOX_fetch.>`、`_INBOX_backfill.>`、`_INBOX_td.>`）。account worker 是 `_INBOX_td_{api_id}.>`，一份憑證一個前綴（D611）。沒有任何 user 拿到預設的 `_INBOX.>`。
- **同一個平面的 instance 可以讀到彼此的回覆。** nats.go 在前綴後面加隨機 nuid，平常的 mux 只訂 `_INBOX_{plane}.<nuid>.*`，所以不會自動收到別的 instance 的回覆，inbox 也不會撞。這把平面憑證仍然可以另外訂閱整個 `_INBOX_{plane}.>`，讀到該平面每一個 instance 的回覆。這是接受的：系統是單人使用，跨 region 取資料是設計目的，因為有些交易所鎖 region，要另一個 region 轉發。
- **`natsx/conn` 加 inbox 前綴選項**，傳給 `nats.CustomInboxPrefix`。空的維持客戶端預設。`natsx/rpc` 的介面不改。

### D417 權限檢查

- **CI 用嵌入式 nats-server 載入同一份設定與產生的 creds**，檢查三件事：發佈 `md.f`；用自訂 inbox 做 request／reply；未授權的發佈與訂閱被拒（另一個平面的 inbox、session 的 inbox、`_INBOX.>`，以及另一個 instance 的 `md.f`）。
- **檢查會在記憶體裡用簽章金鑰簽一個暫時的 session user**，當合法的呼叫者。不把 session creds 寫進磁碟。
- **md.fetch 的 queue 名稱在檢查裡用 `md.v1.fetch.Binance`。** 這是一個 venue subject；natsx 的 `Serve` 在 queue 空著時用 subject 本身（D418）。恰好 subject `md.v1.fetch` 不能訂。JWT 不鎖 queue 名稱。
- **compose 的煙測腳本跑同一組檢查：** `deploy/dev/nats-smoke.sh`。
- **不開 TLS。**

### D418 md.fetch 的 subject 分 venue

- **subject 字串已由 D456 取代。** 現在是 `md.v1.fetch.{venue}`。venue 仍是人讀的名字，與 `sym.v1.changed.{venue}.{category}` 的 venue 相同。下面的分 venue、queue group、不選 region、權限萬用字元仍有效。沒有版號的舊寫法不再用。
- **每個 replica 只訂閱它連得到的 venue。** queue group 的名稱是 subject（natsx `Serve` 在 queue 空著時的預設）。哪些 venue 由部署設定決定，不是全部。
- **程式不寫跨 region 路由。** queue group 優先本地，本地沒有 responder 才轉到別的 region。
- **權限用 `md.v1.fetch.>`。** 這是「每個 venue 一條」與「萬用字元加上部署清單」裡較簡單的一種。行程仍只訂清單裡的 venue。session 的 publish 是這個萬用字元（D906、D456）。SDK 的 fetch 走 `md.v1.fetch.{venue}`，venue 取自 ticker，不選 region。
- **恰好 subject `md.v1.fetch` 沒有 venue，不能訂。**

### D419 K 線分頁與上限

- **`limit` 不在 1 到 5000 回 `invalid`。** 5000 是單次最多的已收盤 K 線。
- **超過 `KlinePage()` 時**，服務用 `Klines(endNs)` 從已拿到的最早一根往回翻。回覆只有已收盤的，由舊到新。某一頁失敗就整筆失敗，不回半頁。邊界那根若又出現，丟掉，不重試。
- **回覆達到或超過 NATS 的 1 MiB 回 `invalid`，不送出。** 5000 根的大小由測試驗證，在這個上限以下。

### D420 限速

- **`exchange.Used` 帶 `ResetAt`。** 這是該計數器歸零的絕對時間。`Usage(resp, now)` 由呼叫者傳入收到回應的時間，adapter 不讀時鐘（D302）。Binance 依 bucket 的窗口對齊下一個邊界：`weight_1m` 與 `orders_1m` 是 1 分鐘，`orders_10s` 是 10 秒，`orders_1d` 是 24 小時，對齊 Unix epoch。回應剛好落在邊界上時，屬於剛打開的那一窗，歸零時間是下一個邊界。Bybit 讀 `X-Bapi-Limit-Reset-Timestamp`（毫秒）。各 venue 真實的窗口與標頭在 E4-09 到 E4-11 驗收時確認。
- **用 `PublicREST.Usage` 記下交易所在回應裡回報的用量。** 這個數字含同一 IP 上的其他人，包括同主機的 conn worker。
- **到 `ResetAt` 用量作廢。** 該 bucket 不再算進閾值，預先拒絕跟著解除。沒有 `ResetAt` 的用量不會自己過期。
- **閾值是設定。** 未設定（0）不預先拒絕：平台不知道交易所的上限。設了之後，任一尚未作廢的 bucket 的回報用量達到閾值，下一筆對該 venue 的呼叫不送出，回 `rate_limited`。
- **429 與 418 用 `RateLimited.RetryAfterMs`。** adapter 從 `Retry-After`（秒）填上。這段時間內對該 venue 的呼叫都不送出。418 是 IP ban：期間內一筆都不送，也不送探測。時間到了才允許下一筆真正的請求。回應裡的 `ResetAt` 比較早也不提前放行。
- **429 沒有 `Retry-After` 時等到回報過的 `ResetAt`。** 有多個 bucket 就等到最晚的那個，讓回報過的計數器都已歸零。`ResetAt` 也沒有，才用可設定的保守預設：行程旗標 `-rate-hold`，預設 1 分鐘。418 沒有 `Retry-After` 時同樣不送出，用同一個後備。
- **不排隊、不重試。** 只送一次。

### D421 不快取

- **這一版不快取。** 每次請求都打交易所。服務維持無狀態。

### D422 商品解析

- **ticker 經 NATS Resolver 換成 `symbol.Info`。** 只查活躍表。不在活躍表的（含已下架、已到期）回 `invalid`。
- **ticker 的 venue 與 subject 的 venue 不符回 `invalid`。** 在查表之前。
- **`ErrUnsupported` 回 `unsupported`。** 其他讀取失敗回 `venue_error`。Resolver 暫時不可用不是這四個業務碼，走 natsx 的錯誤回覆。
- **md.fetch user 另外 subscribe `sym.v1.changed.>`**（D457），Resolver 才能收到變更通知。publish `sym` 維持 D415。

### D423 位置與部署

- **程式在 `internal/md/fetch`，執行檔是 `cmd/mftiko-mdfetch`。** 平台服務，跟 SYM 一樣隨平台發版、逐台重啟 replica。不放進 md-agent，不是 worker artifact。
- **連線用 md.fetch 的 user creds（`MFTIKO_NATS_CREDS`）與 `_INBOX_fetch`。** `natsx.Options.CredsFile` 把 creds 交給客戶端。

### D424 重抓節奏

- **快照 HTTP 失敗用指數退避。** 1 秒起，加倍，上限 30 秒，與連線重撥同一條曲線。成功套進本地本的一張把這個窗口歸零。
- **429 與 418 不走這條指數。** `Retry-After` 優先。沒有就用這一回 `Usage` 裡最晚的 `ResetAt`。兩個都沒有，用行程旗標 `-rate-hold`，預設 1 分鐘，跟 md.fetch 同一個預設。這段時間不送出，418 也一樣，不送探測。`-rate-hold` 不在 `InitPayload`。
- **不跟 md.fetch 共用計數。** 各自讀自己那次回應的 `Usage`。conn worker 不設預先拒絕的權重閾值。
- **等快照的緩衝有兩個上限，都在 `InitPayload`。** 0 表示預設 10000 筆、10 秒。任一個到了（含等於）就丟掉緩衝，進行中的那次 Depth 作廢，重新要一張。時間從這一次嘗試的第一筆緩衝更新起算。
- **斷號與 checksum 失敗的第一次重抓不等這段指數。** HTTP 失敗才走。

### D425 簿的訊息佔 seq

- **`orderbook_delta` 是唯一的 book feed。** 每發布一則佔該 feed 的下一個 envelope `seq`。沒有發出去的變化不佔號。
- **`reset` 是一則 `orderbook_delta`（`reset` 為 true），佔這個 feed 的 seq。** 內容是窗內的完整 book（D430）。
- **第一次成功的快照，在把緩衝裡接得上的更新套完之後，也推一則 `reset`。** 重抓同樣。套用緩衝的過程不逐筆推增量。
- **斷號或 checksum 失敗不發布那一筆。** checksum 失敗的那筆不留在緩衝裡重套。
- **重抓不改 `conn_epoch`。** 序號在同一條連線上連續。重連才換 epoch，本地本跟著那條連線丟掉。
- **交易所序號只給 `BookSync.Check`。** 不進 envelope，也不進 `OrderBookDelta`。

### D426 一張快照加增量（SnapshotREST）

- **SnapshotREST 抓一張快照，之後只套增量，不自己重抓。** 靠近盤口的檔更新得快，價格走到某一檔之前，增量已經把它更新過了。
- **重抓只由斷號（`BookResync`）或 OKX checksum 失敗觸發。** 沒有價格邊界，也不因距離重抓。視窗滑動不重抓；窗外的檔見 D431。`md.view` 回的是窗內的 book（D430、D441）。
- **交易所回多少檔就用多少。** REST 的 limit 是 `BookSync.Depth()`（D453）。不因為少於某個檔數而重抓，也不因為想要的檔數大於 `Depth()` 而退出。沒有 N。
- **SnapshotStream 不打 REST。** 全本就是串流給的。斷號或 checksum 丟掉本地本，並只重訂那一個 channel（D434）。下一則串流快照是 `reset`。快照之前緩衝的增量在快照到達時丟掉。
- **`exch_ticker` 來自該 venue 的活躍表。** conn worker 在 Init 載入這張表，並跟著 `sym.v1.changed` 重載（D457）。Agent 只把 `topic.UniversalTicker` 送進 worker 的 reconcile loop；worker 用表上的 `symbol.Info` 呼叫 `Market.Atom`，自己 diff 要訂與要退的 channel。不從 channel 解析。沒有這份表時才用 assignment 上的 atom id 與 `exch_ticker`（測試）。SnapshotREST 沒有這個欄位就不打 Depth，只記日誌。

### D427 `orderbook` 不節流

- **已由 D453 取代。** 沒有 top-N，也沒有 `orderbook` 這個 feed。增量原樣轉成 `orderbook_delta`（D432），沒有另外的節流。推送頻率的上限就是交易所自己的推送頻率；策略跟不上由 prehook 丟（D803）。

### D428 以交易所序號為權威

- **序號正確就套用。** 不另外檢查交叉簿。
- **只有斷號（`BookResync`）或 `BookChecksum` 失敗才因為序號重抓並推 `reset`。** OKX 的 checksum 仍是 E4-11；有實作就呼叫。

### D429 fixture

- **從公開端點錄，不需要 API key。** BinanceUM 的 depth session 加 REST 快照，Bybit 的全本串流。fixture commit 進 repo，測試只重播，不連線。
- **OKX 留到 E4-11。**

### D430 刻度環狀訂單簿

- **本地 book 是 `sdk/book` 的環。** STS worker 與 MD conn worker 用同一份。bids、asks 各一個固定長度的 slice，長度 N，預設 2048，必須是 2 的冪，索引是 `tick & (N-1)`。這一 N 是環長。沒有發布的 top-N（D453）。
- **每一格只存 `tick int64` 與 `size float64`。** 不另存 bool，也不把格子清掉。讀的時候，格子裡的 tick 與要的 tick 不同就是空的；`size == 0` 是刪除。
- **價格換成 tick 用 `round(price / price_tick)`，一半進位遠離零。** 從環上讀回的價格是 `tick * price_tick`。`price_tick` 見 D433。
- **`BestBidTick`、`BestAskTick` 是目前的最佳 tick。** 比最佳價更好、而且 size > 0 的價格改寫最佳。最佳檔被刪掉時，往外掃到下一個 tick 相符且 size > 0 的格子。
- **Iterator 從最佳價往外走，跳過空格，不配置記憶體。** `reset` 與 `md.view` 走完整個窗（D441）。

### D431 窗外的檔

- **離最佳價達 N 個 tick（含）的更新，在遠端那一側，套用時丟掉。** 窗是 N 個連續 tick，最遠是 N-1；再遠一格與最佳價落在同一個索引。
- **同一則更新由最佳價那一側先套。** 每一檔用套到它的那一刻的最佳 tick 判斷在不在窗內。
- **被丟掉的深檔不會因為最佳價後來移近就自己出現。** 格子不清，有效與否只看 tick 是否相符。等下一筆增量再寫進那一格。視窗滑動不重抓（D426 的重抓條件不變）。

### D432 `orderbook_delta` 原樣轉發

- **`orderbook_delta` 轉發正規化後的增量，內容與順序都不改，worker 不先過濾。** 不對整本做 diff。qty 0 仍是刪除。
- **丟不丟只在 `sdk/book` 套用時決定，依那一刻的最佳 tick。**
- **`reset` 是窗內的完整 book。** 第一次成功的快照與重抓都是這則 `reset`。套用緩衝的過程仍不逐筆推增量（D425）。

### D433 `price_tick` 在指派上

- **`price_tick` 是 SYM 的 `price_tick`。** 跟 `exch_ticker` 同一張活躍表、同一時刻。conn worker 在推出 atom 時從記憶體讀。沒有這份表時才用 assignment 上的值。
- **缺，或小於等於 0，這本訂單簿只記日誌、不維護。** 不打 Depth，也不套用串流。

### D434 SnapshotStream 斷號後重訂該 channel

- **Bybit 與 OKX 的全本只在訂閱之後由串流送來。** 斷號或 checksum 失敗把本地本丟掉之後，後面的增量不會再變成一張快照。不重訂的話這本簿停在斷掉之前的窗內狀態，直到整條連線重連。
- **從已確認的訂閱拿掉這個 channel，經現有的訂閱路徑先退訂再訂閱。** 仍由 session 的速率限制發幀。不拆整條連線，其他 channel 不動。`conn_epoch` 不變。
- **每一本簿自己一條指數退避，與 D424 相同：1 秒起，加倍，上限 30 秒。** 成功裝上新的串流快照把這本的窗口歸零，下一次斷號再從 1 秒起。已經在退避或重訂途中的，不另起一次，所以連續斷號不會熱迴圈。
- **下一則串流快照裝上本地本，並推 `reset`。** 快照之前緩衝的增量丟掉（D426）。這一版不打 REST。

### D435 `md.f` 不變，`md.view` 的 tape 不帶 envelope

- **`md.f` 仍是 40-byte envelope ＋ protobuf `FeedEvent`（D405–D411）。** ring 存發佈出去的整段 bytes：發一次、存一次，不重新編碼。
- **`md.view` 回 tape 時去掉每則的 envelope。** 只帶對齊要用的欄位與 `FeedEvent`：`TapeEvent` 是 `conn_epoch`、`seq`、`md_ts_ns`、`event`。`TapePage.events` 是 `repeated TapeEvent`。不帶 `digest`。
- **不重新編碼。** embedded message 與 bytes 的 wire 格式相同。worker 以 protowire 把 ring 裡 offset 40 之後的 payload 原樣寫進 field 4，不 unmarshal 再 marshal。
- **`read.proto` 已在 E4-12 定案（D460）。** 定案之後同一個 major 只做相容擴充。

### D436 tape 分頁，可以用時間查

- **由新到舊。** 游標是 `(conn_epoch, seq)`，字典序。`before` 不含本身。
- **`ViewRequest` 加 `optional int64 since_ns`。** 只回 `md_ts_ns` ≥ `since_ns` 的事件。讀到比 `since_ns` 舊的就停，`more` 為假。可以和 `before` 一起用。
- **要讀完整段就照 `next` 一頁一頁拿。** `next` 是這一頁最舊一筆的游標，當下一頁的 `before`。`more` 為假時不填。

### D437 `TapePage` 帶回覆當下的 high-water

- **`TapePage.head` 是回覆當下這個 feed 已發佈到 `md.f` 的最新 `(conn_epoch, seq)`。**
- **寫進 ring 與發佈 `md.f` 在同一個臨界區。** 先寫進 ring 並發布索引，再發 `md.f`。第一頁最新一則一定 ≤ 該頁帶回的 `head`。讀取端用它讀到的那份索引上的 `head`，不另採一個還沒進這一頁的較新游標。
- **接 live：** SDK 先訂 `md.f` 並緩衝，同時要 tape。live 的 `(conn_epoch, seq)` 大於 `head` 就套用，小於等於就丟。不必等第一筆 live。PROTOCOL §7 與 §9.4 照這條。
- **ring 裡沒有事件時，`head` 是目前的 `conn_epoch`、seq 0。** 留下的事件後來被砍光也一樣。有事件時，`head` 是最後發佈到 `md.f` 的游標。

### D438 每個 feed 的 ring 上限

- **每個 feed 10 分鐘或 100000 筆，先到先砍。** 時間看事件的 `md_ts_ns`，以寫入那一筆時的時鐘往回算。筆數只算事件，不算 gap。
- **整個 worker 總上限 256 MiB。** 超過時砍全 worker 最舊的事件。位元組只算留下的 `md.f` 訊息（envelope 加 payload）。**這一段的位元組與回收由 D458 取代。**
- **三個值放 `mdctl.InitPayload`，可設定，0 用預設。** 欄位與單位見 D449。

### D439 一頁大小

- **`limit` 為 0 時是 1000，上限 5000。** 超過回 `invalid`，不截短。`limit` 只數事件。
- **一頁編碼到約 900 KiB 就截斷，`more` 為真。** 900 KiB 是 900×1024。壓在 NATS 1 MiB 以下，留餘量。大小含這一頁的 `more` 與 `next`，所以送出去的回覆本身不超過這個上限。

### D440 gap

- **`Gap` 有 `start_ns`、`end_ns`、`Cursor last`、`Cursor first`。** `start_ns` 是斷線時的 md 主機時鐘，`end_ns` 是重連完成時的。`last` 是斷線前最後一則已發佈的游標；那之前沒有事件就不填。
- **ring 跨 `conn_epoch` 保留。** 重連後 seq 從 1 起算（D409）。gap 的排序鍵是新 epoch 的 `(conn_epoch, 0)`，夾在舊事件與新事件之間。
- **`first` 是重連後第一則 tape 事件。** 那則還沒進 ring 就不填。它是 gap 下一格的事件，讀取時從索引得出，不回頭改已經寫入的 gap。
- **`continuous_since_ns` 維持：從這個時間起，留下的 tape 沒有 gap。** 還有 gap 時，是最新一個 gap 的 `end_ns`。沒有 gap 時，是留下的最舊一則事件的 `md_ts_ns`。空的是 0。每一頁帶同一個值。

### D441 `BookSnapshot` 只回現在那本

- **不支援指定歷史 seq。** 回最後套用的 `(conn_epoch, seq)` 的窗內全本（D430）。
- **快照上的游標是 `orderbook_delta` 已發佈的 `(conn_epoch, seq)`。** 增量接縫比的是它（D425、D453）。這個 feed 還沒發佈過時，簿還沒同步好。
- **簿還沒同步好時回 `not_synced`。** 快照中、退避中、窗口無效，或這條連線已經斷，都是。`ViewReply.error` 加這個值，不帶 book。SDK 退避重試。
- **讀取不拿連線鎖。** 寫入端在發布之後換上一份不可變的快照；`md.view` 只讀那份。

### D442 依 topic 回什麼

- **book topic 只有 `orderbook_delta`，回 `BookSnapshot`。** 沒有 top-N。`orderbook` 已刪除（D453）。
- **`trade`、`aggtrade`、`liquidation` 回 tape。**
- **其他 topic（kline、best_quote 等）回 `unsupported`。** 錄不錄不看 `record`（D447）。持有這個 feed 就有 responder；沒有 responder 是沒有持有，不是 `unsupported`。

### D443 linger

- **feed 還在指派裡就照常回 `md.view`。** linger 期間 agent 仍留著它（D905）。conn worker 自己不算 linger。
- **feed 從清單移除時退訂 `md.view` 並清掉 ring。** REJECTED 也一樣（D448）。

### D444 subject

- **已由 D455 取代。** subject 是 `md.v1.view.{inst}.{topic}.{ticker}`。`inst` 保留。#181 沒有拿掉它。

### D445 讀取不擋發布

- **`md.view` 在另一個 goroutine 上處理。**
- **讀取完全不拿鎖（D450）。** conn worker 是單一寫入端，只追加。寫完用 atomic 推進「已寫到第幾筆」。讀取端讀索引、讀資料，再以序號確認該段未被覆蓋；被覆蓋就往後縮或重讀。
- **讀取絕不阻擋 websocket 讀取與 `md.f` 的發布。**

### D446 `FeedEnd`

- **已由 D453、D454 取代。** `FeedEnd` 從 proto 與 `exchange.Sink` 拿掉。沒有這個訊息可錄。

### D447 錄不錄由 worker 依 topic 決定

- **`trade`、`aggtrade`、`liquidation` 錄進 tape，其他不錄（D402、D442）。**
- **`AtomAssignment.record` 忽略。** 不刪這個欄位，也不標成 reserved。E4-12 定案時照現狀保留（D460）。

### D448 feed 模式下 ring 的生命週期

- **在 `project()` 換掉 `wanted` 的同一個臨界區裡，依 `wanted` 以 feed key 算 owners，同步 tape。** 鎖外再 rebind `md.view`。訂閱不在持有連線鎖時做。
- **feed 從清單移除，或變成 REJECTED 時，退訂 `md.view` 並清掉 ring（D443）。**
- **在表上但不是 `TRADABLE`（PENDING）時，保留已有的 ring 和 `md.view`，直到 feed 真的從清單移除。** 保留是留下已經有的。從來沒進過 `wanted` 的 PENDING 不新建 ring、也不訂 `md.view`。進了 `wanted` 才建；之後變成 PENDING 就留著。

### D449 一個 feed 一個 ring

- **ring 的鍵是 feed key（`topic.UniversalTicker`），不是 atom。** 每個 feed 各自 10 分鐘或 100000 筆，先到先砍。worker 總上限 256 MiB 不變（D438）。
- **三個值在 `mdctl.InitPayload`。** `tape_window_ms`（uint32，0 是 600000，單位毫秒）、`tape_max_events`（uint32，0 是 100000）、`tape_budget_bytes`（uint64，0 是 256 MiB）。
- **100000 筆只算事件。** gap 佔 ring 的一格，不計入筆數，也不計入位元組。位元組是留下的 `md.f` 訊息長度。超過總上限時，從最舊的事件砍，連坐落在它前面的 gap 一起拿掉。**位元組怎麼算、以及超過總上限時砍的單位，由 D458 取代。** 筆數與「gap 不計入筆數」仍有效。
- **feed 移除或 REJECTED 就清掉它的 ring（D448）。**

### D450 讀取路徑無鎖

- **每個 ring 只由 conn worker 單一寫入，只追加。** 寫完用 atomic 推進「已寫到第幾筆」。覆蓋舊格子之前，先發布新的尾端，讀取端才不會把正在被蓋掉的格子當成還在範圍裡。
- **`md.view` 讀取完全不拿鎖。** 讀 atomic 索引、讀資料，讀完以序號確認該段未被寫入端覆蓋。序號對不上就重讀。仍被覆蓋就往後縮：停在那一格，不把它算進這一頁，`more` 為假。
- **讀取絕不阻擋 websocket 讀取與 `md.f` 發布。** book 的 `md.view` 同樣只讀寫入端換上的快照，不拿連線鎖（D441）。
- **重用一塊 chunk 時，確認改看 chunk 的 generation（D459）。** 先發布新的尾端，再改寫。讀取端以 atomic 字組複製，generation 對不上就重讀。

### D451 建頁成本只跟頁大小有關

- **`before` 與 `since_ns` 在索引上二分搜尋，定位這一頁的起點。** 再往回讀最多 `limit` 筆事件，邊讀邊累加編碼大小，到約 900 KiB 就停（D439）。**先二分 chunk，再二分 chunk 裡的 slot（D459）。**
- **成本是 O(limit)。** 讀取路徑不掃整個 ring，不重算整頁，也不 trim。
- **砍舊資料只在寫入端做。** 時間窗與筆數從最舊的事件拿。worker 的總上限回收最舊的 chunk（D458）。

### D452 只接正式的 feed 路徑

- **tape 與 `md.view` 只接在 `setFeeds` → `project()`。** 錄不錄依 D447。
- **測試走 `setFeeds`，不得手動設 `Record`。**
- **舊的 `apply()` 路徑不接 tape。** 那條路徑沒有 ring，也不訂 `md.view`。

### D453 沒有 top-N，book topic 只剩 `orderbook_delta`

- **`orderbook` 刪掉，不是 `orderbook_delta` 的別名。** 設定或訂閱寫 `orderbook` 回 `invalid`，不自動換成 delta。只有 `orderbook_delta`。
- **`FeedEvent` 拿掉 `OrderBook` 與 `FeedEnd`。** field 4、100 與 `InitPayload.book_depth` 的 `reserved` 已刪除，號碼不重排（D460）。
- **SnapshotREST 抓 `BookSync.Depth()`。** 不再因為 N 大於 `Depth()` 而退出。沒有 N。
- **修訂 D402 的 book、D425、D426、D430、D432、D441、D442。** D427 整條由這條取代。

### D454 `exchange.Sink` 沒有 `FeedEnd`

- **拿掉方法與型別。** 沒有 adapter 發布它，proto 訊息也沒了。留一個空方法會讓每個 Sink 都實作空函數。
- **修訂 D446。**

### D455 行情 subject 是 `md.v1.f.{inst}.{topic}.{ticker}`（P14，#181）

- **保留 `inst`。** 它是 instance 名稱（PROTOCOL §2），一個字串，不綁 region。subject 不另帶 region。兩個 MD instance 持有同一個 atom，subject 不同；session 仍只訂綁定的那個 instance，`md.v1.f.{inst}.>`。`md.view` 仍由該 instance 裡持有這個 feed 的 conn worker 回答，一個 subject 一個 responder。
- **版號放 subject 前段，現在是 `v1`。** `md.v1.f.{inst}.{topic}.{ticker}`、`md.v1.view.{inst}.{topic}.{ticker}`。新舊版 conn worker 並存時，新的發在新的根，舊的繼續發舊的，平台內不做遷移。這是 C6 不相容時換 subject 根。SDK 仍用 binding 給的 subject，不自己組。selector 的前綴是 `md.v1.f.{inst}`，成員再加 `.{topic}.{ticker}`。`md.view` 是把開頭的 `md.v1.f.` 換成 `md.v1.view.`。
- **行情 subject 不帶 venue。** venue 只出現在 `md.v1.fetch.{venue}` 與 `sym.v1.changed.{venue}.{category}`。
- **ticker 不轉義。** UniversalTicker 原樣當一個 token。它不含 `.`（PROTOCOL §2），也不含 `*`、`>`。
- **`topic` 是 `exchange.Topic` 的字串。** 例如 `trade`、`aggtrade`、`orderbook_delta`、`bestquote`、`kline_<interval>`，其餘見 PROTOCOL §2。不另取 subject 用的名字。
- **權限：** session publish `md.v1.view.{綁定的 MD instance}.>`、subscribe `md.v1.f.{綁定的 MD instance}.>`。MD instance publish `md.v1.f.{inst}.>`、subscribe `md.v1.view.{inst}.>`。
- **實作在 #194。** 這一條只定拼法。改名、JWT 與 dev creds 不在這張。
- **修訂 D106 的版號位置、D402 的 `md.view` subject。** D444 整條由這一條取代。

### D456 `md.fetch` 的 subject 是 `md.v1.fetch.{venue}`

- **版號同樣放前段。** venue 仍是人讀的名字，與 `sym.v1.changed.{venue}.{category}` 的 venue 相同。
- **沿用 D418 的其餘部分。** 每個 replica 只訂連得到的 venue；queue group 的名稱是 subject；程式不選 region。權限用 `md.v1.fetch.>`，行程只訂清單裡的 venue。恰好 subject `md.v1.fetch` 不能訂。SDK 走 `md.v1.fetch.{venue}`，venue 取自 ticker。
- **修訂 D418 的 subject 字串。** venue 分開與不寫跨 region 路由仍有效。沒有版號的舊寫法不再用。
- 實作在 #194。

### D457 變更通知是 `sym.v1.changed.{venue}.{category}`

- **SYM 只有一個，不帶 `inst`。**
- **保留 venue 與 category。** 刷新單位仍是 `(venue, category)`。conn worker 只訂自己的 venue：`sym.v1.changed.{venue}.>`。MD instance 的 JWT 仍是 `sym.v1.changed.>`，一個 instance 可以有多個 venue，憑證是 instance 的（D415）。session、account worker、md.fetch、歷史補正的 subscribe 是 `sym.v1.changed.>`。
- **請求 subject 仍是恰好 `sym`。** 不是 `sym.>`，也不是 `sym.v1`。決定裡「`md.fetch`、`sym` 同理」落在變更通知這條，以及 `md.v1.fetch.{venue}`（D456）。
- **內容仍是 `Changed{venue, category, revision}`**，用人讀的名字。漏收與重載見 PROTOCOL §7.3。
- **修訂 E2-09 的 subject 字串，以及 D202、D203 裡「P14 定案時可能改名」。**
- 實作在 #194。

### D458 worker 級的 tape arena

- **256 MiB 是這個 worker 全部 tape 記憶體的硬上限。** 訊息 bytes、slot、每筆的固定開銷、chunk，以及每個 ring 的 chunk 指標陣列，都算在裡面。0 仍是 256 MiB（D449）。修訂 D438 的位元組算法，以及 D449「位元組是留下的 `md.f` 訊息長度」。
- **記憶體以固定 64 KiB 的 chunk 為單位，不可設定。** 一塊 chunk 就是 64 KiB：前 128 bytes 是標頭，其餘是 slot 與 payload。slot 從前面往後長，payload 從後面往前長，8 bytes 對齊。一筆事件不跨 chunk。當前這塊的尾巴放不下時，尾巴留著不用，改拿下一塊。
- **耗盡時回收全 worker 最舊的 chunk，O(log n)。** 排序鍵是該 chunk 裡最新一筆的 `md_ts_ns`，相同則比 feed key，再比 chunk 序號。只回收某個 ring 最舊的那一塊，不在中間挖洞。每個 feed 的 10 分鐘與 100000 筆仍在，誰先到用誰。時間窗與筆數仍從最舊的事件砍；整塊 chunk 回到 arena，要等裡面每一筆都已經不在範圍裡。
- **每個 ring 至少留 1 個 chunk。** 最新、還在寫的那一塊不進回收堆，別的 feed 拿不走。沒有可設定的保留額。閒置的 ring 不先配 chunk。自己這一塊放不下、arena 也拿不出新的時，在這塊裡改寫，留下放得下的最新後綴。
- **ring 是固定長度的 chunk 指標陣列，本身也是一個環，不是鏈結串列。** 長度是 `min(tape_max_events, max(1, floor(budget / 64KiB)))`。預設 256 MiB / 64 KiB = 4096 個指標。閒置的 ring 是這份指標陣列，加上對齊用的 ring 本體，小於一個 chunk，而不是預先配好的 100000 格。
- **對齊用的 ring 本體在配出來時就計入上限。** 即使因此超過一個很小的 budget，也照計。chunk 用完回到 free list，不還給 GC，所以上限不依賴回收行程。ring 被移除時，指標陣列的帳先扣掉，那塊記憶體等 GC；在途的讀取仍拿得到這個 ring。

### D459 讀取，與放不下的事件

- **建頁先二分搜尋 chunk，再二分搜尋 chunk 裡的 slot。** `before` 與 `since_ns` 都這樣定位，然後往回讀最多 `limit` 筆（D451）。成本仍是 O(limit)。讀取不拿鎖，也不 trim。
- **無鎖讀取比的是 chunk 的 generation。** 寫入端要重用一塊之前，先發布新的尾端，再把 generation 改成奇數；寫完改回偶數。讀取端讀 generation、以 atomic 字組複製 slot 與 payload、再讀一次。不一致就重讀。指標槽重用時，chunk 序號對不上同樣重讀（D450）。
- **大於 32 KiB 的事件不錄，改記一筆 gap。** 比的是整則 `md.f` 訊息（envelope 加 payload）。剛好 32 KiB 照錄。這筆 gap 的排序鍵是該事件的 `(conn_epoch, seq)`，不是重連的 `(epoch, 0)`。`start_ns` 與 `end_ns` 都是該事件的 `md_ts_ns`。`last` 是前一筆還留著的事件游標；沒有就不填。`first` 照 D440，讀取時從下一筆事件得出。它計入 `continuous_since_ns`。`head` 推進到這個游標，live 不會再套用這筆。`md.f` 仍照常發布。
- **一筆紀錄完全寫不進去時，不推進 `head`。** 連 gap 都沒有 chunk 可放就是這種。ring 是空的話，`head` 維持 `(conn_epoch, 0)`（D437），live 仍能把這筆送出去。
- **修訂 D450 的覆蓋確認，以及 D451 的二分搜尋。** 時間窗與筆數仍在寫入端從最舊的事件拿；worker 上限拿的是最舊的 chunk（D458）。

### D460 定案前刪掉 `reserved`；`mdctl`、`md`、`read` 定案（E4-12，#152）

- **系統尚未上線。** `mdctl.proto`、`md.proto` 裡已刪欄位留下的 `reserved` 直接刪除，不保留。現有欄位號碼不重新編號。`read.proto` 沒有 `reserved`。
- **刪掉的是：**
  - `InitPayload` 的 5 `tape_url`（D402）、8 `book_depth`（D453）、9 `book_boundary_bp`（D426）
  - `Report` 的 5 `atoms`（D426）
  - `Meta` 的 1 `seq`、2 `conn_epoch`、4 `md_ts_ns`（移到 envelope，D803、D409）
  - `FeedEvent` 的 4 `order_book`、100 `feed_end`（D453）
- **這三份定案。** 從 `buf.yaml` 的 `breaking.ignore` 移出，`buf breaking` 開始檢查。PROTOCOL §1 的 `mdctl.proto` 列，以及 `md.proto`、`read.proto`、envelope（§7.1）那一列，改為定案。envelope 的位元組已由 D405–D411 定死。
- **`AtomAssignment.record` 這張不改。** 仍忽略（D447）。
- **修訂 D453 裡「reserved」的說法。**

### D906 session worker 的 NATS 憑證：每個 session 一份

- **簽發：** STS agent 持有 NATS account 的簽章金鑰，spawn 時為每個 session 簽一份 user JWT（NKeys）。和其他憑證一樣經 `Init` 的 socketpair 送進去，不放環境變數、不落檔。簽發是本機的密碼學運算，不需要連 server。
- **權限從 spec 推：**

  | | 允許 |
  |---|---|
  | publish | `td.order.{綁定的 api_id}`、`td.view.{綁定的 api_id}`、`md.v1.view.{綁定的 MD instance}.>`、`md.v1.fetch.>`（D456）、`sym`（D202）、`log.sts.{session_id}` |
  | subscribe | `md.v1.f.{綁定的 MD instance}.>`、`td.evt.{綁定的 api_id}`、`_INBOX_{session_id}.>`、`sym.v1.changed.>`（E2-09 的字串已由 D457 取代） |

  - 行情的訂閱權限用萬用字元，只分到 instance：selector 的成員會變，沒辦法逐一列出；行情是公開資料，放寬讀取的風險很低。
  - reply inbox 每個 session 一個獨立的前綴 `_INBOX_{session_id}`。預設的 `_INBOX.>` 會讓任何連線都能讀到別人的回覆，包括 `td.view` 查回來的帳務資料。
- **分層：** 跨帳號由 broker 保證，策略只能對自己綁定的帳號發 `td.order`；同一個帳號內的 algo 由 TD 檢查（D602，防呆）。
- **不設過期、不做撤銷：** 設過期的話，到期時 server 會斷開連線，重連瞬間 core NATS 的 `td.evt` 可能漏。憑證只在進程記憶體裡，session 結束、進程一死就沒了。
- **不影響下單延遲：** 授權只在建立連線時做一次（`CONNECT` 帶 JWT 與對 nonce 的簽章）。之後每次發佈，server 只拿 subject 比對這條連線的權限快取，訊息本身不帶任何 token。
- MD 的 worker 是平台程式，用所屬 instance 的憑證。TD 的 account worker 每個帳號一份，由 TD agent 簽（D611）。

### D202 UniversalTicker 是佔位，解析之後才知道要訂閱什麼

照 mftik 的 symbol plane（D201）。

- **給人寫的是 UniversalTicker，系統內部是 UID（D203）。** spec 與設定寫 ticker 字串。STS 的 session worker、TD 的 account worker 載入時解成 UID，這兩個 worker 傳這個整數，再用 UID 向 SYM 拿欄位。TD agent 不查 SYM。歷史補正服務反查交易所寫法（D601）。MD 的需求與 `md.f` 維持 ticker，conn worker 用 ticker 查活躍表。需求項目、`td.order` 今天仍帶 ticker，換成 UID 是後續。NATS 的 subject 用人讀的名字，不用 UID。確切拼法已由 D455 取代，是 `md.v1.f.{inst}.{topic}.{ticker}`。SYM 表的主鍵是 UID。交易所的寫法（`exch_ticker`）只出現在 adapter 和 SYM。
- **翻譯是查表，經 SYM 解析，不是字串轉換。** `BTCUSDT` 不帶 base／quote 的切法，交易所的寫法也不一定是 base＋分隔＋quote。
- **ticker 的 base／quote 是 SYM 正規化之後的資產（D203）。** 資產代碼一律大寫，再套別名表（例如 `XBT` → `BTC`），只在 SYM。先正規化，再做 quanto 判斷，再推 ticker；strike 先依 `spell_strike` 正規化，`6400.0` 與 `6400` 是同一個 ticker。`USD`、`USDC`、`USDT` 不合併。quanto 不入庫。
- **MD agent 不查 SYM。** 它從 ticker 的 venue，以及 topic 決定的 route，選 `md/<venue>/<route>/<n>`，把 `topic.UniversalTicker` 送進那個 worker。問 route 時只用 ticker 組出的 Info，channel 丟掉，不拿來訂閱。
  - conn worker 在 Init 載入該 venue 的活躍表（D203：不讀終結表），用表上的 `symbol.Info` 呼叫 `Market.Atom`，跟目前的訂閱做 diff。不在活躍表，或不是 `TRADABLE`（quanto 不入庫），這筆就不訂。`EXPIRED` 與 `DELISTED` 不在活躍表。worker 把每一筆 feed 放進 `Report.feeds`：不在活躍表，或這條 route 訂不了，是 `REJECTED`，agent 照寫；在表上但不是 `TRADABLE` 是 `PENDING`。channel 被交易所 ack 之後，掛在上面的 feed 是 `SUBSCRIBED`，worker 在跑時 agent 寫 `READY`。
  - adapter 仍是純函數：不查 SYM、不做 I/O，只用資料就能測。
  - mftik 的 adapter 自己帶 SymbolResolver，在裡面查；mftik refactor 的 B7-01 改成先解析再 `atoms_for`。這裡由 conn worker 先從表拿出 Info，再呼叫 `Atom`。
- **回程帶交易所那一側的 key。** 私有事件、REST 拿回的成交與訂單帶 `(venue, category, exch_ticker)`（venue 見 D301），account worker 與歷史補正服務以 SYM 反查換回 UniversalTicker。TD agent 不反查。category 是 key 的一部分：統一帳戶的 venue 上，同一個寫法可能是兩個商品（Bybit 現貨與 linear 的 `BTCUSDT`）。
- **發布路徑不查 SYM。** conn worker 在 reconcile 時讀記憶體裡的活躍表，表跟著 `sym.v1.changed` 重載。事件上的 UniversalTicker 來自 feed。account worker 每個 ticker 第一次下單時解析一次，之後查記憶體。
- **atom 的身分是解析後的 `(venue, route, channel)`，id 為 `<venue>:<route>:<channel>`。** channel 仍逐字照抄交易所參數；route 從解析後的商品推得（Bybit 依 category），所以同一個 channel 字串在不同 route 上是不同的 atom。
  - 不拿 `topic.UniversalTicker` 當 atom 的身分。一個 channel 常同時服務多個 topic：Bybit linear 的 `tickers.*` 帶 ticker、funding、OI、mark price；Binance UM 的 `markPrice` 帶 mark price 與 funding。以 (topic, ticker) 當身分，同一條連線上會重複訂閱同一個 channel，退訂一個就把其他的斷掉。這是 mftik `MdVenueSubscriptions` 處理過的問題：產品的 refcount 和 wire 上的訂閱是兩本帳。
  - 佔位留在 MD 外面：`md.f` 的 subject 是 feed（`topic.UniversalTicker`），atom 不出現在資料面（PROTOCOL §7）。
- **topic 名稱沿用 mftik：** `bestquote`、`trade`、`aggtrade`、`ticker`、`liquidation`、`funding_rate`、`open_interest`、`greeks`、`kline_<interval>`，加上這裡自己的 `orderbook_delta`、`mark_price`。`orderbook` 已刪除（D453）。表在 PROTOCOL §2。
- **SYM 照 mftik：**
  - 刷新以 `(venue, category)` 為單位，各自刷新、各自下架：現貨的刷新不會讓 perp 下架。
  - 下架與到期不改 UniversalTicker（D203）。狀態還會變的留在活躍表；進了終結狀態就搬到終結表，ticker 字串不變。訂單與 session 仍指向同一個 ticker。
  - 契約見 PROTOCOL §7.3，表見 `migrations/0002_symbol`。
- **誰用 SYM：STS 的 session worker、TD 的 account worker、歷史補正服務、MD 的 conn worker，以及 md.fetch。MD agent、TD agent 與 API 不用。** STS 這端是 session worker（經 SDK），所以它的 NATS 權限有 `sym`（D906）。conn worker 用同一份 MD instance 憑證載入自己的 venue 表。歷史補正服務與 md.fetch 各自有自己的 user，publish 恰好 subject `sym`，subscribe `sym.v1.changed.>`（D415、D601）。
- **啟動時把用得到的 venue 的活躍表載入，之後跟著變更重載（D203）。** 進程啟動時，依 `(venue, category)` 分頁把未終結的列讀進記憶體（每頁最多 500 筆）。Resolver 與熱路徑只讀這張表；終結列不進這份快取。盤中會有新上市、到期與下架，selector 也要挑得到新合約，所以 SYM 在 revision commit 之後發 `sym.v1.changed.{venue}.{category}`（內容 `Changed{venue, category, revision}`，用人讀的名字），收到的進程重載那個 `(venue, category)` 的活躍列。已定，見 PROTOCOL §7.3。subject 字串已由 D457 取代。這一條取代原本的「單一商品精確查詢、用戶端快取 10 分鐘」。
- venue 名稱照 mftik 用 CamelCase：OKX 的 token 是 `Okx`。

### D301 帳號種類和 venue 是兩個 enum

- **venue 是市場，帳號種類是 api_id 是什麼。** 兩個獨立的 enum，名稱可能相同，不可互查。
  - venue：UniversalTicker 的第一段，一條連線的公開行情、一組商品（D403）。`Binance`、`BinanceUM`、`BinanceCM`、`BinanceEO`、`Okx`、`Bybit`。
  - 帳號種類：收哪種憑證、可以交易哪些 venue 的商品，由各交易所的套件在程式裡宣告。`Binance`、`BinanceUM`、`BinanceCM`、`BinanceEO`、`BinancePM`、`Okx`、`Bybit`。
  - 大多數帳號種類只交易同名的那個 venue。
- **BinancePM 是帳號種類，不是 venue。** 它可以交易 `Binance_Spot_BTCUSDT`、`BinanceUM_Perp_BTCUSDT`、`BinanceCM_*`；請求依帳號種類走 papi，再依 ticker 的 venue 選 margin、um、cm 的端點。沒有 `BinancePM_Spot_BTCUSDT` 這種 ticker：保證金模式改的是帳號怎麼算錢、單送到哪，不是商品本身，同一本訂單簿只有一個身分。
  - 帳號宣告（`state.v1.Account`）記的是帳號種類（`kind`），不是 venue。
  - account worker 在 `Submit` 前檢查 ticker 的 venue 在帳號種類的清單裡，不在就以 1xx 拒絕。
  - 交易所回來的事件帶 `(venue, category, exch_ticker)`：一條 PM 的私有連線同時有 margin、UM、CM 的事件，所以 D202 的反查 key 加上 venue。
  - D601 的 backfill 範圍 `(api_id, uid)`、D602 的 algo_id 都不假設帳號只有一個 venue，不用改。
- **BinanceCM、BinanceEO 維持獨立的 venue。** 2026-06 的 CM-UM 整合之後，CM 仍然是 `dapi`、`dstream`，錢包也沒合併；EO 的行情雖然在 `fstream` 上，REST 與帳戶都是自己的。D403 的規則不變。
  - CM 的 `dstream` 沒有路徑分流，只有 `default` 一個 route。
- **UM 和 CM 不共用連線。** 整合之後部分 stream 可以跨 host 訂，但連線的身分仍是 `(venue, route, n)`。全市場陣列在兩個 host 上都會送 UM＋CM 合併的內容，以 `st` 區分，adapter 解碼時丟掉另一個 venue 的列。
- **UM、CM 之間的耦合不寫進 adapter 契約。** 整合之後兩者共用 IP 的 weight 與下單數上限、共用持倉模式（`dualSidePosition`），CM 的 STP 跟著 UM。只記在 Binance adapter 的實作說明。

### D302 adapter 是 sans-I/O 的 codec，技術棧與資料邊界

E3 開工前先定的介面、設計模式與技術棧。各交易所的實作照這一條寫，不各自發明。

- **adapter 不做 I/O。** 它只把意圖變成位元組（訂閱的 frame、簽好名的 `HTTPRequest`），把位元組變成事件（`Session.Decode`、`PrivateStream.Decode`、`Call.Parse`）。不連線、不讀時鐘、不 sleep、不起 goroutine、不重試。擁有連線的 worker 送出 adapter 做好的東西、交回收到的東西。
  - WS 本來就是這個形狀。REST 也改成這樣：每個方法回傳 `Call[T]`，也就是一個 `HTTPRequest` 加上解析它的回應的 `Parse`；分頁的讀取是 `Page[E]`，`Next` 以送出當下的時間產生下一頁的請求，讓簽名的時間戳是新的。`Run`、`Collect` 經 `Doer` 執行。要先打 REST 才能連的 WS（Binance 的 listenKey）拆成 `PrivateStream.Prepare` 回傳的 `Call`，加上 `Dial`。
  - 需要時間的地方（簽名）由呼叫者傳入 `now`。public 的請求不簽名，不帶 `now`。
  - 不採用注入 `Doer` 讓 adapter 自己送：`exchange` 是公開套件（D006），`Doer` 的形狀會把傳輸函式庫的型別帶進介面；而且 adapter 拿得到連線，就擋不住它自己重試。
  - 好處：測試只需要錄下來的 fixture，`Decode` 與 `Parse` 都能 fuzz；重試、重連、限速只在 worker；傳輸可以換，adapter 不動。代價：有狀態的流程要寫成顯式的步驟。
- **技術棧：** WS 用 `github.com/coder/websocket`，REST 用 `github.com/valyala/fasthttp`，JSON 用 `github.com/bytedance/sonic`。coder/websocket 與 fasthttp 只出現在 worker 與 `exchange/transport/*`；sonic 只出現在 `exchange/internal/codec`（adapter 共用的兩份凍結設定）、worker 與 transport。`exchange` 的介面裡沒有它們的型別。
  - sonic 的兩份凍結設定：WS 借用（不複製字串），REST 複製（`CopyString`），欄位名稱都區分大小寫。它碰 runtime 內部，Go 每次升版都要先確認 sonic 跟上了。
  - fasthttp 沒有 context：以 ctx 的 deadline 呼叫 `DoDeadline`，送出後無法中途取消；`MaxIdemponentCallAttempts` 設 1，重試由 worker 決定。HTTP/1.1。
  - coder/websocket 的讀取上限預設 32 KiB，每個 venue 依自己的訊息大小設 `SetReadLimit`；pong 只在有 `Read` 進行時處理，讀的 goroutine 不停。握手走 net/http，proxy、TLS、DNS 與 fasthttp 由同一份設定產生。
- **緩衝區是 worker 的，借給 adapter。** WS 用 `Conn.Reader` 讀進每條連線自己的緩衝區，穩態零分配；交給 `Decode` 的 frame、交給 `Parse` 的 response 只在呼叫期間有效。sonic 不複製字串，解出來的字串可能指向緩衝區：Sink 收到的指標與其中的字串只在回呼期間有效，要留的自己複製。REST 的 `Parse` 一律以複製字串的設定解碼（fasthttp 會重用 response），不值得為它借用。conformance 驗證：回呼返回後把緩衝區塗掉，下游拿到的不變。數字是 float64（D008），不受這條影響。
- **資料邊界：**
  - 商品：進去的是解析好的 `*symbol.Info`，出來的是 `symbol.ExchKey`（D202、D203），adapter 不碰 SYM。
  - 時間：`int64` 奈秒。adapter 只填交易所給的時間（`ExchTsNs`）；收到的時間由 worker 蓋。
  - enum：`exchange` 保留自己的 `Side`、`OrderType`、`TimeInForce`、`OrderStatus`（adapter 不依賴產生碼），數值與 `common.v1`、`td.v1` 逐一相同，worker 以型別轉換映射，`enums_test.go` 鎖住。
  - 錯誤：`Parse` 判讀交易所的回答：`*Reject`（明確拒絕）、`*RateLimited`、`ErrAuth`（簽名、權限、IP 白名單；不重試，告警）。沒有回答的由傳輸判定：請求可能已送出的是 `ErrUnknownOutcome`；`HTTPRequest.Mutates` 的請求（下單、撤單）遇到它，worker 查單或 recon，不當成被拒。`ErrUnsupported`、`ErrNotImplemented` 不變。
  - 限速：每個 `HTTPRequest` 宣告它的 `Cost`；交易所在回應裡報的用量由 `Usage(resp, now)` 讀出，`now` 是收到回應的時間。`Used.ResetAt` 是該計數器歸零的絕對時間（D420）。節流只在 worker。
  - `Account.Leverage` 回 float64（D008 的遺漏）。`Account.Snapshot` 是多個可分頁的 `Call`，worker 全部跑完再合併；`AsOfNs` 由 worker 在第一個請求送出前蓋。
- SYM 讀的是 `exchange.Lister`（整個 category 讀到最後一頁）：真的 venue 是 `ListingOver(PublicREST, Doer, now)`，測試是 `exchangetest.Source`。`exchangetest.Source.REST()` 是它的 sans-I/O 面，`Source` 本身就是回應它的 `Doer`。`mftiko-sym` 以 `exchange/transport/fasthttp` 的 Doer 執行它；proxy 與 TLS roots 是這個行程的旗標。沒有 Doer 時 refresh 回 `ErrNoTransport`。

### D303 WS 下單：TradeStream

Binance 一律以 ed25519 經 WS API 收發單（2026-10-10 定）。D302 的 `Account.Submit`、`Cancel` 回傳 HTTP 的 `Call`，裝不下「在一條 WS 上送請求、以 id 對回答」，所以加一個 codec。

- **`TradeStream` 是 sans-I/O 的 codec（D302）。** `Dial(now)` 產生 URL 與連上之後要送的 frame（logon；連線也帶私有事件時，再加上訂閱）。`Submit(now, id, req)`、`Cancel(now, id, ref)` 把一個請求編成 frame。`Decode(frame, sink)` 把回答以 id 交給 `TradeSink.Submitted`、`Canceled`，交易所回報的用量交給 `Used`。logon 被拒是 `ErrAuth`。
- **id 屬於 worker。** 從 1 起算，同一條連線上不重複；codec 自己的 frame（logon、訂閱）用不會撞號的 id。
- **`Decode` 不靠記憶。** codec 把讀回答需要的東西寫進交易所的請求 id（Binance 收字串：`o17` 是下單、`c18` 是撤單；Bybit 的 `reqId` 是 worker id 的十進位，回答同時帶回 `op`）。這樣斷線重連之後讀得懂遲到的回答，錄下的 session 也能重播（E3-04）。
- **同一條連線也帶私有事件時（`TradeLimits.Events`），worker 不另開 `PrivateStream`。** `TradeSink` 本身就是 `AccountSink`：`Ready` 表示連線可用（已 logon；帶事件時也已訂閱），`Order`、`Fill`、`Balances`、`Positions` 從這裡進來。Binance 現貨是這種；UM 的 WS API 不帶私有事件，另開 `PrivateStream`。Bybit 的 trade socket 也不帶，訂單狀態仍走 `/v5/private`。
- **逾時、重連在 worker。** `exchange.Inflight` 記下送出的請求與期限，純邏輯、時間由 worker 傳入。過期或斷線時還沒回答的請求是 `ErrUnknownOutcome`：訂單可能在、也可能不在，由私有串流或下一次 recon 判定，不當成被拒。過期之後才到的回答不會被當成即時回答，worker 把它當成一則訂單消息處理。
- **`Account.NewTradeStream()` 回傳 codec**；交易所或這個 adapter 不走 WS 下單時回 `ErrUnsupported`，worker 改送 REST 的 `Submit`、`Cancel`。一期 Binance 與 Bybit 的 `Submit`、`Cancel` 走 WS；Okx 用 REST。Bybit 的單筆下單與撤單在 `wss://stream.bybit.com/v5/trade`，連線 auth 與私有串流相同，訂單 frame 不再簽名。`CancelBatch`、`Snapshot`、`Leverage`、`History` 都留在 REST，Binance 以 ed25519 簽名。
- `exchangetest.TradeVenue` 是給 worker 測試用的假交易所：回答順序由測試決定，可以丟掉、拒絕、拒絕 logon。conformance 加 `trade` session、`trade_dial`（logon 與請求 frame 的 golden）、`trade_answers`（每個 id 至多回答一次）。

### D907 控制平面：API server 與 controller

控制平面持有所有進程的即時狀態，並依 MD、TD 的狀態管理 STS。它分成兩個進程：

| 進程 | 寫什麼 | 實例數 |
|---|---|---|
| API server | 使用者的宣告：session spec、帳號。讀狀態給 console | 無狀態，可以多個 |
| controller | 需求項目（D901）、STS 的指派 | 選主，同時只有一個有效 |

- **controller 的職責：**
  1. 從 session spec 推導 MD、TD 的需求項目，寫進 provider 所在 region 的 cluster（D005）。
  2. 為每個非 terminal 的 session 寫 STS 的指派 `/sts/{inst}/demand/{session_id}`：要不要跑、能不能 `Start`、目前的綁定（PROTOCOL §3.3）。session 一開始就發，讓 spawn 與 `OnInit` 和需求的建立並行（D107）。
  3. watch 所有平面的 ack、`SessionStatus`、`AgentRecord`、`WorkerStatus`，作為全域的即時狀態。
  4. 依這些狀態管理 session：
     - 所有 MD、TD 需求都 `READY`，才在指派上放行 `Start`。這就是原本 STS agent 的 Gate。
     - 收到 nack（`REJECTED`、`DEAD`）就結束 session，以 `FAILED` 記下 ack 的原因。
     - 沉默時維持現狀：還沒有 ack，或 provider 的 agent 不在、ack 是最後已知的狀態（D901）。暫時性的降級照 D603 留給策略處理，不停 session。
     - `Start` 之前等超過 `deps_timeout_s`，以 `FAILED` 結束。
     - session 結束時依 D901 的順序撤指派、刪需求。
  5. 每次當選、每次全量 resync 都比對並回收孤兒（D901）。
- **controller 只到資源這一層，不碰進程。** 它寫「session S 需要 feed F」「session S 跑在 STS instance X」，不寫「起哪個 worker」。進程的生命週期只有 agent 碰（D001、D101）。它也不 import exchange、不查 SYM：商品是否存在由 MD、TD 解析後以 `REJECTED` 回報。
- **controller 要 fencing，和 agent 一樣（PROTOCOL §3.2 W2）。** 在 home region 的 cluster 參選，當選後 epoch 加 1，在每一個要寫入的 cluster 放 writer key，之後每次寫入都以它為條件。被取代的舊 controller 寫不進去。home region 是部署設定。
  - instance 名稱固定是 `controller`。名稱可以設定的話，兩個名稱不同的 controller 各有各的 writer key，會同時寫同一批需求；名稱固定，兩個進程即使對 home region 的設定不一致，也會在每個 cluster 搶同一把 writer key。
- **controller 不在交易的路徑上。** 它停機或輪換時，需求與指派原樣保留，跑著的 session 照跑，只有新的起停要等它回來。
- API server 不參選。它的寫入怎麼滿足 W1（例如 spec 只用 create-only 的 txn、帳號以 `mod_revision` 做 CAS），在 E10 定（PROTOCOL §10）。

### D908 一期不做跨主機的故障轉移

- session 固定在它的 STS instance 上，帳號固定在它的 TD instance 上，都寫在 key 的路徑裡（PROTOCOL §3.1）。controller 不搬動它們。
- **所以不需要帳號級的 fencing lease。** 同一個帳號只有一個 account worker，由這幾條保證，而且全在同一台主機上：
  - incarnation 先取號再 spawn（D106、PROTOCOL §3.2 W6）；
  - 換 incarnation 一律 delete-before-create（D106）；
  - shim 消失，worker 必死（D104）。
- **之後要做故障轉移，前提是 account worker 先有帳號級的 fencing lease。** 交易所檢查不了 epoch 這類 token，只能靠時間：
  1. 先以 txn 取得帳號的 owner key（綁 etcd lease），才連私有 websocket、才訂閱 `td.order.{api_id}`。NATS 的一般訂閱會讓兩個訂閱者都收到同一筆下單，「誰在訂閱」本身就是 fence 的一部分。
  2. 以本機單調時鐘算「上次續約成功 ＋ TTL − margin」，過了就退訂、停止送單、結束。
  3. 新的持有者要等舊的 etcd lease 過期才取得得到。
  - session worker 不需要自己的 lease：它的單都經過 account worker，而 account worker 檢查得了 token，只接受目前需求裡那個 session incarnation 的單。
- Strategon 的 `sdk/lease` 就是這種 lease：它支援把策略搬到另一台機器，所以需要。

### D006 專案分三塊，加上獨立的 exchange

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
- **共用的交易 enum 定義在契約裡。** `Side`、`OrderType`、`TimeInForce` 放進 `common.v1`。SDK 與其他語言用這一份；SDK 不能 import exchange。exchange 另有同名型別，再加上 `OrderStatus`，數值與 `common.v1`、`td.v1` 相同，worker 以型別轉換映射，`enums_test.go` 鎖住（D302，E3-17 定案）。exchange 根目錄還放憑證、錯誤分類、限速。
- **依賴規則由 `internal/archtest` 的測試檢查**，`make check` 與 CI 都會跑：
  - 平面之間互不 import；平面只用 exchange 的介面；API 不 import exchange、不用 `sym.v1`。
  - `internal/platform` 不 import 任何平面或 exchange。
  - exchange 只依賴 contract；contract 只依賴 protobuf 與標準庫。
  - sdk 只依賴 contract，整個依賴閉包不得出現 etcd、Postgres driver、exchange 或 internal：策略不碰 etcd，策略的 binary 裡也不該有簽交易所請求的程式。
  - examples 只用 sdk 與 contract，模擬外部的策略作者。
- 搬家在 E1-07 之前做，那時大部分套件還是空殼。

### D007 契約分成草案與定案

- 每份契約都有狀態與定案的 EPIC，表在 PROTOCOL §1。`sym.proto` 在 E2-10 定案。`mdctl.proto`、`md.proto`、`read.proto` 在 E4-12 定案（D460）。exchange 的 adaptor 介面在 E3-17 定案。其餘仍是草案。
- `buf.yaml` 的 `breaking.ignore` 列出草案的 proto：草案可以自由改，CI 不擋。定案的 EPIC 把自己的檔案移出清單，寫進完成標準。`sym.proto` 已移出。`mdctl.proto`、`md.proto`、`read.proto` 已移出（D460）。adaptor 介面是 Go，沒有 proto，不進這份清單；定案之後靠 review 維持相容。C3、C4 在 E1 結束時移出。
- 代價：定案之前，不小心改壞草案不會被 CI 抓到，只能靠 review。還沒有任何東西部署，沒有相容性要保護，可以接受。
- **趁還是草案，拿掉兩條跨契約的 import：**
  - `stsctl.v1` 不再 import `state.v1`。策略的契約版本偏差最久（PROTOCOL §1），etcd 的 schema 卻是不相容就換 key 根，兩者不該綁在一起。session 的宣告（`SessionSpec` 與它的組成）抽成獨立的套件 `mftiko.session.v1`，歸 STS；`state.v1`、`stsctl.v1` 與 E10 的 API 契約都 import 它。`EntryState`、`Universe` 在 `stsctl` 裡另定綁定自己的型別。
  - `td.v1` 不再為了 `Side` import `md.v1`：`Side` 搬到 `common.v1`（D006）。

### D008 十進位數一律是 float64

- **價格、數量、金額、手續費、費率、greeks、IV、tick 與 step、`contract_size`、strike、槓桿，在 Go 裡是 `float64`，在契約裡是 `double`。** 範圍包括 `md.v1`、`td.v1`、`sym.v1`、SDK 與 `exchange` 的型別。這條推翻 PROTOCOL §2 原本的「十進位數一律是字串」。
- 理由：熱路徑（WS 解碼、策略）不必再 parse 字串；值不帶指標，E3 零分配解碼時不受緩衝區重用的影響；策略本來就用浮點運算，字串只是把轉換推給每個使用者。代價是不精確，用下面的規則收住。
- **不變的：** `client_order_id` 仍是十進位字串（它是整數，不是數量）；UniversalTicker 裡 strike 的拼法是身分，由 float 的最短十進位表示推出（`symbol.StrikeSpelling`，D203）；Postgres 的欄位仍是 `NUMERIC`，SYM 在寫入前把 float 轉成最短的十進位文字，`"1.10"` 與 `1.1` 因此是同一個值。
- **規則**（`contract/wire/decimal`）：
  1. 入站用 `decimal.Parse`，正確捨入；NaN、Inf、十六進位、底線一律拒收，正負號由各欄位自己檢查。有效位數不超過 15 位的文字，Parse 之後再以同樣的小數位 Format，得回原文字。超過的（例如千萬級數量加 8 位小數）會被捨入，E3 的 conformance 對每個 venue 的 tick 與 step 範圍驗證。
  2. 出站用 `decimal.Append`：先對齊 tick 或 step，再以 step 的小數位、不帶指數寫出；交易所拒收 `1e-05`。有方向的對齊（買價往下、賣價往上）用 `SnapDown`、`SnapUp`，帶 1e-9 個 step 的容差：`0.3/0.1` 是 `2.9999999999999996`。
  3. 不用 `==` 比較。換成整數個 step 再比（`Steps`、`Equal`）：累計成交量是否等於下單量、持倉是否歸零都照這條。
  4. 張數換算（D203）：交易所的張數是 `Snap(qty / contract_size, 張數的 step)`，下單前驗證殘差。
  5. 累加值（餘額、PnL）以交易所回報為準，本地累加只是估計，對帳帶容差。
  6. 缺值在程序裡是 `decimal.Absent()`（NaN），在契約上是不設的 optional 欄位；NaN 不上線。`Filters` 的「有這個限制但沒有界限」也是 NaN，JSON 裡是 `null`。
- 簽名的 request body 只 marshal 一次：簽那份 bytes，送那份 bytes（E3）。

### D108 agent 以快照恢復，shim 的事件只是通知

- **agent 做任何決策所依據的事實，都必須在 `Attached`（或結束檔）裡有最新值。** agent 不在時（輪換、crash、斷線），shim 發出的事件可能沒人收；agent 回來 attach，拿快照和 etcd、需求比對，有差異就改。這是 level-triggered，和 W4、`Mirror` 同一套思路。
- **事件是降低延遲的通知，不是正確性的來源。** agent 不在時遺失的事件，只少了歷史（互相抵銷的轉換、轉換的時間點），不影響決策。
- **shim 不需要知道 agent 做完了沒有。** `Ack` 只讓 shim 提早釋放記憶體，不是提交；環一律最多保留 1024 則，`gap` 只說明歷史不完整。
- 不能等 agent 回來的事都在 shim：`Stop` 的計時、自己收到 SIGTERM 時送 `Stop`、結束的記錄、對 worker 的 `Relay` 回 `Undeliverable`。其餘的判斷（例如要不要重啟）本來就要等 agent，事件保留得再完整也不會更快。
- 不採用的做法：`Ack` 當成提交（agent 把效果寫進 etcd 之後才 Ack，未 Ack 的事件等於 etcd 還沒反映的部分）。它讓 shim 的保留語意綁上 agent 何時持久化，agent 還得處理 at-least-once 的冪等；而只要快照完整，這些都不需要。
- 因此 `Attached` 帶 `exiting`（worker 正在跑 OnStop 時，原本打算怎麼結束）與 `exit`（記錄完的 `ExitInfo`）。

### D203 幣與數量的單位

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
  - E2-03 拆了表。`migrations/0002_symbol` 是活躍表 `symbol_active` 與終結表 `symbol_terminal`，欄位相同，主鍵都是 `uid`。`symbol_filter_active` 參照活躍表、`symbol_filter_terminal` 參照終結表，終結時跟列一起搬。活躍表用 generated column 從 `universal_ticker` 拆出 `venue`、`category`，並有 `UNIQUE (venue, category, exch_ticker)` 與 `universal_ticker` 唯一；終結表沒有這兩個唯一，同一個 ticker 可以留多列。`symbol_revision (venue, category, revision, refreshed_at_ns)` 給變更通知比對 revision（PROTOCOL §7.3，subject 是 `sym.v1.changed.{venue}.{category}`，D457）。另有 `symbol_uid`（主鍵 `uid`，列不刪），兩張 listing 與兩張 filter 參照它，所以歸檔之後也不會再抽到同一個 uid。結算幣欄位名是 `settle`，與契約一致，另外有 `contract_value_ccy`、`price_ccy`。strike 入庫時去掉尾端的零。
- **UID 是系統內部的商品身分。** UniversalTicker 是給人看的字串，只用在寫設定與 spec。系統裡面傳商品用 UID（整數）。各平面要欄位時用 UID 向 SYM 查。STS、TD、MD 之間用 UID，不用 ticker 字串。
  - UID 由 SYM 分配，是隨機整數：唯一、永不重用，不是單調序號，也不是 Postgres sequence。活躍表與終結表合在一起唯一，同一個 UID 不會同時在兩張表。UID 跟列一起持久化，所以重啟與重載都不改它。搬到終結表時 UID 跟著列走。碰撞時 SYM 重試，再抽一個。
  - 同一個 ticker 下架之後再上市，是新的一列，拿新的 UID。舊的成交與持倉仍指向舊 UID。
  - ticker 解成 UID 只發生在設定與 spec 載入的邊界。`SessionSpec` 的 feed 仍寫 UniversalTicker，載入時用 `GetRequest` 換成 UID。
  - `symbol_filter` 也拆成活躍表與終結表。列進了終結狀態時，filter 跟 listing 列一起搬。
  - SYM 的 `List` 以 offset＋limit 分頁。翻頁期間列可能搬表或新上市，跨頁不一致可以接受。
  - `Universe.current` 是 selector，尚未定，不在這一條。selector 沒有 UID。selector 的成員今天仍是 ticker 字串；成員是商品，改成商品 UID 是後續。
  - 這一版只在 `SymbolInfo` 加上 `uid`，並加 `GetByUidRequest`。`td.order`、需求項目、selector 成員今天仍是 ticker 字串。SQL 的兩張表以 `uid` 為主鍵。
  - NATS 的 subject 用人讀的名字，不用 UID。確切拼法已由 D455 取代：`md.v1.f.{inst}.{topic}.{ticker}`，不改成 UID。策略 hook 看 ticker 還是 UID 見 P15。
  - 契約型別是 `uint32`，範圍 `1..4294967295`（`1..2^32-1`），0 非法（PROTOCOL §10 P13）。Postgres 欄位是 `bigint`，`CHECK (uid BETWEEN 1 AND 4294967295)`。E2-03 的 store 照這個範圍抽號，先插入 `symbol_uid`，撞到 unique violation 就換一個再插。`symbol_reject_shared_uid` 仍擋兩張 listing 同時持有同一個 uid；搬表在同一個交易裡先刪活躍列再插入終結列。
  - **進程內 Resolver（E2-05）只給測試與開發。** 它對設定的每個 `(venue, category)` 直接呼叫 `PublicREST.Listing`，來源必須實作 `exchange.InProcessUIDSource`（`exchangetest`）。載入前先呼叫與 SYM refresh 相同的 `normalizeAssets`（大寫並套別名），再 `Listing.Validate`。只快取活躍表狀態：`PRELISTING`、`TRADABLE`、`HALTED`、`SETTLING`、`UNKNOWN`。`EXPIRED` 與 `DELISTED` 不進這個進程的快取（沒有終結表），`UNSPECIFIED` 非法。只有 `TRADABLE` 可交易。`Validate` 過了但 `Ticker()` 失敗的列略過：記 log、計數，不讓整次 venue 重載失敗。不經過 SYM 服務，也不寫 Postgres。UID 由這個進程自己發；測試注入固定順序，沒注入就用與 SYM 相同的隨機抽號，但抽到的號碼不寫進 `symbol_uid`。這種 UID 不是正式 UID：不落庫，不跨進程、不跨平面傳。真實 venue 的進程等 SYM 分配的 UID。重載在這個進程裡整份換上；還在清單上的 ticker 沿用同一個進程內 UID，從清單消失的不重用，同一個 ticker 再上市拿新的。

- **不採用：** adapter 自行改名，或在 adapter 裡收別名；下游用類別回推三個幣欄；沿用 `reservations.is_linear_margin`，或為 Deribit 開特例；把 `settlement_asset` 當成保證金幣（Binance 的 `marginAsset`、Bitget 寫成 quote）；inverse 的 qty 用張數；quanto 入庫並用非 `TRADABLE` 狀態表示；把 `USD`、`USDC`、`USDT` 收成同一個；已終結的列留在熱路徑讀的那張表；搬表時改寫 UniversalTicker 或重發 UID；下架再上市沿用舊 UID；用單調序號或 Postgres sequence（`symbol_uid_seq`）當 UID；重啟或重載時重發 UID；給 selector 一個 UID；系統內部繼續用 ticker 字串當商品身分；NATS subject 用 UID；進程內 Resolver 接真實 venue，或把進程內發的 UID 落庫、傳到別的進程或別的平面。

### D109 需求過舊時保持現狀

agent 依需求做的改變，前提是需求是新的。Mirror 的 `LastContact` 超過門檻（預設 15 秒，大約是 probe 間隔的 3 倍；各平面的部署設定）就算過舊。這時 view 停在最後一次聯繫的 revision：它曾經為真，只是之後的變化看不到。

- **過舊時不做依據需求的減法，也不改變活著的 worker。** 不停 worker、不因為換 spec 而替換、不推 live config（推的可能是退訂）。恢復聯繫之後的下一輪 reconcile 才做。
  - 看不到不等於不要：不在舊 view 裡的 worker，可能正是失聯之後才加的需求。
  - 錯誤的代價不對稱：多跑一個 worker，聯繫恢復後就停掉，會自己修正；停掉一個還需要的 worker，影響傳到系統外（MD 的缺口補不回來，TD 要撤單、對帳，STS 丟掉策略的狀態）。
  - 故障是相關的：etcd 分區時所有 agent 一起過舊，過舊就減法等於一次 etcd 故障就讓交易全停。控制面的故障不擴散到資料面（fail static）。
- **維持最後一份需求由 Policy 決定。** 只有兩件事：重啟結束了的 worker、對已初始化的 worker 送 `Start`。`Policy.KeepWhileStale` 說可以才做。MD 可以（唯讀，多一份沒有副作用）；TD 由 Policy 判斷（失聯期間帳號可能被撤銷）；STS 的 `Start` 要等 controller 設 `start`（§3.3 N2），自然最多走到 `Init`。
- **過舊時不寫 `WorkerStatus`。** 寫入會卡在連不上的 cluster；狀態只在值改變時寫，恢復後一次補上（W7.2）。
- 骨架只提供「是否過舊」與上面兩類動作的區分（`internal/platform/agent` 的 `Reconciler`），平面的判斷放在 Policy。
- 不採用：過舊時照常 reconcile；過舊時只禁止減法、加法照常（新的 spawn 也是依據需求的改變，而且 TD 的加法不一定安全）；過舊時停掉一切（就是上面說的故障擴散）。

### D110 平面的決策與 I/O 分開

- 平面的根套件（`internal/sts`、`md`、`td`）放 Planner、Policy 與 domain 型別，是純的邏輯，不需要 cluster 就能測。接線（Intake 的 Mirror、Fleet 的 shim、NATS）放在子套件 `internal/{plane}/daemon`。cmd 只組裝 daemon。
- 骨架同樣分開：`internal/platform/agent` 是型別與 `Reconciler`（diff 與重啟的記帳，不做 I/O）；`agent/runner` 是一個 term 的 loop（連自己 region、等需求、收養、比對 `WorkerStatus`、寫 `AgentRecord`、reconcile）；`agent/fleet` 是以 shim 為底的 Fleet，平面只提供 `Program`（跑什麼、送什麼）；`agent/c3` 是 shim 的 client；`agent/testplane` 是測試用平面（E1-19），以假 worker 跑完整個骨架。
- `internal/archtest` 檢查：平面的根套件與 `internal/platform/agent` 的依賴閉包裡沒有 etcd、NATS、`state`、`natsx`、`shim`、`agent/c3`、`agent/fleet`、`agent/runner`、`agent/testplane`。
- 子套件叫 `daemon`：它是常駐的 agent 進程。不用 `runtime`（遮住標準庫）、`agent`（與 `internal/platform/agent` 同名，平面的接線一定 import 它）、`wire`（與 `contract/wire` 同名）、`adapter`（與 exchange 的 adaptor 混淆）。

### D111 live config 以 etcd 為唯一來源

- **live config 是完整的期望狀態，不是增量**：例如 MD 的 atom 集合、STS 的依賴綁定、TD 的 drain。worker 收到之後和手上的版本 diff 再套用，所以收到同一份就什麼都不做。
- **唯一的來源是 etcd，加上 SYM。** agent 每次都從 etcd 推導，不信任自己記得推過什麼，也不在 shim 另外留一份副本。
  - 推導當中依賴當下狀態的部分，例如 MD selector 的成員（價格決定置中、時間決定換月）、placement，**先寫進 etcd，再推給 worker**。否則 agent 推完、還沒寫就掛了，新 agent 依當下的狀態重算，會得到另一份。
  - 因此 Planner 的輸入是需求加上平面自己持久化的推導狀態，例如 MD 的 `selector/`、`placement/`，不只是 `demand/`。selector 的評估是一個先寫 etcd、再影響計畫的動作，不放在純的 Planner 裡（D110）。這兩點在 E4、E5 實作。
- **worker 手上是哪一份，以 worker 的回報為準。** agent 推的時候附上 digest，worker 套用後回報 `Applied{digest}`，shim 把最新一則記在 `Attached`（D108）。agent 拿它和 etcd 推導出的 digest 比對，不同才推。digest 防的是漂移（推到一半、套用失敗、輪換期間需求變了），不是竄改，用 `Hash()` 就夠。
- **推送走 C3／C4 的 `Live`，不走 C5 的 `Relay`**（E1-22）：digest 是骨架的事，不該寫進各平面的 C5 契約。`Init` 帶 `live_digest`，worker 套用第一份之後也回 `Applied`。送出去還沒收到 `Applied` 的，隔一段時間（預設 5 秒）再送；這也補上了套用失敗時 agent 不會再推的漏洞。E1-17 的暫行做法（收養時一律重推）就此拿掉。
- **不採用：**
  - 收養時不推：輪換期間的需求變化會被永久漏掉。
  - shim 記下 agent 交出的 `live_hash`：多一份副本，而且只代表交了，不代表套用了。
  - live config 寫成增量：重推會重複套用，漂移也無法用 digest 比出來。

## 待討論

目前沒有。協議層還沒定的見 PROTOCOL §10。

已解決：舊的 Q2（綁定變化由 controller 從 ack 整理進 STS 的指派，STS agent 轉送給 worker，見 D907）、Q4（durable store 是 etcd，訂單歷史另放 Postgres）、Q12（每個 region 一個 etcd，見 D005）、Q1（每個策略一個 artifact，見 D902、D802）、Q13（生命週期兩段，見 D107）、Q11（TD 也依 refcount 起停，見 D901）、Q16（歷史補正的排程與落庫在 TD agent，交易所讀取比照 md.fetch，見 D601）、Q9（tape 與 book 的權威在 conn worker，以 `md.view` 讀取；md.fetch 是無狀態服務，見 D402）、Q3（一期沒有執行中加減訂閱的介面，見 D903）、Q6（spec 不可變維持；策略狀態經 volume 與資料 artifact 延續，見 D904）、Q5（單一 dispatcher、TD／MD 兩個 buffer、prehook 依 `recv_window_ms` 丟舊行情，見 D803）、Q8（平台不清場；策略的 SDK 照 CancelBatch 拆批自己清，否則人工撤，見 D602）、Q7（連線以 `(venue, route, n)` 共用，滿了才開下一條，不整併，見 D403）、Q10（線上編碼一律 protobuf，`buf breaking` 機械檢查相容性；`md.f` 在 protobuf 前面加固定長度的二進位 envelope，見 D803、PROTOCOL §1、§7.1）、Q14（行情過舊由策略自己驗時間戳；TD 降級時拒新單、放行撤單，重連後以 `OnRecon` 重置狀態，見 D603）、Q15（只有一層 linger，5 秒；需求不綁 TTL，見 D901、D905）、Q17（backfill 的範圍從下單決定：worker 以 Report 回報每個商品第一次下單的時間，agent 建 cursor，見 D601）、Q18（atom 的身分加上 route，商品先經 SYM 解析，見 D202）、Q19（topic 名稱沿用 mftik，見 D202、PROTOCOL §2）、Q20（BinanceCM、BinanceEO 維持獨立 venue、不共用連線；BinancePM 是帳號種類，見 D301）。
