# DECISIONS

> 決策紀錄（2026-10-09 更新：幣與數量的單位、SYM listing 分活躍表與終結表，以及 symbol UID，D203。同日再定 UID 的分配：SYM 發隨機、唯一、永不重用的整數，不是單調序號；跟列持久化，重啟與重載不改。寬度與範圍已定：契約型別 `uint32`，`1..4294967295`（`1..2^32-1`），0 非法；Postgres 欄位是 `bigint`，`CHECK (uid BETWEEN 1 AND 4294967295)`，見 PROTOCOL §10 P13）。只收已經講定的事；還在討論的放在 [§待討論](#待討論)。協議與契約見 [PROTOCOL.md](PROTOCOL.md)。
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
- worker 換版一律走換 incarnation。各平面的強制換法：MD 對連線 restart；TD drain-replace；STS 結束 session 後開新的。
- **MD 連線不做 make-before-break。** 需要新舊版並存時，在 NATS subject 加版號，新版 worker 發在新的 subject，平台內不做遷移。為此 SDK 一律用 binding 給的 subject，不自己組（PROTOCOL §7），之後加版號只改 MD agent。
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
- **TD 和 MD 一樣依 refcount 起停 worker。** 需求的單位是帳號，每個 `api_id` 一個 owner 集合 `{(session, api_id)}`。0→1 時拉起 account worker（私有 websocket ＋ HTTP 連線池，Start 時 recon）；1→0 時 linger 之後才停。worker 存在就等於這個帳號可以交易，沒有另外的交易層開關，也不保留 mftik 的常駐層。閒置帳號的歷史補正由 TD agent 做（D601）。
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

- **atom** 是交易所那一側的一個訂閱項：在一條 websocket 上 subscribe 一次、交易所 ack 一次的最小單位，身分是 `(venue, route, channel)`，channel 逐字照抄交易所參數；route 是 WS 路徑，由 adapter 從解析後的商品推得（D403、D202）。
- **feed** 是策略宣告的行情：`topic.UniversalTicker`。
- **在同一個 venue 上，feed → atom 一律 1:1。** 一個平台 topic 在一個 venue 上只對應一個 channel。
- **atom → 事件可以 1:N。** 一個 frame 解碼出多種事件（Deribit `ticker.*` → quote、greeks、OI），是解碼端的扇出，不需要 join。
- topic 的模型依「交易所原生同時推送的欄位」定義。某 venue 不推的欄位是 optional，並提供能力表給策略查詢。`ticker` 只有 24h 統計；最佳買賣是 `bestquote`。
- atom 層的 refcount 仍然需要（多個 feed 共用一個 atom），由 MD agent 的 orchestrator 計算（D901），沒有組合語意。conn worker 只照 desired 清單做 subscribe／unsubscribe 的 diff。
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

- **三種 worker 都是 artifact，manifest 帶 `kind`：** `strategy`（STS 的 session worker）、`md.conn`（conn worker）、`td.account`（account worker）。agent 只接受自己那一種。agent 依指派、controller 給的 artifact 引用，加上 manifest 的 entrypoint 與參數，找到並起動進程（PROTOCOL §8）。
- **MD、TD 的 artifact 各只有一種，都在這個 repo 裡實作，** 由 CI 在 release 時建好推上 registry。哪個 instance 用哪一版由 controller 的配置指定，worker 的版本因此可以單獨升級；換版仍然走換 incarnation（D106）。MD、TD 主機上的拉取與驗證和 STS 用同一套程式（`internal/platform/artifact`，D006）。
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

### D601 歷史補正是 TD agent 的 job

account worker 依 refcount 起停，沒有常駐層（D901），所以閒置帳號的歷史由 TD agent 自己補，不為此拉起 worker。

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
  - confirmed：backfill job 從交易所 REST 拉回的。
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
  - 成交的 `order_id` 在 orders 表還沒有終態列的，backfill 每輪按 id 查單：終態就落，否則下一輪再查。待查清單由 DB 算出（fills LEFT JOIN orders），不另存。
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
  - TD agent 收到後，經由同一個 queue 把 scope `(api_id, ticker, since)` 寫進 Postgres，`since` 取 `LEAST`（重啟、換 incarnation 都不會把起點往後推）。cursor 的建立完全是非同步的，不影響下單。
  - 起點是第一次下單的時間，一定早於任何成交，所以第一批成交即使 provisional 丟了，也讀得到。
  - 不涵蓋：人工在新商品上的交易。人工單不屬於平台（D602）；之後需要時，可以由 adapter 用交易所的帳戶層級介面發現商品。
  - 資金流水（出入金）之後以同樣的方式加入，另一條 cursor。
- **rate：** 只靠固定 interval 加 jitter。一輪內的請求量與 429 先不另外處理，實測有問題再加。
- **TD agent 因此多了：**
  - 為 REST 讀取解開帳號憑證。憑證本來就以 TD instance 的公鑰封裝，由 agent 解開後交給 worker。
  - venue 的 REST 讀取，每個 job 自己 recover panic。
  - Postgres 連線。Postgres 不在時 queue 會滿、backfill 停住，交易不受影響。
- agent 掛掉時 worker 照跑（D102–D104），只影響補正與落庫。

### D402 行情狀態的權威在 conn worker；md.fetch 是無狀態服務

行情狀態（book、tape）的權威在持有該 atom 的 conn worker 記憶體裡，跟 TD 的 ledger 在 account worker 裡是同一個模式：快照打底，fold 串流的增量，有變化就推出去，晚加入的人向持有者要目前的狀態。

- **book：**
  - 不管策略訂的是 `orderbook` 還是 `orderbook_delta`，conn worker 都維護一本本地全量 book，再依 feed 決定推什麼。兩個 topic 對到同一個 atom：交易所的增量 channel；沒有增量 channel 的交易所，用它最完整的 book channel。
  - 跟交易所同步的邏輯全在 conn worker 裡：快照與增量對齊、驗序號、斷號後重抓。快照用 adapter 的 REST reader 在進程內抓（Binance 類），或用訂閱時 WS 推來的那一張（OKX、Bybit 類）。不經過 md.fetch 服務：正確性不依賴另一個服務，而且對齊有時序要求，多一跳 NATS 只會讓快照變慢。
  - `orderbook`：本地 book 的 top-N 全量（`OrderBook`），top-N 有變才推。N 由 MD 的設定決定，同一個 venue 上固定。
  - `orderbook_delta`：正規化後的增量（`OrderBookDelta`：價位 → 數量，`"0"` 為刪除）。conn worker 對交易所重新同步之後推一則 `reset`，內容是完整的 book。增量被 prehook 丟掉（D803）時，seq 會跳號，SDK 的小工具就向 `md.view` 重拿。
  - 晚加入：SDK 先訂 `md.f`，再向持有者要 `md.view`，拿到 seq S 時的 book，之後只套用 seq > S 的增量。seq 斷號或 `conn_epoch` 改變時，重新要一次。這是 SDK 提供的可選小工具，fold 跟交易所無關。
- **tape：**
  - 只錄 `trade`、`aggtrade`、`liquidation`，book 類不錄。錄不錄由 topic 決定，MD agent 推導 `AtomAssignment.record`。
  - 每個 atom 一個 ring，存已經發佈的訊息 bytes（envelope ＋ `FeedEvent`，D803），回覆時不用重新編碼。上限是時間窗加筆數，worker 另有總上限，算進 `Limits.memory_mb`。
  - WS 重連時在 ring 裡記一個 gap。
  - 接 live：SDK 先訂 `md.f` 再讀 tape，丟掉 `(conn_epoch, seq)` 不小於第一筆 live 的部分。live 和 tape 出自同一個進程、同一套序號，接縫是精確的；沒有 id 的 liquidation 也適用。
  - 不用 Redis。tape 跟著 worker 進程和 atom 一起活：只有進程重新掛起（crash，或停掉後 spawn 新的 incarnation）、或 atom 的 linger 結束，tape 才會沒了。
  - worker 不隨版本遷移（D103、D106）：發新版只影響之後 spawn 的 worker，跑著的進程繼續用原本的 binary，tape 不受影響。只有刻意強制換版（D106：MD 對連線 restart）才會清空那條連線上的 tape。
  - tape 只是熱身用，回覆會說明涵蓋多少。要一直熱的，跑一個 tape keeper：不下單、不綁帳號的策略，跟 mftik 一樣，tape 跟著它的生命週期走（D404）。
- **md.view：** 持有 atom 的 conn worker 服務 `md.view.{inst}.{topic}.{ticker}`，形狀跟 `md.f` 一樣，對應 `td.view.{api_id}`。回覆依 topic：trade 類回 tape 分頁（由新到舊，以 `(conn_epoch, seq)` 當游標，每頁壓在 NATS 的 1 MiB 以下），`orderbook_delta` 回 seq S 時的 book。沒有 responder，就是沒有 worker 持有這個 atom。
- **md.fetch：**
  - 無狀態的服務，跟 SYM 一樣：每個 region 部署，NATS queue group，任何 replica 都能回答。不需要 agent、lease、fencing。不落庫。
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
- envelope 是 `md.f` 訊息開頭的固定長度欄位（PROTOCOL §7.1），不用 NATS header：header 是文字、nats.go 每則都要解析成 map，固定位置的欄位可以直接 unsafe 讀。
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
- **平台不清場（D602）。** 新的 incarnation 從零開始；上一個進程留下的掛單，由策略自己以 algo 範圍的 `batch_cancel` 處理，或接手。第一次 `OnRecon` 會帶上自己 algo 的掛單與帳號的部位，對帳在那裡做（D603）。
- 重啟期間 MD、TD 的需求項目不刪（D901），account worker 與行情不受影響。

### D602 策略的單由策略自己管：algo_id

- **平台不替策略撤單。** crash、`FAILED`、STS agent 不在時留下的掛單，平台都不自動處理：要嘛由宣告同一個 algo_id 的 session 下一次啟動時自己清，要嘛人工撤。帳號層級的 cancel-on-disconnect（`AccountSettings`）是另一回事，照帳號的設定。
- **algo_id 是訂單的命名空間，由設定宣告，不由 API 分配。**
  - 一個 session 可以宣告多個，例如同一個帳號上 BTCUSDT、ETHUSDT、BNBUSDT 各一個。宣告在 spec 的帳號綁定上（`AccountDecl.algo_ids`），策略下單時自己選用哪一個。
  - 1–1023（10 bits），0 保留，讓人工單或格式不對的 id 不會被當成某個 algo。只需要在同一個帳號內唯一：`client_order_id` 與 `batch_cancel` 都以帳號為範圍。
  - 跨 session、跨 incarnation 不變：新 session 宣告同樣的 algo_id，就能接手上一個 session 留下的單。
- **同一個帳號上，一個 algo_id 同時只屬於一個非 terminal 的 session**，否則兩邊會撤到對方的單。由 TD agent 檢查：TD 的需求項目帶 algo_id 清單，和同帳號其他 owner 重疊就回 `REJECTED`（`algo_conflict`）。帳號的權威在 TD，檢查留在那裡；controller 是唯一的寫入者，本身不會並行寫出衝突，但 API server 在不同 region 並行建立 session 時仍可能宣告重疊的 algo_id。
- **編進 `client_order_id`：** uint64 `ver(4) | algo(10) | ts_ms(40) | seq(10)`，以十進位字串傳。沿用 mftik 的 uint64 形式，欄位重新分配：
  - `ts_ms`：自 2026-01-01 UTC 起算的毫秒，40 bits 可用約 34 年（到 2060 年）。
  - `seq`：每個進程每毫秒 1024 個。超過時 `ts_ms` 往前借一毫秒，實際上不會發生。
  - `ver` 只用 1–7，最高位維持 0，用 int64 解析也不會壞。十進位最多 19 位，各家交易所的長度限制都放得下。
  - 新 incarnation 的 seq 從 0 起算也不會撞號：重啟至少退避 1 秒，`ts_ms` 不會和上一個進程重疊。
  - `Submit` 本身仍帶 `session_id`。
- **從 `(api_id, algo, ts_ms)` 對到 session。** 同一個帳號上一個 algo 同時只有一個 session，所以靠持有紀錄就能對：TD agent 在 ack READY 時寫入取得時間、需求項目消失時寫入釋放時間，存成 Postgres 的 `(api_id, algo_id, session_id, acquired_at, released_at)`。需求項目在 session 結束時就從 etcd 刪了，不能當歷史用。
  - 交接時，舊 session 的進程結束、需求被刪、TD agent ack 新 session、新 session spawn 到 Start，這段實際間隔至少幾百 ms，遠大於主機之間 ms 級的時鐘誤差，所以不需要冷卻期。
  - 落庫的歸屬（D601）就用這條規則。
- **只能動自己的單。** SDK 只允許使用 spec 裡宣告給該帳號的 algo_id。TD 檢查 `Submit`、`Cancel`、`BatchCancel` 帶的 algo_id 與 `client_order_id` 的 algo 欄位一致，不符就以 1xx 拒絕。沒有 `client_order_id` 的單（人工下的）不屬於任何策略。這是防呆，不是防惡意。
- **SDK 只提供能力，怎麼用是策略的事：**
  - `ctx.batch_cancel(api_id, algo_id, …)`：TD 撤掉這個帳號上 algo 欄位等於 algo_id 的所有掛單，可以再限定商品，不分 session、incarnation。TD 在 account worker 裡轉譯成交易所的撤單。
  - 以 algo 查自己的掛單（`td.view` 的 OMS 查詢）。
  - 策略重啟或新 session 接手時，在 `OnStart` 用它們清場或接手。
- STS agent 不再需要 `td.order` 的權限。session 進入 terminal 時，controller 依 D901 的順序刪除 TD 需求項目。

### D403 MD 連線的身分、endpoint 與放置

- **venue 以「一條連線、一組商品」為單位**，照 mftik 的 venue registry（D201）。host 或產品線不同就是不同 venue，例如 `Binance`（現貨）、`BinanceUM`、`BinanceCM`、`BinanceEO`。憑證屬於帳號種類，不屬於 venue；`BinancePM` 是帳號種類，不是 venue（D301）。
- **endpoint 是連線的可選參數，不是身分。** 同一家交易所提供的替代 host（例如針對某個雲端優化的 endpoint）視為同一個 venue，由 MD instance 的設定選擇，進 conn worker 的 spawn spec。換 endpoint 要換 incarnation（D106）。
- **route：同一個 venue 裡，商品和 topic 決定要連哪條 WS 路徑。** 例如 OKX 的 K 線在 `/ws/v5/business`，其他公開行情在 `/ws/v5/public`；Bybit 的公開行情依類別分成 `/v5/public/spot`、`linear`、`inverse`、`option`。不同 route 不能共用連線。OKX、Bybit 是一份憑證的統一帳戶，拆成不同 venue 會破壞 UniversalTicker 與 TD 的帳號，所以由 adapter 從解析後的商品推出 route（D202）。大多數 venue 只有一個 route。
- **連線的身分是 `(venue, route, n)`。** 之後加版號（D106）就是 `(version, venue, route, n)`。同一組 atom 一律共用連線，兩個 session 要同一個 atom 就是 refcount=2，不會多開連線。
- **只有連線達到交易所的上限才開 n+1。** 上限由 adapter 宣告，例如 Binance USDⓈ-M 一條連線 1024 個 stream。新 atom 放進同一個 `(venue, route)` 下還有空間、n 最小的那條。不整併：1024 連期權鏈也用不完，實際上幾乎只會有 n=0。

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

### D906 session worker 的 NATS 憑證：每個 session 一份

- **簽發：** STS agent 持有 NATS account 的簽章金鑰，spawn 時為每個 session 簽一份 user JWT（NKeys）。和其他憑證一樣經 `Init` 的 socketpair 送進去，不放環境變數、不落檔。簽發是本機的密碼學運算，不需要連 server。
- **權限從 spec 推：**

  | | 允許 |
  |---|---|
  | publish | `td.order.{綁定的 api_id}`、`td.view.{綁定的 api_id}`、`md.view.{綁定的 MD instance}.>`、`md.fetch`、`sym`（D202）、`log.sts.{session_id}` |
  | subscribe | `md.f.{綁定的 MD instance}.>`、`td.evt.{綁定的 api_id}`、`_INBOX_{session_id}.>` |

  - 行情的訂閱權限用萬用字元，只分到 instance：selector 的成員會變，沒辦法逐一列出；行情是公開資料，放寬讀取的風險很低。
  - reply inbox 每個 session 一個獨立的前綴 `_INBOX_{session_id}`。預設的 `_INBOX.>` 會讓任何連線都能讀到別人的回覆，包括 `td.view` 查回來的帳務資料。
- **分層：** 跨帳號由 broker 保證，策略只能對自己綁定的帳號發 `td.order`；同一個帳號內的 algo 由 TD 檢查（D602，防呆）。
- **不設過期、不做撤銷：** 設過期的話，到期時 server 會斷開連線，重連瞬間 core NATS 的 `td.evt` 可能漏。憑證只在進程記憶體裡，session 結束、進程一死就沒了。
- **不影響下單延遲：** 授權只在建立連線時做一次（`CONNECT` 帶 JWT 與對 nonce 的簽章）。之後每次發佈，server 只拿 subject 比對這條連線的權限快取，訊息本身不帶任何 token。
- MD、TD 的 worker 是平台程式，用所屬 instance 的憑證。

### D202 UniversalTicker 是佔位，解析之後才知道要訂閱什麼

照 mftik 的 symbol plane（D201）。

- **給人寫的是 UniversalTicker，系統內部是 UID（D203）。** spec 與設定寫 ticker 字串，載入時才解成 UID。STS、TD、MD 之間傳這個整數，各平面再用 UID 向 SYM 拿欄位。需求項目、`td.order` 今天仍帶 ticker，換成 UID 是後續。NATS 的 subject 用人讀的名字，不用 UID；確切拼法還沒定（PROTOCOL §10 P14），這一版的 `md.f` 仍是 `md.f.{inst}.{topic}.{ticker}`。SYM 表的主鍵是 UID。交易所的寫法（`exch_ticker`）只出現在 adapter 和 SYM。
- **翻譯是查表，經 SYM 解析，不是字串轉換。** `BTCUSDT` 不帶 base／quote 的切法，交易所的寫法也不一定是 base＋分隔＋quote。
- **ticker 的 base／quote 是 SYM 正規化之後的資產（D203）。** 資產代碼一律大寫，再套別名表（例如 `XBT` → `BTC`），只在 SYM。先正規化，再做 quanto 判斷，再推 ticker；strike 先依 `spell_strike` 正規化，`6400.0` 與 `6400` 是同一個 ticker。`USD`、`USDC`、`USDT` 不合併。quanto 不入庫。
- **先解析，再交給 adapter。** MD agent 收到需求項目，先在已載入的活躍表裡解析（D203：Resolver 與熱路徑不讀終結表），找不到、已下架、已到期，或不是可交易（不是 `TRADABLE`；quanto 不入庫）就是 `REJECTED`；再把解析結果交給 adapter，由它決定 route 和 channel。
  - adapter 因此是純函數：不查 SYM、不做 I/O，只用資料就能測。
  - mftik 的 adapter 自己帶 SymbolResolver，在裡面查；mftik refactor 的 B7-01 改成先解析再 `atoms_for`。這裡照後者。
- **回程帶交易所那一側的 key。** 私有事件、REST 拿回的成交與訂單帶 `(venue, category, exch_ticker)`（venue 見 D301），account worker 和 backfill 以 SYM 反查換回 UniversalTicker。category 是 key 的一部分：統一帳戶的 venue 上，同一個寫法可能是兩個商品（Bybit 現貨與 linear 的 `BTCUSDT`）。
- **行情的熱路徑不查表。** conn worker 依 `AtomAssignment` 的 feed 在事件上蓋 UniversalTicker，解析只在 atom 建立時做一次。account worker 每個 ticker 第一次下單時解析一次，之後查記憶體。
- **atom 的身分是解析後的 `(venue, route, channel)`，id 為 `<venue>:<route>:<channel>`。** channel 仍逐字照抄交易所參數；route 從解析後的商品推得（Bybit 依 category），所以同一個 channel 字串在不同 route 上是不同的 atom。
  - 不拿 `topic.UniversalTicker` 當 atom 的身分。一個 channel 常同時服務多個 topic：`orderbook` 和 `orderbook_delta`；Bybit linear 的 `tickers.*` 帶 ticker、funding、OI、mark price。以 (topic, ticker) 當身分，同一條連線上會重複訂閱同一個 channel，退訂一個就把其他的斷掉。這是 mftik `MdVenueSubscriptions` 處理過的問題：產品的 refcount 和 wire 上的訂閱是兩本帳。
  - 佔位留在 MD 外面：`md.f` 的 subject 是 feed（`topic.UniversalTicker`），atom 不出現在資料面（PROTOCOL §7）。
- **topic 名稱沿用 mftik：** `bestquote`、`trade`、`aggtrade`、`orderbook`、`ticker`、`liquidation`、`funding_rate`、`open_interest`、`greeks`、`kline_<interval>`，加上這裡自己的 `orderbook_delta`、`mark_price`。表在 PROTOCOL §2。
- **SYM 照 mftik：**
  - 刷新以 `(venue, category)` 為單位，各自刷新、各自下架：現貨的刷新不會讓 perp 下架。
  - 下架與到期不改 UniversalTicker（D203）。狀態還會變的留在活躍表；進了終結狀態就搬到終結表，ticker 字串不變。訂單與 session 仍指向同一個 ticker。
  - 契約見 PROTOCOL §7.3，表見 `migrations/0002_symbol`。
- **誰用 SYM：STS、MD、TD；API 不用。** STS 這端是 session worker（經 SDK），所以它的 NATS 權限有 `sym`（D906）。
- **啟動時把用得到的 venue 的活躍表載入，之後跟著變更重載（D203）。** 進程啟動時，依 `(venue, category)` 分頁把未終結的列讀進記憶體（每頁最多 500 筆）。Resolver 與熱路徑只讀這張表；終結列不進這份快取。盤中會有新上市、到期與下架，selector 也要挑得到新合約，所以 SYM 刷新出差異時發變更通知，帶 revision，收到的進程重載那個 `(venue, category)` 的活躍列。通知的 subject 與訊息在 E2 定（PROTOCOL §10）。取代原本的「單一商品精確查詢、用戶端快取 10 分鐘」。
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
  - D601 的 backfill 範圍 `(api_id, ticker)`、D602 的 algo_id 都不假設帳號只有一個 venue，不用改。
- **BinanceCM、BinanceEO 維持獨立的 venue。** 2026-06 的 CM-UM 整合之後，CM 仍然是 `dapi`、`dstream`，錢包也沒合併；EO 的行情雖然在 `fstream` 上，REST 與帳戶都是自己的。D403 的規則不變。
  - CM 的 `dstream` 沒有路徑分流，只有 `default` 一個 route。
- **UM 和 CM 不共用連線。** 整合之後部分 stream 可以跨 host 訂，但連線的身分仍是 `(venue, route, n)`。全市場陣列在兩個 host 上都會送 UM＋CM 合併的內容，以 `st` 區分，adapter 解碼時丟掉另一個 venue 的列。
- **UM、CM 之間的耦合不寫進 adapter 契約。** 整合之後兩者共用 IP 的 weight 與下單數上限、共用持倉模式（`dualSidePosition`），CM 的 STP 跟著 UM。只記在 Binance adapter 的實作說明。

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
  - 根目錄依平面定義 adaptor 介面：MD 用的 `Market`、`BookSync`、`PublicREST`；TD 用的 `AccountKind`、`PrivateStream`、`History`；SYM 用的 `Listing`。
  - `binance/`、`bybit/`、`okx/` 各自實作。cmd 明確地把它們註冊進 MD、TD、SYM 各自的 factory，不靠 `init()` 的副作用。平面只看介面，不 import 具體的交易所。
  - 介面不放進各平面：放進去的話 exchange 要 import `internal/md`、`internal/td`，就不能獨立。
  - exchange 只依賴 contract，之後要拆成自己的 Go module 也不用改結構。一期留在同一個 module。
- **共用的交易 enum 定義在契約裡，不在 exchange。** `Side`（Buy／Sell）、`OrderType`、`TimeInForce`（GTC／IOC／FOK）放進 `common.v1`，Go 這邊 exchange、各平面、SDK 都直接用產生的型別。SDK 下單也要用它們，而 SDK 不能 import exchange；其他語言的 SDK 也需要同一份定義。exchange 根目錄只放交易所之間共用、平台其他地方用不到的東西：憑證、錯誤分類、限速。
- **依賴規則由 `internal/archtest` 的測試檢查**，`make check` 與 CI 都會跑：
  - 平面之間互不 import；平面只用 exchange 的介面；API 不 import exchange、不用 `sym.v1`。
  - `internal/platform` 不 import 任何平面或 exchange。
  - exchange 只依賴 contract；contract 只依賴 protobuf 與標準庫。
  - sdk 只依賴 contract，整個依賴閉包不得出現 etcd、Postgres driver、exchange 或 internal：策略不碰 etcd，策略的 binary 裡也不該有簽交易所請求的程式。
  - examples 只用 sdk 與 contract，模擬外部的策略作者。
- 搬家在 E1-07 之前做，那時大部分套件還是空殼。

### D007 契約分成草案與定案

- 每份契約都有狀態與定案的 EPIC，表在 PROTOCOL §1。現在全部是草案。
- `buf.yaml` 的 `breaking.ignore` 列出草案的檔案：草案可以自由改，CI 不擋。定案的 EPIC 把自己的檔案移出清單，寫進完成標準。C3、C4 在 E1 結束時第一批移出。
- 代價：定案之前，不小心改壞草案不會被 CI 抓到，只能靠 review。還沒有任何東西部署，沒有相容性要保護，可以接受。
- **趁還是草案，拿掉兩條跨契約的 import：**
  - `stsctl.v1` 不再 import `state.v1`。策略的契約版本偏差最久（PROTOCOL §1），etcd 的 schema 卻是不相容就換 key 根，兩者不該綁在一起。session 的宣告（`SessionSpec` 與它的組成）抽成獨立的套件 `mftiko.session.v1`，歸 STS；`state.v1`、`stsctl.v1` 與 E10 的 API 契約都 import 它。`EntryState`、`Universe` 在 `stsctl` 裡另定綁定自己的型別。
  - `td.v1` 不再為了 `Side` import `md.v1`：`Side` 搬到 `common.v1`（D006）。

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
  - E2-03 拆了表。`migrations/0002_symbol` 是活躍表 `symbol_active` 與終結表 `symbol_terminal`，欄位相同，主鍵都是 `uid`。`symbol_filter_active` 參照活躍表、`symbol_filter_terminal` 參照終結表，終結時跟列一起搬。活躍表用 generated column 從 `universal_ticker` 拆出 `venue`、`category`，並有 `UNIQUE (venue, category, exch_ticker)` 與 `universal_ticker` 唯一；終結表沒有這兩個唯一，同一個 ticker 可以留多列。`symbol_revision (venue, category, revision, refreshed_at_ns)` 給變更通知比對 revision（PROTOCOL §10 P7 的 subject 與訊息仍開放）。另有 `symbol_uid`（主鍵 `uid`，列不刪），兩張 listing 與兩張 filter 參照它，所以歸檔之後也不會再抽到同一個 uid。結算幣欄位名是 `settle`，與契約一致，另外有 `contract_value_ccy`、`price_ccy`。strike 入庫時去掉尾端的零。
- **UID 是系統內部的商品身分。** UniversalTicker 是給人看的字串，只用在寫設定與 spec。系統裡面傳商品用 UID（整數）。各平面要欄位時用 UID 向 SYM 查。STS、TD、MD 之間用 UID，不用 ticker 字串。
  - UID 由 SYM 分配，是隨機整數：唯一、永不重用，不是單調序號，也不是 Postgres sequence。活躍表與終結表合在一起唯一，同一個 UID 不會同時在兩張表。UID 跟列一起持久化，所以重啟與重載都不改它。搬到終結表時 UID 跟著列走。碰撞時 SYM 重試，再抽一個。
  - 同一個 ticker 下架之後再上市，是新的一列，拿新的 UID。舊的成交與持倉仍指向舊 UID。
  - ticker 解成 UID 只發生在設定與 spec 載入的邊界。`SessionSpec` 的 feed 仍寫 UniversalTicker，載入時用 `GetRequest` 換成 UID。
  - `symbol_filter` 也拆成活躍表與終結表。列進了終結狀態時，filter 跟 listing 列一起搬。
  - SYM 的 `List` 以 offset＋limit 分頁。翻頁期間列可能搬表或新上市，跨頁不一致可以接受。
  - `Universe.current` 是 selector，尚未定，不在這一條。selector 沒有 UID。selector 的成員今天仍是 ticker 字串；成員是商品，改成商品 UID 是後續。
  - 這一版只在 `SymbolInfo` 加上 `uid`，並加 `GetByUidRequest`。`td.order`、需求項目、selector 成員今天仍是 ticker 字串。SQL 的兩張表以 `uid` 為主鍵。
  - NATS 的 subject 用人讀的名字，不用 UID。確切拼法還沒定（PROTOCOL §10 P14）。這一版維持 `md.f.{inst}.{topic}.{ticker}`，不改 subject。策略 hook 看 ticker 還是 UID 見 P15。
  - 契約型別是 `uint32`，範圍 `1..4294967295`（`1..2^32-1`），0 非法（PROTOCOL §10 P13）。Postgres 欄位是 `bigint`，`CHECK (uid BETWEEN 1 AND 4294967295)`。E2-03 的 store 照這個範圍抽號，先插入 `symbol_uid`，撞到 unique violation 就換一個再插。`symbol_reject_shared_uid` 仍擋兩張 listing 同時持有同一個 uid；搬表在同一個交易裡先刪活躍列再插入終結列。
  - **進程內 Resolver（E2-05）只給測試與開發。** 它對設定的每個 `(venue, category)` 直接呼叫 `PublicREST.Listing`，來源必須實作 `exchange.InProcessUIDSource`（`exchangetest`）。列以 `Listing.Validate` 過濾後放進記憶體。`Validate` 過了但 `Ticker()` 失敗的列略過：記 log、計數，不讓整次 venue 重載失敗。不經過 SYM 服務，也不寫 Postgres。UID 由這個進程自己發；測試注入固定順序，沒注入就用與 SYM 相同的隨機抽號，但抽到的號碼不寫進 `symbol_uid`。這種 UID 不是正式 UID：不落庫，不跨進程、不跨平面傳。真實 venue 的進程等 SYM 分配的 UID。重載在這個進程裡整份換上；還在清單上的 ticker 沿用同一個進程內 UID，從清單消失的不重用，同一個 ticker 再上市拿新的。

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

已解決：舊的 Q2（綁定變化由 controller 從 ack 整理進 STS 的指派，STS agent 轉送給 worker，見 D907）、Q4（durable store 是 etcd，訂單歷史另放 Postgres）、Q12（每個 region 一個 etcd，見 D005）、Q1（每個策略一個 artifact，見 D902、D802）、Q13（生命週期兩段，見 D107）、Q11（TD 也依 refcount 起停，見 D901）、Q16（歷史補正是 TD agent 的 job，見 D601）、Q9（tape 與 book 的權威在 conn worker，以 `md.view` 讀取；md.fetch 是無狀態服務，見 D402）、Q3（一期沒有執行中加減訂閱的介面，見 D903）、Q6（spec 不可變維持；策略狀態經 volume 與資料 artifact 延續，見 D904）、Q5（單一 dispatcher、TD／MD 兩個 buffer、prehook 依 `recv_window_ms` 丟舊行情，見 D803）、Q8（平台不清場；策略以 algo_id 範圍的 `batch_cancel` 自己清，否則人工撤，見 D602）、Q7（連線以 `(venue, route, n)` 共用，滿了才開下一條，不整併，見 D403）、Q10（線上編碼一律 protobuf，`buf breaking` 機械檢查相容性；`md.f` 在 protobuf 前面加固定長度的二進位 envelope，見 D803、PROTOCOL §1、§7.1）、Q14（行情過舊由策略自己驗時間戳；TD 降級時拒新單、放行撤單，重連後以 `OnRecon` 重置狀態，見 D603）、Q15（只有一層 linger，5 秒；需求不綁 TTL，見 D901、D905）、Q17（backfill 的範圍從下單決定：worker 以 Report 回報每個商品第一次下單的時間，agent 建 cursor，見 D601）、Q18（atom 的身分加上 route，商品先經 SYM 解析，見 D202）、Q19（topic 名稱沿用 mftik，見 D202、PROTOCOL §2）、Q20（BinanceCM、BinanceEO 維持獨立 venue、不共用連線；BinancePM 是帳號種類，見 D301）。
