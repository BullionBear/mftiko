# DECISIONS

> 決策紀錄（2026-10-05 更新）。只收已經講定的事；還在討論的放在 [§待討論](#待討論)。協議與契約見 [PROTOCOL.md](PROTOCOL.md)。
>
> 不照抄 mftik `refactor/process-planes`，也不照抄 Strategon 的 agent。兩者都只當參考。

## 已定

### D1 三個平面是三種專門的 agent

STS（策略）、MD（行情）、TD（執行）各是一種 agent，角色接近 Strategon 的 agent：收 desired、reconcile、拉起並看管進程。但每種 agent 只管**一種已知形式**的 worker，所以不需要 Strategon 為了跑任意程式而有的 artifact、OCI driver、download／verify／switch 部署階段。

| agent | worker 形式 | worker 身分 |
|---|---|---|
| STS | Strategy Actor 的宿主：runtime ＋ 一個繼承 Strategy Actor 的策略 | `session_id` |
| TD | 一個帳號的私有 websocket ＋ HTTP 連線池 | `api_id` |
| MD | 一條公用 websocket | `(venue, route, n)`（D26） |

### D2 agent 的骨架

| 抽象 | 職責 | 共用或平面自有 |
|---|---|---|
| Intake | 收 desired（watch etcd）：主資源來自 API，lease 項目來自 STS agent | 共用 |
| Planner | desired → 每個 worker 的 spawn spec ＋ live config，純函數 | 平面自有 |
| Fleet | 以 shim 為底的 worker 集合：spawn、attach、推 live config、觀測、release。supervisor 在這裡，只管 incarnation 與 backoff | 共用 |
| Reconciler | 單一 loop，diff 計畫與觀測，產生動作 | 共用 |
| Policy | 失敗語意與重啟規則 | 平面自有 |
| Provider／Gate | 跨平面依賴：MD、TD 依 lease 推導並回 ack／nack（Provider），STS 等依賴就緒（Gate） | 平面自有 |

各平面最重要的設計決定，是哪些欄位放在 **spawn spec**（變了只能換 incarnation），哪些放在 **live config**（推給活著的 worker）。

| | spawn spec | live config |
|---|---|---|
| STS | 策略、參數、依賴綁定的初始值 | stop／fail、依賴綁定的變化 |
| TD | 帳號、venue、憑證 | drain |
| MD | venue、route、連線 URL（endpoint 參數，D26） | atom 集合（帶 generation） |

### D3 shim 保留，是平面輪換時收養 worker 的地方

不採用 Strategon agent 的 adopt（pid ＋ start_time ＋ pidfd）。shim 是 worker 唯一的父進程，agent 輪換時 worker 留在 shim 底下。

### D4 shim 與 worker 同壽命，輪換只換 agent

- shim 不原地升級、不重啟。shim 的版本就是 spawn 它的 release。
- 新 agent 必須能和舊版 shim 溝通：shim 協定獨立版本、只做向後相容的擴充。
- shim 的修正只在 worker 換 incarnation 時生效。

### D5 shim 消失，worker 必須死

- shim 消失時，worker 自行 graceful stop。
- 理由是 at-most-one：agent 必須能把「shim 不在」當成「worker 不在」。parent 關係也轉不走，新 shim 拿不到 wait status，也收不到孫進程。
- agent spawn 新 incarnation 之前，仍以 pid ＋ start_time 確認舊 worker 已消失。

### D6 控制權的 fencing 在 shim

agent 每次啟動帶 epoch。shim 只接受 epoch 最大的 agent 的指令；更大的 epoch attach 時，舊連線被踢掉。新舊 agent 可以短暫重疊。

### D7 控制走 shim，資料走 NATS

- 控制：agent ⇄ shim ⇄ worker。shim 只轉送、不解讀。live config 的推送因此天然受 epoch fencing 保護，控制面不依賴 NATS。
- agent 不在時，shim 把 worker 的事件帶 seq 緩衝；新 agent attach 時從上次 ack 的 seq 補讀。exit 另外落檔。
- 資料面（行情、下單、回報）走 NATS。
- agent 之間、API 與 agent 之間的控制走 etcd（D14），不走 NATS。

### D8 incarnation

- 同一個 worker 身分下，實體進程（shim ＋ worker）的計數。每換一個進程加 1。
- **計數存在 etcd，不綁 lease。** key 是 `/{plane}/{inst}/incarnation/{h}`，寫入者是該平面的 agent，三個平面共用同一套（PROTOCOL §3.2 W6）。spawn 前先以 txn 取號，條件是 writer key 的 rev（§3.2 W2）：被取代的 agent 取不到號，也就 spawn 不了。
  - 不放 `WorkerStatus`：它綁在 agent 的 lease 上，agent 一死就消失。舊 shim 都 Release 之後 agent 再起來，號碼會從 1 重來，和舊進程的 `td.evt` seq、`Recon` 撞號。
  - 不放 `/td/{inst}/account/{api_id}`：那是 API 寫的宣告（W1）。TD agent 寫進去會和 API 搶同一個 key，每次 spawn 都改動宣告的 revision，RBAC 也得讓 TD agent 能寫放憑證的前綴。MD 也沒有對應的宣告可放。
  - 不由 `(epoch, 本機 seq)` 推導：incarnation 和 epoch 就不再是兩個獨立的計數（見本節最後一條），型別也要從 `uint32` 改成兩個欄位。
  - 不用雜湊：雜湊只分得出不同，分不出新舊，「舊的晚到就丟」需要比大小。
  - 號碼只保證遞增，可以跳號。key 不刪：worker 停掉之後同一個 worker key 還會再起來，刪了就從 1 重來。例外是 STS 的 session 結束之後，見 PROTOCOL §3.2 W6。
- 訊息與事件帶 `(身分, incarnation)`：舊的晚到就丟；下游看到 incarnation 變了就重新同步。
- 換 incarnation 一律 delete-before-create：舊的確認消失之後才起新的。
- worker 換版一律走換 incarnation。各平面的強制換法：MD 對連線 restart；TD drain-replace；STS 結束 session 後開新的。
- **MD 連線不做 make-before-break。** 需要新舊版並存時，在 NATS subject 加版號，新版 worker 發在新的 subject，平台內不做遷移。為此 SDK 一律用 binding 給的 subject，不自己組（PROTOCOL §7），之後加版號只改 MD agent。
- 和 agent 的 epoch 是兩個獨立的計數：輪換 agent 不動 worker，換 incarnation 只動一個 worker。

### D9 跨平面依賴是單向 DAG，以 agent 層級的 lease 表達

```
API ──Session──────────▶ STS agent
API ──Account──────────▶ TD agent
STS agent ──lease──────▶ MD agent、TD agent
MD agent、TD agent ──ack／nack──▶ STS agent
MD agent、TD agent ─────▶ SYM
```

- API 只寫各 agent 自己的主資源，不替 STS 去要依賴。
- **lease 由 STS agent 發，不是策略 worker。** mftik 刪掉 lease，是因為發送端在策略的 loop 裡，hook 一卡 lease 就斷。搬到 agent 之後沒有這個問題。
- **TTL 必須比 agent 輪換的時間長**，否則輪換一次就會回收全部資源。
- **lease 的粒度是行情原子，或能推導成原子的規格**（feed、selector）。STS 不自己展開：lease 項目送到 MD agent，由它的 orchestrator 推導成 atom，再 reconcile → shim → conn worker。期權鏈、滾動合約這類動態訂閱因此全在 MD 內部消化，STS 的 lease 不必跟著變。
- **兩層 refcount：**
  - atom：0→1 訂閱，1→0 退訂。
  - 連線（conn worker）：refcount = 0 若且唯若這條連線上所有 atom 的 refcount 都是 0。
- **refcount 是 owner 集合的大小，不是加減計數器。** 每個 atom 記 `{(session, feed 或 selector)}`。lease 會重送、會重試，集合是 level-triggered，計數器會漂移。多個 feed 共用一個 atom 也自然處理。
- **只有一層 linger（D28）。** atom 的 owner 集合歸零後，等 linger 才退訂；連線在最後一個 atom 退訂時就停，最後一個 atom 的 linger 就是連線的 linger，不另設參數。
- **TD 和 MD 一樣依 refcount 起停 worker。** lease 單位是帳號，每個 `api_id` 一個 owner 集合 `{(session, api_id)}`。0→1 時拉起 account worker（私有 websocket ＋ HTTP 連線池，Start 時 recon）；1→0 時 linger 之後才停。worker 存在就等於這個帳號可以交易，沒有另外的交易層開關，也不保留 mftik 的常駐層。閒置帳號的歷史補正由 TD agent 做（D20）。
  - 代價：帳號的第一個 session 要冷啟動（建連線、recon）。`OnInit` 可以和它並行，影響有限。
  - session 進入 terminal 時，TD lease 項目直接刪除。平台不替策略撤單（D25）。
  - `restarting` 期間 lease 項目不刪，策略重啟不會讓 account worker 跟著起停。
- **ack／nack 是三種狀態，粒度是資源：**
  - ack：資源正常。
  - nack：provider agent 明確知道某個資源死了（它透過 shim 管 worker，所以知道）。
  - 沉默：不知道。可能只是對方 agent 在輪換，worker 照跑。TTL 之內維持現狀，不能當成 nack。
- 資料面的依賴（session worker ⇄ conn worker、account worker）走 NATS，不經過 agent。

### D10 Strategon 那一側的前提

「輪換時 shim 不動」依賴以下四條。任何一條破了，就等於 shim 被動了，worker 跟著死。

1. 停 agent 的信號只送到 agent 的 process group。shim 以 `setsid` 開自己的 session。
2. 開 `oci_host_pid`，不建 PID namespace。
3. 不以 `cgroup.kill` 整個 slot 停止。要有測試鎖住。
4. 舊 release 的 rootfs 仍被 shim 或 worker 當作 root 時不 GC。

### D11 Symbol 照抄 mftik

UniversalTicker（`<Venue>_<Category>_<SYMBOL>`）、`Parse` 嚴格／`Resolve` 寬鬆的分工、SymbolResolver（翻譯是查表不是字串轉換）、listing 與 filters、feed key `topic.UniversalTicker`。SYM 是普通服務，不是 agent。

### D12 feed 與 atom

- **atom** 是交易所那一側的一個訂閱項：在一條 websocket 上 subscribe 一次、交易所 ack 一次的最小單位，身分是 `(venue, route, channel)`，channel 逐字照抄交易所參數；route 是 WS 路徑，由 adapter 從解析後的商品推得（D26、D31）。
- **feed** 是策略宣告的行情：`topic.UniversalTicker`。
- **在同一個 venue 上，feed → atom 一律 1:1。** 一個平台 topic 在一個 venue 上只對應一個 channel。
- **atom → 事件可以 1:N。** 一個 frame 解碼出多種事件（Deribit `ticker.*` → quote、greeks、OI），是解碼端的扇出，不需要 join。
- topic 的模型依「交易所原生同時推送的欄位」定義。某 venue 不推的欄位是 optional，並提供能力表給策略查詢。`ticker` 只有 24h 統計；最佳買賣是 `bestquote`。
- atom 層的 refcount 仍然需要（多個 feed 共用一個 atom），由 MD agent 的 orchestrator 計算（D9），沒有組合語意。conn worker 只照 desired 清單做 subscribe／unsubscribe 的 diff。
- 沒有 projector、沒有多 atom 組合出來的 down 狀態、沒有多 atom 的 late-joiner 等待。feed 的狀態就是它那一個 atom 的狀態。
- 交易所沒有推送的東西不做成 feed，只能查詢（例如 Binance UM 的 open interest，走 md.fetch，D21），不以 REST 輪詢假裝成 feed。
- 策略需要兩個 feed 的最新值時，SDK 提供 opt-in 的小工具，那是函式庫，不是平台語意。

### D13 API 的角色

- API 層以宣告式設定操作平面，只負責開始與結束（start／end）。
- API 只寫宣告到 etcd（D14），不直接呼叫 agent。

### D14 控制面在 etcd，agent 無狀態

- 宣告、lease 項目、ack／nack、不能重新推導的 agent 狀態，全部放 etcd。agent 在本機不留狀態：desired 從 etcd 讀，worker 的實際狀態從 shim 取回。
- API → agent、STS agent → MD／TD agent 的控制，都是「寫 etcd、對方 watch」，沒有 NATS request-reply。控制面的契約就是 etcd 的 key 結構，加上每個前綴的寫入者。
- D9 的 lease 直接用 etcd lease：lease 項目綁在 STS agent 的 etcd lease 上，過期時 etcd 自動刪除；ack／nack 綁在 provider agent 的 etcd lease 上，provider agent 消失超過 TTL 時自動刪除，對應「沉默 = 不知道」。
- agent 重啟時 list 加 watch，owner 集合立刻完整恢復，不需要寬限期。
- etcd 本身故障不會造成大量 lease 過期：沒有 leader 時 lease 不會過期，新 leader 會把所有 lease 延長一個 TTL。
- etcd 不在時，所有 agent 都不能改狀態，但資料面照跑。
- NATS 只負責資料面。

### D15 生命週期由 shim 送給 worker，worker 回報結束方式

- agent 決定時機，經 shim 對 worker 送 `Init`、`Start`、`Stop`。shim 認得這組動詞與 worker 目前的階段，但不解讀內容。agent 不在時（例如主機關機），shim 可以自己送 `Stop`。
- worker 結束前告訴 shim 它要怎麼結束（`Exiting`：正常結束、初始化失敗、主動 fail、crash、卡死），`OnStop` 跑完再送 `Stopped`。exit code 是備援：shim 是 parent，一定拿得到 wait status。
- 沒有送 `Exiting` 又被 signal 結束的，就是被殺。
- **生命週期分兩段（`Init`、`Start`），加上 `Stop`。**

  | 階段 | agent 何時送 | 策略做什麼 |
  |---|---|---|
  | `Init` → `OnInit` | spawn 後立刻送，不等任何 lease | 解析參數、載模型、讀檔。沒有行情，不能下單 |
  | `Start` → `OnStart` | 所有 MD、TD lease 都 READY 之後 | 訂閱行情、需要行情的熱身（tape、歷史資料）。可以下單；`OnStart` 回傳之後才開始交付 hook |
  | `Stop` → `OnStop` | — | 收尾 |

  `OnInit` 可以和 lease 的建立並行，所以慢的準備工作不會拉長啟動時間。mftik 的三段制（多一個 `Ready`：拿行情熱身但平台禁止下單，帳號就緒後才開放）先不做；之後需要時以新的契約版本加入，只對在 `Hello` 宣告支援的 worker 送。

### D16 策略不處理可用性

- 策略只寫交易邏輯，不處理容錯。SDK 沒有 `OnMdUpdate`、`OnTdUpdate`。
- 行情斷了：策略收不到事件，自然不會反應。
- TD 壞了：下單失敗，策略拿到原因（帶原因的拒單）。
- 存活判斷由 agent 的 lease 與 ack／nack 處理（D9），不經過策略。

### D17 每個 region 一個 etcd cluster

一開始就每個 region 各架一個 cluster，不先用全域的。理由是故障範圍要和實際的依賴對齊：全域 etcd 在 TW、JP 斷線超過 TTL 時，少數方 region 的 lease 會被健康的 leader 判定過期，恢復時造成大量回收；每個 region 一個，則只有真的跨 region 的依賴受影響，而那些依賴的資料面（NATS）在同一次斷線裡本來就斷了。

1. **key 放在「會依據它行動」的 agent 所在的 region。** session spec 與 status 在 STS 的 region；lease 項目與 ack／nack 在 provider（MD／TD）的 region；其他 key 在該 instance 的 region。所有存取經過路由函數 `clusterFor(instance)`。
2. **只有 STS agent 和 API 連多個 cluster。** MD、TD agent 只連自己 region 的 cluster。STS agent 在每個它要寫入的 cluster 各持有一個 etcd lease。
3. **fencing 不跨 cluster。** agent 在每個要寫入的 cluster 各放一個 writer key（記錄 epoch），寫入時只比對那個 cluster 裡的 writer key。
4. **版本號不用 etcd revision。** agent 的 epoch 是持久化的計數器；推給 worker 的指令帶 `(epoch, seq)`。不同 cluster 的 revision 不能比較，cluster 重建時 revision 也會從頭開始，計數器不受影響。
5. **instance 名稱帶 region**（`md-tw-1`），在所有 cluster 之間唯一。
6. **跨 region 的 lease**（TW 的 STS 寫進 JP 的 cluster）：
   - TTL 要比 WAN 的抖動長。所有 lease 統一 60 秒（D28），已經滿足。
   - atom 的 linger 要長於「重連後重新 grant lease、重寫項目」所需的時間。
   - STS agent 重連時先重建 lease，再做其他 reconcile。
   - 監控跨 region 的 keepalive 失敗次數與 watch 中斷次數。
7. 只部署一個 region 時，就只有一個 cluster，程式碼不變。

### D18 每個策略一個 artifact，以 OCI image 格式打包

- 每個策略各自編譯、各自發版，與平台的 release 無關。session 釘住策略的 image digest，重新掛起也沿用同一個 digest。
- **只拿 OCI 當打包格式，不跑容器。** STS 把 image 解開成 rootfs，shim 以 rootfs 為工作目錄直接 exec。之後若要隔離，可以換成 Strategon 的 rootless OCI driver，格式不變。
- **分層讓策略之間共用：**

  | 層 | Go 策略 | 其他語言（由該語言的 SDK 決定，D19） | 共用 |
  |---|---|---|---|
  | runtime | — | 解譯器與 SDK，例如 python-build-standalone 加上自行實作的 Python SDK | 同一個 runtime 的策略 |
  | 依賴 | — | 第三方套件，例如依 `uv.lock` 安裝的 site-packages | lockfile 相同者 |
  | 策略本體 | 靜態 binary | 策略的程式碼 | 否 |

- **config blob 放 mftiko 的 manifest**：schema、名稱、runtime、os／arch、entrypoint、env、SDK 版本、支援的契約（格式見 PROTOCOL §8）。
- **build 不需要在目標平台執行任何東西。** Go 的 SDK 版本由 `debug/buildinfo` 從 binary 讀出。其他語言由該語言 SDK 的打包工具產生各層與 manifest，格式照 PROTOCOL §8。
- **每一層可重現**：檔案依路徑排序、mtime 歸零、uid／gid 為 0。輸入相同，digest 就相同。
- **發佈：** 推到 OCI registry；API 的策略目錄記錄 `name → digest`。
- **STS 主機：** 先檢查 os／arch、契約與大小，不符就 `INIT_FAILED`，不 spawn；下載缺少的層並驗 sha256；解開時拒絕絕對路徑、`..`、指出根目錄的 symlink、裝置檔、setuid；GC 刪掉沒被非 terminal session 釘住、也不是最新版的 rootfs，以及沒有引用的層。
- 依賴都在 artifact 裡，不需要 mftik 的節點層級 extras。
- 結果之一：STS worker 與 shim 會有版本偏差。部署時以 manifest 的 `contracts` 檢查，執行時以 `Hello` 協商（PROTOCOL §1）。

### D19 一期只做 Go SDK；其他語言照契約自行實作

- **不做 Python 綁 Go。** Python 呼叫 Go 的生態還不成熟：沒有像 PyO3 那樣官方支援的方案，只能 `-buildmode=c-shared` 編成 `.so` 再用 ctypes 匯入，還得處理一個進程只能有一個 Go runtime、cgo 的指標規則、GIL 與 Go thread 的互動、manylinux 編譯等問題。成本和風險都高，一期不做。
- **平台只出 Go SDK，它是參考實作。**
- **平台契約與語言無關。** session worker 只要做到 PROTOCOL §9 的每一條，就能用任何語言實作（例如另外做一個純 Python 的 SDK），以策略 artifact（D18）打包，平台不需要為它改任何東西。
- **一致性由 conformance 測試保證。** 平台提供一套黑箱測試：假的 shim（走 fd 3）加上假的 NATS 對端，逐條驗 §9 的契約。Go SDK 也跑同一套。SDK 通過之後，manifest 的 `contracts` 才能宣稱支援。
- **平台另有一道不依賴 SDK 的保險：** 不是每種語言都能保證 heartbeat 不被 hook 卡住（例如 Python 的 hook 卡在不放 GIL 的 C 擴充裡）。STS agent 以 `Beat` 判斷：超過 `hook_timeout_s` 再加 10 秒沒有 `Beat`，就經 shim 送 SIGKILL，outcome 記為 `HUNG`（PROTOCOL §9.7）。

### D20 歷史補正是 TD agent 的 job

account worker 依 refcount 起停，沒有常駐層（D9），所以閒置帳號的歷史由 TD agent 自己補，不為此拉起 worker。

- **Postgres 的 cursor 是成交進度的權威。** live 路徑寫進去的都是暫定（provisional）；哪一段已經確認，只看 cursor。
- **TD agent 專有一個 scheduler**（不在 D2 的共用骨架裡）：
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
  - 走 Report 是因為它不會丟：shim 保留最新一份，worker 死了也留著，agent attach 時一定拿得到。`td.evt` 在輪換、Postgres 不在、私有連線斷線（D27）時都可能漏掉成交，若剛好是某個商品的第一批，就永遠進不了範圍。
  - 新商品的第一筆單：worker 先把 Report 寫給 shim（一次本機寫入，幾 µs），再送單，不留「單已送出、Report 還沒出去」的窗口。之後的單只查一次記憶體裡的 map，成本可忽略。每個商品每個 incarnation 只發生一次。
  - TD agent 收到後，經由同一個 queue 把 scope `(api_id, ticker, since)` 寫進 Postgres，`since` 取 `LEAST`（重啟、換 incarnation 都不會把起點往後推）。cursor 的建立完全是非同步的，不影響下單。
  - 起點是第一次下單的時間，一定早於任何成交，所以第一批成交即使 provisional 丟了，也讀得到。
  - 不涵蓋：人工在新商品上的交易。人工單不屬於平台（D25）；之後需要時，可以由 adapter 用交易所的帳戶層級介面發現商品。
  - 資金流水（出入金）之後以同樣的方式加入，另一條 cursor。
- **rate：** 只靠固定 interval 加 jitter。一輪內的請求量與 429 先不另外處理，實測有問題再加。
- **TD agent 因此多了：**
  - 為 REST 讀取解開帳號憑證。憑證本來就以 TD instance 的公鑰封裝，由 agent 解開後交給 worker。
  - venue 的 REST 讀取，每個 job 自己 recover panic。
  - Postgres 連線。Postgres 不在時 queue 會滿、backfill 停住，交易不受影響。
- agent 掛掉時 worker 照跑（D3–D5），只影響補正與落庫。

### D21 行情狀態的權威在 conn worker；md.fetch 是無狀態服務

行情狀態（book、tape）的權威在持有該 atom 的 conn worker 記憶體裡，跟 TD 的 ledger 在 account worker 裡是同一個模式：快照打底，fold 串流的增量，有變化就推出去，晚加入的人向持有者要目前的狀態。

- **book：**
  - 不管策略訂的是 `orderbook` 還是 `orderbook_delta`，conn worker 都維護一本本地全量 book，再依 feed 決定推什麼。兩個 topic 對到同一個 atom：交易所的增量 channel；沒有增量 channel 的交易所，用它最完整的 book channel。
  - 跟交易所同步的邏輯全在 conn worker 裡：快照與增量對齊、驗序號、斷號後重抓。快照用 adapter 的 REST reader 在進程內抓（Binance 類），或用訂閱時 WS 推來的那一張（OKX、Bybit 類）。不經過 md.fetch 服務：正確性不依賴另一個服務，而且對齊有時序要求，多一跳 NATS 只會讓快照變慢。
  - `orderbook`：本地 book 的 top-N 全量（`OrderBook`），top-N 有變才推。N 由 MD 的設定決定，同一個 venue 上固定。
  - `orderbook_delta`：正規化後的增量（`OrderBookDelta`：價位 → 數量，`"0"` 為刪除）。conn worker 對交易所重新同步之後推一則 `reset`，內容是完整的 book。增量被 prehook 丟掉（D24）時，seq 會跳號，SDK 的小工具就向 `md.view` 重拿。
  - 晚加入：SDK 先訂 `md.f`，再向持有者要 `md.view`，拿到 seq S 時的 book，之後只套用 seq > S 的增量。seq 斷號或 `conn_epoch` 改變時，重新要一次。這是 SDK 提供的可選小工具，fold 跟交易所無關。
- **tape：**
  - 只錄 `trade`、`aggtrade`、`liquidation`，book 類不錄。錄不錄由 topic 決定，MD agent 推導 `AtomAssignment.record`。
  - 每個 atom 一個 ring，存已經發佈的訊息 bytes（envelope ＋ `FeedEvent`，D24），回覆時不用重新編碼。上限是時間窗加筆數，worker 另有總上限，算進 `Limits.memory_mb`。
  - WS 重連時在 ring 裡記一個 gap。
  - 接 live：SDK 先訂 `md.f` 再讀 tape，丟掉 `(conn_epoch, seq)` 不小於第一筆 live 的部分。live 和 tape 出自同一個進程、同一套序號，接縫是精確的；沒有 id 的 liquidation 也適用。
  - 不用 Redis。tape 跟著 worker 進程和 atom 一起活：只有進程重新掛起（crash，或停掉後 spawn 新的 incarnation）、或 atom 的 linger 結束，tape 才會沒了。
  - worker 不隨版本遷移（D4、D8）：發新版只影響之後 spawn 的 worker，跑著的進程繼續用原本的 binary，tape 不受影響。只有刻意強制換版（D8：MD 對連線 restart）才會清空那條連線上的 tape。
  - tape 只是熱身用，回覆會說明涵蓋多少。要一直熱的，跑一個 tape keeper：不下單、不綁帳號的策略，跟 mftik 一樣，tape 跟著它的生命週期走（D29）。
- **md.view：** 持有 atom 的 conn worker 服務 `md.view.{inst}.{topic}.{ticker}`，形狀跟 `md.f` 一樣，對應 `td.view.{api_id}`。回覆依 topic：trade 類回 tape 分頁（由新到舊，以 `(conn_epoch, seq)` 當游標，每頁壓在 NATS 的 1 MiB 以下），`orderbook_delta` 回 seq S 時的 book。沒有 responder，就是沒有 worker 持有這個 atom。
- **md.fetch：**
  - 無狀態的服務，跟 SYM 一樣：每個 region 部署，NATS queue group，任何 replica 都能回答。不需要 agent、lease、fencing。不落庫。
  - 只轉發交易所的公開 REST：K 線，以及交易所不推送、只能查詢的值（D12，例如 Binance UM 的 open interest）。
  - K 線接 live 的 kline feed，以 `open_time` 對齊。
  - book 快照不走 md.fetch，走 `md.view`。best quote 這類交易所有推送的值，訂閱 feed。
- **共用的是 adapter 的 REST reader 程式碼，不是服務：** conn worker 在進程內呼叫它，md.fetch 服務拿它轉發。SDK 那一層可以都叫 fetch（`fetch_book`、`fetch_klines`），線上依回答者不同走兩個 subject。

### D22 一期的策略完全宣告式

- 策略要的行情全部寫在 session spec 裡，lease 項目只從 spec 推導。
- 沒有讓策略在執行中加減訂閱的介面。動態的需求（期權鏈重新置中、滾動合約換月）由 selector 在 MD 內部處理（D9）。
- 一次性的讀取（K 線、只能查詢的值）走 md.fetch（D21），不算訂閱。

### D23 策略狀態：session volume 與資料 artifact

spec 維持不可變，平台不解讀策略狀態，重啟仍是新進程從零開始。不同的是，新進程有兩個地方可以讀：

- **volume**：每個 session 一個可寫目錄，在 STS 主機上（`MFTIKO_VOLUME_DIR`）。跨 incarnation 保留，所以同一個 session 內 crash 後重拉，讀得到 crash 前最後寫入的內容。容量上限是 `Limits.volume_mb`。
- **資料 artifact**：不可變、以 digest 定址的檔案樹，存在 D18 的 OCI registry，只是 mediaType 不同（PROTOCOL §8.4）。新 session 的 spec 以 `inputs` 引用（名稱 → digest），STS 主機在 spawn 前拉下、驗 sha256，解到 `MFTIKO_INPUT_DIR/<名稱>/`。上一個 session 的狀態，以及運維上傳的模型、參數表，都走這條路。
- **為什麼要兩個：** spec 不可變，注入的 inputs 在整個 session 期間固定。session 內重啟如果只讀 inputs，會回到 session 開始時的狀態；最新的狀態只會在 volume 裡。
- **spec 只放引用，不放內容。** etcd 的值預設上限是 1.5 MiB，狀態檔很容易超過。
- **session 進入 terminal 後，STS agent 把 volume 打包成資料 artifact 推到 registry。**
  - digest 記在 `SessionStatus.state_out`，下一個 session 拿它注入。
  - 標上 `clean`，取最後一個 incarnation 的 `ExitInfo.on_stop_completed`，讓接手的人知道這份是不是正常結束時寫的。
  - 推成功之後才刪 volume；registry 不在就保留 volume 並重試。volume 是空的就不推。
- **讀取順序由 SDK 的小工具處理，不是平台語意：** volume 有就用 volume，否則用 inputs，兩者都沒有就從零開始。
- **寫入：** SDK 提供原子寫入（暫存檔加 rename），OnStop 逾時被殺也不會留下寫到一半的檔案。
- **只在 OnStop 寫，crash 時就沒有狀態可用。** crash、HUNG、被 kill 都不跑 OnStop。要讓 crash 後也能 rebuild，策略得在執行中定期寫；寫不寫、多久寫一次，由策略自己決定。
- **讀檔放 `OnInit`**（D15 的分工）。狀態檔是策略自己的認知，帳上的部位與掛單以 TD 為準，對帳在第一次 `OnRecon`（D27）。
- **volume 在主機本地。** session 綁在一個 STS instance 上，主機掛了 volume 就沒了，只能退回 inputs 那份較舊的狀態。一期接受。
- **容量：** STS agent 定期量 volume 的大小，超過 `Limits.volume_mb` 就以 `FAILED` 結束 session（原因 `volume_over_limit`），因為磁碟滿會拖累同一台主機上所有 session。
- 名稱上區分「策略 artifact」（D18，程式）與「資料 artifact」（本條，狀態與輸入）。

### D24 session worker 的執行模型、背壓與重啟

**執行模型**

- **單一 dispatcher（Actor）。** hook 一次一個、依序執行，策略程式碼不需要鎖。下單這類呼叫不阻塞，結果以事件回來。
- 跨 thread 只有一個入口 `Post`：策略自己開的 thread 或 goroutine 用它把結果放回 TD buffer。其他 SDK 呼叫只能在 hook 裡做。
- NATS 收訊、心跳、watchdog 跑在 Go 核心的 goroutine 上（D19），hook 卡住不影響它們。mftik 的雙 thread、offload、breathe 都不需要。

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
- 丟掉之後，策略用 `(conn_epoch, seq)` 判斷連續性自己處理。SDK 提供可選的小工具：`orderbook_delta` 跳號就向 `md.view` 重拿快照（D21）；已收盤的 kline 被丟了就用 `md.fetch` 補。
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
- **平台不清場（D25）。** 新的 incarnation 從零開始；上一個進程留下的掛單，由策略自己以 algo 範圍的 `batch_cancel` 處理，或接手。第一次 `OnRecon` 會帶上自己 algo 的掛單與帳號的部位，對帳在那裡做（D27）。
- 重啟期間 MD、TD 的 lease 項目不刪（D9），account worker 與行情不受影響。

### D25 策略的單由策略自己管：algo_id

- **平台不替策略撤單。** crash、`FAILED`、STS agent 不在時留下的掛單，平台都不自動處理：要嘛由宣告同一個 algo_id 的 session 下一次啟動時自己清，要嘛人工撤。帳號層級的 cancel-on-disconnect（`AccountSettings`）是另一回事，照帳號的設定。
- **algo_id 是訂單的命名空間，由設定宣告，不由 API 分配。**
  - 一個 session 可以宣告多個，例如同一個帳號上 BTCUSDT、ETHUSDT、BNBUSDT 各一個。宣告在 spec 的帳號綁定上（`AccountDecl.algo_ids`），策略下單時自己選用哪一個。
  - 1–1023（10 bits），0 保留，讓人工單或格式不對的 id 不會被當成某個 algo。只需要在同一個帳號內唯一：`client_order_id` 與 `batch_cancel` 都以帳號為範圍。
  - 跨 session、跨 incarnation 不變：新 session 宣告同樣的 algo_id，就能接手上一個 session 留下的單。
- **同一個帳號上，一個 algo_id 同時只屬於一個非 terminal 的 session**，否則兩邊會撤到對方的單。由 TD agent 檢查：TD 的 lease 項目帶 algo_id 清單，和同帳號其他 owner 重疊就回 `REJECTED`（`algo_conflict`）。TD agent 看得到這個帳號所有的 lease 項目，是唯一不會有競態的地方；API 在不同 region 的 cluster 並行建立 session 時擋不住。
- **編進 `client_order_id`：** uint64 `ver(4) | algo(10) | ts_ms(40) | seq(10)`，以十進位字串傳。沿用 mftik 的 uint64 形式，欄位重新分配：
  - `ts_ms`：自 2026-01-01 UTC 起算的毫秒，40 bits 可用約 34 年（到 2060 年）。
  - `seq`：每個進程每毫秒 1024 個。超過時 `ts_ms` 往前借一毫秒，實際上不會發生。
  - `ver` 只用 1–7，最高位維持 0，用 int64 解析也不會壞。十進位最多 19 位，各家交易所的長度限制都放得下。
  - 新 incarnation 的 seq 從 0 起算也不會撞號：重啟至少退避 1 秒，`ts_ms` 不會和上一個進程重疊。
  - `Submit` 本身仍帶 `session_id`。
- **從 `(api_id, algo, ts_ms)` 對到 session。** 同一個帳號上一個 algo 同時只有一個 session，所以靠持有紀錄就能對：TD agent 在 ack READY 時寫入取得時間、lease 項目消失時寫入釋放時間，存成 Postgres 的 `(api_id, algo_id, session_id, acquired_at, released_at)`。lease 項目在 session 結束時就從 etcd 刪了，不能當歷史用。
  - 交接時，舊 session 的進程結束、lease 被刪、TD agent ack 新 session、新 session spawn 到 Start，這段實際間隔至少幾百 ms，遠大於主機之間 ms 級的時鐘誤差，所以不需要冷卻期。
  - 落庫的歸屬（D20）就用這條規則。
- **只能動自己的單。** SDK 只允許使用 spec 裡宣告給該帳號的 algo_id。TD 檢查 `Submit`、`Cancel`、`BatchCancel` 帶的 algo_id 與 `client_order_id` 的 algo 欄位一致，不符就以 1xx 拒絕。沒有 `client_order_id` 的單（人工下的）不屬於任何策略。這是防呆，不是防惡意。
- **SDK 只提供能力，怎麼用是策略的事：**
  - `ctx.batch_cancel(api_id, algo_id, …)`：TD 撤掉這個帳號上 algo 欄位等於 algo_id 的所有掛單，可以再限定商品，不分 session、incarnation。TD 在 account worker 裡轉譯成交易所的撤單。
  - 以 algo 查自己的掛單（`td.view` 的 OMS 查詢）。
  - 策略重啟或新 session 接手時，在 `OnStart` 用它們清場或接手。
- STS agent 不再需要 `td.order` 的權限。session 進入 terminal 時，TD lease 項目直接刪除。

### D26 MD 連線的身分、endpoint 與放置

- **venue 以「一條連線、一組商品」為單位**，照 mftik 的 venue registry（D11）。host 或產品線不同就是不同 venue，例如 `Binance`（現貨）、`BinanceUM`、`BinanceCM`、`BinanceEO`。憑證屬於帳號種類，不屬於 venue；`BinancePM` 是帳號種類，不是 venue（D32）。
- **endpoint 是連線的可選參數，不是身分。** 同一家交易所提供的替代 host（例如針對某個雲端優化的 endpoint）視為同一個 venue，由 MD instance 的設定選擇，進 conn worker 的 spawn spec。換 endpoint 要換 incarnation（D8）。
- **route：同一個 venue 裡，商品和 topic 決定要連哪條 WS 路徑。** 例如 OKX 的 K 線在 `/ws/v5/business`，其他公開行情在 `/ws/v5/public`；Bybit 的公開行情依類別分成 `/v5/public/spot`、`linear`、`inverse`、`option`。不同 route 不能共用連線。OKX、Bybit 是一份憑證的統一帳戶，拆成不同 venue 會破壞 UniversalTicker 與 TD 的帳號，所以由 adapter 從解析後的商品推出 route（D31）。大多數 venue 只有一個 route。
- **連線的身分是 `(venue, route, n)`。** 之後加版號（D8）就是 `(version, venue, route, n)`。同一組 atom 一律共用連線，兩個 session 要同一個 atom 就是 refcount=2，不會多開連線。
- **只有連線達到交易所的上限才開 n+1。** 上限由 adapter 宣告，例如 Binance USDⓈ-M 一條連線 1024 個 stream。新 atom 放進同一個 `(venue, route)` 下還有空間、n 最小的那條。不整併：1024 連期權鏈也用不完，實際上幾乎只會有 n=0。

### D27 行情過舊與 TD 降級

- **行情過舊是策略的風險，平台不兜底。** timer 驅動的策略、用多個 feed 算價差的策略，下單前自己驗事件裡的 `md_ts_ns`、`exch_ts_ns`。系統不在下單時擋：SDK 不知道一筆單依賴哪個 feed。掛著的單要不要撤，也是策略的事（D25）。
- **TD 私有 websocket 斷線（`DEGRADED`）時：**
  - 新單以 1xx 拒絕（`account_degraded`）。收不到回報時送出去的單，策略只會拿到 accepted、看不到後續，很容易重複送單。這也就是 D16 的「TD 壞了 → 下單失敗，拿到原因」。
  - `Cancel`、`BatchCancel` 照常走 HTTP：降低風險的動作不擋。
  - account worker 自己重連。重連後 recon，在 `td.evt` 發一則 `Recon`，之後才恢復接受新單。
  - TD 的 ack 維持 `READY`：worker 還活著，只是暫時降級，改成 `DEAD` 會觸發 STS 依 session 規則的處理，太重。
- **`OnRecon(reconState)`：帳號狀態重置只有這一條路徑。**
  - 內容：自己 algo 的 open orders，帳號的 positions、balances。
  - 觸發：私有連線重連並 recon 之後；account worker 換 incarnation 之後（取代原本的 `AccountReset`）；`OnStart` 之後 SDK 以 `td.view` 取得的第一份。D23 說的對帳、D25 說的接手或清場，都在這裡做。
  - 和其他 TD 事件共用 seq，順序由 TD buffer 的 FIFO 保證。策略在 `OnRecon` 裡整份換掉自己的狀態，之後的 `Order`、`Fill` 照常疊上去。
  - 快照是整個帳號的。SDK 把 `open_orders` 篩成自己宣告的 algo_id；`positions`、`balances` 是帳號層級、所有 algo 共用的，原樣給。
  - 斷線期間的個別成交不會補發給策略，策略拿到的是重置後的狀態。DB 的完整紀錄由 D20 的 backfill 補。
  - 這不是 D16 說的可用性 hook：策略不會被告知「TD 斷了」，只會拿到拒單，之後拿到一份新的狀態。

### D28 lease 的 TTL 與 linger

- **linger 只有一層，MD、TD 同一個概念：** owner 集合歸零後，等 linger 才釋放資源。
  - MD：atom 的 owner 歸零 → linger → 退訂。連線在最後一個 atom 退訂時就停 worker，不另設連線的 linger。
  - TD：帳號的 owner 歸零 → linger → 停 account worker。最後一個 lease 的 linger 就是帳號的 linger。
- **linger 只需要吸收一件事：STS agent 消失超過 TTL。** 其他情況都不靠 linger：
  - 期權鏈重新置中：由 orchestrator 先訂新的、再退訂舊的，置中時一直聽 perp 的行情，不會反覆進出。
  - 換參數重開 session：中間的空白無法控制，照規則走，該退訂就退訂。
  - tape 保溫：跑一個 tape keeper 策略（D29），tape 跟著它的生命週期走。
- **預設值：**
  - TTL：所有 lease 統一 60 秒，region 內、跨 region 都一樣。keepalive 間隔仍是 TTL 的三分之一。
    - TTL 保護的是 STS agent 不在時還在跑的策略：shim 讓策略在 agent 消失後繼續跑，這段期間 MD、TD 的資源必須留著。太短的話，agent 掛十幾秒，所有在跑的策略就同時失去行情與帳號。
    - 預期 agent 在 5 秒內完成輪換，60 秒留了足夠的餘裕，也比 WAN 的抖動長（D17）。
    - 代價：agent 真的掛掉、不再回來時，資源最多多留 60 秒才釋放。
  - linger：5 秒。輪換真正能容忍的時間是 TTL ＋ linger。
- 這些值是 MD、TD、STS instance 的部署設定，不放進 session spec。

### D29 沒有常駐訂閱，行情只由策略持有

- MD 的 owner 只有 session 的 lease 項目。拿掉常駐訂閱（`StandingSubscription`）這個 API，少一個特殊的入口。
- 要讓行情或 tape 一直保持熱的，就跑一個 **tape keeper**：不下單、不綁帳號的策略，跟 mftik 一樣。它和其他策略一樣經 API 開始、結束。
- 使用者的認知只有一條：行情由策略持有，要全部關掉行情，就是把策略全部關掉。
- 代價：tape keeper 的 lease 由 STS agent 發，STS agent 消失超過 TTL ＋ linger（D28）時，它持有的 atom 會被釋放，tape 跟著斷。這和其他策略承受的是同一件事。

### D30 session worker 的 NATS 憑證：每個 session 一份

- **簽發：** STS agent 持有 NATS account 的簽章金鑰，spawn 時為每個 session 簽一份 user JWT（NKeys）。和其他憑證一樣經 `Init` 的 socketpair 送進去，不放環境變數、不落檔。簽發是本機的密碼學運算，不需要連 server。
- **權限從 spec 推：**

  | | 允許 |
  |---|---|
  | publish | `td.order.{綁定的 api_id}`、`td.view.{綁定的 api_id}`、`md.view.{綁定的 MD instance}.>`、`md.fetch`、`log.sts.{session_id}` |
  | subscribe | `md.f.{綁定的 MD instance}.>`、`td.evt.{綁定的 api_id}`、`_INBOX_{session_id}.>` |

  - 行情的訂閱權限用萬用字元，只分到 instance：selector 的成員會變，沒辦法逐一列出；行情是公開資料，放寬讀取的風險很低。
  - reply inbox 每個 session 一個獨立的前綴 `_INBOX_{session_id}`。預設的 `_INBOX.>` 會讓任何連線都能讀到別人的回覆，包括 `td.view` 查回來的帳務資料。
- **分層：** 跨帳號由 broker 保證，策略只能對自己綁定的帳號發 `td.order`；同一個帳號內的 algo 由 TD 檢查（D25，防呆）。
- **不設過期、不做撤銷：** 設過期的話，到期時 server 會斷開連線，重連瞬間 core NATS 的 `td.evt` 可能漏。憑證只在進程記憶體裡，session 結束、進程一死就沒了。
- **不影響下單延遲：** 授權只在建立連線時做一次（`CONNECT` 帶 JWT 與對 nonce 的簽章）。之後每次發佈，server 只拿 subject 比對這條連線的權限快取，訊息本身不帶任何 token。
- MD、TD 的 worker 是平台程式，用所屬 instance 的憑證。

### D31 UniversalTicker 是佔位，解析之後才知道要訂閱什麼

照 mftik 的 symbol plane（D11）。

- **平台裡指涉商品一律用 UniversalTicker：** spec、lease 項目、`md.f` 的 subject、`td.order`、DB。交易所的寫法（`exch_ticker`）只出現在 adapter 和 SYM 的表裡。
- **翻譯是查表，經 SYM 解析，不是字串轉換。** `BTCUSDT` 不帶 base／quote 的切法，交易所的寫法也不一定是 base＋分隔＋quote。
- **先解析，再交給 adapter。** MD agent 收到 lease 項目，先向 SYM 取得商品資料，找不到或已下架就是 `REJECTED`；再把解析結果交給 adapter，由它決定 route 和 channel。
  - adapter 因此是純函數：不查 SYM、不做 I/O，只用資料就能測。
  - mftik 的 adapter 自己帶 SymbolResolver，在裡面查；mftik refactor 的 B7-01 改成先解析再 `atoms_for`。這裡照後者。
- **回程帶交易所那一側的 key。** 私有事件、REST 拿回的成交與訂單帶 `(venue, category, exch_ticker)`（venue 見 D32），account worker 和 backfill 以 SYM 反查換回 UniversalTicker。category 是 key 的一部分：統一帳戶的 venue 上，同一個寫法可能是兩個商品（Bybit 現貨與 linear 的 `BTCUSDT`）。
- **行情的熱路徑不查表。** conn worker 依 `AtomAssignment` 的 feed 在事件上蓋 UniversalTicker，解析只在 atom 建立時做一次。account worker 每個 ticker 第一次下單時解析一次，之後查記憶體。
- **atom 的身分是解析後的 `(venue, route, channel)`，id 為 `<venue>:<route>:<channel>`。** channel 仍逐字照抄交易所參數；route 從解析後的商品推得（Bybit 依 category），所以同一個 channel 字串在不同 route 上是不同的 atom。
  - 不拿 `topic.UniversalTicker` 當 atom 的身分。一個 channel 常同時服務多個 topic：`orderbook` 和 `orderbook_delta`；Bybit linear 的 `tickers.*` 帶 ticker、funding、OI、mark price。以 (topic, ticker) 當身分，同一條連線上會重複訂閱同一個 channel，退訂一個就把其他的斷掉。這是 mftik `MdVenueSubscriptions` 處理過的問題：產品的 refcount 和 wire 上的訂閱是兩本帳。
  - 佔位留在 MD 外面：`md.f` 的 subject 是 feed（`topic.UniversalTicker`），atom 不出現在資料面（PROTOCOL §7）。
- **topic 名稱沿用 mftik：** `bestquote`、`trade`、`aggtrade`、`orderbook`、`ticker`、`liquidation`、`funding_rate`、`open_interest`、`greeks`、`kline_<interval>`，加上這裡自己的 `orderbook_delta`、`mark_price`。表在 PROTOCOL §2。
- **SYM 照 mftik：**
  - 刷新以 `(venue, category)` 為單位，各自刷新、各自下架：現貨的刷新不會讓 perp 下架。
  - 列永遠不刪，訂單與 session 會一直指向下架的商品。
  - 單一商品以 ticker 精確查詢，不載入整個 venue 的表；整表分頁讀，每頁最多 500 筆。
  - 用戶端快取，TTL 10 分鐘。
  - 契約見 PROTOCOL §7.3，表見 `migrations/0002_symbol`。
- venue 名稱照 mftik 用 CamelCase：OKX 的 token 是 `Okx`。

### D32 帳號種類和 venue 是兩個 enum

- **venue 是市場，帳號種類是 api_id 是什麼。** 兩個獨立的 enum，名稱可能相同，不可互查。
  - venue：UniversalTicker 的第一段，一條連線的公開行情、一組商品（D26）。`Binance`、`BinanceUM`、`BinanceCM`、`BinanceEO`、`Okx`、`Bybit`。
  - 帳號種類：收哪種憑證、可以交易哪些 venue 的商品，由各交易所的套件在程式裡宣告。`Binance`、`BinanceUM`、`BinanceCM`、`BinanceEO`、`BinancePM`、`Okx`、`Bybit`。
  - 大多數帳號種類只交易同名的那個 venue。
- **BinancePM 是帳號種類，不是 venue。** 它可以交易 `Binance_Spot_BTCUSDT`、`BinanceUM_Perp_BTCUSDT`、`BinanceCM_*`；請求依帳號種類走 papi，再依 ticker 的 venue 選 margin、um、cm 的端點。沒有 `BinancePM_Spot_BTCUSDT` 這種 ticker：保證金模式改的是帳號怎麼算錢、單送到哪，不是商品本身，同一本訂單簿只有一個身分。
  - 帳號宣告（`state.v1.Account`）記的是帳號種類（`kind`），不是 venue。
  - account worker 在 `Submit` 前檢查 ticker 的 venue 在帳號種類的清單裡，不在就以 1xx 拒絕。
  - 交易所回來的事件帶 `(venue, category, exch_ticker)`：一條 PM 的私有連線同時有 margin、UM、CM 的事件，所以 D31 的反查 key 加上 venue。
  - D20 的 backfill 範圍 `(api_id, ticker)`、D25 的 algo_id 都不假設帳號只有一個 venue，不用改。
- **BinanceCM、BinanceEO 維持獨立的 venue。** 2026-06 的 CM-UM 整合之後，CM 仍然是 `dapi`、`dstream`，錢包也沒合併；EO 的行情雖然在 `fstream` 上，REST 與帳戶都是自己的。D26 的規則不變。
  - CM 的 `dstream` 沒有路徑分流，只有 `default` 一個 route。
- **UM 和 CM 不共用連線。** 整合之後部分 stream 可以跨 host 訂，但連線的身分仍是 `(venue, route, n)`。全市場陣列在兩個 host 上都會送 UM＋CM 合併的內容，以 `st` 區分，adapter 解碼時丟掉另一個 venue 的列。
- **UM、CM 之間的耦合不寫進 adapter 契約。** 整合之後兩者共用 IP 的 weight 與下單數上限、共用持倉模式（`dualSidePosition`），CM 的 STP 跟著 UM。只記在 Binance adapter 的實作說明。

## 待討論

目前沒有。協議層還沒定的見 PROTOCOL §10。

已解決：舊的 Q2（綁定變化經 STS agent 轉送，因為 ack 在 etcd、由 STS agent watch）、Q4（durable store 是 etcd，訂單歷史另放 Postgres）、Q12（每個 region 一個 etcd，見 D17）、Q1（每個策略一個 artifact，見 D18、D19）、Q13（生命週期兩段，見 D15）、Q11（TD 也依 refcount 起停，見 D9）、Q16（歷史補正是 TD agent 的 job，見 D20）、Q9（tape 與 book 的權威在 conn worker，以 `md.view` 讀取；md.fetch 是無狀態服務，見 D21）、Q3（一期沒有執行中加減訂閱的介面，見 D22）、Q6（spec 不可變維持；策略狀態經 volume 與資料 artifact 延續，見 D23）、Q5（單一 dispatcher、TD／MD 兩個 buffer、prehook 依 `recv_window_ms` 丟舊行情，見 D24）、Q8（平台不清場；策略以 algo_id 範圍的 `batch_cancel` 自己清，否則人工撤，見 D25）、Q7（連線以 `(venue, route, n)` 共用，滿了才開下一條，不整併，見 D26）、Q10（線上編碼一律 protobuf，`buf breaking` 機械檢查相容性；`md.f` 在 protobuf 前面加固定長度的二進位 envelope，見 D24、PROTOCOL §1、§7.1）、Q14（行情過舊由策略自己驗時間戳；TD 降級時拒新單、放行撤單，重連後以 `OnRecon` 重置狀態，見 D27）、Q15（只有一層 linger；TTL 統一 60 秒、linger 5 秒，見 D28）、Q17（backfill 的範圍從下單決定：worker 以 Report 回報每個商品第一次下單的時間，agent 建 cursor，見 D20）、Q18（atom 的身分加上 route，商品先經 SYM 解析，見 D31）、Q19（topic 名稱沿用 mftik，見 D31、PROTOCOL §2）、Q20（BinanceCM、BinanceEO 維持獨立 venue、不共用連線；BinancePM 是帳號種類，見 D32）。
