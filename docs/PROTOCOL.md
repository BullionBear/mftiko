> **狀態：草案 v1.59（2026-10-24）。** 依據 [DECISIONS.md](DECISIONS.md)。v1.59 依 E11-D024–E11-D028（E11-06，#358）：§0 寫 `mftiko session` 的 list、show、end、delete，以及 `mftiko record` 的 list、show。`session list` 篩選是 `--phase`（可重複）、`--active`、`--sts-instance`，分頁沿用 E11-D010–E11-D012（`SessionList` 帶 freshness，自動翻頁的 `-o json` 合併成 `{"items","freshness"}`，與 `account list` 相同）。`session show` 印 spec 摘要（策略 `id`（回應有才印）、名稱、digest 前 7 碼（不含 `sha256:` 前綴）、STS instance、本地 `created_at`、`created_by`）、狀態（`phase`、`reason`、重啟次數）、需求項目（ack 狀態，`REJECTED`／`DEAD` 標出並附原因）、dependency Condition、結束資訊（`outcome`、exit code、signal），最後印 log。log 位置是 `<sts_instance> <host>:<完整路徑>`；`AgentRecord.last_known` 為真時註明「最後已知，agent 目前不在線」。一期不讀 log 內容（E10-D015）。`session end` 用 E11-D009 確認，已結束仍成功。`session delete` 用同一確認；非 terminal 回 `not_terminal` 時 exit 14，並提示先執行 `session end`。`record list` 的時間接受 RFC 3339 或 `YYYY-MM-DD`（本地時區午夜），不收相對時間，分頁照 E11-D010。E11-D013 修訂：短 digest 統一為前 7 碼（不含 `sha256:` 前綴），`strategy list` 由 12 碼改為 7 碼。與 #357 的 E11-D019–D023／v1.58 並行，後合併者 merge main，兩邊的決策編號與版本都留下。v1.57 依 E11-D014–E11-D018（E11-04，#356）：§0 寫 `mftiko account` 的 list、show、create、update、delete。憑證只從 `-key-file`、`-secret-file`、`-passphrase-file` 讀（`-` 是 stdin，最多一個；TTY 上沒給的必要檔案改為不回顯輸入，ed25519 的 secret 讀到 EOF），ed25519 的 secret 原樣送出，HMAC 的值只去掉結尾一個換行。`key_types` 一種就用那一種，多種要 `-key-type`。送出前讀 `/account-kinds`，`supported` 為 false 則 client 不送出，exit 10，訊息與 API 的 400 相同。必要欄位依 `key_types` 與 `passphrase`。`account create` 的 `-td-instance` 必填，名稱先對文法再讀 `/instances/{name}`，不是 td、不在 topology 或名稱不合法就不送出。`account update` 先讀 `revision` 再 CAS，給了任一憑證檔就換整組，一組都沒給就不送 `credentials`。409 `conflict` 不重試，exit 13，並提示重新執行。`account delete` 帶讀到的 `revision`，確認用 E11-D009，409 `referenced` 列出 `referenced_by`，exit 15。輸出與 `-v` 都不含憑證。`account list` 沿用 E11-D010–E11-D012（與 #355 的 v1.56 並行）：預設以 `limit=1000` 翻完，`-limit` 限制總筆數，`-cursor` 只取該頁且 `-o json` 原樣輸出；自動翻頁的 `-o json` 合併成單一 `{"items","freshness"}`，`unavailable_regions` 取各頁聯集，不帶 `next_cursor`；有 `unavailable_regions` 或 `controller_absent` 時 stderr 只警告一次，exit code 不變。v1.56 依 E11-D010–E11-D013（E11-03，#355）：§0 的 mftiko 加上策略、instance 與帳號種類的唯讀查詢。strategy list 與 strategy versions 預設自動翻頁，每頁 limit=1000；--limit N 限制總筆數；--cursor 只取一頁。自動翻頁的 -o json 合併成 {"items":[...]}，unavailable_regions 取各頁聯集，不帶 next_cursor；--cursor 照 E11-D008 原樣輸出。有 unavailable_regions 或 controller_absent 時 stderr 警告一次並列出缺少的 region，exit code 不變。單一資源 503 是 region_unavailable（exit 17）。registry_unavailable（exit 19）的訊息明寫為 registry 連線問題。strategy list 顯示 latest 的 tag 與 digest 前 12 碼；strategy versions 在 digest 等於 latest 的列標 latest；strategy show 印完整 digest。§1 這些仍是契約 A 的客戶端慣例，不改 OpenAPI。v1.55 依 E11-D001–E11-D009（E11-02，#354）：§0 寫 `cmd/mftiko` 只對 API 說話（`mftiko-pack`、`mftiko-seal` 維持獨立）、標準庫 `flag` 的分派與 `help`、設定檔與 context、TLS、`login`／`whoami`、exit code（`invalid_artifact` 為 21）、表格與 `-o json`、確認函式；§1 寫這些是契約 A 的客戶端慣例，不改 OpenAPI。v1.54：決策編號改成 `Eee-Dsss`（DECISIONS 的編號說明），只換號碼，內容不變。v1.53 依 E10-D116（E10-20，#325）：§1 契約 A（`contract/openapi/api/v1.yaml`）定案，`info.version` 為 `1.0.0`，CI 的 `oasdiff breaking` 遇到不相容的改動就失敗；§10 移出已定的 P8（§3.2 W10、E10-D006）。v1.52 依 E10-D110（E10-19，#324）：§0 的認證，`Content-Type: application/json` 只對 cookie 認證的寫入（沒有 body 的也算）與登入要求，Bearer 不檢查。v1.51 依 E10-D104–E10-D109（E10-18，#323）：§0 寫 session 詳細頁的 phase、incarnation、`end_reason`、需求項目的狀態與 `REJECTED` 原因、log 的主機與完整路徑（附複製；`AgentRecord` 不在而用最後已知值時加註）、`state_out` 只顯示 digest 並可複製且不下載、結束按鈕先確認且 `ended` 或 terminal 時不顯示並呼叫 `POST /sessions/{id}/end`；agents 頁依平面分組、最後已知的 `AgentRecord` 加標記、`controller_absent` 與 `unavailable_regions` 用頁頂橫幅；market 頁唯讀列出 MD instance 與各 feed 狀態，不加搜尋或篩選；每頁一條 EventSource，只訂該頁需要的 topic，收到 `reset` 重新 list。v1.50 依 E10-D092–E10-D097（E10-13，#318）：`AgentRecord` 加 `host`、`work_dir`，當選時寫入；STS 寫 terminal `SessionStatus` 時，依當下 `SessionDemand.bindings` 的 `REJECTED`、`DEAD` 各寫一個 `dependency` Condition，需求已不在則不寫；每條 SSE 連線對每個 region 從 `Last-Event-ID` 的 revision+1 開 etcd watch，compacted 送 `reset`，不經 mirror；送出緩衝滿了就斷線；副本在記憶體保留最後的 `AgentRecord` 並標明最後已知值，重啟後沒有就只標示不在；`/session-records` 依 `created_at` 由新到舊。§3.2 W4、W7，§3.3 N3。v1.49 依 E10-D098–E10-D103（E10-16，#321）：§0 寫帳號頁的種類選單（`supported` 為 false 的灰色不可選並附原因）、TD instance 自由輸入且不讀 `/instances`、更新時憑證區預設收起（勾「更換憑證」才展開，不勾沿用原密文）、ed25519 的 secret 用保留換行的多行文字框並可選讀入 `.pem`（關閉自動填入與拼字檢查，送出後立即清除、不存、不回顯，bytes 為原樣 PEM）、HMAC 的 key、secret、passphrase 維持 password、刪除先確認（409 `referenced` 列出 `referenced_by` 的 session，409 `conflict` 重新載入後再確認）、列表顯示種類、TD instance、`referenced_by`（session 可點）。與 #318 的 v1.48 平行，後合併者 merge main 並維持版本單調。v1.47 依 E10-D086–E10-D091（E10-17，#322）：§0 寫策略頁的版本與 manifest，以及建立 session 的精靈（策略與版本、params 原樣送出、feeds 自由輸入、帳號下拉與本 session 宣告的 algo_id（1–1023，換帳號清空）、name 與 sts_instance、重啟與逾時與限制、每次打開產生的 Idempotency-Key、JSON Pointer 對到欄位、session 列表與建立成功後的詳細頁）。與 #317 的 v1.46 平行，後合併者 merge main 並維持版本單調。v1.46 依 E10-D080–E10-D085（E10-12，#317）：terminal session 過保留期由既有 reconcile 刪 spec 與 status，先關閉 `session_records` 再 `deleteIf`；Postgres 不可用則該輪不刪，`DELETE /sessions/{id}` 回 503 `storage_unavailable`；etcd 有 session 而沒有列則不刪、只告警；只有 status 時以該 key 的 `mod_revision` 刪；立即刪除非 terminal 回 409 `not_terminal`，成功與重複刪除回 204，稽核沿用中介層的 DELETE。§0、§3.2 W5、W10。v1.45 依 E10-D068–E10-D073（E10-11，#316）：建立 session 可選帶 Idempotency-Key，同 key 同 body 回同一個 session，同 key 不同 body 回 409，保留 24 小時；pending 逾 2 分鐘且 etcd 沒有對應 spec 標 abandoned，之後 create_revision 與雜湊對上則改回同一列 created；每個副本每 30 秒以及 mirror 變動時 reconcile，有條件 upsert、不選主；雜湊不同只記 error log 與計數器，只同步 desired_state 與 terminal 結局；etcd 有 spec 而 Postgres 沒有列時只告警不補列（E10-D072 修改 E10-D031）。§0、§3.2 W10。v1.44 依 E10-D062–E10-D067（E10-09，#314）：§3.2 W10 寫建立帳號必帶憑證、更新省略則保留原密文、憑證格式檢查、`td_instance` 與公鑰、`name` 依副本不重複，讀取不回傳明文、密文或指紋；§3.6 改由 API 以 `internal/platform/sealbox` 封裝，`cmd/mftiko-seal` 仍用於重封。v1.43 依 E10-D074–E10-D079（E10-15，#320）：§0 寫 console 用 openapi-fetch client；樣式是 Svelte scoped CSS 與全域 CSS 變數；即時更新用瀏覽器原生 EventSource（cookie、Last-Event-ID、reset 重新 list）；401 導向 /login 後回原頁，token 不進 localStorage；狀態是 Svelte 5 runes 的全域 store；vite dev 把 /api proxy 到 localhost:8080；CI 跑 svelte-check 與 build。#314（v1.41）與 #316（v1.42）平行，後合併者 merge main 並維持版本單調。v1.40 依 E10-D048–E10-D055（E10-07，#312）：§0 寫 API 啟動時建立 Postgres 連線池但不等連上（修改 E10-D024），readiness 仍只看 etcd；稽核列進有上限的佇列由背景寫入，SIGTERM 在 E10-D027 的收尾時限內寫完；cookie 一律 Secure；Bearer 優先；稽核 body 為 jsonb、上限 64 KiB；X-Request-Id 由伺服器產生；來源只記 RemoteAddr；登入不限速，token 至少 32 bytes 隨機值只以 SHA-256 入設定。v1.39 依 E10-D056–E10-D061（E10-10，#315）：策略目錄沿用 `internal/platform/artifact` 的 oras-go client 與 manifest 解析，只加唯讀的 list tags 與 tag 解析，既有函式不變；目錄不快取，列出版本時同時最多 8 個解析；一次取回完整 tag 清單、依 tag 名稱排序，`latest` 是普通項，API 以既有 cursor 分頁；連不上或 registry 5xx 回 503 `registry_unavailable`，manifest 不是策略 artifact 回 502 `invalid_artifact`，找不到回 404；registry 位址、Docker config.json 與 `plain_http` 放在 API 的設定，載入不連線；測試用 CI 的 `registry:2`。§0、§8.1、§8.3。v1.37 依 E10-D043–E10-D047（E10-04，#309）：§1 寫明契約 A 在 E10-20 之前 CI 照跑 `oasdiff breaking`、印出報告但不擋，base 沒有規格就略過。並依 E10-D032 把 `session.proto` 的 `FeedDecl.md_instance` 註解改成「API 驗證後填入」。v1.36 依 E10-D042（E10-08，#313）：§3.2 W10 寫明 API 的寫入走 `state.APIWriter`（put 只限 `spec/`、`account/`，`status/` 只能刪）；條件不成立回 409，etcd 不可用回 503 且寫入可能已生效；配號只查可用 region 的副本，要寫入的 region 不可用就回 503；任何 key 帶著 `session_id` 都算佔用；reconcile 只刪沒有 spec 的 status，以 spec 的 `create_revision` 對應 `session_records`。v1.35 依 E10-D040、E10-D041（E10-06，#311）：§3.1 寫明 `status/` 的寫入權是刻意的缺口（etcd RBAC 不分寫入與刪除，W5 要刪 status，所以 `api` 拿整個前綴的寫入權；W1「status 只有 STS 寫」在 status 上只靠 API 程式守），key 表與 `api` 角色對齊，測試在 `TestRBACEnforced`。v1.34 依 E10-D031–E10-D039（E10-03，#308）：契約 A 的規格在 `contract/openapi/api/v1.yaml`；§3.2 W10 的建立與刪除先寫 Postgres 的 `session_records`。v1.33 依 E10-D030（E10-14，#319）：§9 的 `-violate subject` 改把單送到 `td.view.{api_id}`。session 有權限送、但不是下單 subject，單不會出現在 `td.order`，檢查仍判不合格，不再依賴權限擋下。v1.32 依 E10-D023–E10-D029（E10-05，#310）：§0 寫 API 進程的三類設定、驗證但不連 Postgres 與 registry、每個 cluster 一個 Mirror 監看整個 `/mftiko/v1/`、任一 region 可用時 `/readyz` 回 200 並列出 `unavailable_regions`、全部不可用才 503、`/healthz` 只看進程、SIGTERM 後 readiness 先轉 503 再等進行中請求最多 10 秒（可設定）並以 0 結束、`-web` 從磁碟提供靜態檔且非 `/api/` 又找不到檔案回 `index.html`、有憑證則 HTTPS 與 HTTP/2 否則明文 HTTP 只給 dev、監聽預設 `:8080`；§3.2 W4 寫明這個前綴不拆。v1.31 依 E10-D019–E10-D022（E10-02，#307）：第二段清理只看 reconcile 手上的 spec 檢視，等第一次完整 list 才開始；刪除順序是 shim log、incarnation key、`terminal.json`、`sessions/<id>/`；log 檔與目錄建立後 Chmod 0640/0750，不受 umask 影響，目錄上已有的 setgid 保留；shim 每次啟動把 `${WORK_DIR}/log/{plane}` chmod 成 0750，已存在的舊 log 檔不改。v1.30 依 E09-D095、E09-D096（#327）：§3.3 的 `SessionStatus.phase` 加上「結束在重啟策略判斷之前寫 `STOPPING`」；§8.4 的 inputs 在 rename 之前就唯讀。v1.29 依 E10-D002–E10-D018（E10-01，#306）：§0、§1 加上 API 的契約 A（OpenAPI，JSON，是 protobuf 的例外）；§2 的 `session_id` 只保證存活與保留期內唯一，`api_id` 隨機配號，`algo_id` 的檢查改為與 E06-D016、E10-D012 一致；§3.2 的 W5 加上保留期，新增 W10（API server 的寫入），§10 的 P8 定案；§4.5 S4 的 log 檔改成 0640、目錄 0750，STS 的 log 在 spec 被刪之後才清；§7 刪掉 `log.{plane}.{id}` 與 session 的 `log.sts.{session_id}`；§8.4 的清理拆成兩段，registry 的資料 artifact 一期不刪。v1.28 依 E09-D094（#302）：C3 的 `shim.proto` 與 C4 的 `worker.proto`、`common.proto` 定案。v1.27 依 E09-D027、E09-D093（E9-12，#290）：`state.proto`、`session.proto`、`stsctl.proto` 與 §8 的 artifact manifest 定案，§8.2 加上 manifest 的相容規則。v1.26 依 E09-D091、E09-D092（E9-12，#290）：selector 的型別搬到各自 package 的 `selector.proto`，留在草案清單到 E5；`session.proto` 刪掉定案前的 `reserved`。`state.proto` 的 `AccountSettings` 註解與 §3.6 對齊。v1.25 依 E09-D085–E09-D090（E9-10，#288）：§8 在 `v*` tag 與 `workflow_dispatch` 打包 conn、account worker，推 `ghcr.io/bullionbear/` 的 `mftiko-md-conn`、`mftiko-td-account`，tag 是 git tag 與 `latest`；workflow 只推、不設可見度，第一個 tag 前由 org owner 把 Package creation 設成 Public；`-kind` 預設 strategy，`md.conn`／`td.account` 單層 `bin/worker`、contracts 固定、`sdk` 留空；一期只 `linux/amd64`，PR 對 `registry:2` 推了再依 digest 拉回。v1.24 依 E09-D078–E09-D084（E9-09，#287）：§8.4 在 worker 進程確定結束後才打包、另寫資料推送（暫存檔再串流）、打包內容與空 volume、terminal.json、state_out 欄位與 1–60 秒退避、清理順序。v1.23 依 E09-D072–E09-D077（E9-08，#286）：§3.3 rootfs 與 inputs 就緒且 volume 已建立才進計畫、volume 超量以一般 Stop 結束且不重啟；§5.4 inputs 檔案 0444、目錄 0555，volume 0700；§8.3 GC 保留正在拉或正在解的 input 層，以及 Pull 回傳後到 UnpackImage 結束前的策略層；§8.4 資料 artifact 的拉取、解開時機與 volume 測量。v1.22 依 E09-D066–E09-D071（E9-07，#285）：§3.3 實作重啟、退避中 `RESTARTING`、`restart_limit`、沒心跳的 `no_beat`、`shim_lost` 不重啟；§6 同一 epoch 的新 incarnation 把 bindings 的 seq 接下去加；§9.7 分階段的門檻，shim 照實記 signal，`SessionStatus` 寫 `HUNG`／`no_beat`。v1.21 依 E09-D058–E09-D065（E9-06，#284）：§3.3 拉取中維持 PENDING、永久 artifact 錯誤以 FAILED 結束、ENDED 的 terminal phase、worker 自己結束不重啟；§6 的 bindings 第一份在 Start、之後以 Command 推送；§8.3 背景拉取最多 4 個、GC keep 含非 terminal 與正在拉取的 digest，正式路徑不再有 `-worker`。v1.20 依 E09-D050–E09-D057（E9-03，#281）：§8.3 直接解壓寫檔並在 artifact 目錄記錄所用層，不再 hardlink；拒絕的 tar 項目、`os.Root`、解壓上限、`artifact_unsafe_layer`、同一進程只解一次、`GC(keep)` 不判斷最新版、inputs 共用解開函式。v1.19 依 E09-D028–E09-D034（E9-02，#280）：§8.1 的帳密檔是 Docker config.json，部署設定加 `plain_http`；§8.3 拒絕 OCI index、`contracts` 必須全含、下載前加總大小、錯誤分永久與暫時且套件不重試、暫存檔驗過才改名、快取只比大小。v1.18 依 E09-D042–E09-D049（E9-05，#283）：§3.3 寫明沒有 spec 不寫 status、terminal 不重起、phase 對到骨架狀態、`RUNNING` 只看 `Stage(running)`、shim 消失是 `shim_lost`。v1.17 依 E09-D035（E9-04，#282，取代 E09-D012）：§8 的 `contracts` 改由 sdk 嵌入 binary，`mftiko-pack` 讀出填入，不比對 sdk 版本；讀不到就拒絕打包。v1.16 依 E09-D009–E09-D027（E9-01，#279）：§3.3 加 STS agent 的 phase 與 algo 重疊；§8 加 registry 設定、只依 digest 拉、`contracts` 的來源、大小上限、GC 在 reconcile 裡、`state_out` 的 tag 與重試；§9.7 寫明 SIGKILL 之後照 `RestartPolicy`。v1.15 依 E13-D037–E13-D039（E13-07，#268）：reconcile 不等寫入完成，寫入依 region 排隊、同一個 key 只留最新值；provider region 未完成首次同步時 `bindings` 保留現有 status；home region 不一致時每個 cluster 只由 home 在那裡的 controller 寫入；舊 leader 在 lease 過期後仍可能寫入新 leader 未 fence 的 region，時間窗由 #277 關掉。§3.2、§3.3。v1.14 依 E13-D033–E13-D035（E13-06，#267）：刪完 MD、TD 需求並在 view 確認不在才刪 `SessionDemand`，任一 provider region 過舊則指派跟著等；孤兒須所有 STS region 已同步且不過舊、沒有任何 instance 有這個 session；controller 只刪需求項目與 `SessionDemand`。v1.13 依 E13-D028–E13-D032（E13-05，#266）：結束不可逆，`end_reason` 留第一次的值；同一輪 `ended_by_spec` > `deps_failed` > `deps_timeout`；`ENDED` 之後不抄 `bindings`；逾時到期即跑一輪，不等全量；過舊 region 的 `READY` 不放行 `start`，`bindings` 照抄。§3.2、§3.3、§3.5。v1.12 依 E13-D022–E13-D027（E13-04，#265）：spec 的 revision 與指派不同時不重推、不新增不刪除、不以 invalid_spec 結束；比較再忽略 `DemandEntry.spec_revision`；一開始就 `ENDED` 且沒有指派則不寫；plane 標錯也是 invalid_spec。v1.11：conformance 的 `beat` 檢查改成 hook 那一行之後 2 秒內 Beat 次數增加，合約不變（每 1 秒一則）；1 秒沒有容許誤差，合規的 worker 也會偶爾判失敗。§9.8。v1.10 依 E13-D001–E13-D011（E13-01，#262）：controller 每個 region 第一次寫入前才 fence；和 region 斷線時保持現狀（W9）；`deps_timeout_s` 從 `SessionDemand.issued_at_ns` 起算，0 取 60 秒；spec 只有 `desired_state` 可改，刪掉當成 `ended_by_spec`；不合法的 spec 以 `invalid_spec` 結束；N4 看 `WorkerStatus`；全量 resync 60 秒。§3.2、§3.3、§3.6。v1.09 依 E08-D062–E08-D064（E8-13，#237）：`hello` 從 params 解析商品、讀 Book 與 Recon、在遠離市價處掛一張限價單，`OnOrder` 確認 `ORDER_STATUS_ACCEPTED` 後撤單；`tape-keeper` 不改。conformance 檢查改到符合後面的決定，不改 SDK 去遷就舊檢查。每個 PR 用 `examples/strategies/conform` 跑完整份 Go SDK conformance。§9.8。v1.08 依 E08-D057–E08-D061（E8-15，#239）：本地訂單簿由 SDK 向 MD 的 `md.view` 要快照再套 `orderbook_delta`，不向交易所要；斷號或 `conn_epoch` 改變就重同步；`not_synced` 與其他讀取失敗沿用 E04-D024 的 1 秒起、加倍、30 秒上限退避；等快照的緩衝上限 10000 筆或 10 秒，到了就丟掉並重要，這些 delta 不是 hook；檔數用 `book.DefaultN`，與 MD conn worker 一致，不用快照窗口大小；tick 仍取自 SYM `Info` 的 `price_tick`；未同步時 `Book` 回 `(nil, false)`；`reset` 清掉本地本並重要快照。§7、§9.4 第 4 條、§9.5 第 7 條。Go 實作在 `sdk/internal/bookpath`。v1.07 依 E08-D053–E08-D056（E8-14，#238）：`Balance` 的 free、locked、prelock 互不重疊、加總為總額，由 TD 填，SDK 原樣覆蓋；Recon 整份換 OMS，該次 ReconRequest 送出之後才 Submit 且快照沒有的 pending_new 保留；Fill 不改 Ledger；讀取回傳複製且不阻塞。§7.2、§9.6 第 4 條。Go 實作在 `sdk/internal/tdpath`。v1.06 依 E08-D050–E08-D052（E8-10，#234）：`Init` 期間連不上 NATS，在 `deadline_ms` 內以退避重試，到期 `INIT_FAILED`；volume 的讀取順序是策略自己呼叫的函式，SDK 不在啟動時自動還原；原子寫入是暫存檔、fsync、rename、再 fsync 目錄。§9.1 第 4 條、§9.2 第 4 與第 9 條、§9.3 第 5 條、§9.4 第 1 條。Go 實作在 `sdk`。v1.05 依 E08-D046–E08-D049（E8-09，#233）：binding 只套用較新的完整快照，一期 selector 回 error、不解析成員；SDK 不自動要 tape、不扣住 live，策略用 `TapePage.head` 去重；`md.view` 與 `md.fetch` 等 `recv_window_ms`，逾時或無 responder 時 callback 收到 error；壞 envelope 丟掉、計入 `dropped`、寫 log、session 不結束。§7、§7.1、§9.3 第 2 條、§9.4 第 2–4 條、§9.5 第 6 條。Go 實作在 `sdk/internal/mdpath`。v1.04 依 E08-D040–E08-D045（E8-08，#232）：OMS 先做自己 algo 的單，Ledger 與完整狀態機留給 #238；第一份 Recon 扣住事件，上限 65536，滿了 FAILED；Recon 逾時或無 responder 以 1–30 秒退避重問；Submit 逾時用 recv_window_ms，標 UNKNOWN；新 incarnation 或 seq 斷號重走 Recon；第一份回覆時，扣住的較舊 incarnation 可丟、較新的再問一次；撤單逾時不標 UNKNOWN。§9.4 第 7 條、§9.6 第 1 條。Go 實作在 `sdk/internal/tdpath`。v1.03 依 E08-D028–E08-D039（E8-12，#236）：`mftiko-conformance` 拉起 worker 執行命令，fd 3 是假 shim，行程內嵌 nats-server（`max_control_line` 262144）。標準策略的行為由 `InitPayload.params` 的 JSON 控制，欄位、檢查 id、JSON 報告寫在 §9.8。`Init` 被接受前收到其他訊息或第二則 `Init` 以 `INIT_FAILED` 結束；`Init` 成功、`Start` 成功前的 `Stop` 走 `DONE`、不呼叫 `OnStart`／`OnStop`（§5.2、§9.2）。任一檢查 fail 則程序 exit 1；相依契約不在 `Hello.contracts` 才 skip。這一期只出報告，STS 部署不讀它。CI 以 fakeworker 一次一種違規驗證對應失敗。Go SDK 整套通過留到 E8-13。v1.02 依 E08-D023–E08-D026（E8-07，#231）：§9.4 第 8 條只追蹤策略 Resolve 過的商品；事件照常立即交付，不為沒見過的 UID 在背景查 SYM、不扣住事件；`Info` 回 `(symbol.Info, error)`，查不到回 error，框架不重試；已解析的商品下架時以 `GetByUidRequest` 加 `include_inactive` 重拿，保留最後一份並標為不活躍，`Info` 仍查得到。v1.01 依 E08-D018–E08-D022、E08-D027（E8-11，#235）：session worker 的行情權限精確到每個 feed，publish 是 `md.v1.view.{inst}.{topic}.{ticker}`、subscribe 是 `md.v1.f.{inst}.{topic}.{ticker}`，不再用 `{inst}.>`；`md.v1.fetch.>` 不變；td 每個 `api_id` 各展開一條；去重後排序，只列 allow；沒有帳號不給 `td.*`，沒有 feed 不給 `md.v1.f`／`md.v1.view`。`api_id`、MD instance、`session_id`、topic、ticker 含 `.`、`*`、`>` 或空白時回 error，不轉義。spec 含 selectors 回 error。E08-D027：維持每個 feed 一條權限，natsdev 產生的 server 設定 `max_control_line` 為 262144；STS 在簽發 session JWT 時檢查 CONNECT 引數，超過就拒絕這份 spec，錯誤寫明 feed 過多（§7）。v1.00 依 E08-D015–E08-D017（E8-05，#229）：TD buffer 上限是 SDK 常數 65536，不進契約，滿了以 FAILED 結束；MD ring 是一塊預先配好的 32 MiB `[]byte`，每則可變長，放不下就丟最舊的若干則直到放得下（跳過正在讀的），單則大於整個 ring 直接丟並計入 `dropped`；hook 執行超過 100ms 計一次 `hook_slow`，`hook_blocked_ms` 是目前這個 hook 已執行的時間（§9.5 第 2、5 條）。v0.99 依 E08-D011–E08-D014（E8-06，#230）：`client_order_id` 的 seq 一個進程一顆計數器，所有 algo、所有帳號共用，每毫秒 1024 筆（0..1023）；用完往前借毫秒，不設上限，`ts_ms` 可能超前牆鐘，實務上要每秒超過一百萬筆才會；時鐘倒退沿用上次的 `ts_ms`。產生器狀態不持久化，跨 incarnation 撞號由 TD 冪等處理（內容不同回 106）。Go 的產生器在 `sdk/internal/cloidgen`，把 `ts_ms` 與 seq 併成一個 uint64，以 `atomic.CompareAndSwap` 更新，不用 mutex（§2、§9.4）。v0.98 依 E08-D006–E08-D010（E8-03，#227）：`contract/wire/c4` 加 `Stopper`，在 `Exiting` 與 `Stopped` 之間呼叫；`Init`／`Start` 的 `deadline_ms` 逾時不等 hook，送 `Done(false)`、`Exiting(INIT_FAILED)`、`Stopped(false)`，exit 10，不呼叫 `Stopper`；Start 失敗（非逾時）才呼叫。`Run` 返回時 `OnStop` 的期限是 plane 在 Init 後交出的 `hook_timeout_s`（`HookTimeout`），逾時 exit 21、不送 `Stopped`，`Exiting` 的 outcome 不變；沒交出的 plane 不限時。因 `Stop`／SIGTERM 結束仍以 `grace_ms` 剩餘為期限。SIGTERM 由 worker main 的 `signal.NotifyContext` 傳入 `Serve`，`Serve` 不攔訊號。刪除 `sdk/internal/c4`（§5.2、§5.3、§9.2）。v0.97 依 E08-D005（E8-04，#228）：§9.6 加第 4 條，建議 SDK 先更新本地 OMS 與 Ledger 再交給策略。v0.96 依 E08-D003（E8-02，#226）：`stsctl.InitPayload` 加 `recv_window_ms`、`hook_timeout_s`，是 STS agent 解析後的實值，不會是 0（§6.1、§9.3 第 1 條）。v0.95 依 E06-D038、E06-D039：成交由 account worker 發在 `td.evt`，目前沒有人落庫，由誰落庫見 §10 P18，不會是 TD agent；TD agent 沒有 NATS user，dev 不發它的 creds。v0.94 依 E06-D035–E06-D037（#200）：事件連線快到 `MaxLifetime` 時先接後斷，換完跑一次 recon，reason 是 `RECONNECT`，期間不報 `DEGRADED`；下單連線的換線不 recon（§6.3，修改 E06-D014）。v0.93 依 E06-D034：`td.proto` 定案，從 `breaking.ignore` 移出，§1 改回定案。v0.92 依 E08-D066：`read.proto` 定案，從 `breaking.ignore` 移出，§1 改回定案。v0.91 依 E06-D034（E6-17，#220）：比照 E6-15 分兩個 PR。#242 刪項目、照 E06-D033 不留 `reserved`，`td.proto` 仍留在 `breaking.ignore`，§1 仍是草案；移出清單與 §1 定案是 #242 合併後的後續。v0.90 依 E08-D066：比照 E6-15 分兩個 PR。#241 刪欄位、照 E08-D065 不留 `reserved`，`read.proto` 仍留在 `breaking.ignore`，§1 維持草案；#241 合併後另開小 PR 把 `read.proto` 移出清單、§1 改回定案。v0.89 依 E08-D003：`orderbook_delta` 不進 prehook，SDK 一律套進本地 book，只有呼叫 `OnBook` 受 `recv_window_ms` 管；prehook 的 `now` 在 dispatcher 取出時取（§7.1、§9.5 第 4 條）。v0.88 依 E08-D003 補上 session worker 結束的順序：先送 `Exiting`，`DONE`、`FAILED` 交付完 TD buffer 再跑 `OnStop`，`CRASHED` 不交付；`grace_ms` 管整個收尾；沒有 `Stop` 的結束，`OnStop` 套 `hook_timeout_s`（§9.2 第 5、6 條，§9.5 第 5 條）。v0.87 依 E08-D004 定案 P15：策略 API 以 UID 為商品身分，ticker 只在 `OnInit`、`OnStart` 的 `Resolve` 出現；§9.4 第 8 條改成只追蹤策略解析過的商品。v0.86 依 E08-D065：每一則行情的 payload 帶商品的 UID，payload 裡沒有 ticker 字串：`FeedEvent.uid`、`BookSnapshot.uid`、`FetchReply.uid`，`md.fetch` 的請求以 UID 指定商品。subject 不變。`read.proto` 回到草案，E8-01 改完再定案。v0.85 依 E06-D033：`td.view` 的 OMS 與帳本查詢換成一個 `ReconRequest`，回一份 `Recon`；拿掉 `Submit.algo_id`、`Cancel.algo_id` 與 104，TD 不解碼 `client_order_id`。`td.proto` 回到草案清單，E6-17 改完再定案。v0.74 依 E04-D058、E04-D059：tape 的 256 MiB 是這個 worker 全部 tape 記憶體，耗盡時回收最舊的 64 KiB chunk；大於 32 KiB 的事件改記 gap。v0.3 把控制面改到 etcd（E00-D004），依賴改成 agent 層級的 lease（E09-D001），shim 與 worker 之間加入生命週期動詞（E01-D007），資料面拿掉給策略看的可用性廣播（E08-D001）。v0.4 依 E00-D005 改成每個 region 一個 etcd cluster：路由函數、每個 cluster 各自的 writer key、版本號不依賴 etcd revision；並依 E09-D002 加入策略 artifact 的格式（§8）。v0.5 依 E06-D001 加入歷史補正：`AccountSettings` 加 interval 與手動觸發，TD agent 訂閱 `td.evt` 落庫，`Fill.fill_id` 規定為交易所的 trade id。v0.6 依 E04-D002：md.proto 加 `OrderBookDelta`，新增 `read.proto`（`md.view`、`md.fetch`），tape 不再用 Redis（拿掉 `tape_url` 與 `StandingSubscription.record`）。v0.7 依 E01-D006：行情的 subject 一律由 MD agent 給，`SelectorBinding` 加 `subject_prefix`。v0.8 依 E09-D003 拿掉 `stsctl.SubscribeRequest`：一期策略完全宣告式。v0.9 依 E09-D004 加入 session volume 與資料 artifact：`SessionSpec.inputs`、`Limits.volume_mb`、`SessionStatus.state_out`、兩個環境變數、§8.4。v0.10 依 E08-D003：`md.f` 加 envelope（`seq`、`conn_epoch`、`md_ts_ns` 從 `Meta` 移過去），拿掉 `Delivery`，加 `SessionSpec.recv_window_ms`、`Limits.hook_timeout_s`、`QueueStat.max_age_ms`。v0.11 依 E06-D002：加 `algo_id`（由 spec 在每個帳號上宣告，可以多個，TD agent 以 lease 檢查衝突），`client_order_id` 的 session 欄位換成 algo，`CancelSession` 換成策略自己發的 `BatchCancel`，STS agent 不再有 `td.order` 的權限。v0.12：`client_order_id` 改成 `ver(4) | algo(10) | ts_ms(40) | seq(10)`。v0.13 依 E04-D003：MD 連線的身分改成 `(venue, route, n)`，endpoint 變成 `mdctl.InitPayload.url` 這個參數，atom id 拿掉 endpoint。v0.14 依 E06-D003：`td.evt` 的 `AccountReset` 換成帶快照的 `Recon`，TD 降級時拒新單、放行撤單。v0.15 依 E09-D005：只有一層 linger，連線在最後一個 atom 退訂時就停；lease 的 TTL 統一 60 秒。v0.16 依 E04-D004 拿掉常駐訂閱（`StandingSubscription` 與 `/md/{inst}/standing/`）：MD 的 owner 只有 session 的 lease 項目。v0.17 依 E06-D001：`tdctl.Report` 加 `first_submit_ns`，backfill 的範圍從下單決定。v0.18 依 E09-D006：session worker 的 NATS 憑證每個 session 一份（`stsctl.InitPayload.nats`），權限見 §7。v0.19 依 E08-D002：一期只做 Go SDK，拿掉 Python 綁 Go 的設計；新增 §9，寫明其他語言的 SDK 要遵守的契約，以及 STS agent 以 `Beat` 逾時判定 `HUNG` 的保險。v0.20 依 E02-D002：UniversalTicker 是佔位，經 SYM 解析之後才決定訂閱什麼；atom id 改成 `<venue>:<route>:<channel>`；加入 topic 表與 SYM 的契約（§7.3，`mftiko.sym.v1`）。v0.21 依 E03-D001：帳號種類和 venue 分成兩個 enum，`state.v1.Account.venue`、`tdctl.InitPayload.venue` 改為 `kind`；BinancePM 是帳號種類，不是 venue。v0.22 依 E01-D006：incarnation 存在不綁 lease 的 `/{plane}/{inst}/incarnation/{h}`，spawn 前以 txn 取號（§3.2 W6）。v0.23：W4 的 watch 改從 list 的 revision（含）開始，不再加 1。v0.24 依 E09-D007–E09-D008、E00-D006–E00-D007：控制平面分成 API server 與 controller；跨平面的依賴改由 controller 以宣告式的需求表達，不綁 TTL（E09-D001、E09-D005），lease 項目改名需求項目，key 拿掉 `{sts_inst}`；新增 STS 的指派 `/sts/{inst}/demand/{session_id}`，Gate 與綁定的整理移到 controller；ack 由 controller watch；SYM 改成啟動時整表載入、跟著變更重載，使用者是 STS、MD、TD；三種 worker 都從 artifact 起（§8）；契約分成草案與定案（§1）；session 的宣告抽成 `mftiko.session.v1`，`stsctl.v1` 不再 import `state.v1`，改用自己的 `BindingState`、`Universe`；`Side`、`OrderType`、`TimeInForce` 移到 `common.v1`，`td.v1` 不再 import `md.v1`。v0.25 依 E00-D004：etcd lease 只有選主 session 一種；`AgentRecord` 綁在它上面，agent 在不在就看它；ack、`WorkerStatus` 不綁 lease，由 agent 自己維護（§3.2 W7）。v0.26：W1 由 etcd 的 auth 與 RBAC 強制，角色表由 instance 清單產生（§3.2 W1）。v0.27：shim 的啟動參數 `Launch` 由 agent 經 shim 的 stdin 送入，欄位由 artifact 的 manifest 換算而來（§4.6、§8.3）。v0.28：C3 的 server（E1-09）：`Launch` 加 `work_dir`，socket 的權限、只收同一個 uid 的連線、殘留與重複的處理（§4.1）；attach 的檢查順序、shim 記住接受過的最大 epoch、錯誤碼表、major 沒有交集時只能 `Signal` 與跨 major 不變的欄位（§4.2）；worker 被 reap 之後 `Signal` 回 `exited`（§4.3）；新增 §10 P12。v0.29：生命週期指令（E1-11）：shim 記下的階段 `Phase` 放進 `Attached`，`Init`、`Start` 只在對應的階段接受，`Stop` 的計時與重複的 `Stop`、`Release` 的條件、shim 自己收到 SIGTERM 時的 grace，錯誤碼加 `worker_busy`（§4.2、§4.3）；`cred` 的 uid 和 shim 不同而 shim 沒有 `CAP_KILL` 時不 spawn（§4.6）。v0.30：worker 的輸出（E1-14）：`Launch.output`，檔案放在 `${WORK_DIR}/log/{plane}/{h}.stdout`、`{h}.stderr`，依大小輪替，寫不及時丟棄並在檔案裡註明，不讓 worker 阻塞（§4.5 S4、§4.6）。v0.31：事件緩衝與補送（E1-12）：只有進環狀緩衝的事件有 seq；`Ack` 釋放已確認的事件，環的 1024 則只算未確認的；`gap` 只標在 agent 要的事件被溢出丟掉時；attach 之後先送最新的 `Report` 再補送；控制者換人或離開時，shim 對 worker 送 `Controller`；agent 送完指令就關閉連線時，已送出的指令照樣執行（§4.1、§4.2、§4.4）。v0.32：結束檔與 `Exited`（E1-13）：新增 `ExitRecord`；`ExitInfo` 的合成規則（code、signal 一律依 wait status，outcome、reason 有 `Exiting` 就依它，沒有就依 exit code）；reap 之後讀完 fd 3 才記錄，記錄完才是 `EXITED`、才接受 `Release`；有前一個 incarnation 沒收走的結束檔時 shim 不啟動（§4.3、§4.5 S3、§4.6、§5.3）。v0.33 依 E01-D008：agent 以快照恢復，事件只是通知；`Attached` 加 `exiting`、`exit`，且 `next_seq` 是快照的時間點；`Ack` 不是提交（§4.4）。v0.34 依 E02-D003：幣與數量的單位。qty 的單位是 `contract_value_ccy`（張數 × `contract_size`）：linear 與期權是 base，inverse 是 quote，不是張數；`price_ccy` 是 price、`price_tick` 與權利金的幣，現貨與期貨（含 inverse）是 quote，幣本位期權是 base，U 本位期權是 quote；`settlement_asset` 是損益結算幣，不是保證金幣，三個欄位由 adapter 從原生欄位填，下游不推導。別名只在 SYM 收（一律大寫，例如 XBT→BTC），先正規化再判定 quanto 再推 ticker；strike 先依 `spell_strike`（`6400.0` 與 `6400` 是同一個 ticker）。`USD`、`USDC`、`USDT` 不合併，報價 USD 而結算 USDC 是 quanto，一期不入庫、需求 `REJECTED`。狀態 enum 暫定（`PRELISTING`、`TRADABLE`、`HALTED`、`SETTLING`、`EXPIRED`、`DELISTED`、`UNKNOWN`），契約仍是 `bool active`，交易路徑只接受 `TRADABLE`。SYM 的 listing 預定分兩張表：活躍表是狀態還會變的（`PRELISTING`、`TRADABLE`、`HALTED`、`SETTLING`、`UNKNOWN`），Resolver 與熱路徑只讀它；終結表是 `EXPIRED`、`DELISTED`，狀態不再變，不再刷新，給歷史查詢，可以歸檔；搬過去時 UniversalTicker 字串不變。這一版不拆表。UniversalTicker 只給人寫 spec；系統內部用 UID，是兩張表的主鍵，跨表唯一，搬表時不變，下架再上市拿新的 UID。`SymbolInfo` 加 `uid`（寬度暫定，契約暫用 `uint64`，0 不分配）與 `GetByUidRequest`。寬度、分配方式、重啟是否沿用還沒定；建議方向是一條 Postgres sequence（例如 `symbol_uid_seq`）餵兩張表的 `uid` 主鍵，`symbol_filter` 以 uid 為鍵，終結時整列連 uid 搬走（§10 P13），這一版不實作。NATS subject 用人讀的名字，不用 UID；確切拼法還沒定（§10 P14），這一版仍是 `md.f.{inst}.{topic}.{ticker}`。策略 hook 看 ticker 還是 UID 還沒定（§10 P15）。`td.order` 等仍是 ticker。`td.proto` 的數量欄位註上單位（§7.2、§7.3）。v0.35 依 E01-D009：需求過舊（`LastContact` 超過門檻，預設 15 秒）時，agent 不依需求停 worker、替換、推 live config，也不寫 `WorkerStatus`；重啟結束的 worker 與送 `Start` 由各平面的 Policy 決定（§3.2 W8）。v0.36 改寫 §10 P13：UID 由 SYM 分配，是隨機、唯一、永不重用的整數，不是單調序號，也不是 Postgres sequence；跟列一起持久化，重啟與重載不變，下架再上市拿新的，碰撞就重試。契約型別是 `uint64`，0 非法。寬度與範圍仍開放，Postgres BIGINT 有號，建議 `1..2^63-1`。`symbol_filter` 也分活躍表與終結表，終結時跟列一起搬。SYM `List` 以 offset＋limit 分頁，跨頁不一致可以接受。`Universe.current` 是 selector，尚未定，不在 E02-D003；selector 沒有 UID。P14、P15 仍開放。v0.37：agent 起 shim 的方式（E1-15）：開新 session、shim 的 log 接到 `${WORK_DIR}/log/{plane}/{h}.shim.log`、`Launch.env` 最後由 agent 補上身分變數（§4.6）。v0.38：同一個 worker key 至多一個存活的 incarnation（E1-16）：上一個沒有結束紀錄（shim 不見了）時，agent 以 pid 加 start_time 確認它已經不在，才起下一個；pid 被重用或成了 zombie 都算不在（§4.6）。v0.39：Beat 逾時（E1-18）：agent 記下每個 worker 最後一次 `Beat`，超過平面 Policy 的逾時就通知 Policy，每段沉默一次，由 Policy 決定要不要停掉它（§4.4）。v0.40：啟動時收養（E1-17）：新的 term 在第一次 reconcile 之前掃 `run/{plane}/` 的 socket 與結束檔，逐一 attach；attach 不成的保留 `WorkerStatus`、不起新的 incarnation、定期重試；shim 已不在的結束檔當成已結束的 worker；收養後依 etcd 把 live config 再推一次，暫行到 E1-22 以 worker 回報的 digest 比對（E01-D011，§4.6）。v0.41 定案 symbol UID 的寬度與範圍（§10 P13、E02-D003）：契約型別 `uint32`，範圍 `1..4294967295`（`1..2^32-1`），0 非法。Postgres 欄位是 `bigint`，`CHECK (uid BETWEEN 1 AND 4294967295)`。v0.42 依 E2-03：`migrations/0002_symbol` 改成活躍表 `symbol_active` 與終結表 `symbol_terminal`，主鍵是 uid；`symbol_filter_active` 與 `symbol_filter_terminal` 跟列一起搬。活躍表以 generated column 拆出 venue、category，`UNIQUE (venue, category, exch_ticker)` 與 `universal_ticker` 唯一只在活躍表。加 `contract_value_ccy`、`price_ccy`，結算欄位仍叫 `settle`。strike 入庫前去掉尾端的零。`symbol_revision` 記下每個 `(venue, category)` 的 revision，通知的訊息仍見 P7。契約的 `active` 仍是 bool。另加 symbol_uid（主鍵 uid，列不刪）：兩張 listing 與兩張 filter 的 uid 參照它，分配先插入 symbol_uid，撞 unique 就重抽，歸檔或刪列之後也不會再抽到同一個 uid。symbol_reject_shared_uid 仍在，因為登記表擋不住兩張 listing 同時持有同一個 uid；搬表在同一個交易裡先刪活躍列再插入終結列。uid 是 BIGINT，CHECK (uid BETWEEN 1 AND 4294967295)。契約型別見 P13。v0.43：agent 在任何一個 shim 收到 `Preempted`，就當成自己的 term 已經結束：立刻退出，不把那個 worker 當成失聯，不 Release，也不寫 `WorkerStatus`（§4.2，E1-20）。v0.44 依 E01-D011：live config 改由 C3／C4 的 `Live` 推送（帶 digest），worker 套用之後回 `Applied{digest}`，`Init` 帶 `live_digest`；shim 把最新的 `Applied` 記在 `Attached.applied_digest` 並放進事件環；agent 以它判斷 worker 手上是哪一份，收養後不再無條件重推（§4.3、§4.4、§4.6、§5.2、§9.2，E1-22）。v0.45：`SymbolInfo` 的 `bool active` 改成 `SymbolStatus`（`SYMBOL_STATUS_UNSPECIFIED = 0`，以及 `PRELISTING`、`TRADABLE`、`HALTED`、`SETTLING`、`EXPIRED`、`DELISTED`、`UNKNOWN`）。`UNSPECIFIED` 非法、不可交易、不入庫；只有 `TRADABLE` 可交易；對不上的原生狀態是 `UNKNOWN`。列舉仍暫定，契約已改用這個 enum。活躍表與終結表的 CHECK 不變。SYM 在 `Validate` 之前套資產別名（`XBT`→`BTC`），`exch_ticker` 維持交易所拼法。v0.46 依 E2-08：SYM 在 subject `sym` 上回答 Get、GetByUid、List、Reverse、Refresh。成功是 `Reply`。業務錯誤 `invalid`、`not_found`、`unavailable` 走 natsx 的 error reply（option A），不填 `Reply.error`。`SymbolInfo` 加 `contract_value_ccy`、`price_ccy`，結算欄位仍叫 `settle`。`include_inactive` 預設只讀活躍表。`List` 維持 offset＋limit，每頁最多 500 筆，回覆小於 1 MiB。v0.47 依 E2-08 定案查詢。同一 ticker 或交易所拼法同時在活躍表與終結表時回活躍列（下架再上市會同時存在）。只有終結列、而且多於一列時，取 `updated_at_ns` 最大者，相同再取 uid 最大者。`ReverseRequest` 加 `include_inactive`，預設 false，只讀活躍表；設了才在活躍表沒有時讀終結表，回該列原本的 uid。`List` 的 `limit` 為 0 時是 500，大於 500 是 `invalid`，不截短。一頁 protobuf 達到或超過 1 MiB 是 `unavailable`，不改成較短的一頁。`invalid`、`not_found`、`unavailable` 的原因都寫出欄位、值與為什麼。`RefreshResult` 的 map key 是 `venue/category`。`-feeds` 只決定這個行程刷新哪些 feed；讀取查整庫。文法正確但沒有列的 venue，List 是空清單（`total` 0）。刷新一個這個行程沒有的 canonical venue 是 `invalid`；venue 拼法不合文法也是 `invalid`。v0.48 依 E00-D008：十進位數改成 `double`。`md.v1`、`td.v1`、`sym.v1` 的價格、數量、金額、手續費、費率、greeks、`contract_size`、strike、filter 的界限，都從 `string` 改成 `double`，optional 的仍是 optional；`OrderBookDelta` 以 qty 0 刪除價位。`client_order_id` 仍是十進位字串（§2、§7.2、§7.3）。v0.49 依 E2-09 定案 P7：revision commit 之後，SYM 發 `sym.changed.{venue}.{category}`，內容是 `Changed{venue, category, revision}`。venue 與 category 用人讀的名字，不用 UID。整體 topic 拼法仍見 P14，P14 定案時這個 subject 可能改名。經 NATS 的 Resolver 只以 `List` 分頁載入活躍表，索引與進程內 Resolver 相同（UID → Info、ticker → UID、ExchKey → UID），整份快照一次換上。`include_inactive` 直接問 SYM，不進快取。漏收時約每 5 分鐘以 `RevisionsRequest` 比對全部表的 revision，NATS 重連時也比一次，落後就重載那張表。`sym` 另回答 `Revisions`。session 的 subscribe 加上 `sym.changed.>`（E09-D006）。v0.50 依 E03-D002：exchange 的 adaptor 是 sans-I/O 的 codec。REST 的方法回傳 `Call`（`HTTPRequest` 加上解析回應的 `Parse`），分頁是 `Page`，由 worker 經傳輸執行；adapter 不連線、不讀時鐘、不重試。錯誤加 `ErrAuth`、`ErrUnknownOutcome`。技術棧是 coder/websocket、fasthttp、sonic，都不出現在 adaptor 的介面裡（§1）。v0.51 依 E03-D003：WS 下單。exchange 加 `TradeStream` codec：`Dial` 送 logon，`Submit`、`Cancel` 以 worker 的 id 編成 frame，`Decode` 以 id 交回答；codec 把讀回答所需寫進交易所的請求 id，不靠記憶。連線也帶私有事件時不另開 `PrivateStream`。逾時與斷線未回答的請求是 `ErrUnknownOutcome`（`exchange.Inflight`）。`Account.NewTradeStream`，不走 WS 下單時是 `ErrUnsupported`（§1）。還沒定的寫在 [§10](#10-待定)。v0.52 依 E2-10：`sym.proto` 定案（E00-D007），從草案清單移出，`buf breaking` 開始檢查它。v0.53 依 E3-17：exchange 的 adaptor 介面定案（E00-D006、E03-D002、E03-D003）。它是 Go 介面，沒有 proto，不進 `buf breaking`。交易面是 `Account`，`AccountKind` 是註冊 struct。enum 由 exchange 自持，數值與 `common.v1`、`td.v1` 對齊。v0.54 定案 P6、P9：`DEAD` 只在放棄重拉時寫，`BACKOFF` 期間需求是 `PENDING`，`LOST` 維持最後一次的狀態（§3.5）。MD、TD 每次 spawn 取當時的最新版，不在 controller 或 etcd 釘版本；跑著的進程保持原 binary，換 agent 不換 incarnation。E9 之前執行這個 repo 建出的 binary（§8、E01-D006、E09-D002）。同一個 session 重拉仍用 spec 釘住的策略 digest。v0.55 依 E04-D005–E04-D011：`md.f` 的 v1 envelope 固定 40 bytes、little-endian，用 `encoding/binary` 讀寫，不用 unsafe。拿掉 `len`：長度由版本決定，offset 1 到 7 保留且必須為 0。欄位是 `version`、`md_ts_ns`、`conn_epoch`、`seq`、`digest`（正規化 payload 的 xxHash64，seed 0，不含 envelope）。不認識的版本、保留 byte 不是 0、長度不足，讀取端拒收（§7.1、§9.4）。v0.56 依 E04-D012–E04-D017：dev NATS 用 operator／account JWT 與 full resolver（單台、不開 TLS）。MD 與 md.fetch 的權限、平台 inbox 前綴寫在 §7。多 region 的拓撲還沒定（§10 P16）。v0.57 依 E04-D018–E04-D023：`md.fetch` 的 subject 改成 `md.fetch.{venue}`。K 線超過單頁時以 `endNs` 往回分頁，單次 1 到 5000 根，回覆在 1 MiB 以下。用量達到設定的閾值回 `rate_limited`，不排隊、不重試、不快取。ticker 只解活躍表。執行檔是 `cmd/mftiko-mdfetch`。session 的 publish、md.fetch 的 subscribe，以及 SDK 的 fetch 路徑見 §7、§9.4。v0.58 依 E04-D020：`exchange.Used` 加 `ResetAt`（計數器歸零的絕對時間），`Usage` 多收回應時間。Binance 依窗口對齊下一個邊界，Bybit 讀 reset timestamp。`md.fetch` 在 `ResetAt` 讓用量作廢；429 與 418 依 `Retry-After` 停送，429 沒有時等到 `ResetAt`，再沒有才用可設定的保守預設。418 期間不送請求、不探測。各 venue 的真實窗口與標頭留到 E4-09 到 E4-11（§1、§7）。v0.59：`mdctl.InitPayload` 加 `conn_epoch`。它是上次上報的號，0 表示還沒有上報過，第一次撥號用 1。重連與新行程的第一次撥號都先把新號寫進 `Report`，寫完才撥，`md.f` 才用這個號。撥不上不消耗。不寫 etcd（§6.2，E04-D009）。v0.60 依 E06-D004–E06-D009（E6-01）：連線與重建是 TD 的事，STS 是最後一道防線；`td.order` 逾時不再轉成 1xx，SDK 交出 `ORDER_STATUS_UNKNOWN`，不查、不重送；交易所沒有回答的單由 account worker 過了請求有效期之後以 `client_order_id` 主動查，下單與撤單一律帶有效期，同一個 incarnation 裡以 cloid 冪等；`td.view` 只讀本地快照，槓桿讀本地快取，回覆帶狀態、incarnation 與 seq；一期只支援單向持倉（`hedge_mode`）；TD 的 Policy 與 MD 相同，`ErrAuth`、`hedge_mode` 不重拉；新增 §10 P17（§7、§9.4）。契約的改動在 E6-02。 v0.61 依 E04-D024–E04-D029：conn worker 維護本地訂單簿。`orderbook` 推 top-N，有變才推；`orderbook_delta` 推正規化增量，第一次成功的快照與重抓都是 `reset`。SnapshotREST 在進程內抓一張 Depth，之後只套增量，不自己重抓。重抓只因斷號或 checksum 失敗。快照價格範圍以外的檔照常推，之後的 `md.view` 也照回。HTTP 失敗從 1 秒指數退避到 30 秒；429 與 418 依 `Retry-After`，沒有就依這次回應的 `ResetAt`，再沒有用 `-rate-hold`（預設 1 分鐘）。緩衝上限預設 10 秒與 10000 筆。N 與緩衝上限在 `InitPayload`，`exch_ticker` 在指派上。SnapshotStream 不打 REST。這兩個 feed 的交易所序號不進 envelope 與 payload（§6.2、§7.1）。 v0.62 依 E04-D030–E04-D033：本地訂單簿改成 `sdk/book` 的刻度環，環長預設 2048，須為 2 的冪。`price_tick` 在指派上，從 SYM 的 price_tick 於載入 spec 時抄上；缺或小於等於 0 則這本簿只記日誌、不維護。`orderbook_delta` 原樣轉發增量，不預先過濾；窗外的檔在套用時丟掉，視窗滑動不重抓。`orderbook` 的 top-N 只在增量碰到 top-N 時重算。`reset` 與之後的 `md.view` 都是窗內的 book。 v0.63 依 E04-D034：SnapshotStream（Bybit、OKX）在斷號或 checksum 失敗時從已確認的訂閱拿掉該 channel，經既有的速率限制先退訂再訂閱，不拆連線。每一本簿自己以 1 秒起、加倍、上限 30 秒退避。下一則串流快照是 `reset`。v0.64 依 E06-D010–E06-D011（E6-02）：`td.order` 的回覆只有 ack 或本地拒絕（`OrderReply{accepted, code, reason, client_order_id}`），之後的狀態一律從 `td.evt` 拿；單的每一次變化都是一則 `OrderUpdate{order, cause, code, reason}`，拒單與撤單被拒也是，拿掉 `OrderAck`、`BatchCancelResult`、`OrderReject`、`CancelReject` 與 `Order.reject_reason`，`BatchCancel` 拿掉 `timeout_ms`；`OrderStatus` 改成單在交易所那一側的狀態並重新編號：加 `ACCEPTED`（上簿），`NEW` 只表示交易所收到、當下沒拒；拿掉 `PENDING_CANCEL`，撤單在途改成 `Order.cancel_sent_ns`，撤單被拒只清掉它；`OrderCause` 加 `ACKNOWLEDGED`；撤單沒有回答時不記成 `UNKNOWN`（修改 E06-D005）；同一次變化只發一則。1xx 有了名字（`RejectCode`），只列一期會碰到的：101 `ACCOUNT_OFFLINE` 到 108 `RATE_LIMITED` 是本地拒絕，120 `NOT_PLACED` 是查單後確定沒下成（§7.2）。商品欄位從 ticker 字串改成 UID（`uint32`，E02-D003），含 `tdctl.Report.first_submit_ns` 的 key。`td.view` 的 `state`、`reconciled_at_ns`、`incarnation`、`seq` 放在 `ViewReply` 上；`AccountState` 從 `tdctl.v1` 移到 `td.v1`；`EnsureLeverageRequest` 改成 `LeverageRequest`，`LeverageInfo.leverage` 改成 optional；`LedgerViewRequest` 可依 UID 篩選；`OmsViewRequest` 拿掉 `session_id`。account worker 每個 `api_id` 一份 NATS 憑證，由 TD agent 簽，經 `tdctl.InitPayload.nats` 送進去，inbox 前綴 `_INBOX_td_{api_id}`；TD agent 一份 instance 憑證，inbox 前綴 `_INBOX_td`（§5.4、§6.3、§7）。v0.65：conn worker 在 Init 載入自己那個 venue 的活躍表，訂閱 `sym.changed`，之後從這份記憶體讀 `exch_ticker` 與 `price_tick`。MD user 的 subscribe 加上 `sym.changed.>`（E04-D015、E04-D026、E04-D033）。v0.66 依 E06-D001：TD agent 不查 SYM。歷史補正的交易所讀取是無狀態服務，比照 md.fetch，subject 是 `td.backfill.{venue}`。服務載入連得到的 venue 的活躍表並反查；TD agent 排程、落庫，把解開的憑證放進該次請求。v0.67 依 E06-D012（E6-05）：`exchange.AccountSnapshot` 加 `HedgeMode`，帳號在任何地方是雙向持倉就是真，worker 在每次 recon（含 `Start` 的第一次）檢查；真實 adapter 讀持倉模式在 E6-16。`exchangetest` 加假帳號 `AccountVenue`。v0.68：`mdctl.Report` 的 `atoms` 改成 `feeds`（`FeedStatus`）。conn worker 每次上報都帶目前的 `conn_epoch` 和每一筆 `topic.UniversalTicker` 的狀態。不在活躍表或這條 route 訂不了是 `REJECTED`，agent 照寫；在表上但不是 `TRADABLE` 是 `PENDING`；channel 被交易所 ack 後，掛在上面的 feed 是 `SUBSCRIBED`，worker `RUNNING` 時 agent 寫 `READY`（E04-D026、§3.5、§6.2）。v0.69 依 E06-D013（E6-04）：`Account.sealed_credentials` 的格式定案：每個 TD instance 一對 X25519 金鑰，NaCl sealed box 前加版本與收件公鑰的 8 bytes 指紋；明文是新的 `state.v1.AccountCredentials`，連 `api_id`、`kind` 一起封，解開後比對；解不開的三種 reason。封裝工具是 `cmd/mftiko-seal`（§3.6）。 v0.70 依 E04-D035–E04-D052（E4-06）：一個 feed 一個 tape ring，讀取無鎖；`md.view` 的 tape 不帶 envelope，分頁帶 `head`、`since_ns` 與 gap 的 `last`／`first`；book 回現在的窗內全本，未同步是 `not_synced`。`InitPayload` 加 tape 的三個上限。接 live 以 `head` 對齊，不必等第一筆（§6.2、§7、§7.1、§9.4）。 v0.71 依 E04-D053–E04-D054（#180）：md.f 不再有 top-N。拿掉 OrderBook 與 FeedEnd，FeedEvent 的 field 4、100 reserved；InitPayload.book_depth reserved。book topic 只剩 orderbook_delta。寫 orderbook 的設定或訂閱回 invalid，不換成 delta。SnapshotREST 抓 BookSync.Depth()，不再因為 N 大於檔數而退出。exchange.Sink 拿掉 FeedEnd（E04-D054）。 v0.72 依 E06-D014（E6-06）：account worker 的 Init、Start、Report 與連線角色：只有事件連線決定 `DEGRADED`，計畫性換線先接後斷，reason `auth`、`hedge_mode`、`kind_unsupported`、`stream_lost`；一期不跑交易多個 venue 的帳號種類（BinancePM）；endpoint 是 worker 的旗標（§6.3）。 v0.73 依 E04-D055–E04-D057（#181）定案 P14，並從 §10 移出：行情 subject 是 `md.v1.f.{inst}.{topic}.{ticker}` 與 `md.v1.view.{inst}.{topic}.{ticker}`，`inst` 保留、是字串、不綁 region；版號在 subject 前段；行情 subject 不帶 venue；ticker 不轉義；topic 是 `exchange.Topic` 的字串。`md.fetch` 改為 `md.v1.fetch.{venue}`。變更通知改為 `sym.v1.changed.{venue}.{category}`，不帶 inst。請求 subject 仍是恰好 `sym`。§7 的權限表改用這些 subject。改名、權限與 creds 的實作在 #194。 v0.75 依 E06-D015（E6-07）：OMS 只放活著的單，`OmsViewRequest.settled` 不再有作用；槓桿快取取自持倉的 leverage；recon 期間的事件先排著、快照之後重放；斷線期間消失的掛單記成 `UNKNOWN`；account worker 從 Init 起連 NATS 並回答 `td.view`（§6.3、§7.2）。 v0.76 依 E04-D060（#152）：刪掉 `mdctl.proto`、`md.proto` 的 `reserved`，現有欄位號碼不重排。`read.proto` 沒有 `reserved`。三份仍在草案清單，§1 仍是草案。移出清單並把 §1 改成定案，是這版合併之後的後續。 v0.77 依 E04-D060（E4-12，#152）：`mdctl.proto`、`md.proto`、`read.proto` 定案，從草案清單移出。`AtomAssignment.record` 保留且仍忽略。 v0.78 依 E06-D016（E6-10）：td-agent；`tdctl.InitPayload` 加 `rest_url`、`stream_url`、`simulated`，由 td-agent 依帳號種類給；`AccountState` 拿掉 `UNAVAILABLE`（reserved 3），持倉模式只在 Start 檢查；`algo_ids` 的重疊由 STS agent 檢查，TD 不檢查（§3.6、§6.3、§7.2）。 v0.79 依 E06-D017（E6-16）：`exchange.Account` 加 `QueryOrder`，以 `client_order_id` 經 REST 查單，交易所說沒有這張單時是 `exchange.ErrNotFound`。`AccountConfig.Validity` 是請求有效期（預設 5 秒、上限 60 秒）：Binance、Bybit 的 `recvWindow`，OKX 下單撤單的 `expTime`。`Snapshot` 讀持倉模式：Binance UM `positionSide/dual`、OKX `account/config` 的 `posMode`、Bybit 掛單與持倉的 `positionIdx`。`exchangetest.AccountVenue` 可查單，並以 `Outcome.Late`、`Arrive` 模擬晚到的請求。 v0.80 依 E06-D018–E06-D024（E6-08）：`Submit` 與 `Cancel` 加 `recv_window_ms`（0 用 TD instance 的 `-validity`，預設 5 秒；超過 60 秒壓到上限；時鐘誤差是 `-clock-skew`，預設 1 秒）。cloid 冪等只記在這次執行的記憶體（24 小時或 10 萬筆，先到者），不持久化；同 cloid 內容不同回 106，內容相同回原結果、不送出。`OrderRate` 用到 90% 回 108。查單失敗從 1 秒加倍到 30 秒，超過 5 分鐘帳號 `DEGRADED`（reason `query_failed`），查單有結論且事件連線在就回到 `READY`。`UNKNOWN` 可以用 cloid 撤。`TradeStream` 不可用時改走 REST，不拒單。E06-D020 修改 E06-D017：有效期可以在每一筆請求上。 v0.81 依 E06-D025–E06-D028（E6-09，同日修訂）：OMS 與交易所之間（事件、recon、查單）以 `order_id` 對單，沒有 client id 的單仍以 `order_id` 追蹤；系統裡面（策略、`td.order`、`Cancel`、`BatchCancel`、`td.view`、向外的 `OrderUpdate`）以 cloid 為鍵。`Cancel` 與 `BatchCancel` 只收 `client_order_id`（`order_id` 欄位保留），OMS 送出時換成那張單的 `order_id`。`BatchCancel` 是一筆交易所批次撤單，不挑單、不檢查歸屬、不分批、不排隊、不重試。`CancelBatch` 為 1 時本地拒絕。長過上限仍送出，交易所的拒絕原樣回報。一批算 `OrderRate` 的一筆。商品解不出來只影響那一筆。解不開成平台 cloid 的不屬於任何 algo。一次撤掉某 algo 的全部掛單改由 STS SDK 做。cancel-on-disconnect 不接。 v0.82 依 E06-D029–E06-D031（E6-15，#175）：刪掉 `td.proto` 的 `reserved`，現有欄位號碼不重排。`tdctl.proto` 沒有 `reserved`。兩份仍在草案清單，§1 仍是草案；移出清單並把 §1 改成定案，是這版合併之後的後續。`ViewReply` 加 `cancel_batch`，每一則 `td.view` 回覆都帶這個帳號的 `Limits.CancelBatch`（1 表示沒有批次撤單），策略的 SDK 照它拆批。`AccountSettings` 的補正 interval 與手動觸發，以及 `td.backfill` 相關欄位（含 `Report.first_submit_ns`）照現狀保留。 v0.83 依 E06-D032（E6-15，#175）：拿掉 `OmsViewRequest.settled`（欄位 2），改成 `reserved 2` 與 `reserved "settled"`，`algo_id` 仍是 3。 v0.84 依 E06-D029（E6-15，#175）：`tdctl.proto`、`td.proto` 定案，從草案清單移出，`buf breaking` 開始檢查。`OmsViewRequest` 的 `reserved 2` 留下（E06-D032）。歷史補正欄位留下（E06-D030）。>
> proto 區塊開頭的 `// file:` 是它在 `contract/proto/` 下的路徑。區塊以 `protoc` 編譯驗證過。v0.86 依 E08-D065 改了 `md.proto` 的 `FeedEvent` 與 `read.proto`。v0.85 依 E06-D033 改了 `td.proto` 的 `td.order` 與 `td.view` 區塊。v0.83 把 `OmsViewRequest.settled` 改成 `reserved 2` 與 `reserved "settled"`。v0.82 刪掉 `td.proto` 的 `reserved`，並加 `ViewReply.cancel_batch`。v0.77 改了 `mdctl.proto` 的 `AtomAssignment.record` 註解：定案時保留。v0.76 刪掉 `mdctl.proto`、`md.proto` 的 `reserved`。v0.74 改了 `mdctl.proto` 的 `tape_budget_bytes` 註解。v0.73 改了 subject 註解（`state.proto` 的 `EntryStatus.subject`、`stsctl.proto` 的 `subject_prefix`、`read.proto` 的 view／fetch 標題與 `FetchKlines.ticker`、`sym.proto` 的 `RevisionsRequest` 與 `Changed`）。#194 已把 `contract/proto/` 對齊這幾行；其餘仍逐字相同（第一行除外）。

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

控制流只走垂直方向：API server 寫宣告，controller 推導需求與指派，agent 照著起停 worker（DECISIONS E09-D001、E09-D007）。agent 之間沒有控制流，平面之間只有 C6 的資料流。

| 契約 | 兩端 | 傳輸 | 內容 | 擁有者 |
|---|---|---|---|---|
| E | API server、controller、agent ⇄ etcd | etcd key ＋ watch | 宣告、需求項目、STS 的指派、ack／nack、agent 狀態（`mftiko.state.v1`；session 的宣告在 `mftiko.session.v1`） | 依前綴，見 §3.1 |
| C3 | agent ⇄ shim | unix socket | attach、生命週期、事件、信號（`mftiko.shim.v1`） | 平台 |
| C4 | shim ⇄ worker | socketpair（worker 的 fd 3） | 生命週期、心跳、回報、結束方式（`mftiko.worker.v1`） | 平台 |
| C5 | agent ⇄ worker（包在 C3、C4 裡） | — | 各平面的 payload（`stsctl`、`mdctl`、`tdctl`） | 各平面 |
| C6 | worker ⇄ worker、服務 | NATS | 行情、行情讀取、下單、帳號事件、商品資料（`mftiko.md.v1`、`mftiko.td.v1`、`mftiko.sym.v1`） | `md.v1` MD、`td.v1` TD、`sym.v1` SYM |
| A | console、CLI ⇄ API server | HTTPS（JSON；串流是 SSE） | 策略目錄、session、帳號、各平面與 controller 的狀態、認證（`contract/openapi/api/v1.yaml`，DECISIONS E10-D002） | API server |

每份契約目前的狀態與定案的時點見 §1。

API server（`cmd/mftiko-api`）的設定分三類。`-in` 讀 `mftiko-etcd-roles` 同一份部署 JSON（E10-D012、E13-D012）。`-config` 讀 API 專用 JSON：使用者 `name` 與 token SHA-256（E10-D009）、策略 repository 清單（E10-D013）、registry 位址、Docker config.json 路徑（`registry_creds`，空則匿名）與 `plain_http`（E09-D033、E10-D060）、各 TD instance 公鑰（E10-D011）、W5 保留期（E10-D017，預設 7 天）、region 不可用門檻（E10-D014，預設 15 秒）、監聽位址。秘密值走環境變數，比照 controller：etcd `api` 密碼、cookie HMAC 金鑰、Postgres DSN（DECISIONS E10-D023）。Load 讀進並驗證全部設定，不在 Load 時連 Postgres 或 registry。API 啟動時建立 Postgres 連線池，但不等它連上；readiness 仍只看 etcd（E10-D048，修改 E10-D024）。Postgres 不可用時稽核寫不進去，建立 session 回 503 且不寫 etcd（E10-D031）。`repositories` 仍是不含 host 的路徑；回應的 `repository` 是位址加路徑，`id` 是最後一段（E10-D035）。目錄的請求才連 registry，而且不快取（E10-D057、E10-D060）。W5 在既有的 reconcile 裡刪除過保留期的 terminal session：先關閉 `session_records`，再以 `deleteIf` 刪 etcd；Postgres 不可用則該輪不刪（E10-D080–E10-D083）。`DELETE /api/v1/sessions/{session_id}` 立即刪除 terminal session，非 terminal 回 409 `not_terminal`，成功與重複刪除回 204，Postgres 不可用回 503 `storage_unavailable`（E10-D084）。除了 `POST /api/v1/auth/login`，每個 `/api` 請求都要認證（E10-D009）。CLI 送 `Authorization: Bearer <token>`。console 用該 token 換 cookie，名稱 `mftiko_session`，`HttpOnly`、`Secure`（明文 HTTP 也一樣，E10-D050）、`SameSite=Strict`、`Path=/`。cookie 值是 `base64url(JSON)` + `.` + `base64url(HMAC-SHA256)`，沒有 padding；JSON 是緊湊的 `{"name":"<name>","exp":<unix 秒>}`，金鑰順序與這兩個欄位固定，HMAC 蓋這段 JSON 的原始位元組，金鑰是各副本共用的 cookie HMAC 字串的原始位元組，預設 12 小時。空的 Bearer 也算帶了且無效。Bearer 與 cookie 同時存在時看 Bearer；Bearer 無效回 401，不退回 cookie（E10-D051）。`Authorization` 不是 Bearer 時才看 cookie。登出只清 cookie，沒有拒絕清單，客戶端留著的舊值在 `exp` 之前仍然有效。以 cookie 認證的寫入（沒有 body 的也算）與登入要求 `Content-Type: application/json`，Bearer 不檢查（E10-D110）。稽核在回應之後把一列放進有上限的佇列，由背景寫入，不拖慢回應；佇列滿了丟棄，記 error log 與計數器；SIGTERM 在 E10-D027 的收尾時限內把佇列寫完（E10-D049）。`/healthz` 與 `/readyz` 不記，靜態檔也不記。body 存 jsonb，上限 64 KiB，超過就截斷並標記成 `{"truncated":true,"len":<redacted bytes>,"prefix":"<head>"}`；不是 JSON 只記長度；帳號憑證欄位與 `token` 欄位先移除（E10-D010、E10-D052）。`X-Request-Id` 一律由伺服器產生並回給呼叫端，不採用外面帶進來的（E10-D053）。來源只記 `RemoteAddr`（含 port），不信任 `X-Forwarded-For`（E10-D054）。登入不限速。token 須是至少 32 bytes 的隨機值，產生方式是 `token=$(openssl rand -base64 32 | tr -d '\n'); printf '%s' "$token" | sha256sum`，把 hex 放進 `users[].token_sha256`，呼叫端送 token 字串本身；設定只存 SHA-256，API 無法檢查長度（E10-D055）。每個 cluster 一個 `Mirror`，監看整個 `/mftiko/v1/` keyspace（`api` 角色可讀全部），不拆前綴（E10-D025）。任一 region 可用，`/readyz` 回 200，body 列出每個 region 的狀態與 `unavailable_regions`；全部不可用才回 503。`/healthz` 只看進程存活（E10-D026）。SIGTERM 後 readiness 先轉 503、停止接新請求、等進行中請求，最多 10 秒（可設定），以 0 結束（E10-D027）。`-web` 指向 `web/build`，從磁碟讀、不嵌入 binary；非 `/api/` 且找不到檔案的路徑回 `index.html`；未設 `-web` 只提供 API（E10-D028）。有 `-tls-cert`、`-tls-key` 跑 HTTPS 加 HTTP/2（SSE 需要，E10-D005）；沒有則明文 HTTP，只給 dev。監聽預設 `:8080`，與 `make web-dev` 的 proxy 一致（E10-D029）。

console（`web/`）呼叫 API 用 E10-04 產的 openapi-fetch client（`web/src/lib/api.ts`，型別在 `web/src/lib/gen/api/v1.ts`）。樣式是 Svelte 元件的 scoped CSS，加上一份全域 CSS 變數，沒有 UI 框架（E10-D074）。即時更新用瀏覽器原生 `EventSource` 連 `GET /api/v1/events`：cookie 自動帶上，斷線由瀏覽器重連並送 `Last-Event-ID`，收到 `reset` 就重新 list（E10-D075、E10-D005）。每一頁至多一條，只訂該頁需要的 topic（E10-D107）。任何 API 回 401，console 導向 `/login`，登入成功後回到原來的頁；登入頁只有 token 欄位，token 不寫入 localStorage，認證只靠 cookie（E10-D076、E10-D009）。跨頁狀態是 Svelte 5 runes 的一個全域 store，只放登入使用者、region 可用性、`controller_absent`（E10-D077、E10-D014）。`vite dev` 把 `/api` proxy 到 `localhost:8080`，瀏覽器眼中 cookie 同源，與正式環境由 API 同源提供 console 一致（E10-D078、E10-D009、E10-D028）。CI 跑 `svelte-check` 與 console 的 build；不在這裡跑 vitest 或 Playwright，端到端驗收是 #324（E10-D079）。

策略頁列出目錄裡的策略，連到版本的 tag、digest 與 manifest（`name`、`repository`、`digest`、`tags`、`runtime`、`os`、`arch`、`sdk`、`contracts`、`size_bytes`）。網址沒有 `ref` 時改到 `latest`，沒有 `latest` 就用第一個 tag（E10-D058）。建立 session 在 `/sessions/new`。必填 `name` 與 `sts_instance`（E10-D032：使用者指定，且必須是 topology 裡的 STS）。再選策略與版本。params 是 JSON 文字框：console 只檢查是合法 JSON，原樣送出，不把文字解析成 JS 值再序列化（E10-D033、E10-D086）；空白省略。manifest 有預設 params 時預填；目前的 `StrategyVersion` 沒有這個欄位，所以文字框是空的（E10-D086、§8.2）。feeds 是可增刪的列，`feed` 與 `md_instance` 自由輸入，console 不比對清單，由 API 驗證（E10-D087）。帳號從 `GET /accounts` 下拉。algo_id 由使用者在本 session 自行宣告（1–1023，可增刪），換帳號就清空。帳號不存 `algo_ids`，不新增每帳號的 algo 清單（E10-D088、E06-D002、E10-D012）。重啟模式省略則是 `on_failure`；`max_restarts` 或 `window_s` 為 0 時取部署預設 5 次、300 秒（E09-D019）。逾時與限制的空欄省略，0 用平台預設：`deps_timeout_s` 60 秒（E13-D002），`start_timeout_s` 60 秒、`grace_ms` 10 秒、`recv_window_ms`、`hook_timeout_s`、`volume_mb` 取 STS 的部署設定（E09-D018、E08-D003）。每次打開精靈產生一個 UUID 當 `Idempotency-Key`，送出失敗重試沿用同一個（E10-D089、E10-D068）。API 錯誤的 JSON Pointer 對到欄位；對不到的放表單頂端（E10-D090）。`/sessions` 列現存 session 的狀態、策略、帳號、建立者、時間，SSE 即時更新；`/session-records` 歷史不在本票。建立成功後導向該 session 的詳細頁（E10-D091）。

帳號頁 `/accounts` 列出現有帳號的種類、TD instance、`referenced_by`。`referenced_by` 的 session 連到 `/sessions/{id}`，`api_id` 連到 `/accounts/{api_id}`（E10-D103）。建立在 `/accounts/new`。種類選單來自 `GET /account-kinds`，順序與 API 相同。`supported` 為 false 的項目不可選、顯示為灰色，原因寫在項目上：`kind_unsupported`，並附上 `trades`。`supported` 為 false 是因為該種類交易的 venue 不是恰好一個（E06-D014、E10-D011、E10-D098）。`td_instance` 是自由輸入，console 不呼叫 `/instances`，由 API 依 E10-D065 驗證（E10-D099）。選了種類之後，表單只列出該種類的 `key_types`；`passphrase` 為 true 才出現 passphrase。更新在 `/accounts/{api_id}`：`kind` 與 `td_instance` 只顯示、不能改。憑證區預設收起，勾「更換憑證」才展開；不勾則請求不帶 `credentials`，沿用原密文（E10-D100、E10-D062）。key type 為 `ed25519` 時，secret 是多行文字框，換行原樣保留，並可選一個 `.pem` 檔把檔案文字讀進這個框；自動填入與拼字檢查關閉。送出的 `credentials.secret` 是框裡的原樣文字，含換行，不另做轉換。HMAC 的 key、secret、passphrase 是 `type=password`；key 在兩種 key type 都是 password。送出當下把 key、secret、passphrase 從頁面狀態清掉，不寫入 localStorage、sessionStorage 或其他儲存；進行中的 `.pem` 讀入若在清除之後才完成就丟棄。成功或失敗的畫面都不回顯密鑰（E10-D101）。建立成功後導向該帳號頁。刪除先確認。409 `referenced` 重新讀取帳號並列出 `referenced_by` 的 session。409 `conflict` 重新載入帳號後再確認一次，不自動刪（E10-D102）。欄位錯誤的 JSON Pointer 對到欄位，對不到的放表單頂端（E10-D090）。


Session 詳細頁 `/sessions/{id}` 顯示 phase、incarnation、`end_reason`、狀態原因，以及各需求項目的種類、對象、provider、狀態與原因（`REJECTED` 的原因在這一欄，未知 ticker 因此看得到）。log 顯示 `LogLocation` 的主機、stdout 與 stderr 的完整路徑，各附複製按鈕。STS instance 的 `AgentRecord.last_known` 為真時，log 加註最後已知（E10-D105、E10-D096）。`state_out` 只顯示 digest，附複製，不顯示其他欄位，也不提供下載（E10-D106）。結束按鈕先確認。`spec.desired_state` 已是 `ended`，或 phase 是 `done`、`failed`（terminal，W5）時不顯示按鈕。確認後呼叫 `POST /sessions/{id}/end`（E10-D104）。這一頁一條 EventSource：`sessions` 帶上 `session_id`，以及 `instances`（最後已知值在 STS 的 `AgentRecord` 上，不在 `LogLocation`）。收到 `reset` 重新讀這筆 session。

Agents 頁 `/agents` 依平面 `sts`、`md`、`td` 分組。每組列出 instance 的 `AgentRecord` 與 worker；`last_known` 的紀錄加標記。controller 另列 `ControllerStatus` 的 `AgentRecord`（這份回應沒有 worker）。`controller_absent` 與 `unavailable_regions` 用版面上方既有的橫幅（E10-D108、E10-D077）。這一頁一條 EventSource，只訂 `instances`。收到 `reset` 重新 list。worker 來自 `GET /instances/{name}`，列表本身沒有 worker。

Market 頁 `/market` 唯讀列出 MD instance，以及各 instance 上 kind 為 `feed` 的需求狀態與原因。沒有搜尋、沒有篩選、沒有寫入（E10-D109）。feed 狀態同樣來自 `GET /instances/{name}`。這一頁一條 EventSource，只訂 `instances`。收到 `reset` 重新 list。

總覽同時要 session 與 instance，所以一條 EventSource 訂這兩個 topic。Session 列表只訂 `sessions`。沒有這些資料的頁不開連線（E10-D107）。收到 `reset` 由 `web/src/lib/live.svelte.ts` 重新 list。

`cmd/mftiko` 只對 API 說話，用 `contract/gen/api/v1` 的 Go client（E11-D001、E10-D003）。`mftiko-pack` 與 `mftiko-seal` 維持獨立工具。命令列是標準庫 `flag`，分派 `mftiko <資源> <動作>`，另有單字命令 `login`、`whoami`、`help`；不加 cobra，一期不做 shell 補全（E11-D002）。設定檔是 `~/.config/mftiko/config.yaml`，可以有多組具名 context。`--context` 與 `MFTIKO_CONTEXT` 選擇；兩個都設時必須相同，都沒設就拒絕，不挑預設 context。檔案權限寬於 0600 就拒絕讀取。token 來自 `MFTIKO_TOKEN` 或該 context，環境變數優先。`login` 例外：token 只從 stdin 讀，`GET /auth/me` 成功才寫進所選 context，失敗不改檔。token 不是旗標，也不出現在輸出與錯誤訊息。`whoami` 只呼叫 `GET /auth/me`。認證只用 `Authorization: Bearer`，不使用 cookie，也不呼叫 `POST /auth/login`（E11-D003、E11-D004、E11-D006）。context 的 `url` 是這個 client 的 server，路徑是 `/api/v1`。TLS 預設驗證；`ca_file` 的 PEM 是唯一信任根，仍檢查主機名。明文 HTTP 要在該 context 設 `insecure_http: true`，否則拒絕（E11-D005）。成功 exit 0，用法與設定錯誤 exit 1，網路錯誤 exit 2。API problem 的 `code` 對到固定 exit code：`validation_failed` 10、`unauthenticated` 11、`not_found` 12、`conflict` 13、`not_terminal` 14、`referenced` 15、`algo_overlap` 16、`region_unavailable` 17、`storage_unavailable` 18、`registry_unavailable` 19、`internal` 20、`invalid_artifact` 21。錯誤輸出同一行寫名稱與號碼，接著 `detail`、每條 `errors[].path`，以及回應的 `X-Request-Id`。exit code 看 body 的 `code`，不看 HTTP status（E11-D007、E11-D008）。預設是表格，時間用本地時區；`-o json` 原樣寫出 API 的 JSON。HTTP 逾時預設 30 秒，`--timeout` 可改。`User-Agent` 是 `mftiko-cli/<版本>`。沒有 body 的請求不補 `Content-Type`（E10-D110）。刪除類命令在 TTY 上詢問，提示 `[y/N]`，只接受 `y` 或 `Y`；`--yes` 在 TTY 上不跳過詢問。非 TTY 沒有 `--yes` 就拒絕（exit 1），不讀 stdin。確認函式給 E11-04、E11-06 用（E11-D009）。唯讀查詢是 `mftiko strategy list`、`mftiko strategy versions <id>`、`mftiko strategy show <id> <ref>`、`mftiko instance list`、`mftiko instance show <name>`、`mftiko account-kind list`（E11-D010–E11-D013）。strategy list 與 versions 預設自動翻完全部頁，每一頁請求的 limit 是 1000；`--limit N` 限制總筆數；`--cursor` 只取那一頁。自動翻頁的 `-o json` 合併成單一 `{"items":[...]}`，`unavailable_regions` 取各頁聯集，不帶 `next_cursor`；指定 `--cursor` 時照 E11-D008 原樣輸出該頁。有 `unavailable_regions` 或 `controller_absent` 時 stderr 警告一次，並列出缺少的 region，exit code 不變。單一資源 503 是 `region_unavailable`（exit 17）。`registry_unavailable`（exit 19）的訊息明寫為 registry 連線問題。strategy list 顯示 latest 的 tag 與 digest 前 7 碼（不含 `sha256:` 前綴）；versions 在 digest 等於 latest 的列標 `latest`；show 印完整 digest。instance 的表格含平面、agent 是否在、以及 `last_known`。account-kind 的表格含 `trades`、`key_types`、`passphrase`、`supported`。

`mftiko account list`、`show <api_id>`、`create`、`update <api_id>`、`delete <api_id>` 管理交易帳號（E11-D014–E11-D018）。憑證只從 `-key-file`、`-secret-file`、`-passphrase-file` 讀，旗標值是路徑；`-` 表示 stdin，最多一個。TTY 上沒給的必要檔案改為不回顯的互動輸入：HMAC 的 key、secret、passphrase 各讀一行，ed25519 的 secret 讀到 EOF。ed25519 的 secret 原樣送出（含換行）；HMAC 的值只去掉結尾一個換行，CR LF 算一個。`key_types` 只有一種時用那一種；多於一種時 `-key-type` 必須是其中之一，這個值不是憑證。送出前讀 `/account-kinds`。`supported` 為 false，或種類不在清單裡，client 不送出，exit 10，訊息與 API 的 400 相同（`/kind` 的 reason 是 `is not accepted`）。必要欄位依 `key_types` 與 `passphrase`：passphrase 為 true 則必填，為 false 則不能帶，key 與 secret 不可為空。`account create` 的 `-name`、`-kind`、`-td-instance` 必填。`-td-instance` 先對上 instance 名稱文法（`^[a-z][a-z0-9-]{0,63}$`），再讀 `/instances/{name}`；不是 td 平面、不在 topology，或名稱不合法，client 不送出，訊息與 API 的 400 相同（`is not a td instance`、`is not in the topology`、`is not an instance name`）。`account update` 先讀 `revision`，再以 body 的 `revision` CAS。給了任一憑證檔就換成完整的一組；一組都沒給就不送 `credentials`，沿用原密文（E10-D062）。單獨的 `-key-type` 是用法錯誤。409 `conflict` 不自動重試，exit 13，stderr 提示重新執行。`account delete` 帶讀到的 `revision`，確認用 E11-D009；409 `referenced` 列出 `referenced_by` 的 session，exit 15。stdout、stderr、錯誤與 `-v` 的 `X-Request-Id` 都不含憑證。`account list` 沿用 E11-D010–E11-D012：預設每頁 `limit=1000` 並翻到沒有 `next_cursor`；`-limit N` 限制總筆數；`-cursor` 只取該頁，`-o json` 原樣寫出。自動翻頁的 `-o json` 合併成單一物件，`items` 串接，`freshness.unavailable_regions` 是各頁聯集，`freshness.controller_absent` 是各頁的或，不帶 `next_cursor`。有 `unavailable_regions` 或 `controller_absent` 時 stderr 只警告一次（`mftiko: warning: unavailable regions: <名，排序後以逗號空格隔開>`、`mftiko: warning: controller absent`），exit code 不變。

`mftiko session list`、`show <id>`、`end <id>`、`delete <id>`，以及 `mftiko record list`、`show <record_id>`，讀取與操作 session（E11-D024–E11-D028）。`session list` 呼叫 `GET /sessions`。篩選旗標是 `-phase`（可重複）、`-active`（`true` 或 `false`）、`-sts-instance`。分頁沿用 E11-D010–E11-D012：預設每頁 `limit=1000` 並翻到沒有 `next_cursor`；`-limit N` 限制總筆數；`-cursor` 只取該頁，`-o json` 原樣寫出。自動翻頁的 `-o json` 合併成單一 `{"items","freshness"}`，`unavailable_regions` 取各頁聯集，`controller_absent` 是各頁的或，不帶 `next_cursor`。有 `unavailable_regions` 或 `controller_absent` 時 stderr 只警告一次，exit code 不變。`session show` 呼叫 `GET /sessions/{id}`。表格依序是 spec 摘要、狀態、需求項目、dependency Condition、結束資訊、log 位置。spec 摘要印策略的 `id`（回應有才印）、名稱、digest 前 7 碼（不含 `sha256:` 前綴）、STS instance、`created_at`（本地時間，E11-D008）、`created_by`。不反查 registry。`-o json` 保留完整 digest。狀態印 `phase`、`reason`、重啟次數。需求項目每項印 ack 狀態；`rejected`、`dead` 標成 `REJECTED`、`DEAD` 並附原因；`silent` 照 E10-D036 顯示。結束資訊印 `outcome`、exit code、signal。log 位置印 `<sts_instance> <host>:<完整路徑>`（stdout 與 stderr 各一），並印 `LogLocation` 的主機。STS instance 的 `AgentRecord.last_known` 為真時註明「最後已知，agent 目前不在線」。一期不讀 log 內容（E10-D015）。`-o json` 原樣寫出 session 的 body，不另外讀 instance。`session end` 呼叫 `POST /sessions/{id}/end`，確認用 E11-D009：TTY 詢問 y/N，非 TTY 須 `-yes`；已結束的照樣成功。`session delete` 呼叫 `DELETE /sessions/{id}`，確認同樣用 E11-D009；非 terminal 回 `not_terminal` 時 exit 14，並提示先執行 `session end`。`record list` 呼叫 `GET /session-records`，篩選是 `-session-id`、`-strategy-id`、`-created-by`、`-created-after`、`-created-before`。時間接受 RFC 3339，或 `YYYY-MM-DD`（本地時區午夜），不收相對時間。分頁照 E11-D010；自動翻頁的 `-o json` 合併成 `{"items":[...]}`，不帶 `next_cursor`。`record show` 呼叫 `GET /session-records/{record_id}`。spec 在 session 刪除之後仍印得出來（E10-D097）。

## 1. 版本與相容

shim 與 worker 會比 agent 活得久（E01-D003），所以**版本偏差是常態**，沒有全域的 `pv`。

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
| A | 不協商。路徑帶 major（`/api/v1`），同一個 major 只做相容擴充，讀取端忽略不認得的欄位；不相容時開新路徑 | 目前的 major |

編碼一律是 protobuf，理由是 `buf breaking` 能機械地檢查相容性。唯一的例外是 `md.f`：protobuf 前面加一段由版本決定長度的二進位 envelope（v1 是 40 bytes），給 SDK 的 prehook 不解碼就能讀（§7.1、E08-D003、E04-D005–E04-D011）。API 的契約 A 不屬於平台內部，是 OpenAPI 描述的 JSON，相容性以 `oasdiff breaking` 檢查（E10-D002）。定案前（E10-20，E10-D018、E10-D046）CI 照跑，印出報告但不擋；比較的 base 沒有這份規格就略過。E10-20 起不相容的改動會擋。

`cmd/mftiko` 是契約 A 的客戶端，不是另一份契約（E11-D001）。exit code、表格、分頁、部分可用的警告、設定檔、帳號命令、session 與 record 命令是客戶端的慣例（E11-D003–E11-D028），不改 `contract/openapi/api/v1.yaml`，也不改 `info.version`。

**草案與定案（DECISIONS E00-D007）。** 上面的相容規則只約束定案的契約。草案可以自由改：`buf.yaml` 的 `breaking.ignore` 列出草案的檔案，CI 不擋；定案的 EPIC 把自己的檔案移出清單，寫進完成標準。`sym.proto` 已定案（E2-10）。`mdctl.proto`、`md.proto`、`read.proto` 已定案（E4-12，E04-D060）。`read.proto` 依 E08-D065 曾回到草案，E08-D066 的後續在 #241 合併後再次定案，從 `breaking.ignore` 移出。`md.proto` 的變更是純新增，維持定案。`tdctl.proto` 已定案（E6-15，E06-D029）。`td.proto` 在 E6-15 定案之後，依 E06-D033 曾回到草案。E06-D034 的後續在 #242 合併後再次定案，從 `breaking.ignore` 移出。`client_order_id` 的版面不變，仍是定案。`state.proto`、`session.proto`、`stsctl.proto` 與 §8 的 artifact manifest 已定案（E9-12，E09-D027）；三個 package 的 `selector.proto` 留在草案到 E5（E09-D091）。manifest 不是 proto，相容規則見 §8.2（E09-D093）。C3 的 `shim.proto` 與 C4 的 `worker.proto`、`common.proto` 已定案（E09-D094）。exchange 的 adaptor 介面已定案（E3-17）；它是 Go 介面，沒有 proto，`buf breaking` 不檢查它。其餘仍是草案。

| 契約 | 檔案 | 兩端 | 狀態 | 定案 |
|---|---|---|---|---|
| E | `state.proto` | API server、controller、三種 agent | 定案 | agent 紀錄在 E1；需求、指派、ack 在 E9 |
| E、C5 | `session.proto` | API server、controller、STS agent、session worker | 定案 | E9 |
| C3 | `shim.proto` | agent ⇄ shim | 定案 | E1 |
| C4 | `worker.proto`、`common.proto` | shim ⇄ worker | 定案 | E1 |
| C5 | `mdctl.proto` | MD agent ⇄ conn worker | 定案 | E4 |
| C5 | `tdctl.proto` | TD agent ⇄ account worker | 定案 | E6 |
| C5 | `stsctl.proto` | STS agent ⇄ session worker | 定案 | E8、E9 |
| E、C5 | `session/v1/selector.proto`、`state/v1/selector.proto`、`stsctl/v1/selector.proto` | API server、controller、MD agent、STS agent、session worker | 草案 | E5（DECISIONS E09-D091） |
| C6 | `md.proto`、envelope（§7.1） | conn worker ⇄ session worker | 定案 | E4 |
| C6 | `read.proto` | conn worker、md.fetch ⇄ session worker | 定案 | E4、E8 |
| C6 | `td.proto` | account worker ⇄ session worker | 定案 | E6 |
| C6 | `client_order_id`（§2） | session worker ⇄ TD、落庫的歸屬 | 定案 | E6 |
| C6 | `sym.proto` | SYM ⇄ STS、MD、TD | 定案 | E2 |
| §8 | artifact manifest（§8.2、§8.4 的 config） | `mftiko-pack`、CI、STS agent ⇄ 三種 agent | 定案 | E9（E09-D093） |
| A | `contract/openapi/api/v1.yaml` | console、CLI ⇄ API server | 定案（CI 跑 lint 與 `oasdiff breaking`，不相容的改動擋下，E10-D116） | E10（E10-D018） |
| — | exchange 的 adaptor 介面（DECISIONS E00-D006、E03-D002、E03-D003、E04-D020：sans-I/O 的 codec，REST 是 `Call`，`Used.ResetAt` 是計數器歸零時間，傳輸由 worker 執行） | exchange ⇄ MD、TD、SYM | 定案 | E3 |

## 2. 識別碼與慣例

| 名稱 | 形式 | 由誰分配 |
|---|---|---|
| instance 名稱 | `^[a-z][a-z0-9-]{0,63}$`，慣例是 `{plane}-{region}-{n}`（`md-tw-1`），拆 region 後仍然全域唯一 | 部署設定 |
| `session_id` | 6 個小寫 hex，隨機產生。存活與保留期內唯一：W5 刪掉 spec 之後可能被新 session 重用，只影響以它為鍵的歷史（DECISIONS E10-D007） | API |
| `api_id` | `int64`，1 到 2⁵³−1 隨機產生（JavaScript 的 number 表示得精確），所有 TD instance 之間唯一（DECISIONS E10-D008） | API |
| worker key | `sts/<session_id>`、`td/<api_id>`、`md/<venue>/<route>/<n>`（DECISIONS E04-D003） | 各 agent |
| incarnation | `uint32`，每個 worker key 從 1 起各自遞增；只保證遞增，可以跳號。存在 `/{plane}/{inst}/incarnation/{h}`，spawn 前取號（§3.2 W6） | 各 agent |
| agent epoch | `int64`，持久化在 `/{plane}/{inst}/epoch`，agent 每次當選加 1（§3.2）。不用 etcd revision：不同 cluster 的 revision 不能比較，cluster 重建時 revision 也會從頭開始 | agent |
| `Version` | `(epoch, seq)`，字典序比較。agent 推給 worker 的指令都帶它；`seq` 在同一個 agent 進程內遞增 | agent |
| generation | session spec key 的 `mod_revision`（§3.2），只在該 key 所在的 cluster 內比較 | etcd |
| UniversalTicker | `<Venue>_<Category>_<SYMBOL>`，照抄 mftik（E02-D001）。給人寫設定與 spec 的字串（E02-D003）。系統內部不拿它當商品身分，載入 spec 時解成 UID。Venue 是 CamelCase（`BinanceUM`、`Okx`）；Category 是 `Spot`、`Perp`、`Inverse`、`Future`、`Option`；SYMBOL 是 base＋quote 不加分隔，到期合約加 `-YYMMDD`，期權再加 `-STRIKE-C`／`-P`。strike 先依 `spell_strike` 正規化再拼（E02-D003）：`6400.0` 與 `6400` 是同一個 ticker，小數點寫成 `D`（`6D4`）。不含 `_` 以外的分隔、不含 `.` | SYM（由 listing 推得） |
| symbol UID | SYM 分配的隨機整數，唯一且永不重用，不是單調序號（E02-D003）。活躍表與終結表的主鍵，兩張表合在一起唯一，跟列一起持久化：重啟與重載都不改它。搬到終結表時不變；同一個 ticker 下架再上市是新的 UID；碰撞時 SYM 重試。契約型別 `uint32`，範圍 `1..4294967295`（`1..2^32-1`），0 非法。Postgres 欄位是 `bigint`，`CHECK (uid BETWEEN 1 AND 4294967295)`（§10 P13） | SYM |
| venue | 市場，UniversalTicker 的第一段，CamelCase：`Binance`、`BinanceUM`、`BinanceCM`、`BinanceEO`、`Okx`、`Bybit`。一條連線的公開行情、一組商品（E04-D003） | 程式（`internal/exchange`） |
| 帳號種類 | api_id 是什麼：收哪種憑證、可以交易哪些 venue 的商品：`Binance`、`BinanceUM`、`BinanceCM`、`BinanceEO`、`BinancePM`、`Okx`、`Bybit`。和 venue 是兩個獨立的 enum，名稱可能相同，不可互查。`BinancePM` 只是帳號種類，交易 `Binance`、`BinanceUM`、`BinanceCM` 的商品，依 ticker 的 venue 路由，沒有 `BinancePM_*` 這種 ticker（E03-D001） | 程式（`internal/exchange`） |
| exch_ticker | 交易所自己的寫法，只出現在 adapter 與 SYM。反查要帶 venue 與 category：一個帳號種類可能交易多個 venue（BinancePM），統一帳戶的 venue 上同一個寫法可能是兩個商品（Bybit 現貨與 linear 的 `BTCUSDT`） | 交易所 |
| topic | `bestquote`、`trade`、`aggtrade`、`orderbook_delta`、`ticker`、`liquidation`、`funding_rate`、`open_interest`、`greeks`、`mark_price`、`kline_<interval>`（interval 是 `1m`、`4h`、`1mo` 這類正規寫法）。前面幾個沿用 mftik 的名稱（E02-D002） | — |
| feed key | `<topic>.<UniversalTicker>`。spec 裡寫這個字串；載入時 ticker 才解成 UID（E02-D003）。NATS subject 是 `md.v1.f.{inst}.{topic}.{ticker}`（E04-D055）：用人讀的名字，不用 UID | — |
| atom id | `<venue>:<route>:<channel>`，只出現在 MD 內部與 `mdctl`。channel 是交易所的訂閱參數，逐字照抄，可能含 `:` 與 `.`，所以只切前兩個 `:`。route 是身分的一部分：同一個 channel 字串可能出現在不同 route（Bybit 現貨與 linear 的 `publicTrade.BTCUSDT`）（E02-D002） | MD adapter（由解析後的商品推得） |
| route | 同一個 venue 裡 atom 所屬的 WS 路徑，一個小寫單字，例如 OKX 的 `public`、`business`，Bybit 的 `spot`、`linear`；只有一條路徑的 venue 是 `default`（E04-D003） | MD adapter |
| `algo_id` | `uint32`，1–1023（10 bits），0 保留。訂單的命名空間，跨 session 不變；一個 session 可以有多個。只需在同一個帳號內唯一；同一個帳號上同時只屬於一個非 terminal 的 session：API 建立時以副本做 best-effort 檢查，STS agent 檢查它自己 instance 上的 session，其餘由使用者保證（DECISIONS E06-D002、E06-D016、E09-D017、E10-D012） | spec（`AccountDecl.algo_ids`） |
| `client_order_id` | uint64 `ver(4) \| algo(10) \| ts_ms(40) \| seq(10)`，以十進位字串傳。`ts_ms` 自 2026-01-01 UTC 起算，同一個進程內單調；時鐘倒退沿用上次的值。`seq` 一個進程一顆計數器，所有 algo、所有帳號共用，每毫秒 1024 個（0..1023），用完往前借毫秒且不設上限，所以 `ts_ms` 可能超前牆鐘（實務上要每秒超過一百萬筆，E08-D011、E08-D012）；`ver` 只用 1–7，最高位為 0（E06-D002） | session worker |

- 十進位數（價格、數量、金額、費率、tick 與 step 等）一律是 `double`，Go 裡是 `float64`，不用字串。解析、寫出、比較的規則見 DECISIONS E00-D008 與 `contract/wire/decimal`。缺值是不設的 optional 欄位，不送 NaN。時間一律是 `int64` unix 奈秒，欄位名以 `_ns` 結尾。
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

// worker 宣告的結束方式（E01-D007）。
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

// 交易共用的 enum：行情的成交與強平、TD 的訂單、SDK 都用這一份（DECISIONS E00-D006）。
// exchange 的 adaptor 另有同名型別，數值與這一份以及 td.v1 的 OrderStatus 相同，worker 以型別轉換映射（E03-D002）。
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

**cluster 與路由（E00-D005）。** 每個 region 一個 etcd cluster。所有存取經過路由函數 `clusterFor(instance)`，依 key 歸屬的 instance 所在的 region 決定連哪個 cluster。歸屬的規則是「會依據這個 key 行動的那個 agent」：

| key | 歸屬 |
|---|---|
| session spec、STS 的指派、status | STS 所在的 region |
| 需求項目、ack／nack | provider（MD／TD）所在的 region。TW 的 session 要 JP 的行情，controller 就寫進 JP 的 cluster |
| controller 的 election、epoch、agent | controller 的 home region（部署設定，DECISIONS E09-D007） |
| controller 的 writer | 每一個 controller 會寫入的 cluster 各一份 |
| 其他（election、epoch、writer、agent、worker、incarnation、selector、placement、帳號） | 該 instance 所在的 region |

只有 controller 和 API server 會連多個 cluster；三種 agent 都只連自己 region 的。下表的 key 路徑在每個 cluster 裡都一樣，差別只在存在哪個 cluster。

### 3.1 key 結構

根是 `/mftiko/v1`。表中「綁 lease」指 key 綁在哪個 etcd lease 上：該 lease 過期，key 由 etcd 自動刪除。只有一種 lease，就是 W2 參選用的選主 session（W7）。

| key | 值 | 寫入者 | watch 者 | 綁 lease |
|---|---|---|---|---|
| `/sts/{inst}/spec/{session_id}` | `SessionSpec` | API server | controller、STS agent | — |
| `/sts/{inst}/demand/{session_id}` | `SessionDemand`（STS 的指派） | controller | STS agent | — |
| `/sts/{inst}/status/{session_id}` | `SessionStatus` | STS agent；etcd 角色另給 `api` 寫入權（E10-D040） | controller、API server | — |
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

**`api` 角色（E10-D040、E10-D041）。** 角色由部署的 instance 清單產生（`AuthTables`；`mftiko-etcd-roles` 印出同一份）。每個 cluster 的使用者 `api` 讀 `/mftiko/v1/` 全部，寫該 region 每個 STS instance 的 `spec/` 與 `status/`、每個 TD instance 的 `account/`。`demand/`、`ack/`、`agent`、`worker/`、`writer`，以及其他 instance 的前綴，etcd 拒絕。上表其餘列的寫入者與這個角色一致。

`status` 的寫入者仍是 STS agent。etcd RBAC 不分寫入與刪除：W5 要刪 `status/`，`api` 只能拿這個前綴的整個寫入權，所以 etcd 擋不住 `api` 寫 status。W1「status 只有 STS 寫」在 status 上只靠 API 程式守，不靠 etcd。這是刻意的缺口。正向測試記錄「`api` 寫得進 status」。不改成由 STS 清 status，那會推翻 E10-D017。測試補在 `internal/platform/state` 的 `TestRBACEnforced`，用 etcdtest 開 auth 的真 etcd 跑：正向是讀全部、寫 spec、寫 account、刪 status、寫 status；負向是上面那些被拒絕的寫入。

### 3.2 寫入規則

- **W1 單一寫入者。** 每個前綴只有表中的那一個寫入者。API server 不參選，它的寫入怎麼滿足 W1 見 W10。
  - etcd 開 auth 與 RBAC，由 etcd 自己強制這條：每個進程在它連的每個 cluster 有自己的使用者，agent 用 instance 名稱，controller 用 `controller`，API server 用 `api`。角色只給讀寫上表寫入者欄裡屬於它的 key（`status/` 的例外見 §3.1）：agent 只在自己 region 的 cluster 有使用者，讀自己 instance 的前綴、寫自己的 key，不寫任何 `demand/`；controller 讀全部，寫 `/ctl/controller/` 與該 region 每個 agent instance 的 `demand/`；API server 讀全部，寫該 region 的 `spec/`、`account/`，並依 W5 刪除 `status/`。etcd 給的是 `status/` 整個前綴的寫入權，擋不住寫入；這個缺口是刻意的（§3.1、E10-D040），W1 在 status 上只靠 API 程式守。
  - 角色表由部署的 instance 清單產生（`mftiko-etcd-roles`，實作是 `internal/platform/state` 的 `AuthTables`），帳密的存放與輪換、套用角色表在部署時做。
  - token 是 JWT。過期，或使用者與角色變更之後（auth revision 變了），client 自動重新認證，進行中的 watch 與 lease keepalive 不受影響。
- **W2 寫入以 writer key 為條件。** agent 與 controller 都照這個流程；controller 的 key 在 `/ctl/controller/` 底下，在 home region 參選。
  1. agent 在自己 region 的 cluster 參選 `/{plane}/{inst}/election`（etcd `concurrency.Election`）。
  2. 當選後以 txn 把 `/{plane}/{inst}/epoch` 加 1，得到 epoch E。這也是 C3 `Attach` 帶的 epoch。
  3. 在每一個要寫入的 cluster，以 txn 把 `/{plane}/{inst}/writer` 改成 E，條件是原值小於 E 或不存在（值是 20 位補零的十進位，位元組比較就等於數值比較），並記下這次寫入的 `mod_revision` R。
  4. 之後在該 cluster 的每一次寫入都是 txn，條件是 writer key 的 `mod_revision` 仍然等於 R。被取代的舊 agent 寫不進去。

  fencing 因此只依賴「被寫入的那個 cluster」裡的 key，跨 cluster 寫入時不需要跨 cluster 的 txn。

  controller 不等所有 cluster 都完成第 3 步才開始：每個 region 在第一次寫入之前才做，一個 region 連不上不擋其他 region。條件不變：一個 cluster 在第 3 步之前不寫入。任何一個 region 發現 writer key 已經是更大的 epoch，term 就結束，controller 停下所有動作並退出（DECISIONS E13-D005）。兩個 controller 的 home region 設定不一致時，每座 cluster 只由 home 在那裡的一方寫入，另一方寫到那裡就被取代（DECISIONS E13-D038）。舊 leader 在 server 端 lease 過期之後、自己察覺之前，仍寫得進新 leader 還沒 fence 的 region，時間窗小於一個 TTL（DECISIONS E13-D039，#277 關掉）。
- **W3 generation 就是 revision。** session spec 的 generation 是該 key 的 `mod_revision`。`SessionStatus.observed_generation` 是 STS agent 處理過的那個 `mod_revision`。
- **W4 list ＋ watch。** 讀取端先 list 前綴，再從該 revision（含）開始 watch，revision 不大於副本的事件直接丟掉。效果等同從加 1 開始，但 watch 的起點不會超過 cluster 目前的 revision；起點超過的話，etcd 會停發整條連線的 progress notification，cluster 安靜時就一直停著。收到 compacted 錯誤就重新 list。處理邏輯是 level-triggered 的。實作是 `internal/platform/state` 的 `Mirror`。API server 每個 cluster 一個 `Mirror`，前綴是整個 `/mftiko/v1/`，不拆成多個前綴（DECISIONS E10-D025）。SSE 不走這個 Mirror：每條連線對每個 region 另開一條 watch，起點是 `Last-Event-ID` 裡該 region 的 revision 加 1；沒有帶到的 region 先 list，再從那個 revision 加 1 watch。compacted 送 `reset`（DECISIONS E10-D094）。Mirror 沒有歷史，接不上已經送出的事件。
- **W5 terminal 是吸收態。** session 進入 `DONE` 或 `FAILED` 之後，spec 再怎麼改都沒有效果。API 在保留期之後刪掉 spec 和 status。保留期預設 7 天，從 terminal 的 `SessionStatus.updated_at_ns` 起算，是 API 的部署設定（DECISIONS E10-D017）。掃描在每個副本既有的 reconcile 裡，每 30 秒以及 mirror 變動時，不另開迴圈（E10-D083）。`updated_at_ns` 比現在減去保留期更早才刪；沒有這個時間的 status 還沒開始計。先把最後的 status、結束時間、`state_out` digest 寫進 `session_records` 並關閉那一列，成功才刪 etcd（E10-D031）。Postgres 不可用則這輪不刪（E10-D080）。etcd 有 session 而 Postgres 沒有列時不刪、不補列，只記 error log 與計數器（E10-D081）。有 spec 時一個 txn 同時刪 spec 與 status，條件是兩者的 `mod_revision`；沒有 spec 的 status（STS 推完 `state_out` 寫回的）只以該 status 的 `mod_revision` 刪（E10-D082、E10-D042）。使用者也可以 `DELETE /api/v1/sessions/{session_id}` 立即刪除：非 terminal 回 409 `not_terminal`，成功與重複刪除回 204（E10-D084）。spec 被刪之後，STS agent 才清掉主機上這個 session 的 log 與目錄（§8.4、DECISIONS E10-D016）。
- **W6 incarnation 先取號再 spawn（DECISIONS E01-D006）。**
  1. agent 當選後 list `/{plane}/{inst}/incarnation/`，把每個 worker key 目前的號碼記在記憶體裡。這個前綴只有它寫，不需要 watch。
  2. spawn 前以一個 txn 取號。條件有兩個：writer key 的 `mod_revision` 仍是 R（W2）；incarnation key 的值仍是記憶體裡的 n，第一次 spawn 則是 `create_revision == 0`。成立就寫入 n＋1。
  3. txn 成功才 spawn，以 n＋1 當這個進程的 incarnation。writer 的條件不成立表示 agent 已被取代，不 spawn。incarnation key 的條件不成立表示記憶體裡的值錯了，重讀後重試。
  4. 取號之後 spawn 失敗，這個號碼就作廢，下次取新的。
  5. key 不刪：worker 停掉之後，同一個 worker key 還會再起來（MD 重新開到同一個連線編號、refcount 又回到 1 的 `api_id`）。刪掉的話它會從 1 重來，下游記得的號碼比它大，新進程的訊息會被當成遲到的舊訊息丟掉。唯一的例外是 STS：`session_id` 對所有歷史唯一，session 進入 terminal 之後同一個 key 不會再起來。STS agent 在 spec 不在、而且第一段清理已完成之後，於第二段刪掉這個 key（§8.4、DECISIONS E10-D016、E10-D020）。
  6. `SessionStatus.incarnation`、`WorkerStatus.worker.incarnation` 只是顯示，值從這裡來。
- **W7 agent 在不在，看 `AgentRecord`（DECISIONS E00-D004）。** agent 與 controller 都只有一個 etcd lease，就是 W2 參選用的選主 session。
  1. ack 與 `WorkerStatus` 不綁 lease，只有它的寫入者會刪它，所以「不存在」只有一種意思。
     - `WorkerStatus` 在 worker 確定不在之後才刪：`Release` 完成，或 shim 的 socket 與進程都已不存在。attach 失敗、逾時都不算，key 保留。
     - ack 在需求項目消失之後才刪。
  2. 只在值改變時寫：`WorkerStatus` 在階段改變時才寫，`Beat`、`Report` 不進 etcd。
  3. agent 每次當選，在第一次 reconcile 之前 list 自己的 `worker/` 與 `ack/`，和收養得到的 worker（§4.2）、目前的需求比對：有差異的重寫，多出來的照 1 的條件刪掉。和 controller 的 N5 是同一套做法。
  4. 比對完才寫 `AgentRecord`，綁在選主 session 的 lease 上。只有這一個寫入點，agent 的其他寫入碰不到它（實作是 `internal/platform/state` 的 `Term.PutRecord`）。同時寫上 `host`（`os.Hostname()`；取不到則空字串，不因此結束 term）與 `work_dir`（該行程的 `WORK_DIR`；controller 沒有 `WORK_DIR`，留空）（DECISIONS E10-D092）。term 一結束（session 斷了、被取代），session 就不再續約，`AgentRecord` 最晚在一個 TTL 之後消失；agent 正常停下時 revoke 這個 lease，立刻消失。`epoch` 說明是哪一個 term。
  5. 讀的一方把 ack、`WorkerStatus` 和寫入者的 `AgentRecord` 一起看：`AgentRecord` 在，值是新的（已經比對過）；不在，值是最後一次已知的狀態，可能已經過時。controller 對兩種情況的處理相同（§3.5）。instance 永久下線的程序見 §10 P11。
- **W8 需求過舊時保持現狀（DECISIONS E01-D009）。** agent 的 `demand/` 副本的 `LastContact` 超過門檻（預設 15 秒，部署設定）就是過舊。過舊時：
  1. 不依需求停 worker、不因為 spawn spec 變了而替換、不推 live config。恢復聯繫之後的下一輪才做。
  2. 重啟結束的 worker、對已初始化的 worker 送 `Start`，只在該平面的 Policy 允許時做（`KeepWhileStale`）。
  3. 不寫 `WorkerStatus`；恢復之後只寫有改變的（W7.2）。
  4. 收養（§4.2）、Release 已經結束且不再需要的 worker 照常。
- **W9 controller 和某個 region 斷線時保持現狀（DECISIONS E13-D006）。** controller 對每個 region 的 Mirror 看 `LastContact`，門檻與 W8 相同。過舊時：
  1. 寫入依 region 分開排隊，一個 region 寫不進去不擋其他 region。reconcile 把寫入交給佇列就繼續，不等結果；佇列裡同一個 key 只留最新的值（DECISIONS E13-D037）。過舊判定之前送出的寫入，在斷線期間卡在那個 region 的佇列裡，不擋 reconcile。
  2. 不做減法：不刪那裡的需求項目與 `SessionDemand`，不因為那裡的 ack 結束 session，也不因為那裡的 `READY` 放行 `start`（DECISIONS E13-D032）。STS 的 region 過舊時，那裡的 session 照 N4 繼續等。任一 provider region 過舊時，別的 region 的 `SessionDemand` 也先不刪（DECISIONS E13-D033）。
  3. `deps_timeout_s` 照常計時（N3）。
  4. 恢復聯繫之後的下一輪 reconcile 再對齊，只改有差異的（N6）。

  agent 自己和 etcd 斷線不是這一條：controller 看到的是它的 `AgentRecord` 消失，照 §3.5 處理。
- **W10 API server 的寫入（DECISIONS E10-D006）。** API server 不參選，可以多個副本，都用 etcd 使用者 `api`；W1 以角色為單位成立，副本之間的競態由 txn 的條件擋：
  1. 建立 spec、建立帳號：`create_revision == 0`。建立 spec 之前，先在 Postgres 的 `session_records` 寫一列 `pending`，寫入 etcd 之後標成 `created`（DECISIONS E10-D031）。
  2. 結束 session：只改 `desired_state`（DECISIONS E13-D003），條件是 spec 的 `mod_revision` 等於讀到的值。
  3. 更新帳號：條件是 `mod_revision` 等於呼叫端讀到的值。
  4. 刪除 session（W5）：先把最後的 status 寫進 `session_records` 並關閉那一列（DECISIONS E10-D031），再以一個 txn 刪 spec 與 status，條件是兩者的 `mod_revision` 都等於讀到的值，而讀到的 status 是 terminal。Postgres 不可用則不刪，立即刪除回 503 `storage_unavailable`（E10-D080）。沒有對應列則不刪（E10-D081）。只有 status 時，txn 只刪那一個 status key（E10-D082）。
  5. 刪除帳號：條件是 `mod_revision` 等於讀到的值；仍被非 terminal 的 spec 引用時不刪（DECISIONS E10-D011）。

  建立與更新帳號時，API 以該 TD instance 的公鑰、經 `internal/platform/sealbox` 封裝憑證，`api_id` 與 `kind` 一起封進去（DECISIONS E06-D013、E10-D062）。`credentials` 建立時必填，缺少回 400；更新省略則保留原密文。`key_type` 必須在該種類的 `key_types` 內，passphrase 依種類必須有或不能有，key 與 secret 不可為空，違反回 400，錯誤只說欄位，不向交易所驗證（E10-D064）。`td_instance` 必須在 topology、平面是 td，且 `td_keys` 有公鑰，否則回 400，缺公鑰要寫明（E10-D065）。`name` 依副本 best-effort 不重複，同名回 409（E10-D066）。讀取不回傳明文、密文或指紋（E10-D063）。

  API 的寫入只走 `state.APIWriter`，不經 `Term`：put 只限 STS instance 的 `spec/` 與 TD instance 的 `account/`，`status/` 只能刪。etcd RBAC 擋不住 `api` put status（§3.1），擋的是這一層（DECISIONS E10-D042）。

  條件不成立就回 409，由呼叫端重新讀取；結束 session 由伺服器端重試，不回 409（DECISIONS E10-D037）。etcd 連不上或逾時回 503（`region_unavailable`），這時寫入可能已經生效（DECISIONS E10-D042）。

  `session_id`、`api_id` 以亂數抽出，寫入前查 API 在可用 region 的副本（DECISIONS E10-D007、E10-D008、E10-D042）：
  - 任何 key 帶著這個 `session_id` 就換一個：spec，以及沒有 spec 的 status、`SessionDemand`、需求項目、ack。
  - 任何 TD instance 有這個 `api_id` 的帳號，就換一個。
  - 抽到佔用的 id 與第 1 條失敗都算一次，最多 5 次。
  - 要寫入的 region 不可用時直接回 503，不寫。

  建立可帶 `Idempotency-Key`，同 key 同 body 回同一個 session，同 key 不同 body 回 409，保留 24 小時（DECISIONS E10-D068）。`pending` 超過 2 分鐘且 etcd 沒有對應 spec 就標 `abandoned`，之後 spec 的 `create_revision` 與雜湊對上則改回同一列 `created`（DECISIONS E10-D069）。etcd 與 `session_records` 之間由每個副本的 reconcile 收斂：每 30 秒全量比對，mirror 有變動時也跑，寫入一律有條件的 upsert，不選主（DECISIONS E10-D031、E10-D070）。雜湊不同只記 error log 與計數器，不覆蓋任何一邊，只同步 `desired_state` 與 terminal 結局（DECISIONS E10-D071）。etcd 有 spec 而 Postgres 沒有列時只告警、不補列（DECISIONS E10-D072，修改 E10-D031）。W5 也在這個迴圈裡（E10-D083）。重刪時，有 spec 的要 `create_revision` 等於那一列記下的值，才是那一列的 session；沒有 spec 的 status 另以自己的 `mod_revision` 刪（DECISIONS E10-D042、E10-D082）。id 會重用（E10-D007）。

### 3.3 需求與 STS 的指派

需求項目與 STS 的指派都由 controller 寫，是宣告式的：不綁 etcd lease，存在的理由是「controller 決定它應該存在」，和 controller 活著與否無關（DECISIONS E09-D001、E09-D007）。

- **N1 由 session spec 推導。** 每個非 terminal 的 session，controller 寫：
  - 一個 `SessionDemand`，在 STS 的 cluster；
  - 每個 feed、每個 selector 一個 MD 的 `DemandEntry`，在 `FeedDecl.md_instance`、`SelectorDecl.md_instance` 的 cluster；
  - 每個帳號一個 TD 的 `DemandEntry`，在 `AccountDecl.td_instance` 的 cluster。

  需求只來自 spec，一期沒有執行中加減訂閱的介面（DECISIONS E09-D003）。spec 只有 `desired_state` 可以改；其他欄位改了，controller 不重新推導（DECISIONS E13-D003）。`SessionDemand` 已存在且 spec 的 revision 與其 `spec_revision` 不同時，以 etcd 裡現有的需求項目為準：不新增、不刪除，只記一次 log，不以 `invalid_spec` 結束（DECISIONS E13-D022）。只在推導出的值和 etcd 裡的不同時才寫，比較時不看 `SessionDemand.updated_at_ns` 與 `DemandEntry.spec_revision`（DECISIONS E13-D008、E13-D023）。尚無 `SessionDemand` 而 spec 的 `desired_state` 已是 `ENDED`：什麼都不寫（DECISIONS E13-D026）。

  spec 不合法（`md_instance`、`td_instance` 不在 topology 裡，或在 topology 裡但部署 JSON 的 `planes` 不是該平面；同一個 spec 有重複的 feed、selector 名稱或帳號）：只寫一筆 `desired = ENDED`、`end_reason = invalid_spec` 的 `SessionDemand`，不寫需求項目（DECISIONS E13-D004、E13-D027）。
- **N2 STS 的指派一開始就發。** `desired` 是 `RUNNING`，`start` 是 false：策略的 rootfs 就緒後 STS agent spawn 並送 `Init`，拉取中 phase 維持 `PENDING`（E09-D058）。`OnInit` 和需求的建立並行（DECISIONS E01-D007）。`bindings` 在推導時就列出每個項目的目標與提供的 instance；ack 的狀態隨後抄進去，還沒有 ack 時不填（DECISIONS E13-D024）。全部 `READY` 時把 `start` 設成 true，STS agent 才送 `Start`。過舊 region 的 `READY` 不算（DECISIONS E13-D032）：`bindings` 仍抄上那個 ack，但不因此放行 `start`。provider 的 region 尚未完成首次同步時，`bindings` 保留 etcd 裡現有的 status，不清成空值（DECISIONS E13-D037）。`start` 一旦是 true 就不改回 false；之後的 ack 變化照樣更新 `bindings`，直到 `desired` 變成 `ENDED`。`ENDED` 之後不再抄 ack（DECISIONS E13-D030）。STS agent 把 `bindings` 轉成 `stsctl.Bindings` 送給 worker（§6.1）。需求項目與 `SessionDemand` 同一輪各自進該 region 的佇列，不保證跨 region 的先後（DECISIONS E13-D025）。
- **N3 controller 結束 session 的方式是把 `desired` 改成 `ENDED`，並寫明 `end_reason`：** `ended_by_spec`（spec 的 `desired_state` 改成 `ENDED`，或 spec 在 terminal 之前被刪掉，DECISIONS E13-D003）、`deps_failed`（收到 nack，§3.5）、`deps_timeout`（`start` 之前，現在超過 `issued_at_ns + deps_timeout_s`；`deps_timeout_s` 是 0 時取 60 秒）、`invalid_spec`（N1）。`issued_at_ns` 在第一次寫指派時填，之後不改，controller 換手也不重新計時（DECISIONS E13-D002）。`desired` 變成 `ENDED` 之後不再改回 `RUNNING`，`end_reason` 保留第一次寫下的值，之後 ack 恢復或 spec 改回都不理會（DECISIONS E13-D028）。同一輪多個原因時 `ended_by_spec` > `deps_failed` > `deps_timeout`（DECISIONS E13-D029）。`deps_timeout` 的期限到了就跑一輪 reconcile，不等 60 秒全量（DECISIONS E13-D031）。STS agent 停掉 worker，寫入 terminal 的 `SessionStatus`。`SessionStatus` 只有 STS agent 寫（W1）：worker 自己結束、重啟次數用完、volume 超量（DECISIONS E09-D004、E08-D003）這些 terminal 也由它判定。寫下 terminal 的那一次，若當下 `SessionDemand` 還在，把它的 `bindings` 裡 `REJECTED` 與 `DEAD` 的項目各寫成一個 `Condition`：`type` 是 `dependency`，`status` 是 false，`reason` 是 ack 的 reason，`message` 是四個以單一空白分開的 token：`feed`、`account` 或 `selector`，然後是 feed key、十進位 `api_id` 或 selector 名稱，然後是 provider instance，最後是 `rejected` 或 `dead`（DECISIONS E10-D093、E10-D014）。`SessionDemand` 已經不在就不寫，不用先前記得的指派。API 把 `feed` 與 `account` 還原成 `Dependency`；`selector` 留在 status 上。
- **N4 刪除的順序。** controller 看到 `SessionStatus` 進入 terminal，且 `WorkerStatus` 確認 session worker 已經消失之後（`/sts/{inst}/worker/{h}` 不存在，`h = WorkerHash("sts/<session_id>")`，DECISIONS E13-D009），才刪掉這個 session 的 MD、TD 需求項目。STS 的 `AgentRecord` 不在時照最後的值：key 還在就一直等，不把 agent 不在當成 worker 已經消失（P11 歸 E12）。刪完要在 view 裡確認這些需求項目已不存在，才刪 `SessionDemand`；任一 provider region 過舊時 `SessionDemand` 跟著等，讓這個順序跨 region 成立（DECISIONS E13-D033）。STS 的 region 過舊時，那裡的 session 整段都等（W9、E13-D006）。controller 只刪需求項目與 `SessionDemand`，不刪 spec、`SessionStatus`、ack、`WorkerStatus`（DECISIONS E13-D035、W1）。平台不替策略撤單（DECISIONS E06-D002）。`SessionDemand` 不見了，STS agent 一律當成 `ENDED`。
- **N5 孤兒靠比對回收。** controller 每次當選、每次全量 resync（預設 60 秒，部署設定，DECISIONS E13-D010），都比對「應該存在的」與「實際存在的」。需求的 key 只帶 `session_id`，須查所有 STS instance。只有所有 STS region 都已同步且不過舊、確定沒有任何 instance 有這個 session 時，才當孤兒刪除（DECISIONS E13-D034）：任一 instance 的 view 裡有這筆 session（spec、status 或 `SessionDemand`）就不是孤兒；解不開的 spec、status 或 `SessionDemand` 也不當成沒有（E13-D021）。session 已經 terminal 的項目依 N4 刪，不等全量。過舊的 region 不做減法（W9）。
- **N6 controller 停機或輪換時，需求與指派原樣保留。** 新 leader 先 relist 再比對，只改有差異的項目，不先全部刪掉再重建。萬一刪掉又馬上加回，MD、TD 的 linger 會吸收（DECISIONS E09-D005）。
- **N7 provider 只讀。** MD、TD agent 不寫需求項目；STS agent 不寫 `SessionDemand`。
- **STS agent 只照指派 spawn（DECISIONS E09-D016）。** 有指派而且 `desired` 是 `RUNNING` 才 spawn；spec 只用來組 `InitPayload`。沒有 spec，或 spec 解不開：不 spawn、不寫 status，只記 log，等 spec 出現（E09-D044）。已經是 `DONE` 或 `FAILED` 的不再 spawn，即使指派還在（E09-D045）。自己 instance 上同一個 `api_id` 的 `algo_ids` 有交集時，`issued_at_ns` 較晚的不 spawn，以 `FAILED`、`algo_conflict` 結束（E09-D017）；`algo_conflict` 同樣不回頭（E09-D045）。spawn 的 hash 是整份 spec 的編碼；`observed_generation` 是該 spec 的 `mod_revision`。spec 不應改（E13-D022），改了就換 incarnation 並記 log（E09-D047）。
- **`SessionStatus.phase`（DECISIONS E09-D021、E09-D046）。** 有指派未 spawn 是 `PENDING`。骨架的 `Initializing` 與 `Initialized` 是 `WAITING`（`Init` 已送、`Start` 還沒送）。`Starting` 是 `STARTING`。`RUNNING` 要等 worker 的 `Stage(running)`，不看 `Done(START, ok)`。已送 `Stop` 是 `STOPPING`；退避中是 `RESTARTING`。shim 消失且沒有結束紀錄是 `FAILED`、`shim_lost`，不重啟（E09-D070）。rootfs、所有 inputs 就緒且 volume 已建立才進計畫，期間 phase 維持 `PENDING`；永久的 artifact 錯誤以 `FAILED` 結束、不 spawn，reason 是 artifact 的 reason（E09-D058、E09-D074）。volume 超過上限時以一般 Stop 停下（有 grace、`OnStop` 照跑），phase `FAILED`、reason `volume_over_limit`，不重啟（E09-D076）。terminal 只有 `DONE` 與 `FAILED`。`end_reason` 為 `ended_by_spec` 且 worker 正常結束（或尚未 spawn）是 `DONE`；`deps_failed`、`deps_timeout`、`invalid_spec` 是 `FAILED`，reason 即 `end_reason`，尚未 spawn 也一樣（E09-D063）。worker 的 outcome 是 `DONE` 時是 `DONE`，不重啟。`FAILED`、`INIT_FAILED` 是 `FAILED`，reason 用 worker 給的，不重啟（E09-D019、E09-D064）。`CRASHED`、`HUNG` 與不是 agent 要求的 signal，在 `ON_FAILURE`（`UNSPECIFIED` 也算）下重啟：退避期間 phase 是 `RESTARTING`，reason 是上次結束的原因；worker 結束之後、重啟策略判斷這個 incarnation 之前是 `STOPPING`，不寫 terminal（E09-D095）；退避用骨架的 `Ended.Restarts`，1 秒起、上限 60 秒，可改（E09-D019、E09-D071）。`window_s` 內重啟超過 `max_restarts` 次以 `FAILED`、`restart_limit` 結束，計數在這個 agent 進程的記憶體，輪換後從 0 起（E09-D069）。沒有 `Beat` 超過門檻則直接 SIGKILL，不送 `Stop`；shim 的 `ExitInfo` 照實記 signal，`SessionStatus` 把它當成 `HUNG`、`no_beat`（E09-D066、E09-D067、E09-D068）。其他 terminal 是 `FAILED`，`reason` 寫原因。只在 phase 或 `reason` 改變時寫。

### 3.4 MD 的推導與 refcount

- **owner** 只有 `{session_id}/{entry}`（需求項目）。沒有常駐訂閱；要保持行情或 tape，跑一個 tape keeper 策略（DECISIONS E04-D004）。
- **推導：** feed key 是 `topic.UniversalTicker`。MD agent 從 ticker 的 venue 與 topic 決定的 route 選 `md/<venue>/<route>/<n>`，把 feed 放進那個 worker。它不查 SYM，也不組 atom。conn worker 載入該 venue 的活躍表，用 `Market.Atom` 推出 atom，跟目前的訂閱做 diff。不在活躍表或不是 `TRADABLE` 的不訂（`EXPIRED`、`DELISTED` 不在活躍表）。adapter 是純函數，不查 SYM。selector 的成員從已載入的表裡挑，仍是後續。selector 的狀態（universe、epoch、置中點）存在 `/md/{inst}/selector/{spec_hash}`，規格相同的 selector 只算一份。
- **feed 的 owner 是集合，不是計數。**
  - 新的 atom 放進同一個 `(venue, route)` 下還有空間、n 最小的連線；都滿了才開 n+1（容量由 adapter 宣告，DECISIONS E04-D003）。共用同一個 channel 的 feed 是同一個 atom，只佔一份額度，放在同一條連線。有黏性，記在 `/md/{inst}/placement/`，推給 conn worker 的是 `topic.UniversalTicker`。不整併。
  - 最後一個 owner 離開：linger 之後才從 desired 移除。worker 再退訂已經沒有 feed 的 atom。
- **連線在最後一筆 feed 的 linger 結束時就送 `Stop`**，沒有另外的 linger（DECISIONS E09-D005）。linger 預設 5 秒，是 MD instance 的部署設定。
- **重啟：** list ＋ watch 讀回全部 owner、selector 狀態與 placement，再 reconcile。worker 自己從活躍表推出目前該訂的 atom。

TD 的對應（DECISIONS E09-D001）：每個 `api_id` 一個 owner 集合，refcount 是它的大小。0→1 時 spawn account worker；1→0 時 linger 之後才送 `Stop`，並排一次歷史補正（E06-D001）。account worker 存在與否就等於「這個帳號可以交易」，沒有另外的交易層開關。

### 3.5 ack／nack

每個需求項目，provider 寫一個對應的 `EntryStatus`，不綁 lease（W7）。key 的狀態對應 ack／nack／沉默：

| key | 意思 | controller 怎麼處理 |
|---|---|---|
| 存在，`READY` | ack：資源正常 | 全部 `READY` 時放行 `start`（§3.3 N2）。過舊 region 的 `READY` 不放行；`bindings` 仍抄（E13-D032） |
| 存在，`PENDING` | 還在建立 | 繼續等；`start` 之前超過 `deps_timeout_s` 就以 `deps_timeout` 結束 session |
| 存在，`REJECTED` | 這個項目不可能成立（venue 沒有這個 topic、商品不存在、帳號不存在、algo_id 與同帳號其他 session 衝突） | 以 `deps_failed` 結束 session（§3.3 N3） |
| 存在，`DEAD` | nack：provider 放棄這筆仍被需要的需求（下面） | 以 `deps_failed` 結束 session。看到一次就結束，不改回 `RUNNING` |
| 不存在 | 不知道：provider 還沒處理這個項目 | 維持現狀；`start` 之前照常計 `deps_timeout_s` |

provider 的 `AgentRecord` 不在時，上表的值是它最後寫下的狀態：agent 可能只是在輪換，worker 照跑。controller 照值處理，不把「agent 不在」當成 nack，也不因此改變 session。沒有對應需求項目的 ack（N4 刪掉需求之後，provider 不在、還沒刪的）controller 不理會。

- feed 的 `READY`：RUNNING 的 conn worker 在 `Report` 裡把這筆 feed 標成 `SUBSCRIBED`，也就是它所在的 channel 已被交易所 ack。不在活躍表，或這條 route 訂不了，worker 標成 `REJECTED`，agent 照寫。在表上但不是 `TRADABLE` 維持 `PENDING`。
- selector 的 `READY`：已算出 universe，且每個成員的 atom 都已 ack。
- 帳號的 `READY`：account worker RUNNING、私有連線已建立、recon 完成。

**`DEAD` 只在放棄重拉時寫（P6）。** `WorkerStatus.phase` 本身不結束 session。provider 把一條 worker 翻成它上面每一筆需求的 `EntryStatus`：

- `BACKOFF`（進程已結束，還要再拉）：那些需求寫成 `PENDING`。已經 `Start` 的 session 繼續跑。
- `LOST`（shim 不見，沒有這次的結束紀錄）：維持最後一次寫下的狀態。
- `EXITED`，這條 worker 仍被需要，而且 Policy 決定不再拉：寫 `DEAD`。
- 需求已經不在而進程是 `EXITED`：正常停掉。刪 `WorkerStatus`，不寫 `DEAD`。

同一個進程裡 websocket 自己重連（MD 的 `conn_epoch`、TD 的 `DEGRADED`），worker 還在，不是 `DEAD`。TD 的 ack 維持 `READY`（E06-D003）。

controller watch 這些 key，依上表決定 session 的去留，並把每個項目的 `EntryStatus` 抄進 `SessionDemand.bindings`。`desired` 已經是 `ENDED` 之後不再抄（E13-D030）。STS agent 不讀 ack，只讀自己的 `SessionDemand`：`start` 變成 true 時送 `Start`，`bindings` 變化時以 `stsctl.Bindings` 轉送給 session worker（§6.1）。

### 3.6 值

session 的宣告在 `mftiko.session.v1`，三份契約共用（DECISIONS E00-D007）；其餘 etcd 的值在 `mftiko.state.v1`。

```proto
// file: mftiko/session/v1/session.proto
syntax = "proto3";

package mftiko.session.v1;

import "mftiko/session/v1/selector.proto";

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1;sessionv1";

// session 的宣告：使用者經 API server 寫進 etcd（§3.1）；controller 依它推導需求，STS agent 依它起 worker，策略在
// InitPayload 拿到自己宣告的那一份（§6.1）。歸 STS 擁有；state.v1、stsctl.v1 與 API 的契約都 import 它（DECISIONS E00-D007）。
// 商品在這裡寫 UniversalTicker，不寫 UID（E02-D003）。ticker 解成 UID 發生在這份 spec 載入時，不在這份宣告裡。

message SessionSpec {
  string name = 1;
  map<string, string> labels = 2;
  DesiredState desired_state = 3;
  StrategyRef strategy = 4;
  bytes params = 5; // 策略自己的參數，平台不解讀
  repeated FeedDecl feeds = 6;
  repeated SelectorDecl selectors = 7;
  repeated AccountDecl accounts = 8;
  RestartPolicy restart = 9; // 語意見 DECISIONS E08-D003
  uint32 deps_timeout_s = 10; // 從 SessionDemand.issued_at_ns 起算。0 = 60 秒（E13-D002）
  uint32 start_timeout_s = 11;
  Limits limits = 12;
  repeated InputDecl inputs = 13; // 資料 artifact（DECISIONS E09-D004）
  // MD 事件在 buffer 裡的最長壽命，以 envelope 的 md_ts_ns 計（E08-D003）。0 = STS 的預設值。
  // 跨 region 的 session 要比 WAN 延遲大。
  uint32 recv_window_ms = 14;
  // Stop 的 grace（E09-D018、E09-D062）。0 = STS 部署設定的預設，10 秒。
  uint32 grace_ms = 16;
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

// 人寫的規格。feed key 裡是 UniversalTicker，不是 UID（E02-D003）。
// ticker 解成 UID 發生在 spec 載入時（sym.GetRequest），不在這份宣告裡。
message FeedDecl {
  string feed = 1;        // 正規 feed key（<topic>.<UniversalTicker>）。載入邊界才解成 UID
  string md_instance = 2; // API 驗證後填入
}

message AccountDecl {
  string name = 1;
  int64 api_id = 2;
  string td_instance = 3;
  // 這個 session 在這個帳號上可以使用的 algo_id，每個 1–1023（E06-D002）。
  // 同一個帳號上不可與其他非 terminal 的 session 重疊：STS agent 檢查它自己的 session，跨 instance 的由使用者保證（E06-D016）。
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
  uint32 volume_mb = 3; // session volume 的上限（E09-D004）；0 = STS 的預設值。超過就以 FAILED 結束（volume_over_limit）
  uint32 hook_timeout_s = 4; // 單一 hook 的硬上限（E08-D003）；0 = STS 的預設值。超過判定 HUNG
}
```

```proto
// file: mftiko/session/v1/selector.proto
syntax = "proto3";

package mftiko.session.v1;

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1;sessionv1";

// selector 的宣告。形狀在 E5 定（#7、#208），這份檔案留在草案清單，session.proto 定案時不跟著凍結（DECISIONS E09-D091）。

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
```

```proto
// file: mftiko/state/v1/state.proto
syntax = "proto3";

package mftiko.state.v1;

import "mftiko/common/v1/common.proto";
import "mftiko/session/v1/selector.proto";
import "mftiko/session/v1/session.proto";
import "mftiko/state/v1/selector.proto";

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1;statev1";

// ---------- 帳號的宣告（API server 寫）----------

message Account {
  string name = 1;
  // 帳號種類（DECISIONS E03-D001），不是 venue：BinancePM 可以交易 Binance、BinanceUM、BinanceCM 的商品。
  // 兩個 enum 的名稱可能相同（帳號種類 BinanceUM 交易 venue BinanceUM），不可互查。
  string kind = 2;
  bytes sealed_credentials = 3; // AccountCredentials 以 TD instance 的公鑰封裝（E06-D013、§3.6）；etcd 裡不放明文
  AccountSettings settings = 4;
  bool enabled = 5;
}

// sealed_credentials 封裝前的明文（E06-D013）。只在封裝工具、API server 與 TD agent 的記憶體裡；不進 etcd、不進 log。
// api_id 與 kind 一起封進去，解開之後與帳號的 key、宣告比對：一個帳號的密文搬到另一個帳號底下就解不出來用。
message AccountCredentials {
  int64 api_id = 1;
  string kind = 2;
  string key_type = 3; // hmac | ed25519
  string key = 4;
  string secret = 5; // HMAC secret，或 PEM 編碼的 ed25519 私鑰
  string passphrase = 6; // OKX
}

message AccountSettings {
  uint32 cancel_on_disconnect_s = 1; // 0 = 關閉
  // 歷史補正（DECISIONS E06-D001）。
  uint32 backfill_interval_s = 2; // 另加 jitter；0 = 預設。必須短於交易所成交歷史的保留期。誰用見 §10 P18，不是 TD agent
  // 手動觸發：比上次處理過的值新，就提前跑一輪。誰跑見 §10 P18，不是 TD agent。重啟後可能多跑一次，無害。
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
  StateSnapshot state_out = 9; // session terminal 之後 volume 的快照（E09-D004）；推送完成前不填
}

message StateSnapshot {
  string digest = 1;     // 資料 artifact 的 OCI manifest digest；新 session 以 InputDecl 引用
  string repository = 2;
  bool clean = 3;        // 最後一個 incarnation 的 ExitInfo.on_stop_completed
  uint32 incarnation = 4;
  int64 at_ns = 5;
  uint64 size_bytes = 6;
}

// ---------- controller 寫（不綁 lease，DECISIONS E09-D001、E09-D007）----------

// MD、TD 的需求項目（§3.3）。
message DemandEntry {
  oneof target {
    string feed = 1;
    mftiko.session.v1.SelectorDecl selector = 2;
    int64 api_id = 3;
  }
  int64 spec_revision = 10; // 推導自哪一版 session spec；只供除錯，可能來自另一個 cluster，不做比較
  repeated uint32 algo_ids = 11; // 只有帳號項目：同 AccountDecl.algo_ids。TD agent 不讀它（E06-D016）
}

// STS 的指派（§3.3）。STS agent 只跑有指派的 session。
message SessionDemand {
  mftiko.session.v1.DesiredState desired = 1; // RUNNING：spawn 並送 Init；ENDED：停掉 worker
  bool start = 2;           // 所有 MD、TD 需求都 READY，STS agent 可以送 Start（E01-D007）
  string end_reason = 3;    // desired 是 ENDED 時：ended_by_spec、deps_failed、deps_timeout、invalid_spec
  repeated Binding bindings = 4;
  int64 spec_revision = 5;  // 推導自哪一版 session spec；只供除錯
  int64 updated_at_ns = 6;
  int64 issued_at_ns = 7;   // 第一次寫這個指派的時間，之後不改；deps_timeout_s 從這裡起算（E13-D002）
}

// 一個需求項目目前的 ack，由 controller 從 provider 的 EntryStatus 抄來。
message Binding {
  oneof target {
    string feed = 1; // feed key。spec 載入時才把其中的 ticker 解成 UID（E02-D003）；這個欄位今天仍是字串
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
  // feed：完整的 subject（md.v1.f.{inst}.{topic}.{ticker}，E04-D055）。NATS topic 用人讀的名字，不用 UID。selector：成員 subject 的前綴（md.v1.f.{inst}）；
  // 帳號：td.order.{api_id}。由 provider 決定，訂閱端不自己組（PROTOCOL §7、DECISIONS E01-D006）。
  string subject = 3;
  mftiko.common.v1.WorkerRef worker = 4;
  Universe universe = 5; // 只有 selector
  int64 updated_at_ns = 6;
}

// ---------- agent 自己 ----------

// 綁在選主 session 的 lease 上（§3.2 W7）：存在就表示這個 instance 有活著的 agent。
message AgentRecord {
  string plane = 1;
  string instance = 2;
  int64 epoch = 3;
  string release = 4;
  int64 started_at_ns = 5;
  string host = 6;     // os.Hostname()，當選時寫入（E10-D092）。取不到則空字串。
  string work_dir = 7; // 該行程的 WORK_DIR（E10-D092）。controller 沒有 WORK_DIR，留空。
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

message Placement {
  string conn = 1; // atom 所在的 conn worker key
}
```

```proto
// file: mftiko/state/v1/selector.proto
syntax = "proto3";

package mftiko.state.v1;

import "mftiko/session/v1/selector.proto";

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1;statev1";

// selector 的 ack 內容與 MD agent 的 selector 狀態。形狀在 E5 定（#7、#208），留在草案清單（DECISIONS E09-D091）。

message Universe {
  uint64 epoch = 1;
  repeated string members = 2; // 成員是商品。今天是 UniversalTicker；改成商品 UID 是後續（E02-D003）。selector 本身沒有 UID
  string current = 3;          // 只有 rolling_future。這是 selector，尚未定，不在 E02-D003 的範圍；selector 沒有 UID
  repeated string topics = 4;
}

message SelectorState {
  mftiko.session.v1.SelectorDecl spec = 1;
  uint64 epoch = 2;
  repeated string members = 3; // 同 Universe.members：商品。今天是 UniversalTicker
  string current = 4;           // 同 Universe.current：selector，尚未定，不在 E02-D003；selector 沒有 UID
  string center = 5; // 置中的 strike
  int64 center_at_ns = 6;
}
```

**sealed_credentials（E06-D013）。** 每個 TD instance 一對 X25519 金鑰。私鑰是該 instance 主機上的檔案，權限 0600，路徑是部署設定；公鑰在 API 的 `td_keys`。API 寫帳號時以 `internal/platform/sealbox` 封裝（E10-D011、E10-D062）。`cmd/mftiko-seal` 仍用來重封。讀取不回傳明文、密文或指紋（E10-D063）。格式：

```
version (1 byte，目前是 1) | 收件公鑰的指紋 (8 bytes = SHA-256(公鑰) 的前 8 bytes) | NaCl sealed box（X25519、XSalsa20-Poly1305）
```

封進去的是 `AccountCredentials`。TD agent 解開之後比對 `api_id`（etcd key 上的）與 `kind`（`Account.kind`），不符就不用。解不開時需求 `REJECTED`，reason 是 `credentials_other_key`（指紋不是這個 instance 的，要用它的公鑰重封）、`credentials_unreadable`（空的、版本不認得、長度不足或內容損壞）或 `credentials_other_account`（解得開但封給別的帳號）。指紋只是標籤，不是驗證：竄改成本 instance 的指紋仍然解不開，算 `credentials_unreadable`。換 key 就是讓封給舊 key 的帳號全部重封。

## 4. C3：agent ⇄ shim

### 4.1 傳輸

unix socket `${WORK_DIR}/run/{plane}/{h}.sock`，`WORK_DIR` 是 `Launch.work_dir`（§4.6）。frame 是 4 bytes big-endian 長度加一則 protobuf，上限 1 MiB。agent → shim 是 `ShimRequest`，shim → agent 是 `ShimMessage`。

- shim 讀到 `Launch` 之後、spawn 之前開始 listen（§4.6 第 4 步），listen 不成就以 1 結束、不 spawn。agent 以重試 connect 等 socket 出現。
- 權限只給 agent 的 uid。shim 由 agent exec，沿用 agent 的 uid，所以 agent 的 uid 就是 shim 自己的 uid：
  - `run/{plane}` 不存在就以 0700 建立；已經存在的必須是 shim 的 uid 擁有、group 與 other 沒有任何權限，否則以 1 結束。socket 是 0600。
  - 每條連線以 `SO_PEERCRED` 檢查，uid 不同就直接關掉。root 也一樣，除非 shim 自己是 root。
- 路徑上已經有 socket 時先 connect：連得上表示另一個 shim 還在服務同一個 worker key，以 1 結束。換 incarnation 是 delete-before-create（E01-D006），舊的 shim 要先 `Release`。連不上就是殘留（shim 被 SIGKILL），刪掉重建。路徑上是別種檔案時以 1 結束，不刪。
- shim 結束時刪掉自己的 socket。
- 同時可以掛多條連線，只有一條是控制者（§4.2）。每條連線的送出有自己的佇列，寫入有逾時；agent 不讀，佇列滿了或逾時，shim 就關掉這條連線，不會因此卡住其他連線。寫入因為 agent 已經關閉（例如送完 `Stop` 就退出）而失敗時，shim 不再寫，但照樣讀完並執行它已經送出的指令，直到讀到 EOF。

### 4.2 attach 與 fencing（E01-D005）

1. agent 送 `Attach{plane, agent_instance, epoch, shim_majors, resume_from}`，`epoch` 是 §3.2 W2 的值，從 1 起。
2. 依序檢查。不通過就回錯誤，什麼都不改，原來的控制者照舊：
   - `plane`、`agent_instance` 與 `Launch` 不同 → `wrong_agent`。
   - `epoch` 小於 1 → `bad_request`。
   - `epoch` 小於 shim 接受過的最大 epoch → `stale_epoch`，`details["epoch"]` 是那個最大值。這個值在控制者斷線之後仍然保留：新 agent 斷線時，被它取代的舊 agent 重連也拿不回控制權。
3. 通過時（`epoch` 大於或等於接受過的最大值），對舊的控制者連線送 `Preempted{by_epoch}`，送完就關閉它，這條連線成為控制者。相等也接受：同一個 agent 重連時，舊的連線可能還沒斷。控制者自己再送 `Attach` 不會收到 `Preempted`。
4. 協商 major：`shim_majors` 與 shim 支援的 major 的交集裡最大的那個，放在 `Attached.shim_major`。沒有交集時 `shim_major` 是 0：attach 照樣成功，fencing 照樣生效，但這條連線只能送 `Signal`，其他指令回 `no_common_major`，也不串流事件。
5. shim 回 `Attached`，接著送最新的 `Report`，然後從 `resume_from` 開始串流事件。太舊時從最舊的開始送；中間有在確認之前就被丟掉的，第一則標 `gap`（§4.4）。
6. 只有目前控制者的指令會被執行，其他連線收到 `not_controller`，`Signal` 也一樣。
7. agent 在任何一個 shim 收到 `Preempted`，表示已經有更新的 term：它和 term 結束（§3.2 W2）一樣立刻退出，交給 systemd 重啟。被搶走的 worker 照樣在跑，只是換了控制者，所以不把它當成失聯，不 Release，也不寫 `WorkerStatus`；etcd 的寫入遲早也會因為 writer key 失敗，提前退出只是更早停手。

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

### 4.3 生命週期（E01-D007）

agent 經 shim 送 `Init`、`Start`、`Stop`。shim 把 `Init`、`Start` 原樣轉給 worker，自己只記下 worker 目前的階段。生命週期是兩段（DECISIONS E01-D007）：`Init` 在 spawn 後立刻送，不等任何需求；`Start` 在依賴都就緒之後才送（STS 等 controller 把指派的 `start` 設成 true，§3.3 N2；MD、TD 的 worker 沒有跨平面依賴，`Init` 完成就送）。

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
| `Live{contract, payload, digest}` | 轉給 worker（live config，E01-D011），任何階段都可以。shim 不記內容；worker 手上是哪一份，以它回的 `Applied` 為準 |
| `Release{}` | worker 的結束已記錄（S3）時才接受，否則 `running`。回應送出後，shim 刪除結束檔與 socket 後退出 |

worker 被 reap 之後，`Init`、`Start`、`Stop`、`Relay` 都回 `exited`。轉給 worker 的指令依收到的順序寫進 fd 3；worker 不讀時，寫入排在 shim 的佇列裡，佇列滿了就回 `worker_busy`，不會卡住其他連線。`Stop` 的 C4 `Stop` 排不進去時，計時照樣開始：SIGTERM 會在 grace 之後到，SDK 把 SIGTERM 當成 `Stop`（§9.2）。

shim 收到 SIGTERM（例如主機關機）時，自己對 worker 送 `Stop`，不先退出：`reason` 是 `shim: terminated`，grace 10 秒，再過 5 秒 SIGKILL。worker 已經在 `Stop` 的計時裡時沿用原來的計時。worker 被 reap 之後 shim 才退出，不等 `Release`。收到 SIGTERM 時 worker 已經結束的話，shim 立刻退出。SIGINT 一樣。

### 4.4 事件與緩衝

| 事件 | 緩衝 |
|---|---|
| `Started`、`Hello`、`Done`、`Stage`、`Exiting`、`Stopped`、`Applied` | 環狀緩衝（1024 則），滿了丟最舊的 |
| `Report` | 只保留最新一份；attach 時先送一次 |
| `Beat` | 不緩衝；最多每秒轉發一則。`Attached` 帶最後一次的時間 |
| `Relay`（worker 的請求） | 不緩衝；沒有控制者時，shim 立刻對 worker 回 `Undeliverable` |
| `Exited` | 環狀緩衝，另外落檔 `run/{plane}/{h}.exit.pb`，直到 `Release`（S3） |

- **快照為準（DECISIONS E01-D008）。** agent 做任何決策所依據的事實，都必須在 `Attached`（或結束檔）裡有最新值：state、phase、最後一個 stage、`Hello`、最後一次 `Beat` 的時間、`exiting`（worker 送的第一則 `Exiting`）、`exit`（記錄完的 `ExitInfo`）、`applied_digest`（worker 最後回報已套用的 live config，E01-D011），加上 attach 時先送的最新 `Report`。事件是降低延遲的通知；agent 不在時遺失的事件只少了歷史，不影響它回來之後的判斷。
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
  - 檔案是 `${WORK_DIR}/log/{plane}/{h}.stdout` 與 `{h}.stderr`，`h` 同 §4.1 的 socket。目錄不存在就建立；shim 每次啟動都把這個目錄 Chmod 成 0750，目錄上已有的 setgid 保留，新檔才沿用部署設在 `${WORK_DIR}/log` 的 group（DECISIONS E10-D015、E10-D022）。新檔建立後再 Chmod 成 0640，不受 umask 影響（E10-D021）。已經存在的舊 log 檔不改權限（E10-D022）。agent 建立的 `{h}.shim.log`（§4.6）也一樣。
  - 依 worker key、不依 incarnation：新的 incarnation 接在同一組檔案後面寫，開頭一行 `[mftiko-shim] <時間> worker <key> incarnation <n>`。同一個 worker key 同時只有一個 shim（§4.1），所以每個檔案只有一個寫入者。
  - 檔案到了 `Launch.output.max_bytes` 就輪替：`{h}.stdout` 改名成 `.1`，原來的 `.1` 改成 `.2`，依此類推；每個 stream 只留 `keep` 個檔案（含目前寫的那個），最舊的刪掉。沒有任何檔案超過 `max_bytes`。預設 64 MiB、5 個。
  - worker 絕不因為輸出而阻塞：shim 一直讀 pipe，寫檔在另一個執行緒。寫檔跟不上（最多暫存 2 MiB）或寫入失敗（磁碟滿）時丟棄，並在恢復寫入的位置插入一行 `[mftiko-shim] N bytes of stdout lost here: …`。
  - worker 被 reap 之後，shim 退出前最多等 2 秒把剩下的輸出寫完；worker 留下的孫進程若還拿著 pipe，不等它。
  - 檔案不隨 `Release` 刪除。STS 的 worker key 不會重用，spec 被刪之後由 STS agent 清掉（§8.4、DECISIONS E10-D016）。
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
    mftiko.worker.v1.Live live = 18;
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
  string applied_digest = 17;            // worker 最後一則 Applied 的 digest：它手上的 live config（E01-D011）；沒送過是空的
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
    mftiko.worker.v1.Applied applied = 20;
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

- 以 `SysProcAttr.Setsid` 起 shim，stdout、stderr 都接到 `${WORK_DIR}/log/{plane}/{h}.shim.log`（附加寫入，跨 incarnation 同一個檔，新檔 0640），不接 pipe。目錄是 0750（§4.5 S4、DECISIONS E10-D021、E10-D022）。
- `Launch.env` 是平面給的（manifest 的 `env`，再加平面自己的 §5.4 變數），最後由 agent 補上 `MFTIKO_PLANE`、`MFTIKO_INSTANCE`、`MFTIKO_WORKER_KEY`、`MFTIKO_INCARNATION`。
- 寫完 `Launch` 就 connect 並 `Attach`（§4.1 的重試）；shim 在 listen 之前就結束時立刻放棄，不等逾時。接著送 `Init`。
- agent 活著時由它 reap 自己起的 shim；agent 不在之後由 init 接手。
- 同一個 worker key 至多一個存活的 incarnation（E01-D006）。起下一個之前，上一個要確定不在：
  - 有結束紀錄（`Exited`，或 shim 不見之前寫下的結束檔，S3）就是不在；經 shim `Release`，或 shim 已不在時由 agent 刪掉結束檔。
  - 沒有紀錄（shim 被 SIGKILL）時，以 `Attached` 的 `pid` 與 `start_time` 查 `/proc/<pid>/stat`：pid 不存在、start_time 不同（pid 被重用）、或是 zombie，才算不在；agent 起的那個 shim 也要已經結束。讀不到 `/proc` 不算不在。
  - 還在就不起，等退避之後再試。worker 應該隨 shim 結束（E01-D004：Pdeathsig、fd 3 的 EOF），這只是保險。
- 啟動時收養（E01-D002，E1-17）：新的 term 在第一次 reconcile 之前（§3.2 W7.3）掃 `run/{plane}/` 的 `{h}.sock` 與 `{h}.exit.pb`：
  - socket 有回應：以這個 term 的 epoch `Attach`（舊 term 若還連著就被 `Preempted`），worker 的狀態取自 `Attached`（E01-D008）；`next_seq` 之前的事件已經反映在快照裡，略過。被拒為 `stale_epoch` 表示有更新的 term，agent 結束。
  - socket 有回應但 attach 不成（逾時）：保留它的 `WorkerStatus`（W7.1），不為這個 worker key 起新的 incarnation（它可能還在跑），每隔幾秒重試，直到 attach 成功或 shim 不在。
  - socket 沒有回應而有結束檔：worker 在沒有 agent 時結束，當成已結束的 worker 交給 Policy；`Release` 時由 agent 刪掉結束檔。
  - socket 沒有回應也沒有結束檔：shim 在記錄結束之前被殺，worker key 不明，無從收養；它的 `WorkerStatus` 依 W7.1 處理（需求還要它就保留）。下一個 shim 會清掉殘留的 socket（§4.1）。
  - live config 以 etcd 為唯一來源（DECISIONS E01-D011）。worker 手上是哪一份看 `Attached.applied_digest`：和從 etcd 推導出的 digest 相同就不推，不同才推。推出去還沒收到 `Applied` 的，等一段時間（預設 5 秒）再推。

部署時 shim 不能和 agent 放在會一起被殺的 cgroup 或 systemd unit 裡，否則 agent 重啟時 shim 和 worker 會跟著死，E01-D002–E01-D004 就不成立。

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
              （之後：Beat 每 1 秒、Report、Relay；收到 Live 套用後回 Applied）
          ◀── Stop                       或策略自己 Exit／Fail，或 crash，或呼叫端 ctx 取消（SIGTERM）
          ──▶ Exiting(outcome, reason)
              OnStop（Stopper；HUNG、Init 失敗、Start 逾時不跑）
          ──▶ Stopped(on_stop_completed)
              exit(code)
```

- `Exiting` 一定在 `OnStop` 之前送。`OnStop` 卡住被殺時，agent 仍然知道這個 worker 原本打算怎麼結束。Go 的 worker 把這段做成 `Stopper`：有實作才在這兩則之間呼叫 `OnStop(ctx)`；沒實作的（MD、TD 目前）行為不變，`Stopped.on_stop_completed` 仍照 §5.3（E08-D009）。回傳 error 時記 false，outcome 與 exit code 不變，因為 `Exiting` 已經送出。
- `Init.deadline_ms`、`Start.deadline_ms`（E08-D007）：大於 0 而該階段逾時，不等 hook 返回，送 `Done(verb, false)`、`Exiting(INIT_FAILED, deadline)`、`Stopped(false)`，exit 10。0 表示不限時。Start 逾時不跑 `OnStop`；Start 的其他失敗要跑，因為 Init 已經成功（E08-D008）。
- **`Init` 被接受之前的其他訊息（E08-D037）：** 還沒有一則 `Init` 成功時，收到 `Init` 以外的 `WorkerDown`（`Start`、`Stop`、`Live`、`Relay`、`Controller`），或成功之後又收到一則 `Init`，都以 `INIT_FAILED` 結束：`Done(verb, false)`、`Exiting(INIT_FAILED)`、`Stopped(false)`、exit 10，不呼叫策略 hook。`Start` 的 verb 是 `START`，其餘（含第二則 `Init`）的 verb 是 `INIT`。不得回 `Done(ok)`。
- **`Init` 成功、`Start` 尚未成功時的 `Stop`（E08-D037）：** 走正常的 `DONE` 結束順序：`Exiting(DONE)`、`Stopped(false)`、exit 0。不呼叫 `OnStart`，也不呼叫 `OnStop`（`on_stop_completed` 是 false）。`OnInit` 已經跑完，不重跑。
- `OnStop` 的 ctx 不是拿來結束 Init、Start、Run 的那一個。讀過 `Stop` 時，期限是那則 `Stop.grace_ms` 的剩餘：從 worker 讀到它起算，加上 `grace_ms`。`grace_ms` 為 0 就是已經到期。之後的 `Stop` 不改這個期限（對齊 shim 只讓第一個 `Stop` 設定計時，§4.3）。只因呼叫端 ctx 取消而結束（SIGTERM 自己到，沒有 grace）時不設期限，c4 不另造一個 grace。worker 自己結束（`Run` 返回）時，期限是 plane 在 `Init` 成功後交給 c4 的 `hook_timeout_s`（E08-D009）。c4 不解析 C5 payload：Go 的介面是 `HookTimeout`，`Serve` 在 `Init` 返回後讀一次 `HookTimeout() time.Duration`；STS 把 `InitPayload.hook_timeout_s` 的秒數換成這個 duration。回 0，或 plane 不是 `HookTimeout`（MD、TD），就不限時。超過就不等 `OnStop`，exit 21，不送 `Stopped`；`Exiting` 已經送了，outcome 不改（E08-D003）。Start 失敗（非逾時）不套這個期限。
- SIGTERM（E08-D006）：Go worker 的 main（`mftiko-md-conn`、`mftiko-td-account`、SDK 的 session worker）以 `signal.NotifyContext` 把 ctx 傳入 `Serve`。ctx 取消視同 `Stop`。`Serve` 不攔訊號。
- `Beat` 由不會被 hook 阻塞的執行緒送（Go SDK 是獨立的 goroutine）。做不到的時候，由 STS agent 的 `Beat` 逾時接手（§9.7）。
- 收到 `Controller` 時更新「目前有沒有 agent」。沒有時，worker 對 agent 的請求會立刻收到 `Undeliverable`。
- live config（E01-D011）：`Init.payload` 帶第一份，`Init.live_digest` 是它的 digest；之後的每一份是一則 `Live{payload, digest}`，都是完整的期望狀態，不是增量。worker 套用成功就回 `Applied{digest}`（`Init` 的在 `Done(INIT, ok)` 之前送），失敗就不送；收到和手上相同的 live config 什麼都不做，照樣回 `Applied`。agent 以 `Applied` 判斷 worker 手上是哪一份，沒收到就隔一段時間再送。

### 5.3 結束方式與 exit code

| outcome | 觸發 | `OnStop` | exit code |
|---|---|---|---|
| `DONE` | `Stop`，或策略 `Exit()` | 跑 | 0 |
| `INIT_FAILED` | `Init`／`Start` 失敗或逾時 | 只有 Start 失敗且不是逾時才跑（E08-D007、E08-D008） | 10 |
| `CRASHED` | hook panic 或回傳 error | 跑 | 20 |
| `HUNG` | hook 卡太久 | 不跑 | 21 |
| `FAILED` | 策略 `Fail()`，或 agent 要求 fail | 跑 | 30 |
| （沒有 `Exiting`） | 被殺：OOM、SIGKILL、沒接住的 panic | 不跑 | signal，或 1–9（Go 的 runtime 錯誤是 2） |

`Run` 返回且 plane 交了時限時，`OnStop` 超過該時限的 exit code 是 21，而且不送 `Stopped`（`on_stop_completed` 視為 false）。`Exiting` 的 outcome 仍是上面這一列，不改成 `HUNG`（E08-D003、E08-D009）。

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
| `MFTIKO_NATS_CREDS` | 只有 MD：所屬 instance 的 NATS 憑證。STS 的 session worker 與 TD 的 account worker 沒有這個變數：憑證每個 session、每個帳號一份，經 `Init` 送進去（DECISIONS E09-D006、E06-D011） |
| `GOMEMLIMIT`、`GOMAXPROCS` | Go runtime |
| `MFTIKO_VOLUME_DIR` | 只有 STS：session 的可寫目錄，跨 incarnation 保留（E09-D004）。第一次 spawn 前建立，權限 0700（E09-D077） |
| `MFTIKO_INPUT_DIR` | 只有 STS：`inputs` 解開的位置，每個 input 一個子目錄（E09-D004）。解完後檔案 0444、目錄 0555，worker 不應寫入（E09-D075） |

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
    Applied applied = 9;
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

// worker 已經套用的 live config（DECISIONS E01-D011）：Init 成功之後回報 Init.live_digest，套用一則 Live 之後回報它的 digest。
// 套用失敗就不送；agent 照它判斷 worker 手上是哪一份。
message Applied {
  string digest = 1;
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
    Live live = 7;
  }
}

message Init {
  string contract = 1;
  bytes payload = 2; // 該平面 C5 的 InitPayload
  uint32 deadline_ms = 3;
  string live_digest = 4; // payload 裡初始 live config 的 digest；Init 成功之後以 Applied 回報
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

// live config：完整的期望狀態，不是增量；worker 以 diff 套用，收到和手上相同的就什麼都不做（DECISIONS E01-D011）。
message Live {
  string contract = 1;
  bytes payload = 2; // 該平面 C5 的 live config
  string digest = 3; // agent 算的，worker 不解讀，套用之後以 Applied 原樣回報
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

下行的 `Command` 一律是完整快照，帶 `Version(epoch, seq)`；worker 只套用比手上大的。上行的 `Report` 也是完整快照，最新一份取代舊的。agent 輪換之後，新 agent 再推一次完整快照即可。STS 的 bindings 第一份放在 `Start`，seq 為 1；之後內容一變 seq 加 1，以 `Command` 推送。`Start` 之前的變化不推送，只更新 `Start` 要帶的內容。同一個 epoch 裡換 incarnation（重啟）時，即使 bindings 沒變，新的 `Start` 的 seq 也接下去加 1，不回到 1（E09-D071）。spec 換了編碼，替換後那個 incarnation 的 `Start` 從 1 起（E09-D061）。seq 只在記憶體，agent 輪換後 epoch 變大、seq 從 1 重來（E09-D061、E09-D022）。

### 6.1 STS：`stsctl.v1`

```proto
// file: mftiko/stsctl/v1/stsctl.proto
syntax = "proto3";

package mftiko.stsctl.v1;

import "mftiko/common/v1/common.proto";
import "mftiko/session/v1/session.proto";

import "mftiko/stsctl/v1/selector.proto";

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
  NatsCredentials nats = 9; // 這個 session 專屬的 NATS 憑證，只經 socketpair 送進去（DECISIONS E09-D006）
  // 以下兩個是 STS agent 解析後的實值（DECISIONS E08-D003）：spec 寫 0 表示用預設，agent 換成它的預設值再填，
  // 所以這裡不會是 0。SDK 不知道預設值；收到 0 是 agent 的錯，以 INIT_FAILED 結束。
  uint32 recv_window_ms = 10; // MD 事件的最長壽命，prehook 用（SessionSpec.recv_window_ms）
  uint32 hook_timeout_s = 11; // 單一 hook 的硬上限，watchdog 用（SessionSpec.limits.hook_timeout_s）
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

// name 是 td 或 md（E08-D003）。
message QueueStat {
  string name = 1;
  uint32 depth = 2;
  uint64 dropped = 3;    // md：因超過 recv_window_ms 或 buffer 滿而丟掉的則數
  uint32 max_age_ms = 4; // 上一個回報週期內，交給 hook 的事件中最老的那則的年齡
}

// 綁定的狀態，對應 provider 的 ack（§3.5）。值與 state.v1.EntryState 相同；stsctl 自己定義，策略的契約才不綁在 etcd 的
// schema 上（DECISIONS E00-D007）。
enum BindingState {
  BINDING_STATE_UNSPECIFIED = 0; // provider 沒有 ack（沉默）
  BINDING_STATE_PENDING = 1;
  BINDING_STATE_READY = 2;
  BINDING_STATE_REJECTED = 3;
  BINDING_STATE_DEAD = 4;
}
```

```proto
// file: mftiko/stsctl/v1/selector.proto
syntax = "proto3";

package mftiko.stsctl.v1;

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1;stsctlv1";

// selector 的綁定與成員。形狀在 E5 定（#7、#208），留在草案清單（DECISIONS E09-D091）。

message SelectorBinding {
  string name = 1;
  string md_instance = 2;
  Universe universe = 3;
  string subject_prefix = 4; // 成員的 subject 是 {subject_prefix}.{topic}.{ticker}。前綴是 md.v1.f.{inst}（E04-D055）。NATS topic 用人讀的名字，不用 UID
}

// selector 目前的成員。欄位與 state.v1.Universe 相同。
message Universe {
  uint64 epoch = 1;
  repeated string members = 2; // 成員是商品。今天是 UniversalTicker；改成商品 UID 是後續（E02-D003）。selector 本身沒有 UID
  string current = 3;          // 只有 rolling_future。這是 selector，尚未定，不在 E02-D003 的範圍；selector 沒有 UID
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
  string route = 2; // 由 adapter 從解析後的商品推得（E04-D003、E02-D002）
  uint32 n = 3;
  string md_instance = 4;
  // 實際連線的 URL。host 由 MD instance 的設定選：預設，或交易所提供的替代 endpoint（E04-D003）。換 URL 要換 incarnation。
  string url = 6;
  // 上次上報的 conn_epoch。0 表示還沒有上報過，第一次撥號用 1（E04-D009）。
  uint64 conn_epoch = 7;
  // 等快照時緩衝的筆數上限。0 表示 10000。到了就丟掉緩衝，進行中的快照作廢（E04-D024）。
  uint32 book_buffer_max = 10;
  // 等快照時緩衝的時間上限，毫秒。0 表示 10000。到了就丟掉緩衝，進行中的快照作廢（E04-D024）。
  uint32 book_buffer_ms = 11;
  // 每個 feed 的 tape 時間窗，毫秒。0 表示 10 分鐘。先到這個窗或 tape_max_events 就從最舊的砍（E04-D038、E04-D049）。
  uint32 tape_window_ms = 12;
  // 每個 feed 的 tape 筆數上限。0 表示 100000。
  uint32 tape_max_events = 13;
  // 這個 worker 全部 tape 的位元組上限。0 表示 256 MiB。訊息、slot、索引與 chunk 都算。超過時回收最舊的 64 KiB chunk（E04-D058）。
  uint64 tape_budget_bytes = 14;
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
  bool record = 3;           // 忽略。錄不錄由 conn worker 依 topic 決定（E04-D047）。定案時保留（E04-D060）
  // SYM 的 exchTicker。worker 從自己的活躍表讀。沒有這份表、而且 atom_id 已給時，用這裡的值。不從 channel 解析（E04-D026、E02-D002）。
  string exch_ticker = 4;
  // SYM 的 price_tick。跟 exch_ticker 同一張表、同一時刻。
  // 缺或小於等於 0 時這本訂單簿只記日誌、不維護（E04-D033）。
  double price_tick = 5;
}

message Report {
  mftiko.common.v1.Version applied = 1;
  uint64 conn_epoch = 2; // 每次重連 +1
  ConnState state = 3;
  string reason = 4;
  double msg_rate = 6;
  // 這條連線上每一筆 feed 的快照，完整清單。feed 是 topic.UniversalTicker。
  // 不在活躍表，或這條 route 訂不了，是 REJECTED。在表上但不是 TRADABLE 是 PENDING，這輪不訂。
  // channel 被交易所 ack 之後，掛在上面的每一筆 feed 是 SUBSCRIBED。
  repeated FeedStatus feeds = 7;
}

enum ConnState {
  CONN_STATE_UNSPECIFIED = 0;
  CONN_STATE_CONNECTING = 1;
  CONN_STATE_LIVE = 2;
  CONN_STATE_DOWN = 3;
}

message FeedStatus {
  string feed = 1; // topic.UniversalTicker
  FeedState state = 2;
  string reason = 3; // REJECTED 或還沒訂的 PENDING 才填
}

enum FeedState {
  FEED_STATE_UNSPECIFIED = 0;
  FEED_STATE_PENDING = 1;
  FEED_STATE_SUBSCRIBED = 2; // 所在 channel 已被交易所 ack
  FEED_STATE_REJECTED = 3; // 不在活躍表，或這條 route 訂不了
}
```

`InitPayload.conn_epoch` 是 shim 留著的最新一份 `Report`。0 表示這條連線還沒有上報過，第一次撥號用 1。新行程第一次撥號，以及同一個行程裡每一次重連，都先把下一個號寫進 `Report`，這次寫完才撥；撥上之後 `md.f` 才用這個號。撥不上不消耗這個號，行程還在的話下一次還是報同一個。行程若在寫完 `Report` 之後、撥上之前結束，shim 留著的就是這個還沒用過的號，下一個行程從它接著加。shim 退出之後這份快照就沒了，下一次從 1 起。不寫 etcd。agent 在重拉前抄走這個號、寫進下一次 `Init`，是另一張票。

`book_depth` 的 `reserved` 已刪除，欄位號碼不重排（E04-D053、E04-D060）。沒有 top-N。`book_buffer_max`、`book_buffer_ms` 為 0 時用預設：10000 筆、10 秒（E04-D024）。`tape_window_ms`、`tape_max_events`、`tape_budget_bytes` 為 0 時用預設：每個 feed 10 分鐘或 100000 筆，先到先砍，整個 worker 的 tape 256 MiB（E04-D038、E04-D049、E04-D058）。這 256 MiB 含訊息、slot、索引與 chunk。超過時回收最舊的 64 KiB chunk。大於 32 KiB 的事件不錄，改記 gap（E04-D059）。錄不錄由 topic 決定（`trade`、`aggtrade`、`liquidation`），`AtomAssignment.record` 忽略（E04-D047）。`AtomAssignment` 的 `atom_id` 空白時，`feeds` 是 `topic.UniversalTicker`。conn worker 在 Init 載入該 venue 的活躍表，自己推出 atom，並 diff 訂閱；`exch_ticker` 與 `price_tick` 從這張表讀。沒有表、而且 `atom_id` 已給時，才用 assignment 上的值。不從 channel 解析。`price_tick` 缺或小於等於 0 時這本訂單簿只記日誌、不維護（E04-D033）。429／418 沒有 `Retry-After` 也沒有 `ResetAt` 時的等待是行程旗標 `-rate-hold`，不在這份 payload 裡。SnapshotREST 抓一張 Depth 之後只套增量，不自己重抓（E04-D026）。SnapshotStream 不打 REST；斷號或 checksum 失敗只重訂該 channel，下一則串流快照是 `reset`（E04-D034）。本地簿是刻度環，環長預設 2048（E04-D030）。

conn worker 只照 `Desired` 對交易所做 subscribe／unsubscribe 的 diff：observed 以 `conn_epoch` 為鍵，舊 epoch 晚到的 ack 丟棄；重連後 observed 歸零，下一輪補齊。refcount 與 linger 都在 MD agent（§3.4）。每一則 `Report` 都是完整快照，帶目前的 `conn_epoch` 和 `feeds`。交易所 ack 一個 channel 時，掛在上面的 feed 改成 `SUBSCRIBED`；不在活躍表或這條 route 訂不了的是 `REJECTED`。查表暫時失敗時維持上一份快照。

### 6.3 TD：`tdctl.v1`

```proto
// file: mftiko/tdctl/v1/tdctl.proto
syntax = "proto3";

package mftiko.tdctl.v1;

import "mftiko/td/v1/td.proto";

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/tdctl/v1;tdctlv1";

message InitPayload {
  int64 api_id = 1;
  string kind = 2; // 帳號種類（E03-D001），同 state.v1.Account.kind
  string td_instance = 3;
  uint32 cancel_on_disconnect_s = 4;
  Credentials credentials = 5; // 交易所的憑證，只經 socketpair 送進去
  NatsCredentials nats = 6; // 這個帳號專屬的 NATS 憑證，只經 socketpair 送進去（DECISIONS E06-D011）
  // 交易所的 host，由 TD instance 依帳號種類設定（E06-D016，比照 MD 的 url，E04-D003）。空的是 adapter 的預設。
  string rest_url = 7;
  string stream_url = 8; // 私有 websocket
  bool simulated = 9; // 交易所的模擬交易模式是旗標而不是另一個 host 時（OKX）
}

message Credentials {
  string key_type = 1; // hmac | ed25519
  string key = 2;
  string secret = 3;
  string passphrase = 4;
}

// TD agent 為這個 api_id 簽的 user JWT。權限見 PROTOCOL §7。
message NatsCredentials {
  string user_jwt = 1;
  string seed = 2; // NKey seed，只在進程記憶體裡
  string inbox_prefix = 3; // _INBOX_td_{api_id}；worker 以它設定 CustomInboxPrefix
}

// Start：建 HTTP 連線池、連私有 websocket、recon。全部完成才回 Done(START, ok)，之後才服務 td.order（E06-D010）。
message StartPayload {}

message Command {
  oneof cmd {
    Drain drain = 1;
  }
}

// drain-replace 的第一步：不收新單，等在途 ack 收齊。一期沒有人送它，新單的拒單碼等 drain-replace 落地時再加（E06-D010）。
message Drain {
  uint32 deadline_ms = 1;
}

message Report {
  mftiko.td.v1.AccountState state = 1; // 與 td.view 回覆的 state 相同（E06-D006）
  string reason = 2;
  int64 reconciled_at_ns = 3;
  uint32 open_orders = 4;
  uint32 inflight = 5;
  bool draining = 6;
  // 這個 incarnation 下過單的商品（UID，E02-D003）→ 第一次 Submit 的時間（DECISIONS E06-D001）。落庫的範圍從這裡起；誰寫 scope 見 §10 P18，不是 TD agent。
  // 新商品的第一筆單：worker 先送出含它的 Report，再送單。
  map<uint32, int64> first_submit_ns = 7;
}
```

**account worker（E06-D014，E6-06）。** C4 的生命週期由 `contract/wire/c4` 實作，worker 只處理 tdctl 的 payload。

- **Init**：解 `InitPayload`，依 `kind` 建 `exchange.Account`。以下情形是 `INIT_FAILED`，reason 不引用憑證內容：
  - 帳號種類不認得。
  - 帳號種類交易不只一個 venue：reason 是 `kind_unsupported`，一期不跑 BinancePM。
  - 憑證的 key type、key、secret、passphrase 不合該種類。
- **連線分兩種角色：**
  - **事件連線**：帶私有事件的那一條。`TradeLimits.Events` 為真時是 trade stream（Binance 現貨），否則是 `PrivateStream`。
  - **下單連線**：不帶事件的 trade stream（OKX、Bybit、Binance UM），另外開。
- **`Start`**：開連線，等事件連線 Ready，跑完 `Snapshot` 的每一頁之後，送 `Report(READY, reconciled_at)`，再回 `Done(START, ok)`。
  - 失敗的撥號與 recon 一律退避重試，上限是 shim 的 Start 期限。
  - 交易所拒絕憑證是 `INIT_FAILED`，reason `auth`，不再重撥。
  - 雙向持倉是 `INIT_FAILED`，reason `hedge_mode`。
- **執行中：**
  - 事件連線斷線：`Report(DEGRADED, stream_lost)`。重連、recon 之後回到 `READY`。持倉模式只在 Start 檢查（E06-D016）。
  - 下單連線斷線：狀態不變。worker 重撥，期間下單改走 REST，不拒單（E06-D014、E06-D024）。
  - 連線快到 `MaxLifetime`：先接上新連線再關舊的，不經過 `DEGRADED`。事件連線換完跑一次 recon，`Recon` 的 reason 是 `RECONNECT`，期間維持 `READY`，完成時再送 `Report(READY)`（E06-D035–E06-D037，修改 E06-D014）。下單連線的換線不 recon、狀態不變。
  - 重撥時交易所拒絕憑證：以 `CRASHED` 結束，reason `auth`，exit 20。
- **帳號狀態（E06-D015，E6-07）：** worker 從 Init 起以 `InitPayload.nats` 連 NATS，載入 venue 的活躍表，並回答 `td.view.{api_id}`。OMS 只放活著的單，終態的 `OrderUpdate` 發出之後就刪掉；`td.view` 的 `Recon` 只回活著的單。OMS 與交易所之間以 `order_id` 對單（事件、recon、查單）。交易所還沒給號的單，第一則帶著同一個 `client_order_id` 的訊息先對上，再記下 `order_id`。沒有 client id 的單只以 `order_id` 追蹤，不發起查單。系統裡面（策略、`td.order`、`Cancel`、`BatchCancel`、`td.view`、向外的 `OrderUpdate`）以 cloid 為鍵。recon 期間到的事件先排著，快照取代狀態、發 `Recon` 之後依序重放；`filled_qty` 與狀態不倒退，終態不復活。斷線期間從快照消失的掛單記成 `UNKNOWN`，接著查單：已有 `order_id` 用它，否則用 cloid（E06-D005、E06-D017、E06-D022）。交易所的寫法換成 UID，數量乘 `ContractSize`；SYM 沒有的商品丟掉。槓桿快取取自持倉的 leverage。
- **下單與撤單（E06-D018–E06-D028，E6-08、E6-09）：** `Start` 成功之後才訂 `td.order.{api_id}`。本地拒絕是回覆裡的 1xx，什麼都沒送出，不發 `td.evt`。檢查依序，先碰到的先回：
  - `Submit`、`Cancel` 的 `client_order_id` 是空的，是 106。TD 不解碼 `client_order_id`，把它當成不透明的字串（E06-D033）。
  - 冪等只記這次執行的記憶體（最近 24 小時或 10 萬筆，先到者），不持久化（E06-D018）。比的是 `uid`、`side`、`order_type`、`qty`、`quote_qty`、`price`、`tif`、`reduce_only`、`params`，不含 `session_id` 與 `recv_window_ms`；數量與價格用刻度相等（E00-D008）。內容不同回 106，不送出；內容相同回原本的結果，不送出，本地拒絕也算（E06-D019）。recon 取回、記憶體裡還沒有的活單：重疊的欄位相同回 `accepted`，不同回 106，而且這次不對的嘗試不記進記憶體。記憶體過期之後，OMS 裡還活著的單仍不重送。撤單不是同一筆 cloid 的重送。
  - 商品要在活躍表且 `TRADABLE`，否則 106。價格依方向對齊 tick（買單 `SnapDown`、賣單 `SnapUp`，E00-D008），數量對齊 `qty_step` 再檢查是否整張；不合上下限、限價沒有價格、side 或類型沒給、數量與 `quote_qty` 都沒有，都是 106。`ContractSize` 是 0 或 1 時不檢查整張。
  - venue 不在這個帳號種類的可交易清單是 105。`params` 的 `positionSide`、`posSide`、`positionIdx` 是 103。執行中不檢查持倉模式（E06-D016）。
  - `DEGRADED` 時新單是 102；`Cancel` 與 `BatchCancel` 不受影響（E06-D003、E06-D027）。`OrderRate` 在送出前檢查。`Submit` 用到 90%（`used * 10 >= limit * 9`）是 108（E06-D021）。`Cancel` 與 `BatchCancel` 不受這條 90% 限制，可用到 100%（E06-D028，修改 E06-D021）。`Cancel` 算一張。`BatchCancel` 算一筆，不問清單裡有幾張。放不進這一筆才是 108，什麼都不送、不排隊。本地拒絕與冪等重放不扣額度。交易所回報的 bucket 對得上才併進用量。429 與 418 依 `Retry-After` 停送，沒有就等到回報的 `ResetAt`，再沒有用 `-rate-hold`（預設 1 分鐘，E04-D020）。停送期間含查單在內都不送，回 108，不排隊、不重試。已經送出才收到 429 或 418 是讀得到的拒絕：下單發 `REJECTED`、撤單發 `CANCEL_REJECTED`，code 是 HTTP 狀態；108 只出現在 `td.order` 的回覆。
  - 通過的請求先回 `accepted`，再送出。這個 incarnation 裡某商品的第一筆會送出的單，先送含 `first_submit_ns` 的 `Report`，送失敗不擋這張單（E06-D001）。
  - `TradeStream` 可用就走它的 WS；不可用，或請求還沒寫出去，改走 REST，不拒單（E06-D024）。`DEGRADED` 時撤單走 HTTP，即使下單連線還在（E06-D003）。寫出去之後的錯誤是 `ErrUnknownOutcome`，不改走 REST、不重送。adapter 在送出前組不出請求是 `REJECTED`，code 106，不查單。
  - 下單沒有回答：OMS 記 `UNKNOWN`，發 `OUTCOME_UNKNOWN`。撤單沒有回答：`status` 不變，`cancel_sent_ns` 保留（E06-D010）。查單時點是送出時間＋這筆解析後的有效期＋時鐘誤差，在那之前不查。已有 `order_id` 時查單只帶它，還沒有才用 `client_order_id`（E06-D017）。查到發 `OrderUpdate`，cause `RECONCILED`；查無此單是 `REJECTED`，code 120。查詢失敗從 1 秒加倍到 30 秒並持續重試；從第一次失敗起超過 5 分鐘，帳號標成 `DEGRADED`，reason `query_failed`，不蓋掉已經是 `stream_lost` 的（E06-D022）。事件連線斷線會把 `query_failed` 改成 `stream_lost`。每一筆失敗中的查單都有結論（查到或 `ErrNotFound`），而且事件連線在，才回到 `READY`。限速停送不算查單失敗。recon 把單記成 `UNKNOWN`、又沒有在途的查單時點時，立刻查。
  - `recv_window_ms` 為 0 用 `-validity`（0 是 5 秒）；負值當 0；超過 60 秒壓到 60 秒（E06-D020，修改 E06-D017）。時鐘誤差是 `-clock-skew`，預設 1 秒。目標單已有 `cancel_sent_ns` 時再撤，回 `accepted`，不再送。`Cancel` 只收 `client_order_id`（欄位 4 已刪，號碼不重排，E06-D029）。OMS 以 cloid 找到活單，送出時帶上那張單的 `order_id`；還沒有才用 `client_order_id`。`UNKNOWN` 可以用 cloid 撤，不回 107（E06-D023）。
  - `BatchCancel`（E06-D025–E06-D028）帶明確的 `client_order_id` 清單（`order_id` 欄位已刪，號碼不重排，E06-D029）。TD 以 client id 找到 OMS 裡的活單，送出時帶上那張單的 `order_id`（還沒有才用 client id），轉成一筆 `CancelBatch`。不挑單、不檢查歸屬、不分批、不排隊、不重試，也不走 `TradeStream`。解不開成平台 cloid 的 client id 不屬於任何 algo，但仍能指名那張單。空清單、或其中一筆沒有 `client_order_id`，是 106。`Limits.CancelBatch` 為 1 時是 106，reason 說明這個方法不能用，不改走單筆 `Cancel`。長過交易所上限仍送出，交易所的拒絕原樣記在各張單上。清單裡任何一張對不到活單，整筆 107，什麼都不送。任何一張的商品解不出來，整筆 106，什麼都不送；別的 `BatchCancel` 不受影響。通過之後回 `accepted`（回覆的 `client_order_id` 是空的），每一張先發 `CANCEL_REQUESTED` 並設定 `cancel_sent_ns`，已經在途的也照送。這則訊息沒有 `recv_window_ms`，有效期用 instance 預設。整筆結果不明（`ErrUnknownOutcome`，或沒有逐張的結果）時，批內每張維持原狀態與 `cancel_sent_ns`，各自查單（已有 `order_id` 用它，否則用 cloid），不重送（E06-D026、E06-D022）。整筆或其中一張被讀得到地拒絕，那張發 `CANCEL_REJECTED`。`DEGRADED` 照常（E06-D027）。一次撤掉某 algo 的全部掛單是 STS SDK 的事：SDK 照 `ViewReply.cancel_batch` 自己拆批，沒有批次撤單就改走單筆 `Cancel`（E06-D031）。`AccountSettings.cancel_on_disconnect_s` 與 `CancelOnDisconnect` 不接（E06-D025）。
- **其他：**
  - 每次 recon 都會送 Report（`reconciled_at_ns` 變了）。
  - TD 還沒有 live config：收到 `Live` 照樣回 `Applied`。
  - REST 與私有 websocket 的 host 是 TD instance 依帳號種類的部署設定，由 td-agent 的 `-endpoints`、`-rest-endpoints`、`-simulated` 決定，經 `InitPayload.rest_url`、`stream_url`、`simulated` 交給 worker（E06-D016）。

**td-agent（E06-D016，E6-10）。** 讀 `/td/{inst}/demand/` 與 `/td/{inst}/account/`。一個 `api_id` 有需求就跑一顆 account worker（worker key `account/{api_id}`），最後一個需求離開後等 linger（預設 5 秒）才停。帳號不存在、停用、種類不認得、`kind_unsupported`、憑證打不開，ack 是 `REJECTED`，不跑 worker。worker 回報 `READY` 或 `DEGRADED` 時 ack 是 `READY`；以 `auth`、`hedge_mode`、`kind_unsupported` 結束時是 `REJECTED`，不重拉；重拉用完是 `DEAD`。spawn 時才解封憑證，並簽一份這個帳號專屬、不到期的 NATS user。TD 不檢查 `algo_ids`：STS agent 檢查自己的 session（E06-D016）。

## 7. C6：資料面（NATS）

| subject | 型態 | 發佈者／服務者 | 訂閱者／呼叫者 |
|---|---|---|---|
| `md.v1.f.{inst}.{topic}.{ticker}` | pub | MD conn worker | session worker |
| `md.v1.view.{inst}.{topic}.{ticker}` | req | 持有該 feed 的 MD conn worker | session worker |
| `md.v1.fetch.{venue}` | req | 連得到該 venue 的 md.fetch replica（queue group，E04-D018、E04-D056） | session worker |
| `td.order.{api_id}` | req | TD account worker | session worker |
| `td.view.{api_id}` | req | TD account worker | session worker |
| `td.evt.{api_id}` | pub | TD account worker | session worker、API |
| `td.backfill.{venue}` | req | 連得到該 venue 的歷史補正 replica（queue group，E06-D001） | 尚未定（§10 P18）。不是 TD agent |
| `sym` | req | SYM（queue group） | session worker（經 SDK）、account worker、conn worker、md.fetch、歷史補正服務（§7.3）。MD agent、TD agent、API 不查 |
| `sym.v1.changed.{venue}.{category}` | pub | SYM | 載入了該 `(venue, category)` 的進程（§7.3） |

- 資料面不用 NATS header：subject 決定訊息種類，payload 是該種類的 wrapper。`md.f` 的 payload 前面多一段固定長度的 envelope（v1 是 40 bytes，§7.1），給 SDK 的 prehook 不解碼就能讀（E08-D003、E04-D005–E04-D011）。
- 沒有給策略看的可用性廣播（E08-D001）。存活由 controller 依 ack／nack 處理（§3.5）。
- **行情 subject 是 `md.v1.f.{inst}.{topic}.{ticker}`（E04-D055）。** feed 對 atom 1:1（E04-D001），atom 不出現在資料面上。`inst` 是 instance 名稱（§2），一個字串，不綁 region，subject 也不另帶 region；P16 仍只定多 region 的拓撲。兩個 MD instance 持有同一個 atom 時 subject 不同，session 只訂綁定的那個 instance。版號在前段，現在是 `v1`：新舊版 conn worker 並存時，新的發在新的根，舊的繼續發舊的，平台內不做遷移（E01-D006）。這是 §1 的 C6：不相容時換 subject 根。行情 subject 不帶 venue。`topic` 是 `exchange.Topic` 的字串（§2），例如 `trade`、`aggtrade`、`orderbook_delta`、`bestquote`、`kline_<interval>`。`ticker` 是 UniversalTicker，原樣放入、不轉義：它不含 `.`（§2），也不含 `*`、`>`，所以是一個 token。
- **行情的 subject 由 MD agent 給，SDK 不自己組（E01-D006、E04-D055）。** feed 用 `FeedBinding.subject`；selector 成員用 `SelectorBinding.subject_prefix`（`md.v1.f.{inst}`）加上 `.{topic}.{ticker}`；`md.view` 是把開頭的 `md.v1.f.` 換成 `md.v1.view.`，得到 `md.v1.view.{inst}.{topic}.{ticker}`。
- **`md.v1.view.{inst}.{topic}.{ticker}` 由持有該 feed 的 conn worker 服務（E04-D002、E04-D048、E04-D055）。** 保留 `inst`，同一個 subject 仍只有該 instance 裡持有這個 feed 的那一個 worker 回答。feed 進入 `wanted` 時訂閱。從清單移除或變成 `REJECTED` 時退訂並清掉 ring。在表上但不是 `TRADABLE`（`PENDING`）時保留已有的 ring 與訂閱。沒有 responder 就是沒有 worker 持有這個 feed。linger 在 agent，不在 conn worker（E04-D043、E09-D005）。
- **`md.v1.fetch.{venue}` 是無狀態服務（E04-D002、E04-D018–E04-D023、E04-D056）**，和 SYM 一樣不歸任何 agent 管。venue 是人讀的名字，與 `sym.v1.changed.{venue}.{category}` 的 venue 相同。每個 replica 只訂閱部署設定裡它連得到的 venue，queue group 的名稱是 subject。程式不選 region：本地有 responder 就由本地回答。conn worker 自己要快照時在進程內呼叫 REST reader，不經過 `md.fetch`。`limit` 是 1 到 5000，不在這個範圍回 `invalid`；超過 `KlinePage()` 時服務以 `Klines(endNs)` 從已拿到的最早一根往回翻，只回已收盤的，由舊到新。某一頁失敗就整筆失敗。回覆達到或超過 1 MiB 回 `invalid`，不送出。`Usage(resp, now)` 記下交易所回報的用量與 `ResetAt`（含同一 IP 上的 conn worker）。到 `ResetAt` 該 bucket 作廢，預先拒絕解除。達到設定的閾值就回 `rate_limited`。429 與 418 依 `Retry-After` 停送；429 沒有 `Retry-After` 就等到 `ResetAt`，兩者都沒有才用可設定的保守預設（行程預設 1 分鐘）。418 期間不送任何請求，也不探測。不排隊、不重試。閾值未設定則不因用量預先拒絕。各 venue 的真實窗口與標頭在 E4-09 到 E4-11 確認。不快取。ticker 只解活躍表；不在表裡、或 venue 與 subject 不符，回 `invalid`。`ErrUnsupported` 回 `unsupported`，其他讀取失敗回 `venue_error`。
- **TD 壞掉時下單失敗並帶原因（E08-D001、E06-D010）：** `td.order.{api_id}` 沒有 responder（account worker 不在，或還沒完成 `Start`），SDK 把它當成本地拒絕 101（`ACCOUNT_OFFLINE`），什麼都沒送出。account worker 自己的本地拒絕（降級、雙向持倉、規格不合等，§7.2 的表）在 `td.order` 的回覆裡，不發到 `td.evt`。
- **`td.order` 逾時不是 1xx（E06-D005）。** 單可能已經到了交易所。SDK 把它以 `ORDER_STATUS_UNKNOWN` 交給策略，不查 `td.view`、不重送；之後這個 `client_order_id` 的 `OrderUpdate` 來了照常更新。
- **交易所沒有回答的單由 worker 主動查（E06-D005、E06-D018–E06-D028）。** 下單得到 `ErrUnknownOutcome` 時，OMS 記成 `UNKNOWN` 並在 `td.evt` 發出；撤單得到時 `status` 不變，`cancel_sent_ns` 保留（E06-D010）。一批撤單整批結果不明時，批內每張單同樣如此，各自以 cloid 查、不重送（E06-D026）。查單時點是送出時間＋這筆解析後的有效期＋時鐘誤差，在那之前不查。查到就照狀態發 `OrderUpdate`（cause `RECONCILED`），查無此單是 `REJECTED`，code 120（`NOT_PLACED`）。查詢失敗從 1 秒加倍到 30 秒並持續重試；超過 5 分鐘帳號 `DEGRADED`（reason `query_failed`），每一筆都有結論且事件連線在就回到 `READY`（E06-D022）。不重送原單。`recv_window_ms` 為 0 用 TD instance 的預設，超過交易所上限就壓到上限（E06-D020）。cloid 冪等只記這次執行的記憶體（24 小時或 10 萬筆，先到者），不持久化：內容不同回 106，內容相同回原結果（E06-D018、E06-D019）。`TradeStream` 不可用時改走 REST（E06-D024）。細節在 §6.3。
- **`td.view` 只讀本地快照（E06-D006、E06-D033）。** `Recon`（活單、持倉、餘額一份，在同一個 seq 上）與槓桿都從 account worker 的記憶體回答，不打交易所。快照還沒好也照樣回；`ViewReply` 帶 `state`、`reconciled_at_ns`、`incarnation`、快照涵蓋到的 `td.evt` seq（E06-D010），以及這個帳號的 `cancel_batch`（`Limits.CancelBatch`，E06-D031）。`Recon` 只帶活著的單，不篩 algo。
- **只支援單向持倉（E06-D007）。** `Start` 時是雙向持倉就 `INIT_FAILED`（`hedge_mode`）；執行中不檢查（E06-D016）；`Submit.params` 的 `positionSide`、`posSide`、`positionIdx` 一律以 103 拒絕。
- **TD 降級（E06-D003、E06-D022、E06-D027）。** 私有 websocket 斷線期間，`Submit` 以 102（`ACCOUNT_DEGRADED`）拒絕，`Cancel` 與 `BatchCancel` 照常走 HTTP。查單失敗超過 5 分鐘是同一個 `DEGRADED`，reason `query_failed`：新單仍是 102，撤單與查單照常；每一筆失敗中的查單都有結論、而且事件連線在，才回到 `READY`。`stream_lost` 不改寫成 `query_failed`；事件連線斷線會把 `query_failed` 改成 `stream_lost`。重連並 recon 之後，在 `td.evt` 發一則 `Recon`，之後才恢復接受新單。事件連線的計畫性換線也發一則 `Recon`（reason `RECONNECT`），但不進 `DEGRADED`，新單不中斷（E06-D035–E06-D037）。account worker 換 incarnation 之後也發一則 `Recon`。TD 的 ack 維持 `READY`。
- **第一份 `Recon` 由 SDK 自己取得（E06-D003、E06-D033）：** `OnStart` 之後，SDK 以 `td.view` 的 `ReconRequest` 取得一份，當作第一次 `OnRecon` 交給策略。
- **session worker 的 NATS 權限（E09-D006、E08-D018–E08-D022、E08-D027）。** 每個 session 一份 user JWT，由 STS agent 依 spec 簽發。清單由 `SessionPermissions` 產生：去重後排序，只列 allow，沒有 deny。
  - publish：每個綁定的 `api_id` 一條 `td.order.{api_id}`、`td.view.{api_id}`；每個宣告的 feed 一條 `md.v1.view.{inst}.{topic}.{ticker}`；以及 `md.v1.fetch.>`、`sym`。一期沒有 `log.sts.{session_id}`：worker 的輸出只寫進 shim 的檔案（§4.5 S4、DECISIONS E10-D015、E10-D030）。沒有帳號就沒有 `td.order`／`td.view`。沒有 feed 就沒有 `md.v1.view`。
  - subscribe：每個宣告的 feed 一條 `md.v1.f.{inst}.{topic}.{ticker}`；每個綁定的 `api_id` 一條 `td.evt.{api_id}`；以及 `_INBOX_{session_id}.>`、`sym.v1.changed.>`。沒有帳號就沒有 `td.evt`。沒有 feed 就沒有 `md.v1.f`。
  - `inst` 由呼叫端解析後傳入，函數不讀 spec 上的 `md_instance`，不連 etcd、NATS。`api_id` 的十進位、instance、`session_id`、topic、ticker 含 `.`、`*`、`>` 或空白，或是空字串，就不簽，回 error，不轉義。`api_id` 小於等於 0 回 error（§2）。spec 含 `selectors` 時回 error，不展開。
  - 授權只在連線時做一次，訊息不帶 token。對沒綁定的帳號發 `td.order`，broker 直接拒絕。
  - 簽發時檢查 CONNECT 引數的長度（E08-D027）。natsdev 產生的 server 設定 `max_control_line` 是 262144，寫進部署用的 `nats-server.conf`（`deploy/dev` 的 compose 與 README）。檢查在 `internal/sts/issuer.SignSession`，不在 `SessionPermissions`，也不在 SDK。`SessionPermissions` 只列出 subject，不簽 JWT。server 比較的是 CONNECT 的 JSON 引數（不算動詞與 CRLF）。簽發處量的是 session 連線會送出的那一行：user JWT、nonce 簽章、client name 留空、headers 開。長度大於 262144 就拒絕這份 spec，錯誤寫明 feed 過多，JWT 不交出。auth callout 不在這一版。
- **TD agent 沒有 NATS user（E06-D039）。** 不發憑證，不 publish、不 subscribe，沒有 inbox。account worker 的憑證仍由它以 account 的簽章金鑰簽（E06-D011），那是簽發，不是它自己連線。
- **account worker 的 NATS 權限（E06-D011）。** 每個 `api_id` 一份 user JWT，由 TD agent 在 spawn 時以 account 的簽章金鑰簽，經 `tdctl.InitPayload.nats` 送進去，不放環境變數、不落檔：
  - publish：`td.evt.{api_id}`、`sym`。
  - subscribe：`td.order.{api_id}`、`td.view.{api_id}`、`sym.v1.changed.>`、`_INBOX_td_{api_id}.>`。
  - 回答 `td.order`、`td.view` 時帶 response permission：一則、兩分鐘。
  - 一顆 worker 只服務自己的帳號：同一個帳號被兩顆 worker 同時服務，同一筆請求會送兩次，而交易所只在掛單之間檢查 `client_order_id` 唯一。
- **MD instance 的 NATS 權限（E04-D015）。** 每個 MD instance 一份 user JWT，憑證是 `MFTIKO_NATS_CREDS`：
  - publish：`md.v1.f.{inst}.>`、`sym`。
  - subscribe：`md.v1.view.{inst}.>`、`sym.v1.changed.>`、`_INBOX_md.>`。conn worker 在 Init 以 `List` 載入自己那個 venue 的活躍表，並且只訂自己的 venue：`sym.v1.changed.{venue}.>`（E04-D057）。JWT 仍是 `sym.v1.changed.>`，一個 MD instance 可以有多個 venue。通知到了，或定期 revision 比對落後時，重載該表（E02-D003、E04-D026）。
  - 回答 `md.view` 時帶 response permission：一則、兩分鐘（NATS `allow_responses` 的預設）。不能發佈到 `_INBOX.>`。
- **md.fetch 的 NATS 權限（E04-D015、E04-D018、E04-D022、E04-D056）。** 無狀態服務，不是 MD worker（E04-D002）。每個 md.fetch instance 一份 user JWT：
  - publish：`sym`（恰好這個 subject，不是 `sym.>`）。
  - subscribe：`md.v1.fetch.>`（queue group；行程只訂部署清單裡的 `md.v1.fetch.{venue}`）、`sym.v1.changed.>`（NATS Resolver）、`_INBOX_fetch.>`。恰好 subject `md.v1.fetch` 不能訂。
  - 回答 `md.v1.fetch.{venue}` 時同樣是一則、兩分鐘，不能發佈到 `_INBOX.>`。
- **平台 inbox 前綴（E04-D016）。** session 仍是 `_INBOX_{session_id}`（E09-D006）。平台行程是 `_INBOX_{plane}`，不含 instance：MD 是 `_INBOX_md`，md.fetch 是 `_INBOX_fetch`，歷史補正是 `_INBOX_backfill`。TD agent 沒有 inbox（E06-D039）。account worker 一份憑證一個前綴，是 `_INBOX_td_{api_id}`，和 session 一樣（E06-D011）。同一個平面的 instance 共用前綴，一把平面憑證可以讀到該平面每一個 instance 的回覆。系統是單人使用，跨 region 取資料是設計目的，因為有些交易所鎖 region，要另一個 region 轉發。權限只開自己平面的前綴，沒有人拿到 `_INBOX.>`，也沒有人拿到 `_INBOX_td.>`。`natsx` 以 `Options.InboxPrefix` 設定這個前綴。SYM 的 user 權限留到它自己的票。
- **`td.backfill.{venue}` 是無狀態服務（E06-D001），比照 md.fetch。** 不歸 TD agent 管，沒有 lease、fencing，不落庫。venue 是人讀的名字。每個 replica 只訂部署清單裡它連得到的 venue，queue group 的名稱是 subject。恰好 subject `td.backfill` 不能訂。誰呼叫、誰把解開的憑證放進請求見 §10 P18，不是 TD agent。服務用完不留，也沒有 TD 的私鑰。服務啟動時載入這些 venue 的活躍表，訂閱 `sym.v1.changed.>`。REST 拿回的 `(venue, category, exch_ticker)` 在這份表換成 ticker。不在活躍表的這一筆，以 `include_inactive` 直接問 Reverse，不進快取；兩張表都沒有，這筆這輪跳過，確認線不越過它。限速比照 `md.fetch`（E04-D020）。
- **歷史補正服務的 NATS 權限（E06-D001，比照 E04-D015）。** 無狀態服務，不是 account worker。每個 instance 一份 user JWT：
  - publish：`sym`（恰好這個 subject，不是 `sym.>`）。
  - subscribe：`td.backfill.>`（queue group；行程只訂部署清單裡的 `td.backfill.{venue}`）、`sym.v1.changed.>`、`_INBOX_backfill.>`。恰好 subject `td.backfill` 不能訂。
  - 回答 `td.backfill.{venue}` 時同樣是一則、兩分鐘，不能發佈到 `_INBOX.>`。
- **帳號只交易它的種類可交易的 venue（E03-D001）。** `Submit` 的商品所屬的 venue 不在帳號種類的清單裡，account worker 以 105（`VENUE_NOT_TRADABLE`）拒絕，不送出。
- **數量與價格的單位（E02-D003）。** `qty`、`filled_qty`、`Fill.qty`、`Position.size` 的單位是 `contract_value_ccy`（qty = 張數 × `contract_size`）：linear 與期權是 base，inverse 是 quote，不是張數。`quote_qty` 的單位是 quote。`price`、`avg_price`、持倉的 `entry_price` 與 `mark_price`，以及 `price_tick`，用 `price_ccy`：現貨與期貨（含 inverse）是 quote，幣本位期權是 base，U 本位期權是 quote（穩定幣）。`unrealized_pnl` 用 `settlement_asset`（契約欄位 `settle`），是損益結算幣，不是保證金幣。不是 `TRADABLE` 的商品，交易路徑拒單。見 §7.3。
- **策略只能動自己的單（E06-D002、E06-D025、E06-D033）。** 由 SDK 保證，TD 不檢查：SDK 只允許使用 spec 裡宣告給該帳號的 algo_id，`client_order_id` 由 SDK 編碼，也只把自己的單放進 `BatchCancel`；以 cloid 的 algo 欄位把 `Recon` 的活單篩成自己的。其他語言的 SDK 由 conformance 驗 cloid 版面（§9.8）。TD 不解碼 `client_order_id`；OMS 仍以 `order_id` 追蹤沒有 client id 的單。這是防呆，不是防惡意。平台不替策略撤單，STS agent 沒有 `td.order` 的權限。
- **成交發在 `td.evt`，目前沒有人落庫（E06-D038）。** account worker 發布所有 `Fill`，以及 `order` 是終態且 `filled_qty > 0` 的 `OrderUpdate`。沒有訂閱者為了落庫而收。由誰收、誰寫 Postgres 見 §10 P18，不會是 TD agent。走 core NATS；漏收怎麼補是落庫那一方的事，不用 JetStream。

### 7.1 MD

**`md.f` 的訊息 = 40-byte envelope ＋ protobuf `FeedEvent`（E08-D003、E04-D005–E04-D011）。** v1 的長度由版本決定，固定 40 bytes，payload 緊接在後面。8-byte 欄位對齊 8，little-endian。讀寫用 `encoding/binary`，不用 unsafe：NATS 的 `msg.Data` 不保證 8-byte 對齊（E04-D007）。

| offset | 長度 | 欄位 |
|---|---|---|
| 0 | 1 | `version`（uint8）。大版本，目前是 `1`。只有行情 schema 的 breaking change 才升；envelope 加欄位也算 breaking（E04-D005）。不是 `1`，讀取端拒收 |
| 1 | 7 | 保留，寫入填 0。讀取端檢查，有一個不是 0 就拒收。v1 沒有長度 byte：原本的 `len` 拿掉，不留一個必須等於 40 的 byte。保留欄位一律是 0，讀取端只檢查這一件事（E04-D006）。比 40 bytes 短的訊息拒收 |
| 8 | 8 | `md_ts_ns`（int64）：conn worker 發佈的時間，MD 主機的時鐘 |
| 16 | 8 | `conn_epoch`（uint64）。傳輸層的連續性用 `(conn_epoch, seq)`，連 prehook 丟掉的和 NATS 漏掉的都算在內；tape 與 `md.view` 也用它對齊（E04-D009）。conn worker 重啟之後繼續單調遞增，這是 conn worker 的事 |
| 24 | 8 | `seq`（uint64）：以 `(feed, conn_epoch)` 起算，同一條連線上連續；重連後從 1 重新起算。交易所自己的序號（Binance `U`／`u`／`pu`、Bybit `seq`）在 payload 裡，兩層並存（E04-D009）。`orderbook_delta` 例外：那個序號只給 `BookSync.Check`，不進 envelope，也不進 payload（E04-D025） |
| 32 | 8 | `digest`（uint64）：正規化後的 payload（不含 envelope）的 xxHash64，seed 0（E04-D010）。以後做跨連線去重。這版不去重，讀取端也不核對 digest，照讀出 |
| 40 | — | protobuf `FeedEvent` |

- 讀取端拒收長度小於 40、`version` 不是 1、或保留 byte 不是 0 的訊息。SDK 丟掉這種訊息，計入 `QueueStat.dropped`，寫 log，session 不結束（E08-D049）。
- SDK 的 prehook 只讀 `md_ts_ns`：`now − md_ts_ns > recv_window_ms` 就丟，不解碼 payload。`orderbook_delta` 例外，不進 prehook（DECISIONS E08-D003）。
- **商品在 payload 裡（E08-D065）。** subject 屬於協議，不屬於訊息：訊息剝開之後，策略只拿得到 payload。每一則 `FeedEvent` 都帶 `uid`（SYM 分配的 UID，E02-D003），payload 裡沒有 ticker 字串。conn worker 從活躍表取得 UID；`digest` 算的 payload 包含它。
- 交給策略的事件帶 `uid`、`md_ts_ns`、`conn_epoch`、`seq`，加上 `Meta.exch_ts_ns`。`digest` 留在 envelope 上。
- ring 存的是這整段 bytes（envelope ＋ payload）。`md.view` 的 tape 去掉 envelope，不帶 `digest`（E04-D035）。

```proto
// file: mftiko/md/v1/md.proto
syntax = "proto3";

package mftiko.md.v1;

import "mftiko/common/v1/common.proto";

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1;mdv1";

// seq、conn_epoch、md_ts_ns 移到 envelope（上表）。
message Meta {
  int64 exch_ts_ns = 3;
}

message FeedEvent {
  uint32 uid = 13; // 商品（E02-D003、E08-D065）。每一則行情都帶；payload 裡沒有 ticker 字串
  oneof event {
    BestQuote best_quote = 1;
    Trade trade = 2;
    AggTrade agg_trade = 3;
    Kline kline = 5;
    Ticker ticker = 6;
    Liquidation liquidation = 7;
    FundingRate funding_rate = 8;
    OpenInterest open_interest = 9;
    Greeks greeks = 10;
    MarkPrice mark_price = 11;
    OrderBookDelta order_book_delta = 12;
  }
}

message BestQuote {
  Meta meta = 1;
  double bid = 2;
  double bid_qty = 3;
  double ask = 4;
  double ask_qty = 5;
}

message Trade {
  Meta meta = 1;
  string trade_id = 2;
  double price = 3;
  double qty = 4;
  mftiko.common.v1.Side side = 5; // taker
}

message AggTrade {
  Meta meta = 1;
  string trade_id = 2;
  double price = 3;
  double qty = 4;
  mftiko.common.v1.Side side = 5;
  string first_trade_id = 6;
  string last_trade_id = 7;
  uint32 match_count = 8;
}

message Level {
  double price = 1;
  double qty = 2;
}

// orderbook_delta：本地全量 book 的增量（E04-D002、E04-D053）。qty 為 0 表示刪除該價位。
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
  double open = 4;
  double high = 5;
  double low = 6;
  double close = 7;
  double volume = 8;
  optional double quote_volume = 9;
  bool closed = 10;
}

// 只有 24h 統計（E04-D001）；venue 不推的欄位不填。
message Ticker {
  Meta meta = 1;
  optional double last = 2;
  optional double open_24h = 3;
  optional double high_24h = 4;
  optional double low_24h = 5;
  optional double volume_24h = 6;
  optional double quote_volume_24h = 7;
  optional double change_pct_24h = 8;
}

message Liquidation {
  Meta meta = 1;
  mftiko.common.v1.Side side = 2; // 被平倉部位的方向
  double price = 3;
  double qty = 4;
}

message FundingRate {
  Meta meta = 1;
  double rate = 2;
  int64 next_funding_ns = 3;
  optional uint32 interval_s = 4;
}

message OpenInterest {
  Meta meta = 1;
  double open_interest = 2;
  optional double open_interest_value = 3;
}

message Greeks {
  Meta meta = 1;
  optional double delta = 2;
  optional double gamma = 3;
  optional double vega = 4;
  optional double theta = 5;
  optional double rho = 6;
  optional double mark_iv = 7;
  optional double bid_iv = 8;
  optional double ask_iv = 9;
  optional double mark_price = 10;
  optional double underlying_price = 11;
}

message MarkPrice {
  Meta meta = 1;
  double mark_price = 2;
  optional double index_price = 3;
}
```

**行情讀取（E04-D002）。** `md.view` 問持有 atom 的 conn worker，回答它記憶體裡的狀態；`md.fetch` 問無狀態服務，轉發交易所的公開 REST。

- **tape 的接法：** SDK 不自動向 `md.view` 要 tape，也不扣住 live；live 立即交付，仍走 prehook（E08-D047）。策略要歷史時自己呼叫 `Tape`。`TapePage.head` 是回覆當下這個 feed 已發佈到 `md.f` 的最新 `(conn_epoch, seq)`。策略把小於等於 `head` 的 live 當成重複，大於 `head` 的當成新的。ring 裡沒有事件時，`head` 是目前的 `conn_epoch`、seq 0。由新到舊，下一頁用 `next` 當 `before`。`since_ns` 只留 `md_ts_ns` ≥ `since_ns` 的事件，再舊就停，`more` 為假（E04-D036、E04-D037、E08-D047）。
- **ring：** 一個 feed 一個，只錄 `trade`、`aggtrade`、`liquidation`（E04-D047、E04-D049）。每個 feed 10 分鐘或 100000 筆，先到先砍；worker 合計 256 MiB，這是全部 tape 記憶體，超過就回收最舊的 64 KiB chunk（E04-D058）。大於 32 KiB 的事件改記 gap（E04-D059）。0 在 `InitPayload` 表示這些預設（§6.2）。conn worker 單一寫入。`md.view` 的讀取不拿鎖：先二分 chunk，再二分 slot，以 chunk generation 確認沒被覆蓋（E04-D050、E04-D059）。`before` 與 `since_ns` 這樣定位，再往回讀最多 `limit` 筆，到約 900 KiB 停止。讀取不掃整個 ring，也不 trim（E04-D051）。`limit` 為 0 是 1000，超過 5000 是 `invalid`（E04-D039）。重連在 ring 裡記 gap，ring 跨 `conn_epoch` 保留，seq 從 1 起（E04-D040）。`FeedEnd` 已從契約拿掉（E04-D053、E04-D054）。
- **`orderbook_delta` 的接法（E08-D057–E08-D061）：** SDK 維護本地 book（E08-D005）。快照只向 MD 的 `md.view` 要，內容是 conn worker 記憶體裡現在的窗內全本（E04-D030、E04-D041），SDK 不向交易所要。拿到的 `(conn_epoch, seq)` 是 S。等快照期間的增量留在內部緩衝，不是 hook；快照之後只套 `(conn_epoch, seq)` 大於 S、且與游標連續的。緩衝裡有洞就留下，再要一張。緩衝上限沿用 E04-D024：10000 筆或 10 秒（從這一次嘗試的第一筆起算，含等於），到了就丟掉緩衝、進行中的快照作廢、立刻再要。seq 跳號（包括 ring 丟掉的，E08-D003）或 `conn_epoch` 改變時，本地本清成未同步並重要。未同步不呼叫 `OnBook`。`not_synced` 與其他讀取失敗（逾時、沒有 responder）沿用 E04-D024 的退避：1 秒起、加倍、上限 30 秒；成功把窗口歸零；已經在退避或有快照在途，不另起一次（E04-D034、E04-D041）。`reset` 清掉本地本、標未同步、重要 `md.view`，不套這則訊息裡的檔位（E08-D061）；裝上的快照游標不得早於這次 `reset`。檔數用 `book.DefaultN`，與 MD conn worker 相同（E04-D030），不用快照的窗口大小；`BookSnapshot` 不帶檔數。tick 用這個 session 已 `Resolve` 的 `price_tick`；缺、或小於等於 0，只記日誌、不維護（E04-D033）。UID 還沒 `Resolve` 就維持未同步並記日誌，SDK 不自己問 SYM（E08-D023、E08-D059）。未同步時 `Book(uid)` 回 `(nil, false)`（E08-D060）。`OnBook` 只在已同步、這一則已經套進 book、而且 `md_ts_ns` 仍在 `recv_window_ms` 內時呼叫；太舊的 delta 仍套用、不呼叫。增量原樣轉發，窗外的檔在 `sdk/book` 套用時丟掉，視窗滑動不把丟掉的檔補回來（E04-D031、E04-D032）。book topic 只有 `orderbook_delta`（E04-D042、E04-D053）。Go 實作在 `sdk/internal/bookpath`。
- **K 線的接法：** `md.fetch` 拿最近 N 根已收盤的，再接 live 的 kline feed，以 `open_time_ns` 對齊。

```proto
// file: mftiko/md/v1/read.proto
syntax = "proto3";

package mftiko.md.v1;

import "mftiko/md/v1/md.proto";

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1;mdv1";

// ---------- md.v1.view.{inst}.{topic}.{ticker} ----------
// subject 最後一段是 UniversalTicker，原樣、不轉義（E04-D055）。NATS topic 用人讀的名字，不用 UID。
// 由持有該 feed 的 conn worker 回答（E04-D044、E04-D048）。回什麼由 subject 的 topic 決定（E04-D042）：
// trade、aggtrade、liquidation 回 tape；orderbook_delta 回現在的窗內 book（E04-D053）。
// 其他 topic 回 error = "unsupported"。沒有 responder 就是沒有 worker 持有這個 feed。

message ViewRequest {
  uint32 limit = 1;             // 只用於 tape。0 是 1000，上限 5000，超過是 invalid，不截短（E04-D039）
  optional Cursor before = 2;   // 只用於 tape：不含本身。第一頁不填（E04-D036）
  optional int64 since_ns = 3;  // 只用於 tape：只回 md_ts_ns ≥ since_ns 的事件（E04-D036）
}

// 以 (conn_epoch, seq) 的字典序比較。
message Cursor {
  uint64 conn_epoch = 1;
  uint64 seq = 2;
}

message ViewReply {
  string error = 1; // 空字串 = 成功；unsupported | not_held | invalid | not_synced
  oneof result {
    TapePage tape = 10;
    BookSnapshot book = 11;
  }
}

// 一則 tape 事件。不帶 envelope，也不帶 digest（E04-D035）。
// event 的位元組就是 ring 裡 md.f 訊息 offset 40 之後的 payload，原樣寫入，不重新編碼。
message TapeEvent {
  uint64 conn_epoch = 1;
  uint64 seq = 2;
  int64 md_ts_ns = 3;
  FeedEvent event = 4;
}

message TapePage {
  // 由新到舊（E04-D036）。
  repeated TapeEvent events = 1;
  bool more = 2;
  optional Cursor next = 3;      // 下一頁的 before。more 為假時不填
  int64 continuous_since_ns = 4; // 從這個時間起，留下的 tape 沒有 gap（E04-D040）
  repeated Gap gaps = 5;         // 這一頁涵蓋範圍內的 gap
  Cursor head = 6;               // 回覆當下這個 feed 已發佈到 md.f 的最新游標（E04-D037）
}

message Gap {
  int64 start_ns = 1; // 斷線的時間，md 主機時鐘
  int64 end_ns = 2;   // 重連完成的時間，md 主機時鐘
  Cursor last = 3;    // 斷線前最後一則。沒有發佈過則不填（E04-D040）
  Cursor first = 4;   // 重連後第一則。那則還沒進 ring 則不填（E04-D040）
}

// 這個 feed 最後套用的 (conn_epoch, seq) 的窗內 book（E04-D030、E04-D041）。窗外的檔不在這裡。
// 不支援指定歷史 seq。簿還沒同步好時 ViewReply.error 是 not_synced，不帶 book。
message BookSnapshot {
  Meta meta = 1;
  repeated Level bids = 2;
  repeated Level asks = 3;
  uint64 conn_epoch = 4;
  uint64 seq = 5;
  int64 md_ts_ns = 6;
  uint32 uid = 7; // 商品（E08-D065）
}

// ---------- md.v1.fetch.{venue} ----------
// 無狀態服務，轉發交易所的公開 REST。venue 是人讀的名字（E04-D018）。
// queue group，名稱是 subject。每個 replica 只訂閱它連得到的 venue。

message FetchRequest {
  oneof req {
    FetchKlines klines = 1;
    FetchOpenInterest open_interest = 2; // 交易所不推送、只能查詢的值（E04-D001）
  }
}

message FetchKlines {
  string interval = 2; // 正規寫法：1m、4h、1mo
  uint32 limit = 3;    // 最近 N 根已收盤的，1 到 5000；超出是 invalid。超過交易所單頁時由服務分頁（E04-D019）
  uint32 uid = 4;      // 商品（E08-D065）。subject 是 md.v1.fetch.{venue}，用人讀的名字（E04-D056）；UID 的 venue 必須是它
}

message FetchOpenInterest {
  uint32 uid = 2; // 商品（E08-D065）
}

message FetchReply {
  string error = 1; // 空字串 = 成功；invalid | unsupported | rate_limited | venue_error
  string reason = 2;
  uint32 uid = 3; // 請求的商品（E08-D065）
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

三個 subject（E06-D010）：

- **`td.order.{api_id}`**：request／reply。回覆只有兩種：`accepted` 為真，TD 收下了，接著送往交易所，不代表交易所接受；`accepted` 為假，本地拒絕，code 是 1xx，什麼都沒送出。之後的狀態一律從 `td.evt` 拿。`BatchCancel` 也一樣：回覆只說收下了，每一張單撤成或撤不成，各自是一則 `OrderUpdate`。同一個 incarnation 裡，見過的 `client_order_id` 回原本的回覆，不再送交易所（E06-D005）。account worker 完成 `Start` 之後才服務它。
- **`td.view.{api_id}`**：request／reply，只讀 account worker 的記憶體（E06-D006）。從連上 NATS 起就回答，快照還沒好也照樣回，由呼叫端看 `ViewReply` 的 `state`、`reconciled_at_ns` 判斷新舊。每一則回覆都帶 `cancel_batch`（E06-D031）。`ReconRequest` 回一份 `Recon`：所有活單、持倉、餘額在同一個 `seq` 上，不篩 algo（E06-D033）。
- **`td.evt.{api_id}`**：publish。一張單的每一次變化都是一則 `OrderUpdate`，拒單與撤單被拒也是；成交明細另外是一則 `Fill`。本地拒絕不發到這裡。同一張單的同一次變化只發一則：交易所的回答與私有事件都報了，worker 發佈前去重。

reject code 照抄 mftik 的分段：`0` 無；`100–199` TD 判定的拒絕，交易所上沒有這張單；`200–299` 已對照的交易所拒絕；其他值是交易所原生代碼。1xx 只列一期會碰到、有票驗收的，之後碰到新的情況再加（E06-D010）。`reason` 是給人讀的說明，程式以 code 判斷。

| code | `RejectCode` | 出現在 | 什麼時候 |
|---|---|---|---|
| 101 | `ACCOUNT_OFFLINE` | `td.order` 的回覆，由 SDK 產生 | `td.order` 沒有 responder：account worker 不在，或還沒完成 `Start` |
| 102 | `ACCOUNT_DEGRADED` | `td.order` 的回覆 | 私有連線斷線中，暫停收新單；`Cancel` 與 `BatchCancel` 照常（E06-D003、E06-D027） |
| 103 | `HEDGE_MODE` | `td.order` 的回覆 | 執行中發現帳號是雙向持倉，或 `params` 帶了 `positionSide`、`posSide`、`positionIdx`（E06-D007） |
| 105 | `VENUE_NOT_TRADABLE` | `td.order` 的回覆 | 帳號種類不能交易這個商品的 venue（E03-D001） |
| 106 | `INVALID_ORDER` | `td.order` 的回覆 | 商品不存在或不是 `TRADABLE`；數量或價格不合商品規格（tick、step、上下限）；`Submit`、`Cancel` 的 `client_order_id` 是空的（E06-D033）；`BatchCancel` 在沒有批次撤單的交易所上不能用（E06-D025） |
| 107 | `ORDER_NOT_OPEN` | `td.order` 的回覆 | 撤單的目標不是掛單：TD 不認得這張單，或它已經終結（例如撤單與成交擦身而過）。`BatchCancel` 的清單裡有一張對不到活單時，整筆是 107，什麼都不送（E06-D025） |
| 108 | `RATE_LIMITED` | `td.order` 的回覆 | `Submit` 用到 `OrderRate` 的 90%，`Cancel` 與 `BatchCancel` 用到 100%（`BatchCancel` 算一筆，不是一張），或交易所以 429、418 要求停送（E06-D009、E04-D020、E06-D021、E06-D028）。不排隊、不重試 |
| 120 | `NOT_PLACED` | `OrderUpdate`（`REJECTED`） | 主動查單後確定交易所沒有這張單（E06-D005） |

`td.order` 逾時不在表裡：它不是拒絕，是結果未知（E06-D005）。

`OrderStatus` 是單在交易所那一側的狀態（E06-D010）：

| `status` | 意思 |
|---|---|
| `PENDING_NEW` | TD 已送出下單，交易所還沒回答。TD 自己的標記，adapter 不產生 |
| `NEW` | 交易所收到請求，當下沒有拒絕；還沒上簿，之後仍可能被拒 |
| `ACCEPTED` | 在訂單簿上，還沒有成交 |
| `PARTIALLY_FILLED` | 部分成交；GTC 的剩餘仍在簿上 |
| `FILLED`、`CANCELLED`、`REJECTED`、`EXPIRED` | 終態 |
| `UNKNOWN` | 下單送出但交易所沒有回答，worker 查單之後收斂（E06-D005） |

`Balance` 的 `free`、`locked`、`prelock` 互不重疊，加總是總額（E08-D053）。`free` 是可用。`prelock` 是單已送出、交易所未確認的預扣，確認後轉入 `locked`。`locked` 是掛單凍結。三格都由 TD 填。SDK 不計算，`BalanceUpdate` 與 `Recon` 原樣覆蓋。

回覆本身就代表上簿的交易所從 `PENDING_NEW` 直接到 `ACCEPTED`。各交易所的對應：

| | `NEW` | `ACCEPTED` |
|---|---|---|
| Binance | 沒有這一步 | 回覆或私有事件的 `NEW`（撮合引擎已收下） |
| OKX | 下單回覆 `sCode` 0 | orders channel 的 `live` |
| Bybit | 下單回覆 `retCode` 0；`Created`、`Untriggered`、`Triggered` | `New` |

撤單在途不是狀態，是 `Order.cancel_sent_ns`：TD 送出撤單的時間，0 表示沒有撤單在途。撤單途中 `status` 照交易所的事件走（上簿、成交都照常更新），撤單被拒時只清掉 `cancel_sent_ns`，沒有要還原的狀態。目標單已經有撤單在途時再撤，回 `accepted`，不再送交易所。交易所自己的 pending-cancel 狀態（Binance `PENDING_CANCEL`、Bybit `PendingCancel`）是還在簿上的單：`ACCEPTED`，有成交就是 `PARTIALLY_FILLED`。

`OrderUpdate` 的 `cause` 與變化之後的 `status`：

| `OrderCause` | `status` | 什麼時候 |
|---|---|---|
| `SUBMITTED` | `PENDING_NEW` | TD 把下單送往交易所 |
| `ACKNOWLEDGED` | `NEW` | 交易所收到，當下沒有拒絕 |
| `ACCEPTED` | `ACCEPTED` | 上簿 |
| `FILLED` | `PARTIALLY_FILLED`、`FILLED` | 成交，明細在 `Fill` |
| `CANCEL_REQUESTED` | 不變 | TD 把撤單送往交易所（策略的 `Cancel`，或 `BatchCancel` 點名的每一張），設定 `cancel_sent_ns` |
| `CANCELLED` | `CANCELLED` | 撤掉了，不論誰撤的 |
| `CANCEL_REJECTED` | 不變 | 撤單被拒，清掉 `cancel_sent_ns`；`code`、`reason` 是交易所給的 |
| `REJECTED` | `REJECTED` | 交易所拒單（2xx 或原生代碼，同步或非同步），或查單後確定沒下成（120） |
| `EXPIRED` | `EXPIRED` | 交易所讓它過期，例如 IOC、FOK 沒成交的部分 |
| `OUTCOME_UNKNOWN` | `UNKNOWN` | 下單送出但交易所沒有回答（E06-D005）。撤單沒有回答時不發這則：`status` 不變，`cancel_sent_ns` 保留，同樣由 worker 查單收斂 |
| `RECONCILED` | 交易所的狀態 | recon 或主動查單之後，交易所的狀態和 TD 原本記的不同 |

進終態時一律清掉 `cancel_sent_ns`；之後才到的撤單結果不再發 `OrderUpdate`。

```proto
// file: mftiko/td/v1/td.proto
syntax = "proto3";

package mftiko.td.v1;

import "mftiko/common/v1/common.proto";

option go_package = "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1;tdv1";

// 一張單在交易所那一側的狀態（E06-D010）。出現在 OrderUpdate 與快照的 Order 裡。
// 撤單在途不是狀態，是 Order.cancel_sent_ns。PENDING_NEW 是 TD 自己的標記，adapter 不產生它。
enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;
  ORDER_STATUS_PENDING_NEW = 1; // TD 已送出下單，交易所還沒回答
  ORDER_STATUS_NEW = 2; // 交易所收到請求，當下沒有拒絕；還沒上簿，之後仍可能被拒
  ORDER_STATUS_ACCEPTED = 3; // 在訂單簿上，還沒有成交。回覆本身就代表上簿的交易所（Binance）從 PENDING_NEW 直接到這裡
  ORDER_STATUS_PARTIALLY_FILLED = 4; // 部分成交；GTC 的剩餘仍在簿上
  ORDER_STATUS_FILLED = 5;
  ORDER_STATUS_CANCELLED = 6;
  ORDER_STATUS_REJECTED = 7; // 交易所拒單（同步或非同步），或查單後確定沒下成（REJECT_CODE_NOT_PLACED）
  ORDER_STATUS_EXPIRED = 8; // 交易所讓它過期，例如 IOC、FOK 沒成交的部分
  ORDER_STATUS_UNKNOWN = 9; // 下單送出但交易所沒有回答，worker 查單之後收斂（E06-D005）
}

// 1xx 的名字（E06-D010、PROTOCOL §7.2）。代碼欄位是 int32，因為它也帶 2xx 與交易所原生代碼；這個 enum 只替 1xx 取名。
// 101–119 是本地拒絕，什麼都沒送出，只出現在 td.order 的回覆；120 起是送出之後確定沒下成，只出現在 OrderUpdate。
// 只列一期會碰到、有票驗收的；之後碰到新的情況再加。
enum RejectCode {
  REJECT_CODE_UNSPECIFIED = 0;
  REJECT_CODE_ACCOUNT_OFFLINE = 101; // td.order 沒有 responder：account worker 不在，或還沒完成 Start。由 SDK 產生
  REJECT_CODE_ACCOUNT_DEGRADED = 102; // 私有連線斷線中，暫停收新單；Cancel 與 BatchCancel 照常（E06-D003、E06-D027）
  REJECT_CODE_HEDGE_MODE = 103; // params 帶了持倉方向鍵（E06-D007）。雙向持倉的帳號在 Start 就起不來，執行中不檢查（E06-D016）
  REJECT_CODE_VENUE_NOT_TRADABLE = 105; // 帳號種類不能交易這個商品的 venue（E03-D001）
  REJECT_CODE_INVALID_ORDER = 106; // 商品不存在或不是 TRADABLE；數量或價格不合商品規格（tick、step、上下限）；client_order_id 是空的；同一 cloid 內容不同（E06-D019）；BatchCancel 在沒有批次撤單的交易所上不能用（E06-D025）
  REJECT_CODE_ORDER_NOT_OPEN = 107; // 撤單的目標不是掛單：TD 不認得這張單，或它已經終結。UNKNOWN 可以用 cloid 撤，不回 107（E06-D023）。BatchCancel 的清單裡有一張對不到活單時，整筆是 107，什麼都不送（E06-D025）
  REJECT_CODE_RATE_LIMITED = 108; // Submit 用到 OrderRate 的 90%，Cancel 與 BatchCancel 用到 100%（BatchCancel 算一筆，不是一張），或交易所以 429、418 要求停送（E06-D009、E04-D020、E06-D021、E06-D028）。不排隊、不重試
  REJECT_CODE_NOT_PLACED = 120; // 主動查單後確定交易所沒有這張單（E06-D005）
}

// account worker 的狀態（E06-D003、E06-D006）。td.view 的回覆與 tdctl.Report 都帶它。
enum AccountState {
  ACCOUNT_STATE_UNSPECIFIED = 0;
  ACCOUNT_STATE_READY = 1;
  ACCOUNT_STATE_DEGRADED = 2; // 私有 websocket 斷線，或查單失敗超過 5 分鐘：新單以 102 拒絕，Cancel 與 BatchCancel 照常（E06-D003、E06-D022、E06-D027）
}

// ---------- td.order.{api_id} ----------
// request／reply（E06-D010）。回覆只有兩種：ack（TD 收下了，接著送往交易所），或本地拒絕（1xx，什麼都沒送出）。
// 之後的狀態一律從 td.evt.{api_id} 拿。

message OrderRequest {
  oneof req {
    Submit submit = 1;
    Cancel cancel = 2;
    BatchCancel batch_cancel = 4;
  }
}

message Submit {
  string session_id = 1; // 只給 log 與歸屬
  uint32 uid = 2; // 商品（E02-D003）
  mftiko.common.v1.Side side = 3;
  mftiko.common.v1.OrderType order_type = 4;
  optional double qty = 5; // 單位是 contract_value_ccy（E02-D003）：qty = 張數 × contract_size。現貨、linear、期權是 base，inverse 是 quote，不是張數
  optional double quote_qty = 6; // quote 資產的金額，單位是 quote，不是 contract_value_ccy（E02-D003）
  optional double price = 7; // 單位是 price_ccy（E02-D003）：現貨與期貨（含 inverse）是 quote；幣本位期權的權利金是 base；U 本位期權的權利金是 quote（穩定幣）
  mftiko.common.v1.TimeInForce tif = 8;
  bool reduce_only = 9;
  string client_order_id = 10; // 冪等與查單的 key。TD 當成不透明的字串，不解碼；空的是 106（E06-D033）
  // 交易所特有的參數，原樣交給 adapter。positionSide、posSide、positionIdx 一律以 103 拒絕（E06-D007）
  map<string, string> params = 11;
  // 請求有效期，毫秒（E06-D020）。0 = 這個 TD instance 的預設（部署設定，預設 5 秒）。
  // 超過交易所上限（60 秒）時 TD 壓到上限。時鐘誤差是 instance 的設定，預設 1 秒，不在這則訊息裡。
  int64 recv_window_ms = 13;
}

// 目標單已經有撤單在途（cancel_sent_ns 不是 0）：回 accepted，不再送交易所（E06-D010）。
// 只收 client_order_id。OMS 以它找到活單，送出時帶上那張單的 order_id；還沒有 order_id 才用 client_order_id（E06-D023）。
message Cancel {
  string session_id = 1; // 只給 log 與歸屬
  uint32 uid = 2;
  string client_order_id = 3; // 空的是 106（E06-D033）
  // 請求有效期，毫秒（E06-D020）。0 = 這個 TD instance 的預設（部署設定，預設 5 秒）。
  // 超過交易所上限（60 秒）時 TD 壓到上限。時鐘誤差是 instance 的設定，預設 1 秒，不在這則訊息裡。
  int64 recv_window_ms = 6;
}

// 一筆交易所的批次撤單（E06-D025）。每一筆只帶 client_order_id。TD 以它找到 OMS 裡的活單，送出時帶上那張單的 order_id（還沒有才用 client_order_id）。
// 不挑單、不分批、不排隊、不重試，也不解碼 client_order_id（E06-D033）。
// 商品取自 OMS 裡那張活單的 UID。空的 client_order_id 整筆 106；對不到活單，整筆 107；商品解不出來，整筆 106。都什麼都不送，不影響別的 BatchCancel。
// CancelBatch 為 1（交易所沒有批次撤單）是 106，不改走單筆 Cancel。長過交易所上限仍送出，交易所的拒絕原樣回報。
// 這則訊息算 OrderRate 的一筆，與 Cancel 相同可用到 100%（E06-D028）。沒有 recv_window_ms，有效期用 instance 預設。
// 回覆只說收下了；每一張單撤成或撤不成，各自是一則 OrderUpdate（E06-D010）。DEGRADED 照常，走 REST，不走 TradeStream（E06-D027）。
// 整筆結果不明時，批內每張查單（已有 order_id 用它，否則用 client_order_id），不重送（E06-D026）。cancel-on-disconnect 不接。
message BatchCancel {
  repeated BatchCancelOrder orders = 4;
}

// BatchCancel 清單裡的一張單。只收 client_order_id。
message BatchCancelOrder {
  string client_order_id = 1;
}

// td.order 的回覆（E06-D010）。
message OrderReply {
  bool accepted = 1; // 真：TD 收下了，接著送往交易所。假：本地拒絕，什麼都沒送出
  int32 code = 2; // accepted 為假時是 1xx（RejectCode）
  string reason = 3;
  string client_order_id = 4; // Submit、Cancel 的 client_order_id；BatchCancel 是空的
}

// ---------- td.view.{api_id} ----------
// 只讀 account worker 的記憶體，不打交易所（E06-D006）。

message ViewRequest {
  oneof req {
    LeverageRequest leverage = 3;
    ReconRequest recon = 4;
  }
}

// 帳號的整份快照：所有活單、所有持倉、所有餘額，在同一個 ViewReply.seq 上（E06-D003、E06-D033）。
// 帳號由 subject 決定。不篩 algo：SDK 以 cloid 的 algo 欄位自己篩。
message ReconRequest {}

// 本地的槓桿快取：取自持倉的 leverage（recon 與持倉事件），持倉平掉之後仍保留；沒看過的商品回未知（E06-D006、E06-D015）。
message LeverageRequest {
  uint32 uid = 1;
}

// 快照還沒好也照樣回，不擋、不等：呼叫端看 state、reconciled_at_ns 判斷新舊（E06-D006）。
message ViewReply {
  oneof reply {
    LeverageInfo leverage = 3;
    mftiko.common.v1.Error error = 4;
    Recon recon = 5; // reason 是 RECON_REASON_REQUESTED（E06-D033）
  }
  AccountState state = 10;
  int64 reconciled_at_ns = 11; // 最近一次 recon 完成的時間；0 = 這個 incarnation 還沒完成第一次 recon
  uint32 incarnation = 12;
  uint64 seq = 13; // 這份快照已經涵蓋到的 td.evt seq：之後只要套用 seq 更大、incarnation 相同的事件
  // 這個帳號的 Limits.CancelBatch（E06-D031）。1 表示交易所沒有批次撤單，SDK 改走單筆 Cancel（E06-D025）。
  // 每一則回覆都帶，不論問的是 Recon 或槓桿。策略照這個數把一個 algo 的掛單拆成多筆 BatchCancel。
  uint32 cancel_batch = 14;
}

message LeverageInfo {
  uint32 uid = 1;
  optional double leverage = 2; // 不設 = 快取裡沒有這個商品
}

// ---------- td.evt.{api_id} ----------

message AccountEvent {
  uint64 seq = 1; // 每個 incarnation 從 1 起算
  mftiko.common.v1.WorkerRef source = 2;
  oneof event {
    OrderUpdate order_update = 10;
    Fill fill = 11;
    BalanceUpdate balance_update = 14;
    PositionUpdate position_update = 15;
    Recon recon = 17;
  }
}

// 一張單的一次變化（E06-D010）。order 是變化之後的完整狀態，cause 說為什麼變。
// 拒單（交易所拒單、查單後確定沒下成）也是一則 OrderUpdate，status 是 REJECTED。
// 撤單只動 cancel_sent_ns：送出時設定（CANCEL_REQUESTED），被拒時清掉（CANCEL_REJECTED），status 都不變。
// 本地拒絕只在 td.order 的回覆裡，不發到這裡。同一個 client_order_id 的同一次變化只發一則：交易所的回答與私有事件
// 都報了，worker 發佈前去重。
message OrderUpdate {
  Order order = 1;
  OrderCause cause = 2;
  int32 code = 3; // 只有 REJECTED、CANCEL_REJECTED：2xx、交易所原生代碼，或 120（NOT_PLACED）
  string reason = 4;
}

enum OrderCause {
  ORDER_CAUSE_UNSPECIFIED = 0;
  ORDER_CAUSE_SUBMITTED = 1; // TD 送出下單 → PENDING_NEW
  ORDER_CAUSE_ACKNOWLEDGED = 2; // 交易所收到，當下沒有拒絕 → NEW
  ORDER_CAUSE_ACCEPTED = 3; // 上簿 → ACCEPTED
  ORDER_CAUSE_FILLED = 4; // 成交 → PARTIALLY_FILLED 或 FILLED；明細在 Fill
  ORDER_CAUSE_CANCEL_REQUESTED = 5; // TD 送出撤單：設定 cancel_sent_ns，status 不變
  ORDER_CAUSE_CANCELLED = 6; // → CANCELLED：策略撤的、BatchCancel、交易所撤的都是
  ORDER_CAUSE_CANCEL_REJECTED = 7; // 撤單被拒：清掉 cancel_sent_ns，status 不變
  ORDER_CAUSE_REJECTED = 8; // → REJECTED
  ORDER_CAUSE_EXPIRED = 9; // → EXPIRED
  ORDER_CAUSE_OUTCOME_UNKNOWN = 10; // 下單送出但交易所沒有回答 → UNKNOWN。撤單沒有回答時 cancel_sent_ns 保留，同樣由 worker 查單收斂（E06-D005）
  ORDER_CAUSE_RECONCILED = 11; // recon 或主動查單之後，交易所的狀態和 TD 原本記的不同（E06-D005）
}

// 一張單的狀態紀錄：OrderUpdate、Recon 共用。拒絕的原因在 OrderUpdate 上，不在這裡（E06-D010）。
message Order {
  uint32 uid = 1; // 商品（E02-D003）
  string order_id = 2;
  string client_order_id = 3;
  mftiko.common.v1.Side side = 4;
  mftiko.common.v1.OrderType order_type = 5;
  OrderStatus status = 6;
  double qty = 7; // 同 Submit.qty，單位是 contract_value_ccy（E02-D003）
  optional double quote_qty = 8; // 同 Submit.quote_qty（E02-D003）
  optional double price = 9; // 同 Submit.price，單位是 price_ccy（E02-D003）
  double filled_qty = 10; // 已成交數量，單位同 qty（contract_value_ccy，E02-D003）
  optional double avg_price = 11; // 同 Submit.price，單位是 price_ccy（E02-D003）
  int64 exch_ts_ns = 13;
  int64 td_ts_ns = 14;
  // TD 送出撤單的時間；0 = 沒有撤單在途。撤單在途是旗標，不是 status（E06-D010）。
  // 撤單被拒時清掉；撤單沒有回答時保留，由 worker 查單收斂（E06-D005）；進終態時一律清掉，之後才到的撤單結果不再發 OrderUpdate。
  int64 cancel_sent_ns = 15;
}

message Fill {
  uint32 uid = 1; // 商品（E02-D003）
  // 交易所的 trade id。WS 與 REST 必須給同一個值：落庫以 (api_id, fill_id) 去重（E06-D001）。
  string fill_id = 2;
  string order_id = 3;
  string client_order_id = 4;
  mftiko.common.v1.Side side = 5;
  double price = 6; // 同 Submit.price，單位是 price_ccy（E02-D003）
  double qty = 7; // 成交數量，單位同 Submit.qty（contract_value_ccy，E02-D003）
  double fee = 8;
  string fee_asset = 9;
  string liquidity = 10;
  int64 exch_ts_ns = 11;
}

// 餘額。free、locked、prelock 互不重疊，加總是總額，三格都由 TD 填（E08-D053）。
// SDK 不計算：BalanceUpdate 與 Recon 原樣覆蓋。
message Balance {
  string asset = 1;
  double free = 2; // 可用
  double locked = 3; // 掛單凍結
  // 單已送出、交易所未確認的預扣；確認後轉入 locked。
  double prelock = 4;
}

// 一期只支援單向持倉，size 帶號：負的是空（E06-D007）。
message Position {
  uint32 uid = 1; // 商品（E02-D003）
  double size = 2; // 持倉數量，單位同 Submit.qty（contract_value_ccy，E02-D003）
  optional double entry_price = 3; // 同 Submit.price，單位是 price_ccy（E02-D003）
  optional double mark_price = 4; // 同 Submit.price，單位是 price_ccy（E02-D003）
  optional double unrealized_pnl = 5; // 損益結算幣 settlement_asset（欄位 settle），不是保證金幣（E02-D003）
  optional double leverage = 6;
}

message BalanceUpdate {
  repeated Balance balances = 1;
}

message PositionUpdate {
  repeated Position positions = 1;
}

// 帳號狀態的整份快照（DECISIONS E06-D003、E06-D004、E06-D033）：td.evt 推的，或 td.view 的 ReconRequest 問到的。
// SDK 把它交給策略的 OnRecon：open_orders 由 SDK 以 cloid 的 algo 欄位篩成自己宣告的 algo_id，positions、balances 原樣給。
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
  RECON_REASON_RECONNECT = 1; // 私有 websocket 重連並 recon 之後
  RECON_REASON_INCARNATION = 2; // account worker 換了 incarnation：被重新拉起之後的第一次 recon（E06-D004）
  RECON_REASON_REQUESTED = 3; // td.view 的 ReconRequest 的回覆，不在 td.evt 上（E06-D033）
}
```

### 7.3 SYM

照 mftik 的 symbol plane（DECISIONS E02-D001、E02-D002）。SYM 是商品資料的權威：交易所怎麼寫一個商品、它的交易限制是什麼。翻譯是查表，不是字串轉換。

- **不是 agent。** 每個 region 部署，NATS queue group，任何 replica 都能回答；沒有 lease、fencing。只有 SYM 寫。listing 預定分兩張表，看狀態還會不會變（E02-D003，enum 仍暫定）：活躍表是 `PRELISTING`、`TRADABLE`、`HALTED`、`SETTLING`、`UNKNOWN`，Resolver 與熱路徑只讀它，只有 `TRADABLE` 可交易；終結表是 `EXPIRED`、`DELISTED`，狀態不再變，給歷史查詢（舊成交、持倉、依 UID 或 ticker 反查），不再刷新，可以歸檔。兩張表的主鍵都是 UID，合在一起唯一。UID 是 SYM 分配的隨機整數，永不重用，不是單調序號，跟列一起持久化。列進了終結狀態就從活躍表搬到終結表，UniversalTicker 與 UID 都不變。`symbol_filter` 也分活躍表與終結表，跟列一起搬。同一個 ticker 下架再上市是新的 UID。E2-03 的 `migrations/0002_symbol` 已拆成 `symbol_active` 與 `symbol_terminal`，主鍵是 uid；`symbol_filter_active`、`symbol_filter_terminal` 跟列一起搬。活躍表的 `venue`、`category` 是從 `universal_ticker` 生成的欄位，`UNIQUE (venue, category, exch_ticker)` 與 `universal_ticker` 唯一只在活躍表。
- **刷新的單位是 `(venue, category)`。** 每個單位一個來源（adapter 的 `PublicREST.Listing`），各自刷新，一個 venue 掛了不影響其他。這次沒出現的列標成 `DELISTED`，只在同一個 `(venue, category)` 裡判斷：現貨的刷新不會讓 perp 下架。`EXPIRED` 或 `DELISTED` 都搬進終結表，之後不再刷新。ticker 字串與 UID 都不變。分頁的 listing 要讀完才算數，讀一半就當失敗。預設每小時一次，也可以 `RefreshRequest`。
- **使用者是 STS 的 session worker、TD 的 account worker、歷史補正服務，以及 MD 的 conn worker 與 md.fetch。MD agent、TD agent 與 API 不查（E02-D002）。**
- **啟動時載入活躍表，之後跟著變更重載（E02-D003）。** 進程啟動時，以 `ListRequest` 把用得到的每個 `(venue, category)` 的未終結列分頁讀進記憶體，每頁最多 500 筆，壓在 NATS 的 1 MiB 以下。分頁是 offset＋limit。翻頁之間列可能搬表或新上市，跨頁不一致可以接受（§10 P13）。Resolver 與熱路徑只查這份記憶體，不打 SYM，也不讀終結表。歷史查詢（舊成交、持倉、依 UID 反查）才讀終結表。MD 的 conn worker 只讀活躍表；ticker 離開活躍表就不訂。SYM 在 revision 推進並 commit 之後發變更通知；收到的進程重載那個 `(venue, category)` 的活躍列。subject 與訊息見下方「變更通知」。`GetRequest` 是 session worker 與 account worker 載入時把 ticker 解成 UID 的那一次。MD 的 feed 維持 `topic.UniversalTicker`，由 conn worker 在活躍表裡對 ticker 查。歷史補正服務把交易所寫法反查成 ticker（E06-D001）。`GetByUidRequest` 是之後 session worker、account worker 用 UID 拿欄位；`ReverseRequest` 把交易所寫法換回 listing（回覆帶 UID）。三者都留著，給載入、熱路徑找不到、或要查終結列時用。
- **誰載入什麼（E02-D002）：**
  - session worker：SDK 在啟動時載入 spec 裡用到的 venue，讓策略拿到 tick、qty step 這類 filter 做下單取整。權限見 §7 的 `sym`。
  - MD agent 不載表、不查 SYM。它把 `topic.UniversalTicker` 放上 conn worker。selector 的成員仍是後續。
  - conn worker：Init 載入自己那個 venue 的活躍表，推出 atom。事件上蓋 feed 的 ticker。subject 是 `md.v1.f.{inst}.{topic}.{ticker}`，用人讀的名字，不用 UID，ticker 原樣、不轉義（E04-D055）。只訂自己 venue 的 `sym.v1.changed.{venue}.>`（E04-D057）。
  - account worker：載入帳號種類可交易的 venue。私有事件帶回交易所的寫法，反查得到 UID。
  - TD agent 不載表、不查 SYM。它不訂 `td.evt`，也不落庫（E06-D038、E06-D039）。
  - 歷史補正服務：比照 md.fetch，載入連得到的 venue 的活躍表，訂閱 `sym.v1.changed`。REST 成交在這份表反查。活躍表沒有的那筆，以 `include_inactive` 直接問 SYM，不進快取。
  - md.fetch：載入它服務的 venue。

**幣與數量（E02-D003）。** `contract_value_ccy`、`price_ccy`、`settlement_asset` 由 adapter 從交易所的原生欄位填（例如 OKX 的 `ctValCcy`、Deribit 期權的 `quote_currency`），下游不推導。不沿用 mftik 的 `reservations.is_linear_margin`，也不為 Deribit 開特例。`SymbolInfo` 帶 `contract_value_ccy` 與 `price_ccy`；結算欄位仍叫 `settle`，不另開 `settlement_asset`。

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

資產代碼一律大寫。別名表（例如 `XBT` → `BTC`）在 `contract/symbol`，只由 SYM 套：adapter 傳原生代碼，不自行改名。各 venue 的別名在做 venue integration 時補進這張表。順序固定：SYM 在 `Validate` 之前先大寫並套別名，再做 quanto 判斷，再推 ticker。交易所拼法留在 `exch_ticker`（Bitmex 的 `XBTUSDT` 入庫為 `Bitmex_Spot_BTCUSDT`）。`USD`、`USDC`、`USDT` 不合併，不進別名表。報價 USD、結算 USDC 是 quanto，一期不入庫，需求 `REJECTED`，這是刻意的。

strike 先依 `spell_strike` 正規化，再推 ticker。整數去掉尾端的小數零，`6400.0` 與 `6400` 是同一個 ticker；有小數的把小數點寫成 `D`（§2）。

`qty_step`、`min_qty`、`max_qty` 與 qty 同一個單位（`contract_value_ccy`）。`price_tick` 用 `price_ccy`。

quanto（`settlement_asset` 既不是 base 也不是 quote）不入庫。解析不到就是 `REJECTED`（§3.4）。

**狀態（暫定）。** 契約的狀態是 `SymbolStatus`，不再是 `bool active`：`PRELISTING`、`TRADABLE`、`HALTED`、`SETTLING`、`EXPIRED`、`DELISTED`、`UNKNOWN`，再加上 proto 零值 `SYMBOL_STATUS_UNSPECIFIED`（非法、不可交易、不入庫）。完整列舉還沒定案。交易路徑只接受 `TRADABLE`，其他狀態拒單。之後新增 enum 值不會讓原本不可交易的變成可交易。交易所的原生狀態對不上，記成 `UNKNOWN`，不可交易；各 venue 的對照在做 venue integration 時補。活躍表的 CHECK 是 `PRELISTING`、`TRADABLE`、`HALTED`、`SETTLING`、`UNKNOWN`；終結表是 `EXPIRED`、`DELISTED`。

分表跟著這個暫定的切法：活躍表是 `PRELISTING`、`TRADABLE`、`HALTED`、`SETTLING`、`UNKNOWN`；終結表是 `EXPIRED`、`DELISTED`。Resolver 與熱路徑只讀活躍表。終結列狀態不再變，不再刷新，可以歸檔；搬過去時 ticker 字串與 UID 都不變。表已拆（E2-03）。

**UID（E02-D003）。** UniversalTicker 只出現在人寫的設定與 spec。STS 的 session worker、TD 的 account worker 載入時，`GetRequest` 把 ticker 解成 UID。之後這兩個 worker 傳 UID，要 base、filter、狀態時用 `GetByUidRequest` 查。TD agent 不查 SYM。歷史補正服務反查交易所寫法，不把帳號需求解成 UID。MD 的 feed 維持 `topic.UniversalTicker`，conn worker 在活躍表裡對 ticker 查，不把 feed 解成 UID；`md.f` 的 payload 帶那一列的 UID（E08-D065）。UID 由 SYM 分配，是隨機整數：唯一、永不重用，不是單調序號，也不是 Postgres sequence。它是兩張表的主鍵，跨表唯一，跟列一起持久化，所以重啟與重載都不改它。搬到終結表不換 UID。同一個 ticker 下架再上市拿新的 UID，舊資料仍指著舊的。碰撞時 SYM 重試。`symbol_filter` 也分活躍表與終結表，終結時跟列一起搬。`List` 以 offset＋limit 分頁，跨頁不一致可以接受。selector 沒有 UID；`Universe.current` 是 selector，尚未定，不在這一條。契約型別是 `uint32`，範圍 `1..4294967295`（`1..2^32-1`），0 非法（§10 P13）。Postgres 欄位是 `bigint`，`CHECK (uid BETWEEN 1 AND 4294967295)`。E2-03 已落表並抽號。E2-05 的進程內 Resolver 是例外，只給測試與開發，而且來源必須是 exchangetest：它自己發 UID，這種 UID 不落庫，也不跨進程或跨平面傳。它不保留終結表，所以 `include_inactive` 不會多查出列。真實 venue 仍等 SYM 分配的 UID。`sym` 由 SYM 以 queue group 回答 Get、GetByUid、List、Reverse、Refresh、Revisions（E2-08、E2-09）。成功是 `Reply` protobuf。`invalid`、`not_found`、`unavailable` 是 natsx 的 error reply（option A）：訊息是這三個字，接著 `: ` 與原因，原因寫出欄位、值與為什麼，不填 `Reply.error`。NATS subject 用人讀的名字，不用 UID，拼法見 §7 與 E04-D055：`md.v1.f.{inst}.{topic}.{ticker}`。策略 API 以 UID 為商品身分，ticker 只在 `Resolve` 出現（E08-D004，P15）。`td.order` 已是 UID（E06-D010）；需求項目、selector 成員這一版仍是 ticker 字串。

**查詢（E2-08，已定）。** Get、GetByUid、List、Reverse 的 `include_inactive` 預設 false，只讀活躍表；設了才含終結表。同一個 ticker 或交易所拼法同時在兩張表時回活躍列，下架再上市會同時存在。只有終結列、而且多於一列時，取 `updated_at_ns` 最大者，再取 uid 最大者。Reverse 在 `include_inactive` 為 true、且活躍表沒有這一列時，回終結列原本的 uid，不另配。`List` 的 `limit` 為 0 是 500，大於 500 是 `invalid`（`limit 600 exceeds max 500`），不截成 500。一頁的 protobuf 達到或超過 1 MiB 是 `unavailable`（`page of N rows is X bytes, exceeds 1 MiB limit; lower limit`），不改成較短的一頁。`RefreshResult` 的三個 map 以 `venue/category` 為 key。一個行程的 `-feeds` 決定它刷新哪些 feed；讀取查整庫，所以任一 replica 答案相同。文法正確、庫裡沒有列的 venue，List 是空清單（`total` 0），不是 `not_found`。Refresh 一個這個行程沒有的 canonical venue 是 `invalid`（`venue Okx is not refreshed by this process`）。venue 拼法不合文法是 `invalid`（`venue "bybit" is not canonical, use "Bybit"`）。`uid` 0 是 `not_found`（`uid 0 not found; 0 is not a valid uid`）。業務錯誤的原因都寫出欄位、值與為什麼，例如 `ticker Bybit_Spot_NOSUCH not found in active table (include_inactive=false)`、`uid 123 not found in active table (include_inactive=false)`。

**變更通知（P7，已定）。** revision 在同一個交易裡推進並 commit 之後，SYM 才發 `sym.v1.changed.{venue}.{category}`（E04-D057）。內容是 `Changed`：`venue`、`category`、`revision`。沒有推進就不發。Insert 與 Move 一定推進；Refresh 只有這次 feed 有寫入才推進。venue 與 category 是人讀的名字，不用 UID，不帶 `inst`。conn worker 只訂自己的 venue。經 NATS 的 Resolver 只以 `List`（`include_inactive` 為 false）分頁載入活躍表，索引與進程內 Resolver 相同：UID → `Info`、ticker → UID、`ExchKey` → UID。重載整份換上，讀取端不會看到混在一起的兩份快照。商品搬到終結表之後，新快照裡就沒有它。`include_inactive` 的 Get、GetByUid、Reverse 直接問 SYM，不進這份快取。漏收時，客戶端約每 5 分鐘以 `RevisionsRequest` 取回每張表的 revision，和自己載入的比；NATS 重連時也比一次。遠端比本地新，或這張表還沒載入過，就重載。`RevisionsRequest` 是空訊息，成功是 `RevisionList`。session 的 subscribe 加上 `sym.v1.changed.>`（E09-D006）。

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
    RevisionsRequest revisions = 6;
  }
}

// Spec 與設定載入的邊界（E02-D003）：人寫的 UniversalTicker 在這裡解成 UID。
// 平面之間不走這條，它們已經持有 UID，用 GetByUidRequest。
// One instrument by exact ticker: a single row, never a venue table.
message GetRequest {
  string universal_ticker = 1;
  bool include_inactive = 2; // E02-D003：預設只讀活躍表。設了才讀終結列（EXPIRED、DELISTED），給歷史查詢。Resolver 與熱路徑不設。表是 symbol_active 與 symbol_terminal
}

// 系統內部的查詢（E02-D003）。STS、TD、MD 持有 UID，用它拿這一列的欄位。
// 預設只讀活躍表；include_inactive 才含終結表。0 不是合法 UID，查詢是 not_found。
message GetByUidRequest {
  // 契約型別 uint32，範圍 1..4294967295（1..2^32-1），0 非法（PROTOCOL §10 P13）。
  // Postgres 欄位是 bigint，CHECK (uid BETWEEN 1 AND 4294967295)。
  uint32 uid = 1;
  bool include_inactive = 2; // 同 GetRequest.include_inactive（E02-D003）
}

// One (venue, category) table, paged by offset and limit. limit 0 means 500; a limit above
// 500 is invalid, not clamped. A page whose protobuf reaches 1 MiB is unavailable, not a
// shorter page, so offset and limit stay what the caller sent. A row may move or appear
// between pages; that inconsistency is accepted (E02-D003, PROTOCOL §10 P13).
message ListRequest {
  string venue = 1;
  string category = 2;
  uint32 limit = 3; // 0 means 500; above 500 is invalid
  uint32 offset = 4;
  bool include_inactive = 5; // 同 GetRequest.include_inactive（E02-D003）
}

// The venue's spelling -> the listing. Callers keep SymbolInfo.uid (E02-D003). Category is required:
// on a unified-account venue the spelling alone is ambiguous (Bybit's spot and linear "BTCUSDT").
// include_inactive defaults to the active table. Set it to fall through to the terminal table
// when the active table has no match; the uid is the one stored on that row. When both tables
// match, the active row wins (a delist followed by a relist).
message ReverseRequest {
  string venue = 1;
  string category = 2;
  string exch_ticker = 3;
  bool include_inactive = 4; // 同 GetRequest.include_inactive（E02-D003）
}

// Re-pull one venue's listings now, or every venue when venue is empty.
message RefreshRequest {
  string venue = 1;
}

// A success is this message: result is set, and error and reason stay empty.
// invalid, not_found and unavailable are not this message. They are a natsx
// error reply (option A). The text is that word, then ": " and a reason that
// names the field, the value, and why.
message Reply {
  string error = 1; // unused; business errors use the natsx error reply
  string reason = 2; // unused
  oneof result {
    SymbolInfo info = 10;     // get, get_by_uid, reverse
    SymbolList list = 11;     // list
    RefreshResult refresh = 12;
    RevisionList revisions = 13;
  }
}

// Every (venue, category) revision. A client that may have missed
// sym.v1.changed compares these with the revision it loaded (P7, E04-D057, PROTOCOL §7.3).
message RevisionsRequest {}

// One table's revision, as stored after the commit that advanced it.
message TableRevision {
  string venue = 1;
  string category = 2;
  int64 revision = 3;
}

message RevisionList {
  repeated TableRevision tables = 1;
}

// Published on sym.v1.changed.{venue}.{category} after that table's revision
// commits (P7, E04-D057). Venue and category are the human-readable names, not a UID.
// SYM has no inst. A conn worker subscribes only its own venue.
message Changed {
  string venue = 1;
  string category = 2;
  int64 revision = 3;
}

// 交易狀態（E02-D003）。列舉仍暫定。UNSPECIFIED 是 proto 的零值，非法且不可交易。
// 只有 TRADABLE 可交易。交易所原生狀態對不上時記 UNKNOWN，不可交易。
// 之後新增的值不會讓原本不可交易的變成可交易。
// 活躍表：PRELISTING、TRADABLE、HALTED、SETTLING、UNKNOWN。
// 終結表：EXPIRED、DELISTED。搬過去時 ticker 與 uid 都不變。
enum SymbolStatus {
  SYMBOL_STATUS_UNSPECIFIED = 0;
  SYMBOL_STATUS_PRELISTING = 1;
  SYMBOL_STATUS_TRADABLE = 2;
  SYMBOL_STATUS_HALTED = 3;
  SYMBOL_STATUS_SETTLING = 4;
  SYMBOL_STATUS_EXPIRED = 5;
  SYMBOL_STATUS_DELISTED = 6;
  SYMBOL_STATUS_UNKNOWN = 7;
}

message SymbolInfo {
  string universal_ticker = 1; // 給人看的字串。系統內部用 uid（E02-D003）
  string exch_ticker = 2;
  string base = 3;
  string quote = 4;
  optional double contract_size = 5; // 一張的面值。qty = 張數 × 此值，單位是 contract_value_ccy：linear 與期權是 base，inverse 是 quote（E02-D003）
  optional string settle = 6; // 損益結算幣（settlement_asset），不是保證金幣。空 = 現貨。quanto（既不是 base 也不是 quote）一期不入庫（E02-D003）
  int64 expiry_ns = 7; // 0 = does not expire
  optional double strike = 8; // ticker 裡的拼法由此值的最短十進位表示推出（spell_strike，E02-D003、E00-D008）：6400.0 與 6400 是同一個值
  string option_type = 9; // C | P | ""
  SymbolStatus status = 10; // 暫定（E02-D003）。UNSPECIFIED 非法、不可交易。只接受 TRADABLE。對不上的是 UNKNOWN，不可交易。之後加 enum 值不會讓原本不可交易的變成可交易。活躍表是 PRELISTING、TRADABLE、HALTED、SETTLING、UNKNOWN；終結表是 EXPIRED、DELISTED，搬過去時 ticker 與 uid 都不變。列舉仍暫定，契約已用這個 enum。表已拆成 symbol_active 與 symbol_terminal
  // name -> bound. A key with an absent value: the venue publishes the restriction, no bound.
  // price_tick is price_ccy; qty_step, min_qty, max_qty are contract_value_ccy (E02-D003).
  map<string, Bound> filters = 11;
  int64 updated_at_ns = 12;
  // SYM 分配的隨機整數：跨活躍表與終結表唯一、永不重用，跟列持久化，不是單調序號（E02-D003）。
  // 重啟與重載不改它。下架再上市是新的 UID。碰撞時 SYM 重試。
  // 契約型別 uint32，範圍 1..4294967295（1..2^32-1），0 非法（PROTOCOL §10 P13）。
  // Postgres 欄位是 bigint，CHECK (uid BETWEEN 1 AND 4294967295)。
  // SQL 的兩張表以 uid 為主鍵（migrations/0002_symbol）。
  uint32 uid = 13;
  // qty 的單位（E02-D003）。adapter 從交易所原生欄位填，下游不推導。linear 與期權是 base，inverse 是 quote。
  // 現貨可以留空；入庫時存成 base。
  string contract_value_ccy = 14;
  // price、price_tick 與權利金的單位（E02-D003）。adapter 填。現貨與期貨（含 inverse）是 quote；
  // 幣本位期權是 base；U 本位期權是 quote。現貨可以留空；入庫時存成 quote。
  string price_ccy = 15;
}

message Bound {
  optional double value = 1;
}

message SymbolList {
  repeated SymbolInfo symbols = 1;
  uint32 total = 2;
}

message RefreshResult {
  map<string, uint32> refreshed = 1;   // venue/category -> instruments seen
  map<string, uint32> deactivated = 2; // venue/category -> rows marked inactive
  map<string, string> failed = 3;      // venue/category -> reason
}
```

## 8. artifact（E09-D002、E09-D004）

三種 worker 都從 artifact 起（DECISIONS E09-D002）：策略 artifact（STS 的 session worker），以及這個 repo 的 conn worker、account worker 由 CI 打包的 artifact。它們是同一個格式的 OCI image，只當打包格式用，不跑容器，manifest 的 `kind` 區分種類。資料 artifact（§8.4）放在同一個 registry，用另一種 mediaType。

### 8.1 結構

| OCI 物件 | 內容 |
|---|---|
| image manifest | 各層的 digest；`config.mediaType` 是 `application/vnd.mftiko.worker.config.v1+json`，三種 worker 共用 |
| config blob | 下面的 mftiko manifest（JSON） |
| 層 | Go 策略：一層，`bin/strategy`。其他語言由該語言的 SDK 決定，例如 Python：runtime 層（解譯器與 SDK）、依賴層（`site-packages/`）、策略層（`app/`）。MD、TD：一層，`bin/worker` |

策略的 image digest 就是 `StrategyRef.code`。同一個 session 重拉仍用這一個，不改拿 registry 上的最新；拉取一律用 digest，不用 tag（DECISIONS E09-D011）。tag 仍可供人查找。API 的策略目錄把 tag 解析成 digest（E09-D034、E10-D056）。建立 session 時沿用同一個解析，再寫入 `StrategyRef.code`，那一步是 E10-11。STS 的拉取仍只用 digest，不用 tag。

registry 一期只有一個，所有 region 共用；位址、帳密檔、資料 artifact 的 repository 寫在 STS instance 的部署設定（E09-D010）。帳密檔是 Docker config.json，以 oras-go 內建的 credentials 讀取，basic 與 token 交換都支援；帳密不進 etcd、不進 spec（E09-D033、E09-D010）。部署設定加 `plain_http`，dev 的 registry:2 走 http（E09-D033）。推拉用 oras-go v2（E09-D009）。

conn 與 account 的發布見 [release.md](release.md)（E09-D085–E09-D090）。推 `v*` tag，或在該 tag 上 `workflow_dispatch`，把 `linux/amd64` 的靜態 `bin/worker` 推到 `ghcr.io/bullionbear/` 的 `mftiko-md-conn` 與 `mftiko-td-account`，tag 是該 git tag 與 `latest`。workflow 只用 `GITHUB_TOKEN` 的 `packages: write` 推送，不設定可見度。第一個 `v*` tag 之前，由 org owner 把 BullionBear org 的 Package creation 預設設成 Public（一次）。一般 PR 不推這個 registry；CI 對 `registry:2` 跑同一支打包腳本，推了再依 digest 拉回。

MD、TD 不按 instance 釘版本（P9）。每次 spawn 取當時的最新。E9 只把 conn、account 打包進 registry（E09-D014、E09-D085–E09-D090）；agent 仍執行這個 repo 建出的 binary，版本是 build 戳進的 `Version`、`CommitHash`、`BuildTime`。改拉 registry 上的最新留到 E12。已經在跑的進程保持原來的 binary。部署新的 agent，或 agent 輪換，都不因此換 incarnation：行情連線不斷、tape 還在，account worker 不斷，session worker 也不斷（E01-D003、E01-D006）。

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

`md.conn` 的例子（release workflow 傳的 `-name` 與 repository 同名；`td.account` 同形，name 是 `mftiko-td-account`，contracts 見下表）：

```json
{
  "schema": "mftiko.worker.v1",
  "kind": "md.conn",
  "name": "mftiko-md-conn",
  "runtime": "go",
  "os": "linux",
  "arch": "amd64",
  "entrypoint": ["bin/worker"],
  "env": {},
  "sdk": { "lang": "", "version": "" },
  "contracts": ["worker.v1", "mdctl.v1"]
}
```

| 欄位 | 意思 |
|---|---|
| `schema` | manifest 自己的版本 |
| `kind` | `strategy`、`md.conn` 或 `td.account`。agent 只接受自己那一種 |
| `runtime` | `go`，或其他語言 SDK 自訂的名稱（例如 `python`）。只用於顯示與統計，平台不依它改變行為 |
| `os`、`arch` | 目標平台；與主機不符就 `INIT_FAILED` |
| `entrypoint` | 相對於 rootfs 的路徑，不得以 `/` 開頭、不得含 `..`；agent 換算成 `Launch` 的 `path`、`args`（§4.6）。Go 策略是 `["bin/strategy"]`。`md.conn` 與 `td.account` 是 `["bin/worker"]`（E09-D088）。Python SDK 例如 `["python/bin/python3", "-m", "<sdk>.run", "<module>:<Class>"]` |
| `env` | 放在平台注入的環境變數之前（§4.6）；不得設 `MFTIKO_` 開頭的變數 |
| `sdk` | SDK 的語言與版本。策略由 `mftiko-pack` 從 buildinfo 填入。`md.conn` 與 `td.account` 留空，`lang` 與 `version` 都是空字串（E09-D089） |
| `contracts` | 支援的 C4、C5、C6 契約 major；STS 在 spawn 前比對。Go 策略由 sdk 把這份清單以可辨識字串嵌入 binary（與 `Hello.contracts` 同源），`mftiko-pack` 從 binary 讀出填入，不比對 sdk 版本；讀不到就拒絕打包。`-contracts` 只留給未來的非 Go runtime（DECISIONS E09-D035，取代 E09-D012）。非策略 kind 不讀嵌入字串：`md.conn` 固定 `worker.v1`、`mdctl.v1`，`td.account` 固定 `worker.v1`、`tdctl.v1`，pack 依 kind 填入（E09-D089） |

**相容規則（E09-D093）。** manifest 是 JSON，不經 `buf breaking`，規則寫在這裡。同一個 `schema`（`mftiko.worker.v1`；資料 artifact 的 config 是 `mftiko.data.v1`，§8.4）之內只能新增欄位；新欄位的零值（缺席）必須保持舊的意思。讀的一方忽略不認得的欄位。改名、刪除、改型別或改意思就換 `schema` 字串，讀的一方不認得的 `schema` 一律以 `artifact_bad_manifest` 拒絕。

`mftiko-pack` 的 `-kind` 是 `strategy`、`md.conn` 或 `td.account`，預設 `strategy`；省略時與顯式 `strategy` 相同（E09-D088）。一期只打包 `linux/amd64`（E09-D090，E09-D034 不收 OCI index）。

### 8.3 主機上的處理

三種 agent 都照這個流程，程式共用（`internal/platform/artifact`，DECISIONS E00-D006）。E9 把 conn、account 打包進 registry，agent 仍不走第 1–3 步，執行這個 repo 建出的 binary；改拉 registry 留到 E12（E09-D014，§8.1）。

1. 取得 image manifest 與 config。digest 指向 OCI index（多平台清單）時拒絕，不從 index 裡挑一個平台（DECISIONS E09-D034）。檢查 config 的 mediaType（`application/vnd.mftiko.worker.config.v1+json`）、`schema`、`kind`、`os`／`arch`、`contracts`、總大小。呼叫端傳入這次需要的契約，manifest 的 `contracts` 必須全部包含，多出來的不算錯，缺任何一個就是 `artifact_contracts_mismatch`（E09-D029）。下載前把 manifest 宣告的 config 與各層大小加總，超過上限（部署設定，預設 1 GiB，E09-D013）就是 `artifact_too_large`，不下載；下載中實際讀到的位元組不得超過該描述子宣告的大小（E09-D032）。不符就不 spawn；策略以 `INIT_FAILED` 結束 session。錯誤帶 reason，並標明永久或暫時：registry 連不上或回應 5xx 是暫時，401 與 403（`artifact_unauthorized`）以及 artifact 本身不符是永久。套件不重試；要不要重試、何時標 `INIT_FAILED`，由呼叫端決定（E09-D028）。STS 的呼叫端把 rootfs 與 inputs 的拉取與解開放在 reconcile 之外的背景 goroutine，共用最多同時 4 個（E09-D058、E09-D074）；計畫只放 rootfs 與所有 inputs 都就緒、volume 已建立的 session，期間 phase 維持 `PENDING`。永久錯誤以 `INIT_FAILED` 結束、不 spawn，reason 用 artifact 的 reason；暫時錯誤用骨架 Backoff（1–60 秒）重試到指派消失（E09-D058、E09-D074）。正式路徑一律從 artifact 起，沒有 `-worker`（E09-D060，取代 E09-D043）。
2. 缺少的層先下載到 `${DATA}/layers/` 下的暫存檔，邊下載邊算 sha256，驗過才 rename 成 `<digest>`；驗不過就刪暫存檔，不留半檔。啟動時清掉殘留的暫存檔。同一個進程裡，同一層的並行請求只下載一次（E09-D030）。`<digest>` 這個檔名只在驗證通過之後才出現，重用時只比對大小，不重算 sha256（E09-D031）。
3. 依序直接解壓寫入 `${DATA}/artifacts/<image digest>/rootfs`。層快取存的是 tar.gz，內部檔案無法 hardlink；`artifacts/<image digest>/` 內記一份所用層的清單，供 GC 判斷層是否被引用（E09-D050）。只接受一般檔、目錄、指向 rootfs 內的相對 symlink、指向 rootfs 內前面已出現檔案的 hardlink。拒絕絕對路徑、`..`、指出 rootfs 的 symlink（含絕對 target）、裝置檔、FIFO、setuid／setgid、OCI whiteout（`.wh.`）。uid／gid 一律忽略，檔案屬 agent 使用者（E09-D051）。寫入用 `os.Root`，擋住經 symlink 逃出 rootfs（先 `a → /etc` 再 `a/passwd` 這種），不自己逐段檢查路徑（E09-D052）。解壓後總大小上限預設 4 GiB、檔案數上限預設 100,000，皆為部署設定；超過與其他拒絕一樣處理，永久 reason 是 `artifact_unsafe_layer`，策略以 `INIT_FAILED` 結束。層的 sha256 正確，不刪快取，交給 GC（E09-D053、E09-D054）。同一 image digest 在同一進程只解一次；解到暫存目錄再 rename，rootfs 目錄存在即完整、直接重用不重驗；啟動時清殘留暫存目錄。任一拒絕都整個作廢，不留半個 rootfs（E09-D055）。解開函式收一組層與目標目錄，資料 artifact 解到 `sessions/<id>/inputs/<name>/` 時呼叫它，拒絕規則相同（E09-D057）。
4. 依 §4.6 的對應表換算成 `Launch` 交給 shim；shim 以 rootfs 為工作目錄執行 `entrypoint`。
5. GC：策略的 rootfs 沒有被非 terminal session 釘住、也不是目錄裡的最新版，就刪；MD、TD 的 rootfs 沒有進程在用、也不是最新版，就刪；層沒有任何 rootfs 引用，就刪。GC 在每一輪 reconcile 裡做，不另外排時間（DECISIONS E09-D015）。`GC(keep)` 收要保留的 image digest，刪其餘 rootfs，再刪沒有 rootfs 引用的層；函式不判斷目錄裡的最新版。目錄裡的最新版是 `latest` tag 指向的 digest（E10-D013、E09-D087）；E9 的 STS 仍只傳釘住的 digest，把最新版交給 GC 留到 E12。正在拉取或解開的 digest 由呼叫端放進 `keep`。暫存檔與暫存目錄不碰（E09-D056）。STS 每一輪 reconcile 都呼叫 GC。keep 是非 terminal session 用到的 image digest，加上正在拉取或解開的 image digest（E09-D059）。另一組是正在拉或正在解的 input 層 digest；解開之後不再傳入，之後的 incarnation 沿用解開的目錄。策略 artifact 的層在 `Pull` 回傳後以 HoldLayers 保留，直到 `UnpackImage` 結束（成功或失敗）；下載期間的空窗不另處理（E09-D073）。

### 8.4 資料 artifact（E09-D004）

| OCI 物件 | 內容 |
|---|---|
| manifest | `artifactType` 是 `application/vnd.mftiko.data.v1`；一層 |
| config blob | `application/vnd.mftiko.data.config.v1+json`：`{"schema": "mftiko.data.v1", "source": {"session_id": …, "incarnation": …, "clean": …}}`。運維上傳的沒有 `source` |
| 層 | `application/vnd.mftiko.data.layer.v1.tar+gzip`：檔案樹，路徑相對於 input 的根 |

STS 主機上：

1. **spawn 前：** 每個 `InputDecl` 的層下載到 `${DATA}/layers/<digest>`（與策略 artifact 共用快取），驗 sha256，以 §8.3 的解開函式解到 `${DATA}/sessions/<session_id>/inputs/<name>/`（E09-D057、E09-D072）。下載、驗證、層快取與 single-flight 跟策略 artifact 相同；解開上限是 4 GiB、100,000 個檔案（E09-D053）。每個 session 的每個 input 只解一次，先解到暫存目錄再 rename；目錄已在就沿用，agent 重啟時目錄不在就重解（E09-D074）。拒絕的項目與 §8.3 相同。拉不到或驗不過就以 `INIT_FAILED` 結束、不 spawn，reason 用 artifact 的 reason。檔案 0444、目錄 0555，在暫存目錄裡改好再 rename，正式位置一出現就是唯讀（E09-D075、E09-D096）。
2. **volume：** `${DATA}/sessions/<session_id>/volume`，權限 0700，第一個 incarnation spawn 前建立，worker crash 或 agent 重啟後沿用磁碟上的目錄（E09-D077）。STS agent 在背景每 10 秒量一次實際佔用（`st_blocks`，同 `du`，間隔可設定）。超過 `Limits.volume_mb`（0 取部署預設 1024）就用一般 Stop 停下，phase `FAILED`、reason `volume_over_limit`，不重啟（E09-D076）。
3. **session terminal 之後：** worker 進程確定結束才打包，避免打包時 volume 還在被寫（E09-D078）。從未 spawn 的 session（例如 `INIT_FAILED`）沒有 volume 或 volume 是空的，直接清理。沒有任何一般檔或 symlink 就算空（只有空目錄也算）；socket、device、fifo 不算內容。空的不推、不寫 `state_out`，直接清理（E09-D081）。解開時會拒絕的一般檔（例如 `.wh.*`）仍算非空，打包時跳過並寫 log。進入 terminal 且進程已結束時，在 volume 旁寫 `sessions/<session_id>/terminal.json`（`session_id`、`incarnation`、`clean`）。`clean` 是最後一個 incarnation 的 `on_stop_completed`；被 SIGKILL 或沒有 `Stopped` 一律 false。agent 重啟時掃 `sessions/`，有這個檔且 volume 還在就接著推（E09-D082、E09-D026）。非空就打包成一層：依路徑排序、mtime 歸零、uid／gid 為 0；只收解開時接受的項目（一般檔、目錄、指向 volume 內的相對 symlink，E09-D051）；權限只保留執行位元（檔案 0644／0755，目錄 0755）；其他項目（指向外面的 symlink、socket、device）跳過並寫 log，不讓推送失敗；hardlink 當各自獨立的檔案。打包可重現，重推得到同一個 digest（E09-D080）。層先打包成 `${DATA}` 下的暫存檔再串流上傳；manifest、config、層的 mediaType 照本節；憑證與 `plain_http` 用拉 artifact 的同一份設定（E09-D033、E09-D079）。推到設定的 data repository，tag 是 `<session_id>`（E09-D025）。成功後寫 `SessionStatus.state_out`：`size_bytes` 是層的壓縮後大小，`at_ns` 是推送成功的時間，`digest`、`repository`、`clean`、`incarnation` 照本節（E09-D083）。推不成功就保留 volume，以 1 秒起、上限 60 秒退避，一直重試到成功，不擋 terminal 的 `SessionStatus`（phase 先寫，`state_out` 成功後再補，E09-D026）。清理分兩段（E10-D016，修改 E09-D084、E09-D023）：推送成功（或空 volume）且 worker 已結束後，只刪 volume 與 inputs（先改回可寫，E09-D075）。第二段在第一段完成，而且 reconcile 手上的 spec 檢視裡沒有這個 session 時才做，不另外對每個 session 打 etcd Get；agent 啟動後要等第一次完整 list 同步完成才開始，避免快取還是空的時候誤刪（E10-D019）。刪除順序是 shim 的 log 檔（`${WORK_DIR}/log/sts/{h}.*`，含 `{h}.shim.log`）、以 `Incarnations.Forget` 刪 incarnation key、`terminal.json`，最後刪掉整個 `sessions/<session_id>/`（E10-D020）。`terminal.json` 留到最後，中途 crash 下一輪可以接著清，也用來和 E09-D044 那種從未 spawn 的目錄分開。第二段是 level-triggered：每輪 reconcile 判斷；agent 重啟後 list 一次，第一段已完成、spec 已不在的照樣清；只清本機有 `terminal.json`、確認過 terminal 的 session。spec 在 terminal 之前被刪（§3.3 N3 `ended_by_spec`）也一樣：session 照常結束、推完 `state_out`、第一段完成後才清。
4. **GC：** 層快取的規則同 §8.3。正在拉或正在解的 input 層另外保留，解開後不再保留（E09-D073）。registry 上的資料 artifact 一期不刪，STS 與 API 都不刪：新 session 可以在 W5 的保留期之後仍以 digest 引用舊的 `state_out`（E10-D017）。容量由運維管理。

## 9. session worker 的實作契約（給其他語言的 SDK）

平台只出 Go SDK，它是參考實作（DECISIONS E08-D002）。其他語言（例如另外實作的 Python SDK）只要做到本節的每一條，就能在平台上跑，平台不需要為它改任何東西。

- **必須**：契約。違反時平台會判定失敗、拒絕請求，或行為與其他策略不一致。
- **建議**：讓行為和 Go SDK 一致，或避開已知的坑。

### 9.1 打包與環境

1. **必須**以 §8 的 OCI 格式打包。manifest 填 `runtime`、`entrypoint`、`sdk`、`contracts`；`contracts` 列出實作的 major（`worker.v1`、`stsctl.v1`、`md.v1`、`td.v1`），STS 在 spawn 前比對。
2. **必須**把解譯器、SDK、第三方套件全部放進 artifact。不得假設主機上有任何語言 runtime 或套件。
3. **必須**從 §5.4 的環境變數取得身分與位址，不得覆蓋 `MFTIKO_*`。憑證不在環境變數裡，在 `Init` 的 payload 裡。
4. **必須**只寫 `MFTIKO_VOLUME_DIR`（與系統暫存目錄）；`MFTIKO_INPUT_DIR` 唯讀（DECISIONS E09-D004）。**必須**用原子寫入：同一目錄的暫存檔、寫完 fsync、rename 蓋上目標、再 fsync 目錄（E08-D052）。中途被殺時目標檔要不在、要不仍是上一份完整內容。讀取順序是函式，策略自己呼叫：volume 有該檔就用 volume，否則用 inputs，兩者都沒有就從零開始（E09-D004、E08-D051）。SDK 不在啟動時自動還原。
5. **必須**讓自己開的子進程跟著 worker 一起結束。shim 對 worker 的 process group 送信號（§4.3）。
6. STS 在 spawn 時不讀 conformance 報告（E08-D032）。「宣稱支援必須先通過」是發布流程，不是 agent 的檢查。

### 9.2 C4：fd 3

1. **frame**：4 bytes big-endian 長度加一則 protobuf，上限 1 MiB。上行 `WorkerUp`，下行 `WorkerDown`（§5.1）。拿到 fd 3 先設 close-on-exec：worker 起的子進程不能拿著它，否則 worker 死了 shim 也讀不到 EOF，子進程還能冒充 worker 送訊息。
2. **流程照 §5.2：**
   - 啟動後先送 `Hello`。`contracts` 列出這支 worker 實作的 major。agent 從中挑 C5 的 `stsctl.v1`。conformance 讀同一份清單決定 skip（§9.8）：session worker 在這裡列出 `worker.v1`、`stsctl.v1`、`md.v1`、`td.v1`。
   - 收到 `Init`：解析 payload → 呼叫策略的 `OnInit` → 回 `Done(INIT, ok)`。`Init` 還沒成功時收到其他訊息，或第二則 `Init`，照 §5.2 以 `INIT_FAILED` 結束，不呼叫 hook（E08-D037）。
   - 收到 `Start`：套用 `Bindings`、訂閱 subject → `OnStart` → 回 `Done(START, ok)` → `Stage(running)`。`Init` 已成功而 `Start` 尚未成功時收到 `Stop`：`Exiting(DONE)`、`Stopped(false)`、exit 0，不呼叫 `OnStart` 與 `OnStop`（E08-D037）。
   - `deadline_ms` 內沒完成，算 `INIT_FAILED`（E08-D007）：不等 hook 返回，`Done(verb, false)`、`Exiting(INIT_FAILED)`、`Stopped(false)`、exit 10。0 表示不限時。Start 逾時不跑 `OnStop`；Start 失敗（非逾時）要跑（E08-D008）。
3. **`Beat` 每 1 秒一則**，必須由不會被 hook 阻塞的執行緒送。做不到時，由 §9.7 的保險接手。
4. **`Report`**：`stsctl.Report` 有變化時送，至少每 5 秒一次；最新一份取代舊的。`QueueStat` 的 `td`、`md` 兩個 buffer 都要回報（§9.5）。`bindings_applied` 是 MD path 已經套用的 binding `Version`（epoch 與 seq）；還沒套用過就不填。這份版本變了就送，不等 5 秒。
5. **結束的順序必須是**：停止交付 MD、等目前的 hook 跑完 → 送 `Exiting(outcome, reason)` → `DONE`、`FAILED` 退訂 `td.evt` 並交付完 TD buffer 裡已收到的事件，`CRASHED` 不交付 → 該跑的才跑 `OnStop`（§5.3）→ 送 `Stopped(on_stop_completed)` → 以 §5.3 的 exit code 結束（DECISIONS E08-D003）。`Exiting` 不等交付，一停下就送。
6. **收到 `Stop`**：在 `grace_ms` 內走完上面的流程。`grace_ms` 從 shim 收到 `Stop` 起算，涵蓋整個收尾，不只 `OnStop`（§4.3）。逾時 shim 會 SIGTERM，再逾時 SIGKILL。SIGTERM 當成 `Stop` 處理。Go 的參考實作由 worker main 以 `signal.NotifyContext` 接 SIGTERM，把取消的 ctx 傳入 C4；C4 自己不攔訊號（E08-D006）。因 `Stop` 或 SIGTERM 而結束時，`OnStop` 的 ctx 期限是 `grace_ms` 的剩餘；只有呼叫端 ctx 取消、沒有帶 `grace_ms` 的 `Stop` 時不另造期限（E08-D009）。worker 自行結束（`Run` 返回）時，期限改為 plane 在 `Init` 成功後交給 C4 的 `hook_timeout_s`。Go 的 C4 不解析 C5 payload，plane 以 `HookTimeout` 交出一個 `time.Duration`；沒交（回 0，或不是這個介面，MD 與 TD 即是）就不限時。這段逾時不等 `OnStop`，exit 21，不送 `Stopped`，`Exiting` 的 outcome 不變（E08-D003）。Start 失敗不套這個期限。
7. **fd 3 讀到 EOF（shim 消失）**：必須自行 graceful stop（§4.5 S2）。
8. **`Controller`**：記下目前有沒有 agent。沒有時，對 agent 的 `Relay` 會收到 `Undeliverable`。
9. **live config 與 `Applied`**（§5.2，E01-D011）：`Init` 成功時，若 `live_digest` 不是空的，在 `Done(INIT, ok)` 之前送 `Applied{live_digest}`。STS 的 `Live.payload` 是 `stsctl.Bindings`（完整快照，不是 `Command`）。套用規則與 `Start`、`Command.bindings` 相同（E08-D046）。成功，包含版本沒有比較新、集合不變，就送 `Applied{digest}`，原樣帶回 agent 給的 digest。selector 或快照不合法就不送，agent 會再送。

### 9.3 C5：`stsctl.v1`

1. `InitPayload`：`session_id`、`incarnation`、策略參數（`params`，平台不解讀）、`feeds`、`accounts`（含 `algo_ids`）、`nats`（§9.4），以及 prehook 用的 `recv_window_ms` 與 watchdog 用的 `hook_timeout_s`（§9.5）。這兩個是 STS agent 把 spec 的「0 = 預設」解析之後的實值，不會是 0；SDK 不必知道預設值，收到 0 以 `INIT_FAILED` 結束（DECISIONS E08-D003）。
2. `StartPayload.bindings` 與 `Command.bindings`：完整快照，帶 `Version`，**只套用比手上嚴格更大的**（先比 epoch，再比 seq；手上還沒有時第一份就套用，包含 version 0）（§6、E08-D046）。集合是列出的 feed subject：多出來的訂閱，少掉的退訂。快照裡有 selector 就回 error，不解析成員（E08-D022、E08-D046）。
3. `Command.fail`：以 `FAILED` 結束（跑 `OnStop`，exit 30）。
4. protobuf 的讀取端**必須**容忍未知欄位與未知 enum 值（§1）。
5. `Live.payload` 是 `stsctl.Bindings`，與 `StartPayload.bindings`、`Command.bindings` 同一份快照格式（E01-D011）。`Relay` 的 payload 才是 `Command`：`bindings` 套用，失敗只記 log、不結束 session；`fail` 以 `FAILED` 結束。

### 9.4 C6：NATS

1. **連線**：用 `InitPayload.nats` 的 `user_jwt` 與 `seed`；reply inbox 前綴設成 `inbox_prefix`。位址是 `MFTIKO_NATS_URL`。不得使用其他憑證（DECISIONS E09-D006）。`Init` 期間連不上時，在 `deadline_ms` 內以退避重試，到期以 `INIT_FAILED` 結束（E08-D050）。`deadline_ms` 為 0 則重試到連上或收到 `Stop`（E08-D007）。
2. **feed 的 subject 用 binding 給的，不自己組**（DECISIONS E01-D006）：
   - feed：`FeedBinding.subject`。
   - selector：一期不實作。快照裡有 selector 就回 error，不把 `subject_prefix` 組成員 subject（E08-D022、E08-D046）。
   - `md.view`：把 feed subject 開頭的 `md.v1.f.` 換成 `md.v1.view.`（E04-D055）。
   - `md.fetch`：`md.v1.fetch.{venue}`（E04-D056）。請求以 UID 指定商品（E08-D065）；venue 是這個商品在活躍表上的 venue，人讀的名字，與 `sym.v1.changed.{venue}.{category}` 的 venue 相同。這不是 feed，不從 binding 來，SDK 也不選 region。行情 subject 不帶 venue。
   - `md.view` 與 `md.fetch` 一次請求等 `InitPayload.recv_window_ms`。逾時或沒有 responder，callback 收到 error。回覆走 TD buffer（E08-D048）。
3. **`md.f` 的訊息**：先讀 envelope（§7.1）。v1 的 payload 從 offset 40 開始。長度小於 40、`version` 不是 1、或保留 byte 不是 0，丟掉這則，計入 `QueueStat.dropped`，寫 log，session 不結束（E08-D049）。不依長度 byte 跳過較長的 envelope。商品以 payload 的 `FeedEvent.uid` 辨識，不從 subject 推（E08-D065）；tape、book 快照、`md.fetch` 的回覆也各自帶 `uid`。envelope 壞掉的仍先計入 `dropped`。envelope 可讀、但這個 topic 沒有 hook 的，在解碼 payload 之前跳過（E08-D005）。
4. **tape 接 live（E04-D037、E08-D047）**：SDK 不自動向 `md.view` 要 tape，也不扣住 live。live 立即交付，仍走 prehook。策略要歷史時自己呼叫 `Tape`，把 `(conn_epoch, seq)` 小於等於 `TapePage.head` 的當成重複、大於 `head` 的當成新的。ring 裡沒有事件時，`head` 是目前的 `conn_epoch`、seq 0。book 由 SDK 維護：快照只向 MD 的 `md.view` 要 `BookSnapshot`（MD 自己的本地本），不向交易所要（E08-D057）。未同步（含 `not_synced`）退避重試、不呼叫 `OnBook`，`Book` 回 `(nil, false)`（E08-D060）。`reset` 清掉本地本並重要快照（E08-D061）。
5. **下單**：`Submit`、`Cancel` 必須帶 `session_id`。`client_order_id` 的 algo 欄位只能用 `AccountDecl.algo_ids` 裡宣告給該帳號的（DECISIONS E06-D002）：TD 不解碼、不檢查，這一條只由 SDK 遵守（E06-D033）。`Cancel` 與 `BatchCancel` 只收 `client_order_id`。`BatchCancel` 不檢查歸屬：TD 以 client id 對到活單後一對一轉發。一次撤掉某 algo 全部掛單由 SDK 照 `ViewReply.cancel_batch` 自己拆批，沒有批次撤單就改走單筆 `Cancel`（E06-D025、E06-D031）。商品以 UID 指定（E02-D003）。
6. **`client_order_id`**：照 §2 的版面 `ver(4) | algo(10) | ts_ms(40) | seq(10)`，`ver` 填 1，以十進位字串傳。seq 計數器一個進程一個，所有 algo、所有帳號共用（E08-D011）。同一個進程內 `ts_ms` 單調；時鐘倒退沿用上次的 `ts_ms`。同一毫秒的 `seq`（0..1023）用完時，`ts_ms` 往前借一毫秒，不設上限，所以 `ts_ms` 可能超前牆鐘；實務上不會發生，要每秒超過一百萬筆（E08-D012）。時間由呼叫端傳入。產生器狀態不持久化。跨 incarnation 撞號是接受的理論風險：撞到時 TD 依冪等處理，內容不同回 106，內容相同回原結果（E08-D013、E06-D019）。Go SDK 的產生器是 `sdk/internal/cloidgen`：`ts_ms` 與 seq 併成一個 uint64，以 `atomic.CompareAndSwap` 更新，不用 mutex（E08-D014）。algo 必須在該帳號的 `AccountDecl.algo_ids` 裡，否則 SDK 同步回 error，且不取號（E06-D033、E08-D014）。
7. **`td.order` 的回覆只有 ack 或本地拒絕（E06-D010）**：`accepted` 為真只表示 TD 收下了，之後的狀態從 `td.evt` 的 `OrderUpdate` 拿；`accepted` 為假是 1xx，原樣交給策略。**沒有 responder**：當成 101（`ACCOUNT_OFFLINE`）的本地拒絕交給策略（§7、DECISIONS E08-D001）。一次 `Submit`、`Cancel`、`BatchCancel` 的等待是 `InitPayload.recv_window_ms`（E08-D043）。**`Submit` 逾時**：以 `ORDER_STATUS_UNKNOWN` 交給策略，不查 `td.view`、不重送（E08-D043、E06-D005）。**撤單逾時**：不標 `UNKNOWN`、狀態不變，SDK 不另外通知策略，等 `td.evt`（E08-D045）。
8. **商品資料（E08-D004、E08-D023–E08-D026）**：策略交易哪些商品是策略自己的設定（`params`），平台不從 spec 推。策略在 `OnInit` 或 `OnStart` 以 ticker 向 SDK 解析（`sym` 的 `GetRequest`，可以阻塞），拿到 UID 與欄位；SYM 查不到就是平台不支援，以 `INIT_FAILED` 結束。其他 hook 呼叫 `Resolve` 同步回 error。之後的 hook 不得阻塞。`Info(uid)` 只讀 SDK 的記憶體，回 `(symbol.Info, error)`；這個 session 沒有 `Resolve` 過的 UID 回 error，框架不重試、也不替策略結束（E08-D024、E08-D025）。SDK 只追蹤 `Resolve` 過的商品：訂閱它們所屬的 `sym.v1.changed.{venue}.{category}`，收到通知、約每 5 分鐘比對 revision 發現落後、或 NATS 重連時，以 `GetByUidRequest` 重拿那些列（§7.3）。事件一律照常立即交付。沒見過的 UID（策略沒有 `Resolve` 過的，例如 `Recon` 裡的舊持倉）不在背景查 SYM，也不扣住事件（E08-D023）。一期沒有 selector（E08-D022）。已解析的商品下架、`changed` 之後搬到終結表時，以 `GetByUidRequest` 加 `include_inactive` 重拿，保留最後一份並標為不活躍，`Info` 仍查得到（E08-D026）。不要每次下單都問 SYM。

### 9.5 交付語意（DECISIONS E08-D003、E08-D015–E08-D017、E08-D057–E08-D061）

1. **單一 dispatcher**：hook 一次一個、依序執行。策略程式碼不需要鎖。跨執行緒只有 `Post` 一個入口。
2. **兩個 buffer：**
   - TD：`td.evt`、下單與撤單的回覆、timer、`Post`、`md.view` 與 `md.fetch` 的回覆。FIFO，不丟。上限是 SDK 內的常數 65536 筆，不進契約，spec 不能設定；滿了以 `FAILED` 結束（E08-D015）。同一個 timer 積了多次只觸發一次。
   - MD：所有行情。一塊預先配好的 32 MiB `[]byte` ring，每則可變長。放不下就丟最舊的若干則直到放得下，跳過正在讀的那一則；單則大於整個 ring 直接丟，計入 `dropped`（E08-D016）。
3. **每跑完一個 hook**：TD 有東西就先取 TD；TD 空了才取 MD。
4. **prehook**：MD 事件交給 hook 之前只看 envelope：`now − md_ts_ns > recv_window_ms` 就丟，不解碼 payload。通過的才解析。`now` 在 dispatcher 從 ring 取出這則時取，不是收訊時，所以 window 包含本地排隊。
   - **`orderbook_delta` 不進 prehook（DECISIONS E08-D003）：** 一律解碼、套進本地 book，book 才不會因為過載跳號。只有「呼叫 `OnBook`」受同一個規則管：太舊就不呼叫，下一則新鮮的套用之後才呼叫。這種 delta 不計入 `QueueStat.dropped`。
5. **hook 的時間預算：**
   - 軟上限：hook 執行超過 100ms 計一次 `hook_slow`，並記 log。`hook_blocked_ms` 是目前這個 hook 已執行的時間，沒有 hook 在跑就是 0。100ms 是 SDK 常數，不是 spec 欄位（E08-D017）。
   - 超過 `Limits.hook_timeout_s`：送 `Exiting(HUNG)`，不跑 `OnStop`，exit 21。
   - 沒有 `Stop` 的結束（策略 `Exit()`、`Fail()`、`Command.fail`、hook 拋出例外、fd 3 讀到 EOF）：`OnStop` 也算一個 hook，套 `hook_timeout_s`。超過就 exit 21，不送 `Stopped`；`Exiting` 已經送了，不再改（DECISIONS E08-D003）。結束途中交付 TD 的 hook 同樣受這個上限管。
   - hook 拋出例外：送 `Exiting(CRASHED)`，跑 `OnStop`，exit 20。
6. `QueueStat` 的 `depth`、`dropped`、`max_age_ms` 照實回報。`dropped` 含 recv window 丟掉的、ring 放不下丟掉的，以及壞 envelope（E08-D049）。
7. **本地訂單簿（E08-D057–E08-D061）：** `orderbook_delta` 由 SDK 套進本地 book，快照只向 MD 的 `md.view` 要，不向交易所要。斷號或 `conn_epoch` 改變就把這本標成未同步並重要。`not_synced` 與其他讀取失敗以 1 秒起、加倍到 30 秒退避。等快照時增量留在內部緩衝，上限 10000 筆或 10 秒，到了就丟掉並重要；緩衝裡的 delta 不是 hook。快照之後只套比快照新、而且連續的 delta。`reset` 清掉本地本、標未同步、重要快照，不套這則的檔位。檔數用 `book.DefaultN`，與 MD conn worker 相同（E04-D030），不用快照的窗口大小；tick 仍取自已 `Resolve` 的 SYM `Info` `price_tick`；沒有、或小於等於 0，或 UID 還沒解析，維持未同步並記日誌，不查 SYM。未同步時 `Book` 回 `(nil, false)`。`OnBook` 只在已同步、這一則已經套上、而且 `md_ts_ns` 仍在 `recv_window_ms` 內時呼叫。Go 實作在 `sdk/internal/bookpath`。

### 9.6 帳號狀態（DECISIONS E06-D003、E08-D053–E08-D056）

1. `OnStart` 之後，以 `td.view` 的 `ReconRequest` 取得第一份 `Recon`（E06-D033），處理方式同第 2 條。`td.evt` 在送出請求之前就訂好；回覆到之前收到的事件先扣住，回覆交給 `OnRecon` 之後，只放行 incarnation 相同、`seq` 大於 `ViewReply.seq` 的事件。扣住沿用 TD buffer 上限 65536，滿了以 `FAILED` 結束（E08-D041、E08-D015）。一次 `ReconRequest` 的等待是 `InitPayload.recv_window_ms`；逾時或沒有 responder 以 1 秒起、加倍到 30 秒重問，事件繼續扣住（E08-D042）。接上之後，新的 incarnation 或同一 incarnation 的 `seq` 斷號，扣住後續事件並重走這段流程，回覆交給 `OnRecon`（E08-D044）。第一份回覆放行時，incarnation 比 `ViewReply.incarnation` 舊的扣住事件可以丟掉；比它新的留在佇列裡，再送一次 `ReconRequest`，與接上之後相同，重複的詢問沿用 E08-D042 的退避（E08-D044）。
2. `Recon`（`td.evt` 推的與 `td.view` 問到的）：`open_orders` 以 `client_order_id` 的 algo 欄位篩成自己的 `algo_ids`，解不開的不屬於任何 algo；`positions`、`balances` 原樣交給 `OnRecon`。交給策略的 `Recon`、`OrderUpdate`、`Fill` 都要帶上帳號的 `api_id`：訊息本身沒有，帳號由 subject 決定。
3. 不提供可用性 hook（DECISIONS E08-D001）。策略只會拿到拒單與新的帳號狀態。
4. SDK 維護 OMS（自己 algo 的活單）與 Ledger（帳號的持倉、餘額），先更新再呼叫 hook（E08-D005、E08-D053–E08-D056）。`OnOrder`、`OnFill`、`OnPosition`、`OnBalance`、`OnRecon` 裡，`Order`、`OpenOrders`、`Position`、`Balance` 已經含這則事件。`OnFill` 不改這四個讀取的內容。下單被拒、逾時也當成單的狀態變化交給 `OnOrder`。`BatchCancel` 從這份 OMS 挑單（§9.4 第 5 條）。Go 實作在 `sdk/internal/tdpath`。
   - **OMS。** 鍵是 `(api_id, client_order_id)`，只放這個 session 在該帳號宣告的 algo。`Submit` 在送出前寫入 `PENDING_NEW`。`OrderUpdate` 帶完整狀態，整筆覆蓋，SDK 不推算下一狀態，也不從 `Fill` 改單。終態（`FILLED`、`CANCELLED`、`REJECTED`、`EXPIRED`）在 `OnOrder` 返回後從 OMS 移除：hook 裡面 `Order` 看得到這筆終態，`OpenOrders` 不含終態。
   - **Recon 換 OMS（E08-D054）。** 整份換成快照裡自己 algo 的活單。快照有的鍵用快照那一筆（終態不放進 OMS）；快照沒有的刪掉。例外只對這份回覆所回答的那次 `ReconRequest`：本地仍是 `PENDING_NEW`、是在該次請求送出之後才 `Submit`、而且快照沒有這筆，則保留。`td.evt` 推來的 `Recon` 不是 `ReconRequest` 的回覆，不適用這個例外。
   - **Ledger。** 每個 `api_id` 一份。持倉以 UID 為鍵，餘額以 asset 為鍵。`PositionUpdate`、`BalanceUpdate` 是絕對值，覆蓋出現的鍵，其他鍵留著；數值是 0 也照覆蓋留下，直到一份不含它的 `Recon`。同一則裡同一個鍵出現多次，後者覆蓋前者。`Recon` 整份換掉持倉與餘額，沒有列出來的鍵就沒有了。`Fill` 不改 Ledger，SDK 不從成交推算持倉或餘額（E08-D055）。
   - **餘額三格（E08-D053）。** `free`、`locked`、`prelock` 的意思見 §7.2。SDK 原樣保存，不把 `prelock` 轉進 `locked`，也不用三格相加。
   - **讀取（E08-D056）。** `Order`、`OpenOrders`、`Position`、`Balance` 每次回傳 protobuf 複製，讀取不阻塞。

### 9.7 平台的保險：不依賴 SDK

- STS agent 以 `Beat` 判斷 worker 是否卡住（DECISIONS E09-D020、E09-D066–E09-D068）：
  - 收到 `Done(START, ok)` 之前（`Init` 進行中，或 `Start` 已送、還沒完成）：超過該階段的 `deadline_ms` 再加 10 秒沒有 `Beat`。
  - 之後，含 `Init` 完成、等 `start` 的 `WAITING`：超過 `hook_timeout_s` 再加 10 秒沒有 `Beat`。兩個值都是 agent 解析後的實值（E08-D003、E09-D018）。
  - 符合任一條件，STS 經 shim 直接送 SIGKILL，不先送 `Stop`（E09-D066）。shim 的 `ExitInfo` 照實記這次結束（signal 9，沒有 `Exiting`）。STS agent 記住這個 incarnation，判斷重啟與寫 `SessionStatus` 時把 outcome 當成 `HUNG`、reason 當成 `no_beat`（E09-D067）。之後照 `RestartPolicy` 決定要不要重啟（E09-D019）。
- 這條保險讓「heartbeat 可能被 hook 卡住」的語言也能安全上線，但 hook 卡住期間的狀態不會被即時發現，延遲最多到上面的門檻。

### 9.8 conformance 測試

平台提供 `mftiko-conformance`（E08-D028）：黑箱。假 shim 走 fd 3，行程內嵌的 nats-server（E08-D030，`max_control_line` 262144，E08-D027），加上假的 MD、TD、SYM 對端（發 `md.f`、`td.evt`，回應 `td.order`、`td.view`、`md.view`、`md.fetch`、`sym`）。不另開一條給觀察用的 NATS subject。`Stage(running)` 表示訂閱已經完成。

這一期只收 worker 的執行命令（E08-D036）：`mftiko-conformance [-json 路徑] -- <worker argv>`。`-json` 另把同一份 JSON 寫到檔案。OCI artifact 解開留介面，這一期呼叫它是錯誤，不實作。

標準測試策略的行為由工具寫進 `InitPayload.params` 的 JSON 控制（E08-D035）。每種語言各自實作同一份設定。這個 repo 的 Go 版在 `examples/strategies/conform`，經 `sdk.Run` 掛上。buffer、prehook、recon 水位、`deadline_ms` 中止是 SDK 的事，不在策略裡重做。每個 PR 的 CI 都跑這支 Go 策略，整套必須全綠（E08-D064）。fakeworker 的違規檢查仍在 `go test`：合規跑一次，再以 `-violate <id>` 一次打開一種違規，該項必須 fail（E08-D033、E08-D034、E08-D039）。

工具對每一項給 `pass`、`fail` 或 `skip`。stdout 是下面的 JSON，stderr 是同內容的表格（E08-D031）。任一項 `fail`，行程 exit 1。沒有 `fail`（只有 `pass` 與 `skip`）exit 0。工具自己無法跑（沒有命令、artifact 還沒實作、NATS 起不來、不認識的檢查 id）exit 2，不出報告。STS 部署時不讀這份報告（E08-D032）。

`Hello.contracts` 是這支 worker 實作的 major。某項檢查相依的契約有一個不在裡面，該項 `skip`，不算 `fail`（E08-D038）。`detail` 寫缺的名字，例如 `md.v1 not in contracts`。

這一期的夾具：session `c0ffee`，`api_id` 7，`algo_id` 1，商品 UID 1001，feed `trade.Binance_Spot_BTCUSDT`，fetch 的 venue 是 `Binance`。MD instance 用 natsdev 產生的那一個，worker 用 `Start` 的 binding subject，不自己猜 instance。`recv_window_ms` 填 500，`hook_timeout_s` 填 1。

#### 設定（`InitPayload.params`）

JSON 物件，工具產生。欄位：

| 欄位 | 意義 |
|---|---|
| `hook_sleep_ms` | hook 進入後睡這麼久，用來證明 `Beat` 不被 hook 堵住 |
| `order_count` | 要送出的下單筆數 |
| `init_sleep_ms` | `OnInit` 睡這麼久，配合 `Init.deadline_ms` |
| `start_sleep_ms` | `OnStart` 睡這麼久，配合 `Start.deadline_ms` |
| `on_stop_sleep_ms` | `OnStop` 睡這麼久，配合 `hook_timeout_s` |
| `self_exit` | true：`Start` 成功後自己結束，不等 `Stop` |
| `probe_reads` | true：額外打 `md.view` 與 `md.fetch` |
| `scenario` | 空字串，或 `priority`、`prehook`、`ring`、`overflow`、`cloid`、`subject`、`recon`、`ctx`、`beat`、`shutdown` |
| `until_seq` | 看到這個行情 seq 才開始交付 |
| `api_id` | 帳號 |
| `algo_id` | 該帳號宣告過的 algo |
| `uid` | 商品 UID |

#### 檢查

| id | 相依契約 | 通過 |
|---|---|---|
| `flow` | `worker.v1` | 下面四段都過 |
| `beat` | `worker.v1` | hook 那一行印出之後的 2 秒內，`Beat` 的次數還在增加。合約仍是每 1 秒一則（§9.2 第 3 條）；多等一個週期，是因為起點可能剛好在一則 Beat 之後，下一則加上計時與排程的誤差會超過 1 秒 |
| `deadline` | `worker.v1` | `deadline_ms` 到了就 `Done(verb, false)`、`Exiting(INIT_FAILED)`、reason 是 `deadline`、`Stopped(false)`、exit 10；不等 hook，不跑 `OnStop`。`Init` 與 `Start` 各測一次 |
| `shutdown` | `worker.v1` | `Stop`：`Exiting(DONE)` 在 `Stopped(true)` 之前，exit 0。自己結束且 `OnStop` 超過 `hook_timeout_s`：`Exiting` 的 outcome 仍是 `DONE`，不送 `Stopped`，exit 21 |
| `buffer_priority` | `md.v1`、`td.v1` | 一則 TD 與一則 MD 都在佇列時，先 `OnOrder` 再 `OnTrade` |
| `prehook` | `md.v1` | `now − md_ts_ns > recv_window_ms` 的不交付，`now` 在取出時取；ring 是 32 MiB，放不下就丟最舊的直到放得下，單則大於 ring 直接丟。兩段都要 `QueueStat` 名稱 `md` 的 `dropped` ≥ 1。第二段是 70 則 512 KiB 再加 seq 10000 的小訊息：交付含 10000、不含 seq 1 |
| `td_overflow` | `td.v1` | TD 佇列上限 65536。第 65537 則以 `FAILED` 結束，reason 是 `td buffer full`，跑 `OnStop`，exit 30 |
| `cloid` | `td.v1` | 送出的 `client_order_id` 解得開、`ver` 是 1、algo 在該帳號的宣告裡、`(ts_ms, seq)` 嚴格遞增 |
| `subject` | `stsctl.v1`、`md.v1`、`td.v1` | 同一份 session JWT 擋下未授權的 publish／subscribe；下單出現在 `td.order.{api_id}` |
| `recon` | `td.v1` | 訂好 `td.evt` 再問 `td.view`。只交付 `source.incarnation` 等於 `ViewReply.incarnation`、且 `seq` 大於 `ViewReply.seq` 的事件。夾具的快照是 incarnation 3、seq 5。第一份回覆前可送出同一 incarnation、seq 4 與 seq 6；水位套上之後不送別的 incarnation（E08-D044：live 上的不同 incarnation 會重問，而下一份回覆仍是 seq 5，cursor 會退回） |
| `ctx` | `md.v1`、`td.v1` | `OnOrder` 與 `OnPosition` 各一則，呼叫當下本地狀態已經含這則事件（`ctx` true） |

`flow` 每段一個新行程：

1. `Start` 在 `Init` 之前：`INIT_FAILED`，不得 `Done(ok)`。
2. 第二則 `Init`：第一則 `Done(ok)`，第二則 `Done(false)`，exit 10。
3. `Init` 成功後、`Start` 之前的 `Stop`：`DONE`，有 `OnInit`，沒有 `OnStart` 與 `OnStop`，`on_stop_completed` false。
4. 正常：`live_digest` 的 `Applied` 在 `Done(INIT, ok)` 之前；`Done(START)`；`Stage(running)`；`Relay` 原樣帶回 corr；`Live` 回對應的 `Applied`；`Stop` 後先 `Exiting` 再 `Stopped`，exit 0；`OnInit`、`OnStart`、`OnStop` 都有。

#### 報告 JSON

```json
{"checks":[{"id":"flow","status":"pass","detail":"..."}]}
```

`id` 是上表的名字。`status` 是 `pass`、`fail` 或 `skip`。`detail` 是人讀的原因，空的可以不出現。

#### 策略觀察

stdout 一行一個 JSON 物件，行程結束後再讀，不拿它和 fd 3 的時間戳對先後。`false` 與 `0` 也要能看出有這個欄位。欄位：`hook`（`OnInit`、`OnStart`、`OnStop`、`OnTrade`、`OnOrder`、`OnPosition`、`OnRecon`、`gate`）、`ctx`（bool）、`seq`、`md`（bool）、`incarnation`、`view`、`fetch`、`queued`。`gate` 表示訂閱已完成、還沒交付。

#### 違規

fakeworker 的 `-mftiko-conformance` 跑上面這份行為；`-violate <id>` 只打開一種，id 與上表相同；`-contracts` 是逗號分隔的 `Hello.contracts`，預設 `worker.v1,stsctl.v1,md.v1,td.v1`。不認識的 `-violate` 使 worker exit 2。

| `-violate` | 做錯的事 |
|---|---|
| `flow` | `Init` 之前回 `Done(ok)`；第二則 `Init` 也回 `Done(ok)`；`Start` 之前的 `Stop` 去跑 `OnStop`；`Stopped` 寫在 `Exiting` 前面 |
| `beat` | 不送 `Beat` |
| `deadline` | `deadline_ms` 大於 0 時立刻 `Done(ok)`，不睡 |
| `shutdown` | `Stopped` 寫在 `Exiting` 前面 |
| `buffer_priority` | 先交付 MD 再交付 TD |
| `prehook` | 不套 recv window，ring 也不丟 |
| `td_overflow` | 收下第 65537 則，stdout 寫 `queued` 65537，不以 `FAILED` 結束 |
| `cloid` | `client_order_id` 用解不開的字串 |
| `subject` | 下單送到 `td.view.{api_id}`，而不是 `td.order.{api_id}` |
| `recon` | 水位以下、incarnation 不同的事件也交付 |
| `ctx` | `ctx` 填 false |

### 9.9 給 Python 實作的建議

- `Beat`、NATS 收訊放在獨立的 thread，hook 在主 thread。hook 卡在不放 GIL 的 C 擴充裡時，`Beat` 會停，§9.7 會接手。
- 不要讓 NATS 收訊和 dispatcher 共用同一個 asyncio loop：hook 一卡，收訊也跟著停（mftik 的經驗）。收訊放在獨立 thread，經由 thread-safe 的佇列交給主 thread。
- `multiprocessing` 用 `spawn` 或 `forkserver`：有背景 thread 時 fork 不安全。
- 沒有 `GOMEMLIMIT` 這種機制。`Limits.memory_mb` 由 SDK 自己遵守（例如監控 RSS），或交給主機的資源限制。

## 10. 待定

P1–P5 已依 DECISIONS 定案：P1 見 E08-D003 與 §1，P2 見 E04-D002，P3 見 E09-D003，P4 見 E09-D006，P5 見 E08-D003。v0.24 新增 P6–P10，v0.25 新增 P11，v0.28 新增 P12，v0.34 新增 P13–P15，v0.36 改寫 P13，v0.41 定案 P13 的寬度與範圍，v0.49 定案 P7，v0.54 定案 P6、P9，v0.56 新增 P16，v0.60 新增 P17。v0.73 定案 P14，從這一節移出，寫在 §7 與 E04-D055–E04-D057。v0.87 定案 P15（E08-D004）。v0.95 新增 P18。v1.29 定案 P8（E10-D006），v1.53 從這一節移出，寫在 §3.2 W10 與 E10-D006、E10-D007、E10-D008、E10-D042：

- **P6 `DEAD` 的確切時機（E4、E6，2026-10-11）。** 已定，寫在 §3.5。`BACKOFF` 寫成 `PENDING`，`LOST` 維持最後狀態，仍被需要且不再重拉的 `EXITED` 才寫 `DEAD`。
- **P7 SYM 的變更通知（E2，2026-10-09）。** 已定，寫在 §7.3。subject 是 `sym.v1.changed.{venue}.{category}`（E04-D057），內容是 `Changed{venue, category, revision}`。用人讀的名字，不用 UID，不帶 `inst`。漏收以約每 5 分鐘的 `RevisionsRequest`，以及 NATS 重連時的比對發現；落後就重載那張活躍表。
- **P9 MD、TD 的 artifact 版本放在哪裡（E4、E6，2026-10-11）。** 已定，寫在 §8、E09-D002、E01-D006。不釘在 controller 的部署設定，也不釘在 etcd。每次 spawn 取當時的最新；E9 之前是這個 repo 建出的 binary。換版不排程換 incarnation，換 agent 也不重啟正在跑的 worker。策略的 digest 仍是 `StrategyRef.code`。
- **P10** 已在 v0.24 完成：session 的宣告抽成 `mftiko.session.v1`，`Side`、`OrderType`、`TimeInForce` 在 `common.v1`。
- **P11 instance 永久下線的程序（E12）。** ack、`WorkerStatus`、`SessionStatus` 不綁 lease，instance 不再回來時會一直留著：非 terminal 的 `SessionStatus` 加上 `RUNNING` 的 `WorkerStatus`，會讓 controller 的 N4 一直等，MD、TD 的需求與 `algo_id` 的佔用都不會釋放。這些 key 的寫入者是那個 agent（W1），維運也不該直接改。候選：API 宣告某個 instance 已退役，controller 據此把它的 session 當成 terminal，再由一次性的工具以該 instance 的身分清掉它的前綴。
- **P12 沒有共同 major 時怎麼收尾（E1-13）。** agent 只能 `Signal`（§4.2）。worker 結束之後，shim 依 S3 等 `Release` 才退出；在那之前，同一個 worker key 的下一個 shim 會因為 socket 還有人服務而起不來（§4.1）。候選：`Release` 也列入跨 major 不變的部分。
- **P13 symbol UID 的寬度與範圍（E02-D003，2026-10-09）。** 已定，寫在 E02-D003 與 §7.3。
  - **已定。** UID 是 SYM 分配的隨機整數：唯一、永不重用，不是單調序號，也不是 Postgres sequence。活躍表與終結表合在一起唯一。UID 跟列一起持久化，重啟與重載都不改它。同一個 ticker 下架再上市拿新的 UID。碰撞時 SYM 重試。契約型別是 `uint32`，範圍 `1..4294967295`（`1..2^32-1`），0 非法。Postgres 欄位是 `bigint`，`CHECK (uid BETWEEN 1 AND 4294967295)`。`symbol_filter` 也拆成活躍表與終結表，列進終結狀態時跟列一起搬。SYM 的 `List` 用 offset＋limit 分頁，跨頁不一致可以接受。`Universe.current` 是 selector，尚未定，不在 E02-D003 的範圍；selector 沒有 UID。NATS subject 不用 UID，見 §7、E04-D055。策略 hook 見 P15。
- **P15 策略 hook 看 ticker 還是 UID（E02-D003，2026-10-16）。** 已定，寫在 E08-D004、E08-D023–E08-D026 與 §9.4 第 8 條。策略 API 一律用 UID：下單、撤單、行情與 TD 事件都是 UID。ticker 只出現在 spec 與策略自己的設定，策略在 `OnInit`、`OnStart` 以 `Resolve` 換成 UID。SDK 只追蹤 `Resolve` 過的商品；事件不因沒見過的 UID 被扣住。
- **P16 NATS 多 region 的拓撲（E4-03，E04-D012）。** 還沒定。候選是 cluster 加 gateway，或 leaf node。這一版 dev 只有一台 server，不開 TLS。跨 region 部署時再定。
- **P17 同一個出口 IP 上多個帳號共用的限速額度（E06-D009）。** 還沒定。Binance 的 request weight 依 IP 計，UM 與 CM 共用下單額度；同一台主機上的 account worker 各自依 `OrderRate` 與 `Usage` 限速，看不到彼此。候選是 TD agent 分配每台主機的額度，或每個帳號一個出口 IP。
- **P18 誰把 `td.evt` 上的成交落庫（E06-D038，2026-10-16）。** 還沒定。account worker 把 `Fill`，以及終態且 `filled_qty > 0` 的 `OrderUpdate`，發在 `td.evt.{api_id}`。目前沒有人訂閱為了落庫，也沒有人寫 Postgres。不會是 TD agent。歷史補正的交易所讀取仍是無狀態服務，比照 md.fetch（E06-D001）；誰呼叫 `td.backfill.{venue}`、誰把解開的帳號憑證放進那次請求，跟落庫是同一件還沒定的事，也不會是 TD agent（它沒有 NATS user，E06-D039）。queue、cursor 與 `(api_id, fill_id)` 去重仍是 E06-D001 的規則，執行的人就是這一題。
