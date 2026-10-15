# MD feed 路徑驗收

E4 的驗收（#6；E4-09 BinanceUM、E4-10 Binance 現貨、E4-11 Bybit／OKX）。需求項目手寫（E13 之前），對交易所的 testnet 跑四條：

1. `md.v1.f` 收到事件，同一個 feed 上 `seq` 連續。
2. 晚加入：先訂 `md.v1.f`，再向 `md.v1.view` 拿當時的 book 或 tape，之後只套用較新的 seq。
3. 連線斷掉再接上，`conn_epoch` 加一。
4. 換掉 md-agent 的行程，websocket 不斷，事件繼續來。

`accept.sh` 驅動這組環境，`mftiko-md-accept` 負責檢查。每個子指令在檢查失敗時以 1 結束。

## 需要的東西

- Docker、Go 1.26+。shim 只能在 Linux 上跑，所以 md-agent、shim、conn worker 都在 `runner` 容器裡執行（`accept.sh up` 會建出 `bin/linux/`）。macOS 也可以用。
- 能連到交易所的 testnet。行情是公開資料，不需要 API key。

這組 compose 是獨立的專案（`mftiko-accept`），沒有對外的 port，不會和 `deploy/dev` 或機器上其他服務衝突。紀錄寫在 `deploy/accept/data/work/`（gitignore）。

## 環境

```sh
make accept-up        # 等於 deploy/accept/accept.sh up
```

`up` 會依序：建 Linux binary、`mftiko-nats-dev` 產生 NATS 憑證、起 etcd／NATS／PostgreSQL、套 migration（`mftiko-migrate` 還沒做，這裡直接用 psql）、`setup`（etcd 的角色與使用者，與部署相同的 RBAC，然後開 auth）、`creds`（觀察端用的、可收發所有 subject 的 NATS 使用者；SYM 也用它，因為 `mftiko-nats-dev` 不發 SYM 使用者）、啟動 `mftiko-sym`，最後要 SYM 立刻刷新並印出商品。可以重跑。

```
... refreshed map[BinanceUM/Future:4 BinanceUM/Perp:924] ... failed map[]
... BinanceUM_Perp_BTCUSDT uid=... exch=BTCUSDT status=SYMBOL_STATUS_TRADABLE price_tick=0.1
```

換交易所時設定 `ACCEPT_VENUE`、`ACCEPT_SYM_FEEDS`（conn worker 會載入該 venue 的每一個 category，所以要全部列出）、`ACCEPT_TICKER`，並在 `endpoints/ws.json`、`endpoints/rest.json` 加上那個 venue 的 testnet 位址。目前列了驗證過的 BinanceUM（E4-09）、Binance 現貨（E4-10）、Bybit testnet 與 OKX demo（E4-11）。`rest.json` 給 SnapshotREST 的快照用（`md-agent -rest-endpoints`）：testnet 的串流必須搭配 testnet 的快照，正式站的 update id 對不上。

Binance 現貨：

```sh
ACCEPT_VENUE=Binance ACCEPT_SYM_FEEDS=Binance/Spot ACCEPT_TICKER=Binance_Spot_BTCUSDT make accept-up
```

現貨只有一條 route（`default`），`aggtrade` 與 `orderbook_delta` 會放在同一個 conn（`md/Binance/default/0`）上，所以第 3 條切一次就會讓兩個 feed 一起進到下一個 epoch。tick 是 0.01，2048 tick 的窗大約只有最佳價上下 20 USD。

Bybit linear 與 OKX USDT 本位永續（E4-11）可以在同一組環境一起跑：

```sh
ACCEPT_VENUE=Bybit ACCEPT_TICKER=Bybit_Perp_BTCUSDT \
ACCEPT_SYM_FEEDS=Bybit/Spot,Bybit/Perp,Bybit/Future,Okx/Spot,Okx/Perp,Okx/Future,Okx/Inverse make accept-up
deploy/accept/accept.sh refresh -venue Okx -ticker Okx_Perp_BTCUSDT
```

這兩家的簿是 SnapshotStream：訂閱後的第一則就是全本，不打 depth REST，所以不需要 `rest.json`。兩家都有 `trade` topic。Bybit 走 `linear` route，OKX 走 `public` route（位址是 `wss://wspap.okx.com:8443`，即 demo，不需要額外的 header）。Bybit testnet 的 `orderbook.50` 很稀疏，50 檔分散在最佳價上下大約 2000 USD，落在窗內（±204.8 USD）的只有十幾檔，這是 D431 的結果。

以下以 BinanceUM 為例。`A=deploy/accept/accept.sh`。BinanceUM 沒有 `trade` topic，trade 類用 `aggtrade`。

## 1. 收到事件，seq 連續

```sh
$A bg watch watch -out /work/watch.jsonl     # 訂 md.v1.f.dev.>，每則寫進 watch.jsonl
$A agent                                     # log: data/work/agent-1.log
$A demand put s1 aggtrade.BinanceUM_Perp_BTCUSDT
$A demand put s1 orderbook_delta.BinanceUM_Perp_BTCUSDT
$A acks                                      # 兩筆 ENTRY_STATE_READY，各自的 subject 與 worker
tail data/work/watch.log
```

通過的條件：兩個 subject 都出現 `first epoch=1 seq=1`；`orderbook_delta` 的第一則是 `reset=true`（快照）；`SUMMARY` 一直是 `gaps=0 dups=0`；book 的 `resets` 只有 1（沒有因為斷號而重抓）。每一則 payload 的 `uid` 必須是 SYM 活躍表上該 ticker 的 uid，0 或對不上就失敗（D461）。

## 2. 晚加入

```sh
$A late -feed orderbook_delta.BinanceUM_Perp_BTCUSDT -follow 15s
$A late -feed aggtrade.BinanceUM_Perp_BTCUSDT -follow 10s
```

`late` 會先訂 `md.v1.f` 並 flush，再問 `md.v1.view`：

- book：從 `BookSnapshot` 的 `(epoch, S)` 開始，丟掉 ≤ S 的 live，第一則套用的必須是 S+1。用 `sdk/book` 折（tick 從 SYM 取），跟完一段之後再問一次 `md.v1.view`，在同一個 cursor 上逐檔比對。
- tape：往回分頁讀到 `-since`，印出 gap，從 `TapePage.head` 接上 live（D437）。

通過的條件：`gaps=0`；book 的比對結果是 `differing within 1024 ticks of the best: 0`。離最佳價較遠的檔（窗的外半）若有差異，只印出、不算失敗。這是 D431 的結果：一檔在晚加入者的快照當下落在窗外，或曾經在窗外被丟掉，在它下一筆更新之前晚加入者都不會知道它；worker 的環則還留著那一格。book 快照、tape 的每一則、以及之後的 live `md.f`，`uid` 都必須是 SYM 上這個 feed 的 uid（D461）。

## 3. 斷線再接上

```sh
$A conns                                     # 每個 conn worker：key、pid、連到交易所的 socket
$A cut -conn md/BinanceUM/public/0
$A cut -conn md/BinanceUM/market/0
$A conns                                     # 同一個 pid，新的 socket
grep -v SUMMARY data/work/watch.log | tail
$A late -feed aggtrade.BinanceUM_Perp_BTCUSDT -follow 5s
```

`cut` 從行程外面用 `pidfd_getfd` 取出 worker 連到交易所的 socket，再呼叫 `shutdown`。對 worker 來說跟交易所主動斷線一樣，行程本身不受影響。

通過的條件：watch 印出 `new epoch: (1,n) -> (2,1)`，book 那一則是 `reset=true`；另一個 conn 上的 feed 不受影響；tape 有一個 `tape gap`，其中的 `last (1,n)` 與 `first (2,1)` 吻合；`conns` 顯示同一個 pid。

## 4. 換掉 md-agent

```sh
$A conns
$A stop-agent                                # SIGTERM
$A ps                                        # shim 被 init 收養，md-conn 還在
$A agent                                     # log: data/work/agent-2.log
grep "shims adopted" data/work/agent-2.log   # adopted=2 unreachable=0
$A conns                                     # pid 與 socket 都沒變
$A acks                                      # incarnation 不變
```

通過的條件：整段期間 watch 沒有 gap，`conn_epoch` 不變。

## 收尾與紀錄

```sh
$A stop watch                                # 印出最後的 SUMMARY；有 gap 或 dup 時以 1 結束
make accept-down                             # 停掉並刪除 volume；data/work 留著
```

紀錄：`data/work/watch.log`、`watch.jsonl`（每一則 md.v1.f 的 `(epoch, seq)`）、`agent-*.log`、`sym.log`，以及上面各個指令的輸出。新的一輪要乾淨的紀錄時，先清空 `data/work/`。E4-09 的紀錄在 #193，E4-10 在 #195，E4-11 則在這張票的 PR。

# TD 交易路徑驗收

E6 的驗收（#8；E6-12 BinanceUM，E6-13、E6-14 補 Bybit、OKX）。用同一組環境（先 `make accept-up`），需求項目與帳號手寫（E13、E10 之前），對 testnet 跑四條：

1. 下單、撤單、`BatchCancel` 都通，`td.evt` 收到對應的 `OrderUpdate`、`Fill`。
2. 拔掉私有連線：進 `DEGRADED`、新單 102、撤單照常；重連後收到 `Recon(RECONNECT)`。
3. 換掉 td-agent 的行程，account worker 不停，私有連線不斷。
4. 帳號切到雙向持倉，worker 以 `hedge_mode` 起不來（`REJECTED`，不重拉）。

`accept.sh td ...` 在 runner 裡執行 `mftiko-td-accept`。每個子指令在檢查失敗時以 1 結束；`submit`、`cancel`、`batch` 在 TD 本地拒絕時也是 1，並印出 code。

## 需要的東西

- testnet 的 API key。憑證寫成 `mftiko-seal` 的 JSON（`{"key_type", "key", "secret"}`），放在 `data/work/`（gitignore），權限 0600。BinanceUM 的 adapter 只用 ed25519 簽名：`key_type` 是 `ed25519`，`secret` 是 PEM 私鑰的內容。
- 帳號是單向持倉、沒有掛單與持倉。`td mode` 讀或切換持倉模式，只對 testnet 的 host。

## 環境

```sh
A=deploy/accept/accept.sh
$A td-up                                     # TD instance（td1）的 etcd 使用者與 topology、seal key
docker compose -f deploy/accept/compose.yaml exec -T runner sh -c \
  '/mftiko/mftiko-td-accept account -instance td1 -api-id 1 -kind BinanceUM -seal-key /work/td-seal.key < /work/binanceum.creds.json'
$A td mode -creds /work/binanceum.creds.json # {"dualSidePosition":false}
$A td-bg td-watch watch -api-id 1 -out /work/td-watch.jsonl
$A td-agent                                  # log: data/work/td-agent-1.log
$A td demand put s1 1
$A td acks                                   # ENTRY_STATE_READY，worker account/1
```

td-agent 用 `endpoints/rest.json` 的 BinanceUM testnet REST host；交易 websocket（`testnet.binancefuture.com/ws-fapi`）與私有連線（`stream.binancefuture.com/private/ws`）由它推得。worker 的 log 在 runner 的 `/var/td/log/td/`。

## 1. 下單、撤單、BatchCancel

```sh
$A td submit -api-id 1 -side buy -type limit -qty 0.002 -price <遠低於市價>   # 印出 cid
$A td view -api-id 1 recon
$A td cancel -api-id 1 -cid <cid>
$A td submit ... ; $A td submit ...                                           # 兩張
$A td batch -api-id 1 -cid <cid2>,<cid3>
$A td submit -api-id 1 -side buy -type market -qty 0.002                      # Fill
$A td submit -api-id 1 -side sell -type market -qty 0.002 -reduce-only        # 平倉
```

通過的條件：每張單在 `td-watch.log` 依序有 `PENDING_NEW SUBMITTED`、`ACCEPTED`、`CANCEL_REQUESTED`、`CANCELLED`；batch 的兩張各自 `CANCELLED`；市價單有 `Fill`、`PositionUpdate`，平倉後持倉是 0。

## 2. 拔掉私有連線

worker 約一秒內就重連，所以切斷、送新單、撤單要在同一個 shell 裡接著做：

```sh
docker compose -f deploy/accept/compose.yaml exec -T runner sh -c '
T=/mftiko/mftiko-td-accept
$T cut -instance td1 -host stream.binancefuture.com
$T view -instance td1 -api-id 1 recon | head -1
$T submit -instance td1 -api-id 1 -side buy -type limit -qty 0.002 -price <價>
$T cancel -instance td1 -api-id 1 -cid <掛著的 cid>'
```

`cut` 從行程外面用 `pidfd_getfd` 關掉 worker 連到該 host 的 socket；REST 與交易 websocket 在別的位址，不受影響。

通過的條件：`view` 是 `ACCOUNT_STATE_DEGRADED`；新單回 `102 account is degraded`；撤單 `accepted`、之後 `CANCELLED`；接著 `td-watch.log` 有 `Recon RECON_REASON_RECONNECT`，`view` 回到 `READY`；`td conns` 是同一個 pid、新的 stream socket。

## 3. 換掉 td-agent

```sh
$A td conns                                  # pid 與 socket
$A stop-td-agent
$A ps                                        # shim 被 init 收養，mftiko-td-account 還在
$A td submit ... ; $A td cancel ...          # 沒有 agent 時 td.order 照常
$A td-agent                                  # log: data/work/td-agent-2.log
grep "shims adopted" data/work/td-agent-2.log  # adopted=1 unreachable=0
$A td acks; $A td conns                      # incarnation、pid、socket 都不變
```

通過的條件：沒有新的 spawn；`td-watch.log` 沒有新的 `Recon`。

## 4. 雙向持倉

worker 只在 Start 檢查持倉模式（D616），所以切換後讓它重拉：

```sh
$A td mode -creds /work/binanceum.creds.json -dual true
docker compose -f deploy/accept/compose.yaml exec -T runner sh -c 'kill -9 $(pgrep -f "^/mftiko/mftiko-td-account")'
$A td acks                                   # ENTRY_STATE_REJECTED reason:"hedge_mode"，下一個 incarnation
$A ps                                        # 沒有 mftiko-td-account：不重拉（D608）
```

還原：`td mode ... -dual false`，`td demand del s1 1`，等過 linger（5 秒），再 `td demand put s1 1`，ack 回到 `READY`。

## 收尾與紀錄

```sh
$A stop td-watch                             # SUMMARY；有 gap 或 dup 時以 1 結束
$A stop-td-agent
```

紀錄：`data/work/td-watch.log`、`td-watch.jsonl`（每一則 td.evt）、`td-agent-*.log`，以及上面各個指令的輸出。E6-12 的紀錄在它的 PR。

## Binance 現貨（E6-13）

SYM 要服務現貨的表，所以從 `up` 起換 venue；帳號的 kind 是 `Binance`：

```sh
ACCEPT_VENUE=Binance ACCEPT_SYM_FEEDS=Binance/Spot ACCEPT_TICKER=Binance_Spot_BTCUSDT $A up
$A td-up
docker compose -f deploy/accept/compose.yaml exec -T runner sh -c \
  '/mftiko/mftiko-td-accept account -instance td1 -api-id 1 -kind Binance -seal-key /work/td-seal.key < /work/binance.creds.json'
```

和 UM 不同的地方：

- 下單與私有事件在同一條交易連線（`ws-api.testnet.binance.vision`）。`td conns` 只有它和 REST host 的 socket，沒有 `stream.testnet.binance.vision`。
- 第 2 條切的是交易連線：`cut -host ws-api.testnet.binance.vision`。斷線期間撤單改走 REST。
- 現貨沒有批次撤單（`CancelBatch` 是 1），`batch` 在本地回 `106 batch cancel is not available`（D625），改用單筆 `cancel`。
- `submit` 加 `-ticker Binance_Spot_BTCUSDT`；沒有 `-reduce-only`，買多少賣回多少。限價要在 `PERCENT_PRICE_BY_SIDE` 之內（買單不低於均價的 0.5 倍）。
- 沒有持倉模式，不跑第 4 條。

## Bybit 與 OKX（E6-14）

兩家是統一帳戶，worker 載入 venue 的每個 category，所以 SYM 要服務兩家的每張表。Bybit 的 Inverse、Option 與 OKX 的 Option 沒有公開清單，`refresh` 會說那幾張失敗，不影響 worker。兩個帳號放在同一個 TD instance，api_id 分開：

```sh
F=Bybit/Spot,Bybit/Perp,Bybit/Inverse,Bybit/Future,Bybit/Option,Okx/Spot,Okx/Perp,Okx/Inverse,Okx/Future,Okx/Option
ACCEPT_VENUE=Bybit ACCEPT_SYM_FEEDS=$F ACCEPT_TICKER=Bybit_Perp_BTCUSDT $A up
$A refresh -venue Okx -ticker Okx_Perp_BTCUSDT
$A td-up
docker compose -f deploy/accept/compose.yaml exec -T runner sh -c \
  '/mftiko/mftiko-td-accept account -instance td1 -api-id 1 -kind Bybit -seal-key /work/td-seal.key < /work/bybit.creds.json'
docker compose -f deploy/accept/compose.yaml exec -T runner sh -c \
  '/mftiko/mftiko-td-accept account -instance td1 -api-id 2 -kind Okx -seal-key /work/td-seal.key < /work/okx.creds.json'
```

一次跑一家：`td-bg td-watch watch -api-id N`、`td demand put s1 N`，指令都帶 `-api-id N` 與 `-ticker`。憑證都是 `key_type` `hmac`；OKX 另有 `passphrase`。

- Bybit 由 `rest.json` 的 `api-testnet.bybit.com` 選 testnet。OKX 沒有 testnet host：`td-agent` 帶 `-simulated`，走 demo trading（header `x-simulated-trading: 1`、`wspap.okx.com:8443`）。
- OKX 的 API key 若有 IP 白名單，要加上跑這套環境的出口 IP（否則 `50110`）。demo 帳號要是 `acctLv` 2 以上才能交易 SWAP（否則下單 `51010`）；這只能在 OKX 網站或 app 切換，`td mode -venue Okx` 讀得到 `acctLv`。
- 數量都是平台單位。OKX SWAP 一張是 0.01 BTC：`-qty 0.01` 送出 `sz 1`，`Fill` 與 `PositionUpdate` 也是 BTC，不是張數。
- 第 2 條：
  - Bybit 的 `/v5/private` 與 `/v5/trade` 是同一個位址，`cut -host stream-testnet.bybit.com` 兩條都會斷。用 `td conns` 看 fd，`-fd` 一次切一條：只切交易連線不進 `DEGRADED`、沒有 recon（D614）；切私有連線才是第 2 條。
  - OKX 下單撤單走 REST。`wspap.okx.com` 與 REST 的 keep-alive 可能是同一個位址，`cut` 會一起斷，斷線那一刻送出的撤單沒有回答。過了有效期（5 秒加 1 秒誤差）worker 查到單還在，發 `CANCEL_REJECTED` code 120，再撤一次即可（D610）。
- 第 4 條：
  - OKX：`td mode -venue Okx -creds /work/okx.creds.json -dual true`，`posMode` 是整個帳號的。
  - Bybit：v5 讀不到持倉模式，worker 只從 `positionIdx` 1、2 的掛單或持倉看出雙向（D617），所以切換之外還要有一張單。範圍是整個帳號：在別的商品上也算。

    ```sh
    $A td mode -venue Bybit -creds /work/bybit.creds.json -dual true -symbol ETHUSDT
    $A td mode -venue Bybit -creds /work/bybit.creds.json -symbol ETHUSDT -hedge-order 1700 -qty 0.01
    # kill worker → REJECTED hedge_mode
    $A td mode -venue Bybit -creds /work/bybit.creds.json -symbol ETHUSDT -cancel-all
    $A td mode -venue Bybit -creds /work/bybit.creds.json -symbol ETHUSDT -dual false
    ```

    只切換、沒有任何單或持倉時，worker 照樣起得來；之後不帶 `positionIdx` 的單由交易所拒絕。
