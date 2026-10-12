# MD feed 路徑驗收

E4 的驗收（#6；E4-09 BinanceUM、E4-10 Binance 現貨、E4-11 Bybit／OKX）。需求項目手寫（E13 之前），對交易所的 testnet 跑四條：

1. `md.f` 收到事件，同一個 feed 上 `seq` 連續。
2. 晚加入：先訂 `md.f`，再向 `md.view` 拿當時的 book 或 tape，之後只套用較新的 seq。
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

換交易所時設定 `ACCEPT_VENUE`、`ACCEPT_SYM_FEEDS`（conn worker 會載入該 venue 的每一個 category，所以要全部列出）、`ACCEPT_TICKER`，並在 `endpoints/ws.json`、`endpoints/rest.json` 加上那個 venue 的 testnet 位址。目前列了驗證過的 BinanceUM（E4-09）與 Binance 現貨（E4-10）。`rest.json` 給 SnapshotREST 的快照用（`md-agent -rest-endpoints`）：testnet 的串流必須搭配 testnet 的快照，正式站的 update id 對不上。

Binance 現貨：

```sh
ACCEPT_VENUE=Binance ACCEPT_SYM_FEEDS=Binance/Spot ACCEPT_TICKER=Binance_Spot_BTCUSDT make accept-up
```

現貨只有一條 route（`default`），`aggtrade` 與 `orderbook_delta` 會放在同一個 conn（`md/Binance/default/0`）上，所以第 3 條切一次就會讓兩個 feed 一起進到下一個 epoch。tick 是 0.01，2048 tick 的窗大約只有最佳價上下 20 USD。

以下以 BinanceUM 為例。`A=deploy/accept/accept.sh`。BinanceUM 沒有 `trade` topic，trade 類用 `aggtrade`。

## 1. 收到事件，seq 連續

```sh
$A bg watch watch -out /work/watch.jsonl     # 訂 md.f.dev.>，每則寫進 watch.jsonl
$A agent                                     # log: data/work/agent-1.log
$A demand put s1 aggtrade.BinanceUM_Perp_BTCUSDT
$A demand put s1 orderbook_delta.BinanceUM_Perp_BTCUSDT
$A acks                                      # 兩筆 ENTRY_STATE_READY，各自的 subject 與 worker
tail data/work/watch.log
```

通過的條件：兩個 subject 都出現 `first epoch=1 seq=1`；`orderbook_delta` 的第一則是 `reset=true`（快照）；`SUMMARY` 一直是 `gaps=0 dups=0`；book 的 `resets` 只有 1（沒有因為斷號而重抓）。

## 2. 晚加入

```sh
$A late -feed orderbook_delta.BinanceUM_Perp_BTCUSDT -follow 15s
$A late -feed aggtrade.BinanceUM_Perp_BTCUSDT -follow 10s
```

`late` 會先訂 `md.f` 並 flush，再問 `md.view`：

- book：從 `BookSnapshot` 的 `(epoch, S)` 開始，丟掉 ≤ S 的 live，第一則套用的必須是 S+1。用 `sdk/book` 折（tick 從 SYM 取），跟完一段之後再問一次 `md.view`，在同一個 cursor 上逐檔比對。
- tape：往回分頁讀到 `-since`，印出 gap，從 `TapePage.head` 接上 live（D437）。

通過的條件：`gaps=0`；book 的比對結果是 `differing within 1024 ticks of the best: 0`。離最佳價較遠的檔（窗的外半）若有差異，只印出、不算失敗。這是 D431 的結果：一檔在晚加入者的快照當下落在窗外，或曾經在窗外被丟掉，在它下一筆更新之前晚加入者都不會知道它；worker 的環則還留著那一格。

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

紀錄：`data/work/watch.log`、`watch.jsonl`（每一則 md.f 的 `(epoch, seq)`）、`agent-*.log`、`sym.log`，以及上面各個指令的輸出。新的一輪要乾淨的紀錄時，先清空 `data/work/`。E4-09 的紀錄在 #193，E4-10 的在 #195。
