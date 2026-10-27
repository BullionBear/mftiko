# mftiko CLI

`mftiko` 是平台的命令列客戶端，只對 API（`contract/openapi/api/v1.yaml`）說話，不連 etcd、NATS 或 Postgres（DECISIONS E11-D001）。這份文件講安裝、設定與認證、session 檔，以及 exit code。每個命令的細節看 `mftiko help <命令>`。

## 安裝

三種方式，版本號的來源不同（E11-D035、E11-D036）。

### GitHub Release

每個 `v*` tag 的 [GitHub Release](https://github.com/BullionBear/mftiko/releases) 附上四個平台的 tarball 與 `SHA256SUMS`：

| 平台 | 檔名 |
|---|---|
| macOS Apple silicon | `mftiko_<tag>_darwin_arm64.tar.gz` |
| macOS Intel | `mftiko_<tag>_darwin_amd64.tar.gz` |
| Linux x86-64 | `mftiko_<tag>_linux_amd64.tar.gz` |
| Linux ARM64 | `mftiko_<tag>_linux_arm64.tar.gz` |

tarball 裡只有一個靜態 binary `mftiko`（`CGO_ENABLED=0`）。同一份 `SHA256SUMS` 也列出主機的 `mftiko-server_<tag>_linux_amd64.tar.gz`（見 [release.md](release.md)）。

```sh
TAG=v0.2.0 OS=darwin ARCH=arm64
base=https://github.com/BullionBear/mftiko/releases/download/$TAG
curl -fsSLO "$base/mftiko_${TAG}_${OS}_${ARCH}.tar.gz"
curl -fsSLO "$base/SHA256SUMS"
grep " mftiko_${TAG}_${OS}_${ARCH}.tar.gz\$" SHA256SUMS | shasum -a 256 -c -   # Linux 可用 sha256sum -c -
tar -xzf "mftiko_${TAG}_${OS}_${ARCH}.tar.gz"
install -m 0755 mftiko /usr/local/bin/mftiko
mftiko version
```

macOS 從瀏覽器下載的檔案會帶 quarantine 屬性；用 `curl` 下載的不會。binary 沒有簽章與公證，被 Gatekeeper 擋下時執行 `xattr -d com.apple.quarantine /usr/local/bin/mftiko`。

### go install

需要 Go（版本見 `go.mod`）。

```sh
go install github.com/BullionBear/mftiko/cmd/mftiko@v0.2.0
```

這條路徑沒有 Makefile 的 `-ldflags`。`mftiko version` 改印 Go 記錄的 module 版本（上例是 `v0.2.0`），build 時間是 `unknown`。

### 從原始碼

```sh
make build        # bin/mftiko，連同其他命令
make cli-dist     # bin/dist/：與 Release 相同的四個 tarball 與 SHA256SUMS
```

## 版本

```sh
$ mftiko version
mftiko v0.2.0 (commit 12c62e6, built 2026-10-24T11:22:25Z)
api.v1 2.0.0
```

第一行是 CLI 的版本：Makefile 或 release 注入的 `git describe`（符合 `vX.Y.Z` 的 tag）、短 commit 與 UTC build 時間。第二行是這個 CLI 編譯時所用 `api.v1` 的 `info.version`（E10-D116、E10-D118）。`2.0.0` 在原地拿掉策略 `id`。對著 `1.x` 編譯的 CLI 不能在 `2.0.0` 的 API 上建立 session 或列出策略。`2.0.0` 之後，同一個 major 只加相容欄位，直到下一次打破另開 v2。`-version` 印一樣的內容。`-o json` 印 `{"version","commit","built","api_version"}`。這個命令不讀設定檔，也不呼叫 API。每個請求的 `User-Agent` 是 `mftiko-cli/<版本>`。

## 設定與認證

設定檔是 `~/.config/mftiko/config.yaml`，權限不得寬於 `0600`，否則 CLI 拒絕讀取（E11-D003）。檔案裡只有具名的 context（E11-D004）：

```yaml
contexts:
  dev:
    url: http://127.0.0.1:8080/api/v1
    insecure_http: true
  prod:
    url: https://mftiko.example.com/api/v1
    ca_file: /etc/mftiko/ca.pem
```

- `url` 是 API 的位址，包含 `/api/v1`。
- TLS 預設會驗證憑證。設了 `ca_file` 時，那份 PEM 是唯一的信任根，主機名仍會檢查。明文 HTTP 要設 `insecure_http: true`（E11-D005）。
- 用 `--context <名>` 或 `MFTIKO_CONTEXT` 選 context。兩個都設時必須相同。都沒設就拒絕，CLI 不會替你挑預設的 context。

```sh
mkdir -p ~/.config/mftiko
$EDITOR ~/.config/mftiko/config.yaml
chmod 600 ~/.config/mftiko/config.yaml
export MFTIKO_CONTEXT=dev
```

token 由 API 的設定發放（README「API authentication」；`make dev-up` 的 token 在 `deploy/dev/data/api/token`）。token 有兩種給法，環境變數優先：

- `MFTIKO_TOKEN`：適合 CI 與腳本。
- `mftiko login`：從 stdin 讀一行 token，`GET /auth/me` 成功之後才寫進所選 context（E11-D006）。

```sh
mftiko login < deploy/dev/data/api/token
mftiko whoami
```

token 不能當旗標傳入，也不會出現在輸出與錯誤訊息裡。

## 第一個 session：hello

`examples/strategies/hello` 是參考策略：它在 book 的最佳買價下方 `offset` 處掛一張買單，看到單子 `ACCEPTED` 就撤掉（E08-D062）。

1. 打包並推到 `allow` 列出的 host，見 README「Packing a strategy」。repository 是完整的 `host/path`，必須落在 API 的 `allow`（E10-D119、E10-D122）。之後 `mftiko strategy list registry:5000/strategies/hello` 列出它的 tag 與完整 digest。
2. 查 instance 名稱與交易帳號：`mftiko instance list`（`sts`、`md` 平面）、`mftiko account list`（`api_id`）。帳號用 `mftiko account create` 建立，見 `mftiko help account`。永久退役一個 instance 用 `mftiko instance retire <name>`（E12-D033）；步驟在 [operations.md](operations.md)。
3. 寫 session 檔，例如 `hello.yaml`。欄位與 API 的 `SessionCreate` 一對一，用 snake_case（E11-D019）：

```yaml
name: hello-btc
sts_instance: sts1
strategy:
  repository: registry:5000/strategies/hello
  ref: v0.2.0            # tag 或 sha256: digest
params:                  # 策略自己的參數，平台不解讀
  tickers: [BinanceUM_Perp_BTCUSDT]
  api_id: 1
  algo_id: 1
  offset: 2000
  qty: 0.002
feeds:
  - feed: orderbook_delta.BinanceUM_Perp_BTCUSDT
    md_instance: dev
accounts:
  - name: main
    api_id: 1
    algo_ids: [1]
restart:
  mode: never
```

4. 建立 session，等它進入 running，然後看它、結束它：

```sh
mftiko session create -f hello.yaml --wait running   # 印出 session_id 與解析出的 digest
mftiko session show <session_id>                     # 狀態、需求項目的 ack、log 位置
mftiko session watch <session_id>                    # 即時重畫，Ctrl-C 離開
mftiko session end <session_id> --wait               # 確認之後結束，等到 terminal
mftiko record list
```

session 檔也可以是 JSON，`-f -` 從 stdin 讀。YAML 與 JSON 用同一個解析器：

- 數字照原文送出。
- 重複的 key 會報錯。
- `0755`、`0x1F`、`.inf` 這類不是合法 JSON 的數字會報錯，並指出行號，要加上引號。
- `true`、`false` 是布林；`yes`、`no`、`on`、`off` 是字串。

每次執行送一個新的 `Idempotency-Key`。連線失敗或逾時，最多用同一個 key 重試 3 次（E11-D020、E11-D021）。

## exit code

錯誤訊息同一行印名稱與號碼，例如 `mftiko: not_found (12): ...`，接著是 `detail`、每條 `errors[].path`，以及回應的 `X-Request-Id`（E11-D007、E11-D008）。exit code 依 problem body 的 `code` 決定，不看 HTTP status。

| code | 名稱 | 意思 |
|---|---|---|
| 0 | | 成功 |
| 1 | 用法或設定 | 旗標錯、設定檔有問題、沒有 token、非 TTY 沒給 `--yes` |
| 2 | 網路 | 連不上、TLS 失敗、逾時 |
| 10 | `validation_failed` | 請求內容不合法；`errors[].path` 指出欄位，session 檔會對回行號 |
| 11 | `unauthenticated` | token 無效或沒帶 |
| 12 | `not_found` | 資源不存在 |
| 13 | `conflict` | 狀態衝突；同一個 `Idempotency-Key` 配上不同內容時印「此 key 已用於不同內容」 |
| 14 | `not_terminal` | 刪除尚未結束的 session；先執行 `session end` |
| 15 | `referenced` | 帳號仍被尚未結束的 session 引用 |
| 16 | `algo_overlap` | `algo_ids` 與同帳號上未結束的 session 重疊 |
| 17 | `region_unavailable` | 該 region 的 etcd 暫時讀不到 |
| 18 | `storage_unavailable` | Postgres 不可用 |
| 19 | `registry_unavailable` | 連不上策略 registry |
| 20 | `internal` | API 內部錯誤；回報時附上 `X-Request-Id` |
| 21 | `invalid_artifact` | 列出 tag 時解析結果讀不出來（E10-D059）。建立時 kind 不對是 `validation_failed`（E10-D120） |

下面這組只出現在 `session create --wait` 與 `session end --wait`（E11-D033）。到達目標 phase、`outcome=done` 或 `end_reason=ended_by_spec` 都回 0：

| code | 名稱 |
|---|---|
| 30 | `failed` |
| 31 | `init_failed` |
| 32 | `crashed` |
| 33 | `hung` |
| 34 | `unknown` |
| 35 | `deps_failed` |
| 36 | `deps_timeout` |
| 37 | `invalid_spec` |
| 38 | 等待逾時（`--wait-timeout`，預設 10 分鐘） |
| 39 | 等待中 session 被刪除 |

## 常見錯誤

| 訊息 | 原因與處理 |
|---|---|
| `... is mode 0644; refuse a config wider than 0600` | `chmod 600 ~/.config/mftiko/config.yaml` |
| `set --context or MFTIKO_CONTEXT` | 沒有選 context；CLI 不挑預設值 |
| `--context and MFTIKO_CONTEXT differ` | 兩者都設但不同；只留一個 |
| `unknown context "x"` | 設定檔裡沒有這個名字 |
| `no token; set MFTIKO_TOKEN or run mftiko login` | 這個 context 還沒有 token |
| `context url is http; set insecure_http: true to allow plaintext` | 明文 HTTP 只給 dev；正式環境用 HTTPS |
| `network (2): ...` | 確認 `url`（含 `/api/v1`）、API 是否在跑、`ca_file` 是否正確；需要時調大 `--timeout` |
| `unauthenticated (11)` | `MFTIKO_TOKEN` 優先於 context 裡的 token，檢查是否設了舊值 |
| `not a terminal; pass --yes` | 腳本裡的刪除或結束要加 `--yes`；在 TTY 上 `--yes` 不會跳過確認 |
| `registry_unavailable (19)` | API 連不上 registry，與 CLI 端無關 |
| 有 `unavailable_regions` 的警告，exit 0 | 部分 region 暫時讀不到，列表可能不完整 |

回報問題時附上 `mftiko version` 的輸出與錯誤裡的 `X-Request-Id`（`-v` 會在成功時也印出）。
