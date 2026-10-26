# 發布

`v*` tag 發布三類產物：MD 的 conn worker 與 TD 的 account worker（下一節），`mftiko` CLI（「CLI」一節），以及主機上的 binary 與 console（「主機」一節）。API 的契約怎麼演進見最後一節。

## conn 與 account worker

依 [DECISIONS.md](DECISIONS.md) E09-D085–E09-D090（E9-10，#288）與 [PROTOCOL.md](PROTOCOL.md) §8。策略 artifact 不走這條 workflow。

產物是 OCI artifact：單層靜態 `bin/worker`，沒有 base OS，不是可 `docker run` 的容器鏡像。

### 什麼時候推

- 推上 `v*` tag（例如 `v0.1.0`）會跑 [`.github/workflows/release.yml`](../.github/workflows/release.yml)。
- 在同一個 tag 上用 `workflow_dispatch` 可以再推一次。從 branch 手動跑會失敗：image 的 tag 就是這個 git tag。workflow 不建立、不推動 git tag。

### 推去哪裡

- registry：`ghcr.io/bullionbear/`
- repository：`mftiko-md-conn`（kind `md.conn`）、`mftiko-td-account`（kind `td.account`）
- tag：該 git tag，以及 `latest`
- 平台：`linux/amd64`。manifest 的 `-name` 與 repository 同名。
- 權限：`GITHUB_TOKEN` 的 `packages: write`。workflow 只推送，不呼叫任何可見度 API。

md-agent、td-agent 以 `-registry ghcr.io` 每分鐘把 `latest` 解析成 digest，之後 spawn 的 worker 用新版，跑著的不換（E12-D008、E12-D010）。所以推了 `v*` tag，新的 conn 與 account worker 不必重啟 agent 就會生效。

### 第一個 tag 之前

新 package 預設是 private，而且 API 改不了 org package 的可見度。推第一個 `v*` tag 之前，由 org owner 做一次：

GitHub → BullionBear → Organization Settings → Packages → Package creation → Public

之後這個 org 新建的 package 才會是 public。只需設一次。workflow 不會改這個設定，也不會在推送之後改個別 package 的可見度。

### PR 怎麼驗證

一般 PR 不推 `ghcr.io`。`.github/workflows/ci.yml` 的 `build` job 起 `registry:2`，用同一支 [`.github/scripts/pack-workers.sh`](../.github/scripts/pack-workers.sh) 打包，推到 `127.0.0.1:5000`，再依 digest 與每個 tag 拉回。檢查單一 image manifest（不是 OCI index）、kind、`entrypoint` `["bin/worker"]`、contracts、空的 `sdk`、`linux/amd64`，以及層裡只有 `bin/worker`。

正式 registry 的推送只在 `v*` tag，或該 tag 的 `workflow_dispatch`。PR 上的 CI 跑的是同一支腳本對 `registry:2` 的這條路徑。

## CLI

依 [DECISIONS.md](DECISIONS.md) E11-D035、E11-D036（E11-08，#360）。安裝與使用見 [cli.md](cli.md)。

- `release.yml` 的 `cli` job 在同一個 `v*` tag 上先執行 [`.github/scripts/build-cli.sh`](../.github/scripts/build-cli.sh)，再建主機 tarball（「主機」）。CLI 建出 `darwin/arm64`、`darwin/amd64`、`linux/amd64`、`linux/arm64` 四個平台，`CGO_ENABLED=0`、`-trimpath`，以 `-ldflags` 注入 `internal/platform/buildinfo`。
- CLI 產物是 `mftiko_<tag>_<os>_<arch>.tar.gz`（裡面只有 `mftiko`）。主機產物是 `mftiko-server_<tag>_linux_amd64.tar.gz`。兩者上傳到同一個 GitHub Release，外加一份 `SHA256SUMS`：CLI tarball 建完會先寫一份，主機 tarball 寫入後整份重算，所以這份檔涵蓋兩種 tarball。Release 不存在時由 job 建立（`--verify-tag`，不建 git tag）。同一個 tag 的 `workflow_dispatch` 會以 `--clobber` 換掉 assets。權限只有這個 job 的 `contents: write`。
- 腳本會執行本機平台的那一個 CLI binary，確認 `mftiko version` 印的就是注入的版本。
- PR 驗證：`ci.yml` 的 `make cli-dist` 與 `make server-dist` 跑這兩支腳本，不上傳。`server-dist` 在 `make web` 之後。
- binary 沒有 macOS 簽章與公證，也沒有 Homebrew tap。

## 主機

依 [DECISIONS.md](DECISIONS.md) E12-D002、E12-D003、E12-D022–E12-D026（E12-04，#377）。unit 檔的內容是 E12-05（#378）；這裡只打包 `deploy/systemd/`，並在換版時把它安裝到 `/etc/systemd/system/`。

### 打包

- 腳本 [`.github/scripts/build-server.sh`](../.github/scripts/build-server.sh)。`make server-dist` 跑它，預設寫到 `bin/dist/`。`release.yml` 的 `cli` job 先建 CLI tarball，再 `make web`、跑同一支腳本，然後把兩種 tarball 與一份 `SHA256SUMS` 一起上傳。`SHA256SUMS` 在兩者都在輸出目錄之後重算，涵蓋全部 `*.tar.gz`。
- 產物是 `mftiko-server_<tag>_linux_amd64.tar.gz`。`CGO_ENABLED=0`、`-trimpath`，`-ldflags` 注入 `internal/platform/buildinfo`。部署機不需要 Go 與 Node。
- **內容：**
  - `bin/`：`mftiko-md-agent`、`mftiko-td-agent`、`mftiko-sts-agent`、`mftiko-shim`、`mftiko-controller`、`mftiko-api`、`mftiko-sym`、`mftiko-mdfetch`、`mftiko-migrate`、`mftiko-etcd-roles`、`mftiko-etcd-backup`、`mftiko-retire-clean`、`mftiko-seal`、`mftiko-release`。
  - `web/`：`make web` 的結果（`web/build`），給 `mftiko-api -web`。
  - `systemd/`：`deploy/systemd/` 裡的 unit，不含點開頭的檔。目錄可以是空的，#378 才放檔。
- 不放：conn 與 account worker（agent 從 registry 拉，E12-02）、CLI、`*-accept`、`mftiko-conformance`、`mftiko-nats-dev`、`mftiko-pack`。
- PR 的 CI 在 `make web` 之後跑 `make server-dist`：檢查上面的清單，並在 linux/amd64 上確認每個 Go binary 的 `-version` 以注入的版本開頭（`Version (commit …)`）。

### 目錄

解開到 `/opt/mftiko/releases/<tag>/`（`bin/`、`web/`、`systemd/`），內容唯讀。`/opt/mftiko/current` 是相對 symlink `releases/<tag>`。unit 執行 `/opt/mftiko/current/bin/…`。換 `current` 是先建暫時 symlink 再 rename。不覆寫舊檔；跑著的 shim 與 worker 繼續用舊 inode（E01-D003）。

### `mftiko-release`

tarball 裡的 `bin/mftiko-release`，在部署機以 root 執行。

| 指令 | 做什麼 |
|---|---|
| `fetch <tag>` | 從 `https://github.com/BullionBear/mftiko/releases/download/<tag>/` 下載 server tarball 與 `SHA256SUMS`，核對檔名完全相符的那一行 |
| `install` | 解開剛剛 fetch 的那一版；目錄已存在就拒絕 |
| `switch <tag>` | 見下方順序 |
| `rollback` | 把 `current` 指回上一版並重啟，不跑 migration、不 gc |
| `gc` | 刪掉保留集合以外的版本 |
| `list` | 已安裝的版本，新的在前，標 `current` 與 `previous` |

`fetch` 的 tag 只允許字母、數字、`.`、`_`、`-`，且不得含 `..`。tarball 的路徑不得是絕對路徑或含 `..`；解開後若 symlink 的 realpath 落在目錄外，或 realpath 解不開，這次 install 失敗，不留下目錄。

**`switch` 的順序：**

1. 執行 `releases/<tag>/bin/mftiko-migrate up`。它讀繼承的 `DATABASE_URL`（與 `-database` 的預設相同），腳本不另傳 `-database`。失敗則 `current`、unit、行程都不動。
2. 把該版 `systemd/` 的 unit 複製到 `/etc/systemd/system/`（`*.service`、`*.socket`、`*.timer`、`*.target`、`*.path`、`*.mount`、`*.slice` 與 `*.service.d`、`*.socket.d`、`*.timer.d`）。同名覆寫；這版沒有的 unit 不刪。然後 `daemon-reload`。
3. 原子換掉 `current`。`previous` 記的是換之前的 tag。
4. 只重啟本機 `systemctl is-enabled` 成功的 unit。順序是 `mftiko-sym`、`mftiko-mdfetch`、`mftiko-api`、`mftiko-controller`、`mftiko-md-agent`、`mftiko-td-agent`、`mftiko-sts-agent`。對哪個檔由 `ExecStart`、`ExecStartPre`、`ExecStartPost` 是否含該 binary 決定（註解不算），不靠檔名，所以 #378 可以自己取檔名。etcd、NATS、Postgres 的 unit 不執行這些 binary，不會被重啟。
5. 上面都成功才 `gc`。重啟失敗不 gc；`current` 已經指到新版。

兩台主機都跑 migrate。`mftiko-migrate up` 依編號套用嵌入的 `migrations/*.up.sql`，已紀錄且 sha256 相符的跳過，所以可以重複執行。每個檔案與 `schema_migrations` 的一列在同一個交易；先拿 `pg_advisory_lock`，兩台一次只跑一個。已套用檔案的 sha256 或 name 與紀錄不同，或紀錄沒有對應檔案，就以非 0 結束，`switch` 停在第 1 步，`current` 不動。沒有 `down`。migration 只做向後相容的新增，不支援不能放進交易的 SQL。沒有本機 Postgres 的主機把 `DATABASE_URL` 指到 home 的資料庫。

**`rollback`** 安裝上一版的 unit、`daemon-reload`、依同一順序重啟。不跑 `mftiko-migrate`，不降 schema，不 gc。每次 `current` 改動都更新 `previous`，所以再 rollback 一次會回到剛離開的版本。上一版的目錄已經不在就拒絕，`current` 不動。

**`gc`** 在 `switch` 成功後自動跑，也可以單獨跑。保留 `/opt/mftiko/installed` 裡還在的最近 3 版（成功的 install 追加一行，最舊在前；同一個 tag 以最後一次為準），再加上 `current`。任一 `/proc/<pid>/exe`（去掉結尾的 ` (deleted)`）落在某版目錄裡，那一版不刪。`/proc` 讀不到則什麼都不刪並失敗。不另外為 `previous` 保留：它若在這兩個條件之外會被刪，之後 rollback 會拒絕。往前換一版時，上一版是次新的，留在最近 3 版裡。刪除前目錄的 realpath 必須正好是 `/opt/mftiko/releases/<tag>`。

真實的 A→B 換版，以及 agent 收養還在跑的 shim，在 #385 的 testnet 演練，不在這張的 CI。這張的腳本測試用假版本 A、B（gc 用到 E），tarball 裡是真正的 `mftiko-migrate`，`DATABASE_URL` 是測試用的 Postgres（使用者 `mftiko`、trust、沒有密碼）。

驗收測試把 `MFTIKO_PREFIX`、`MFTIKO_SYSTEMD_DIR`、`MFTIKO_SYSTEMCTL`、`MFTIKO_PROC`、`MFTIKO_RELEASE_BASE` 指到暫存目錄。部署不設這些變數：預設就是 `/opt/mftiko`、`/etc/systemd/system`、`systemctl`、`/proc` 與上面的 GitHub Release URL。

## API 契約（`api.v1`）

依 E10-D002、E10-D018、E10-D116（E10-20，#325）。`contract/openapi/api/v1.yaml` 從 E10-20 起定案。

- **同一個 major 只做相容擴充**：加端點、加選填的請求欄位、加回應欄位。讀取端忽略不認得的欄位（PROTOCOL §1）。
- **CI 擋不相容的改動。** `make openapi-breaking` 以 PR 的 base 為基準跑 `oasdiff breaking --fail-on ERR`，刪端點、刪欄位、把選填改成必填、改型別都會失敗。WARN 只印出、不擋。
- **`info.version`**：目前 `1.0.1`。相容擴充加 minor（`1.1.0`），只改說明加 patch。和 git tag 無關，不跟著 `v*` tag 走。
- **不相容的改動是新的 major**：另開 `contract/openapi/api/v2.yaml` 與 `/api/v2` 路徑，和 v1 並存到 console、CLI 都換過去。v1 的檔案照舊受 `oasdiff` 保護。
- 改契約的 PR 一起提交 `make openapi` 的產物（`contract/gen/api/v1`、`web/src/lib/gen/api`），`make gen-check` 會檢查。
