# 發布

`v*` tag 發布兩類產物：MD 的 conn worker 與 TD 的 account worker（下一節），以及 `mftiko` CLI（「CLI」一節）。`mftiko-api` 與 console 沒有發布的產物：部署時從同一個 tag 建 `mftiko-api` 與 `make web`，部署方式在 E12 定。API 的契約怎麼演進見最後一節。

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

`latest` 給人查找。E12 的 agent 會把它解析成 digest；這一期 agent 仍執行這個 repo 建出的 binary（E09-D014）。

### 第一個 tag 之前

新 package 預設是 private，而且 API 改不了 org package 的可見度。推第一個 `v*` tag 之前，由 org owner 做一次：

GitHub → BullionBear → Organization Settings → Packages → Package creation → Public

之後這個 org 新建的 package 才會是 public。只需設一次。workflow 不會改這個設定，也不會在推送之後改個別 package 的可見度。

### PR 怎麼驗證

一般 PR 不推 `ghcr.io`。`.github/workflows/ci.yml` 的 `check` job 起 `registry:2`，用同一支 [`.github/scripts/pack-workers.sh`](../.github/scripts/pack-workers.sh) 打包，推到 `127.0.0.1:5000`，再依 digest 與每個 tag 拉回。檢查單一 image manifest（不是 OCI index）、kind、`entrypoint` `["bin/worker"]`、contracts、空的 `sdk`、`linux/amd64`，以及層裡只有 `bin/worker`。

正式 registry 的推送只在 `v*` tag，或該 tag 的 `workflow_dispatch`。PR 上的 CI 跑的是同一支腳本對 `registry:2` 的這條路徑。

## CLI

依 [DECISIONS.md](DECISIONS.md) E11-D035、E11-D036（E11-08，#360）。安裝與使用見 [cli.md](cli.md)。

- `release.yml` 的 `cli` job 在同一個 `v*` tag 上執行 [`.github/scripts/build-cli.sh`](../.github/scripts/build-cli.sh)。它建出 `darwin/arm64`、`darwin/amd64`、`linux/amd64`、`linux/arm64` 四個平台，`CGO_ENABLED=0`、`-trimpath`，以 `-ldflags` 注入 `internal/platform/buildinfo`。
- 產物是 `mftiko_<tag>_<os>_<arch>.tar.gz`（裡面只有 `mftiko`）與 `SHA256SUMS`，上傳到該 tag 的 GitHub Release。Release 不存在時由 job 建立（`--verify-tag`，不建 git tag）。同一個 tag 的 `workflow_dispatch` 會以 `--clobber` 換掉 assets。權限只有這個 job 的 `contents: write`。
- 腳本會執行本機平台的那一個 binary，確認 `mftiko version` 印的就是注入的版本。
- PR 驗證：`ci.yml` 的 `make cli-dist` 跑同一支腳本，不上傳。
- binary 沒有 macOS 簽章與公證，也沒有 Homebrew tap。

## API 契約（`api.v1`）

依 E10-D002、E10-D018、E10-D116（E10-20，#325）。`contract/openapi/api/v1.yaml` 從 E10-20 起定案。

- **同一個 major 只做相容擴充**：加端點、加選填的請求欄位、加回應欄位。讀取端忽略不認得的欄位（PROTOCOL §1）。
- **CI 擋不相容的改動。** `make openapi-breaking` 以 PR 的 base 為基準跑 `oasdiff breaking --fail-on ERR`，刪端點、刪欄位、把選填改成必填、改型別都會失敗。WARN 只印出、不擋。
- **`info.version`**：目前 `1.0.1`。相容擴充加 minor（`1.1.0`），只改說明加 patch。和 git tag 無關，不跟著 `v*` tag 走。
- **不相容的改動是新的 major**：另開 `contract/openapi/api/v2.yaml` 與 `/api/v2` 路徑，和 v1 並存到 console、CLI 都換過去。v1 的檔案照舊受 `oasdiff` 保護。
- 改契約的 PR 一起提交 `make openapi` 的產物（`contract/gen/api/v1`、`web/src/lib/gen/api`），`make gen-check` 會檢查。
