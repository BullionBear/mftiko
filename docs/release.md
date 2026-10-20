# 發布 conn 與 account worker

依 [DECISIONS.md](DECISIONS.md) D985–D990（E9-10，#288）與 [PROTOCOL.md](PROTOCOL.md) §8。策略 artifact 不走這條 workflow。

產物是 OCI artifact：單層靜態 `bin/worker`，沒有 base OS，不是可 `docker run` 的容器鏡像。

## 什麼時候推

- 推上 `v*` tag（例如 `v0.1.0`）會跑 [`.github/workflows/release.yml`](../.github/workflows/release.yml)。
- 在同一個 tag 上用 `workflow_dispatch` 可以再推一次。從 branch 手動跑會失敗：image 的 tag 就是這個 git tag。workflow 不建立、不推動 git tag。

## 推去哪裡

- registry：`ghcr.io/bullionbear/`
- repository：`mftiko-md-conn`（kind `md.conn`）、`mftiko-td-account`（kind `td.account`）
- tag：該 git tag，以及 `latest`
- 平台：`linux/amd64`。manifest 的 `-name` 與 repository 同名。
- 權限：`GITHUB_TOKEN` 的 `packages: write`。workflow 只推送，不呼叫任何可見度 API。

`latest` 給人查找。E12 的 agent 會把它解析成 digest；這一期 agent 仍執行這個 repo 建出的 binary（D914）。

## 第一個 tag 之前

新 package 預設是 private，而且 API 改不了 org package 的可見度。推第一個 `v*` tag 之前，由 org owner 做一次：

GitHub → BullionBear → Organization Settings → Packages → Package creation → Public

之後這個 org 新建的 package 才會是 public。只需設一次。workflow 不會改這個設定，也不會在推送之後改個別 package 的可見度。

## PR 怎麼驗證

一般 PR 不推 `ghcr.io`。`.github/workflows/ci.yml` 的 `check` job 起 `registry:2`，用同一支 [`.github/scripts/pack-workers.sh`](../.github/scripts/pack-workers.sh) 打包，推到 `127.0.0.1:5000`，再依 digest 與每個 tag 拉回。檢查單一 image manifest（不是 OCI index）、kind、`entrypoint` `["bin/worker"]`、contracts、空的 `sdk`、`linux/amd64`，以及層裡只有 `bin/worker`。

正式 registry 的推送只在 `v*` tag，或該 tag 的 `workflow_dispatch`。PR 上的 CI 跑的是同一支腳本對 `registry:2` 的這條路徑。
