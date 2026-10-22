# mftiko web console

SvelteKit single-page app, built to `build/` and served by `mftiko-api`.

- `npm install`, then `npm run dev` (proxies `/api` to a local `mftiko-api` on :8080).
- `src/lib/gen/api/v1.ts` is generated from `contract/openapi/api/v1.yaml` by `make openapi`. `src/lib/api.ts` is the openapi-fetch client.
- Usually driven from the repo root: `make web-install`, `make web`.
