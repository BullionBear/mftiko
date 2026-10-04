# mftiko web console

SvelteKit single-page app, built to `build/` and served by `mftiko-api`.

- `npm install`, then `npm run dev` (proxies `/api` to a local `mftiko-api` on :8080).
- `src/lib/gen/` is generated from `../proto` by `make proto`.
- Usually driven from the repo root: `make web-install`, `make web`.
