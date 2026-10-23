# mftiko web console

SvelteKit single-page app, built to `build/` and served by `mftiko-api` (D1028).

- `npm install`, then `npm run dev`. Vite proxies `/api` to `localhost:8080` so the session cookie stays same-origin (D1078, D1009).
- Sign in at `/login` with a token. The token is not stored in localStorage; the API sets an HttpOnly cookie (D1076, D1009). A 401 from any other API sends the browser back to `/login` and, after a successful login, to the page it left.
- Live updates use one browser `EventSource` on `/api/v1/events` (D1075). The browser reconnects and sends `Last-Event-ID`. A `reset` event reloads the list.
- Styles are Svelte scoped CSS plus the global variables in `src/app.css` (D1074). Shared state is a Svelte 5 rune store: the signed-in user, unavailable regions, and `controller_absent` (D1077).
- `src/lib/gen/api/v1.ts` is generated from `contract/openapi/api/v1.yaml` by `make openapi`. `src/lib/api.ts` is the openapi-fetch client (D1047).
- From the repo root: `make web-install`, `make web`, `make web-check`. CI runs the check and the build (D1079). End-to-end coverage is #324.
