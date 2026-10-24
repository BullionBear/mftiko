# mftiko web console

SvelteKit single-page app, built to `build/` and served by `mftiko-api` (E10-D028).

- `npm install`, then `npm run dev`. Vite proxies `/api` to `localhost:8080` so the session cookie stays same-origin (E10-D078, E10-D009).
- Sign in at `/login` with a token. The token is not stored in localStorage; the API sets an HttpOnly cookie (E10-D076, E10-D009). A 401 from any other API sends the browser back to `/login` and, after a successful login, to the page it left.
- Live updates use one browser `EventSource` per page, subscribed only to the topics that page needs (E10-D107, E10-D075). The browser reconnects and sends `Last-Event-ID`. A `reset` event reloads the list.
- Styles are Svelte scoped CSS plus the global variables in `src/app.css` (E10-D074). Shared state is a Svelte 5 rune store: the signed-in user, unavailable regions, and `controller_absent` (E10-D077).
- `src/lib/gen/api/v1.ts` is generated from `contract/openapi/api/v1.yaml` by `make openapi`. `src/lib/api.ts` is the openapi-fetch client (E10-D047).
- From the repo root: `make web-install`, `make web`, `make web-check`. CI runs the check and the build (E10-D079). End-to-end coverage is #324.
