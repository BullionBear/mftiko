import adapter from '@sveltejs/adapter-static';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
	plugins: [
		sveltekit({
			compilerOptions: {
				// Force runes mode for the project, except for libraries. Can be removed in svelte 6.
				runes: ({ filename }) =>
					filename.split(/[/\\]/).includes('node_modules') ? undefined : true
			},
			// Single-page app: build/ is served as static files by mftiko-api, every unknown path falls back to index.html.
			adapter: adapter({ fallback: 'index.html' })
		})
	],
	server: {
		// Dev proxy (D1078). The browser only talks to this origin, so the session
		// cookie stays same-origin, matching production (D1009).
		proxy: { '/api': 'http://localhost:8080' }
	}
});
