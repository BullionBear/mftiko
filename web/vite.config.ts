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
		// `npm run dev` talks to a local mftiko-api.
		proxy: { '/api': 'http://localhost:8080' }
	}
});
