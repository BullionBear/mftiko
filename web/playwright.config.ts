// Playwright smoke for the console (E10-19). It drives a running stack and starts no server: by default
// the acceptance stack's API replica a on 127.0.0.1:18080 (deploy/accept/README.md). Not part of make check.
import { defineConfig } from '@playwright/test';

export default defineConfig({
	testDir: 'e2e',
	timeout: 10 * 60_000,
	retries: 0,
	workers: 1,
	reporter: [['list']],
	outputDir: 'test-results',
	use: {
		baseURL: process.env.MFTIKO_E2E_URL ?? 'http://127.0.0.1:18080',
		browserName: 'chromium',
		actionTimeout: 30_000,
		navigationTimeout: 30_000,
		trace: 'retain-on-failure'
	}
});
