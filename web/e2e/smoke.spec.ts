// The main flow of #12 through the console only (E10-19): sign in, create an account, deploy hello on it,
// see the account and the feed ready, the session running and its log location, follow its stdout and
// download its logs (E14-08), end it, see it done and the log stream ended, delete the account and sign
// out. Needs the acceptance stack (deploy/accept/accept.sh e2e-up).
//
//   MFTIKO_E2E_URL     the console, default http://127.0.0.1:18080
//   MFTIKO_E2E_TOKEN   file with the API token, default ../deploy/accept/data/work/e2e/token
//   MFTIKO_E2E_CREDS   account credentials JSON, default ../deploy/accept/data/work/binanceum.creds.json
//   MFTIKO_E2E_KIND, MFTIKO_E2E_TICKER   default BinanceUM, BinanceUM_Perp_BTCUSDT
import { readFileSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { expect, test, type Page } from '@playwright/test';

const work = '../deploy/accept/data/work';
const token = readFileSync(process.env.MFTIKO_E2E_TOKEN ?? `${work}/e2e/token`, 'utf8').trim();
const creds = JSON.parse(readFileSync(process.env.MFTIKO_E2E_CREDS ?? `${work}/binanceum.creds.json`, 'utf8')) as {
	key_type: string;
	key: string;
	secret: string;
	passphrase?: string;
};
const kind = process.env.MFTIKO_E2E_KIND ?? 'BinanceUM';
const ticker = process.env.MFTIKO_E2E_TICKER ?? 'BinanceUM_Perp_BTCUSDT';
const feed = `orderbook_delta.${ticker}`;

/** The <dd> after the <dt> named term in the session's facts. */
function fact(page: Page, term: string) {
	return page.locator('dl.facts > dt', { hasText: new RegExp(`^${term}$`) }).locator('xpath=following-sibling::dd[1]');
}

test('console main flow', async ({ page }) => {
	const name = `smoke-${Date.now()}`;

	await page.goto('/login');
	await page.getByLabel('Token').fill(token);
	await page.getByRole('button', { name: 'Sign in' }).click();
	await expect(page).not.toHaveURL(/\/login/);

	await page.goto('/accounts/new');
	await page.getByLabel('Name', { exact: true }).fill(name);
	await page.getByLabel('Kind').selectOption(kind);
	await page.getByLabel('TD instance').fill('td1');
	await page.getByLabel('Key type').selectOption(creds.key_type);
	await page.getByLabel('Key', { exact: true }).fill(creds.key);
	await page.locator('#secret').fill(creds.secret);
	if (creds.passphrase) await page.getByLabel('Passphrase').fill(creds.passphrase);
	await page.getByRole('button', { name: 'Create' }).click();
	await page.waitForURL(/\/accounts\/\d+$/);
	const apiId = Number(new URL(page.url()).pathname.split('/').pop());

	await page.goto('/sessions/new');
	await page.getByLabel('Name', { exact: true }).fill(name);
	await page.getByLabel('STS instance').selectOption('sts1');
	await page.getByLabel('Repository').fill('registry:5000/strategies/hello');
	await page.getByRole('button', { name: 'Load tags' }).click();
	await page.getByLabel('Version').selectOption('accept');
	await page.getByRole('button', { name: 'Next' }).click();
	await page
		.getByLabel('Params')
		.fill(JSON.stringify({ tickers: [ticker], api_id: apiId, algo_id: 2, offset: 2000, qty: 0.002 }));
	await page.getByRole('button', { name: 'Next' }).click();
	await page.getByRole('button', { name: 'Add feed' }).click();
	await page.getByLabel('Feed', { exact: true }).fill(feed);
	await page.getByLabel('MD instance').fill('dev');
	await page.getByRole('button', { name: 'Next' }).click();
	await page.getByRole('button', { name: 'Add account' }).click();
	await page.getByLabel('Account', { exact: true }).selectOption(String(apiId));
	await page.getByLabel('Add algo_id').fill('2');
	await page.getByRole('button', { name: 'Add', exact: true }).click();
	await page.getByRole('button', { name: 'Next' }).click();
	await page.getByRole('button', { name: 'Next' }).click();
	await page.getByRole('button', { name: 'Deploy' }).click();
	await page.waitForURL(/\/sessions\/[0-9a-f]{6}$/);

	await expect(fact(page, 'Phase')).toHaveText('running', { timeout: 3 * 60_000 });
	const deps = page.locator('table.data tbody tr');
	await expect(deps).toHaveCount(2);
	await expect(deps.filter({ hasText: feed })).toContainText('ready');
	await expect(deps.filter({ hasText: String(apiId) })).toContainText('ready');
	await expect(fact(page, 'Host')).not.toBeEmpty();
	await expect(fact(page, 'stdout')).toContainText(/\/log\/sts\/[0-9a-f]+\.stdout/);
	await expect(fact(page, 'stderr')).toContainText(/\/log\/sts\/[0-9a-f]+\.stderr/);

	// Logs (E14-07, E15-D022): hello's recon is on stderr. Follow that tab, then switch back.
	const panel = page.locator('#log-panel');
	await page.getByLabel('Follow').check();
	await page.getByRole('tab', { name: 'stderr' }).click();
	await expect(panel).toHaveAttribute('data-stream', 'stderr');
	await expect(panel).toContainText('"hook":"OnRecon"', { timeout: 2 * 60_000 });
	await page.getByRole('tab', { name: 'stdout' }).click();
	await expect(panel).toHaveAttribute('data-stream', 'stdout');
	const downloaded = page.waitForEvent('download');
	await page.getByRole('link', { name: 'Download' }).click();
	const tgz = await readFile(await (await downloaded).path());
	expect([...tgz.subarray(0, 2)]).toEqual([0x1f, 0x8b]); // gzip

	await page.getByRole('button', { name: 'End session' }).click();
	await page.getByRole('group', { name: 'Confirm end' }).getByRole('button', { name: 'End' }).click();
	await expect(fact(page, 'Phase')).toHaveText('done', { timeout: 90_000 });
	await expect(fact(page, 'Status reason')).toHaveText('ended_by_spec');
	await expect(page.getByRole('status').filter({ hasText: '已結束' })).toBeVisible({ timeout: 60_000 });

	await page.goto(`/accounts/${apiId}`);
	await page.getByRole('button', { name: 'Delete' }).click();
	await page.getByRole('group', { name: 'Confirm delete' }).getByRole('button', { name: 'Delete' }).click();
	await page.waitForURL(/\/accounts$/);

	await page.getByRole('button', { name: 'Log out' }).click();
	await expect(page).toHaveURL(/\/login/);
});
