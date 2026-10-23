<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import CredentialsFields from '#lib/CredentialsFields.svelte';
	import FeedStatus from '#lib/FeedStatus.svelte';
	import {
		applyAccountProblem,
		credentialsFrom,
		fieldDomId,
		isKeyType,
		parseApiId,
		type CredentialsBody,
		type SecretGuard
	} from '#lib/account-form.ts';
	import { api, failureOf, isProblem, type KeyType } from '#lib/api.ts';
	import { AccountDetailFeed, AccountKindFeed } from '#lib/live.svelte.ts';
	import { onMount, untrack } from 'svelte';

	const detail = new AccountDetailFeed();
	const kindsFeed = new AccountKindFeed();
	const guard: SecretGuard = { epoch: 0 };

	const apiId = $derived(parseApiId(page.params.api_id ?? ''));

	let name = $state('');
	let replace = $state(false);
	let keyType = $state<KeyType | ''>('');
	let key = $state('');
	let secret = $state('');
	let passphrase = $state('');
	let clearGen = $state(0);
	let saving = $state(false);
	let deleting = $state(false);
	let saved = $state(false);
	let confirmDelete = $state(false);
	let referencedIds = $state<string[] | null>(null);
	let serverFields = $state<Record<string, string>>({});
	let banner = $state<string[]>([]);

	// Plain lets: these gate effects and must not schedule another run when they change.
	let seenId: number | null | undefined = undefined;
	let nameFor: number | null = null;
	let keyTypeSetFor: number | null = null;

	const selectedKind = $derived(
		detail.account
			? (kindsFeed.items ?? []).find((item) => item.kind === detail.account?.kind)
			: undefined
	);

	function message(path: string): string | undefined {
		return serverFields[path];
	}

	onMount(() => {
		void kindsFeed.reload();
		return () => kindsFeed.cancel();
	});

	// Reload only when the id changes. The feed writes its own fields during load; tracking
	// those would schedule this effect again and refetch forever.
	$effect(() => {
		const id = apiId;
		if (id === undefined) return;
		untrack(() => {
			void detail.load(id);
		});
		return () => detail.cancel();
	});

	// A different account must not keep the previous form, including any secret (D1101).
	$effect(() => {
		const id = apiId ?? null;
		if (id === seenId) return;
		seenId = id;
		nameFor = null;
		keyTypeSetFor = null;
		name = '';
		replace = false;
		key = '';
		secret = '';
		passphrase = '';
		keyType = '';
		guard.epoch += 1;
		clearGen += 1;
		confirmDelete = false;
		referencedIds = null;
		banner = [];
		serverFields = {};
		saved = false;
	});

	$effect(() => {
		const acct = detail.account;
		const id = apiId;
		if (!acct || id === undefined || acct.api_id !== id) return;
		if (nameFor === id) return;
		nameFor = id;
		name = acct.name;
	});

	$effect(() => {
		const acct = detail.account;
		const catalog = kindsFeed.items;
		const id = apiId;
		if (!acct || !catalog || id === undefined || acct.api_id !== id) return;
		if (keyTypeSetFor === id) return;
		const kind = catalog.find((item) => item.kind === acct.kind);
		keyType = kind?.key_types[0] ?? '';
		keyTypeSetFor = id;
	});

	function dropSecretControl(): void {
		secret = '';
		guard.epoch += 1;
		clearGen += 1;
	}

	function applyKeyType(next: KeyType | ''): void {
		if ((keyType === 'ed25519') !== (next === 'ed25519')) dropSecretControl();
		keyType = next;
	}

	function onKeyType(value: string): void {
		if (!isKeyType(value)) return;
		applyKeyType(value);
		saved = false;
		serverFields = {};
		banner = [];
	}

	function setReplace(on: boolean): void {
		replace = on;
		saved = false;
		serverFields = {};
		banner = [];
		if (!on) {
			guard.epoch += 1;
			key = '';
			secret = '';
			passphrase = '';
			clearGen += 1;
		}
	}

	function clearPageSecrets(): void {
		guard.epoch += 1;
		key = '';
		secret = '';
		passphrase = '';
		clearGen += 1;
	}

	function takeSecrets() {
		const sent = { keyType, key, secret, passphrase };
		clearPageSecrets();
		return sent;
	}

	async function onSubmit(event: SubmitEvent) {
		event.preventDefault();
		const acct = detail.account;
		const id = apiId;
		if (!acct || id === undefined || saving || deleting) return;
		const replacing = replace;
		const kind = selectedKind;
		saved = false;
		serverFields = {};
		banner = [];
		referencedIds = null;
		const body: { revision: number; name: string; credentials?: CredentialsBody } = {
			revision: acct.revision,
			name
		};
		if (replacing) {
			const sent = takeSecrets();
			if (!kind) {
				banner = [kindsFeed.items ? 'This kind is not in the catalog.' : 'Account kinds are not loaded.'];
				return;
			}
			const credentials = credentialsFrom(kind, sent);
			if (!credentials) {
				banner = [kind.key_types.length === 0 ? 'This kind has no key type' : 'Select a key type'];
				return;
			}
			body.credentials = credentials;
		} else {
			clearPageSecrets();
		}
		saving = true;
		try {
			const result = await api.PUT('/accounts/{api_id}', {
				params: { path: { api_id: id } },
				body
			});
			if (result.response.status === 200 && result.data) {
				detail.account = result.data;
				name = result.data.name;
				replace = false;
				saved = true;
				return;
			}
			if (result.response.status === 401) return;
			if (isProblem(result.error)) {
				const mapped = applyAccountProblem(result.error);
				serverFields = mapped.fields;
				banner = mapped.banner;
				if (result.response.status === 409 && mapped.fields['/revision']) {
					await detail.load(id);
				}
				return;
			}
			const failure = failureOf(result.error, result.response);
			banner = [failure?.message ?? `HTTP ${result.response.status}`];
		} catch {
			banner = ['API unreachable'];
		} finally {
			saving = false;
		}
	}

	function askDelete(): void {
		referencedIds = null;
		banner = [];
		saved = false;
		confirmDelete = true;
	}

	async function runDelete(): Promise<void> {
		const acct = detail.account;
		const id = apiId;
		if (!acct || id === undefined || saving || deleting) return;
		const revision = acct.revision;
		const priorRefs = acct.referenced_by;
		deleting = true;
		confirmDelete = false;
		banner = [];
		referencedIds = null;
		try {
			const result = await api.DELETE('/accounts/{api_id}', {
				params: { path: { api_id: id }, query: { revision } }
			});
			if (result.response.status === 204) {
				await goto('/accounts');
				return;
			}
			if (result.response.status === 401) return;
			if (isProblem(result.error) && result.error.code === 'referenced') {
				const fresh = await detail.load(id);
				referencedIds = fresh?.referenced_by ?? priorRefs;
				banner = [result.error.detail || result.error.title];
				return;
			}
			if (isProblem(result.error) && result.error.code === 'conflict') {
				const fresh = await detail.load(id);
				banner = [result.error.detail || result.error.title];
				if (fresh) confirmDelete = true;
				return;
			}
			if (result.response.status === 404) {
				detail.markMissing();
				return;
			}
			const failure = failureOf(result.error, result.response);
			banner = [failure?.message ?? `HTTP ${result.response.status}`];
		} catch {
			banner = ['API unreachable'];
		} finally {
			deleting = false;
		}
	}
</script>

{#snippet fieldError(path: string)}
	{#if message(path)}
		<span id={fieldDomId(path)} class="field-error">{message(path)}</span>
	{/if}
{/snippet}

<svelte:head>
	<title>Account {apiId ?? ''} · mftiko</title>
</svelte:head>

<p><a href="/accounts">Accounts</a></p>
<h1>Account {#if apiId !== undefined}<span class="mono">{apiId}</span>{/if}</h1>

{#if banner.length > 0}
	<div class="banner-error" role="alert">
		{#each banner as line, i (i)}
			<p>{line}</p>
		{/each}
		{#if referencedIds}
			{#if referencedIds.length === 0}
				<p>No session is listed on the account.</p>
			{:else}
				<ul>
					{#each referencedIds as id (id)}
						<li><a class="mono" href="/sessions/{id}">{id}</a></li>
					{/each}
				</ul>
			{/if}
		{/if}
	</div>
{/if}

{#if apiId === undefined || detail.missing}
	<p class="status" role="status">Not found.</p>
{:else}
	<FeedStatus
		loading={detail.loading}
		error={detail.error}
		empty={detail.account === null}
		onretry={() => {
			if (apiId !== undefined) void detail.load(apiId);
		}}
	/>
	<FeedStatus
		loading={kindsFeed.loading}
		error={kindsFeed.error}
		empty={kindsFeed.items === null}
		onretry={() => kindsFeed.reload()}
	/>

	{#if detail.account}
		<dl class="facts">
			<dt>Name</dt>
			<dd>{detail.account.name}</dd>
			<dt>Kind</dt>
			<dd>
				{detail.account.kind}
				{@render fieldError('/kind')}
			</dd>
			<dt>TD instance</dt>
			<dd>
				{detail.account.td_instance}
				{@render fieldError('/td_instance')}
			</dd>
			<dt>Region</dt>
			<dd>{detail.account.region}</dd>
			<dt>Revision</dt>
			<dd>
				{detail.account.revision}
				{@render fieldError('/revision')}
			</dd>
			<dt>Referenced by</dt>
			<dd>
				{#if detail.account.referenced_by.length === 0}
					—
				{:else}
					{#each detail.account.referenced_by as id (id)}
						<a class="mono" href="/sessions/{id}">{id}</a>{' '}
					{/each}
				{/if}
			</dd>
		</dl>

		{#if saved}
			<p class="status" role="status">Saved.</p>
		{/if}

		<form autocomplete="off" onsubmit={onSubmit}>
			<div class="field">
				<label for="name">Name</label>
				<input
					id="name"
					bind:value={name}
					autocomplete="off"
					autocapitalize="off"
					spellcheck={false}
					aria-invalid={message('/name') ? 'true' : undefined}
					aria-describedby={message('/name') ? fieldDomId('/name') : undefined}
					oninput={() => (saved = false)}
				/>
				{@render fieldError('/name')}
			</div>
			<label class="check">
				<input
					type="checkbox"
					checked={replace}
					onchange={(event) => {
						const el = event.currentTarget;
						if (el instanceof HTMLInputElement) setReplace(el.checked);
					}}
				/>
				<span lang="zh-Hant">更換憑證</span>
			</label>
			{#if replace && selectedKind}
				{@render fieldError('/credentials')}
				<CredentialsFields
					kind={selectedKind}
					{keyType}
					{guard}
					{clearGen}
					errors={serverFields}
					bind:apiKey={key}
					bind:secret
					bind:passphrase
					{onKeyType}
				/>
			{:else if replace && kindsFeed.items}
				<p class="status">This kind is not in the catalog.</p>
			{/if}
			<button class="primary" type="submit" disabled={saving || deleting}>
				{saving ? 'Saving…' : 'Save'}
			</button>
		</form>

		<h2>Delete</h2>
		{#if confirmDelete}
			<div class="confirm" role="group" aria-label="Confirm delete">
				<p>Delete this account?</p>
				<div class="actions">
					<button type="button" onclick={() => void runDelete()} disabled={deleting || saving}>Delete</button>
					<button type="button" onclick={() => (confirmDelete = false)} disabled={deleting || saving}>Cancel</button>
				</div>
			</div>
		{:else}
			<button type="button" onclick={askDelete} disabled={deleting || saving}>Delete</button>
		{/if}
	{/if}
{/if}

<style>
	form {
		display: flex;
		flex-direction: column;
		gap: 0.85rem;
		max-width: 46rem;
		margin-top: 1rem;
	}

	.field {
		display: flex;
		flex-direction: column;
		gap: 0.3rem;
	}

	label {
		font-size: 0.9rem;
	}

	.check {
		display: flex;
		gap: 0.45rem;
		align-items: center;
	}

	input {
		font: inherit;
		color: var(--fg);
		background: var(--bg);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		padding: 0.45rem 0.55rem;
	}

	.check input {
		padding: 0;
	}

	input[aria-invalid='true'] {
		border-color: var(--danger);
	}

	.field-error {
		color: var(--danger);
		font-size: 0.9rem;
	}

	.banner-error {
		margin: 0 0 0.75rem;
		padding: 0.55rem 0.75rem;
		background: var(--danger-bg);
		color: var(--danger);
	}

	.banner-error p {
		margin: 0.15rem 0;
	}

	.banner-error ul {
		margin: 0.35rem 0 0;
		padding-left: 1.2rem;
	}

	.actions {
		display: flex;
		gap: 0.5rem;
	}
</style>
