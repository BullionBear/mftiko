<script lang="ts">
	import { goto } from '$app/navigation';
	import FeedStatus from '#lib/FeedStatus.svelte';
	import { api, collectPages, createSession, failureOf, isProblem, type Account, type StrategyTag } from '#lib/api.ts';
	import { AccountFeed, InstanceFeed } from '#lib/live.svelte.ts';
	import {
		applyProblem,
		emptyDraft,
		fieldDomId,
		firstErrorStep,
		joinError,
		localErrors,
		newIdempotencyKey,
		parseAlgo,
		sessionRequestBody,
		type Draft
	} from '#lib/session-draft.ts';
	import { onMount } from 'svelte';

	const steps = ['Strategy', 'Params', 'Feeds', 'Accounts', 'Limits', 'Confirm'];

	// One key for the life of this wizard. A failed submit retries with it (E10-D089, E10-D068).
	const idempotencyKey = newIdempotencyKey();

	const accountsFeed = new AccountFeed();
	const sts = new InstanceFeed('sts');

	let draft = $state<Draft>(emptyDraft());
	let stepIndex = $state(0);
	let attempted = $state(false);
	let submitting = $state(false);
	let serverFields = $state<Record<string, string>>({});
	let banner = $state<string[]>([]);
	let rowKey = 1;

	let versions = $state<StrategyTag[] | null>(null);
	let latestTag = $state<StrategyTag | null>(null);
	let versionsLoading = $state(false);
	let versionsError = $state<string | null>(null);
	let versionGen = 0;

	// E10-D086: prefill when the manifest carries default params. The tag list
	// does not include them, so the box starts empty.
	const clientErrors = $derived(localErrors(draft));

	function message(path: string): string | undefined {
		const local = attempted ? clientErrors[path] : undefined;
		return joinError(local, serverFields[path]);
	}

	function invalid(path: string): 'true' | undefined {
		return message(path) ? 'true' : undefined;
	}

	function describedBy(path: string): string | undefined {
		return message(path) ? fieldDomId(path) : undefined;
	}

	function clearServer(): void {
		serverFields = {};
		banner = [];
	}

	onMount(() => {
		void accountsFeed.reload();
		void sts.reload();
		return () => {
			versionGen += 1;
			accountsFeed.cancel();
			sts.cancel();
		};
	});

	function reloadAll() {
		void accountsFeed.reload();
		void sts.reload();
		if (draft.repository.trim() !== '') void loadTags();
	}

	async function loadTags(): Promise<void> {
		const repo = draft.repository.trim();
		const mine = ++versionGen;
		versionsLoading = true;
		versionsError = null;
		versions = null;
		latestTag = null;
		draft.strategyRef = '';
		if (repo === '') {
			versionsLoading = false;
			return;
		}
		const result = await collectPages<StrategyTag>((cursor) =>
			api.GET('/strategies', {
				params: {
					query: cursor ? { repository: repo, limit: 100, cursor } : { repository: repo, limit: 100 }
				}
			})
		);
		if (mine !== versionGen) return;
		versionsLoading = false;
		if (result.failure) {
			if (result.failure.status !== 401) versionsError = result.failure.message;
			return;
		}
		versions = result.items;
		latestTag = result.latest ?? null;
	}

	function addFeed() {
		draft.feeds.push({ key: rowKey++, feed: '', mdInstance: '' });
		clearServer();
	}

	function removeFeed(key: number) {
		draft.feeds = draft.feeds.filter((row) => row.key !== key);
		clearServer();
	}

	function addAccount() {
		draft.accounts.push({ key: rowKey++, apiId: '', name: '', algoIds: [], algoDraft: '' });
		clearServer();
	}

	function removeAccount(key: number) {
		draft.accounts = draft.accounts.filter((row) => row.key !== key);
		clearServer();
	}

	function accountChanged(index: number, apiId: string) {
		const row = draft.accounts[index];
		row.apiId = apiId;
		row.algoIds = [];
		row.algoDraft = '';
		const account = (accountsFeed.items ?? []).find((item) => String(item.api_id) === apiId);
		row.name = account?.name ?? '';
		clearServer();
	}

	function commitAlgos(): boolean {
		for (const row of draft.accounts) {
			if (row.algoDraft.trim() === '') continue;
			const n = parseAlgo(row.algoDraft);
			if (n === undefined || row.algoIds.includes(n)) return false;
		}
		for (const row of draft.accounts) {
			if (row.algoDraft.trim() === '') continue;
			const n = parseAlgo(row.algoDraft);
			if (n === undefined) continue;
			row.algoIds = [...row.algoIds, n];
			row.algoDraft = '';
		}
		return true;
	}

	function goTo(step: number) {
		if (stepIndex === 3 && step !== 3 && !commitAlgos()) {
			attempted = true;
			return;
		}
		stepIndex = step;
	}

	function addAlgo(index: number) {
		const row = draft.accounts[index];
		if (row.algoDraft.trim() === '') return;
		const n = parseAlgo(row.algoDraft);
		if (n === undefined || row.algoIds.includes(n)) {
			attempted = true;
			return;
		}
		row.algoIds = [...row.algoIds, n];
		row.algoDraft = '';
		clearServer();
	}

	function removeAlgo(index: number, id: number) {
		const row = draft.accounts[index];
		row.algoIds = row.algoIds.filter((item) => item !== id);
		clearServer();
	}

	function accountById(apiId: string): Account | undefined {
		return (accountsFeed.items ?? []).find((item) => String(item.api_id) === apiId);
	}

	function shortDigest(digest: string): string {
		return digest.length > 19 ? `${digest.slice(0, 19)}…` : digest;
	}

	function blank(value: string): string {
		return value.trim() === '' ? '—' : value;
	}

	async function deploy() {
		attempted = true;
		clearServer();
		if (!commitAlgos()) {
			stepIndex = 3;
			return;
		}
		const local = localErrors(draft);
		if (Object.keys(local).length > 0) {
			const step = firstErrorStep(local);
			if (step !== undefined) stepIndex = step;
			return;
		}
		submitting = true;
		try {
			const result = await createSession(sessionRequestBody(draft), idempotencyKey);
			if (result.response.status === 201 && result.data) {
				await goto(`/sessions/${result.data.id}`);
				return;
			}
			if (result.response.status === 401) return;
			if (isProblem(result.error)) {
				const mapped = applyProblem(result.error, draft);
				serverFields = mapped.fields;
				banner = mapped.banner;
				const step = firstErrorStep(mapped.fields);
				if (step !== undefined) stepIndex = step;
				return;
			}
			const failure = failureOf(result.error, result.response);
			banner = [failure?.message ?? `HTTP ${result.response.status}`];
		} catch {
			banner = ['API unreachable'];
		} finally {
			submitting = false;
		}
	}

	function onSubmit(event: SubmitEvent) {
		event.preventDefault();
		if (stepIndex < steps.length - 1) goTo(stepIndex + 1);
		else void deploy();
	}

	const selectedTag = $derived(versions?.find((item) => item.tag === draft.strategyRef));
</script>

{#snippet fieldError(path: string)}
	{#if message(path)}
		<span id={fieldDomId(path)} class="field-error">{message(path)}</span>
	{/if}
{/snippet}

<svelte:head>
	<title>Deploy session · mftiko</title>
</svelte:head>

<p><a href="/sessions">Sessions</a></p>
<h1>Deploy session</h1>

{#if banner.length > 0}
	<div class="banner-error" role="alert">
		{#each banner as line, i (i)}
			<p>{line}</p>
		{/each}
	</div>
{/if}

<FeedStatus
	loading={accountsFeed.loading || sts.loading}
	error={accountsFeed.error ?? sts.error}
	empty={accountsFeed.items === null || sts.items === null}
	onretry={reloadAll}
/>

<nav aria-label="Create session">
	<ol class="steps">
		{#each steps as label, i (label)}
			<li>
				<button type="button" aria-current={stepIndex === i ? 'step' : undefined} onclick={() => goTo(i)}>
					{i + 1}. {label}
				</button>
			</li>
		{/each}
	</ol>
</nav>

<form onsubmit={onSubmit}>
	{#if stepIndex === 0}
		<div class="field">
			<label for="name">Name</label>
			<input
				id="name"
				name="name"
				bind:value={draft.name}
				autocomplete="off"
				autocapitalize="off"
				spellcheck="false"
				aria-invalid={invalid('/name')}
				aria-describedby={describedBy('/name')}
			/>
			{@render fieldError('/name')}
		</div>
		<div class="field">
			<label for="sts">STS instance</label>
			<select
				id="sts"
				bind:value={draft.stsInstance}
				aria-invalid={invalid('/sts_instance')}
				aria-describedby={describedBy('/sts_instance')}
			>
				<option value="">Select an STS instance</option>
				{#each sts.items ?? [] as instance (instance.name)}
					<option value={instance.name}>{instance.name} · {instance.region}</option>
				{/each}
			</select>
			{@render fieldError('/sts_instance')}
		</div>
		<div class="field">
			<label for="repository">Repository</label>
			<input
				id="repository"
				name="repository"
				bind:value={draft.repository}
				autocomplete="off"
				autocapitalize="off"
				spellcheck="false"
				placeholder="ghcr.io/acme/md_watch"
				aria-invalid={invalid('/strategy/repository')}
				aria-describedby={describedBy('/strategy/repository')}
				oninput={() => {
					versionGen += 1;
					versions = null;
					latestTag = null;
					draft.strategyRef = '';
					clearServer();
				}}
			/>
			{@render fieldError('/strategy/repository')}
			{@render fieldError('/strategy')}
			<button type="button" onclick={() => void loadTags()} disabled={versionsLoading || draft.repository.trim() === ''}>
				Load tags
			</button>
			{#if versionsError}
				<p class="status">{versionsError}</p>
			{/if}
		</div>
		<div class="field">
			<label for="version">Version</label>
			<select
				id="version"
				bind:value={draft.strategyRef}
				disabled={!versions || versionsLoading}
				aria-invalid={invalid('/strategy/ref')}
				aria-describedby={describedBy('/strategy/ref')}
			>
				<option value="">Select a version</option>
				{#each versions ?? [] as version (version.tag)}
					<option value={version.tag}>
						{version.tag}{latestTag && version.digest === latestTag.digest ? ' · latest' : ''} · {shortDigest(version.digest)}
					</option>
				{/each}
			</select>
			{@render fieldError('/strategy/ref')}
			{#if versions && versions.length === 0 && !versionsLoading}
				<p class="status">No tags.</p>
			{/if}
			{#if selectedTag}
				<p class="status mono">{selectedTag.digest}</p>
			{/if}
		</div>
	{:else if stepIndex === 1}
		<div class="field">
			<label for="params">Params</label>
			<textarea
				id="params"
				bind:value={draft.paramsText}
				rows="12"
				spellcheck="false"
				autocomplete="off"
				autocapitalize="off"
				aria-invalid={invalid('/params')}
				aria-describedby={describedBy('/params')}
			></textarea>
			<p class="status">JSON, sent as typed. The platform does not interpret it.</p>
			{@render fieldError('/params')}
		</div>
	{:else if stepIndex === 2}
		<p class="status">Free input. The console does not check these against a catalog.</p>
		{#each draft.feeds as row, i (row.key)}
			<div class="row">
				<div class="field">
					<label for="feed-{row.key}">Feed</label>
					<input
						id="feed-{row.key}"
						bind:value={draft.feeds[i].feed}
						autocomplete="off"
						autocapitalize="off"
						spellcheck="false"
						placeholder="orderbook_delta.Binance_Spot_BTCUSDT"
						aria-invalid={invalid(`/feeds/${i}/feed`)}
						aria-describedby={describedBy(`/feeds/${i}/feed`)}
					/>
					{@render fieldError(`/feeds/${i}/feed`)}
				</div>
				<div class="field">
					<label for="md-{row.key}">MD instance</label>
					<input
						id="md-{row.key}"
						bind:value={draft.feeds[i].mdInstance}
						autocomplete="off"
						autocapitalize="off"
						spellcheck="false"
						aria-invalid={invalid(`/feeds/${i}/md_instance`)}
						aria-describedby={describedBy(`/feeds/${i}/md_instance`)}
					/>
					{@render fieldError(`/feeds/${i}/md_instance`)}
				</div>
				<button type="button" onclick={() => removeFeed(row.key)}>Remove</button>
			</div>
			{@render fieldError(`/feeds/${i}`)}
		{/each}
		{@render fieldError('/feeds')}
		<button type="button" onclick={addFeed}>Add feed</button>
	{:else if stepIndex === 3}
		{#each draft.accounts as row, i (row.key)}
			<fieldset>
				<legend>Account {i + 1}</legend>
				<div class="field">
					<label for="api-{row.key}">Account</label>
					<select
						id="api-{row.key}"
						value={row.apiId}
						aria-invalid={invalid(`/accounts/${i}/api_id`)}
						aria-describedby={describedBy(`/accounts/${i}/api_id`)}
						onchange={(event) => {
							const el = event.currentTarget;
							if (el instanceof HTMLSelectElement) accountChanged(i, el.value);
						}}
					>
						<option value="">Select an account</option>
						{#each accountsFeed.items ?? [] as account (account.api_id)}
							<option value={String(account.api_id)}>
								{account.name} · {account.api_id} · {account.kind}
							</option>
						{/each}
					</select>
					{@render fieldError(`/accounts/${i}/api_id`)}
				</div>
				{#if row.apiId}
					<div class="field">
						<label for="acct-name-{row.key}">Name</label>
						<input
							id="acct-name-{row.key}"
							bind:value={draft.accounts[i].name}
							autocomplete="off"
							autocapitalize="off"
							spellcheck="false"
							aria-invalid={invalid(`/accounts/${i}/name`)}
							aria-describedby={describedBy(`/accounts/${i}/name`)}
						/>
						{@render fieldError(`/accounts/${i}/name`)}
					</div>
					<div class="field">
						<span id="algos-label-{row.key}">algo_id</span>
						{#if row.algoIds.length === 0}
							<p class="status">None yet.</p>
						{:else}
							<ul aria-labelledby="algos-label-{row.key}">
								{#each row.algoIds as id (id)}
									<li>
										<span class="mono">{id}</span>
										<button type="button" onclick={() => removeAlgo(i, id)}>Remove</button>
									</li>
								{/each}
							</ul>
						{/if}
						<div class="add-algo">
							<label class="status" for="algo-{row.key}">Add algo_id</label>
							<input
								id="algo-{row.key}"
								bind:value={draft.accounts[i].algoDraft}
								inputmode="numeric"
								autocomplete="off"
								spellcheck="false"
								aria-invalid={invalid(`/accounts/${i}/algo_ids`)}
								aria-describedby={describedBy(`/accounts/${i}/algo_ids`)}
								onkeydown={(event) => {
									if (event.key !== 'Enter') return;
									event.preventDefault();
									addAlgo(i);
								}}
							/>
							<button type="button" onclick={() => addAlgo(i)}>Add</button>
						</div>
						<p class="status">1–1023. Listed only for this account.</p>
						{@render fieldError(`/accounts/${i}/algo_ids`)}
					</div>
				{/if}
				{@render fieldError(`/accounts/${i}`)}
				<button type="button" onclick={() => removeAccount(row.key)}>Remove account</button>
			</fieldset>
		{/each}
		{@render fieldError('/accounts')}
		<button type="button" onclick={addAccount}>Add account</button>
	{:else if stepIndex === 4}
		<fieldset>
			<legend>Restart</legend>
			<div class="field">
				<label for="restart-mode">mode</label>
				<select
					id="restart-mode"
					bind:value={draft.restartMode}
					aria-invalid={invalid('/restart/mode')}
					aria-describedby={describedBy('/restart/mode')}
				>
					<option value="">Default (on_failure)</option>
					<option value="on_failure">on_failure</option>
					<option value="never">never</option>
				</select>
				{@render fieldError('/restart/mode')}
				{@render fieldError('/restart')}
				<p class="status">Omitted mode is on_failure. 0 for max_restarts or window_s uses the deployment default (5 times, 300s).</p>
			</div>
			<div class="nums">
				<div class="field">
					<label for="max-restarts">max_restarts</label>
					<input
						id="max-restarts"
						bind:value={draft.maxRestarts}
						inputmode="numeric"
						autocomplete="off"
						spellcheck="false"
						aria-invalid={invalid('/restart/max_restarts')}
						aria-describedby={describedBy('/restart/max_restarts')}
					/>
					{@render fieldError('/restart/max_restarts')}
				</div>
				<div class="field">
					<label for="window-s">window_s</label>
					<input
						id="window-s"
						bind:value={draft.windowS}
						inputmode="numeric"
						autocomplete="off"
						spellcheck="false"
						aria-invalid={invalid('/restart/window_s')}
						aria-describedby={describedBy('/restart/window_s')}
					/>
					{@render fieldError('/restart/window_s')}
				</div>
			</div>
		</fieldset>
		<fieldset>
			<legend>Timeouts</legend>
			<div class="nums">
				<div class="field">
					<label for="deps">deps_timeout_s</label>
					<input
						id="deps"
						bind:value={draft.depsTimeoutS}
						inputmode="numeric"
						autocomplete="off"
						spellcheck="false"
						aria-invalid={invalid('/deps_timeout_s')}
						aria-describedby={describedBy('/deps_timeout_s')}
					/>
					{@render fieldError('/deps_timeout_s')}
				</div>
				<div class="field">
					<label for="start">start_timeout_s</label>
					<input
						id="start"
						bind:value={draft.startTimeoutS}
						inputmode="numeric"
						autocomplete="off"
						spellcheck="false"
						aria-invalid={invalid('/start_timeout_s')}
						aria-describedby={describedBy('/start_timeout_s')}
					/>
					{@render fieldError('/start_timeout_s')}
				</div>
				<div class="field">
					<label for="recv">recv_window_ms</label>
					<input
						id="recv"
						bind:value={draft.recvWindowMs}
						inputmode="numeric"
						autocomplete="off"
						spellcheck="false"
						aria-invalid={invalid('/recv_window_ms')}
						aria-describedby={describedBy('/recv_window_ms')}
					/>
					{@render fieldError('/recv_window_ms')}
				</div>
				<div class="field">
					<label for="grace">grace_ms</label>
					<input
						id="grace"
						bind:value={draft.graceMs}
						inputmode="numeric"
						autocomplete="off"
						spellcheck="false"
						aria-invalid={invalid('/grace_ms')}
						aria-describedby={describedBy('/grace_ms')}
					/>
					{@render fieldError('/grace_ms')}
				</div>
			</div>
			<p class="status">Empty is omitted. 0 uses the platform default: deps timeout 60s, start timeout 60s, grace 10s, recv window from the STS instance.</p>
		</fieldset>
		<fieldset>
			<legend>Limits</legend>
			<div class="nums">
				<div class="field">
					<label for="memory">memory_mb</label>
					<input
						id="memory"
						bind:value={draft.memoryMb}
						inputmode="numeric"
						autocomplete="off"
						spellcheck="false"
						aria-invalid={invalid('/limits/memory_mb')}
						aria-describedby={describedBy('/limits/memory_mb')}
					/>
					{@render fieldError('/limits/memory_mb')}
				</div>
				<div class="field">
					<label for="gomaxprocs">gomaxprocs</label>
					<input
						id="gomaxprocs"
						bind:value={draft.gomaxprocs}
						inputmode="numeric"
						autocomplete="off"
						spellcheck="false"
						aria-invalid={invalid('/limits/gomaxprocs')}
						aria-describedby={describedBy('/limits/gomaxprocs')}
					/>
					{@render fieldError('/limits/gomaxprocs')}
				</div>
				<div class="field">
					<label for="volume">volume_mb</label>
					<input
						id="volume"
						bind:value={draft.volumeMb}
						inputmode="numeric"
						autocomplete="off"
						spellcheck="false"
						aria-invalid={invalid('/limits/volume_mb')}
						aria-describedby={describedBy('/limits/volume_mb')}
					/>
					{@render fieldError('/limits/volume_mb')}
				</div>
				<div class="field">
					<label for="hook">hook_timeout_s</label>
					<input
						id="hook"
						bind:value={draft.hookTimeoutS}
						inputmode="numeric"
						autocomplete="off"
						spellcheck="false"
						aria-invalid={invalid('/limits/hook_timeout_s')}
						aria-describedby={describedBy('/limits/hook_timeout_s')}
					/>
					{@render fieldError('/limits/hook_timeout_s')}
				</div>
			</div>
			{@render fieldError('/limits')}
			<p class="status">Empty is omitted. 0 uses the STS default (hook timeout 10s, volume 1024 MiB).</p>
		</fieldset>
	{:else}
		<h2>Confirm</h2>
		<dl class="facts">
			<dt>Name</dt>
			<dd>{blank(draft.name)} {@render fieldError('/name')}</dd>
			<dt>STS</dt>
			<dd>{draft.stsInstance || '—'} {@render fieldError('/sts_instance')}</dd>
			<dt>Repository</dt>
			<dd>
				{draft.repository.trim() || '—'}
				{@render fieldError('/strategy/repository')}
				{@render fieldError('/strategy')}
			</dd>
			<dt>Version</dt>
			<dd>{draft.strategyRef || '—'} {@render fieldError('/strategy/ref')}</dd>
			<dt>Params</dt>
			<dd>
				{#if draft.paramsText.trim() === ''}
					—
				{:else}
					<pre class="params">{draft.paramsText}</pre>
				{/if}
				{@render fieldError('/params')}
			</dd>
			<dt>Feeds</dt>
			<dd>
				{#if draft.feeds.length === 0}
					—
				{:else}
					<ul>
						{#each draft.feeds as row, i (row.key)}
							<li>
								<span class="mono">{row.feed || '—'}</span>
								· {row.mdInstance || '—'}
								{@render fieldError(`/feeds/${i}`)}
								{@render fieldError(`/feeds/${i}/feed`)}
								{@render fieldError(`/feeds/${i}/md_instance`)}
							</li>
						{/each}
					</ul>
				{/if}
				{@render fieldError('/feeds')}
			</dd>
			<dt>Accounts</dt>
			<dd>
				{#if draft.accounts.length === 0}
					—
				{:else}
					<ul>
						{#each draft.accounts as row, i (row.key)}
							<li>
								{row.name || accountById(row.apiId)?.name || '—'}
								· {row.apiId || '—'}
								· algo {row.algoIds.length === 0 ? '—' : row.algoIds.join(', ')}
								{@render fieldError(`/accounts/${i}`)}
								{@render fieldError(`/accounts/${i}/api_id`)}
								{@render fieldError(`/accounts/${i}/name`)}
								{@render fieldError(`/accounts/${i}/algo_ids`)}
							</li>
						{/each}
					</ul>
				{/if}
				{@render fieldError('/accounts')}
			</dd>
			<dt>Restart</dt>
			<dd>
				{draft.restartMode || 'default (on_failure)'}
				· max {blank(draft.maxRestarts)}
				· window {blank(draft.windowS)}
				{@render fieldError('/restart')}
				{@render fieldError('/restart/mode')}
				{@render fieldError('/restart/max_restarts')}
				{@render fieldError('/restart/window_s')}
			</dd>
			<dt>Timeouts</dt>
			<dd>
				deps {blank(draft.depsTimeoutS)} · start {blank(draft.startTimeoutS)} · recv {blank(draft.recvWindowMs)} · grace {blank(draft.graceMs)}
				{@render fieldError('/deps_timeout_s')}
				{@render fieldError('/start_timeout_s')}
				{@render fieldError('/recv_window_ms')}
				{@render fieldError('/grace_ms')}
			</dd>
			<dt>Limits</dt>
			<dd>
				memory {blank(draft.memoryMb)} · gomaxprocs {blank(draft.gomaxprocs)} · volume {blank(draft.volumeMb)} · hook {blank(draft.hookTimeoutS)}
				{@render fieldError('/limits')}
				{@render fieldError('/limits/memory_mb')}
				{@render fieldError('/limits/gomaxprocs')}
				{@render fieldError('/limits/volume_mb')}
				{@render fieldError('/limits/hook_timeout_s')}
			</dd>
		</dl>
	{/if}

	<div class="actions">
		{#if stepIndex > 0}
			<button type="button" onclick={() => goTo(stepIndex - 1)}>Back</button>
		{/if}
		{#if stepIndex < steps.length - 1}
			<button class="primary" type="submit">Next</button>
		{:else}
			<button class="primary" type="submit" disabled={submitting}>
				{submitting ? 'Deploying…' : 'Deploy'}
			</button>
		{/if}
	</div>
</form>

<style>
	form {
		display: flex;
		flex-direction: column;
		gap: 0.85rem;
		max-width: 46rem;
	}

	.steps {
		display: flex;
		flex-wrap: wrap;
		gap: 0.35rem;
		margin: 0 0 1rem;
		padding: 0;
		list-style: none;
	}

	.steps button[aria-current='step'] {
		background: var(--accent);
		color: var(--accent-fg);
		border-color: transparent;
	}

	.field {
		display: flex;
		flex-direction: column;
		gap: 0.3rem;
	}

	label,
	.field > span {
		font-size: 0.9rem;
	}

	input,
	select,
	textarea {
		font: inherit;
		color: var(--fg);
		background: var(--bg);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		padding: 0.45rem 0.55rem;
	}

	textarea {
		font-family: var(--mono);
		font-size: 0.9rem;
		resize: vertical;
	}

	input[aria-invalid='true'],
	select[aria-invalid='true'],
	textarea[aria-invalid='true'] {
		border-color: var(--danger);
	}

	.field-error {
		color: var(--danger);
		font-size: 0.9rem;
	}

	.row,
	.nums,
	.add-algo {
		display: grid;
		grid-template-columns: 1fr 1fr auto;
		gap: 0.5rem;
		align-items: end;
	}

	.nums {
		grid-template-columns: 1fr 1fr;
		margin-top: 0.75rem;
	}

	.add-algo {
		grid-template-columns: auto 1fr auto;
		align-items: center;
	}

	fieldset {
		border: 1px solid var(--border);
		border-radius: var(--radius);
		margin: 0;
		display: flex;
		flex-direction: column;
		gap: 0.75rem;
	}

	legend {
		padding: 0 0.3rem;
	}

	ul {
		margin: 0.2rem 0;
		padding-left: 1.2rem;
	}

	li {
		margin: 0.2rem 0;
	}

	.actions {
		display: flex;
		gap: 0.5rem;
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

	pre.params {
		white-space: pre-wrap;
		word-break: break-word;
		background: var(--bg);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		padding: 0.55rem;
		margin: 0;
		font-family: var(--mono);
		font-size: 0.85rem;
	}

	@media (max-width: 40rem) {
		.row,
		.nums,
		.add-algo {
			grid-template-columns: 1fr;
		}
	}
</style>
