<script lang="ts">
	import { goto } from '$app/navigation';
	import CredentialsFields from '#lib/CredentialsFields.svelte';
	import FeedStatus from '#lib/FeedStatus.svelte';
	import {
		applyAccountProblem,
		credentialsFrom,
		fieldDomId,
		isKeyType,
		nextKeyType,
		unsupportedReason,
		type SecretGuard
	} from '#lib/account-form.ts';
	import { api, failureOf, isProblem, type KeyType } from '#lib/api.ts';
	import { AccountKindFeed } from '#lib/live.svelte.ts';
	import { onMount } from 'svelte';

	const kindsFeed = new AccountKindFeed();
	const guard: SecretGuard = { epoch: 0 };

	let name = $state('');
	let kindName = $state('');
	let tdInstance = $state('');
	let keyType = $state<KeyType | ''>('');
	let key = $state('');
	let secret = $state('');
	let passphrase = $state('');
	let clearGen = $state(0);
	let submitting = $state(false);
	let serverFields = $state<Record<string, string>>({});
	let banner = $state<string[]>([]);

	const selectedKind = $derived((kindsFeed.items ?? []).find((item) => item.kind === kindName));

	function message(path: string): string | undefined {
		return serverFields[path];
	}

	onMount(() => {
		void kindsFeed.reload();
		return () => kindsFeed.cancel();
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

	function pickKind(value: string): void {
		const next = (kindsFeed.items ?? []).find((item) => item.kind === value);
		if (!next || !next.supported) return;
		applyKeyType(nextKeyType(next, keyType));
		if (!next.passphrase) passphrase = '';
		kindName = next.kind;
		serverFields = {};
		banner = [];
	}

	function onKeyType(value: string): void {
		if (!isKeyType(value)) return;
		applyKeyType(value);
		serverFields = {};
		banner = [];
	}

	/** Copy the secrets for this request, then drop them from page state before any await (D1101). */
	function takeSecrets() {
		const sent = { keyType, key, secret, passphrase };
		guard.epoch += 1;
		key = '';
		secret = '';
		passphrase = '';
		clearGen += 1;
		return sent;
	}

	async function onSubmit(event: SubmitEvent) {
		event.preventDefault();
		if (submitting) return;
		const kind = selectedKind;
		const sent = takeSecrets();
		serverFields = {};
		banner = [];
		if (!kind || !kind.supported) {
			banner = ['Select a kind'];
			return;
		}
		const credentials = credentialsFrom(kind, sent);
		if (!credentials) {
			banner = [kind.key_types.length === 0 ? 'This kind has no key type' : 'Select a key type'];
			return;
		}
		submitting = true;
		try {
			const result = await api.POST('/accounts', {
				body: {
					name,
					kind: kind.kind,
					td_instance: tdInstance,
					credentials
				}
			});
			if (result.response.status === 201 && result.data) {
				await goto(`/accounts/${result.data.api_id}`);
				return;
			}
			if (result.response.status === 401) return;
			if (isProblem(result.error)) {
				const mapped = applyAccountProblem(result.error);
				serverFields = mapped.fields;
				banner = mapped.banner;
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
</script>

{#snippet fieldError(path: string)}
	{#if message(path)}
		<span id={fieldDomId(path)} class="field-error">{message(path)}</span>
	{/if}
{/snippet}

<svelte:head>
	<title>New account · mftiko</title>
</svelte:head>

<p><a href="/accounts">Accounts</a></p>
<h1>New account</h1>

{#if banner.length > 0}
	<div class="banner-error" role="alert">
		{#each banner as line, i (i)}
			<p>{line}</p>
		{/each}
	</div>
{/if}

<FeedStatus
	loading={kindsFeed.loading}
	error={kindsFeed.error}
	empty={kindsFeed.items === null}
	onretry={() => kindsFeed.reload()}
/>

{#if kindsFeed.items}
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
			/>
			{@render fieldError('/name')}
		</div>
		<div class="field">
			<label for="kind">Kind</label>
			<select
				id="kind"
				value={kindName}
				aria-invalid={message('/kind') ? 'true' : undefined}
				aria-describedby={message('/kind') ? fieldDomId('/kind') : undefined}
				onchange={(event) => {
					const el = event.currentTarget;
					if (el instanceof HTMLSelectElement) pickKind(el.value);
				}}
			>
				<option value="">Select a kind</option>
				{#each kindsFeed.items as kind (kind.kind)}
					{@const reason = unsupportedReason(kind)}
					<option value={kind.kind} disabled={reason !== undefined}>
						{kind.label}{#if reason} — {reason}{/if}
					</option>
				{/each}
			</select>
			{@render fieldError('/kind')}
		</div>
		<div class="field">
			<label for="td">TD instance</label>
			<input
				id="td"
				bind:value={tdInstance}
				autocomplete="off"
				autocapitalize="off"
				spellcheck={false}
				aria-invalid={message('/td_instance') ? 'true' : undefined}
				aria-describedby={message('/td_instance') ? fieldDomId('/td_instance') : undefined}
			/>
			<p class="status">Free input. The API checks the topology and the public key.</p>
			{@render fieldError('/td_instance')}
		</div>
		{#if selectedKind}
			<h2>Credentials</h2>
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
		{/if}
		<button class="primary" type="submit" disabled={submitting}>
			{submitting ? 'Creating…' : 'Create'}
		</button>
	</form>
{/if}

<style>
	form {
		display: flex;
		flex-direction: column;
		gap: 0.85rem;
		max-width: 46rem;
	}

	.field {
		display: flex;
		flex-direction: column;
		gap: 0.3rem;
	}

	label {
		font-size: 0.9rem;
	}

	input,
	select {
		font: inherit;
		color: var(--fg);
		background: var(--bg);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		padding: 0.45rem 0.55rem;
	}

	input[aria-invalid='true'],
	select[aria-invalid='true'] {
		border-color: var(--danger);
	}

	option:disabled {
		color: var(--muted);
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
</style>
