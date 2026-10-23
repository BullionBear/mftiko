<script lang="ts">
	// Credential inputs (D1101). ed25519 secret is a textarea so PEM newlines survive;
	// HMAC key, secret and passphrase stay type=password. Nothing here writes storage.
	import type { AccountKind, KeyType } from './api';
	import { fieldDomId, type SecretGuard } from './account-form';

	let {
		kind,
		keyType,
		guard,
		clearGen,
		errors,
		apiKey = $bindable(''),
		secret = $bindable(''),
		passphrase = $bindable(''),
		onKeyType
	}: {
		kind: AccountKind;
		keyType: KeyType | '';
		guard: SecretGuard;
		clearGen: number;
		errors: Record<string, string>;
		apiKey?: string;
		secret?: string;
		passphrase?: string;
		onKeyType: (value: string) => void;
	} = $props();

	let fileEl = $state<HTMLInputElement | null>(null);
	let readError = $state('');

	const pem = $derived(keyType === 'ed25519');

	function message(path: string): string | undefined {
		return errors[path];
	}

	function invalid(path: string): 'true' | undefined {
		return message(path) ? 'true' : undefined;
	}

	function describedBy(path: string): string | undefined {
		return message(path) ? fieldDomId(path) : undefined;
	}

	// Parent bumps clearGen when it drops secrets. Clear the file control too so the
	// chosen file does not stay selected (D1101).
	$effect(() => {
		void clearGen;
		readError = '';
		if (fileEl) fileEl.value = '';
	});

	function dropFile(): void {
		if (fileEl) fileEl.value = '';
	}

	async function readPem(event: Event): Promise<void> {
		const input = event.currentTarget;
		if (!(input instanceof HTMLInputElement)) return;
		const file = input.files?.[0];
		if (!file) return;
		const gen = guard.epoch;
		readError = '';
		let text: string;
		try {
			text = await file.text();
		} catch {
			if (gen !== guard.epoch) {
				dropFile();
				return;
			}
			readError = 'Could not read the file.';
			dropFile();
			return;
		}
		// Submit (or a key-type change) won the race: do not put the text back (D1101).
		if (gen !== guard.epoch) {
			dropFile();
			return;
		}
		secret = text;
		dropFile();
	}
</script>

{#snippet fieldError(path: string)}
	{#if message(path)}
		<span id={fieldDomId(path)} class="field-error">{message(path)}</span>
	{/if}
{/snippet}

<div class="field">
	<label for="key-type">Key type</label>
	<select
		id="key-type"
		value={keyType}
		aria-invalid={invalid('/credentials/key_type')}
		aria-describedby={describedBy('/credentials/key_type')}
		onchange={(event) => {
			const el = event.currentTarget;
			if (el instanceof HTMLSelectElement) onKeyType(el.value);
		}}
	>
		{#each kind.key_types as type (type)}
			<option value={type}>{type}</option>
		{/each}
	</select>
	{@render fieldError('/credentials/key_type')}
</div>

<div class="field">
	<label for="api-key">Key</label>
	<input
		id="api-key"
		type="password"
		bind:value={apiKey}
		autocomplete="off"
		autocapitalize="off"
		spellcheck={false}
		aria-invalid={invalid('/credentials/key')}
		aria-describedby={describedBy('/credentials/key')}
	/>
	{@render fieldError('/credentials/key')}
</div>

<div class="field">
	<label for="secret">Secret</label>
	{#if pem}
		<textarea
			id="secret"
			rows="8"
			bind:value={secret}
			autocomplete="off"
			autocapitalize="off"
			spellcheck={false}
			aria-invalid={invalid('/credentials/secret')}
			aria-describedby={describedBy('/credentials/secret')}
		></textarea>
		<p class="status">Sent as entered, including line breaks.</p>
		{@render fieldError('/credentials/secret')}
		<label for="pem-file">.pem file</label>
		<input
			id="pem-file"
			type="file"
			accept=".pem,.PEM"
			autocomplete="off"
			bind:this={fileEl}
			onchange={(event) => void readPem(event)}
		/>
		<p class="status">Optional. The file text is copied into the secret field.</p>
		{#if readError}
			<span class="field-error">{readError}</span>
		{/if}
	{:else}
		<input
			id="secret"
			type="password"
			bind:value={secret}
			autocomplete="off"
			autocapitalize="off"
			spellcheck={false}
			aria-invalid={invalid('/credentials/secret')}
			aria-describedby={describedBy('/credentials/secret')}
		/>
		{@render fieldError('/credentials/secret')}
	{/if}
</div>

{#if kind.passphrase}
	<div class="field">
		<label for="passphrase">Passphrase</label>
		<input
			id="passphrase"
			type="password"
			bind:value={passphrase}
			autocomplete="off"
			autocapitalize="off"
			spellcheck={false}
			aria-invalid={invalid('/credentials/passphrase')}
			aria-describedby={describedBy('/credentials/passphrase')}
		/>
		{@render fieldError('/credentials/passphrase')}
	</div>
{/if}

<style>
	.field {
		display: flex;
		flex-direction: column;
		gap: 0.3rem;
	}

	label {
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
</style>
