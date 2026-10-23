<script lang="ts">
	// Marks a problem+json field error on the control (D1004). `error` is the reason for this field's pointer.
	let {
		label,
		name,
		error = undefined,
		type = 'text',
		value = $bindable('')
	}: {
		label: string;
		name: string;
		error?: string;
		type?: 'text' | 'password';
		value?: string;
	} = $props();

	const errorId = $derived(`${name}-error`);
</script>

<div class="field">
	<label for={name}>{label}</label>
	<input
		id={name}
		{name}
		{type}
		bind:value
		autocomplete="off"
		autocapitalize="off"
		spellcheck="false"
		aria-invalid={error ? 'true' : undefined}
		aria-describedby={error ? errorId : undefined}
	/>
	{#if error}
		<span id={errorId} class="field-error">{error}</span>
	{/if}
</div>

<style>
	.field {
		display: flex;
		flex-direction: column;
		gap: 0.3rem;
	}

	label {
		font-size: 0.9rem;
	}

	input {
		font: inherit;
		color: var(--fg);
		background: var(--bg);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		padding: 0.45rem 0.55rem;
	}

	input[aria-invalid='true'] {
		border-color: var(--danger);
	}

	.field-error {
		color: var(--danger);
		font-size: 0.9rem;
	}
</style>
