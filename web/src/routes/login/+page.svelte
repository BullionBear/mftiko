<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import Field from '#lib/Field.svelte';
	import { api, failureOf, isProblem, reasonFor, safeNext, type Problem } from '#lib/api.ts';
	import { consoleState } from '#lib/console.svelte.ts';

	let token = $state('');
	let problem = $state<Problem | null>(null);
	let formError = $state<string | null>(null);
	let submitting = $state(false);

	const next = $derived(safeNext(page.url.searchParams.get('next')));
	const tokenError = $derived(reasonFor(problem, '/token'));

	$effect(() => {
		if (consoleState.auth === 'in') void goto(next, { replaceState: true });
	});

	async function submit(event: SubmitEvent) {
		event.preventDefault();
		problem = null;
		formError = null;
		submitting = true;
		const value = token;
		try {
			const result = await api.POST('/auth/login', {
				body: { token: value },
				headers: { 'Content-Type': 'application/json' }
			});
			if (result.response.status === 204) {
				token = '';
				await consoleState.loadMe();
				if (consoleState.auth === 'in') await goto(next, { replaceState: true });
				else formError = consoleState.meError ?? 'Login did not start a session';
				return;
			}
			if (isProblem(result.error)) {
				problem = result.error;
				if (!reasonFor(result.error, '/token')) {
					formError = result.error.detail || result.error.title;
				}
				return;
			}
			const failure = failureOf(result.error, result.response);
			formError = failure?.message ?? `HTTP ${result.response.status}`;
		} catch {
			formError = 'API unreachable';
		} finally {
			submitting = false;
		}
	}
</script>

<svelte:head>
	<title>Sign in · mftiko</title>
</svelte:head>

<main class="card">
	<h1>Sign in</h1>
	<p class="status">The token is exchanged for a session cookie. It is not stored in localStorage.</p>
	{#if consoleState.auth === 'in' || (consoleState.auth === 'unknown' && !consoleState.meError)}
		<p class="status">Loading…</p>
	{:else}
		<form onsubmit={submit}>
			<Field label="Token" name="token" type="password" bind:value={token} error={tokenError} />
			{#if formError}
				<p class="status error" role="alert">{formError}</p>
			{/if}
			{#if consoleState.meError && !formError}
				<p class="status error" role="alert">{consoleState.meError}</p>
			{/if}
			<button class="primary" type="submit" disabled={submitting}>
				{submitting ? 'Signing in…' : 'Sign in'}
			</button>
		</form>
	{/if}
</main>

<style>
	.card {
		max-width: 22rem;
		margin: 4rem auto;
		padding: 1.5rem;
		background: var(--surface);
		border: 1px solid var(--border);
		border-radius: var(--radius);
	}

	form {
		display: flex;
		flex-direction: column;
		gap: 0.75rem;
		margin-top: 1rem;
	}

	button {
		align-self: flex-start;
	}
</style>
