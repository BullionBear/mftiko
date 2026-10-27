<script lang="ts">
	import {
		LOG_GAP_TEXT,
		LOG_RETRY_TEXT,
		LOG_TAILS,
		LOG_UNAVAILABLE_TEXT,
		sessionLogsExportURL
	} from '#lib/logtext.ts';
	import { SessionLogView } from '#lib/logview.svelte.ts';

	let { sessionId }: { sessionId: string } = $props();

	const logs = new SessionLogView();
	const pane = $derived(logs.stream === 'stdout' ? logs.stdout : logs.stderr);

	let box: HTMLDivElement | undefined = $state();
	let stick = true;

	// Reconnect only when the session, tail, follow switch, or retry changes.
	// Reading the panes here would reopen the stream on every line.
	$effect(() => {
		const id = sessionId;
		const tail = logs.tail;
		const follow = logs.follow;
		const attempt = logs.attempt;
		if (id === '') return;
		stick = true;
		return logs.connect(id, tail, follow, attempt);
	});

	function select(stream: 'stdout' | 'stderr'): void {
		logs.select(stream);
		stick = true;
	}

	function onScroll(): void {
		if (!box) return;
		stick = box.scrollHeight - box.scrollTop - box.clientHeight < 48;
	}

	$effect(() => {
		pane.rows.length;
		pane.partial;
		const el = box;
		if (!el || !stick) return;
		el.scrollTop = el.scrollHeight;
	});
</script>

<div class="tools">
	<div role="tablist" aria-label="Log stream">
		<button
			type="button"
			role="tab"
			id="log-tab-stdout"
			aria-selected={logs.stream === 'stdout'}
			aria-controls="log-panel"
			onclick={() => select('stdout')}>stdout</button
		>
		<button
			type="button"
			role="tab"
			id="log-tab-stderr"
			aria-selected={logs.stream === 'stderr'}
			aria-controls="log-panel"
			onclick={() => select('stderr')}>stderr</button
		>
	</div>
	<label for="log-tail">Lines</label>
	<select
		id="log-tail"
		value={String(logs.tail)}
		onchange={(event) => {
			const el = event.currentTarget;
			if (el instanceof HTMLSelectElement) logs.setTail(el.value);
		}}
	>
		{#each LOG_TAILS as n (n)}
			<option value={String(n)}>{n}</option>
		{/each}
	</select>
	<label class="check" for="log-follow">
		<input
			id="log-follow"
			type="checkbox"
			checked={logs.follow}
			onchange={(event) => {
				const el = event.currentTarget;
				if (el instanceof HTMLInputElement) logs.setFollow(el.checked);
			}}
		/>
		Follow
	</label>
	<a href={sessionLogsExportURL(sessionId)}>Download</a>
</div>

{#if logs.unavailable}
	<p class="status error" lang="zh-Hant" role="alert">
		{LOG_UNAVAILABLE_TEXT}
		<button type="button" onclick={() => logs.retry()}>{LOG_RETRY_TEXT}</button>
	</p>
{/if}
{#if logs.endText}
	<p class="status" lang="zh-Hant" role="status">{logs.endText}</p>
{/if}

<div
	class="log"
	id="log-panel"
	role="tabpanel"
	aria-labelledby={logs.stream === 'stdout' ? 'log-tab-stdout' : 'log-tab-stderr'}
	data-stream={logs.stream}
	tabindex="0"
	bind:this={box}
	onscroll={onScroll}
>
	{#each pane.rows as row (row.id)}
		{#if row.kind === 'gap'}
			<div class="gap">{LOG_GAP_TEXT}</div>
		{:else}
			<div class="line">{row.text}</div>
		{/if}
	{/each}
	{#if pane.partial !== ''}
		<div class="line">{pane.partial}</div>
	{/if}
</div>

<style>
	.tools {
		display: flex;
		flex-wrap: wrap;
		gap: 0.75rem 1rem;
		align-items: center;
		margin: 0.75rem 0 0.5rem;
	}

	.tools [role='tablist'] {
		display: flex;
		gap: 0.25rem;
	}

	.tools button[aria-selected='true'] {
		background: var(--accent);
		color: var(--accent-fg);
		border-color: transparent;
	}

	.tools select {
		font: inherit;
		color: var(--fg);
		background: var(--surface);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		padding: 0.3rem 0.4rem;
	}

	.check {
		display: inline-flex;
		gap: 0.4rem;
		align-items: center;
	}

	p button {
		margin-left: 0.5rem;
	}

	.log {
		max-height: 24rem;
		min-height: 8rem;
		overflow: auto;
		margin: 0.5rem 0 0;
		padding: 0.6rem 0.7rem;
		background: var(--surface);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		font-family: var(--mono);
		font-size: 0.8rem;
		line-height: 1.35;
	}

	.line {
		white-space: pre-wrap;
		word-break: break-all;
		min-height: 1.35em;
	}

	.gap {
		color: var(--warning-fg);
		min-height: 1.35em;
	}
</style>
