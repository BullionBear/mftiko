<script lang="ts">
	import { page } from '$app/state';
	import CopyButton from '#lib/CopyButton.svelte';
	import FeedStatus from '#lib/FeedStatus.svelte';
	import type { Dependency, SessionDetail } from '#lib/api.ts';
	import { SessionDetailFeed } from '#lib/live.svelte.ts';
	import { openPageStream } from '#lib/events.svelte.ts';

	const detail = new SessionDetailFeed();
	const id = $derived(page.params.id ?? '');

	let confirmEnd = $state(false);
	let ending = $state(false);
	let endError = $state<string | null>(null);
	let loadedFor = '';

	$effect(() => {
		const sessionId = id;
		if (!sessionId) return;
		if (loadedFor !== sessionId) {
			loadedFor = sessionId;
			confirmEnd = false;
			ending = false;
			endError = null;
		}
		void detail.load(sessionId);
		const stopWatch = detail.watch(sessionId);
		// sessions for this id, plus instances so a last-known STS agent is marked on the logs (E10-D105, E10-D107).
		const stopStream = openPageStream(['sessions', 'instances'], sessionId);
		return () => {
			stopStream();
			stopWatch();
			detail.cancel();
		};
	});

	function canEnd(session: SessionDetail): boolean {
		if (session.spec.desired_state === 'ended') return false;
		const phase = session.status?.phase;
		return phase !== 'done' && phase !== 'failed';
	}

	function dependencyName(dep: Dependency): string {
		if (dep.kind === 'feed') return dep.feed ?? '—';
		if (dep.api_id !== undefined) return String(dep.api_id);
		return '—';
	}

	async function runEnd(): Promise<void> {
		const sessionId = id;
		if (!sessionId) return;
		ending = true;
		endError = null;
		const message = await detail.end(sessionId);
		ending = false;
		if (message) {
			endError = message;
			return;
		}
		confirmEnd = false;
	}
</script>

<svelte:head>
	<title>Session {id} · mftiko</title>
</svelte:head>

<p><a href="/sessions">Sessions</a></p>
<div class="heading">
	<h1>Session <span class="mono">{id}</span></h1>
	{#if detail.session && canEnd(detail.session)}
		{#if confirmEnd}
			<div class="confirm" role="group" aria-label="Confirm end">
				<p>End this session?</p>
				<div class="actions">
					<button type="button" onclick={() => void runEnd()} disabled={ending}>
						{ending ? 'Ending…' : 'End'}
					</button>
					<button type="button" onclick={() => (confirmEnd = false)} disabled={ending}>Cancel</button>
				</div>
			</div>
		{:else}
			<button type="button" onclick={() => (confirmEnd = true)} disabled={ending}>End session</button>
		{/if}
	{/if}
</div>

{#if endError}
	<p class="status error" role="alert">{endError}</p>
{/if}

<FeedStatus
	loading={detail.loading}
	error={detail.error}
	empty={detail.session === null && !detail.missing}
	onretry={() => detail.load(id)}
/>

{#if detail.missing}
	<p class="status" role="status">Not found.</p>
{:else if detail.session}
	<dl class="facts">
		<dt>Name</dt>
		<dd>{detail.session.spec.name}</dd>
		<dt>Phase</dt>
		<dd>{detail.session.status?.phase ?? '—'}</dd>
		<dt>Incarnation</dt>
		<dd>{detail.session.status?.incarnation ?? '—'}</dd>
		<dt>Desired</dt>
		<dd>{detail.session.spec.desired_state}</dd>
		<dt>End reason</dt>
		<dd>{detail.session.assignment?.end_reason ?? '—'}</dd>
		<dt>Status reason</dt>
		<dd>{detail.session.status?.reason || '—'}</dd>
		<dt>STS</dt>
		<dd>{detail.session.sts_instance}</dd>
		<dt>Region</dt>
		<dd>{detail.session.region}</dd>
		<dt>Revision</dt>
		<dd>{detail.session.revision}</dd>
		<dt>Strategy</dt>
		<dd class="mono">{detail.session.spec.strategy.digest}</dd>
	</dl>

	<h2>Dependencies</h2>
	{#if detail.session.dependencies.length === 0}
		<p class="status">None.</p>
	{:else}
		<table class="data">
			<thead>
				<tr>
					<th>Kind</th>
					<th>Item</th>
					<th>Provider</th>
					<th>State</th>
					<th>Reason</th>
				</tr>
			</thead>
			<tbody>
				{#each detail.session.dependencies as dep (`${dep.kind}:${dep.feed ?? ''}:${dep.api_id ?? ''}:${dep.provider_instance}`)}
					<tr>
						<td>{dep.kind}</td>
						<td class="mono">{dependencyName(dep)}</td>
						<td>{dep.provider_instance}</td>
						<td class:bad={dep.state === 'rejected' || dep.state === 'dead'}>{dep.state}</td>
						<td>{dep.reason || '—'}</td>
					</tr>
				{/each}
			</tbody>
		</table>
	{/if}

	<h2>Logs</h2>
	{#if detail.session.logs}
		{#if detail.stsAgent?.last_known}
			<p class="note">Last known agent record.</p>
		{/if}
		<dl class="facts">
			<dt>Host</dt>
			<dd class="path">
				<span class="mono">{detail.session.logs.host}</span>
				<CopyButton value={detail.session.logs.host} label="Copy log host" />
			</dd>
			<dt>stdout</dt>
			<dd class="path">
				<span class="mono">{detail.session.logs.stdout}</span>
				<CopyButton value={detail.session.logs.stdout} label="Copy stdout path" />
			</dd>
			<dt>stderr</dt>
			<dd class="path">
				<span class="mono">{detail.session.logs.stderr}</span>
				<CopyButton value={detail.session.logs.stderr} label="Copy stderr path" />
			</dd>
		</dl>
	{:else}
		<p class="status">Not available.</p>
	{/if}
	{#if detail.agentError}
		<p class="status error" role="alert">{detail.agentError}</p>
	{/if}

	<h2>state_out</h2>
	{#if detail.session.status?.state_out}
		<p class="path">
			<span class="mono">{detail.session.status.state_out.digest}</span>
			<CopyButton value={detail.session.status.state_out.digest} label="Copy state_out digest" />
		</p>
	{:else}
		<p class="status">—</p>
	{/if}
{/if}

<style>
	.heading {
		display: flex;
		flex-wrap: wrap;
		gap: 0.75rem 1.25rem;
		align-items: flex-start;
		justify-content: space-between;
	}

	.actions {
		display: flex;
		gap: 0.5rem;
	}

	.path {
		display: flex;
		flex-wrap: wrap;
		gap: 0.35rem 0.6rem;
		align-items: center;
	}

	.path .mono {
		word-break: break-all;
	}

	.note {
		margin: 0 0 0.5rem;
		color: var(--warning-fg);
	}

	.bad {
		color: var(--danger);
	}
</style>
