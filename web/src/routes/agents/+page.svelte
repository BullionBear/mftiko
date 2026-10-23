<script lang="ts">
	import { openPageStream } from '#lib/events.svelte.ts';
	import FeedStatus from '#lib/FeedStatus.svelte';
	import type { AgentStatus } from '#lib/api.ts';
	import { InstanceFeed, type InstanceRow } from '#lib/live.svelte.ts';
	import { onMount } from 'svelte';

	const feed = new InstanceFeed(undefined, true);
	const planeOrder = ['sts', 'md', 'td'] as const;

	onMount(() => {
		const stop = feed.watch();
		const stopStream = openPageStream(['instances']);
		void feed.reload();
		return () => {
			stopStream();
			stop();
			feed.cancel();
		};
	});

	const grouped = $derived.by(() => {
		const groups: Record<(typeof planeOrder)[number], InstanceRow[]> = { sts: [], md: [], td: [] };
		for (const instance of feed.items ?? []) groups[instance.plane].push(instance);
		return groups;
	});
</script>

{#snippet agentFields(agent: AgentStatus)}
	<dl class="facts">
		<dt>Epoch</dt>
		<dd>{agent.epoch}</dd>
		<dt>Release</dt>
		<dd>{agent.release}</dd>
		<dt>Started</dt>
		<dd>{agent.started_at}</dd>
		<dt>Host</dt>
		<dd class="mono">{agent.host ?? '—'}</dd>
		<dt>Work dir</dt>
		<dd class="mono">{agent.work_dir ?? '—'}</dd>
		{#if agent.last_known}
			<dt>Record</dt>
			<dd class="mark">last known</dd>
		{/if}
	</dl>
{/snippet}

<svelte:head>
	<title>Agents · mftiko</title>
</svelte:head>

<h1>Agents</h1>
<FeedStatus loading={feed.loading} error={feed.error} empty={feed.items === null} onretry={() => feed.reload()} />

{#if feed.controller}
	<h2>Controller</h2>
	<section class="card">
		<dl class="facts">
			<dt>Present</dt>
			<dd>{feed.controller.present ? 'yes' : 'no'}</dd>
			<dt>Home region</dt>
			<dd>{feed.controller.home_region}</dd>
		</dl>
		<h3>Agent</h3>
		{#if feed.controller.agent}
			{@render agentFields(feed.controller.agent)}
		{:else}
			<p class="status">No agent.</p>
		{/if}
	</section>
{/if}

{#if feed.items && feed.items.length === 0 && !feed.loading}
	<p class="status">None.</p>
{:else if feed.items}
	{#each planeOrder as plane (plane)}
		{#if grouped[plane].length > 0}
			<h2>{plane}</h2>
			{#each grouped[plane] as instance (instance.name)}
				<section class="card">
					<h3>{instance.name}</h3>
					<dl class="facts">
						<dt>Region</dt>
						<dd>{instance.region}</dd>
						<dt>Availability</dt>
						<dd>{instance.region_available ? 'up' : 'unavailable'}</dd>
					</dl>
					<h3>Agent</h3>
					{#if instance.agent}
						{@render agentFields(instance.agent)}
					{:else}
						<p class="status">No agent.</p>
					{/if}
					<h3>Workers</h3>
					{#if !instance.region_available}
						<p class="status">Region unavailable.</p>
					{:else if instance.workers === undefined}
						<p class="status">Workers unavailable.</p>
					{:else if instance.workers.length === 0}
						<p class="status">No workers.</p>
					{:else}
						<table class="data">
							<thead>
								<tr>
									<th>Key</th>
									<th>Phase</th>
									<th>Incarnation</th>
									<th>Restarts</th>
									<th>Stage</th>
								</tr>
							</thead>
							<tbody>
								{#each instance.workers as worker (`${worker.key}:${worker.incarnation}`)}
									<tr>
										<td class="mono">{worker.key}</td>
										<td>{worker.phase}</td>
										<td>{worker.incarnation}</td>
										<td>{worker.restarts}</td>
										<td>{worker.stage ?? '—'}</td>
									</tr>
								{/each}
							</tbody>
						</table>
					{/if}
				</section>
			{/each}
		{/if}
	{/each}
{/if}

<style>
	.card {
		margin: 0.75rem 0 1.25rem;
		padding: 0.75rem;
		background: var(--surface);
		border: 1px solid var(--border);
		border-radius: var(--radius);
	}

	.card h3 {
		margin: 0.85rem 0 0.4rem;
		font-size: 1rem;
		font-weight: 600;
	}

	.card h3:first-of-type {
		margin-top: 0.35rem;
	}

	.mark {
		color: var(--warning-fg);
	}

	.mono {
		word-break: break-all;
	}
</style>
