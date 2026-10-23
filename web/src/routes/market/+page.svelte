<script lang="ts">
	import { openPageStream } from '#lib/events.svelte.ts';
	import FeedStatus from '#lib/FeedStatus.svelte';
	import type { DemandStatus } from '#lib/api.ts';
	import { InstanceFeed, type InstanceRow } from '#lib/live.svelte.ts';
	import { onMount } from 'svelte';

	const feed = new InstanceFeed('md', true);

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

	function feedsOf(instance: InstanceRow): DemandStatus[] | null {
		if (instance.demands === undefined) return null;
		return instance.demands.filter((demand) => demand.kind === 'feed');
	}
</script>

<svelte:head>
	<title>Market · mftiko</title>
</svelte:head>

<h1>Market</h1>
<p class="status">MD instances and the status of each feed.</p>
<FeedStatus loading={feed.loading} error={feed.error} empty={feed.items === null} onretry={() => feed.reload()} />

{#if feed.items && feed.items.length === 0 && !feed.loading}
	<p class="status">None.</p>
{:else if feed.items}
	{#each feed.items as instance (instance.name)}
		{@const rows = feedsOf(instance)}
		<section class="card">
			<h2>{instance.name}</h2>
			<dl class="facts">
				<dt>Region</dt>
				<dd>{instance.region}</dd>
				<dt>Availability</dt>
				<dd>{instance.region_available ? 'up' : 'unavailable'}</dd>
			</dl>
			{#if !instance.region_available}
				<p class="status">Region unavailable.</p>
			{:else if rows === null}
				<p class="status">Feed status unavailable.</p>
			{:else if rows.length === 0}
				<p class="status">No feeds.</p>
			{:else}
				<table class="data">
					<thead>
						<tr>
							<th>Feed</th>
							<th>State</th>
							<th>Reason</th>
							<th>Session</th>
						</tr>
					</thead>
					<tbody>
						{#each rows as demand (`${demand.session_id}:${demand.feed ?? ''}`)}
							<tr>
								<td class="mono">{demand.feed ?? '—'}</td>
								<td class:bad={demand.state === 'rejected' || demand.state === 'dead'}>{demand.state}</td>
								<td>{demand.reason || '—'}</td>
								<td><a class="mono" href="/sessions/{demand.session_id}">{demand.session_id}</a></td>
							</tr>
						{/each}
					</tbody>
				</table>
			{/if}
		</section>
	{/each}
{/if}

<style>
	.card {
		margin: 0.75rem 0 1.25rem;
	}

	.card h2 {
		margin-bottom: 0.4rem;
	}

	.bad {
		color: var(--danger);
	}

	.mono {
		word-break: break-all;
	}
</style>
