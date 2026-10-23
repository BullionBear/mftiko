<script lang="ts">
	import FeedStatus from '#lib/FeedStatus.svelte';
	import { InstanceFeed } from '#lib/live.svelte.ts';
	import { onMount } from 'svelte';

	const feed = new InstanceFeed('md');

	onMount(() => {
		const stop = feed.watch();
		void feed.reload();
		return () => {
			stop();
			feed.cancel();
		};
	});
</script>

<svelte:head>
	<title>Market · mftiko</title>
</svelte:head>

<h1>Market</h1>
<p class="status">MD instances.</p>
<FeedStatus loading={feed.loading} error={feed.error} empty={feed.items === null} onretry={() => feed.reload()} />

{#if feed.items && feed.items.length === 0 && !feed.loading}
	<p class="status">None.</p>
{:else if feed.items}
	<table class="data">
		<thead>
			<tr>
				<th>Name</th>
				<th>Region</th>
				<th>Availability</th>
				<th>Agent</th>
			</tr>
		</thead>
		<tbody>
			{#each feed.items as instance (instance.name)}
				<tr>
					<td>{instance.name}</td>
					<td>{instance.region}</td>
					<td>{instance.region_available ? 'up' : 'unavailable'}</td>
					<td>
						{#if instance.agent}
							{instance.agent.release} · epoch {instance.agent.epoch}
						{:else}
							no agent
						{/if}
					</td>
				</tr>
			{/each}
		</tbody>
	</table>
{/if}
