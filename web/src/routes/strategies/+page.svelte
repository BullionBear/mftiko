<script lang="ts">
	import FeedStatus from '#lib/FeedStatus.svelte';
	import { StrategyFeed } from '#lib/live.svelte.ts';
	import { onMount } from 'svelte';

	const feed = new StrategyFeed();

	onMount(() => {
		void feed.reload();
		return () => feed.cancel();
	});

	function shortDigest(digest: string): string {
		return digest.length > 19 ? `${digest.slice(0, 19)}…` : digest;
	}
</script>

<svelte:head>
	<title>Strategies · mftiko</title>
</svelte:head>

<h1>Strategies</h1>
<FeedStatus
	loading={feed.loading}
	error={feed.error}
	empty={feed.items === null}
	onretry={() => feed.reload()}
/>

{#if feed.items && feed.items.length === 0 && !feed.loading}
	<p class="status">None.</p>
{:else if feed.items}
	<table class="data">
		<thead>
			<tr>
				<th>Id</th>
				<th>Repository</th>
				<th>Latest tag</th>
				<th>Digest</th>
			</tr>
		</thead>
		<tbody>
			{#each feed.items as strategy (strategy.id)}
				<tr>
					<td>{strategy.id}</td>
					<td class="mono">{strategy.repository}</td>
					<td>{strategy.latest?.tag ?? '—'}</td>
					<td class="mono" title={strategy.latest?.digest ?? ''}>
						{strategy.latest ? shortDigest(strategy.latest.digest) : '—'}
					</td>
				</tr>
			{/each}
		</tbody>
	</table>
{/if}
