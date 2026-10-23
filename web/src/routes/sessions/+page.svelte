<script lang="ts">
	import FeedStatus from '#lib/FeedStatus.svelte';
	import { SessionFeed } from '#lib/live.svelte.ts';
	import { onMount } from 'svelte';

	const feed = new SessionFeed();

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
	<title>Sessions · mftiko</title>
</svelte:head>

<h1>Sessions</h1>
<FeedStatus loading={feed.loading} error={feed.error} empty={feed.items === null} onretry={() => feed.reload()} />

{#if feed.items && feed.items.length === 0 && !feed.loading}
	<p class="status">None.</p>
{:else if feed.items}
	<table class="data">
		<thead>
			<tr>
				<th>Id</th>
				<th>Name</th>
				<th>Phase</th>
				<th>Desired</th>
				<th>STS</th>
				<th>Region</th>
				<th>Strategy</th>
			</tr>
		</thead>
		<tbody>
			{#each feed.items as session (session.id)}
				<tr>
					<td><a class="mono" href="/sessions/{session.id}">{session.id}</a></td>
					<td>{session.spec.name}</td>
					<td>{session.status?.phase ?? '—'}</td>
					<td>{session.spec.desired_state}</td>
					<td>{session.sts_instance}</td>
					<td>{session.region}</td>
					<td class="mono" title={session.spec.strategy.digest}>{session.spec.strategy.name}</td>
				</tr>
			{/each}
		</tbody>
	</table>
{/if}
