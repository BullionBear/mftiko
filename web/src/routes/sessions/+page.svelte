<script lang="ts">
	import { openPageStream } from '#lib/events.svelte.ts';
	import FeedStatus from '#lib/FeedStatus.svelte';
	import { SessionFeed } from '#lib/live.svelte.ts';
	import { onMount } from 'svelte';

	const feed = new SessionFeed();

	function accountLabel(session: { spec: { accounts: { name: string; api_id: number }[] } }): string {
		if (session.spec.accounts.length === 0) return '—';
		return session.spec.accounts.map((account) => `${account.name} (${account.api_id})`).join(', ');
	}

	onMount(() => {
		const stop = feed.watch();
		const stopStream = openPageStream(['sessions']);
		void feed.reload();
		return () => {
			stopStream();
			stop();
			feed.cancel();
		};
	});
</script>

<style>
	.heading {
		display: flex;
		flex-wrap: wrap;
		gap: 0.75rem 1.25rem;
		align-items: baseline;
		justify-content: space-between;
	}
</style>

<svelte:head>
	<title>Sessions · mftiko</title>
</svelte:head>

<div class="heading">
	<h1>Sessions</h1>
	<a href="/sessions/new">Deploy session</a>
</div>
<FeedStatus loading={feed.loading} error={feed.error} empty={feed.items === null} onretry={() => feed.reload()} />

{#if feed.items && feed.items.length === 0 && !feed.loading}
	<p class="status">None.</p>
{:else if feed.items}
	<table class="data">
		<thead>
			<tr>
				<th>Session</th>
				<th>Status</th>
				<th>Strategy</th>
				<th>Account</th>
				<th>Creator</th>
				<th>Time</th>
			</tr>
		</thead>
		<tbody>
			{#each feed.items as session (session.id)}
				<tr>
					<td>
						<a class="mono" href="/sessions/{session.id}">{session.id}</a>
						<div>{session.spec.name}</div>
					</td>
					<td>{session.status?.phase ?? '—'}</td>
					<td class="mono" title={session.spec.strategy.digest}>{session.spec.strategy.name}</td>
					<td>{accountLabel(session)}</td>
					<td>{session.created_by ?? '—'}</td>
					<td>{session.created_at ?? '—'}</td>
				</tr>
			{/each}
		</tbody>
	</table>
{/if}
