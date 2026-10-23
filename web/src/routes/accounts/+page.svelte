<script lang="ts">
	import FeedStatus from '#lib/FeedStatus.svelte';
	import { AccountFeed } from '#lib/live.svelte.ts';
	import { onMount } from 'svelte';

	const feed = new AccountFeed();

	onMount(() => {
		void feed.reload();
		return () => feed.cancel();
	});
</script>

<svelte:head>
	<title>Accounts · mftiko</title>
</svelte:head>

<div class="heading">
	<h1>Accounts</h1>
	<a href="/accounts/new">New account</a>
</div>
<FeedStatus loading={feed.loading} error={feed.error} empty={feed.items === null} onretry={() => feed.reload()} />

{#if feed.items && feed.items.length === 0 && !feed.loading}
	<p class="status">None.</p>
{:else if feed.items}
	<table class="data">
		<thead>
			<tr>
				<th>api_id</th>
				<th>Name</th>
				<th>Kind</th>
				<th>TD</th>
				<th>Region</th>
				<th>Referenced by</th>
			</tr>
		</thead>
		<tbody>
			{#each feed.items as account (account.api_id)}
				<tr>
					<td class="mono"><a href="/accounts/{account.api_id}">{account.api_id}</a></td>
					<td>{account.name}</td>
					<td>{account.kind}</td>
					<td>{account.td_instance}</td>
					<td>{account.region}</td>
					<td>
						{#if account.referenced_by.length === 0}
							—
						{:else}
							{#each account.referenced_by as id (id)}
								<a class="mono" href="/sessions/{id}">{id}</a>{' '}
							{/each}
						{/if}
					</td>
				</tr>
			{/each}
		</tbody>
	</table>
{/if}

<style>
	.heading {
		display: flex;
		flex-wrap: wrap;
		gap: 0.75rem 1.25rem;
		align-items: baseline;
		justify-content: space-between;
	}
</style>
