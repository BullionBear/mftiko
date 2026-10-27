<script lang="ts">
	import { api, collectPages, type StrategyTag } from '#lib/api.ts';

	let repository = $state('');
	let shown = $state('');
	let items = $state<StrategyTag[] | null>(null);
	let latest = $state<StrategyTag | null>(null);
	let loading = $state(false);
	let error = $state<string | null>(null);

	async function lookup(): Promise<void> {
		const repo = repository.trim();
		if (repo === '') {
			error = 'Repository is required';
			items = null;
			latest = null;
			shown = '';
			return;
		}
		loading = true;
		error = null;
		const result = await collectPages<StrategyTag>((cursor) =>
			api.GET('/strategies', {
				params: {
					query: cursor ? { repository: repo, limit: 100, cursor } : { repository: repo, limit: 100 }
				}
			})
		);
		loading = false;
		if (result.failure) {
			items = null;
			latest = null;
			shown = '';
			if (result.failure.status !== 401) error = result.failure.message;
			return;
		}
		items = result.items;
		latest = result.latest ?? null;
		shown = result.repository ?? repo;
	}

	function marked(tag: StrategyTag): boolean {
		return latest !== null && tag.digest === latest.digest;
	}
</script>

<svelte:head>
	<title>Strategies · mftiko</title>
</svelte:head>

<h1>Strategies</h1>
<form
	onsubmit={(event) => {
		event.preventDefault();
		void lookup();
	}}
>
	<div class="field">
		<label for="repository">Repository</label>
		<input
			id="repository"
			name="repository"
			bind:value={repository}
			autocomplete="off"
			autocapitalize="off"
			spellcheck="false"
			placeholder="ghcr.io/acme/md_watch"
		/>
	</div>
	<button class="primary" type="submit" disabled={loading}>Look up</button>
</form>

{#if error}
	<p class="status">{error}</p>
{:else if loading}
	<p class="status">Loading…</p>
{:else if items && items.length === 0}
	<p class="status">No tags.</p>
{:else if items}
	<p class="status mono">{shown}</p>
	{#if latest}
		<p class="status">Latest <span class="mono">{latest.tag}</span> <span class="mono">{latest.digest}</span></p>
	{/if}
	<table class="data">
		<thead>
			<tr>
				<th>Tag</th>
				<th>Digest</th>
				<th>Latest</th>
			</tr>
		</thead>
		<tbody>
			{#each items as tag (tag.tag)}
				<tr>
					<td>{tag.tag}</td>
					<td class="mono">{tag.digest}</td>
					<td>{marked(tag) ? 'latest' : ''}</td>
				</tr>
			{/each}
		</tbody>
	</table>
{/if}
