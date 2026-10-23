<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import FeedStatus from '#lib/FeedStatus.svelte';
	import { api, collectPages, failureOf, type StrategyTag, type StrategyVersion } from '#lib/api.ts';

	const id = $derived(page.params.id ?? '');
	const wantRef = $derived(page.url.searchParams.get('ref'));

	let versions = $state<StrategyTag[] | null>(null);
	let manifest = $state<StrategyVersion | null>(null);
	let loadingVersions = $state(false);
	let loadingManifest = $state(false);
	let error = $state<string | null>(null);
	let manifestError = $state<string | null>(null);
	let gen = 0;
	let cachedId = '';
	let cachedTags: StrategyTag[] | null = null;
	let reloadNonce = $state(0);

	function reload() {
		cachedId = '';
		cachedTags = null;
		reloadNonce += 1;
	}

	function href(strategyId: string, ref: string): string {
		return `/strategies/${encodeURIComponent(strategyId)}?ref=${encodeURIComponent(ref)}`;
	}

	function choose(items: StrategyTag[], want: string | null): string | null {
		if (want) return want;
		if (items.length === 0) return null;
		const latest = items.find((item) => item.tag === 'latest');
		return (latest ?? items[0]).tag;
	}

	function selected(tag: StrategyTag, ref: string | null): boolean {
		if (!ref) return false;
		return tag.tag === ref || tag.digest === ref;
	}

	function shortDigest(digest: string): string {
		return digest.length > 19 ? `${digest.slice(0, 19)}…` : digest;
	}

	async function loadManifest(strategyId: string, ref: string, mine: number): Promise<void> {
		loadingManifest = true;
		manifestError = null;
		try {
			const result = await api.GET('/strategies/{strategy_id}/versions/{ref}', {
				params: { path: { strategy_id: strategyId, ref } }
			});
			if (mine !== gen) return;
			loadingManifest = false;
			if (result.response.status === 401) return;
			const failure = failureOf(result.error, result.response);
			if (failure || !result.data) {
				manifest = null;
				manifestError = failure?.message ?? 'Empty response';
				return;
			}
			manifest = result.data;
		} catch {
			if (mine !== gen) return;
			loadingManifest = false;
			manifest = null;
			manifestError = 'API unreachable';
		}
	}

	$effect(() => {
		const strategyId = id;
		const want = wantRef;
		const nonce = reloadNonce;
		if (!strategyId) return;
		void nonce;
		const mine = ++gen;
		loadingVersions = cachedId !== strategyId || cachedTags === null;
		if (cachedId !== strategyId) {
			versions = null;
			manifest = null;
			error = null;
		}
		void (async () => {
			let tags = cachedId === strategyId ? cachedTags : null;
			if (!tags) {
				const result = await collectPages<StrategyTag>((cursor) =>
					api.GET('/strategies/{strategy_id}/versions', {
						params: {
							path: { strategy_id: strategyId },
							query: cursor ? { limit: 100, cursor } : { limit: 100 }
						}
					})
				);
				if (mine !== gen) return;
				loadingVersions = false;
				if (result.failure) {
					if (result.failure.status !== 401) error = result.failure.message;
					versions = null;
					return;
				}
				tags = result.items;
				cachedId = strategyId;
				cachedTags = tags;
			} else {
				loadingVersions = false;
			}
			if (mine !== gen) return;
			versions = tags;
			error = null;
			if (!tags) return;
			const ref = choose(tags, want);
			if (!ref) {
				manifest = null;
				return;
			}
			if (want !== ref) {
				await goto(href(strategyId, ref), { replace: true, reset: false });
				return;
			}
			await loadManifest(strategyId, ref, mine);
		})();
		return () => {
			gen += 1;
		};
	});
</script>

<svelte:head>
	<title>{id} · Strategies · mftiko</title>
</svelte:head>

<p><a href="/strategies">Strategies</a></p>
<h1>{id}</h1>
<FeedStatus loading={loadingVersions} error={error} empty={versions === null} onretry={reload} />

{#if versions && versions.length === 0 && !loadingVersions}
	<p class="status">No versions.</p>
{:else if versions}
	<h2>Versions</h2>
	<table class="data">
		<thead>
			<tr>
				<th>Tag</th>
				<th>Digest</th>
			</tr>
		</thead>
		<tbody>
			{#each versions as version (version.tag)}
				<tr>
					<td>
						<a
							href={href(id, version.tag)}
							aria-current={selected(version, wantRef) ? 'page' : undefined}
						>
							{version.tag}
						</a>
					</td>
					<td class="mono" title={version.digest}>{shortDigest(version.digest)}</td>
				</tr>
			{/each}
		</tbody>
	</table>
{/if}

{#if manifestError}
	<p class="status error" role="alert">{manifestError}</p>
{:else if loadingManifest && !manifest}
	<p class="status">Loading manifest…</p>
{/if}

{#if manifest}
	<h2>Manifest</h2>
	<dl class="facts">
		<dt>Name</dt>
		<dd>{manifest.name}</dd>
		<dt>Repository</dt>
		<dd class="mono">{manifest.repository}</dd>
		<dt>Digest</dt>
		<dd class="mono">{manifest.digest}</dd>
		<dt>Tags</dt>
		<dd>{manifest.tags.length === 0 ? '—' : manifest.tags.join(', ')}</dd>
		<dt>Runtime</dt>
		<dd>{manifest.runtime}</dd>
		<dt>OS</dt>
		<dd>{manifest.os}</dd>
		<dt>Arch</dt>
		<dd>{manifest.arch}</dd>
		<dt>SDK</dt>
		<dd>{manifest.sdk || '—'}</dd>
		<dt>Contracts</dt>
		<dd>{manifest.contracts.length === 0 ? '—' : manifest.contracts.join(', ')}</dd>
		<dt>Size</dt>
		<dd>{manifest.size_bytes ?? '—'}</dd>
	</dl>
{/if}
