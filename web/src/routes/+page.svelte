<script lang="ts">
	import FeedStatus from '#lib/FeedStatus.svelte';
	import { InstanceFeed, SessionFeed } from '#lib/live.svelte.ts';
	import { onMount } from 'svelte';

	const sessions = new SessionFeed();
	const instances = new InstanceFeed();

	onMount(() => {
		const stopSessions = sessions.watch();
		const stopInstances = instances.watch();
		void sessions.reload();
		void instances.reload();
		return () => {
			stopSessions();
			stopInstances();
			sessions.cancel();
			instances.cancel();
		};
	});

	const planes = $derived.by(() => {
		const counts = {
			sts: { n: 0, up: 0 },
			md: { n: 0, up: 0 },
			td: { n: 0, up: 0 }
		};
		for (const instance of instances.items ?? []) {
			counts[instance.plane].n += 1;
			if (instance.region_available) counts[instance.plane].up += 1;
		}
		return counts;
	});

	const failing = $derived(
		(sessions.items ?? []).filter(
			(session) =>
				session.status?.phase === 'failed' ||
				session.dependencies.some((dependency) => dependency.state === 'rejected' || dependency.state === 'dead')
		)
	);

	function reload() {
		void sessions.reload();
		void instances.reload();
	}
</script>

<svelte:head>
	<title>Overview · mftiko</title>
</svelte:head>

<h1>Overview</h1>
<FeedStatus
	loading={sessions.loading || instances.loading}
	error={sessions.error ?? instances.error}
	empty={sessions.items === null || instances.items === null}
	onretry={reload}
/>

{#if instances.controller}
	<h2>Controller</h2>
	<dl class="facts">
		<dt>Present</dt>
		<dd>{instances.controller.present ? 'yes' : 'no'}</dd>
		<dt>Home region</dt>
		<dd>{instances.controller.home_region}</dd>
		<dt>Release</dt>
		<dd>{instances.controller.agent?.release ?? '—'}</dd>
	</dl>
{/if}

{#if instances.items}
	<h2>Planes</h2>
	<table class="data">
		<thead>
			<tr>
				<th>Plane</th>
				<th>Instances</th>
				<th>Regions up</th>
			</tr>
		</thead>
		<tbody>
			{#each ['sts', 'md', 'td'] as plane (plane)}
				<tr>
					<td><a href={plane === 'md' ? '/market' : '/agents'}>{plane}</a></td>
					<td>{planes[plane as 'sts' | 'md' | 'td'].n}</td>
					<td>{planes[plane as 'sts' | 'md' | 'td'].up}</td>
				</tr>
			{/each}
		</tbody>
	</table>
{/if}

{#if sessions.items}
	<h2>Sessions</h2>
	<p>
		<a href="/sessions">{sessions.items.length}</a> in the mirror.
		{failing.length === 0 ? 'No failed sessions.' : `${failing.length} failed or rejected.`}
	</p>
	{#if failing.length > 0}
		<table class="data">
			<thead>
				<tr>
					<th>Session</th>
					<th>Name</th>
					<th>Phase</th>
					<th>Reason</th>
				</tr>
			</thead>
			<tbody>
				{#each failing as session (session.id)}
					<tr>
						<td><a class="mono" href="/sessions/{session.id}">{session.id}</a></td>
						<td>{session.spec.name}</td>
						<td>{session.status?.phase ?? '—'}</td>
						<td>
							{session.dependencies.find(
								(dependency) => dependency.state === 'rejected' || dependency.state === 'dead'
							)?.reason ?? session.status?.reason ?? '—'}
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	{/if}
{/if}
