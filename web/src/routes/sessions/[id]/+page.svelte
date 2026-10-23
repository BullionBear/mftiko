<script lang="ts">
	import { page } from '$app/state';
	import FeedStatus from '#lib/FeedStatus.svelte';
	import { SessionDetailFeed } from '#lib/live.svelte.ts';

	const detail = new SessionDetailFeed();
	const id = $derived(page.params.id ?? '');

	$effect(() => {
		const sessionId = id;
		if (!sessionId) return;
		const stop = detail.watch(sessionId);
		void detail.load(sessionId);
		return () => {
			stop();
			detail.cancel();
		};
	});
</script>

<svelte:head>
	<title>Session {id} · mftiko</title>
</svelte:head>

<p><a href="/sessions">Sessions</a></p>
<h1>Session <span class="mono">{id}</span></h1>
<FeedStatus
	loading={detail.loading}
	error={detail.error}
	empty={detail.session === null && !detail.missing}
	onretry={() => detail.load(id)}
/>

{#if detail.missing}
	<p class="status" role="status">Not found.</p>
{:else if detail.session}
	<dl class="facts">
		<dt>Name</dt>
		<dd>{detail.session.spec.name}</dd>
		<dt>Phase</dt>
		<dd>{detail.session.status?.phase ?? '—'}</dd>
		<dt>Desired</dt>
		<dd>{detail.session.spec.desired_state}</dd>
		<dt>STS</dt>
		<dd>{detail.session.sts_instance}</dd>
		<dt>Region</dt>
		<dd>{detail.session.region}</dd>
		<dt>Revision</dt>
		<dd>{detail.session.revision}</dd>
		<dt>Strategy</dt>
		<dd class="mono">{detail.session.spec.strategy.digest}</dd>
		{#if detail.session.assignment?.end_reason}
			<dt>End reason</dt>
			<dd>{detail.session.assignment.end_reason}</dd>
		{/if}
		{#if detail.session.status?.reason}
			<dt>Status reason</dt>
			<dd>{detail.session.status.reason}</dd>
		{/if}
		<dt>Dependencies</dt>
		<dd>{detail.session.dependencies.length}</dd>
	</dl>
{/if}
