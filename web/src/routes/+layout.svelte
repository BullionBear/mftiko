<script lang="ts">
	import '../app.css';
	import favicon from '#lib/assets/favicon.svg';
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { consoleState } from '#lib/console.svelte.ts';
	import { pageStream } from '#lib/events.svelte.ts';
	import { onMount } from 'svelte';
	import type { LayoutProps } from './$types';

	let { children }: LayoutProps = $props();

	const nav = [
		{ href: '/', label: 'Overview' },
		{ href: '/strategies', label: 'Strategies' },
		{ href: '/sessions', label: 'Sessions' },
		{ href: '/accounts', label: 'Accounts' },
		{ href: '/agents', label: 'Agents' },
		{ href: '/market', label: 'Market' }
	];

	function current(href: string, path: string): boolean {
		if (href === '/') return path === '/';
		return path === href || path.startsWith(`${href}/`);
	}

	onMount(() => {
		void consoleState.loadMe();
	});

	// Any 401 marks the session signed out; send the operator back after login (D1076).
	$effect(() => {
		if (consoleState.auth !== 'out') return;
		if (page.url.pathname === '/login') return;
		const next = page.url.pathname + page.url.search + page.url.hash;
		void goto(`/login?next=${encodeURIComponent(next)}`, { replaceState: true });
	});

	const liveLabel = $derived(
		pageStream.phase === 'open'
			? 'Live'
			: pageStream.phase === 'reconnecting'
				? 'Reconnecting…'
				: pageStream.phase === 'closed'
					? 'Disconnected'
					: ''
	);
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
	<title>mftiko</title>
</svelte:head>

{#if page.url.pathname === '/login'}
	{@render children()}
{:else if consoleState.auth === 'in'}
	<header class="top">
		<a class="brand" href="/">mftiko</a>
		<nav aria-label="Primary">
			{#each nav as item (item.href)}
				<a href={item.href} aria-current={current(item.href, page.url.pathname) ? 'page' : undefined}>
					{item.label}
				</a>
			{/each}
		</nav>
		<div class="user">
			{#if liveLabel}
				<span class="live" data-phase={pageStream.phase}>{liveLabel}</span>
			{/if}
			<span>{consoleState.user?.name}</span>
			<button type="button" onclick={() => consoleState.logout()}>Log out</button>
		</div>
	</header>

	{#if consoleState.meError}
		<p class="status error" role="alert">{consoleState.meError}</p>
	{/if}
	{#if consoleState.unavailableRegions.length > 0}
		<p class="banner" role="status">
			Unavailable regions: {consoleState.unavailableRegions.join(', ')}. Resources in those regions are missing from these results.
		</p>
	{/if}
	{#if consoleState.controllerAbsent}
		<p class="banner" lang="zh-Hant" role="status">狀態可能過期</p>
	{/if}

	<main>
		{@render children()}
	</main>
{:else if consoleState.meError}
	<main>
		<p class="status error" role="alert">{consoleState.meError}</p>
		<button type="button" onclick={() => consoleState.loadMe()}>Retry</button>
	</main>
{:else}
	<main>
		<p class="status">Loading…</p>
	</main>
{/if}

<style>
	.top {
		display: flex;
		flex-wrap: wrap;
		gap: 0.75rem 1.25rem;
		align-items: center;
		padding: 0.75rem 1rem;
		background: var(--surface);
		border-bottom: 1px solid var(--border);
	}

	.brand {
		color: var(--fg);
		font-weight: 600;
		text-decoration: none;
	}

	nav {
		display: flex;
		flex-wrap: wrap;
		gap: 0.25rem 0.9rem;
	}

	nav a {
		color: var(--muted);
		text-decoration: none;
	}

	nav a[aria-current='page'] {
		color: var(--fg);
		font-weight: 600;
	}

	.user {
		margin-left: auto;
		display: flex;
		gap: 0.75rem;
		align-items: center;
	}

	.live[data-phase='open'] {
		color: var(--accent);
	}

	.live[data-phase='reconnecting'],
	.live[data-phase='closed'] {
		color: var(--danger);
	}

	.banner {
		margin: 0;
		padding: 0.55rem 1rem;
		background: var(--warning-bg);
		color: var(--warning-fg);
	}

	main {
		padding: 1rem;
	}
</style>
