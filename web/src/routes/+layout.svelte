<script lang="ts">
	import favicon from '#lib/assets/favicon.svg';
	import type { LayoutProps } from './$types';

	let { children }: LayoutProps = $props();

	const nav = [
		{ href: '/', label: 'Overview' },
		{ href: '/sessions', label: 'Sessions' },
		{ href: '/strategies', label: 'Strategies' },
		{ href: '/accounts', label: 'Accounts' },
		{ href: '/market', label: 'Market data' },
		{ href: '/agents', label: 'Agents' }
	];
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
	<title>mftiko</title>
</svelte:head>

<nav>
	{#each nav as item (item.href)}
		<a href={item.href}>{item.label}</a>
	{/each}
</nav>

<main>
	{@render children()}
</main>
