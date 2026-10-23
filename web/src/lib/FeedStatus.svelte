<script lang="ts">
	let {
		loading,
		error,
		empty,
		onretry
	}: {
		loading: boolean;
		error: string | null;
		empty: boolean;
		onretry: () => void;
	} = $props();
</script>

{#if error}
	<p class="status error" role="alert">
		{error}
		<button type="button" onclick={onretry}>Retry</button>
	</p>
{/if}
{#if loading && empty}
	<p class="status">Loading…</p>
{:else if loading}
	<p class="status">Updating…</p>
{/if}
