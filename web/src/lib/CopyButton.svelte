<script lang="ts">
	let { value, label }: { value: string; label: string } = $props();

	let note = $state<'copied' | 'failed' | ''>('');

	async function writeClipboard(text: string): Promise<boolean> {
		try {
			if (navigator.clipboard?.writeText) {
				await navigator.clipboard.writeText(text);
				return true;
			}
		} catch {
			// Selection fallback covers a non-secure dev origin.
		}
		try {
			const area = document.createElement('textarea');
			area.value = text;
			area.setAttribute('readonly', '');
			area.style.position = 'fixed';
			area.style.top = '0';
			area.style.left = '0';
			area.style.opacity = '0';
			document.body.append(area);
			area.select();
			const ok = document.execCommand('copy');
			area.remove();
			return ok;
		} catch {
			return false;
		}
	}

	async function copy(): Promise<void> {
		note = (await writeClipboard(value)) ? 'copied' : 'failed';
	}
</script>

<button type="button" aria-label={label} onclick={() => void copy()}>Copy</button>
{#if note === 'copied'}
	<span class="status" role="status">Copied</span>
{:else if note === 'failed'}
	<span class="status error" role="status">Copy failed</span>
{/if}
