// Line buffer and SSE framing for the session log view (E14-D037, E14-D038, E14-D040).
// The follow switch uses EventSource; this module does not open connections.

export const LOG_TAILS = [100, 200, 500, 1000] as const;
export const DEFAULT_LOG_TAIL = 200;
export const LOG_LINE_CAP = 5000;
export const LOG_GAP_TEXT = 'some output was rotated out';
export const LOG_UNAVAILABLE_TEXT = '目前讀不到 log';
export const LOG_RETRY_TEXT = '重試';

export type LogTail = (typeof LOG_TAILS)[number];
export type LogStreamName = 'stdout' | 'stderr';

export type LogRow = { id: number; kind: 'line'; text: string } | { id: number; kind: 'gap' };

/** `lines` counts complete lines only. A non-empty `partial` is the current unfinished line. */
export type LogPane = {
	rows: LogRow[];
	partial: string;
	lines: number;
};

export type SSEEvent = { event: string; data: string };

export function emptyPane(): LogPane {
	return { rows: [], partial: '', lines: 0 };
}

export function isLogTail(value: number): boolean {
	return value === 100 || value === 200 || value === 500 || value === 1000;
}

export function sessionLogsURL(sessionId: string, tail: number, follow: boolean): string {
	const query = new URLSearchParams({ tail: String(tail) });
	if (follow) query.set('follow', 'true');
	return `/api/v1/sessions/${encodeURIComponent(sessionId)}/logs?${query}`;
}

export function sessionLogsExportURL(sessionId: string): string {
	return `/api/v1/sessions/${encodeURIComponent(sessionId)}/logs/export`;
}

/** `eof` 已讀到結尾、`terminal` 已結束、`removed` 已刪除 (E14-D040). */
export function endReasonText(reason: string | undefined): string | null {
	if (reason === 'terminal') return '已結束';
	if (reason === 'removed') return '已刪除';
	if (reason === 'eof') return '已讀到結尾';
	return null;
}

export function appendLog(pane: LogPane, text: string, ids: { next: number }): void {
	if (text === '') return;
	const parts = (pane.partial + text).split('\n');
	pane.partial = parts.pop() ?? '';
	for (const part of parts) {
		pane.rows.push({ id: ids.next++, kind: 'line', text: stripCR(part) });
		pane.lines += 1;
	}
	trimPane(pane);
}

/** A gap is a discontinuity: finish the open line, then mark the hole. Gaps are not lines. */
export function noteGap(pane: LogPane, ids: { next: number }): void {
	if (pane.partial !== '') {
		pane.rows.push({ id: ids.next++, kind: 'line', text: stripCR(pane.partial) });
		pane.lines += 1;
		pane.partial = '';
	}
	pane.rows.push({ id: ids.next++, kind: 'gap' });
	trimPane(pane);
}

function stripCR(text: string): string {
	return text.endsWith('\r') ? text.slice(0, -1) : text;
}

function trimPane(pane: LogPane): void {
	const partial = pane.partial === '' ? 0 : 1;
	let extra = pane.lines + partial - LOG_LINE_CAP;
	if (extra <= 0) return;
	const kept: LogRow[] = [];
	for (const row of pane.rows) {
		if (extra > 0 && row.kind === 'line') {
			extra -= 1;
			pane.lines -= 1;
			continue;
		}
		kept.push(row);
	}
	pane.rows = kept;
}

/** Incremental SSE parser. Comment lines (`: keepalive`) and empty dispatches are dropped. */
export class SSEDecoder {
	#buf = '';
	#event = 'message';
	#data: string[] = [];

	push(chunk: string): SSEEvent[] {
		this.#buf += chunk;
		const out: SSEEvent[] = [];
		for (;;) {
			const nl = this.#buf.indexOf('\n');
			if (nl < 0) break;
			let line = this.#buf.slice(0, nl);
			this.#buf = this.#buf.slice(nl + 1);
			if (line.endsWith('\r')) line = line.slice(0, -1);
			if (line === '') {
				const ev = this.#dispatch();
				if (ev) out.push(ev);
				continue;
			}
			if (line.startsWith(':')) continue;
			const colon = line.indexOf(':');
			const field = colon === -1 ? line : line.slice(0, colon);
			let value = colon === -1 ? '' : line.slice(colon + 1);
			if (value.startsWith(' ')) value = value.slice(1);
			if (field === 'event') this.#event = value;
			else if (field === 'data') this.#data.push(value);
		}
		return out;
	}

	#dispatch(): SSEEvent | null {
		const data = this.#data.join('\n');
		const event = this.#event;
		this.#event = 'message';
		this.#data = [];
		if (data === '') return null;
		return { event, data };
	}
}

export async function readEventStream(
	body: ReadableStream<Uint8Array>,
	onEvent: (ev: SSEEvent) => void
): Promise<void> {
	const reader = body.getReader();
	const decoder = new TextDecoder();
	const sse = new SSEDecoder();
	try {
		for (;;) {
			const { done, value } = await reader.read();
			if (done) break;
			for (const ev of sse.push(decoder.decode(value, { stream: true }))) onEvent(ev);
		}
		for (const ev of sse.push(decoder.decode())) onEvent(ev);
	} finally {
		reader.releaseLock();
	}
}
