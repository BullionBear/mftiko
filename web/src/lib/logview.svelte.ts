// Session log view (E14-D036–E14-D040).
// Follow off: one GET with `tail` and no `follow`. The response ends; this is not a kept EventSource.
// Follow on: a separate EventSource. Close it when the switch is turned off or the page goes away.
// `/events` stays the page's other connection (E10-D107).
import { api } from './api';
import {
	appendLog,
	DEFAULT_LOG_TAIL,
	emptyPane,
	endReasonText,
	isLogTail,
	noteGap,
	readEventStream,
	sessionLogsURL,
	type LogPane,
	type LogStreamName,
	type LogTail,
	type SSEEvent
} from './logtext';

type Conn = {
	sessionId: string;
	tail: LogTail;
	follow: boolean;
	attempt: number;
};

export class SessionLogView {
	tail = $state<LogTail>(DEFAULT_LOG_TAIL);
	follow = $state(false);
	stream = $state<LogStreamName>('stdout');
	stdout = $state<LogPane>(emptyPane());
	stderr = $state<LogPane>(emptyPane());
	unavailable = $state(false);
	endText = $state<string | null>(null);
	attempt = $state(0);

	private generation = 0;
	private conn: Conn | null = null;
	private source: EventSource | null = null;
	private abort: AbortController | null = null;
	private ended = false;
	private ids = { next: 1 };
	// Set in the click handler, not read as state from the connect effect.
	// Reading the panes inside that effect would reconnect on every line.
	private keep = false;

	select(stream: LogStreamName): void {
		this.stream = stream;
	}

	setTail(raw: string): void {
		const n = Number(raw);
		if (!isLogTail(n) || n === this.tail) return;
		this.tail = n as LogTail;
	}

	setFollow(on: boolean): void {
		if (on === this.follow) return;
		// Turning follow off closes the live stream. Keep lines already on screen.
		// An empty view (the read was aborted before any line) falls through to one snapshot.
		this.keep = false;
		if (!on) {
			this.keep =
				this.endText !== null ||
				this.stdout.rows.length > 0 ||
				this.stdout.partial !== '' ||
				this.stderr.rows.length > 0 ||
				this.stderr.partial !== '';
		}
		this.follow = on;
	}

	retry(): void {
		this.attempt += 1;
	}

	/**
	 * Opens the read for this tail and follow state.
	 * Turning follow off closes the EventSource. Lines already shown stay (E14-D036).
	 * If nothing has arrived yet, one snapshot runs instead.
	 * The caller must not read the panes here: that would reconnect on every line.
	 */
	connect(sessionId: string, tail: LogTail, follow: boolean, attempt: number): () => void {
		const prev = this.conn;
		const keep = this.keep;
		this.keep = false;
		const turnOff =
			keep &&
			prev !== null &&
			prev.sessionId === sessionId &&
			prev.tail === tail &&
			prev.attempt === attempt &&
			prev.follow &&
			!follow;
		this.conn = { sessionId, tail, follow, attempt };
		const mine = ++this.generation;
		this.stopTransport();
		if (turnOff) return () => this.stopIf(mine);
		this.stdout = emptyPane();
		this.stderr = emptyPane();
		this.unavailable = false;
		this.endText = null;
		this.ended = false;
		if (follow) this.openFollow(mine, sessionId, tail);
		else void this.openSnapshot(mine, sessionId, tail);
		return () => this.stopIf(mine);
	}

	private stopIf(mine: number): void {
		if (this.generation !== mine) return;
		this.generation += 1;
		this.stopTransport();
	}

	private stopTransport(): void {
		const source = this.source;
		this.source = null;
		source?.close();
		const abort = this.abort;
		this.abort = null;
		abort?.abort();
	}

	private commit(stream: LogStreamName, pane: LogPane): void {
		const next: LogPane = { rows: pane.rows, partial: pane.partial, lines: pane.lines };
		if (stream === 'stdout') this.stdout = next;
		else this.stderr = next;
	}

	private apply(ev: SSEEvent): void {
		if (this.ended) return;
		if (ev.event === 'log') {
			const parsed = parseRecord(ev.data);
			const stream = streamOf(parsed?.stream);
			if (!parsed || !stream || typeof parsed.data !== 'string') return;
			const pane = stream === 'stdout' ? this.stdout : this.stderr;
			appendLog(pane, parsed.data, this.ids);
			this.commit(stream, pane);
			return;
		}
		if (ev.event === 'gap') {
			const stream = streamOf(parseRecord(ev.data)?.stream);
			if (!stream) return;
			const pane = stream === 'stdout' ? this.stdout : this.stderr;
			noteGap(pane, this.ids);
			this.commit(stream, pane);
			return;
		}
		if (ev.event === 'end') {
			const reason = parseRecord(ev.data)?.reason;
			this.finish(typeof reason === 'string' ? reason : undefined);
		}
	}

	private finish(reason: string | undefined): void {
		if (this.ended) return;
		this.ended = true;
		this.unavailable = false;
		this.endText = endReasonText(reason);
		this.stopTransport();
	}

	private markUnavailable(): void {
		if (this.ended) return;
		this.unavailable = true;
	}

	private openFollow(mine: number, sessionId: string, tail: LogTail): void {
		const source = new EventSource(sessionLogsURL(sessionId, tail, true));
		this.source = source;
		const take = (ev: Event) => {
			if (this.generation !== mine || this.ended) return;
			if (!(ev instanceof MessageEvent) || typeof ev.data !== 'string') return;
			this.apply({ event: ev.type, data: ev.data });
		};
		source.addEventListener('log', take);
		source.addEventListener('gap', take);
		source.addEventListener('end', take);
		source.onerror = () => {
			if (this.generation !== mine || this.ended) return;
			// CLOSED: the browser will not retry (for example HTTP 503). CONNECTING: it retries
			// with Last-Event-ID. Do not open another EventSource here (E14-D039).
			if (source.readyState !== EventSource.CLOSED) return;
			this.markUnavailable();
			void api.GET('/auth/me');
		};
	}

	private async openSnapshot(mine: number, sessionId: string, tail: LogTail): Promise<void> {
		const ctrl = new AbortController();
		this.abort = ctrl;
		try {
			const response = await fetch(sessionLogsURL(sessionId, tail, false), {
				credentials: 'same-origin',
				cache: 'no-store',
				signal: ctrl.signal,
				headers: { Accept: 'text/event-stream' }
			});
			if (this.generation !== mine) {
				void response.body?.cancel();
				return;
			}
			if (response.status === 401) {
				this.markUnavailable();
				void response.body?.cancel();
				void api.GET('/auth/me');
				return;
			}
			if (!response.ok || !response.body) {
				this.markUnavailable();
				void response.body?.cancel();
				return;
			}
			await readEventStream(response.body, (ev) => {
				if (this.generation !== mine || this.ended) return;
				this.apply(ev);
			});
			if (this.generation !== mine || this.ended) return;
			// The API closes without `end` when it gives up on the agent (E14-D028).
			this.markUnavailable();
		} catch (err) {
			if (this.generation !== mine || this.ended) return;
			if (isAbort(err)) return;
			this.markUnavailable();
		}
	}
}

function parseRecord(data: string): Record<string, unknown> | null {
	try {
		const value = JSON.parse(data) as unknown;
		if (!value || typeof value !== 'object') return null;
		return value as Record<string, unknown>;
	} catch {
		return null;
	}
}

function streamOf(value: unknown): LogStreamName | null {
	if (value === 'stdout' || value === 'stderr') return value;
	return null;
}

function isAbort(err: unknown): boolean {
	return (err instanceof DOMException || err instanceof Error) && err.name === 'AbortError';
}
