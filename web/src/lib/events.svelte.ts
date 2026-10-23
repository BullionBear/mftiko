// One EventSource for /api/v1/events (D1038, D1075).
// The browser sends the cookie and, on reconnect, Last-Event-ID. This module does not.
import { api, type EventTopic, type InstanceDetail, type ResetEvent, type SessionDetail, type SessionRemovedEvent } from './api';
import { consoleState } from './console.svelte';

export type LiveHandlers = {
	session?: (session: SessionDetail) => void;
	sessionRemoved?: (event: SessionRemovedEvent) => void;
	instance?: (instance: InstanceDetail) => void;
	reset?: (topics: EventTopic[]) => void;
};

const handlers = new Set<LiveHandlers>();

export function subscribe(handlersIn: LiveHandlers): () => void {
	handlers.add(handlersIn);
	return () => {
		handlers.delete(handlersIn);
	};
}

const TOPICS: EventTopic[] = ['sessions', 'instances'];

export function eventsURL(): string {
	const query = new URLSearchParams();
	for (const topic of TOPICS) query.append('topics', topic);
	return `/api/v1/events?${query}`;
}

function parseData<T>(ev: Event): T | undefined {
	if (!(ev instanceof MessageEvent) || typeof ev.data !== 'string') return undefined;
	try {
		return JSON.parse(ev.data) as T;
	} catch {
		return undefined;
	}
}

async function refreshFreshness(topics: EventTopic[]): Promise<void> {
	try {
		if (topics.includes('instances')) {
			const result = await api.GET('/instances');
			if (result.data) consoleState.applyFreshness(result.data.freshness);
			return;
		}
		if (topics.includes('sessions')) {
			const result = await api.GET('/sessions', { params: { query: { limit: 1 } } });
			if (result.data) consoleState.applyFreshness(result.data.freshness);
		}
	} catch {
		// The open page surfaces its own reload error. Banners keep the last freshness.
	}
}

class EventStream {
	phase = $state<'off' | 'open' | 'reconnecting' | 'closed'>('off');
	private source: EventSource | null = null;

	start(): void {
		this.stop();
		const source = new EventSource(eventsURL());
		this.source = source;
		this.phase = 'reconnecting';
		source.onopen = () => {
			if (this.source !== source) return;
			this.phase = 'open';
		};
		source.addEventListener('session', (ev) => {
			const data = parseData<SessionDetail>(ev);
			if (!data) return;
			consoleState.applyFreshness(data.freshness);
			for (const handler of handlers) handler.session?.(data);
		});
		source.addEventListener('session_removed', (ev) => {
			const data = parseData<SessionRemovedEvent>(ev);
			if (!data) return;
			for (const handler of handlers) handler.sessionRemoved?.(data);
		});
		source.addEventListener('instance', (ev) => {
			const data = parseData<InstanceDetail>(ev);
			if (!data) return;
			consoleState.applyFreshness(data.freshness);
			for (const handler of handlers) handler.instance?.(data);
		});
		source.addEventListener('reset', (ev) => {
			const data = parseData<ResetEvent>(ev);
			const topics = data?.topics?.length ? data.topics : TOPICS;
			for (const handler of handlers) handler.reset?.(topics);
			void refreshFreshness(topics);
		});
		source.onerror = () => {
			if (this.source !== source) return;
			// CLOSED: the browser will not retry (for example HTTP 401). CONNECTING: it is retrying.
			if (source.readyState === EventSource.CLOSED) {
				this.phase = 'closed';
				void api.GET('/auth/me');
				return;
			}
			this.phase = 'reconnecting';
		};
	}

	stop(): void {
		const source = this.source;
		this.source = null;
		source?.close();
		this.phase = 'off';
	}
}

export const eventStream = new EventStream();
