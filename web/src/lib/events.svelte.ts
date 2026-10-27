// One /events EventSource per page, for the topics that page needs.
// Log follow on the session page is a separate EventSource (E10-D107, E14-D036, E10-D075).
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

export function eventsURL(topics: readonly EventTopic[], sessionId?: string): string {
	const query = new URLSearchParams();
	for (const topic of topics) query.append('topics', topic);
	if (sessionId && topics.includes('sessions')) query.set('session_id', sessionId);
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

class PageStream {
	phase = $state<'off' | 'open' | 'reconnecting' | 'closed'>('off');
	private generation = 0;
	private source: EventSource | null = null;
	private topics: EventTopic[] = [];

	/** Opens this page's only /events EventSource, replacing any page that still held one. */
	open(topics: readonly EventTopic[], sessionId?: string): () => void {
		const mine = ++this.generation;
		this.closeSource();
		this.topics = topics.slice();
		const source = new EventSource(eventsURL(topics, sessionId));
		this.source = source;
		this.phase = 'reconnecting';
		source.onopen = () => {
			if (this.generation !== mine) return;
			this.phase = 'open';
		};
		source.addEventListener('session', (ev) => {
			if (this.generation !== mine) return;
			const data = parseData<SessionDetail>(ev);
			if (!data) return;
			consoleState.applyFreshness(data.freshness);
			for (const handler of handlers) handler.session?.(data);
		});
		source.addEventListener('session_removed', (ev) => {
			if (this.generation !== mine) return;
			const data = parseData<SessionRemovedEvent>(ev);
			if (!data) return;
			for (const handler of handlers) handler.sessionRemoved?.(data);
		});
		source.addEventListener('instance', (ev) => {
			if (this.generation !== mine) return;
			const data = parseData<InstanceDetail>(ev);
			if (!data) return;
			consoleState.applyFreshness(data.freshness);
			for (const handler of handlers) handler.instance?.(data);
		});
		source.addEventListener('reset', (ev) => {
			if (this.generation !== mine) return;
			const data = parseData<ResetEvent>(ev);
			const resetTopics = data?.topics?.length ? data.topics : this.topics.slice();
			for (const handler of handlers) handler.reset?.(resetTopics);
			void refreshFreshness(resetTopics);
		});
		source.onerror = () => {
			if (this.generation !== mine) return;
			// CLOSED: the browser will not retry (for example HTTP 401). CONNECTING: it is retrying.
			if (source.readyState === EventSource.CLOSED) {
				this.phase = 'closed';
				void api.GET('/auth/me');
				return;
			}
			this.phase = 'reconnecting';
		};
		return () => {
			if (this.generation !== mine) return;
			this.generation += 1;
			this.closeSource();
			this.phase = 'off';
		};
	}

	private closeSource(): void {
		const source = this.source;
		this.source = null;
		source?.close();
	}
}

export const pageStream = new PageStream();

export function openPageStream(topics: readonly EventTopic[], sessionId?: string): () => void {
	return pageStream.open(topics, sessionId);
}
