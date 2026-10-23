// Per-page lists. The shared store stays the three fields in console.svelte.ts (D1077).
import { untrack } from 'svelte';
import {
	api,
	collectPages,
	failureOf,
	type Account,
	type AccountKind,
	type ControllerStatus,
	type Instance,
	type InstanceDetail,
	type Plane,
	type Session,
	type SessionDetail,
	type Strategy
} from './api';
import { consoleState } from './console.svelte';
import { subscribe } from './events.svelte';

function preferSession(current: Session | undefined, next: Session): Session {
	if (!current || next.revision >= current.revision) return next;
	return current;
}

function preferInstance(current: Instance | undefined, next: Instance): Instance {
	if (!current) return next;
	const currentEpoch = current.agent?.epoch;
	const nextEpoch = next.agent?.epoch;
	if (currentEpoch === undefined) return next;
	if (nextEpoch === undefined) return current;
	return nextEpoch >= currentEpoch ? next : current;
}

export class SessionFeed {
	items = $state<Session[] | null>(null);
	loading = $state(false);
	error = $state<string | null>(null);
	private gen = 0;
	private pending = new Map<string, Session>();
	private removed = new Set<string>();

	cancel(): void {
		this.gen += 1;
	}

	watch(): () => void {
		return subscribe({
			session: (session) => this.upsert(session),
			sessionRemoved: (event) => this.remove(event.session_id),
			reset: (topics) => {
				if (topics.includes('sessions')) void this.reload();
			}
		});
	}

	upsert(session: Session): void {
		this.removed.delete(session.id);
		if (this.loading) {
			this.pending.set(session.id, preferSession(this.pending.get(session.id), session));
			return;
		}
		const items = this.items ? this.items.slice() : [];
		const index = items.findIndex((item) => item.id === session.id);
		if (index === -1) items.push(session);
		else items[index] = preferSession(items[index], session);
		items.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
		this.items = items;
	}

	remove(id: string): void {
		this.pending.delete(id);
		this.removed.add(id);
		if (this.items) this.items = this.items.filter((session) => session.id !== id);
	}

	async reload(): Promise<void> {
		const mine = ++this.gen;
		this.loading = true;
		this.pending.clear();
		this.removed.clear();
		const result = await collectPages<Session>((cursor) =>
			api.GET('/sessions', {
				params: { query: cursor ? { limit: 100, cursor } : { limit: 100 } }
			})
		);
		if (mine !== this.gen) return;
		this.loading = false;
		if (result.failure) {
			if (result.failure.status !== 401) this.error = result.failure.message;
			return;
		}
		const overlay = [...this.pending.values()];
		const removed = new Set(this.removed);
		this.pending.clear();
		this.removed.clear();
		const items = result.items.filter((session) => !removed.has(session.id));
		for (const session of overlay) {
			if (removed.has(session.id)) continue;
			const index = items.findIndex((item) => item.id === session.id);
			if (index === -1) items.push(session);
			else items[index] = preferSession(items[index], session);
		}
		items.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
		consoleState.applyFreshness(result.freshness);
		this.items = items;
		this.error = null;
	}
}

export class SessionDetailFeed {
	session = $state<SessionDetail | null>(null);
	loading = $state(false);
	error = $state<string | null>(null);
	missing = $state(false);
	private gen = 0;
	private pending: SessionDetail | null = null;
	private removed = false;

	cancel(): void {
		this.gen += 1;
	}

	watch(id: string): () => void {
		return subscribe({
			session: (session) => {
				if (session.id !== id) return;
				this.removed = false;
				if (this.loading) {
					if (!this.pending || session.revision >= this.pending.revision) this.pending = session;
					return;
				}
				if (this.session && session.revision < this.session.revision) return;
				this.session = session;
			},
			sessionRemoved: (event) => {
				if (event.session_id !== id) return;
				this.removed = true;
				this.pending = null;
				this.session = null;
				this.missing = true;
			},
			reset: (topics) => {
				if (topics.includes('sessions')) void this.load(id);
			}
		});
	}

	async load(id: string): Promise<void> {
		const mine = ++this.gen;
		this.loading = true;
		this.pending = null;
		this.removed = false;
		this.error = null;
		try {
			const result = await api.GET('/sessions/{session_id}', {
				params: { path: { session_id: id } }
			});
			if (mine !== this.gen) return;
			this.loading = false;
			if (result.response.status === 401) return;
			if (result.response.status === 404) {
				this.missing = true;
				this.session = null;
				return;
			}
			const failure = failureOf(result.error, result.response);
			if (failure || !result.data) {
				this.error = failure?.message ?? 'Empty response';
				return;
			}
			if (this.removed) {
				this.missing = true;
				this.session = null;
				return;
			}
			const pending = this.pending as SessionDetail | null;
			this.pending = null;
			const chosen = pending !== null && pending.revision >= result.data.revision ? pending : result.data;
			consoleState.applyFreshness(chosen.freshness);
			this.session = chosen;
			this.missing = false;
		} catch {
			if (mine !== this.gen) return;
			this.loading = false;
			this.error = 'API unreachable';
		}
	}
}

export class InstanceFeed {
	items = $state<Instance[] | null>(null);
	controller = $state<ControllerStatus | null>(null);
	loading = $state(false);
	error = $state<string | null>(null);
	private gen = 0;
	private pending = new Map<string, Instance>();

	constructor(private readonly plane?: Plane) {}

	cancel(): void {
		this.gen += 1;
	}

	watch(): () => void {
		return subscribe({
			instance: (instance) => this.upsert(instance),
			reset: (topics) => {
				if (topics.includes('instances')) void this.reload();
			}
		});
	}

	upsert(instance: InstanceDetail): void {
		if (this.plane && instance.plane !== this.plane) return;
		if (this.loading) {
			this.pending.set(instance.name, instance);
			return;
		}
		const items = this.items ? this.items.slice() : [];
		const index = items.findIndex((item) => item.name === instance.name);
		if (index === -1) items.push(instance);
		else items[index] = preferInstance(items[index], instance);
		items.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
		this.items = items;
	}

	async reload(): Promise<void> {
		const mine = ++this.gen;
		this.loading = true;
		this.pending.clear();
		try {
			const result = await api.GET('/instances', {
				params: { query: this.plane ? { plane: this.plane } : {} }
			});
			if (mine !== this.gen) return;
			this.loading = false;
			if (result.response.status === 401) return;
			const failure = failureOf(result.error, result.response);
			if (failure || !result.data) {
				this.error = failure?.message ?? 'Empty response';
				return;
			}
			const overlay = [...this.pending.values()];
			this.pending.clear();
			const byName = new Map(result.data.items.map((instance) => [instance.name, instance]));
			for (const instance of overlay) {
				byName.set(instance.name, preferInstance(byName.get(instance.name), instance));
			}
			consoleState.applyFreshness(result.data.freshness);
			this.items = [...byName.values()].sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
			this.controller = result.data.controller;
			this.error = null;
		} catch {
			if (mine !== this.gen) return;
			this.loading = false;
			this.error = 'API unreachable';
		}
	}
}

export class AccountFeed {
	items = $state<Account[] | null>(null);
	loading = $state(false);
	error = $state<string | null>(null);
	private gen = 0;

	cancel(): void {
		this.gen += 1;
	}

	async reload(): Promise<void> {
		const mine = ++this.gen;
		this.loading = true;
		const result = await collectPages<Account>((cursor) =>
			api.GET('/accounts', {
				params: { query: cursor ? { limit: 100, cursor } : { limit: 100 } }
			})
		);
		if (mine !== this.gen) return;
		this.loading = false;
		if (result.failure) {
			if (result.failure.status !== 401) this.error = result.failure.message;
			return;
		}
		consoleState.applyFreshness(result.freshness);
		this.items = result.items;
		this.error = null;
	}
}

export class AccountDetailFeed {
	account = $state<Account | null>(null);
	loading = $state(false);
	error = $state<string | null>(null);
	missing = $state(false);
	private gen = 0;

	cancel(): void {
		this.gen += 1;
	}

	markMissing(): void {
		this.gen += 1;
		this.account = null;
		this.missing = true;
		this.loading = false;
		this.error = null;
	}

	/** Returns the account, or null when a newer load superseded this one or the request failed. */
	async load(apiId: number): Promise<Account | null> {
		const mine = ++this.gen;
		this.loading = true;
		this.error = null;
		this.missing = false;
		// A caller may be an effect that must not subscribe to `account` (it would reload in a loop).
		const current = untrack(() => this.account);
		if (current && current.api_id !== apiId) this.account = null;
		try {
			const result = await api.GET('/accounts/{api_id}', {
				params: { path: { api_id: apiId } }
			});
			if (mine !== this.gen) return null;
			this.loading = false;
			if (result.response.status === 401) return null;
			if (result.response.status === 404) {
				this.missing = true;
				this.account = null;
				return null;
			}
			const failure = failureOf(result.error, result.response);
			if (failure || !result.data) {
				this.error = failure?.message ?? 'Empty response';
				return null;
			}
			this.account = result.data;
			return result.data;
		} catch {
			if (mine !== this.gen) return null;
			this.loading = false;
			this.error = 'API unreachable';
			return null;
		}
	}
}

export class AccountKindFeed {
	items = $state<AccountKind[] | null>(null);
	loading = $state(false);
	error = $state<string | null>(null);
	private gen = 0;

	cancel(): void {
		this.gen += 1;
	}

	async reload(): Promise<void> {
		const mine = ++this.gen;
		this.loading = true;
		try {
			const result = await api.GET('/account-kinds');
			if (mine !== this.gen) return;
			this.loading = false;
			if (result.response.status === 401) return;
			const failure = failureOf(result.error, result.response);
			if (failure || !result.data) {
				this.error = failure?.message ?? 'Empty response';
				return;
			}
			this.items = result.data.items;
			this.error = null;
		} catch {
			if (mine !== this.gen) return;
			this.loading = false;
			this.error = 'API unreachable';
		}
	}
}

export class StrategyFeed {
	items = $state<Strategy[] | null>(null);
	loading = $state(false);
	error = $state<string | null>(null);
	private gen = 0;

	cancel(): void {
		this.gen += 1;
	}

	async reload(): Promise<void> {
		const mine = ++this.gen;
		this.loading = true;
		const result = await collectPages<Strategy>((cursor) =>
			api.GET('/strategies', {
				params: { query: cursor ? { limit: 100, cursor } : { limit: 100 } }
			})
		);
		if (mine !== this.gen) return;
		this.loading = false;
		if (result.failure) {
			if (result.failure.status !== 401) this.error = result.failure.message;
			return;
		}
		this.items = result.items;
		this.error = null;
	}
}
