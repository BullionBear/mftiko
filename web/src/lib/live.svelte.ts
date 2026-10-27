// Per-page lists. The shared store stays the three fields in console.svelte.ts (E10-D077).
import { untrack } from 'svelte';
import {
	api,
	collectPages,
	failureOf,
	type Account,
	type AccountKind,
	type AgentStatus,
	type ControllerStatus,
	type DemandStatus,
	type Instance,
	type InstanceDetail,
	type InstanceSession,
	type Plane,
	type Session,
	type SessionDetail,
	type Worker
} from './api';
import { consoleState } from './console.svelte';
import { subscribe } from './events.svelte';

function preferSession(current: Session | undefined, next: Session): Session {
	if (!current || next.revision >= current.revision) return next;
	return current;
}

/** Instance list row, plus workers and demands once a detail or SSE snapshot has arrived. */
export type InstanceRow = Instance & {
	workers?: Worker[];
	demands?: DemandStatus[];
	sessions?: InstanceSession[];
};

function sameEpoch(a: InstanceRow, b: InstanceRow): boolean {
	return a.agent?.epoch === b.agent?.epoch;
}

function withCarriedDetail(chosen: InstanceRow, other: InstanceRow): InstanceRow {
	const bothEpochs = chosen.agent?.epoch !== undefined && other.agent?.epoch !== undefined;
	if (bothEpochs && !sameEpoch(chosen, other)) return chosen;
	const workers = chosen.workers ?? other.workers;
	const demands = chosen.demands ?? other.demands;
	const sessions = chosen.sessions ?? other.sessions;
	if (workers === chosen.workers && demands === chosen.demands && sessions === chosen.sessions) return chosen;
	return { ...chosen, workers, demands, sessions };
}

function preferInstance(current: InstanceRow | undefined, next: InstanceRow): InstanceRow {
	if (!current) return next;
	const currentEpoch = current.agent?.epoch;
	const nextEpoch = next.agent?.epoch;
	// A detail snapshot that omits the agent is absence, including no last-known record (E10-D096).
	if (nextEpoch === undefined && next.workers !== undefined && currentEpoch !== undefined) return next;
	if (currentEpoch === undefined) return withCarriedDetail(next, current);
	if (nextEpoch === undefined) return withCarriedDetail(current, next);
	const chosen = nextEpoch >= currentEpoch ? next : current;
	const other = chosen === next ? current : next;
	return withCarriedDetail(chosen, other);
}

function preferAgent(base: AgentStatus | null, pending: AgentStatus | null): AgentStatus | null {
	if (base && pending && pending.epoch < base.epoch) return base;
	if (base && pending === null) return base;
	return pending;
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
	/** STS instance agent, so log paths can be marked last known (E10-D105, E10-D096). */
	stsAgent = $state<AgentStatus | null>(null);
	loading = $state(false);
	error = $state<string | null>(null);
	agentError = $state<string | null>(null);
	missing = $state(false);
	private gen = 0;
	private pending: SessionDetail | null = null;
	private pendingAgents = new Map<string, AgentStatus | null>();
	private removed = false;
	private watching: string | null = null;

	cancel(): void {
		this.gen += 1;
	}

	private takePending(): SessionDetail | null {
		const pending = this.pending;
		this.pending = null;
		return pending;
	}

	watch(id: string): () => void {
		this.watching = id;
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
				this.missing = false;
			},
			sessionRemoved: (event) => {
				if (event.session_id !== id) return;
				this.removed = true;
				this.pending = null;
				this.session = null;
				this.stsAgent = null;
				this.missing = true;
			},
			instance: (instance) => {
				const agent = instance.agent ?? null;
				if (this.loading) {
					this.pendingAgents.set(instance.name, agent);
					return;
				}
				if (this.session?.sts_instance === instance.name) this.stsAgent = agent;
			},
			reset: (topics) => {
				if (topics.includes('sessions') || topics.includes('instances')) void this.load(id);
			}
		});
	}

	async load(id: string): Promise<void> {
		const mine = ++this.gen;
		this.loading = true;
		this.pending = null;
		this.pendingAgents.clear();
		this.removed = false;
		this.error = null;
		this.agentError = null;
		this.stsAgent = null;
		try {
			const result = await api.GET('/sessions/{session_id}', {
				params: { path: { session_id: id } }
			});
			if (mine !== this.gen) return;
			if (result.response.status === 401) {
				this.loading = false;
				return;
			}
			if (result.response.status === 404) {
				this.loading = false;
				this.missing = true;
				this.session = null;
				this.stsAgent = null;
				return;
			}
			const failure = failureOf(result.error, result.response);
			if (failure || !result.data) {
				this.loading = false;
				this.error = failure?.message ?? 'Empty response';
				return;
			}
			if (this.removed) {
				this.loading = false;
				this.missing = true;
				this.session = null;
				this.stsAgent = null;
				return;
			}
			const pending = this.takePending();
			const chosen = pending !== null && pending.revision >= result.data.revision ? pending : result.data;
			consoleState.applyFreshness(chosen.freshness);
			this.session = chosen;
			this.missing = false;
			await this.loadAgent(chosen.sts_instance, mine);
			if (mine !== this.gen) return;
			// Events that arrived after loadAgent applied its snapshot, before loading flips off.
			const queued = this.pendingAgents.get(chosen.sts_instance);
			if (queued !== undefined) this.stsAgent = preferAgent(this.stsAgent, queued);
			this.pendingAgents.clear();
			const later = this.takePending();
			if (later && (!this.session || later.revision >= this.session.revision)) this.session = later;
			this.loading = false;
		} catch {
			if (mine !== this.gen) return;
			this.loading = false;
			this.error = 'API unreachable';
		}
	}

	/** POST /sessions/{id}/end. Already-ended and terminal sessions still return 200 (E10-D104). */
	async end(id: string): Promise<string | null> {
		try {
			const result = await api.POST('/sessions/{session_id}/end', {
				params: { path: { session_id: id } }
			});
			if (this.watching !== id) return null;
			if (result.response.status === 401) return null;
			if (result.response.status === 404) {
				this.missing = true;
				this.session = null;
				this.stsAgent = null;
				return null;
			}
			const failure = failureOf(result.error, result.response);
			if (failure || !result.data || result.data.id !== id) return failure?.message ?? 'Empty response';
			this.accept(result.data);
			return null;
		} catch {
			if (this.watching !== id) return null;
			return 'API unreachable';
		}
	}

	private accept(session: SessionDetail): void {
		if (this.removed) return;
		consoleState.applyFreshness(session.freshness);
		if (this.session && session.revision < this.session.revision) return;
		if (this.loading && (!this.pending || session.revision >= this.pending.revision)) this.pending = session;
		this.session = session;
		this.missing = false;
	}

	private async loadAgent(sts: string, mine: number): Promise<void> {
		try {
			const result = await api.GET('/instances/{name}', {
				params: { path: { name: sts } }
			});
			if (mine !== this.gen) return;
			if (result.response.status === 401) return;
			if (result.response.status === 404) {
				this.applyPendingAgent(sts, null);
				return;
			}
			const failure = failureOf(result.error, result.response);
			if (failure || !result.data) {
				const pending = this.pendingAgents.get(sts);
				this.pendingAgents.clear();
				if (pending !== undefined) {
					this.stsAgent = pending;
					return;
				}
				this.agentError = failure?.message ?? 'Empty response';
				return;
			}
			consoleState.applyFreshness(result.data.freshness);
			this.applyPendingAgent(sts, result.data.agent ?? null);
		} catch {
			if (mine !== this.gen) return;
			const pending = this.pendingAgents.get(sts);
			this.pendingAgents.clear();
			if (pending !== undefined) {
				this.stsAgent = pending;
				return;
			}
			this.agentError = 'API unreachable';
		}
	}

	private applyPendingAgent(sts: string, base: AgentStatus | null): void {
		const pending = this.pendingAgents.get(sts);
		this.pendingAgents.clear();
		this.agentError = null;
		this.stsAgent = pending === undefined ? base : preferAgent(base, pending);
	}
}

export class InstanceFeed {
	items = $state<InstanceRow[] | null>(null);
	controller = $state<ControllerStatus | null>(null);
	loading = $state(false);
	error = $state<string | null>(null);
	private gen = 0;
	private pending = new Map<string, InstanceRow>();

	constructor(
		private readonly plane?: Plane,
		private readonly withDetail = false
	) {}

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
			this.pending.set(instance.name, preferInstance(this.pending.get(instance.name), instance));
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
			if (result.response.status === 401) {
				this.loading = false;
				return;
			}
			const failure = failureOf(result.error, result.response);
			if (failure || !result.data) {
				this.loading = false;
				this.error = failure?.message ?? 'Empty response';
				return;
			}
			let rows: InstanceRow[] = result.data.items;
			let detailError: string | undefined;
			if (this.withDetail) {
				const loaded = await Promise.all(rows.map((instance) => this.fetchDetail(instance, mine)));
				if (mine !== this.gen) return;
				if (loaded.some((item) => item.unauthorized)) {
					this.loading = false;
					return;
				}
				rows = loaded.map((item) => item.row);
				detailError = loaded.find((item) => item.error)?.error;
			}
			const overlay = [...this.pending.values()];
			this.pending.clear();
			const byName = new Map(rows.map((instance) => [instance.name, instance]));
			for (const instance of overlay) {
				byName.set(instance.name, preferInstance(byName.get(instance.name), instance));
			}
			consoleState.applyFreshness(result.data.freshness);
			this.items = [...byName.values()].sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
			this.controller = result.data.controller;
			this.error = detailError ?? null;
			this.loading = false;
		} catch {
			if (mine !== this.gen) return;
			this.loading = false;
			this.error = 'API unreachable';
		}
	}

	private async fetchDetail(
		instance: Instance,
		mine: number
	): Promise<{ row: InstanceRow; error?: string; unauthorized?: boolean }> {
		try {
			const result = await api.GET('/instances/{name}', {
				params: { path: { name: instance.name } }
			});
			if (mine !== this.gen) return { row: instance };
			if (result.response.status === 401) return { row: instance, unauthorized: true };
			const failure = failureOf(result.error, result.response);
			if (failure || !result.data) {
				return { row: instance, error: failure?.message ?? 'Empty response' };
			}
			consoleState.applyFreshness(result.data.freshness);
			return { row: result.data };
		} catch {
			return { row: instance, error: 'API unreachable' };
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

