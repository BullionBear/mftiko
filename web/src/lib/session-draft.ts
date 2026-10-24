// Session create wizard (E10-D086–E10-D091). Params stay text (E10-D033): the request body
// splices the textarea in, and nothing here parses that text back into a value.
import type { Problem } from './api';

export type FeedRow = { key: number; feed: string; mdInstance: string };

export type AccountRow = {
	key: number;
	apiId: string;
	name: string;
	algoIds: number[];
	algoDraft: string;
};

export type Draft = {
	name: string;
	stsInstance: string;
	strategyId: string;
	strategyRef: string;
	paramsText: string;
	feeds: FeedRow[];
	accounts: AccountRow[];
	restartMode: '' | 'on_failure' | 'never';
	maxRestarts: string;
	windowS: string;
	depsTimeoutS: string;
	startTimeoutS: string;
	recvWindowMs: string;
	graceMs: string;
	memoryMb: string;
	gomaxprocs: string;
	volumeMb: string;
	hookTimeoutS: string;
};

const UINT32_MAX = 4294967295;

const STATIC_PATHS = new Set([
	'/name',
	'/sts_instance',
	'/strategy',
	'/strategy/id',
	'/strategy/ref',
	'/params',
	'/feeds',
	'/accounts',
	'/restart',
	'/restart/mode',
	'/restart/max_restarts',
	'/restart/window_s',
	'/deps_timeout_s',
	'/start_timeout_s',
	'/recv_window_ms',
	'/grace_ms',
	'/limits',
	'/limits/memory_mb',
	'/limits/gomaxprocs',
	'/limits/volume_mb',
	'/limits/hook_timeout_s'
]);

export function emptyDraft(): Draft {
	return {
		name: '',
		stsInstance: '',
		strategyId: '',
		strategyRef: '',
		paramsText: '',
		feeds: [],
		accounts: [],
		restartMode: '',
		maxRestarts: '',
		windowS: '',
		depsTimeoutS: '',
		startTimeoutS: '',
		recvWindowMs: '',
		graceMs: '',
		memoryMb: '',
		gomaxprocs: '',
		volumeMb: '',
		hookTimeoutS: ''
	};
}

export function newIdempotencyKey(): string {
	return crypto.randomUUID();
}

export function paramsError(text: string): string | undefined {
	if (text.trim() === '') return undefined;
	try {
		JSON.parse(text);
	} catch {
		return 'is not valid JSON';
	}
	return undefined;
}

export function parseUint32(raw: string): number | undefined {
	const text = raw.trim();
	if (!/^\d+$/.test(text)) return undefined;
	const n = Number(text);
	if (!Number.isSafeInteger(n) || n > UINT32_MAX) return undefined;
	return n;
}

function uint32Error(raw: string): string | undefined {
	if (raw.trim() === '') return undefined;
	if (parseUint32(raw) === undefined) return 'must be between 0 and 4294967295';
	return undefined;
}

/** An algo_id the operator adds to the selected account (1–1023, E10-D012). */
export function parseAlgo(raw: string): number | undefined {
	const text = raw.trim();
	if (!/^\d+$/.test(text)) return undefined;
	const n = Number(text);
	if (!Number.isSafeInteger(n) || n < 1 || n > 1023) return undefined;
	return n;
}

function accountAlgos(row: AccountRow): number[] {
	return row.algoIds.slice();
}

export function localErrors(draft: Draft): Record<string, string> {
	const errors: Record<string, string> = {};
	if (draft.name.trim() === '') errors['/name'] = 'required';
	if (draft.stsInstance === '') errors['/sts_instance'] = 'required';
	if (draft.strategyId === '') errors['/strategy/id'] = 'required';
	if (draft.strategyRef === '') errors['/strategy/ref'] = 'required';
	const params = paramsError(draft.paramsText);
	if (params) errors['/params'] = params;
	const seenFeeds = new Set<string>();
	draft.feeds.forEach((row, i) => {
		if (row.feed.trim() === '') errors[`/feeds/${i}/feed`] = 'required';
		else if (seenFeeds.has(row.feed)) errors[`/feeds/${i}/feed`] = 'duplicate';
		else seenFeeds.add(row.feed);
		if (row.mdInstance.trim() === '') errors[`/feeds/${i}/md_instance`] = 'required';
	});
	const seenAccounts = new Set<string>();
	draft.accounts.forEach((row, i) => {
		if (row.apiId === '') {
			errors[`/accounts/${i}/api_id`] = 'required';
			return;
		}
		if (seenAccounts.has(row.apiId)) errors[`/accounts/${i}/api_id`] = 'duplicate';
		else seenAccounts.add(row.apiId);
		if (row.name.trim() === '') errors[`/accounts/${i}/name`] = 'required';
		const path = `/accounts/${i}/algo_ids`;
		const seen = new Set<number>();
		for (const id of row.algoIds) {
			if (seen.has(id)) errors[path] = errors[path] ? `${errors[path]}; duplicate` : 'duplicate';
			seen.add(id);
		}
		if (row.algoDraft.trim() !== '') {
			const n = parseAlgo(row.algoDraft);
			let reason: string | undefined;
			if (n === undefined) reason = 'must be between 1 and 1023';
			else if (seen.has(n)) reason = 'duplicate';
			if (reason) errors[path] = errors[path] ? `${errors[path]}; ${reason}` : reason;
		} else if (row.algoIds.length === 0 && !errors[path]) {
			errors[path] = 'required';
		}
	});
	const numbers: [string, string][] = [
		['/restart/max_restarts', draft.maxRestarts],
		['/restart/window_s', draft.windowS],
		['/deps_timeout_s', draft.depsTimeoutS],
		['/start_timeout_s', draft.startTimeoutS],
		['/recv_window_ms', draft.recvWindowMs],
		['/grace_ms', draft.graceMs],
		['/limits/memory_mb', draft.memoryMb],
		['/limits/gomaxprocs', draft.gomaxprocs],
		['/limits/volume_mb', draft.volumeMb],
		['/limits/hook_timeout_s', draft.hookTimeoutS]
	];
	for (const [path, value] of numbers) {
		const err = uint32Error(value);
		if (err) errors[path] = err;
	}
	return errors;
}

function putUint(target: Record<string, unknown>, key: string, raw: string): void {
	const n = parseUint32(raw);
	if (n !== undefined) target[key] = n;
}

/** JSON body. `paramsText` is inserted as typed, not re-serialized (E10-D033, E10-D086). */
export function sessionRequestBody(draft: Draft): string {
	const body: Record<string, unknown> = {
		name: draft.name,
		sts_instance: draft.stsInstance,
		strategy: { id: draft.strategyId, ref: draft.strategyRef }
	};
	if (draft.feeds.length > 0) {
		body.feeds = draft.feeds.map((row) => ({ feed: row.feed, md_instance: row.mdInstance }));
	}
	if (draft.accounts.length > 0) {
		body.accounts = draft.accounts.map((row) => ({
			name: row.name,
			api_id: Number(row.apiId),
			algo_ids: accountAlgos(row)
		}));
	}
	const restart: Record<string, unknown> = {};
	if (draft.restartMode) restart.mode = draft.restartMode;
	putUint(restart, 'max_restarts', draft.maxRestarts);
	putUint(restart, 'window_s', draft.windowS);
	if (Object.keys(restart).length > 0) body.restart = restart;
	putUint(body, 'deps_timeout_s', draft.depsTimeoutS);
	putUint(body, 'start_timeout_s', draft.startTimeoutS);
	putUint(body, 'recv_window_ms', draft.recvWindowMs);
	putUint(body, 'grace_ms', draft.graceMs);
	const limits: Record<string, unknown> = {};
	putUint(limits, 'memory_mb', draft.memoryMb);
	putUint(limits, 'gomaxprocs', draft.gomaxprocs);
	putUint(limits, 'volume_mb', draft.volumeMb);
	putUint(limits, 'hook_timeout_s', draft.hookTimeoutS);
	if (Object.keys(limits).length > 0) body.limits = limits;

	const json = JSON.stringify(body);
	if (draft.paramsText.trim() === '') return json;
	return `${json.slice(0, -1)},"params":${draft.paramsText}}`;
}

function knownPath(path: string, draft: Draft): boolean {
	if (STATIC_PATHS.has(path)) return true;
	const feed = /^\/feeds\/(\d+)(?:\/(?:feed|md_instance))?$/.exec(path);
	if (feed) return Number(feed[1]) < draft.feeds.length;
	const account = /^\/accounts\/(\d+)(?:\/(?:name|api_id|algo_ids))?$/.exec(path);
	if (account) return Number(account[1]) < draft.accounts.length;
	return false;
}

export type MappedProblem = { fields: Record<string, string>; banner: string[] };

/** Field errors follow the JSON Pointer. Anything else stays at the top (E10-D090). */
export function applyProblem(problem: Problem, draft: Draft): MappedProblem {
	const fields: Record<string, string> = {};
	const banner: string[] = [];
	const errors = problem.errors ?? [];
	if (errors.length === 0) {
		const msg = problem.detail || problem.title;
		if (msg) banner.push(msg);
		return { fields, banner };
	}
	for (const entry of errors) {
		const raw = entry.path ?? '';
		const reason = entry.reason ?? '';
		const path = raw === '' ? '/' : raw.startsWith('/') ? raw : `/${raw}`;
		if (path === '/' || !knownPath(path, draft)) {
			banner.push(path === '/' ? reason : `${raw}: ${reason}`);
			continue;
		}
		fields[path] = fields[path] ? `${fields[path]}; ${reason}` : reason;
	}
	return { fields, banner };
}

export function joinError(local: string | undefined, server: string | undefined): string | undefined {
	if (local && server && local !== server) return `${local}; ${server}`;
	return local || server;
}

/** Wizard step that owns a JSON Pointer. Unmapped paths stay on the confirm step. */
export function stepForPath(path: string): number {
	if (path === '/params' || path.startsWith('/params/')) return 1;
	if (path === '/feeds' || path.startsWith('/feeds/')) return 2;
	if (path === '/accounts' || path.startsWith('/accounts/')) return 3;
	if (
		path === '/restart' ||
		path.startsWith('/restart/') ||
		path === '/limits' ||
		path.startsWith('/limits/') ||
		path === '/deps_timeout_s' ||
		path === '/start_timeout_s' ||
		path === '/recv_window_ms' ||
		path === '/grace_ms'
	) {
		return 4;
	}
	if (path === '/name' || path === '/sts_instance' || path === '/strategy' || path.startsWith('/strategy/')) return 0;
	return 5;
}

export function firstErrorStep(fields: Record<string, string>): number | undefined {
	let step: number | undefined;
	for (const path of Object.keys(fields)) {
		const n = stepForPath(path);
		if (step === undefined || n < step) step = n;
	}
	return step;
}

export function fieldDomId(path: string): string {
	return `f${path.replace(/[^a-z0-9]+/gi, '-')}`;
}
