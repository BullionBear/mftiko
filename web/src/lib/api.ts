// Client for mftiko-api. Types are generated from contract/openapi/api/v1.yaml
// into src/lib/gen/api/v1.ts (`make openapi`). openapi-fetch is the client (E10-D044, E10-D047).
import createClient, { type Middleware } from 'openapi-fetch';
import type { components, paths } from './gen/api/v1';

export type Problem = components['schemas']['Problem'];
export type Freshness = components['schemas']['Freshness'];
export type Me = components['schemas']['Me'];
export type Session = components['schemas']['Session'];
export type SessionDetail = components['schemas']['SessionDetail'];
export type Instance = components['schemas']['Instance'];
export type InstanceDetail = components['schemas']['InstanceDetail'];
export type InstanceSession = components['schemas']['InstanceSession'];
export type ControllerStatus = components['schemas']['ControllerStatus'];
export type AgentStatus = components['schemas']['AgentStatus'];
export type Worker = components['schemas']['Worker'];
export type DemandStatus = components['schemas']['DemandStatus'];
export type Dependency = components['schemas']['Dependency'];
export type StrategyTag = components['schemas']['StrategyTag'];
export type StrategyRepository = components['schemas']['StrategyRepository'];
export type Account = components['schemas']['Account'];
export type AccountKind = components['schemas']['AccountKind'];
export type KeyType = components['schemas']['KeyType'];
export type Plane = components['schemas']['Plane'];
export type EventTopic = components['schemas']['EventTopic'];
export type ResetEvent = components['schemas']['ResetEvent'];
export type SessionRemovedEvent = components['schemas']['SessionRemovedEvent'];

// Same-origin cookies (E10-D009). Dev reaches the API through the Vite proxy (E10-D078).
export const api = createClient<paths>({
	baseUrl: '/api/v1',
	credentials: 'same-origin'
});

let onUnauthorized: (() => void) | null = null;

/** Called for any API 401 except POST /auth/login (E10-D076). */
export function setUnauthorizedHandler(fn: () => void): void {
	onUnauthorized = fn;
}

const redirectOnUnauthorized: Middleware = {
	onResponse({ request, response }) {
		if (response.status !== 401) return response;
		const path = new URL(request.url).pathname;
		if (path.endsWith('/auth/login')) return response;
		onUnauthorized?.();
		return response;
	}
};

// The API refuses a cookie write without Content-Type: application/json (CSRF, E10-D009, E10-D110).
// openapi-fetch sets it only when there is a body, so end, delete and logout need this.
const jsonWrites: Middleware = {
	onRequest({ request }) {
		if (request.method !== 'GET' && request.method !== 'HEAD' && !request.headers.has('Content-Type')) {
			request.headers.set('Content-Type', 'application/json');
		}
		return request;
	}
};

api.use(jsonWrites, redirectOnUnauthorized);

/** POST /sessions with the body bytes unchanged (E10-D033, E10-D086) and one Idempotency-Key (E10-D089). */
export function createSession(body: string, idempotencyKey: string) {
	return api.POST('/sessions', {
		body: { name: '', sts_instance: 'a', strategy: { repository: '', ref: '' } },
		bodySerializer: () => body,
		params: { header: { 'Idempotency-Key': idempotencyKey } }
	});
}

export function isProblem(value: unknown): value is Problem {
	if (!value || typeof value !== 'object') return false;
	const record = value as Record<string, unknown>;
	return typeof record.status === 'number' && typeof record.code === 'string' && typeof record.title === 'string';
}

export type ApiFailure = {
	status: number;
	problem?: Problem;
	message: string;
};

export function failureOf(error: unknown, response: Response): ApiFailure | undefined {
	if (response.ok) return undefined;
	const problem = isProblem(error) ? error : undefined;
	const message = problem?.detail || problem?.title || `HTTP ${response.status}`;
	return { status: response.status, problem, message };
}

/** Reason for a JSON Pointer field (E10-D004). `pointer` is `/token` or `token`. */
export function reasonFor(problem: Problem | null | undefined, pointer: string): string | undefined {
	if (!problem?.errors?.length) return undefined;
	const want = pointer.startsWith('/') ? pointer : `/${pointer}`;
	return problem.errors.find((entry) => entry.path === want)?.reason;
}

/** A same-origin path to return to after login. Anything else becomes `/`. */
export function safeNext(raw: string | null): string {
	if (raw === null || raw.length === 0 || raw.length > 2048) return '/';
	if (!raw.startsWith('/') || raw.startsWith('//') || raw.startsWith('/\\')) return '/';
	if (raw.includes('\\') || raw.includes('://')) return '/';
	if (/[\u0000-\u001F\u007F]/.test(raw)) return '/';
	if (raw === '/login' || raw.startsWith('/login?') || raw.startsWith('/login#')) return '/';
	return raw;
}

type PageBody<T> = {
	items: T[];
	next_cursor?: string;
	freshness?: Freshness;
	latest?: StrategyTag;
	repository?: string;
};

const PAGE_CAP = 50;

export type CollectedPages<T> = {
	items: T[];
	freshness?: Freshness;
	latest?: StrategyTag;
	repository?: string;
	failure?: ApiFailure;
};

/** Follow `next_cursor` until the list ends (E10-D004). Stops if a cursor repeats.
 * `latest` and `repository` are kept from the first page that sends them (E10-D121). */
export async function collectPages<T>(
	load: (cursor?: string) => Promise<{ data?: PageBody<T>; error?: unknown; response: Response }>
): Promise<CollectedPages<T>> {
	const items: T[] = [];
	const seen = new Set<string>();
	let cursor: string | undefined;
	let freshness: Freshness | undefined;
	let latest: StrategyTag | undefined;
	let repository: string | undefined;
	const page = (): CollectedPages<T> => ({ items, freshness, latest, repository });
	for (let i = 0; i < PAGE_CAP; i++) {
		let result: { data?: PageBody<T>; error?: unknown; response: Response };
		try {
			result = await load(cursor);
		} catch {
			return { ...page(), failure: { status: 0, message: 'API unreachable' } };
		}
		const failure = failureOf(result.error, result.response);
		if (failure) return { ...page(), failure };
		const data = result.data;
		if (!data) {
			return { ...page(), failure: { status: result.response.status, message: 'Empty response' } };
		}
		items.push(...data.items);
		if (data.freshness) freshness = data.freshness;
		if (data.latest && latest === undefined) latest = data.latest;
		if (data.repository && repository === undefined) repository = data.repository;
		if (!data.next_cursor) return page();
		if (seen.has(data.next_cursor)) {
			return { ...page(), failure: { status: 0, message: 'Pagination cursor repeated' } };
		}
		seen.add(data.next_cursor);
		cursor = data.next_cursor;
	}
	return { ...page(), failure: { status: 0, message: 'Result truncated' } };
}
