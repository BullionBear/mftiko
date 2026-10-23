// Client for mftiko-api. Types are generated from contract/openapi/api/v1.yaml
// into src/lib/gen/api/v1.ts (`make openapi`). openapi-fetch is the client (D1044, D1047).
import createClient, { type Middleware } from 'openapi-fetch';
import type { components, paths } from './gen/api/v1';

export type Problem = components['schemas']['Problem'];
export type Freshness = components['schemas']['Freshness'];
export type Me = components['schemas']['Me'];
export type Session = components['schemas']['Session'];
export type SessionDetail = components['schemas']['SessionDetail'];
export type Instance = components['schemas']['Instance'];
export type InstanceDetail = components['schemas']['InstanceDetail'];
export type ControllerStatus = components['schemas']['ControllerStatus'];
export type Strategy = components['schemas']['Strategy'];
export type Account = components['schemas']['Account'];
export type Plane = components['schemas']['Plane'];
export type EventTopic = components['schemas']['EventTopic'];
export type ResetEvent = components['schemas']['ResetEvent'];
export type SessionRemovedEvent = components['schemas']['SessionRemovedEvent'];

// Same-origin cookies (D1009). Dev reaches the API through the Vite proxy (D1078).
export const api = createClient<paths>({
	baseUrl: '/api/v1',
	credentials: 'same-origin'
});

let onUnauthorized: (() => void) | null = null;

/** Called for any API 401 except POST /auth/login (D1076). */
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

api.use(redirectOnUnauthorized);

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

/** Reason for a JSON Pointer field (D1004). `pointer` is `/token` or `token`. */
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
};

const PAGE_CAP = 50;

/** Follow `next_cursor` until the list ends (D1004). Stops if a cursor repeats. */
export async function collectPages<T>(
	load: (cursor?: string) => Promise<{ data?: PageBody<T>; error?: unknown; response: Response }>
): Promise<{ items: T[]; freshness?: Freshness; failure?: ApiFailure }> {
	const items: T[] = [];
	const seen = new Set<string>();
	let cursor: string | undefined;
	let freshness: Freshness | undefined;
	for (let i = 0; i < PAGE_CAP; i++) {
		let result: { data?: PageBody<T>; error?: unknown; response: Response };
		try {
			result = await load(cursor);
		} catch {
			return { items, freshness, failure: { status: 0, message: 'API unreachable' } };
		}
		const failure = failureOf(result.error, result.response);
		if (failure) return { items, freshness, failure };
		const data = result.data;
		if (!data) {
			return { items, freshness, failure: { status: result.response.status, message: 'Empty response' } };
		}
		items.push(...data.items);
		if (data.freshness) freshness = data.freshness;
		if (!data.next_cursor) return { items, freshness };
		if (seen.has(data.next_cursor)) {
			return { items, freshness, failure: { status: 0, message: 'Pagination cursor repeated' } };
		}
		seen.add(data.next_cursor);
		cursor = data.next_cursor;
	}
	return { items, freshness, failure: { status: 0, message: 'Result truncated' } };
}
