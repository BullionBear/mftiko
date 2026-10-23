// Account create and update (D1098–D1103).
// Secret copies exist only long enough to build one request body. Callers clear page
// state before awaiting, and do not keep the copy (D1101).
import type { AccountKind, KeyType, Problem } from './api';

export type SecretDraft = {
	keyType: KeyType | '';
	key: string;
	secret: string;
	passphrase: string;
};

/** Bumped synchronously so a .pem read that finishes after submit is dropped (D1101). */
export type SecretGuard = { epoch: number };

export function isKeyType(value: string): value is KeyType {
	return value === 'hmac' || value === 'ed25519';
}

/**
 * Why a menu entry cannot be selected (D1098). `supported` is false only when the kind does not
 * trade exactly one venue (D614, D1011); the reason is that rule's `kind_unsupported`, plus `trades`.
 */
export function unsupportedReason(kind: AccountKind): string | undefined {
	if (kind.supported) return undefined;
	const venues = kind.trades.length === 0 ? 'none' : kind.trades.join(', ');
	return `kind_unsupported (${venues})`;
}

export function fieldDomId(path: string): string {
	return `f${path.replace(/[^a-z0-9]+/gi, '-')}`;
}

const KNOWN = new Set([
	'/name',
	'/kind',
	'/td_instance',
	'/revision',
	'/credentials',
	'/credentials/key_type',
	'/credentials/key',
	'/credentials/secret',
	'/credentials/passphrase'
]);

export type MappedProblem = { fields: Record<string, string>; banner: string[] };

/** Field errors follow the JSON Pointer. Anything else stays at the top (D1090). */
export function applyAccountProblem(problem: Problem): MappedProblem {
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
		if (!KNOWN.has(path)) {
			banner.push(path === '/' ? reason : `${raw}: ${reason}`);
			continue;
		}
		fields[path] = fields[path] ? `${fields[path]}; ${reason}` : reason;
	}
	return { fields, banner };
}

/** `api_id` is an integer from 1 to 2^53−1 (D1008). Anything else is not an account URL. */
export function parseApiId(raw: string): number | undefined {
	if (!/^[1-9]\d*$/.test(raw)) return undefined;
	const n = Number(raw);
	if (!Number.isSafeInteger(n)) return undefined;
	return n;
}

export type CredentialsBody = {
	key_type: KeyType;
	key: string;
	secret: string;
	passphrase?: string;
};

/**
 * Credential object for one request. The secret is copied through unchanged, including newlines
 * (D1101). Passphrase is omitted when the kind does not use it, so the API does not see an empty
 * string it would reject (D1064).
 */
export function credentialsFrom(kind: AccountKind, secrets: SecretDraft): CredentialsBody | undefined {
	if (!isKeyType(secrets.keyType)) return undefined;
	const credentials: CredentialsBody = {
		key_type: secrets.keyType,
		key: secrets.key,
		secret: secrets.secret
	};
	if (kind.passphrase) credentials.passphrase = secrets.passphrase;
	return credentials;
}

/** Keep the current key type when the kind still accepts it; otherwise the kind's first type. */
export function nextKeyType(kind: AccountKind, current: string): KeyType | '' {
	if (isKeyType(current) && kind.key_types.includes(current)) return current;
	return kind.key_types[0] ?? '';
}
