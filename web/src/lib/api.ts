// Client for mftiko-api. Request and response types come from src/lib/gen (`make proto`);
// import them with the .js suffix, e.g. '#lib/gen/mftiko/state/v1/state_pb.js'.
export async function get<T>(path: string): Promise<T> {
	const res = await fetch(`/api${path}`);
	if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
	return (await res.json()) as T;
}
