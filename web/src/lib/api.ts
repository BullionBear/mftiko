// Client for mftiko-api. Types are generated from contract/openapi/api/v1.yaml
// into src/lib/gen/api/v1.ts (`make openapi`). openapi-fetch is the client (D1044, D1047).
import createClient from 'openapi-fetch';
import type { paths } from './gen/api/v1';

// Paths in the spec are relative to the server url `/api/v1`.
export const api = createClient<paths>({ baseUrl: '/api/v1' });
