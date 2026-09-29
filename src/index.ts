import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { CallToolRequestSchema, ListToolsRequestSchema, ErrorCode, McpError } from '@modelcontextprotocol/sdk/types.js';
import express, { type Request, type Response } from 'express';
import { randomUUID, createHash, timingSafeEqual } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { WhoopClient } from './whoop-client.js';
import { WhoopDatabase } from './database.js';
import { WhoopSync, MAX_HISTORY_DAYS } from './sync.js';

// Single source of truth for server identity. Reads package.json at startup.
// To bump version, edit package.json only. Do not hardcode version strings.
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const packageJson = JSON.parse(
	readFileSync(join(__dirname, '..', 'package.json'), 'utf8')
) as { name: string; version: string };
const SERVER_NAME = packageJson.name;
const SERVER_VERSION = packageJson.version;

interface ToolArguments {
	days?: number;
	full?: boolean;
	id?: string | number;
	cycle_id?: number;
	limit?: number;
	confirm?: boolean;
}

const config = {
	clientId: process.env.WHOOP_CLIENT_ID ?? '',
	clientSecret: process.env.WHOOP_CLIENT_SECRET ?? '',
	redirectUri: process.env.WHOOP_REDIRECT_URI ?? 'http://localhost:3000/callback',
	dbPath: process.env.DB_PATH ?? './whoop.db',
	port: Number.parseInt(process.env.PORT ?? '3000', 10),
	mode: process.env.MCP_MODE ?? 'http',
	// Every /mcp request must carry `Authorization: Bearer <MCP_AUTH_TOKEN>`
	// (a Claude connector "Request header"). Required in http mode.
	mcpAuthToken: process.env.MCP_AUTH_TOKEN ?? '',
};

function isAuthorized(authorizationHeader: string | undefined): boolean {
	const match = /^Bearer (.+)$/.exec(authorizationHeader ?? '');
	const candidate = match ? match[1] : '';
	// Hash both sides so timingSafeEqual always compares equal-length buffers.
	const a = createHash('sha256').update(candidate).digest();
	const b = createHash('sha256').update(config.mcpAuthToken).digest();
	return timingSafeEqual(a, b) && candidate.length > 0;
}

// OAuth `state` values issued by get_auth_url, checked on /callback so nobody
// can bind a different Whoop account to this server. In-memory is fine: single
// instance, and a restart only means re-running get_auth_url.
const OAUTH_STATE_TTL_MS = 15 * 60 * 1000;
const pendingOAuthStates = new Map<string, number>();

function issueOAuthState(): string {
	const now = Date.now();
	for (const [state, issuedAt] of pendingOAuthStates) {
		if (now - issuedAt > OAUTH_STATE_TTL_MS) pendingOAuthStates.delete(state);
	}
	const state = randomUUID();
	pendingOAuthStates.set(state, now);
	return state;
}

function consumeOAuthState(state: string): boolean {
	const issuedAt = pendingOAuthStates.get(state);
	pendingOAuthStates.delete(state);
	return issuedAt !== undefined && Date.now() - issuedAt <= OAUTH_STATE_TTL_MS;
}

const db = new WhoopDatabase(config.dbPath);
const client = new WhoopClient({
	clientId: config.clientId,
	clientSecret: config.clientSecret,
	redirectUri: config.redirectUri,
	onTokenRefresh: tokens => db.saveTokens(tokens),
});

const existingTokens = db.getTokens();
if (existingTokens) {
	client.setTokens(existingTokens);
}

const sync = new WhoopSync(client, db);

// Session TTL controls how long an idle MCP session stays in the transports Map.
// 30 minutes (the original default) was too aggressive: client tokens managed by
// Anthropic's mcp-proxy do not auto-refresh on expiry, so when the server evicted
// a session the client had no recovery path except manual "Refresh tools list".
// 24 hours covers a typical day of idle gaps without artificially killing sessions.
// Memory cost is negligible at single-user scale.
const SESSION_TTL_MS = 24 * 60 * 60 * 1000;
const transports = new Map<string, { transport: StreamableHTTPServerTransport; lastAccess: number }>();

function cleanupStaleSessions(): void {
	const now = Date.now();
	const evicted: string[] = [];
	for (const [sessionId, session] of transports) {
		if (now - session.lastAccess > SESSION_TTL_MS) {
			session.transport.close().catch(() => {});
			transports.delete(sessionId);
			evicted.push(sessionId.slice(0, 8));
		}
	}
	if (evicted.length > 0) {
		console.log(JSON.stringify({
			event: 'session_eviction',
			timestamp: new Date().toISOString(),
			evictedCount: evicted.length,
			evicted,
			remainingSessions: transports.size,
		}));
	}
}

setInterval(cleanupStaleSessions, 5 * 60 * 1000);

// Background sync so history keeps accumulating (and the Whoop refresh token
// keeps getting used) even when nobody asks Claude for data. smartSync is
// gap-aware, so a missed run is simply covered by the next one.
const BACKGROUND_SYNC_MS = 6 * 60 * 60 * 1000;
setInterval(() => {
	const tokens = db.getTokens();
	if (!tokens) return;
	client.setTokens(tokens);
	sync.smartSync()
		.then(result => console.log(JSON.stringify({ event: 'background_sync', timestamp: new Date().toISOString(), type: result.type, stats: result.stats ?? null })))
		.catch(error => console.log(JSON.stringify({ event: 'background_sync_failed', timestamp: new Date().toISOString(), error: error instanceof Error ? error.message : String(error) })));
}, BACKGROUND_SYNC_MS);

function validateDays(value: unknown, defaultDays = 14): number {
	if (value === undefined || value === null) return defaultDays;
	const num = typeof value === 'number' ? value : Number.parseInt(String(value), 10);
	if (Number.isNaN(num) || num < 1) return defaultDays;
	return Math.min(num, MAX_HISTORY_DAYS);
}

function validateBoolean(value: unknown): boolean {
	if (typeof value === 'boolean') return value;
	if (value === 'true') return true;
	return false;
}

function validateLimit(value: unknown, defaultLimit = 25): number {
	if (value === undefined || value === null) return defaultLimit;
	const num = typeof value === 'number' ? value : Number.parseInt(String(value), 10);
	if (Number.isNaN(num) || num < 1) return defaultLimit;
	return Math.min(num, 500);
}

function createMcpServer(): Server {
	const server = new Server(
		{ name: SERVER_NAME, version: SERVER_VERSION },
		{ capabilities: { tools: {} } }
	);

	server.setRequestHandler(ListToolsRequestSchema, async () => ({
		tools: [
			{
				name: 'get_today',
				description: "Returns raw JSON with today's full Whoop state: recovery object (cycle_id, sleep_id, score_state, recovery_score, hrv, rhr, spo2, skin_temp, user_calibrating, created_at), sleep object (id, cycle_id, start, end, all stage durations, sleep_needed breakdown, performance, efficiency, consistency, disturbances, respiratory_rate, score_state, nap), cycle object (id, start, end, strain, kilojoule, avg_hr, max_hr, score_state). All fields included, no formatting applied.",
				inputSchema: { type: 'object', properties: {}, required: [] },
			},
			{
				name: 'get_recovery_trends',
				description: 'Returns raw JSON array of daily recovery records with full fields: date, recovery_score, hrv_rmssd, resting_hr, spo2, skin_temp, user_calibrating, score_state, sleep_id, created_at, synced_at. Includes days_requested and record_count.',
				inputSchema: {
					type: 'object',
					properties: { days: { type: 'number', description: 'Number of days to analyze (default: 14, max: 365)' } },
					required: [],
				},
			},
			{
				name: 'get_sleep_analysis',
				description: 'Returns raw JSON array of daily sleep records with full fields: id (UUID), cycle_id, start_time, end_time, is_nap, score_state, all stage durations (in_bed, awake, light, deep, REM, no_data), sleep_cycle_count, disturbance_count, performance, efficiency, consistency, respiratory_rate, sleep_needed breakdown (baseline, debt, strain, nap), synced_at. Includes days_requested and record_count.',
				inputSchema: {
					type: 'object',
					properties: { days: { type: 'number', description: 'Number of days to analyze (default: 14, max: 365)' } },
					required: [],
				},
			},
			{
				name: 'get_strain_history',
				description: 'Returns raw JSON array of daily cycle records with full fields: id, user_id, start_time, end_time, score_state, strain, kilojoule, avg_hr, max_hr, synced_at. Includes days_requested and record_count.',
				inputSchema: {
					type: 'object',
					properties: { days: { type: 'number', description: 'Number of days to analyze (default: 14, max: 365)' } },
					required: [],
				},
			},
			{
				name: 'get_workouts',
				description: 'Returns raw JSON array of workout records with full fields: id (UUID), sport_id, sport_name, start_time, end_time, score_state, strain, avg_hr, max_hr, kilojoule, percent_recorded, distance_meter, altitude_gain_meter, altitude_change_meter, all 6 zone_milli durations. Includes days_requested, limit_requested, record_count.',
				inputSchema: {
					type: 'object',
					properties: {
						days: { type: 'number', description: 'Number of days to look back (default: 14, max: 365)' },
						limit: { type: 'number', description: 'Maximum number of workouts to return (default: 25, max: 500)' },
					},
					required: [],
				},
			},
			{
				name: 'get_workout_detail',
				description: 'Returns raw JSON object for a single workout by UUID with all fields including score_state, percent_recorded, all zone durations, distance and altitude data.',
				inputSchema: {
					type: 'object',
					properties: { id: { type: 'string', description: 'Workout UUID' } },
					required: ['id'],
				},
			},
			{
				name: 'get_cycle_detail',
				description: 'Returns raw JSON object for a single physiological cycle by ID with all fields: start_time, end_time, score_state, strain, kilojoule, avg_hr, max_hr.',
				inputSchema: {
					type: 'object',
					properties: { id: { type: 'number', description: 'Cycle ID' } },
					required: ['id'],
				},
			},
			{
				name: 'get_sleep_detail',
				description: 'Returns raw JSON object for a single sleep record by UUID with all fields: cycle_id, start_time, end_time, is_nap, score_state, all stage durations, sleep_needed breakdown, performance, efficiency, consistency, disturbance_count, respiratory_rate.',
				inputSchema: {
					type: 'object',
					properties: { id: { type: 'string', description: 'Sleep UUID' } },
					required: ['id'],
				},
			},
			{
				name: 'get_sleep_for_cycle',
				description: 'Returns raw JSON object for the sleep linked to a specific cycle ID, with all sleep fields.',
				inputSchema: {
					type: 'object',
					properties: { cycle_id: { type: 'number', description: 'Cycle ID' } },
					required: ['cycle_id'],
				},
			},
			{
				name: 'get_recovery_for_cycle',
				description: 'Returns raw JSON object for the recovery linked to a specific cycle ID, with all fields: sleep_id, score_state, recovery_score, hrv_rmssd, resting_hr, spo2, skin_temp, user_calibrating, created_at.',
				inputSchema: {
					type: 'object',
					properties: { cycle_id: { type: 'number', description: 'Cycle ID' } },
					required: ['cycle_id'],
				},
			},
			{
				name: 'get_profile',
				description: 'Returns raw JSON object for the authenticated user profile: user_id, email, first_name, last_name, synced_at.',
				inputSchema: { type: 'object', properties: {}, required: [] },
			},
			{
				name: 'get_body_measurement',
				description: 'Returns raw JSON object: height_meter, weight_kilogram, max_heart_rate, synced_at. Imperial conversions handled by the consuming agent.',
				inputSchema: { type: 'object', properties: {}, required: [] },
			},
			{
				name: 'sync_data',
				description: 'Triggers a Whoop data sync. Returns raw JSON with status (complete or skipped), full_sync flag, and stats object (cycles, recoveries, sleeps, workouts, profile, body_measurement counts).',
				inputSchema: {
					type: 'object',
					properties: { full: { type: 'boolean', description: 'Force a full 365-day sync (default: false)' } },
					required: [],
				},
			},
			{
				name: 'get_auth_url',
				description: 'Get the Whoop authorization URL to connect your account.',
				inputSchema: { type: 'object', properties: {}, required: [] },
			},
			{
				name: 'revoke_access',
				description: 'Revoke the Whoop OAuth access token and clear all stored tokens. Requires explicit confirmation.',
				inputSchema: {
					type: 'object',
					properties: {
						confirm: { type: 'boolean', description: 'Must be set to true to confirm revocation. Defaults to false.' },
					},
					required: ['confirm'],
				},
			},
		],
	}));

	server.setRequestHandler(CallToolRequestSchema, async request => {
		const { name, arguments: args } = request.params;
		const typedArgs = (args ?? {}) as ToolArguments;

		try {
			const dataTools = [
				'get_today', 'get_recovery_trends', 'get_sleep_analysis', 'get_strain_history',
				'get_workouts', 'get_workout_detail', 'get_cycle_detail', 'get_sleep_detail',
				'get_sleep_for_cycle', 'get_recovery_for_cycle',
				'get_profile', 'get_body_measurement',
			];
			if (dataTools.includes(name)) {
				const tokens = db.getTokens();
				if (!tokens) {
					return { content: [{ type: 'text', text: 'Not authenticated with Whoop. Use get_auth_url to authorize first.' }] };
				}
				client.setTokens(tokens);
				try {
					await sync.smartSync();
				} catch {
					// Continue with cached data
				}
			}

			switch (name) {
				case 'get_today': {
					const recovery = db.getLatestRecovery();
					const sleep = db.getLatestSleep();
					const cycle = db.getLatestCycle();

					if (!recovery && !sleep && !cycle) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: 'No data available. Try running sync_data first.' }) }] };
					}

					return {
						content: [{
							type: 'text',
							text: JSON.stringify({
								recovery,
								sleep,
								cycle,
							}, null, 2),
						}],
					};
				}

				case 'get_recovery_trends': {
					const days = validateDays(typedArgs.days);
					const trends = db.getRecoveryTrends(days);

					if (trends.length === 0) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: 'No recovery data available for the requested period.', days }) }] };
					}

					return {
						content: [{
							type: 'text',
							text: JSON.stringify({
								days_requested: days,
								record_count: trends.length,
								records: trends,
							}, null, 2),
						}],
					};
				}

				case 'get_sleep_analysis': {
					const days = validateDays(typedArgs.days);
					const trends = db.getSleepTrends(days);

					if (trends.length === 0) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: 'No sleep data available for the requested period.', days }) }] };
					}

					return {
						content: [{
							type: 'text',
							text: JSON.stringify({
								days_requested: days,
								record_count: trends.length,
								records: trends,
							}, null, 2),
						}],
					};
				}

				case 'get_strain_history': {
					const days = validateDays(typedArgs.days);
					const trends = db.getStrainTrends(days);

					if (trends.length === 0) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: 'No strain data available for the requested period.', days }) }] };
					}

					return {
						content: [{
							type: 'text',
							text: JSON.stringify({
								days_requested: days,
								record_count: trends.length,
								records: trends,
							}, null, 2),
						}],
					};
				}

				case 'get_workouts': {
					const days = validateDays(typedArgs.days);
					const limit = validateLimit(typedArgs.limit);
					const workouts = db.getRecentWorkouts(days, limit);

					if (workouts.length === 0) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: `No workouts in the last ${days} days.`, days, limit }) }] };
					}

					return {
						content: [{
							type: 'text',
							text: JSON.stringify({
								days_requested: days,
								limit_requested: limit,
								record_count: workouts.length,
								records: workouts,
							}, null, 2),
						}],
					};
				}

				case 'get_workout_detail': {
					const id = String(typedArgs.id ?? '');
					if (!id) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: 'Workout ID is required.' }) }] };
					}
					const w = db.getWorkoutById(id);
					if (!w) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: `Workout ${id} not found in local database. Try running sync_data.`, id }) }] };
					}

					return { content: [{ type: 'text', text: JSON.stringify(w, null, 2) }] };
				}

				case 'get_cycle_detail': {
					const id = Number(typedArgs.id);
					if (!id || Number.isNaN(id)) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: 'Cycle ID (number) is required.' }) }] };
					}
					const c = db.getCycleById(id);
					if (!c) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: `Cycle ${id} not found in local database. Try running sync_data.`, id }) }] };
					}

					return { content: [{ type: 'text', text: JSON.stringify(c, null, 2) }] };
				}

				case 'get_sleep_detail': {
					const id = String(typedArgs.id ?? '');
					if (!id) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: 'Sleep ID is required.' }) }] };
					}
					const s = db.getSleepById(id);
					if (!s) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: `Sleep ${id} not found in local database. Try running sync_data.`, id }) }] };
					}

					return { content: [{ type: 'text', text: JSON.stringify(s, null, 2) }] };
				}

				case 'get_sleep_for_cycle': {
					const cycleId = Number(typedArgs.cycle_id);
					if (!cycleId || Number.isNaN(cycleId)) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: 'Cycle ID (number) is required.' }) }] };
					}
					const s = db.getSleepForCycle(cycleId);
					if (!s) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: `No sleep found for cycle ${cycleId}. Try running sync_data.`, cycle_id: cycleId }) }] };
					}

					return { content: [{ type: 'text', text: JSON.stringify(s, null, 2) }] };
				}

				case 'get_recovery_for_cycle': {
					const cycleId = Number(typedArgs.cycle_id);
					if (!cycleId || Number.isNaN(cycleId)) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: 'Cycle ID (number) is required.' }) }] };
					}
					const r = db.getRecoveryForCycle(cycleId);
					if (!r) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: `No recovery found for cycle ${cycleId}. Try running sync_data.`, cycle_id: cycleId }) }] };
					}

					return { content: [{ type: 'text', text: JSON.stringify(r, null, 2) }] };
				}

				case 'get_profile': {
					const profile = db.getProfile();
					if (!profile) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: 'Profile not available. Try running sync_data.' }) }] };
					}

					return { content: [{ type: 'text', text: JSON.stringify(profile, null, 2) }] };
				}

				case 'get_body_measurement': {
					const measurement = db.getBodyMeasurement();
					if (!measurement) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: 'Body measurement not available. Try running sync_data.' }) }] };
					}

					return { content: [{ type: 'text', text: JSON.stringify(measurement, null, 2) }] };
				}

				case 'sync_data': {
					const tokens = db.getTokens();
					if (!tokens) {
						return { content: [{ type: 'text', text: JSON.stringify({ error: 'Not authenticated with Whoop. Use get_auth_url to authorize first.' }) }] };
					}
					client.setTokens(tokens);

					const full = validateBoolean(typedArgs.full);
					let stats;
					let skipped = false;

					if (full) {
						stats = await sync.syncDays(MAX_HISTORY_DAYS);
					} else {
						const result = await sync.smartSync();
						if (result.type === 'skip') {
							skipped = true;
						} else {
							stats = result.stats;
						}
					}

					return {
						content: [{
							type: 'text',
							text: JSON.stringify({
								status: skipped ? 'skipped' : 'complete',
								message: skipped ? 'Data is already up to date (synced within the last 10 minutes).' : 'Sync complete',
								full_sync: full,
								stats: stats ?? null,
							}, null, 2),
						}],
					};
				}

				case 'get_auth_url': {
					const scopes = ['read:profile', 'read:body_measurement', 'read:cycles', 'read:recovery', 'read:sleep', 'read:workout', 'offline'];
					const url = client.getAuthorizationUrl(scopes, issueOAuthState());
					return {
						content: [{
							type: 'text',
							text: `To authorize with Whoop:\n\n1. Visit: ${url}\n2. Log in and authorize\n3. You'll be redirected back automatically\n\nRedirect URI: ${config.redirectUri}`,
						}],
					};
				}

				case 'revoke_access': {
					if (!typedArgs.confirm) {
						return {
							content: [{
								type: 'text',
								text: 'This will revoke your Whoop OAuth access and clear all stored tokens. Re-authorization will be required to use the server again. To proceed, call this tool with confirm: true.',
							}],
						};
					}

					const tokens = db.getTokens();
					if (!tokens) {
						return { content: [{ type: 'text', text: 'No active tokens to revoke.' }] };
					}

					client.setTokens(tokens);
					try {
						await client.revokeAccess();
						db.clearTokens();
						return {
							content: [{
								type: 'text',
								text: 'Access revoked successfully. All tokens cleared. Use get_auth_url to re-authorize.',
							}],
						};
					} catch (err) {
						// Even if Whoop rejects the revoke (token already expired etc), clear local state
						db.clearTokens();
						const message = err instanceof Error ? err.message : 'Unknown error';
						return {
							content: [{
								type: 'text',
								text: `Local tokens cleared. Remote revoke response: ${message}. Use get_auth_url to re-authorize.`,
							}],
						};
					}
				}

				default:
					throw new McpError(ErrorCode.MethodNotFound, `Unknown tool: ${name}`);
			}
		} catch (error) {
			const message = error instanceof Error ? error.message : 'Unknown error';
			return { content: [{ type: 'text', text: `Error: ${message}` }], isError: true };
		}
	});

	return server;
}

async function main(): Promise<void> {
	if (config.mode === 'stdio') {
		const server = createMcpServer();
		const transport = new StdioServerTransport();
		await server.connect(transport);
		process.stderr.write('Whoop MCP server running on stdio\n');
	} else {
		if (config.mcpAuthToken.length < 32) {
			throw new Error('MCP_AUTH_TOKEN must be set to at least 32 characters in http mode');
		}

		const app = express();
		app.use((req, res, next) => {
			if (req.path === '/mcp') return next();
			express.json()(req, res, next);
		});

		app.get('/callback', async (req: Request, res: Response) => {
			const code = req.query.code as string | undefined;
			const state = req.query.state as string | undefined;
			if (!code) {
				res.status(400).send('Missing authorization code');
				return;
			}
			if (!state || !consumeOAuthState(state)) {
				res.status(400).send('Invalid or expired state. Run get_auth_url again.');
				return;
			}

			try {
				const tokens = await client.exchangeCodeForTokens(code);
				db.saveTokens(tokens);
				sync.syncDays(MAX_HISTORY_DAYS).catch(() => {});
				res.send('Authorization successful! You can close this window.');
			} catch {
				res.status(500).send('Authorization failed. Please try again.');
			}
		});

		app.get('/health', (_req: Request, res: Response) => {
			res.json({ status: 'ok', authenticated: Boolean(db.getTokens()) });
		});

		app.get('/.well-known/oauth-protected-resource', (_req: Request, res: Response) => {
			res.status(200).json({});
		});

		app.get('/.well-known/oauth-protected-resource/mcp', (_req: Request, res: Response) => {
			res.status(200).json({});
		});
		app.get('/.well-known/oauth-authorization-server', (_req: Request, res: Response) => {
			res.status(200).json({});
		});

		app.post('/register', (_req: Request, res: Response) => {
			res.status(200).json({});
		});

		app.all('/mcp', async (req: Request, res: Response) => {
			if (!isAuthorized(req.headers.authorization)) {
				res.status(401).json({ error: 'unauthorized' });
				return;
			}

			const sessionId = req.headers['mcp-session-id'] as string | undefined;
			const requestStart = Date.now();

			if (req.method === 'DELETE' && sessionId && transports.has(sessionId)) {
				const session = transports.get(sessionId)!;
				await session.transport.close();
				transports.delete(sessionId);
				console.log(JSON.stringify({
					event: 'mcp_request',
					timestamp: new Date().toISOString(),
					method: 'DELETE',
					sessionIdPrefix: sessionId.slice(0, 8),
					action: 'session_closed',
					activeSessions: transports.size,
					durationMs: Date.now() - requestStart,
				}));
				res.status(200).send('Session closed');
				return;
			}

			if (req.method === 'GET') {
				res.status(200).json({ name: SERVER_NAME, version: SERVER_VERSION });
				return;
			}

			if (req.method === 'POST') {
				// Buffer the raw body so we can inspect it AND pass it to the transport.
				// The transport reads the body as a stream; without buffering we cannot
				// look at the JSON-RPC method before deciding how to route the request.
				const chunks: Buffer[] = [];
				for await (const chunk of req) {
					chunks.push(chunk as Buffer);
				}
				const rawBody = Buffer.concat(chunks);

				interface JsonRpcRequest {
					method?: string;
					id?: string | number | null;
				}
				let parsedBody: JsonRpcRequest | null = null;
				try {
					parsedBody = JSON.parse(rawBody.toString('utf8')) as JsonRpcRequest;
				} catch {
					parsedBody = null;
				}

				const isInitializeRequest = parsedBody?.method === 'initialize';
				const sessionKnown = sessionId ? transports.has(sessionId) : false;
				const bodyMethod = parsedBody?.method ?? null;

				// Reject non-initialize requests with unknown session IDs cleanly.
				// HTTP 404 per MCP spec signals "session not found" so a compliant
				// client should re-issue initialize and retry. HTTP 400 (the old
				// behaviour, returned by SDK's "Server not initialized" error) does
				// not tell the client to recover.
				if (sessionId && !sessionKnown && !isInitializeRequest) {
					console.log(JSON.stringify({
						event: 'mcp_request',
						timestamp: new Date().toISOString(),
						method: 'POST',
						sessionIdPrefix: sessionId.slice(0, 8),
						sessionKnown: false,
						bodyMethod,
						action: 'rejected_unknown_session',
						httpStatus: 404,
						activeSessions: transports.size,
						durationMs: Date.now() - requestStart,
					}));
					res.status(404).json({
						jsonrpc: '2.0',
						error: {
							code: -32001,
							message: 'Session not found. Please reinitialize.',
						},
						id: parsedBody?.id ?? null,
					});
					return;
				}

				let transport: StreamableHTTPServerTransport;
				let action: string;

				if (sessionId && sessionKnown) {
					const session = transports.get(sessionId)!;
					session.lastAccess = Date.now();
					transport = session.transport;
					action = 'reused_session';
				} else {
					const newSessionId = randomUUID();
					transport = new StreamableHTTPServerTransport({
						sessionIdGenerator: () => newSessionId,
						onsessioninitialized: (id) => {
							transports.set(id, { transport, lastAccess: Date.now() });
						},
					});

					const server = createMcpServer();
					await server.connect(transport);
					transports.set(newSessionId, { transport, lastAccess: Date.now() });
					action = isInitializeRequest ? 'new_session_initialize' : 'new_session_implicit';
				}

				// Replay the buffered body back onto req so the transport can consume it.
				// The transport reads req as a stream; we already consumed it above.
				const { Readable } = await import('node:stream');
				const replayStream = Readable.from([rawBody]);
				Object.assign(req, replayStream);
				(req as unknown as { readable: boolean }).readable = true;

				console.log(JSON.stringify({
					event: 'mcp_request',
					timestamp: new Date().toISOString(),
					method: 'POST',
					sessionIdPrefix: sessionId ? sessionId.slice(0, 8) : null,
					sessionKnown,
					bodyMethod,
					isInitialize: isInitializeRequest,
					action,
					activeSessions: transports.size,
					durationMs: Date.now() - requestStart,
				}));

				await transport.handleRequest(req, res);
				return;
			}

			res.status(405).send('Method not allowed');
		});

		app.get('/sse', (_req: Request, res: Response) => {
			res.status(410).send('SSE endpoint deprecated. Use /mcp with Streamable HTTP transport.');
		});

		const server = app.listen(config.port, '0.0.0.0', () => {
			process.stdout.write(`Whoop MCP server running on http://0.0.0.0:${config.port}\n`);
		});

		const shutdown = (): void => {
			process.stdout.write('\nShutting down...\n');
			for (const [, session] of transports) {
				session.transport.close().catch(() => {});
			}
			transports.clear();
			db.close();
			server.close(() => process.exit(0));
		};

		process.on('SIGTERM', shutdown);
		process.on('SIGINT', shutdown);
	}
}

main().catch(error => {
	process.stderr.write(`Fatal error: ${error}\n`);
	process.exit(1);
});
