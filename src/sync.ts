import { WhoopClient } from './whoop-client.js';
import { WhoopDatabase } from './database.js';

// How far back a full sync fetches from Whoop, and the max look-back for queries.
export const MAX_HISTORY_DAYS = 365;

interface SyncStats {
	cycles: number;
	recoveries: number;
	sleeps: number;
	workouts: number;
	profile: boolean;
	body_measurement: boolean;
}

interface SmartSyncResult {
	type: 'full' | 'quick' | 'skip';
	stats?: SyncStats;
}

export class WhoopSync {
	private readonly client: WhoopClient;
	private readonly db: WhoopDatabase;

	// Smart-sync freshness gate. If the last sync occurred within this window,
	// smartSync() returns type:'skip' instead of hitting the Whoop API.
	// Tuned to 10 minutes (down from 60 min in v3.1.3) to support active usage
	// patterns like GTGs, BP readings, and post-workout checks without forcing
	// sync_data(full:true). Well under Whoop's 100 req/min rate limit.
	private readonly SYNC_FRESHNESS_MS = 10 * 60 * 1000;

	// Every incremental sync re-fetches at least this many days so records Whoop
	// re-scores after the fact (PENDING_SCORE -> SCORED) get updated.
	private readonly MIN_SYNC_DAYS = 7;

	// Extra overlap added on top of the gap since the last successful sync.
	private readonly GAP_BUFFER_DAYS = 2;

	constructor(client: WhoopClient, db: WhoopDatabase) {
		this.client = client;
		this.db = db;
	}

	async syncDays(days = MAX_HISTORY_DAYS): Promise<SyncStats> {
		const endDate = new Date();
		const startDate = new Date();
		startDate.setDate(startDate.getDate() - days);
		const start = startDate.toISOString();
		const end = endDate.toISOString();

		const [cycles, recoveries, sleeps, workouts, profileResult, measurementResult] = await Promise.allSettled([
			this.client.getAllCycles({ start, end }),
			this.client.getAllRecoveries({ start, end }),
			this.client.getAllSleeps({ start, end }),
			this.client.getAllWorkouts({ start, end }),
			this.client.getProfile(),
			this.client.getBodyMeasurement(),
		]);

		const cyclesData = cycles.status === 'fulfilled' ? cycles.value : [];
		const recoveriesData = recoveries.status === 'fulfilled' ? recoveries.value : [];
		const sleepsData = sleeps.status === 'fulfilled' ? sleeps.value : [];
		const workoutsData = workouts.status === 'fulfilled' ? workouts.value : [];

		if (cyclesData.length > 0) this.db.upsertCycles(cyclesData);
		if (recoveriesData.length > 0) this.db.upsertRecoveries(recoveriesData);
		if (sleepsData.length > 0) this.db.upsertSleeps(sleepsData);
		if (workoutsData.length > 0) this.db.upsertWorkouts(workoutsData);

		let profileSynced = false;
		if (profileResult.status === 'fulfilled') {
			this.db.upsertProfile(profileResult.value);
			profileSynced = true;
		}

		let measurementSynced = false;
		if (measurementResult.status === 'fulfilled') {
			this.db.upsertBodyMeasurement(measurementResult.value);
			measurementSynced = true;
		}

		// Only advance last_sync_at when every collection came back. Otherwise the
		// next sync would start from here and permanently skip what just failed.
		const failed = (
			[['cycles', cycles], ['recoveries', recoveries], ['sleeps', sleeps], ['workouts', workouts]] as const
		).filter(([, r]) => r.status === 'rejected').map(([name]) => name);
		if (failed.length > 0) {
			throw new Error(`Sync incomplete, will retry from the last good sync. Failed: ${failed.join(', ')}`);
		}

		this.db.updateSyncState(
			startDate.toISOString().split('T')[0],
			endDate.toISOString().split('T')[0]
		);

		return {
			cycles: cyclesData.length,
			recoveries: recoveriesData.length,
			sleeps: sleepsData.length,
			workouts: workoutsData.length,
			profile: profileSynced,
			body_measurement: measurementSynced,
		};
	}


	async smartSync(): Promise<SmartSyncResult> {
		const state = this.db.getSyncState();
		if (!state.lastSyncAt) {
			const stats = await this.syncDays(MAX_HISTORY_DAYS);
			return { type: 'full', stats };
		}

		const lastSync = new Date(state.lastSyncAt);
		const msSinceSync = Date.now() - lastSync.getTime();

		// Freshness gate: skip API call if last sync was within the window.
		if (msSinceSync < this.SYNC_FRESHNESS_MS) {
			console.log(JSON.stringify({
				event: 'sync_gate_active',
				timestamp: new Date().toISOString(),
				seconds_since_last_sync: Math.floor(msSinceSync / 1000),
				gate_window_seconds: this.SYNC_FRESHNESS_MS / 1000,
				action: 'skipped',
			}));
			return { type: 'skip' };
		}

		// Backfill: cover the whole gap since the last successful sync (plus a
		// buffer), so asking again after weeks or months leaves no holes.
		const gapDays = Math.ceil(msSinceSync / (24 * 60 * 60 * 1000)) + this.GAP_BUFFER_DAYS;
		const days = Math.min(MAX_HISTORY_DAYS, Math.max(this.MIN_SYNC_DAYS, gapDays));
		const stats = await this.syncDays(days);
		return { type: 'quick', stats };
	}
}
