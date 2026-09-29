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

	// Full-sync threshold. If the last sync is older than this, smartSync()
	// performs a full-history sync (MAX_HISTORY_DAYS) instead of the default 7-day quick sync.
	private readonly FULL_SYNC_THRESHOLD_HOURS = 24;

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

	async quickSync(): Promise<SyncStats> {
		return this.syncDays(7);
	}

	needsFullSync(): boolean {
		const state = this.db.getSyncState();
		if (!state.lastSyncAt) return true;
		const lastSync = new Date(state.lastSyncAt);
		const hoursSinceSync = (Date.now() - lastSync.getTime()) / (1000 * 60 * 60);
		return hoursSinceSync > this.FULL_SYNC_THRESHOLD_HOURS;
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

		const stats = await this.quickSync();
		return { type: 'quick', stats };
	}
}
