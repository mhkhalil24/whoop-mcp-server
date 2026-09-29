import Database from 'better-sqlite3';
import { encrypt, decrypt, isEncrypted } from './crypto.js';
import type {
	WhoopTokens,
	WhoopCycle,
	WhoopRecovery,
	WhoopSleep,
	WhoopWorkout,
	WhoopUser,
	WhoopBodyMeasurement,
	DbCycle,
	DbRecovery,
	DbSleep,
	DbWorkout,
	DbProfile,
	DbBodyMeasurement,
} from './types.js';

interface TokenRow {
	id: number;
	access_token: string;
	refresh_token: string;
	expires_at: number;
	updated_at: string;
}

interface SyncStateRow {
	id: number;
	last_sync_at: string | null;
	oldest_synced_date: string | null;
	newest_synced_date: string | null;
}

export class WhoopDatabase {
	private db: Database.Database;

	constructor(dbPath = './whoop.db') {
		this.db = new Database(dbPath);
		this.db.pragma('journal_mode = WAL');
		this.initSchema();
		this.runMigrations();
	}

	private initSchema(): void {
		this.db.exec(`
			CREATE TABLE IF NOT EXISTS tokens (
				id INTEGER PRIMARY KEY CHECK (id = 1),
				access_token TEXT NOT NULL,
				refresh_token TEXT NOT NULL,
				expires_at INTEGER NOT NULL,
				updated_at TEXT DEFAULT CURRENT_TIMESTAMP
			);

			CREATE TABLE IF NOT EXISTS sync_state (
				id INTEGER PRIMARY KEY CHECK (id = 1),
				last_sync_at TEXT,
				oldest_synced_date TEXT,
				newest_synced_date TEXT
			);

			CREATE TABLE IF NOT EXISTS cycles (
				id INTEGER PRIMARY KEY,
				user_id INTEGER NOT NULL,
				start_time TEXT NOT NULL,
				end_time TEXT,
				score_state TEXT NOT NULL,
				strain REAL,
				kilojoule REAL,
				avg_hr INTEGER,
				max_hr INTEGER,
				synced_at TEXT DEFAULT CURRENT_TIMESTAMP
			);

			CREATE TABLE IF NOT EXISTS recovery (
				id INTEGER PRIMARY KEY,
				user_id INTEGER NOT NULL,
				sleep_id TEXT,
				created_at TEXT NOT NULL,
				score_state TEXT NOT NULL,
				recovery_score INTEGER,
				resting_hr INTEGER,
				hrv_rmssd REAL,
				spo2 REAL,
				skin_temp REAL,
				user_calibrating INTEGER,
				synced_at TEXT DEFAULT CURRENT_TIMESTAMP
			);

			CREATE TABLE IF NOT EXISTS sleep (
				id TEXT PRIMARY KEY,
				user_id INTEGER NOT NULL,
				cycle_id INTEGER,
				start_time TEXT NOT NULL,
				end_time TEXT NOT NULL,
				is_nap INTEGER NOT NULL DEFAULT 0,
				score_state TEXT NOT NULL,
				total_in_bed_milli INTEGER,
				total_awake_milli INTEGER,
				total_light_milli INTEGER,
				total_deep_milli INTEGER,
				total_rem_milli INTEGER,
				total_no_data_milli INTEGER,
				sleep_cycle_count INTEGER,
				disturbance_count INTEGER,
				sleep_performance REAL,
				sleep_efficiency REAL,
				sleep_consistency REAL,
				respiratory_rate REAL,
				sleep_needed_baseline_milli INTEGER,
				sleep_needed_debt_milli INTEGER,
				sleep_needed_strain_milli INTEGER,
				sleep_needed_nap_milli INTEGER,
				synced_at TEXT DEFAULT CURRENT_TIMESTAMP
			);

			CREATE TABLE IF NOT EXISTS workouts (
				id TEXT PRIMARY KEY,
				user_id INTEGER NOT NULL,
				sport_id INTEGER,
				sport_name TEXT,
				start_time TEXT NOT NULL,
				end_time TEXT NOT NULL,
				score_state TEXT NOT NULL,
				strain REAL,
				avg_hr INTEGER,
				max_hr INTEGER,
				kilojoule REAL,
				percent_recorded REAL,
				distance_meter REAL,
				altitude_gain_meter REAL,
				altitude_change_meter REAL,
				zone_zero_milli INTEGER,
				zone_one_milli INTEGER,
				zone_two_milli INTEGER,
				zone_three_milli INTEGER,
				zone_four_milli INTEGER,
				zone_five_milli INTEGER,
				synced_at TEXT DEFAULT CURRENT_TIMESTAMP
			);

			CREATE TABLE IF NOT EXISTS profile (
				id INTEGER PRIMARY KEY CHECK (id = 1),
				user_id INTEGER NOT NULL,
				email TEXT NOT NULL,
				first_name TEXT NOT NULL,
				last_name TEXT NOT NULL,
				synced_at TEXT DEFAULT CURRENT_TIMESTAMP
			);

			CREATE TABLE IF NOT EXISTS body_measurement (
				id INTEGER PRIMARY KEY CHECK (id = 1),
				height_meter REAL NOT NULL,
				weight_kilogram REAL NOT NULL,
				max_heart_rate INTEGER NOT NULL,
				synced_at TEXT DEFAULT CURRENT_TIMESTAMP
			);

			CREATE INDEX IF NOT EXISTS idx_cycles_start ON cycles(start_time);
			CREATE INDEX IF NOT EXISTS idx_recovery_created ON recovery(created_at);
			CREATE INDEX IF NOT EXISTS idx_recovery_sleep ON recovery(sleep_id);
			CREATE INDEX IF NOT EXISTS idx_sleep_start ON sleep(start_time);
			CREATE INDEX IF NOT EXISTS idx_sleep_cycle ON sleep(cycle_id);
			CREATE INDEX IF NOT EXISTS idx_workouts_start ON workouts(start_time);
			CREATE INDEX IF NOT EXISTS idx_workouts_sport ON workouts(sport_id);

			INSERT OR IGNORE INTO sync_state (id) VALUES (1);
		`);
	}

	private runMigrations(): void {
		const sleepCols = this.db.prepare("PRAGMA table_info(sleep)").all() as { name: string }[];
		const sleepColNames = new Set(sleepCols.map(c => c.name));
		if (!sleepColNames.has('total_no_data_milli')) {
			this.db.exec('ALTER TABLE sleep ADD COLUMN total_no_data_milli INTEGER');
		}
		if (!sleepColNames.has('sleep_cycle_count')) {
			this.db.exec('ALTER TABLE sleep ADD COLUMN sleep_cycle_count INTEGER');
		}
		if (!sleepColNames.has('disturbance_count')) {
			this.db.exec('ALTER TABLE sleep ADD COLUMN disturbance_count INTEGER');
		}
		if (!sleepColNames.has('sleep_needed_nap_milli')) {
			this.db.exec('ALTER TABLE sleep ADD COLUMN sleep_needed_nap_milli INTEGER');
		}

		const workoutCols = this.db.prepare("PRAGMA table_info(workouts)").all() as { name: string }[];
		const workoutColNames = new Set(workoutCols.map(c => c.name));
		if (!workoutColNames.has('percent_recorded')) {
			this.db.exec('ALTER TABLE workouts ADD COLUMN percent_recorded REAL');
		}
		if (!workoutColNames.has('sport_name')) {
			this.db.exec('ALTER TABLE workouts ADD COLUMN sport_name TEXT');
		}
		if (!workoutColNames.has('distance_meter')) {
			this.db.exec('ALTER TABLE workouts ADD COLUMN distance_meter REAL');
		}
		if (!workoutColNames.has('altitude_gain_meter')) {
			this.db.exec('ALTER TABLE workouts ADD COLUMN altitude_gain_meter REAL');
		}
		if (!workoutColNames.has('altitude_change_meter')) {
			this.db.exec('ALTER TABLE workouts ADD COLUMN altitude_change_meter REAL');
		}

		// Whoop returns some workouts without sport_id (sport_name is the
		// replacement), which the original NOT NULL constraint rejected. SQLite
		// can't drop a constraint in place, so rebuild the table once.
		const sportIdCol = (this.db.prepare("PRAGMA table_info(workouts)").all() as { name: string; notnull: number }[])
			.find(c => c.name === 'sport_id');
		if (sportIdCol?.notnull === 1) {
			const cols = 'id, user_id, sport_id, sport_name, start_time, end_time, score_state, strain, avg_hr, max_hr, kilojoule, percent_recorded, distance_meter, altitude_gain_meter, altitude_change_meter, zone_zero_milli, zone_one_milli, zone_two_milli, zone_three_milli, zone_four_milli, zone_five_milli, synced_at';
			this.db.transaction(() => {
				this.db.exec(`
					CREATE TABLE workouts_new (
						id TEXT PRIMARY KEY,
						user_id INTEGER NOT NULL,
						sport_id INTEGER,
						sport_name TEXT,
						start_time TEXT NOT NULL,
						end_time TEXT NOT NULL,
						score_state TEXT NOT NULL,
						strain REAL,
						avg_hr INTEGER,
						max_hr INTEGER,
						kilojoule REAL,
						percent_recorded REAL,
						distance_meter REAL,
						altitude_gain_meter REAL,
						altitude_change_meter REAL,
						zone_zero_milli INTEGER,
						zone_one_milli INTEGER,
						zone_two_milli INTEGER,
						zone_three_milli INTEGER,
						zone_four_milli INTEGER,
						zone_five_milli INTEGER,
						synced_at TEXT DEFAULT CURRENT_TIMESTAMP
					);
					INSERT INTO workouts_new (${cols}) SELECT ${cols} FROM workouts;
					DROP TABLE workouts;
					ALTER TABLE workouts_new RENAME TO workouts;
					CREATE INDEX IF NOT EXISTS idx_workouts_start ON workouts(start_time);
					CREATE INDEX IF NOT EXISTS idx_workouts_sport ON workouts(sport_id);
				`);
			})();
		}

		const recoveryCols = this.db.prepare("PRAGMA table_info(recovery)").all() as { name: string }[];
		const recoveryColNames = new Set(recoveryCols.map(c => c.name));
		if (!recoveryColNames.has('user_calibrating')) {
			this.db.exec('ALTER TABLE recovery ADD COLUMN user_calibrating INTEGER');
		}
	}

	saveTokens(tokens: WhoopTokens): void {
		const encryptedAccess = encrypt(tokens.access_token);
		const encryptedRefresh = encrypt(tokens.refresh_token);

		this.db.prepare(`
			INSERT OR REPLACE INTO tokens (id, access_token, refresh_token, expires_at, updated_at)
			VALUES (1, ?, ?, ?, CURRENT_TIMESTAMP)
		`).run(encryptedAccess, encryptedRefresh, tokens.expires_at);
	}

	getTokens(): WhoopTokens | null {
		const row = this.db.prepare('SELECT * FROM tokens WHERE id = 1').get() as TokenRow | undefined;
		if (!row) return null;

		const accessToken = isEncrypted(row.access_token)
			? decrypt(row.access_token)
			: row.access_token;

		const refreshToken = isEncrypted(row.refresh_token)
			? decrypt(row.refresh_token)
			: row.refresh_token;

		return {
			access_token: accessToken,
			refresh_token: refreshToken,
			expires_at: row.expires_at,
		};
	}

	clearTokens(): void {
		this.db.prepare('DELETE FROM tokens WHERE id = 1').run();
	}

	clearAllData(): void {
		this.db.exec(`
			DELETE FROM tokens;
			DELETE FROM sync_state;
			DELETE FROM cycles;
			DELETE FROM recovery;
			DELETE FROM sleep;
			DELETE FROM workouts;
			DELETE FROM profile;
			DELETE FROM body_measurement;
			INSERT OR IGNORE INTO sync_state (id) VALUES (1);
		`);
	}

	getSyncState(): { lastSyncAt: string | null; oldestDate: string | null; newestDate: string | null } {
		const row = this.db.prepare('SELECT * FROM sync_state WHERE id = 1').get() as SyncStateRow | undefined;
		return {
			lastSyncAt: row?.last_sync_at ?? null,
			oldestDate: row?.oldest_synced_date ?? null,
			newestDate: row?.newest_synced_date ?? null,
		};
	}

	updateSyncState(oldestDate: string, newestDate: string): void {
		this.db.prepare(`
			UPDATE sync_state
			SET last_sync_at = CURRENT_TIMESTAMP,
				oldest_synced_date = COALESCE(
					CASE WHEN oldest_synced_date IS NULL OR ? < oldest_synced_date THEN ? ELSE oldest_synced_date END,
					?
				),
				newest_synced_date = COALESCE(
					CASE WHEN newest_synced_date IS NULL OR ? > newest_synced_date THEN ? ELSE newest_synced_date END,
					?
				)
			WHERE id = 1
		`).run(oldestDate, oldestDate, oldestDate, newestDate, newestDate, newestDate);
	}

	upsertCycles(cycles: WhoopCycle[]): void {
		const stmt = this.db.prepare(`
			INSERT OR REPLACE INTO cycles (id, user_id, start_time, end_time, score_state, strain, kilojoule, avg_hr, max_hr, synced_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		`);

		const insertMany = this.db.transaction((items: WhoopCycle[]) => {
			for (const c of items) {
				stmt.run(
					c.id,
					c.user_id,
					c.start,
					c.end,
					c.score_state,
					c.score?.strain ?? null,
					c.score?.kilojoule ?? null,
					c.score?.average_heart_rate ?? null,
					c.score?.max_heart_rate ?? null
				);
			}
		});

		insertMany(cycles);
	}

	upsertRecoveries(recoveries: WhoopRecovery[]): void {
		const stmt = this.db.prepare(`
			INSERT OR REPLACE INTO recovery (id, user_id, sleep_id, created_at, score_state, recovery_score, resting_hr, hrv_rmssd, spo2, skin_temp, user_calibrating, synced_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		`);

		const insertMany = this.db.transaction((items: WhoopRecovery[]) => {
			for (const r of items) {
				stmt.run(
					r.cycle_id,
					r.user_id,
					r.sleep_id,
					r.created_at,
					r.score_state,
					r.score?.recovery_score ?? null,
					r.score?.resting_heart_rate ?? null,
					r.score?.hrv_rmssd_milli ?? null,
					r.score?.spo2_percentage ?? null,
					r.score?.skin_temp_celsius ?? null,
					r.score?.user_calibrating === undefined ? null : (r.score.user_calibrating ? 1 : 0)
				);
			}
		});

		insertMany(recoveries);
	}

	upsertSleeps(sleeps: WhoopSleep[]): void {
		const stmt = this.db.prepare(`
			INSERT OR REPLACE INTO sleep (
				id, user_id, cycle_id, start_time, end_time, is_nap, score_state,
				total_in_bed_milli, total_awake_milli, total_light_milli, total_deep_milli, total_rem_milli,
				total_no_data_milli, sleep_cycle_count, disturbance_count,
				sleep_performance, sleep_efficiency, sleep_consistency, respiratory_rate,
				sleep_needed_baseline_milli, sleep_needed_debt_milli, sleep_needed_strain_milli, sleep_needed_nap_milli, synced_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		`);

		const insertMany = this.db.transaction((items: WhoopSleep[]) => {
			for (const s of items) {
				stmt.run(
					s.id,
					s.user_id,
					s.cycle_id ?? null,
					s.start,
					s.end,
					s.nap ? 1 : 0,
					s.score_state,
					s.score?.stage_summary.total_in_bed_time_milli ?? null,
					s.score?.stage_summary.total_awake_time_milli ?? null,
					s.score?.stage_summary.total_light_sleep_time_milli ?? null,
					s.score?.stage_summary.total_slow_wave_sleep_time_milli ?? null,
					s.score?.stage_summary.total_rem_sleep_time_milli ?? null,
					s.score?.stage_summary.total_no_data_time_milli ?? null,
					s.score?.stage_summary.sleep_cycle_count ?? null,
					s.score?.stage_summary.disturbance_count ?? null,
					s.score?.sleep_performance_percentage ?? null,
					s.score?.sleep_efficiency_percentage ?? null,
					s.score?.sleep_consistency_percentage ?? null,
					s.score?.respiratory_rate ?? null,
					s.score?.sleep_needed.baseline_milli ?? null,
					s.score?.sleep_needed.need_from_sleep_debt_milli ?? null,
					s.score?.sleep_needed.need_from_recent_strain_milli ?? null,
					s.score?.sleep_needed.need_from_recent_nap_milli ?? null
				);
			}
		});

		insertMany(sleeps);
	}

	upsertWorkouts(workouts: WhoopWorkout[]): void {
		const stmt = this.db.prepare(`
			INSERT OR REPLACE INTO workouts (
				id, user_id, sport_id, sport_name, start_time, end_time, score_state,
				strain, avg_hr, max_hr, kilojoule, percent_recorded,
				distance_meter, altitude_gain_meter, altitude_change_meter,
				zone_zero_milli, zone_one_milli, zone_two_milli, zone_three_milli, zone_four_milli, zone_five_milli,
				synced_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		`);

		const insertMany = this.db.transaction((items: WhoopWorkout[]) => {
			for (const w of items) {
				const zones = w.score?.zone_duration ?? w.score?.zone_durations;
				stmt.run(
					w.id,
					w.user_id,
					w.sport_id ?? null,
					w.sport_name ?? null,
					w.start,
					w.end,
					w.score_state,
					w.score?.strain ?? null,
					w.score?.average_heart_rate ?? null,
					w.score?.max_heart_rate ?? null,
					w.score?.kilojoule ?? null,
					w.score?.percent_recorded ?? null,
					w.score?.distance_meter ?? null,
					w.score?.altitude_gain_meter ?? null,
					w.score?.altitude_change_meter ?? null,
					zones?.zone_zero_milli ?? null,
					zones?.zone_one_milli ?? null,
					zones?.zone_two_milli ?? null,
					zones?.zone_three_milli ?? null,
					zones?.zone_four_milli ?? null,
					zones?.zone_five_milli ?? null
				);
			}
		});

		insertMany(workouts);
	}

	upsertProfile(profile: WhoopUser): void {
		this.db.prepare(`
			INSERT OR REPLACE INTO profile (id, user_id, email, first_name, last_name, synced_at)
			VALUES (1, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		`).run(profile.user_id, profile.email, profile.first_name, profile.last_name);
	}

	upsertBodyMeasurement(measurement: WhoopBodyMeasurement): void {
		this.db.prepare(`
			INSERT OR REPLACE INTO body_measurement (id, height_meter, weight_kilogram, max_heart_rate, synced_at)
			VALUES (1, ?, ?, ?, CURRENT_TIMESTAMP)
		`).run(measurement.height_meter, measurement.weight_kilogram, measurement.max_heart_rate);
	}

	getProfile(): DbProfile | null {
		return this.db.prepare('SELECT * FROM profile WHERE id = 1').get() as DbProfile | undefined ?? null;
	}

	getBodyMeasurement(): DbBodyMeasurement | null {
		return this.db.prepare('SELECT * FROM body_measurement WHERE id = 1').get() as DbBodyMeasurement | undefined ?? null;
	}

	getLatestCycle(): DbCycle | null {
		return this.db.prepare('SELECT * FROM cycles ORDER BY start_time DESC LIMIT 1').get() as DbCycle | undefined ?? null;
	}

	getLatestRecovery(): DbRecovery | null {
		return this.db.prepare('SELECT * FROM recovery ORDER BY created_at DESC LIMIT 1').get() as DbRecovery | undefined ?? null;
	}

	getLatestSleep(): DbSleep | null {
		return this.db.prepare('SELECT * FROM sleep WHERE is_nap = 0 ORDER BY start_time DESC LIMIT 1').get() as DbSleep | undefined ?? null;
	}

	getCycleById(id: number): DbCycle | null {
		return this.db.prepare('SELECT * FROM cycles WHERE id = ?').get(id) as DbCycle | undefined ?? null;
	}

	getSleepById(id: string): DbSleep | null {
		return this.db.prepare('SELECT * FROM sleep WHERE id = ?').get(id) as DbSleep | undefined ?? null;
	}

	getWorkoutById(id: string): DbWorkout | null {
		return this.db.prepare('SELECT * FROM workouts WHERE id = ?').get(id) as DbWorkout | undefined ?? null;
	}

	getSleepForCycle(cycleId: number): DbSleep | null {
		return this.db.prepare(`
			SELECT * FROM sleep WHERE cycle_id = ? AND is_nap = 0 ORDER BY start_time DESC LIMIT 1
		`).get(cycleId) as DbSleep | undefined ?? null;
	}

	getRecoveryForCycle(cycleId: number): DbRecovery | null {
		return this.db.prepare(`
			SELECT * FROM recovery WHERE id = ?
		`).get(cycleId) as DbRecovery | undefined ?? null;
	}

	getCyclesByDateRange(startDate: string, endDate: string): DbCycle[] {
		return this.db.prepare(`
			SELECT * FROM cycles WHERE start_time >= ? AND start_time <= ? ORDER BY start_time DESC
		`).all(startDate, endDate) as DbCycle[];
	}

	getRecoveriesByDateRange(startDate: string, endDate: string): DbRecovery[] {
		return this.db.prepare(`
			SELECT * FROM recovery WHERE created_at >= ? AND created_at <= ? ORDER BY created_at DESC
		`).all(startDate, endDate) as DbRecovery[];
	}

	getSleepsByDateRange(startDate: string, endDate: string, includeNaps = false): DbSleep[] {
		const query = includeNaps
			? 'SELECT * FROM sleep WHERE start_time >= ? AND start_time <= ? ORDER BY start_time DESC'
			: 'SELECT * FROM sleep WHERE start_time >= ? AND start_time <= ? AND is_nap = 0 ORDER BY start_time DESC';
		return this.db.prepare(query).all(startDate, endDate) as DbSleep[];
	}

	getWorkoutsByDateRange(startDate: string, endDate: string): DbWorkout[] {
		return this.db.prepare(`
			SELECT * FROM workouts WHERE start_time >= ? AND start_time <= ? ORDER BY start_time DESC
		`).all(startDate, endDate) as DbWorkout[];
	}

	getRecentWorkouts(days: number, limit = 50): DbWorkout[] {
		return this.db.prepare(`
			SELECT * FROM workouts
			WHERE start_time >= DATE('now', '-' || ? || ' days')
			ORDER BY start_time DESC
			LIMIT ?
		`).all(days, limit) as DbWorkout[];
	}

	getRecoveryTrends(days: number): DbRecovery[] {
		return this.db.prepare(`
			SELECT * FROM recovery
			WHERE recovery_score IS NOT NULL AND created_at >= DATE('now', '-' || ? || ' days')
			ORDER BY created_at DESC
		`).all(days) as DbRecovery[];
	}

	getSleepTrends(days: number): DbSleep[] {
		return this.db.prepare(`
			SELECT * FROM sleep
			WHERE is_nap = 0 AND sleep_performance IS NOT NULL AND start_time >= DATE('now', '-' || ? || ' days')
			ORDER BY start_time DESC
		`).all(days) as DbSleep[];
	}

	getStrainTrends(days: number): DbCycle[] {
		return this.db.prepare(`
			SELECT * FROM cycles
			WHERE strain IS NOT NULL AND start_time >= DATE('now', '-' || ? || ' days')
			ORDER BY start_time DESC
		`).all(days) as DbCycle[];
	}

	// Full copy of the Whoop data for offline backup. Deliberately excludes the
	// tokens table so an export never carries OAuth credentials.
	exportAll(): Record<string, unknown[]> {
		const tables = ['cycles', 'recovery', 'sleep', 'workouts', 'profile', 'body_measurement', 'sync_state'] as const;
		const out: Record<string, unknown[]> = {};
		for (const table of tables) {
			out[table] = this.db.prepare(`SELECT * FROM ${table}`).all();
		}
		return out;
	}

	// Same as exportAll, limited to records dated from `from` through `to`
	// inclusive (YYYY-MM-DD, UTC). Profile, body measurement and sync state are
	// always included in full.
	exportRange(from: string, to: string): Record<string, unknown[]> {
		const range = (table: string, column: string) =>
			this.db.prepare(`SELECT * FROM ${table} WHERE ${column} >= DATE(?) AND ${column} < DATE(?, '+1 day') ORDER BY ${column}`).all(from, to);
		return {
			cycles: range('cycles', 'start_time'),
			recovery: range('recovery', 'created_at'),
			sleep: range('sleep', 'start_time'),
			workouts: range('workouts', 'start_time'),
			profile: this.db.prepare('SELECT * FROM profile').all(),
			body_measurement: this.db.prepare('SELECT * FROM body_measurement').all(),
			sync_state: this.db.prepare('SELECT * FROM sync_state').all(),
		};
	}

	close(): void {
		this.db.close();
	}
}
