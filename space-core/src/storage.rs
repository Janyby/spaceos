use crate::query::{EventQuery, ObjectQuery, RelationQuery};
use crate::{Event, Object, Observation, ObservationOutcome, ProviderInfo, Relation, Space};
use anyhow::Result;
use chrono::Utc;
use rusqlite::{params, Connection, Transaction, TransactionBehavior};
use std::path::Path;
use std::sync::Mutex;

/// Test-only crash-injection points.
///
/// Each point fires at most once per arming (see [`Storage::fail_once`])
/// and then auto-clears. `Before*` fails the operation before anything is
/// written; `After*` fails it after the SQLite statement committed.
/// Default (production) path has no armed points and behaves exactly as
/// before — these hooks change no semantics when disarmed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashPoint {
    BeforeObjectCommit,
    AfterObjectCommit,
    BeforeRelationCommit,
    AfterRelationCommit,
    BeforeEventCommit,
    AfterEventCommit,
    BeforeObjectDelete,
    AfterObjectDelete,
    BeforeRelationDelete,
    AfterRelationDelete,
}

/// Internal storage view for use inside a transaction.
/// Mirrors the public mutation methods but operates on the transaction.
pub struct TransactionStorage<'a> {
    conn: &'a Transaction<'a>,
}

impl<'a> TransactionStorage<'a> {
    pub fn create_object(&self, obj: &Object) -> Result<()> {
        // UPSERT, not REPLACE: the row is never deleted+reinserted, so
        // FK references from relations survive updates and the relation
        // graph is never torn down by an object write. Column set is
        // identical to the legacy REPLACE (no contract change).
        self.conn.execute(
            "INSERT INTO objects (id, type, properties, provider_id, external_id, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(id) DO UPDATE SET type=excluded.type, properties=excluded.properties,
                provider_id=excluded.provider_id, external_id=excluded.external_id,
                created_at=excluded.created_at, updated_at=excluded.updated_at",
            params![
                obj.id,
                obj.r#type,
                serde_json::to_string(&obj.properties)?,
                obj.provider_id,
                obj.external_id,
                obj.created_at.to_rfc3339(),
                obj.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn create_relation(&self, r: &Relation) -> Result<()> {
        // Same UPSERT rationale as objects; endpoints must exist
        // (FK-checked) when constraints are on.
        self.conn.execute(
            "INSERT INTO relations (id, source_object_id, relation_type, target_object_id, properties, created_at, world_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(id) DO UPDATE SET source_object_id=excluded.source_object_id,
                relation_type=excluded.relation_type, target_object_id=excluded.target_object_id,
                properties=excluded.properties, created_at=excluded.created_at, world_id=excluded.world_id",
            params![
                r.id,
                r.source_object_id,
                r.relation_type,
                r.target_object_id,
                serde_json::to_string(&r.properties)?,
                r.created_at.to_rfc3339(),
                r.world_id,
            ],
        )?;
        Ok(())
    }

    pub fn delete_relation(&self, id: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM relations WHERE id = ?1", params![id])?;
        Ok(())
    }

    pub fn record_event(&self, e: &Event) -> Result<()> {
        // Immutable log: a repeated id is a redelivery, never an
        // overwrite. DO NOTHING keeps exactly one row; callers that need
        // to know use `record_event_if_new` / `apply_observation`.
        self.conn.execute(
            "INSERT INTO events (id, event_type, actor, provider_id, object_ids, payload, timestamp, correlation_id, world_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(id) DO NOTHING",
            params![
                e.id,
                e.event_type,
                e.actor,
                e.provider_id,
                serde_json::to_string(&e.object_ids)?,
                serde_json::to_string(&e.payload)?,
                e.timestamp.to_rfc3339(),
                e.correlation_id,
                if e.world_id.is_empty() {
                    e.payload.get("world_id").and_then(|v| v.as_str()).unwrap_or("").to_string()
                } else {
                    e.world_id.clone()
                },
            ],
        )?;
        Ok(())
    }

    /// Same insert, but reports whether the row is new (`true`) or a
    /// recognized redelivery (`false`, nothing changed).
    pub fn record_event_if_new(&self, e: &Event) -> Result<bool> {
        let n = self.conn.execute(
            "INSERT INTO events (id, event_type, actor, provider_id, object_ids, payload, timestamp, correlation_id, world_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(id) DO NOTHING",
            params![
                e.id,
                e.event_type,
                e.actor,
                e.provider_id,
                serde_json::to_string(&e.object_ids)?,
                serde_json::to_string(&e.payload)?,
                e.timestamp.to_rfc3339(),
                e.correlation_id,
                if e.world_id.is_empty() {
                    e.payload.get("world_id").and_then(|v| v.as_str()).unwrap_or("").to_string()
                } else {
                    e.world_id.clone()
                },
            ],
        )?;
        Ok(n == 1)
    }

    /// Has this observation id already been applied (redelivery probe)?
    pub fn observation_applied(&self, observation_id: &str) -> Result<Option<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT event_id FROM observation_log WHERE observation_id = ?1")?;
        let mut rows = stmt.query(params![observation_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }

    fn insert_observation_log(&self, observation_id: &str, event_id: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO observation_log (observation_id, event_id, applied_at) VALUES (?1, ?2, ?3)",
            params![observation_id, event_id, now],
        )?;
        Ok(())
    }

    pub fn create_space(&self, s: &Space) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO spaces (id, name, metadata, created_at, updated_at) VALUES (?1,?2,?3,?4,?5)",
            params![s.id, s.name, serde_json::to_string(&s.metadata)?, s.created_at.to_rfc3339(), s.updated_at.to_rfc3339()],
        )?;
        Ok(())
    }

    pub fn upsert_provider(&self, p: &ProviderInfo) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO providers (id, name, capabilities) VALUES (?1, ?2, ?3)",
            params![p.id, p.name, serde_json::to_string(&p.capabilities)?],
        )?;
        Ok(())
    }

    pub fn delete_object_cascade(&self, id: &str) -> Result<()> {
        // Same cascade logic as Storage::delete_object but within transaction
        let attached: Vec<String> = {
            let mut stmt = self.conn.prepare(
                "SELECT id FROM relations WHERE source_object_id = ?1 OR target_object_id = ?1",
            )?;
            let mut rows = stmt.query(params![id])?;
            let mut out = vec![];
            while let Some(row) = rows.next()? {
                out.push(row.get(0)?);
            }
            out
        };
        for rid in attached {
            self.delete_relation(&rid)?;
        }
        self.conn
            .execute("DELETE FROM objects WHERE id = ?1", params![id])?;
        Ok(())
    }
}

pub struct Storage {
    conn: Connection,
    faults: Mutex<Vec<CrashPoint>>,
}

/// Current SQLite schema version. `PRAGMA user_version` is the version
/// store (per-DB, atomic, no extra table): 0 = pre-versioning database,
/// 1 = baseline schema from `init()`, 2 = unified-model schema
/// (`events.seq` cursor, FK constraints, `observation_log`, indexes).
pub const SCHEMA_VERSION: i64 = 2;

/// Upper bound for a single `query_events` page. Callers asking for more
/// are clamped, never rejected (backward compatible); `None` still means
/// "all". WS recovery pages through `after_seq` instead of huge limits.
pub const MAX_EVENT_LIMIT: usize = 500;

impl Storage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        // FK enforcement is per-connection in SQLite: enable on EVERY
        // connection right after open, before any schema or data work.
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        let s = Self {
            conn,
            faults: Mutex::new(vec![]),
        };
        s.init()?;
        s.migrate()?;
        Ok(s)
    }

    /// Open with `journal_mode=WAL`, pinning `synchronous=FULL`.
    ///
    /// V1.8 experiment only: changes the journal MECHANISM, not durability
    /// semantics (FULL fsync per commit, same as the default mode) and not
    /// the data model, queries, batching, or locking. Controlled comparison
    /// CURRENT vs WAL differs in exactly this one pragma pair.
    /// Default `open` is untouched and stays the reproducible default.
    pub fn open_wal(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        // Explicit FULL: default is FULL already; pin it so the experiment
        // varies journaling alone, never durability.
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        let s = Self {
            conn,
            faults: Mutex::new(vec![]),
        };
        s.init()?;
        s.migrate()?;
        Ok(s)
    }

    /// Current journal mode of this connection (for tests).
    pub fn journal_mode(&self) -> Result<String> {
        self.conn
            .query_row("PRAGMA journal_mode;", [], |r| r.get::<_, String>(0))
            .map_err(Into::into)
    }

    /// Current synchronous setting: 2 == FULL (for tests).
    pub fn synchronous(&self) -> Result<i64> {
        self.conn
            .query_row("PRAGMA synchronous;", [], |r| r.get::<_, i64>(0))
            .map_err(Into::into)
    }

    /// Read-only integrity check (for tests).
    pub fn integrity_check(&self) -> Result<String> {
        self.conn
            .query_row("PRAGMA integrity_check;", [], |r| r.get::<_, String>(0))
            .map_err(Into::into)
    }

    /// Execute multiple operations within a single SQLite transaction.
    ///
    /// V1.9 experiment: groups multiple mutations into one commit to
    /// amortize fsync overhead. The closure receives a `TransactionStorage`
    /// that uses the SAME connection but executes within a transaction.
    ///
    /// - All operations in the closure either commit together or rollback together.
    /// - Fault hooks (V1.5/V1.7) fire at their normal points INSIDE the transaction.
    /// - Policy enforcement MUST be done by caller BEFORE entering the transaction.
    /// - Event publication (broadcast) happens AFTER successful commit.
    /// - Returns the closure's result, or rolls back on error.
    pub fn transaction<F, T>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&TransactionStorage) -> Result<T>,
    {
        let tx_corr = format!("tx-{}", uuid::Uuid::new_v4());
        self.transaction_with_correlation(&tx_corr, f)
    }

    /// Same as [`transaction`](Self::transaction), but with an explicit
    /// correlation id so SpaceTrace can link the Storage transaction to
    /// the surrounding semantic Action/IPC chain (Slice 3).
    /// Semantic Action and Storage Transaction remain distinct concepts
    /// (I15): the correlation is observational only.
    pub fn transaction_with_correlation<F, T>(&mut self, correlation_id: &str, f: F) -> Result<T>
    where
        F: FnOnce(&TransactionStorage) -> Result<T>,
    {
        // Use IMMEDIATE to acquire write lock upfront, avoiding BUSY mid-transaction.
        let start = std::time::Instant::now();
        spacetrace::hooks::global_hooks().storage_transaction_begin(correlation_id, 0);
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let tx_storage = TransactionStorage { conn: &tx };
        let result = f(&tx_storage);
        match result {
            Ok(v) => {
                tx.commit()?;
                spacetrace::hooks::global_hooks()
                    .storage_transaction_commit(correlation_id, start.elapsed().as_millis() as u64);
                Ok(v)
            }
            Err(e) => {
                let _ = tx.rollback();
                spacetrace::hooks::global_hooks()
                    .storage_transaction_rollback(correlation_id, &e.to_string());
                Err(e)
            }
        }
    }

    /// Apply one provider observation atomically: redelivery check →
    /// object upserts (in order) → relation upserts → event insert →
    /// observation-log write, all in ONE transaction. Any failure
    /// (including an FK violation on a dangling relation endpoint)
    /// rolls back everything: no partial projections, no orphan event.
    ///
    /// `facts` are folded into `event.payload["facts"]` here when the
    /// caller left that key absent, so the audit trail survives even
    /// when the provider sends facts outside the event body.
    pub fn apply_observation(&mut self, obs: &Observation) -> Result<ObservationOutcome> {
        if obs.observation_id.trim().is_empty() {
            anyhow::bail!("observation_id is required");
        }
        let mut event = obs.event.clone();
        if !obs.facts.is_empty() && event.payload.get("facts").is_none() {
            event.payload["facts"] = serde_json::to_value(&obs.facts)?;
        }
        let event_id = event.id.clone();
        let objects = obs.objects.clone();
        let relations = obs.relations.clone();
        let delete_ids = obs.delete_ids.clone();
        self.transaction(|tx| {
            if tx.observation_applied(&obs.observation_id)?.is_some() {
                return Ok(ObservationOutcome {
                    applied: false,
                    event_id: event_id.clone(),
                    seq: 0,
                });
            }
            for o in &objects {
                tx.create_object(o)?;
            }
            for r in &relations {
                tx.create_relation(r)?;
            }
            // Confirmed deletions (Core cascade) inside the same tx, so
            // the FileDeleted event can never outlive its projection.
            for id in &delete_ids {
                tx.delete_object_cascade(id)?;
            }
            if !tx.record_event_if_new(&event)? {
                anyhow::bail!(
                    "event id collision: {} already stored under a different observation",
                    event.id
                );
            }
            tx.insert_observation_log(&obs.observation_id, &event.id)?;
            let seq: u64 = tx.conn.query_row(
                "SELECT seq FROM events WHERE id = ?1",
                params![event.id],
                |row| row.get::<_, i64>(0),
            )? as u64;
            Ok(ObservationOutcome {
                applied: true,
                event_id: event_id.clone(),
                seq,
            })
        })
    }

    /// Arm a one-shot crash fault for tests. Fires once, then auto-clears.
    /// Never armed in the production path.
    pub fn fail_once(&self, point: CrashPoint) {
        self.faults.lock().unwrap().push(point);
    }

    /// Disarm all pending faults.
    pub fn clear_faults(&self) {
        self.faults.lock().unwrap().clear();
    }

    fn fault(&self, point: CrashPoint) -> Result<()> {
        let mut faults = self.faults.lock().unwrap();
        if let Some(i) = faults.iter().position(|p| *p == point) {
            faults.remove(i);
            anyhow::bail!(
                "injected crash at {point:?} (simulated, nothing further written by this call)"
            );
        }
        Ok(())
    }

    fn init(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS objects (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                properties TEXT NOT NULL,
                provider_id TEXT NOT NULL,
                external_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS relations (
                id TEXT PRIMARY KEY,
                source_object_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                target_object_id TEXT NOT NULL,
                properties TEXT NOT NULL,
                created_at TEXT NOT NULL,
                world_id TEXT DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                actor TEXT NOT NULL,
                provider_id TEXT,
                object_ids TEXT NOT NULL,
                payload TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                correlation_id TEXT,
                world_id TEXT DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS providers (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                capabilities TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS spaces (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                metadata TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );",
        )?;
        // Migration for DBs created before world_id existed on events.
        // Repeatable: only adds when missing (kept for DBs that never
        // reach migrate(), e.g. opened read-only by external tools).
        if !self.has_column("events", "world_id")? {
            self.conn
                .execute("ALTER TABLE events ADD COLUMN world_id TEXT DEFAULT ''", [])?;
        }
        Ok(())
    }

    fn has_column(&self, table: &str, column: &str) -> Result<bool> {
        let mut stmt = self
            .conn
            .prepare("SELECT name FROM pragma_table_info(?1) WHERE name = ?2")?;
        Ok(stmt.exists(params![table, column])?)
    }

    fn user_version(&self) -> Result<i64> {
        self.conn
            .query_row("PRAGMA user_version;", [], |r| r.get::<_, i64>(0))
            .map_err(Into::into)
    }

    fn set_user_version(&self, v: i64) -> Result<()> {
        self.conn
            .execute_batch(&format!("PRAGMA user_version={v};"))?;
        Ok(())
    }

    /// Count relations whose endpoints have no object row. Used BEFORE
    /// adding FK constraints so legacy orphans are reported, never
    /// silently deleted. Returns (missing_source, missing_target).
    fn orphan_relation_counts(&self) -> Result<(i64, i64)> {
        let missing_source: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM relations r LEFT JOIN objects o ON o.id = r.source_object_id WHERE o.id IS NULL",
            [],
            |r| r.get(0),
        )?;
        let missing_target: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM relations r LEFT JOIN objects o ON o.id = r.target_object_id WHERE o.id IS NULL",
            [],
            |r| r.get(0),
        )?;
        Ok((missing_source, missing_target))
    }

    /// Rows violating FK constraints right now (grandfathered legacy
    /// orphans included). Empty vec = fully consistent graph.
    pub fn fk_violations(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare("PRAGMA foreign_key_check;")?;
        let mut out = vec![];
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let table: String = row.get(0)?;
            let rowid: i64 = row.get(1)?;
            let parent: String = row.get(2)?; // column 2 is parent table name
            out.push(format!("{table} rowid={rowid} references missing {parent}"));
        }
        Ok(out)
    }

    /// Versioned schema migration. Repeatable: re-running on a migrated
    /// DB is a no-op (steps key off `PRAGMA user_version`). Crash
    /// windows are single auto-commit statements; a crash before the
    /// DROP..RENAME swap leaves the old table intact for the next run.
    pub fn migrate(&self) -> Result<()> {
        let mut v = self.user_version()?;
        if v < 1 {
            // The `init()` schema above IS v1: stamp it, nothing to do.
            self.set_user_version(1)?;
            v = 1;
        }
        if v < 2 {
            self.migrate_v2()?;
            self.set_user_version(2)?;
        }
        Ok(())
    }

    fn migrate_v2(&self) -> Result<()> {
        if !self.has_column("events", "world_id")? {
            self.conn
                .execute("ALTER TABLE events ADD COLUMN world_id TEXT DEFAULT ''", [])?;
        }
        // Legacy DBs predate world_id on relations as well (the old init
        // only backfilled events). Ensure before the rebuild SELECTs it.
        if !self.has_column("relations", "world_id")? {
            self.conn.execute(
                "ALTER TABLE relations ADD COLUMN world_id TEXT DEFAULT ''",
                [],
            )?;
        }
        // 1. Report legacy orphans; they are preserved, never deleted.
        let (ms, mt) = self.orphan_relation_counts()?;
        if ms + mt > 0 {
            tracing::warn!(
                "migrate v2: preserving {ms} relations with missing source and {mt} with missing target (grandfathered orphans stay readable; future violating writes fail)"
            );
        }
        // 2. Rebuild events with a durable cursor. AUTOINCREMENT is only
        // legal on INTEGER PRIMARY KEY, while the contract key stays the
        // TEXT `id` (now UNIQUE): old rows keep their ids, seq follows
        // (timestamp, id) order so the cursor is stable for old data.
        self.conn.execute_batch(
            "DROP TABLE IF EXISTS events_new;
             CREATE TABLE events_new (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                actor TEXT NOT NULL,
                provider_id TEXT,
                object_ids TEXT NOT NULL,
                payload TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                correlation_id TEXT,
                world_id TEXT DEFAULT ''
             );
             INSERT INTO events_new (id, event_type, actor, provider_id, object_ids, payload, timestamp, correlation_id, world_id)
                SELECT id, event_type, actor, provider_id, object_ids, payload, timestamp, correlation_id, world_id
                FROM events ORDER BY timestamp, id;
             DROP TABLE events;
             ALTER TABLE events_new RENAME TO events;",
        )?;
        // 3. Rebuild relations WITH FK constraints. Enforcement is OFF
        // for the copy so grandfathered orphans survive; every later
        // write (FK ON) is validated.
        self.conn.execute_batch("PRAGMA foreign_keys=OFF;")?;
        let rebuilt = self.conn.execute_batch(
            "DROP TABLE IF EXISTS relations_new;
             CREATE TABLE relations_new (
                id TEXT PRIMARY KEY,
                source_object_id TEXT NOT NULL REFERENCES objects(id),
                relation_type TEXT NOT NULL,
                target_object_id TEXT NOT NULL REFERENCES objects(id),
                properties TEXT NOT NULL,
                created_at TEXT NOT NULL,
                world_id TEXT DEFAULT ''
             );
             INSERT INTO relations_new (id, source_object_id, relation_type, target_object_id, properties, created_at, world_id)
                SELECT id, source_object_id, relation_type, target_object_id, properties, created_at, world_id FROM relations;
             DROP TABLE relations;
             ALTER TABLE relations_new RENAME TO relations;",
        );
        self.conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        rebuilt?;
        // 4. Service table: redelivery log (stable observation keys).
        // Allowed as a service table: it never mutates domain state,
        // it only records WHICH observations were already applied.
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS observation_log (
                observation_id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                applied_at TEXT NOT NULL
             );",
        )?;
        // 5. Indexes for the hot read paths (all IF NOT EXISTS).
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_events_corr ON events(correlation_id);
             CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
             CREATE INDEX IF NOT EXISTS idx_events_world ON events(world_id);
             CREATE INDEX IF NOT EXISTS idx_objects_provider ON objects(provider_id, external_id);
             CREATE INDEX IF NOT EXISTS idx_objects_type ON objects(type);
             CREATE INDEX IF NOT EXISTS idx_relations_source ON relations(source_object_id, relation_type);
             CREATE INDEX IF NOT EXISTS idx_relations_target ON relations(target_object_id);
             CREATE INDEX IF NOT EXISTS idx_relations_world ON relations(world_id);",
        )?;
        Ok(())
    }

    pub fn create_object(&self, obj: &Object) -> Result<()> {
        self.fault(CrashPoint::BeforeObjectCommit)?;
        self.conn.execute(
            "INSERT INTO objects (id, type, properties, provider_id, external_id, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(id) DO UPDATE SET type=excluded.type, properties=excluded.properties,
                provider_id=excluded.provider_id, external_id=excluded.external_id,
                created_at=excluded.created_at, updated_at=excluded.updated_at",
            params![
                obj.id,
                obj.r#type,
                serde_json::to_string(&obj.properties)?,
                obj.provider_id,
                obj.external_id,
                obj.created_at.to_rfc3339(),
                obj.updated_at.to_rfc3339(),
            ],
        )?;
        self.fault(CrashPoint::AfterObjectCommit)?;
        Ok(())
    }

    pub fn get_object(&self, id: &str) -> Result<Option<Object>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, type, properties, provider_id, external_id, created_at, updated_at FROM objects WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(row_to_object(row)?))
        } else {
            Ok(None)
        }
    }

    pub fn get_object_by_external(
        &self,
        provider_id: &str,
        external_id: &str,
    ) -> Result<Option<Object>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, type, properties, provider_id, external_id, created_at, updated_at FROM objects WHERE provider_id = ?1 AND external_id = ?2",
        )?;
        let mut rows = stmt.query(params![provider_id, external_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(row_to_object(row)?))
        } else {
            Ok(None)
        }
    }

    pub fn update_object(&self, obj: &Object) -> Result<()> {
        let mut o = obj.clone();
        o.updated_at = Utc::now();
        self.create_object(&o)
    }

    pub fn delete_object(&self, id: &str) -> Result<()> {
        self.fault(CrashPoint::BeforeObjectDelete)?;
        // Cascade rule (V1.6 R1): relations anchored on a deleted object go
        // with it, relations-first, one row per call so every step is an
        // explicit crash point (Breakage B5). Worst crash window between
        // two statements is a prefix of the cascade, which a repeated
        // delete_object converges (idempotent). The reverse order would
        // leave dangling relations, which no retry could distinguish.
        let attached: Vec<String> = {
            let mut stmt = self.conn.prepare(
                "SELECT id FROM relations WHERE source_object_id = ?1 OR target_object_id = ?1",
            )?;
            let mut rows = stmt.query(params![id])?;
            let mut out = vec![];
            while let Some(row) = rows.next()? {
                out.push(row.get(0)?);
            }
            out
        };
        for rid in attached {
            self.delete_relation(&rid)?;
        }
        self.conn
            .execute("DELETE FROM objects WHERE id = ?1", params![id])?;
        self.fault(CrashPoint::AfterObjectDelete)?;
        Ok(())
    }

    pub fn query_objects(&self, q: &ObjectQuery) -> Result<Vec<Object>> {
        let mut stmt2 = self.conn.prepare(
            "SELECT id, type, properties, provider_id, external_id, created_at, updated_at FROM objects",
        )?;
        let mut all = stmt2.query(params![])?;
        let mut out = vec![];
        while let Some(row) = all.next()? {
            let o = row_to_object(row)?;
            if let Some(t) = &q.r#type {
                if &o.r#type != t {
                    continue;
                }
            }
            if let Some(p) = &q.provider_id {
                if &o.provider_id != p {
                    continue;
                }
            }
            if let Some(pc) = &q.path_contains {
                let props = serde_json::to_string(&o.properties)?;
                if !props.to_lowercase().contains(&pc.to_lowercase()) {
                    continue;
                }
            }
            if let Some(sid) = &q.world_id {
                if o.properties.get("world_id").and_then(|v| v.as_str()) != Some(sid) {
                    continue;
                }
            }
            out.push(o);
        }
        Ok(out)
    }

    pub fn create_relation(&self, r: &Relation) -> Result<()> {
        self.fault(CrashPoint::BeforeRelationCommit)?;
        self.conn.execute(
            "INSERT INTO relations (id, source_object_id, relation_type, target_object_id, properties, created_at, world_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(id) DO UPDATE SET source_object_id=excluded.source_object_id,
                relation_type=excluded.relation_type, target_object_id=excluded.target_object_id,
                properties=excluded.properties, created_at=excluded.created_at, world_id=excluded.world_id",
            params![
                r.id,
                r.source_object_id,
                r.relation_type,
                r.target_object_id,
                serde_json::to_string(&r.properties)?,
                r.created_at.to_rfc3339(),
                r.world_id,
            ],
        )?;
        self.fault(CrashPoint::AfterRelationCommit)?;
        Ok(())
    }

    pub fn delete_relation(&self, id: &str) -> Result<()> {
        self.fault(CrashPoint::BeforeRelationDelete)?;
        self.conn
            .execute("DELETE FROM relations WHERE id = ?1", params![id])?;
        self.fault(CrashPoint::AfterRelationDelete)?;
        Ok(())
    }

    pub fn query_relations(&self, q: &RelationQuery) -> Result<Vec<Relation>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, source_object_id, relation_type, target_object_id, properties, created_at, world_id FROM relations",
        )?;
        let mut rows = stmt.query(params![])?;
        let mut out = vec![];
        while let Some(row) = rows.next()? {
            let r = Relation {
                id: row.get(0)?,
                source_object_id: row.get(1)?,
                relation_type: row.get(2)?,
                target_object_id: row.get(3)?,
                properties: serde_json::from_str(&row.get::<_, String>(4)?)?,
                created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(5)?)?
                    .with_timezone(&Utc),
                world_id: row.get(6).unwrap_or_default(),
            };
            if let Some(t) = &q.relation_type {
                if &r.relation_type != t {
                    continue;
                }
            }
            if let Some(s) = &q.source_object_id {
                if &r.source_object_id != s {
                    continue;
                }
            }
            if let Some(sid) = &q.world_id {
                if &r.world_id != sid {
                    continue;
                }
            }
            out.push(r);
        }
        Ok(out)
    }

    pub fn record_event(&self, e: &Event) -> Result<()> {
        self.fault(CrashPoint::BeforeEventCommit)?;
        // Immutable log (see TransactionStorage::record_event): redelivery
        // keeps the single row. Response shape is unchanged.
        self.conn.execute(
            "INSERT INTO events (id, event_type, actor, provider_id, object_ids, payload, timestamp, correlation_id, world_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(id) DO NOTHING",
            params![
                e.id,
                e.event_type,
                e.actor,
                e.provider_id,
                serde_json::to_string(&e.object_ids)?,
                serde_json::to_string(&e.payload)?,
                e.timestamp.to_rfc3339(),
                e.correlation_id,
                if e.world_id.is_empty() {
                    e.payload.get("world_id").and_then(|v| v.as_str()).unwrap_or("").to_string()
                } else {
                    e.world_id.clone()
                },
            ],
        )?;
        self.fault(CrashPoint::AfterEventCommit)?;
        Ok(())
    }

    /// Event feed in durable cursor order.
    ///
    /// - Rows come back `ORDER BY seq ASC` (insertion order, deterministic
    ///   even for equal timestamps; previously timestamp-string order).
    /// - `after_seq`: only rows with `seq > after_seq` (`None`/0 = all).
    /// - `next_seq` convention (for WS recovery, Phase E): the client
    ///   takes the last returned row's `seq`; on an empty page it reuses
    ///   its `after_seq`. No gaps are possible between a snapshot's
    ///   `next_seq` and a follow-up `after_seq` on a single connection.
    /// - `limit` is clamped to [`MAX_EVENT_LIMIT`]; `None` still means all
    ///   (backward compatible).
    pub fn query_events(&self, q: &EventQuery) -> Result<Vec<Event>> {
        let after: u64 = q.after_seq.unwrap_or(0);
        let mut stmt = self.conn.prepare(
            "SELECT seq, id, event_type, actor, provider_id, object_ids, payload, timestamp, correlation_id, world_id
             FROM events WHERE seq > ?1 ORDER BY seq ASC",
        )?;
        let mut rows = stmt.query(params![after as i64])?;
        let mut out = vec![];
        while let Some(row) = rows.next()? {
            let payload: serde_json::Value = serde_json::from_str(&row.get::<_, String>(6)?)?;
            let col_world: String = row.get(9).unwrap_or_default();
            let e = Event {
                seq: row.get::<_, i64>(0)? as u64,
                id: row.get(1)?,
                event_type: row.get(2)?,
                actor: row.get(3)?,
                provider_id: row.get(4)?,
                object_ids: serde_json::from_str(&row.get::<_, String>(5)?)?,
                payload: payload.clone(),
                timestamp: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(7)?)?
                    .with_timezone(&Utc),
                correlation_id: row.get(8)?,
                world_id: if col_world.is_empty() {
                    payload
                        .get("world_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string()
                } else {
                    col_world
                },
            };
            if let Some(t) = &q.event_type {
                if &e.event_type != t {
                    continue;
                }
            }
            if let Some(p) = &q.provider_id {
                if e.provider_id.as_deref() != Some(p) {
                    continue;
                }
            }
            if let Some(sid) = &q.world_id {
                if e.world_id != *sid {
                    continue;
                }
            }
            if let Some(c) = &q.correlation_id {
                if e.correlation_id.as_deref() != Some(c.as_str()) {
                    continue;
                }
            }
            out.push(e);
        }
        if let Some(limit) = q.limit {
            let lim = limit.min(MAX_EVENT_LIMIT);
            if out.len() > lim {
                out = out[out.len() - lim..].to_vec();
            }
        }
        Ok(out)
    }

    /// Highest assigned event cursor (`0` on an empty log). WS recovery
    /// snapshots use this as their initial `next_seq`.
    pub fn max_seq(&self) -> Result<u64> {
        let v: Option<i64> = self
            .conn
            .query_row("SELECT MAX(seq) FROM events", [], |r| r.get(0))?;
        Ok(v.unwrap_or(0) as u64)
    }

    pub fn create_space(&self, s: &Space) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO spaces (id, name, metadata, created_at, updated_at) VALUES (?1,?2,?3,?4,?5)",
            params![s.id, s.name, serde_json::to_string(&s.metadata)?, s.created_at.to_rfc3339(), s.updated_at.to_rfc3339()],
        )?;
        Ok(())
    }

    pub fn get_space(&self, id: &str) -> Result<Option<Space>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, metadata, created_at, updated_at FROM spaces WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Space {
                id: row.get(0)?,
                name: row.get(1)?,
                metadata: serde_json::from_str(&row.get::<_, String>(2)?)?,
                created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(3)?)?
                    .with_timezone(&Utc),
                updated_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(4)?)?
                    .with_timezone(&Utc),
            }))
        } else {
            Ok(None)
        }
    }

    pub fn list_spaces(&self) -> Result<Vec<Space>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, name, metadata, created_at, updated_at FROM spaces")?;
        let mut rows = stmt.query(params![])?;
        let mut out = vec![];
        while let Some(row) = rows.next()? {
            out.push(Space {
                id: row.get(0)?,
                name: row.get(1)?,
                metadata: serde_json::from_str(&row.get::<_, String>(2)?)?,
                created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(3)?)?
                    .with_timezone(&Utc),
                updated_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(4)?)?
                    .with_timezone(&Utc),
            });
        }
        Ok(out)
    }

    pub fn get_relation(&self, id: &str) -> Result<Option<Relation>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, source_object_id, relation_type, target_object_id, properties, created_at, world_id FROM relations WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Relation {
                id: row.get(0)?,
                source_object_id: row.get(1)?,
                relation_type: row.get(2)?,
                target_object_id: row.get(3)?,
                properties: serde_json::from_str(&row.get::<_, String>(4)?)?,
                created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(5)?)?
                    .with_timezone(&Utc),
                world_id: row.get(6).unwrap_or_default(),
            }))
        } else {
            Ok(None)
        }
    }

    pub fn upsert_provider(&self, p: &ProviderInfo) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO providers (id, name, capabilities) VALUES (?1, ?2, ?3)",
            params![p.id, p.name, serde_json::to_string(&p.capabilities)?],
        )?;
        Ok(())
    }

    pub fn list_providers(&self) -> Result<Vec<ProviderInfo>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, name, capabilities FROM providers")?;
        let mut rows = stmt.query(params![])?;
        let mut out = vec![];
        while let Some(row) = rows.next()? {
            out.push(ProviderInfo {
                id: row.get(0)?,
                name: row.get(1)?,
                capabilities: serde_json::from_str(&row.get::<_, String>(2)?)?,
                version: None,
            });
        }
        Ok(out)
    }

    pub fn get_provider(&self, id: &str) -> Result<Option<ProviderInfo>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, name, capabilities FROM providers WHERE id = ?1")?;
        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(ProviderInfo {
                id: row.get(0)?,
                name: row.get(1)?,
                capabilities: serde_json::from_str(&row.get::<_, String>(2)?)?,
                version: None,
            }))
        } else {
            Ok(None)
        }
    }
}

fn row_to_object(row: &rusqlite::Row) -> rusqlite::Result<Object> {
    Ok(Object {
        id: row.get(0)?,
        r#type: row.get(1)?,
        properties: serde_json::from_str(&row.get::<_, String>(2)?)
            .unwrap_or(serde_json::json!({})),
        provider_id: row.get(3)?,
        external_id: row.get(4)?,
        created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(5)?)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        updated_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(6)?)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
    })
}
