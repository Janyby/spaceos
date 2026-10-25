use crate::query::{EventQuery, ObjectQuery, RelationQuery};
use crate::semantic::{
    merge_properties, validate_correlation_id, validate_properties, CreateSemanticObject,
    CreateSemanticRelation, PatchSemanticObject, SemanticObjectResult, SemanticRelationResult,
    SEMANTIC_ACTOR, USER_RELATION_TYPE,
};
use crate::semantic_read::{
    ContextRelation, ObjectContext, ObjectContextQuery, ObjectSummary, RelationDirection,
};
use crate::{Event, Object, Observation, ObservationOutcome, ProviderInfo, Relation, Space};
use anyhow::Result;
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use std::path::Path;
use std::sync::Mutex;
#[path = "mission_storage.rs"]
mod mission_storage;
#[path = "action_storage.rs"]
mod action_storage;
pub(crate) use action_storage::ActionAuthority;
#[cfg(test)]
#[path = "storage_file_admission_tests.rs"]
mod file_admission_tests;
#[cfg(test)]
#[path = "storage_browser_observation_tests.rs"]
mod browser_observation_tests;

// Private metadata, not a provider payload, semantic object or capability.
const FILE_ADMISSION_TABLE: &str = "CREATE TABLE filesystem_file_admissions (
    object_id TEXT PRIMARY KEY NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    observation_id TEXT NOT NULL UNIQUE REFERENCES observation_log(observation_id),
    event_id TEXT NOT NULL UNIQUE REFERENCES events(id),
    admitted_type TEXT NOT NULL CHECK(admitted_type = 'File'),
    provider_id TEXT NOT NULL CHECK(provider_id = 'filesystem'),
    world_id TEXT NOT NULL CHECK(length(trim(world_id)) > 0),
    external_id TEXT NOT NULL,
    object_created_at TEXT NOT NULL,
    instance_id TEXT NOT NULL CHECK(length(instance_id) > 0),
    source_root TEXT NOT NULL,
    source_binding_digest TEXT NOT NULL CHECK(length(source_binding_digest) = 64)
)";
const FILE_ADMISSION_TRIGGER: &str = "CREATE TRIGGER invalidate_filesystem_file_admission
AFTER UPDATE ON objects BEGIN
    DELETE FROM filesystem_file_admissions WHERE object_id = OLD.id OR object_id = NEW.id;
END";

fn verify_file_admission_schema(conn: &Connection) -> Result<()> {
    anyhow::ensure!(conn.query_row("PRAGMA foreign_keys", [], |r| r.get::<_, i64>(0))? == 1,
        "File admission unavailable: foreign keys disabled");
    anyhow::ensure!(matches!(conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))?, 4 | 5),
        "File admission unavailable: unsupported schema version");
    // Check definitions, not names alone: a no-op replacement trigger is unsafe.
    for (kind, name, expected) in [
        ("table", "filesystem_file_admissions", FILE_ADMISSION_TABLE),
        ("trigger", "invalidate_filesystem_file_admission", FILE_ADMISSION_TRIGGER),
    ] {
        let sql: Option<String> = conn.query_row(
            "SELECT sql FROM sqlite_master WHERE type=?1 AND name=?2", params![kind,name],
            |r| r.get(0)).optional()?;
        anyhow::ensure!(sql.as_deref().map(str::trim) == Some(expected.trim()),
            "File admission unavailable: missing or incompatible {name}");
    }
    Ok(())
}

fn browser_pin(conn:&Connection,id:&str)->Result<Option<Vec<String>>> {
    let row:Option<(String,String)>=conn.query_row("SELECT name,capabilities FROM providers WHERE id=?1",params![id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    row.map(|(name,data)|{anyhow::ensure!(name=="browser-observation-pin-v1","invalid browser authority record");Ok(serde_json::from_str(&data)?)}).transpose()
}

fn next_file_version(previous: chrono::DateTime<Utc>, now: chrono::DateTime<Utc>) -> Result<chrono::DateTime<Utc>> {
    use chrono::Datelike;
    anyhow::ensure!((0..=9999).contains(&previous.year()) && previous.timestamp_subsec_nanos() < 1_000_000_000,
        "invalid FileChanged base timestamp representation");
    let tick = previous.checked_add_signed(chrono::Duration::nanoseconds(1))
        .ok_or_else(|| anyhow::anyhow!("FileChanged timestamp overflow"))?;
    let next = tick.max(now);
    anyhow::ensure!((0..=9999).contains(&next.year()) && next > previous
        && next.timestamp_subsec_nanos() < 1_000_000_000, "FileChanged timestamp overflow or invalid precision");
    anyhow::ensure!(chrono::DateTime::parse_from_rfc3339(&next.to_rfc3339())? == next,
        "FileChanged timestamp cannot round-trip exactly");
    Ok(next)
}

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
    fn get_object(&self, id: &str) -> Result<Option<Object>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, type, properties, provider_id, external_id, created_at, updated_at FROM objects WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        Ok(rows.next()?.map(row_to_object).transpose()?)
    }

    fn semantic_ownership(&self, id: &str) -> Result<Option<(String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT owner_actor, world_id FROM semantic_object_ownership WHERE object_id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            Ok(Some((row.get(0)?, row.get(1)?)))
        } else {
            Ok(None)
        }
    }

    fn insert_semantic_object(&self, obj: &Object, owner: &str, world_id: &str) -> Result<()> {
        verify_file_admission_schema(self.conn)?;
        self.conn.execute(
            "INSERT INTO objects (id, type, properties, provider_id, external_id, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
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
        self.conn.execute(
            "INSERT INTO semantic_object_ownership (object_id, owner_actor, world_id) VALUES (?1, ?2, ?3)",
            params![obj.id, owner, world_id],
        )?;
        Ok(())
    }

    fn update_semantic_properties(
        &self,
        id: &str,
        properties: &serde_json::Value,
        updated_at: chrono::DateTime<Utc>,
    ) -> Result<()> {
        verify_file_admission_schema(self.conn)?;
        let changed = self.conn.execute(
            "UPDATE objects SET properties = ?1, updated_at = ?2 WHERE id = ?3",
            params![
                serde_json::to_string(properties)?,
                updated_at.to_rfc3339(),
                id
            ],
        )?;
        if changed != 1 {
            anyhow::bail!("semantic object disappeared during patch");
        }
        Ok(())
    }

    fn insert_semantic_relation(&self, relation: &Relation) -> Result<()> {
        self.conn.execute(
            "INSERT INTO relations (id, source_object_id, relation_type, target_object_id, properties, created_at, world_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                relation.id,
                relation.source_object_id,
                relation.relation_type,
                relation.target_object_id,
                serde_json::to_string(&relation.properties)?,
                relation.created_at.to_rfc3339(),
                relation.world_id,
            ],
        )?;
        Ok(())
    }

    fn event_seq(&self, event_id: &str) -> Result<u64> {
        Ok(self.conn.query_row(
            "SELECT seq FROM events WHERE id = ?1",
            params![event_id],
            |row| row.get::<_, i64>(0),
        )? as u64)
    }

    pub fn create_object(&self, obj: &Object) -> Result<()> {
        verify_file_admission_schema(self.conn)?;
        // Protect semantic-owned objects from legacy/provider overwrites.
        // If an ownership marker exists for this object_id, deny the write.
        // This check is atomic with the UPSERT because both run inside
        // the same SQLite transaction (IMMEDIATE).
        let has_ownership = self
            .conn
            .query_row(
                "SELECT 1 FROM semantic_object_ownership WHERE object_id = ?1",
                params![&obj.id],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);
        if has_ownership {
            anyhow::bail!("object is semantic-owned and read-only to legacy/provider paths");
        }

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
        verify_file_admission_schema(self.conn)?;
        // Protect semantic-owned objects from provider observation deletion.
        let has_ownership = self
            .conn
            .query_row(
                "SELECT 1 FROM semantic_object_ownership WHERE object_id = ?1",
                params![id],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);
        if has_ownership {
            anyhow::bail!("object is semantic-owned and read-only to legacy/provider paths");
        }

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
/// (`events.seq` cursor, FK constraints, `observation_log`, indexes),
/// 3 = semantic object ownership, 4 = durable filesystem File admissions.
pub const SCHEMA_VERSION: i64 = 5;

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
        self.apply_observation_inner(obs, None)
    }

    pub(crate) fn apply_admitted_file_created(
        &mut self, verified: &crate::provider_observation::VerifiedFilesystemAdmission,
    ) -> Result<ObservationOutcome> {
        anyhow::ensure!(verified.observation().event.event_type == "FileCreated", "FileCreated proof required");
        self.apply_observation_inner(verified.observation(), Some(verified))
    }

    pub(crate) fn apply_verified_file_changed(
        &mut self, verified: &crate::provider_observation::VerifiedFilesystemAdmission,
    ) -> Result<ObservationOutcome> {
        let obs = verified.observation();
        anyhow::ensure!(obs.event.event_type == "FileChanged" && obs.objects.len() == 1
            && obs.relations.is_empty() && obs.delete_ids.is_empty() && !obs.observation_id.trim().is_empty(),
            "unsupported FileChanged observation");
        let incoming = &obs.objects[0];
        self.transaction(|tx| {
            verify_file_admission_schema(tx.conn)?;
            let mut current = tx.get_object(&incoming.id)?.ok_or_else(|| anyhow::anyhow!("FileChanged requires existing File"))?;
            anyhow::ensure!(Self::has_file_admission_on(tx.conn, &current.id)?, "FileChanged requires valid File admission receipt");
            crate::provider_observation::validate_file_change(&current, incoming)?;
            // All columns are protected TEXT. Save the original receipt before
            // UPDATE fires its invalidation trigger; never synthesize missing trust.
            let receipt: Vec<String> = tx.conn.query_row(
                "SELECT object_id,observation_id,event_id,admitted_type,provider_id,world_id,external_id,object_created_at,instance_id,source_root,source_binding_digest
                 FROM filesystem_file_admissions WHERE object_id=?1", params![current.id],
                |row| (0..11).map(|i| row.get(i)).collect())?;
            let (instance, root, digest) = verified.source();
            anyhow::ensure!(receipt[5] == obs.world_id && receipt[8] == instance
                && receipt[9] == root.to_string_lossy() && receipt[10] == digest,
                "FileChanged source authority differs from admission");
            if let Some(event_id) = tx.observation_applied(&obs.observation_id)? {
                anyhow::ensure!(event_id == obs.event.id, "FileChanged observation identity collision");
                let matches: bool = tx.conn.query_row(
                    "SELECT event_type='FileChanged' AND actor='filesystem' AND provider_id='filesystem'
                     AND world_id=?2 AND object_ids=?3 AND json_extract(payload,'$.object_id')=?4
                     AND json_extract(payload,'$.path')=?5 FROM events WHERE id=?1",
                    params![event_id,obs.world_id,serde_json::to_string(&obs.event.object_ids)?,current.id,current.external_id], |r|r.get(0))?;
                anyhow::ensure!(matches, "FileChanged replay identity mismatch");
                return Ok(ObservationOutcome { applied:false, event_id, seq:0 });
            }
            anyhow::ensure!(incoming.updated_at == current.updated_at, "conflict: FileChanged expected updated_at differs from current version");
            current.updated_at = next_file_version(current.updated_at, Utc::now())?;
            current.properties["meta"] = incoming.properties["meta"].clone();
            let rows = tx.conn.execute("UPDATE objects SET properties=?1, updated_at=?2 WHERE id=?3",
                params![serde_json::to_string(&current.properties)?,current.updated_at.to_rfc3339(),current.id])?;
            anyhow::ensure!(rows == 1, "FileChanged canonical File disappeared");
            tx.conn.execute("INSERT INTO filesystem_file_admissions
                (object_id,observation_id,event_id,admitted_type,provider_id,world_id,external_id,object_created_at,instance_id,source_root,source_binding_digest)
                VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)", rusqlite::params_from_iter(receipt.iter()))?;
            let mut event = obs.event.clone();
            if !obs.facts.is_empty() && event.payload.get("facts").is_none() { event.payload["facts"] = serde_json::to_value(&obs.facts)?; }
            anyhow::ensure!(tx.record_event_if_new(&event)?, "FileChanged event id collision");
            tx.insert_observation_log(&obs.observation_id, &event.id)?;
            anyhow::ensure!(Self::has_file_admission_on(tx.conn, &current.id)?, "FileChanged receipt invariant failed");
            Ok(ObservationOutcome { applied:true, event_id:event.id.clone(), seq:tx.event_seq(&event.id)? })
        })
    }

    pub(crate) fn validate_browser_authority(&self, a: &crate::provider_observation::BrowserAuthority) -> Result<()> {
        let mut floor = None;
        for (id, digest) in a.pins() {
            let data = browser_pin(&self.conn, &id)?.ok_or_else(|| anyhow::anyhow!("Browser authority pin missing"))?;
            anyhow::ensure!(data.len() == 3 && data[0] == "browser-pin-v1" && data[1] == digest, "Browser authority pin mismatch");
            let value: u64 = data[2].parse()?;
            anyhow::ensure!(floor.is_none_or(|f| f == value), "Browser authority boundary mismatch");
            floor = Some(value);
        }
        Ok(())
    }

    pub(crate) fn register_browser_authority(&mut self,a:&crate::provider_observation::BrowserAuthority)->Result<()> {
        self.transaction(|tx|{
            let pins=a.pins();let first=browser_pin(tx.conn,&pins[0].0)?;let source=browser_pin(tx.conn,&pins[1].0)?;
            let initial=first.is_none()&&source.is_none();
            anyhow::ensure!(initial||first.is_some()&&source.is_some(),"incomplete browser authority pins");
            anyhow::ensure!(!initial||browser_pin(tx.conn,&pins[2].0)?.is_none(),"orphan browser runtime pin");
            let floor=if let Some(data)=&first {
                anyhow::ensure!(data.len()==3&&data[0]=="browser-pin-v1"&&data[1]==pins[0].1,"browser instance authority changed");
                data[2].parse::<u64>()?
            }else{tx.conn.query_row("SELECT COALESCE(MAX(seq),0) FROM events",[],|r|r.get::<_,u64>(0))?};
            for (i,(id,digest)) in pins.iter().enumerate() {
                let expected=vec!["browser-pin-v1".to_string(),digest.clone(),floor.to_string()];
                match browser_pin(tx.conn,id)? {
                    Some(data)=>anyhow::ensure!(data==expected,"browser source or runtime authority changed"),
                    None=>{
                        anyhow::ensure!(initial||i==2,"missing browser authority pin");
                        if !initial {
                            let history:i64=tx.conn.query_row("SELECT count(*) FROM objects WHERE type='BrowserTab' AND json_extract(properties,'$.source_id')=?1 AND json_extract(properties,'$.runtime_epoch')=?2",params![a.runtime.source_id,a.runtime.runtime_epoch],|r|r.get(0))?;
                            let events:i64=tx.conn.query_row("SELECT count(*) FROM events WHERE event_type='BrowserTabCreated' AND json_extract(payload,'$.tab.properties.source_id')=?1 AND json_extract(payload,'$.tab.properties.runtime_epoch')=?2",params![a.runtime.source_id,a.runtime.runtime_epoch],|r|r.get(0))?;
                            anyhow::ensure!(history==0&&events==0,"missing browser runtime pin with historical state");
                        }
                        tx.conn.execute("INSERT INTO providers(id,name,capabilities) VALUES (?1,'browser-observation-pin-v1',?2)",params![id,serde_json::to_string(&expected)?])?;
                    }
                }
            }
            Ok(())
        })
    }

    pub(crate) fn apply_verified_browser_tab_created(&mut self,v:&crate::provider_observation::VerifiedBrowserAdmission)->Result<(ObservationOutcome,Object,Event)> {
        let obs=&v.observation;let a=&v.authority;
        self.transaction(|tx|{
            verify_file_admission_schema(tx.conn)?;
            let pins=a.pins();let mut floor=None;
            for (id,digest) in &pins {
                let data=browser_pin(tx.conn,id)?.ok_or_else(||anyhow::anyhow!("browser authority pin missing"))?;
                anyhow::ensure!(data.len()==3&&data[0]=="browser-pin-v1"&&data[1]==*digest,"browser authority pin mismatch");
                let value=data[2].parse::<u64>()?;anyhow::ensure!(floor.is_none_or(|f|f==value),"browser admission boundary mismatch");floor=Some(value);
            }
            let candidate=&obs.objects[0];
            if let Some(event_id)=tx.observation_applied(&obs.observation_id)? {
                anyhow::ensure!(event_id==obs.event.id,"browser observation identity collision");
                let current=tx.get_object(&candidate.id)?.ok_or_else(||anyhow::anyhow!("canonical browser tab missing"))?;
                let (seq,kind,actor,provider,world,ids,payload):(u64,String,String,Option<String>,String,String,String)=tx.conn.query_row(
                    "SELECT seq,event_type,actor,provider_id,world_id,object_ids,payload FROM events WHERE id=?1",params![event_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?)))?;
                let saved:serde_json::Value=serde_json::from_str(&payload)?;
                anyhow::ensure!(seq>floor.unwrap()&&kind=="BrowserTabCreated"&&actor=="browser"&&provider.as_deref()==Some("browser")&&world==a.runtime.world_id
                    &&serde_json::from_str::<Vec<String>>(&ids)?==obs.event.object_ids&&saved==serde_json::json!({"tab":current})
                    &&current.id==candidate.id&&current.provider_id=="browser"&&current.r#type=="BrowserTab"&&current.external_id==candidate.external_id,
                    "browser replay provenance or canonical snapshot mismatch");
                for key in ["world_id","source_id","instance_id","runtime_epoch","target_id","target_type"] {
                    anyhow::ensure!(current.properties[key]==candidate.properties[key],"browser replay identity mismatch");
                }
                let mut event=obs.event.clone();event.seq=seq;event.payload=saved;
                return Ok((ObservationOutcome{applied:false,event_id,seq:0},current,event));
            }
            anyhow::ensure!(tx.get_object(&candidate.id)?.is_none(),"browser canonical object collision");
            let external:i64=tx.conn.query_row("SELECT count(*) FROM objects WHERE provider_id='browser' AND external_id=?1",params![candidate.external_id],|r|r.get(0))?;
            anyhow::ensure!(external==0,"browser external identity collision");
            let mut object=candidate.clone();object.created_at=Utc::now();object.updated_at=object.created_at;
            tx.conn.execute("INSERT INTO objects(id,type,properties,provider_id,external_id,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?6)",params![object.id,object.r#type,serde_json::to_string(&object.properties)?,object.provider_id,object.external_id,object.created_at.to_rfc3339()])?;
            let mut event=obs.event.clone();event.timestamp=object.created_at;event.payload=serde_json::json!({"tab":object});
            anyhow::ensure!(tx.record_event_if_new(&event)?,"browser event identity collision");tx.insert_observation_log(&obs.observation_id,&event.id)?;event.seq=tx.event_seq(&event.id)?;
            anyhow::ensure!(event.seq>floor.unwrap(),"browser admission boundary violation");
            Ok((ObservationOutcome{applied:true,event_id:event.id.clone(),seq:event.seq},object,event))
        })
    }

    fn apply_observation_inner(
        &mut self, obs: &Observation,
        verified: Option<&crate::provider_observation::VerifiedFilesystemAdmission>,
    ) -> Result<ObservationOutcome> {
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
            verify_file_admission_schema(tx.conn)?;
            if let Some(stored_event) = tx.observation_applied(&obs.observation_id)? {
                if verified.is_some() {
                    anyhow::ensure!(stored_event == event.id, "observation identity collision");
                    let stored: (String,String,String,String) = tx.conn.query_row(
                        "SELECT world_id,object_ids,json_extract(payload,'$.object_id'),json_extract(payload,'$.path') FROM events WHERE id=?1",
                        params![stored_event], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
                    anyhow::ensure!(stored.0 == obs.world_id
                        && serde_json::from_str::<Vec<String>>(&stored.1)? == event.object_ids
                        && stored.2 == obs.objects[0].id
                        && Some(stored.3.as_str()) == obs.objects[0].external_id.as_deref(),
                        "observation replay conflicts with admitted resource");
                }
                // Replay never reconstructs a receipt lost to an unverified write.
                let existing: Option<(String,String,String)> = tx.conn.query_row(
                    "SELECT admitted_type, external_id, world_id FROM filesystem_file_admissions WHERE object_id=?1",
                    params![obs.objects.first().map(|o|o.id.as_str()).unwrap_or("")],
                    |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
                if verified.is_some() {
                    if let Some((kind,external,world)) = existing {
                        let object = &obs.objects[0];
                        anyhow::ensure!(object.r#type == kind && object.external_id.as_deref() == Some(external.as_str())
                            && obs.world_id == world, "replay conflicts with File admission identity");
                    }
                }
                return Ok(ObservationOutcome {
                    applied: false,
                    event_id: event_id.clone(),
                    seq: 0,
                });
            }
            if verified.is_some() {
                anyhow::ensure!(obs.objects.len() == 1 && tx.get_object(&obs.objects[0].id)?.is_none(),
                    "File admission requires a new canonical object");
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
            if let Some(verified) = verified {
                let object = &objects[0];
                if object.r#type == "File" {
                    let (instance, root, digest) = verified.source();
                    tx.conn.execute(
                        "INSERT INTO filesystem_file_admissions
                        (object_id,observation_id,event_id,admitted_type,provider_id,world_id,external_id,
                         object_created_at,instance_id,source_root,source_binding_digest)
                         VALUES (?1,?2,?3,'File','filesystem',?4,?5,?6,?7,?8,?9)",
                        params![object.id,obs.observation_id,event.id,obs.world_id,object.external_id,
                            object.created_at.to_rfc3339(),instance,root.to_string_lossy(),digest])?;
                }
            }
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

    /// Read-only redelivery check used by Core observation admission. This
    /// does not confer authorization; the caller must still validate source.
    pub fn observation_event_id(&self, observation_id: &str) -> Result<Option<String>> {
        self.conn.query_row(
            "SELECT event_id FROM observation_log WHERE observation_id = ?1",
            params![observation_id],
            |row| row.get(0),
        ).optional().map_err(Into::into)
    }

    /// Core-internal predicate, not a permission or public query endpoint.
    #[allow(dead_code)] // Also exposed to storage tests without opening a transaction.
    pub(crate) fn has_file_admission(&self, object_id: &str) -> Result<bool> {
        Self::has_file_admission_on(&self.conn, object_id)
    }

    // Shared predicate: relation validation passes its existing write transaction.
    fn has_file_admission_on(conn: &Connection, object_id: &str) -> Result<bool> {
        verify_file_admission_schema(conn)?;
        Ok(conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM filesystem_file_admissions a
             JOIN objects o ON o.id=a.object_id
             JOIN observation_log l ON l.observation_id=a.observation_id AND l.event_id=a.event_id
             JOIN events e ON e.id=a.event_id
             WHERE a.object_id=?1 AND o.type='File' AND o.provider_id='filesystem'
             AND o.external_id=a.external_id AND o.created_at=a.object_created_at
             AND json_extract(o.properties,'$.world_id')=a.world_id
             AND json_extract(o.properties,'$.path')=a.external_id
             AND e.event_type='FileCreated' AND e.actor='filesystem' AND e.provider_id='filesystem'
             AND e.world_id=a.world_id AND json_array_length(e.object_ids)=1
             AND json_extract(e.object_ids,'$[0]')=a.object_id
             AND json_extract(e.payload,'$.object_id')=a.object_id
             AND json_extract(e.payload,'$.path')=a.external_id
             AND NOT EXISTS(SELECT 1 FROM semantic_object_ownership s WHERE s.object_id=o.id))",
            params![object_id], |r|r.get(0))?)
    }

    /// Create a user-owned semantic object and its success event in one
    /// transaction. This is intentionally separate from legacy UPSERT APIs.
    pub fn create_semantic_object(
        &mut self,
        command: &CreateSemanticObject,
    ) -> Result<SemanticObjectResult> {
        validate_correlation_id(&command.correlation_id)?;
        validate_properties(&command.properties, "properties")?;
        let object_type = command.object_type.trim();
        if matches!(object_type, "Mission" | "MissionStep" | "MissionAttempt") {
            anyhow::bail!("Mission lifecycle requires mission_command");
        }
        if object_type.is_empty() || object_type.len() > 100 {
            anyhow::bail!("object_type must contain 1..=100 characters");
        }
        let mut properties = command.properties.clone();
        properties["world_id"] = serde_json::json!(command.world_id);
        let object = Object::new(object_type, SEMANTIC_ACTOR, properties);
        let mut event = Event::new(
            "ObjectCreated",
            SEMANTIC_ACTOR,
            serde_json::json!({
                "semantic_operation": "create_semantic_object",
                "object_id": object.id,
                "object_type": object.r#type,
            }),
        );
        event.id = command.correlation_id.clone();
        event.correlation_id = Some(command.correlation_id.clone());
        event.world_id = command.world_id.clone();
        event.object_ids = vec![object.id.clone()];

        let object_out = object.clone();
        let event_out = event.clone();
        self.transaction_with_correlation(&command.correlation_id, |tx| {
            tx.insert_semantic_object(&object, SEMANTIC_ACTOR, &command.world_id)?;
            if !tx.record_event_if_new(&event)? {
                anyhow::bail!("correlation_id conflict: success event already exists");
            }
            let mut persisted_event = event_out.clone();
            persisted_event.seq = tx.event_seq(&event.id)?;
            Ok(SemanticObjectResult {
                object: object_out.clone(),
                event: persisted_event,
            })
        })
    }

    /// Patch only the properties of an owned semantic object, preserving
    /// every identity field, and atomically append ObjectUpdated.
    pub fn patch_semantic_object(
        &mut self,
        command: &PatchSemanticObject,
    ) -> Result<SemanticObjectResult> {
        self.patch_semantic_object_as(command, SEMANTIC_ACTOR)
    }

    /// IPC authenticates and authorizes the actor before entering storage.
    /// Delegation never changes the object's ownership or provider identity.
    pub(crate) fn patch_semantic_object_as(
        &mut self,
        command: &PatchSemanticObject,
        actor: &str,
    ) -> Result<SemanticObjectResult> {
        validate_correlation_id(&command.correlation_id)?;
        validate_properties(&command.properties_patch, "properties_patch")?;
        self.transaction_with_correlation(&command.correlation_id, |tx| {
            let mut object = tx
                .get_object(&command.object_id)?
                .ok_or_else(|| anyhow::anyhow!("semantic object not found"))?;
            let (owner, owned_world) = tx
                .semantic_ownership(&object.id)?
                .ok_or_else(|| anyhow::anyhow!("object is provider-owned and read-only"))?;
            if matches!(object.r#type.as_str(), "Mission" | "MissionStep" | "MissionAttempt") {
                anyhow::bail!("Mission lifecycle requires mission_command");
            }
            if owner != SEMANTIC_ACTOR {
                anyhow::bail!("object is not owned by {SEMANTIC_ACTOR}");
            }
            if owned_world != command.world_id
                || object.properties.get("world_id").and_then(|v| v.as_str())
                    != Some(command.world_id.as_str())
            {
                anyhow::bail!("object does not belong to configured semantic world");
            }
            // Defense in depth: verify identity fields have not been corrupted
            // by legacy/provider write paths. If any mismatch, refuse the patch.
            if object.provider_id != SEMANTIC_ACTOR {
                anyhow::bail!("object identity corrupted: provider_id mismatch (expected {SEMANTIC_ACTOR}, got {})", object.provider_id);
            }
            if object.external_id.is_some() {
                anyhow::bail!("object identity corrupted: external_id must be null for semantic objects");
            }
            // Note: we don't check object.r#type or created_at here because
            // they are not mutable by semantic patch anyway; the ownership
            // marker is the primary guard. The legacy UPSERT guard in
            // create_object/update_object is the primary protection.

            merge_properties(&mut object.properties, &command.properties_patch);
            object.updated_at = Utc::now();
            tx.update_semantic_properties(&object.id, &object.properties, object.updated_at)?;

            let mut event = Event::new(
                "ObjectUpdated",
                actor,
                serde_json::json!({
                    "semantic_operation": "patch_semantic_object",
                    "object_id": object.id,
                }),
            );
            event.id = command.correlation_id.clone();
            event.correlation_id = Some(command.correlation_id.clone());
            event.world_id = command.world_id.clone();
            event.object_ids = vec![object.id.clone()];
            if !tx.record_event_if_new(&event)? {
                anyhow::bail!("correlation_id conflict: success event already exists");
            }
            event.seq = tx.event_seq(&event.id)?;
            Ok(SemanticObjectResult { object, event })
        })
    }

    /// Create a strictly validated user relation and RelationCreated event
    /// atomically. Both endpoints must be semantic-user objects in one World.
    pub fn create_semantic_relation(
        &mut self,
        command: &CreateSemanticRelation,
    ) -> Result<SemanticRelationResult> {
        self.create_semantic_relation_as(command, SEMANTIC_ACTOR)
    }

    pub(crate) fn create_semantic_relation_as(
        &mut self,
        command: &CreateSemanticRelation,
        actor: &str,
    ) -> Result<SemanticRelationResult> {
        validate_correlation_id(&command.correlation_id)?;
        validate_properties(&command.properties, "properties")?;
        if command.relation_type != USER_RELATION_TYPE {
            anyhow::bail!("unsupported semantic relation type");
        }
        self.transaction_with_correlation(&command.correlation_id, |tx| {
            let source = tx
                .get_object(&command.source_id)?
                .ok_or_else(|| anyhow::anyhow!("source object not found"))?;
            let target = tx
                .get_object(&command.target_id)?
                .ok_or_else(|| anyhow::anyhow!("target object not found"))?;
            let mut file_endpoints = 0;
            for (label, object) in [("source", &source), ("target", &target)] {
                if actor != SEMANTIC_ACTOR && matches!(object.r#type.as_str(), "Mission" | "MissionStep" | "MissionAttempt") {
                    anyhow::bail!("Mission lifecycle requires mission_command");
                }
                if let Some((owner, world)) = tx.semantic_ownership(&object.id)? {
                    if owner != SEMANTIC_ACTOR || world != command.world_id {
                        anyhow::bail!("{label} object does not belong to configured semantic world");
                    }
                    if object.provider_id != SEMANTIC_ACTOR || object.external_id.is_some() {
                        anyhow::bail!("{label} object identity corrupted");
                    }
                } else {
                    anyhow::ensure!(object.r#type == "File" && object.provider_id == "filesystem"
                        && Self::has_file_admission_on(tx.conn, &object.id)?,
                        "{label} object has no verified filesystem File admission");
                    file_endpoints += 1;
                }
                if object.properties.get("world_id").and_then(|v| v.as_str())
                    != Some(command.world_id.as_str())
                {
                    anyhow::bail!("{label} object has a mismatched world identity");
                }
            }
            anyhow::ensure!(file_endpoints <= 1, "File to File semantic relations are not allowed");
            let mut relation = Relation::new(
                command.source_id.clone(),
                command.relation_type.clone(),
                command.target_id.clone(),
            );
            relation.properties = command.properties.clone();
            relation.world_id = command.world_id.clone();
            tx.insert_semantic_relation(&relation)?;

            let mut event = Event::new(
                "RelationCreated",
                actor,
                serde_json::json!({
                    "semantic_operation": "create_semantic_relation",
                    "relation_id": relation.id,
                    "relation_type": relation.relation_type,
                    "source_id": relation.source_object_id,
                    "target_id": relation.target_object_id,
                }),
            );
            event.id = command.correlation_id.clone();
            event.correlation_id = Some(command.correlation_id.clone());
            event.world_id = command.world_id.clone();
            event.object_ids = vec![command.source_id.clone(), command.target_id.clone()];
            if !tx.record_event_if_new(&event)? {
                anyhow::bail!("correlation_id conflict: success event already exists");
            }
            event.seq = tx.event_seq(&event.id)?;
            Ok(SemanticRelationResult { relation, event })
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
            );
            CREATE TABLE IF NOT EXISTS semantic_object_ownership (
                object_id TEXT PRIMARY KEY REFERENCES objects(id) ON DELETE CASCADE,
                owner_actor TEXT NOT NULL,
                world_id TEXT NOT NULL
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
        anyhow::ensure!(v <= SCHEMA_VERSION, "unsupported future schema version");
        if v < 1 {
            // The `init()` schema above IS v1: stamp it, nothing to do.
            self.set_user_version(1)?;
            v = 1;
        }
        if v < 2 {
            self.migrate_v2()?;
            self.set_user_version(2)?;
            v = 2;
        }
        if v < 3 {
            self.migrate_v3()?;
            self.set_user_version(3)?;
            v = 3;
        }
        if v < 4 {
            // DDL and version change commit together. No historical backfill.
            let tx = self.conn.unchecked_transaction()?;
            tx.execute_batch(FILE_ADMISSION_TABLE)?;
            tx.execute_batch(FILE_ADMISSION_TRIGGER)?;
            tx.pragma_update(None, "user_version", 4)?;
            verify_file_admission_schema(&tx)?;
            tx.commit()?;
            v = 4;
        }
        if v == 4 { self.migrate_action_authority()?; }
        self.verify_action_schema()?;
        Ok(())
    }

    fn migrate_v3(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS semantic_object_ownership (
                object_id TEXT PRIMARY KEY REFERENCES objects(id) ON DELETE CASCADE,
                owner_actor TEXT NOT NULL,
                world_id TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_semantic_ownership_world
                ON semantic_object_ownership(world_id);",
        )?;
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
        verify_file_admission_schema(&self.conn)?;
        // Protect semantic-owned objects from legacy/provider overwrites.
        // This check is atomic with the UPSERT because both run inside
        // the same SQLite transaction context.
        let has_ownership = self
            .conn
            .query_row(
                "SELECT 1 FROM semantic_object_ownership WHERE object_id = ?1",
                params![&obj.id],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);
        if has_ownership {
            anyhow::bail!("object is semantic-owned and read-only to legacy/provider paths");
        }

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

    /// Return one canonical, bounded, one-hop neighborhood from a single
    /// SQLite read transaction.
    ///
    /// The query preserves every relation row (including duplicate relation
    /// types), marks direction explicitly, reports truncation, and uses the
    /// existing source/target indexes through two UNION ALL branches. A
    /// self-relation appears once. A grandfathered orphan is returned with a
    /// missing neighbor summary; a relation to an object in another World is
    /// excluded so object identity cannot cross the requested scope.
    pub fn get_object_context(
        &mut self,
        query: &ObjectContextQuery,
    ) -> Result<Option<ObjectContext>> {
        let limit = query.validate()?;
        let sql_limit = i64::try_from(limit + 1)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Deferred)?;

        let object = {
            let mut stmt = tx.prepare(
                "SELECT id, type, properties, provider_id, external_id, created_at, updated_at
                 FROM objects WHERE id = ?1",
            )?;
            let mut rows = stmt.query(params![query.object_id])?;
            rows.next()?.map(row_to_object).transpose()?
        };
        let Some(object) = object else {
            tx.commit()?;
            return Ok(None);
        };
        if object.properties.get("world_id").and_then(|v| v.as_str())
            != Some(query.world_id.as_str())
        {
            // Deliberately indistinguishable from a missing object: callers
            // cannot probe canonical ids outside their authorized World.
            tx.commit()?;
            return Ok(None);
        }

        let mut relations = Vec::with_capacity(limit + 1);
        {
            let mut stmt = tx.prepare(
                "WITH adjacent AS (
                    SELECT id, source_object_id, relation_type, target_object_id,
                           created_at, world_id, target_object_id AS neighbor_id,
                           CASE WHEN target_object_id = source_object_id
                                THEN 'self_relation' ELSE 'outgoing' END AS direction
                      FROM relations INDEXED BY idx_relations_source
                     WHERE source_object_id = ?1 AND world_id = ?2
                    UNION ALL
                    SELECT id, source_object_id, relation_type, target_object_id,
                           created_at, world_id, source_object_id AS neighbor_id,
                           'incoming' AS direction
                      FROM relations INDEXED BY idx_relations_target
                     WHERE target_object_id = ?1
                       AND source_object_id <> ?1
                       AND world_id = ?2
                 )
                 SELECT a.id, a.source_object_id, a.relation_type,
                        a.target_object_id, a.created_at, a.world_id, a.direction,
                        n.id, n.type, n.provider_id
                   FROM adjacent a
                   LEFT JOIN objects n ON n.id = a.neighbor_id
                  WHERE n.id IS NULL OR json_extract(n.properties, '$.world_id') = ?2
                  ORDER BY a.created_at ASC, a.id ASC
                  LIMIT ?3",
            )?;
            let mut rows = stmt.query(params![query.object_id, query.world_id, sql_limit])?;
            while let Some(row) = rows.next()? {
                let direction = match row.get::<_, String>(6)?.as_str() {
                    "incoming" => RelationDirection::Incoming,
                    "outgoing" => RelationDirection::Outgoing,
                    "self_relation" => RelationDirection::SelfRelation,
                    other => anyhow::bail!("invalid relation direction returned by query: {other}"),
                };
                let neighbor_id: Option<String> = row.get(7)?;
                let neighbor = match neighbor_id {
                    Some(id) => Some(ObjectSummary {
                        id,
                        r#type: row.get::<_, Option<String>>(8)?.unwrap_or_default(),
                        provider_id: row.get::<_, Option<String>>(9)?.unwrap_or_default(),
                        world_id: query.world_id.clone(),
                    }),
                    None => None,
                };
                relations.push(ContextRelation {
                    id: row.get(0)?,
                    source_object_id: row.get(1)?,
                    relation_type: row.get(2)?,
                    target_object_id: row.get(3)?,
                    created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(4)?)?
                        .with_timezone(&Utc),
                    world_id: row.get(5)?,
                    direction,
                    neighbor,
                });
            }
        }
        let truncated = relations.len() > limit;
        relations.truncate(limit);
        let event_cursor: Option<i64> =
            tx.query_row("SELECT MAX(seq) FROM events", [], |row| row.get(0))?;
        let context = ObjectContext {
            object: ObjectSummary::from_object(&object, &query.world_id),
            relations,
            applied_limit: limit,
            truncated,
            event_cursor: u64::try_from(event_cursor.unwrap_or(0))?,
            world_id: query.world_id.clone(),
        };
        tx.commit()?;
        Ok(Some(context))
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
        verify_file_admission_schema(&self.conn)?;
        // Protect semantic-owned objects from legacy/provider deletion.
        let has_ownership = self
            .conn
            .query_row(
                "SELECT 1 FROM semantic_object_ownership WHERE object_id = ?1",
                params![id],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);
        if has_ownership {
            anyhow::bail!("object is semantic-owned and read-only to legacy/provider paths");
        }

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

    /// Preserve legacy record_event storage semantics while allowing IPC to
    /// publish the committed sequence once, never a fresh seq=0 redelivery.
    pub fn record_event_outcome(&self, event: &Event) -> Result<(bool, u64)> {
        self.record_event(event)?;
        let inserted = self.conn.changes() == 1;
        let seq = self.conn.query_row("SELECT seq FROM events WHERE id=?1", params![event.id], |r| r.get::<_,i64>(0))?;
        Ok((inserted, seq as u64))
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
