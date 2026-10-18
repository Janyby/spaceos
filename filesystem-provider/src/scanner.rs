use crate::{format, identity, metadata};
use anyhow::Result;
use serde_json::{json, Value};
use space_core::{ipc, Event, Object, Relation};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Consecutive scan-misses required before a missing path counts as
/// CONFIRMED_DELETED. A single missing observation is MISSING_UNCERTAIN:
/// not enough evidence to destroy semantic identity. Live watcher Remove
/// events bypass this count (strong, independent evidence).
pub const CONFIRM_MISSES: u32 = 3;

pub struct Scanner {
    pub provider_id: String,
    /// Provider-local uncertainty ledger: external_id -> consecutive
    /// scan-misses. Epistemic provider state, NOT semantic World state:
    /// never stored in Core, never reconstructed, reset on restart
    /// (safe direction -- restart delays confirmation, never hastens it).
    misses: std::sync::Mutex<HashMap<String, u32>>,
    /// Mutual exclusion between full scan passes and single watcher-event
    /// handlings sharing this instance (Breakage B4). Scan snapshots and
    /// watcher decisions must not interleave: an interleaving lets a
    /// duplicate row be born (create vs rename-match race), which later
    /// starves the original into a confirmed delete (+cascade). The guard
    /// is held across IPC awaits; it serializes observers, never Core.
    pub lock: std::sync::Arc<tokio::sync::Mutex<()>>,
}

impl Scanner {
    pub fn new(provider_id: impl Into<String>) -> Self {
        Self {
            provider_id: provider_id.into(),
            misses: std::sync::Mutex::new(HashMap::new()),
            lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Current uncertain set (0 < misses < CONFIRM_MISSES), for tests/metrics.
    pub fn uncertain(&self) -> Vec<String> {
        self.misses.lock().unwrap().keys().cloned().collect()
    }

    /// Walk `root`, reconcile with Core, return stats.
    pub async fn scan(&self, root: &Path) -> Result<Value> {
        self.scan_on(root, ipc::CORE_PIPE).await
    }

    /// Same as [`scan`], but against an explicit Core endpoint (tests).
    pub async fn scan_on(&self, root: &Path, pipe: &str) -> Result<Value> {
        // SpaceTrace Slice 2: one correlation per scan pass links
        // scan_start -> mutations -> scan_end (I26 provider-neutral).
        let scan_corr = format!("scan-{}", uuid::Uuid::new_v4());
        let scan_start = std::time::Instant::now();
        spacetrace::hooks::global_hooks().provider_scan_start(
            &scan_corr,
            &self.provider_id,
            &root.to_string_lossy(),
        );
        // Serialize against concurrent watcher handling on the same
        // instance: the snapshot below must stay valid until the pass ends.
        let _guard = self.lock.lock().await;
        // No observable Reality => no reconciliation. Without this guard a
        // missing/unplugged root would read as "everything deleted".
        if !root.exists() {
            anyhow::bail!("scan root does not exist: {}", root.to_string_lossy());
        }
        let mut files = vec![];
        let mut dirs = vec![];
        walk(root, &mut files, &mut dirs)?;

        let existing = ipc::send_request(
            pipe,
            "query_objects",
            json!({"provider_id": self.provider_id}),
        )
        .await?;
        let existing: Vec<Object> = serde_json::from_value(existing["result"].clone()).unwrap_or_default();

        // map fingerprint -> object for objects whose path no longer exists (rename candidates)
        let mut orphan_by_fp: HashMap<String, Object> = HashMap::new();
        // ids consumed by rename reconciliation below; never treated as deleted
        let mut renamed_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
        let on_disk: std::collections::HashSet<String> = files
            .iter()
            .chain(dirs.iter())
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        for obj in &existing {
            if let Some(ext) = &obj.external_id {
                if !on_disk.contains(ext) {
                    if let Some(fp) = obj.properties.get("fingerprint").and_then(|v| v.as_str()) {
                        orphan_by_fp.insert(fp.to_string(), obj.clone());
                    }
                }
            }
        }

        let mut created = 0;
        let mut updated = 0;
        let mut renamed = 0;
        let existing_by_path: HashMap<String, Object> = existing
            .iter()
            .filter_map(|o| o.external_id.clone().map(|e| (e, o.clone())))
            .collect();
        // Prune ledger keys no row claims anymore (e.g. watcher-side
        // rename moved the row): the ledger tracks rows, not paths.
        // A reappearing path starts counting from zero, never inherits.
        {
            let claimed: std::collections::HashSet<&str> = existing
                .iter()
                .filter_map(|o| o.external_id.as_deref())
                .collect();
            self.misses
                .lock()
                .unwrap()
                .retain(|path, _| claimed.contains(path.as_str()));
        }

        for path in dirs.iter().chain(files.iter()) {
            let meta = metadata::stat(path);
            let is_dir = meta["file_type"] == "Directory";
            let fp = if is_dir { None } else { identity::fingerprint(path) };
            let fmt = if is_dir {
                None
            } else {
                Some(serde_json::to_value(format::detect(path))?)
            };
            let path_str = path.to_string_lossy().to_string();

            let mut props = json!({
                "path": path_str,
                "meta": meta,
            });
            if let Some(f) = &fp {
                props["fingerprint"] = json!(f);
            }
            if let Some(f) = &fmt {
                props["physical_type"] = f["physical_type"].clone();
                props["format"] = f["format"].clone();
                props["semantic_type"] = f["semantic_type"].clone();
                props["detected_by"] = f["detected_by"].clone();
                props["confidence"] = f["confidence"].clone();
            }

            if let Some(existing_obj) = existing_by_path.get(&path_str) {
                let mut updated_obj = existing_obj.clone();
                let old_fp = updated_obj.properties.get("fingerprint").cloned();
                updated_obj.properties = props;
                // Never destroy fingerprint evidence you cannot refresh:
                // if the content is unreadable right now, keep the last
                // known fingerprint so future renames can still match.
                if updated_obj.properties.get("fingerprint").is_none() {
                    if let Some(f) = old_fp {
                        updated_obj.properties["fingerprint"] = f;
                    }
                }
                self.report(
                    pipe,
                    format!("{}-update-{}", self.provider_id, uuid::Uuid::new_v4()),
                    &scan_corr,
                    vec![json!({"path": path_str, "operation": "update"})],
                    vec![updated_obj],
                    vec![],
                    vec![],
                    "FileUpdated",
                    json!({"path": path_str}),
                )
                .await?;
                // Present on disk: any uncertainty about this path is over.
                self.misses.lock().unwrap().remove(&path_str);
                updated += 1;
            } else if let Some(fp) = &fp {
                if let Some(orphan) = orphan_by_fp.remove(fp) {
                    // rename candidate: same fingerprint, old path missing
                    renamed_ids.insert(orphan.id.clone());
                    // Continuity proven by content: clear uncertainty, if any.
                    self.misses.lock().unwrap().remove(&path_str);
                    if let Some(old) = &orphan.external_id {
                        self.misses.lock().unwrap().remove(old);
                    }
                    let mut o = orphan.clone();
                    let old_path = o.external_id.clone().unwrap_or_default();
                    o.external_id = Some(path_str.clone());
                    o.properties = props;
                    o.properties["identity_source"] = json!("fingerprint");
                    self.report(
                        pipe,
                        format!("{}-rename-{}", self.provider_id, uuid::Uuid::new_v4()),
                        &scan_corr,
                        vec![json!({"from": old_path, "to": path_str, "operation": "rename"})],
                        vec![o],
                        vec![],
                        vec![],
                        "FileMoved",
                        json!({"from": old_path, "to": path_str, "observed_at": chrono::Utc::now().to_rfc3339()}),
                    )
                    .await?;
                    renamed += 1;
                    continue;
                }
                self.create_file_object_on(path, props, fmt.is_some(), pipe, &scan_corr).await?;
                created += 1;
            } else if is_dir {
                // Directories carry no fingerprint by design; create as today.
                self.create_file_object_on(path, props, fmt.is_some(), pipe, &scan_corr).await?;
                created += 1;
            } else {
                // Breakage B4: no birth without fingerprint evidence. A file
                // whose content cannot be read right now (transient lock,
                // racing rename) must NOT mint a new identity this scan:
                // that duplicate would shadow the true row's path and
                // starve it into a confirmed delete (+cascade). Skip;
                // the next scan with a readable file decides.
            }
        }

        // V1.7 Phase 1: uncertain evidence. A missing path is
        // MISSING_UNCERTAIN until CONFIRM_MISSES consecutive scans miss it;
        // only then CONFIRMED_DELETED -> hard delete (cascade in Core) with
        // an inferred FileDeleted. Agent/other-provider rows untouched.
        // Present rows above already cleared their miss counts.
        let mut removed = 0;
        for obj in &existing {
            if renamed_ids.contains(&obj.id) {
                continue;
            }
            if let Some(ext) = &obj.external_id {
                if !on_disk.contains(ext) {
                    let n = {
                        let mut m = self.misses.lock().unwrap();
                        let e = m.entry(ext.clone()).or_insert(0);
                        *e += 1;
                        *e
                    };
                    if n < CONFIRM_MISSES {
                        continue;
                    }
                    self.misses.lock().unwrap().remove(ext);
                    // Confirmed deletion: fact + projection die together in
                    // one Core transaction (no orphan FileDeleted events).
                    self.report(
                        pipe,
                        format!("{}-delete-{}", self.provider_id, uuid::Uuid::new_v4()),
                        &scan_corr,
                        vec![json!({"path": ext, "operation": "delete", "confirm_misses": n})],
                        vec![],
                        vec![],
                        vec![obj.id.clone()],
                        "FileDeleted",
                        json!({
                            "path": ext,
                            "observed": "reconciliation",
                            "confirm_misses": n,
                            "observed_at": chrono::Utc::now().to_rfc3339(),
                        }),
                    )
                    .await?;
                    removed += 1;
                }
            }
        }

        let uncertain = self.misses.lock().unwrap().len();
        let stats = json!({"created": created, "updated": updated, "renamed": renamed, "removed": removed, "uncertain": uncertain, "root": root.to_string_lossy()});
        spacetrace::hooks::global_hooks().provider_scan_end(
            &scan_corr,
            &self.provider_id,
            stats.clone(),
            scan_start.elapsed().as_millis() as u64,
        );
        Ok(stats)
    }

    async fn create_file_object_on(&self, path: &Path, props: Value, is_file: bool, pipe: &str, correlation_id: &str) -> Result<()> {
        let mut obj = Object::new(
            if is_file { "File" } else { "Directory" },
            &self.provider_id,
            props,
        );
        obj.external_id = Some(path.to_string_lossy().to_string());
        self.report(
            pipe,
            format!("{}-create-{}", self.provider_id, uuid::Uuid::new_v4()),
            correlation_id,
            vec![json!({"path": path.to_string_lossy(), "operation": "create"})],
            vec![obj.clone()],
            vec![],
            vec![],
            "FileCreated",
            json!({"path": path.to_string_lossy(), "object_id": obj.id}),
        )
        .await
    }

    /// Single funnel for scanner observations: one atomic Core apply per
    /// filesystem fact (create/update/rename/delete). The provider never
    /// touches domain tables; Core validates + applies + records the
    /// event in one transaction. Failures are logged, not fatal: a scan
    /// pass must not die on one bad row.
    async fn report(
        &self,
        pipe: &str,
        observation_id: String,
        correlation_id: &str,
        facts: Vec<Value>,
        objects: Vec<Object>,
        relations: Vec<Relation>,
        delete_ids: Vec<String>,
        event_type: &str,
        payload: Value,
    ) -> Result<()> {
        let mut ev = Event::new(event_type, "filesystem", payload);
        ev.provider_id = Some(self.provider_id.clone());
        // Deleted objects are really affected objects: keep their id on
        // the event so attribution is exact (they are not re-upserted
        // just for it).
        let mut object_ids: Vec<String> = objects.iter().map(|o| o.id.clone()).collect();
        for id in &delete_ids {
            if !object_ids.iter().any(|existing| existing == id) {
                object_ids.push(id.clone());
            }
        }
        ev.object_ids = object_ids;
        // T2 contract: every observation carries the moment the provider
        // observed reality (rename payloads already bring their own). Set
        // in the same atomic report_observation apply; reconciliation
        // readers see the scan observation time, never their read time.
        if ev.payload.get("observed_at").is_none() {
            ev.payload["observed_at"] = json!(chrono::Utc::now().to_rfc3339());
        }
        ev.correlation_id = Some(correlation_id.to_string());
        let obs = space_core::Observation {
            observation_id,
            world_id: String::new(),
            facts,
            objects,
            relations,
            delete_ids,
            event: ev,
        };
        let _ = crate::observation::apply_observation(pipe, &obs).await;
        Ok(())
    }
}

fn walk(root: &Path, files: &mut Vec<PathBuf>, dirs: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if root.is_dir() {
        dirs.push(root.to_path_buf());
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                walk(&path, files, dirs)?;
            } else {
                files.push(path);
            }
        }
    } else if root.exists() {
        files.push(root.to_path_buf());
    }
    Ok(())
}


