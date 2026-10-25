use crate::{format, identity, metadata};
use crate::scanner::Scanner;
use anyhow::Result;
use serde_json::{json, Value};
use space_core::{Event, Object, ipc};
use std::path::Path;

/// Filesystem watcher: translates notify events into Core observations.
/// Shares the given Scanner instance: each event handling holds the same
/// mutual-exclusion guard as full scan passes (Breakage B4), so live
/// decisions never interleave with a scan snapshot mid-pass.
pub async fn watch(root: &Path, scanner: std::sync::Arc<Scanner>, stop: tokio::sync::oneshot::Receiver<()>) -> Result<()> {
    let pipe = ipc::configured_provider_pipe("SPACEOS_CORE_PIPE", ipc::CORE_PIPE)?;
    watch_on(root, scanner, &pipe, stop).await
}

/// Same as [`watch`], but against an explicit Core endpoint (tests).
pub async fn watch_on(
    root: &Path,
    scanner: std::sync::Arc<Scanner>,
    pipe: &str,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) -> Result<()> {
    use notify::{RecommendedWatcher, RecursiveMode, Watcher, EventKind};
    let (tx, mut rx) = tokio::sync::mpsc::channel(512);
    let handle = tokio::runtime::Handle::current();
    let mut watcher: RecommendedWatcher = notify::recommended_watcher(move |res| {
        let tx = tx.clone();
        let _ = handle.spawn(async move {
            let _ = tx.send(res).await;
        });
    })?;
    watcher.watch(root, RecursiveMode::Recursive)?;
    tracing::info!("watching {}", root.display());
    let provider_id = scanner.provider_id.clone();

    loop {
        tokio::select! {
            _ = &mut stop => return Ok(()),
            ev = rx.recv() => {
                let Some(Ok(ev)) = ev else { continue };
                let kind = match ev.kind {
                    EventKind::Create(_) => "FileCreated",
                    EventKind::Modify(_) => "FileChanged",
                    EventKind::Remove(_) => "FileDeleted",
                    _ => continue,
                };
                // Same guard as scan passes: decide-then-act atomically
                // against concurrent scans on this instance.
                let _guard = scanner.lock.lock().await;
                for path in ev.paths {
                    if let Err(e) = handle_fs_event(root, &path, kind, &provider_id, pipe).await {
                        tracing::warn!("watch handling error: {e}");
                    }
                }
            }
        }
    }
}

async fn handle_fs_event(
    root: &Path,
    path: &Path,
    kind: &str,
    provider_id: &str,
    pipe: &str,
) -> Result<()> {
    let path_str = path.to_string_lossy().to_string();
    // SpaceTrace Slice 2: one correlation per watcher event links
    // reality -> watcher -> Core mutations (I26 provider-neutral).
    let watch_corr = format!("watch-{}", uuid::Uuid::new_v4());
    spacetrace::hooks::global_hooks().reality_change(
        &watch_corr,
        provider_id,
        match kind {
            "FileCreated" => "create",
            "FileChanged" => "update",
            "FileDeleted" => "delete",
            _ => "unknown",
        },
        &path_str,
    );
    spacetrace::hooks::global_hooks().provider_watcher_event(
        &watch_corr,
        provider_id,
        kind,
        &path_str,
        identity::fingerprint(path).as_deref(),
    );
    let existing = crate::reconciliation_read::objects(pipe, root, Some(&path_str)).await?.into_iter().next();

    match kind {
        "FileCreated" => {
            if existing.is_none() {
                // Rename-aware birth (Breakage B4): a Create for an unknown
                // path may be the arrival side of a rename whose Remove was
                // lost, reordered, or not yet processed. A blind create
                // here mints a duplicate row that later starves the
                // original into a confirmed delete (+cascade). So check
                // missing-path rows with the same fingerprint first.
                if adopt_rename(root, &path_str, provider_id, pipe, &watch_corr).await? {
                    return Ok(());
                }
                // No birth without evidence either: an unreadable file
                // mints nothing (adopt_rename already returned false).
                if identity::fingerprint(path).is_none()
                    && metadata::stat(path)["file_type"] != "Directory"
                {
                    return Ok(());
                }
                let is_dir = metadata::stat(path)["file_type"] == "Directory";
                let fmt = if is_dir { None } else { Some(format::detect(path)) };
                let fp = if is_dir { None } else { identity::fingerprint(path) };
                let mut props = json!({"path": path_str, "meta": metadata::stat(path)});
                if let Some(f) = fp { props["fingerprint"] = json!(f); }
                if let Some(f) = fmt {
                    props["format"] = json!(f.format);
                    props["semantic_type"] = json!(f.semantic_type);
                    props["confidence"] = json!(f.confidence);
                    props["detected_by"] = json!(f.detected_by);
                }
                let mut obj = Object::new(if is_dir { "Directory" } else { "File" }, provider_id, props);
                obj.external_id = Some(path_str.clone());
                report(
                    pipe,
                    provider_id,
                    format!("{}-create-{}", watch_corr, uuid::Uuid::new_v4()),
                    &watch_corr,
                    vec![json!({"path": path_str, "operation": "create"})],
                    vec![obj.clone()],
                    vec![],
                    "FileCreated",
                    json!({"path": path_str, "object_id": obj.id}),
                )
                .await?;
            }
        }
        "FileChanged" => {
            if let Some(mut obj) = existing {
                obj.properties["meta"] = metadata::stat(path);
                report(
                    pipe,
                    provider_id,
                    format!("{}-change-{}", watch_corr, uuid::Uuid::new_v4()),
                    &watch_corr,
                    vec![json!({"path": path_str, "operation": "change"})],
                    vec![obj.clone()],
                    vec![],
                    "FileChanged",
                    json!({"path": path_str, "object_id": obj.id}),
                )
                .await?;
            }
        }
        "FileDeleted" => {
            if let Some(obj) = existing {
                // Rename-aware death (Breakage B4): before hard-deleting,
                // check whether a same-fingerprint file exists elsewhere on
                // disk without a row (the departure side of a rename whose
                // Create already landed or is imminent). Deleting blindly
                // here splits identity and cascades relations that a later
                // scan can never repair.
                if let Some(new_path) = find_rename_target(root, &obj, provider_id, pipe).await? {
                    let old = obj.external_id.clone().unwrap_or_default();
                    let mut o = obj.clone();
                    o.external_id = Some(new_path.clone());
                    o.properties["path"] = json!(new_path);
                    o.properties["meta"] = metadata::stat(Path::new(&new_path));
                    o.properties["identity_source"] = json!("fingerprint-live");
                    report(
                        pipe,
                        provider_id,
                        format!("{}-move-{}", watch_corr, uuid::Uuid::new_v4()),
                        &watch_corr,
                        vec![json!({"from": old, "to": new_path, "operation": "move"})],
                        vec![o],
                        vec![],
                        "FileMoved",
                        json!({"from": old, "to": new_path, "observed_at": chrono::Utc::now().to_rfc3339()}),
                    )
                    .await?;
                    return Ok(());
                }
                // Confirmed death: fact + projection die together in Core.
                report(
                    pipe,
                    provider_id,
                    format!("{}-delete-{}", watch_corr, uuid::Uuid::new_v4()),
                    &watch_corr,
                    vec![json!({"path": path_str, "operation": "delete"})],
                    vec![],
                    vec![obj.id.clone()],
                    "FileDeleted",
                    json!({"path": path_str, "object_id": obj.id}),
                )
                .await?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Rename-aware birth: is this unknown path the arrival side of a rename?
/// Looks for rows whose path is currently missing on disk with the same
/// content fingerprint. On match, moves the row (same id, FileMoved) and
/// returns true. Same continuity bias as the scanner orphan rule.
async fn adopt_rename(root: &Path, new_path: &str, provider_id: &str, pipe: &str, correlation_id: &str) -> Result<bool> {
    let new_fp = match identity::fingerprint(Path::new(new_path)) {
        Some(f) => f,
        None => return Ok(false),
    };
    let existing = crate::reconciliation_read::objects(pipe, root, None).await?;
    for obj in existing {
        let (Some(ext), Some(stored_fp)) = (
            obj.external_id.clone(),
            obj.properties.get("fingerprint").and_then(|v| v.as_str()).map(|s| s.to_string()),
        ) else {
            continue;
        };
        if ext == new_path || stored_fp != new_fp {
            continue;
        }
        if Path::new(&ext).exists() {
            continue; // still observable; not a rename source
        }
        // Adopt: same identity, new path.
        let mut o = obj.clone();
        let old = obj.external_id.clone();
        o.external_id = Some(new_path.to_string());
        o.properties["path"] = json!(new_path);
        o.properties["meta"] = metadata::stat(Path::new(new_path));
        o.properties["fingerprint"] = json!(new_fp);
        o.properties["identity_source"] = json!("fingerprint-live");
        ipc::send_provider_principal_request(pipe, "filesystem", "update_object", {
            let mut v = json!(o);
            v["correlation_id"] = json!(correlation_id);
            v
        }).await?;
        let mut ev = Event::new(
            "FileMoved",
            "filesystem",
            json!({"from": old, "to": new_path, "observed_at": chrono::Utc::now().to_rfc3339()}),
        );
        ev.provider_id = Some(provider_id.to_string());
        ev.object_ids = vec![o.id.clone()];
        ev.correlation_id = Some(correlation_id.to_string());
        ipc::send_provider_principal_request(pipe, "filesystem", "record_event", json!(ev)).await?;
        return Ok(true);
    }
    Ok(false)
}

/// Rename-aware death: is there a same-fingerprint file elsewhere on disk
/// with no row claiming it? Walks `root` (size-prefiltered, then hashed).
/// Returns the new path when the Remove is really a departure.
async fn find_rename_target(
    root: &Path,
    obj: &Object,
    provider_id: &str,
    pipe: &str,
) -> Result<Option<String>> {
    let stored_fp = match obj.properties.get("fingerprint").and_then(|v| v.as_str()) {
        Some(f) => f.to_string(),
        None => return Ok(None),
    };
    let stored_size: u64 = stored_fp.rsplit(':').next().and_then(|s| s.parse().ok()).unwrap_or(u64::MAX);
    let existing = crate::reconciliation_read::objects(pipe, root, None).await?;
    let claimed: std::collections::HashSet<String> =
        existing.iter().filter_map(|o| o.external_id.clone()).collect();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            let ps = p.to_string_lossy().to_string();
            if Some(&ps) == obj.external_id.as_ref() || claimed.contains(&ps) {
                continue;
            }
            let Ok(md) = std::fs::metadata(&p) else { continue };
            if md.len() != stored_size {
                continue;
            }
            if identity::fingerprint(&p).as_deref() == Some(stored_fp.as_str()) {
                return Ok(Some(ps));
            }
        }
    }
    Ok(None)
}

/// Single funnel for watcher observations: one atomic Core apply per
/// filesystem fact (create/update/rename/delete). The provider never
/// touches domain tables; Core validates + applies + records the event
/// in one transaction. Rejections propagate to the watcher loop for logging.
async fn report(
    pipe: &str,
    provider_id: &str,
    observation_id: String,
    correlation_id: &str,
    facts: Vec<Value>,
    objects: Vec<Object>,
    delete_ids: Vec<String>,
    event_type: &str,
    payload: Value,
) -> Result<()> {
    let mut ev = Event::new(event_type, "filesystem", payload);
    ev.provider_id = Some(provider_id.to_string());
    // Deleted objects are really affected objects: keep their id on the
    // event so attribution is exact (they are not re-upserted just for it).
    let mut object_ids: Vec<String> = objects.iter().map(|o| o.id.clone()).collect();
    for id in &delete_ids {
        if !object_ids.iter().any(|existing| existing == id) {
            object_ids.push(id.clone());
        }
    }
    ev.object_ids = object_ids;
    // T2 contract: every observation carries the moment the provider
    // observed reality (rename payloads already bring their own). Set in
    // the same atomic report_observation apply: history readers see this
    // observation time, never their read time.
    if ev.payload.get("observed_at").is_none() {
        ev.payload["observed_at"] = json!(chrono::Utc::now().to_rfc3339());
    }
    ev.correlation_id = Some(correlation_id.to_string());
    let obs = space_core::Observation {
        observation_id,
        world_id: String::new(),
        facts,
        objects,
        relations: vec![],
        delete_ids,
        event: ev,
    };
    crate::observation::apply_observation(pipe, &obs).await?;
    Ok(())
}
