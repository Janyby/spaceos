//! Scan -> object store -> search chain with a known control file.
//!
//! Uses the REAL Scanner against an in-process Core, then searches with the
//! exact query the Dashboard and MCP use (`provider_id` + `path_contains`),
//! proving the indexed file is discoverable through the shared path.
#![cfg(windows)]

use serde_json::{json, Value};
use space_core::{ipc, Object, Storage};
use std::path::PathBuf;
use std::time::Duration;

#[path = "support/global_lock.rs"]
mod global_lock;

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join("spaceos-scansearch").join(name);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_then_search_finds_control_file() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let dir = tmp("chain");
    let db = dir.join("core.db");
    let world = dir.join("world");
    std::fs::create_dir_all(&world).unwrap();
    let storage = Storage::open(&db).unwrap();
    let state = ipc::CoreState::new(storage);
    let _core_srv = tokio::spawn(async move {
        let _ = ipc::run_server(state, ipc::CORE_PIPE).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Known control file with distinctive content.
    std::fs::write(world.join("control-needle.txt"), "needle-bytes-42").unwrap();
    std::fs::write(world.join("other.txt"), "unrelated").unwrap();

    let scanner = filesystem_provider::scanner::Scanner::new("filesystem");
    let stats = scanner.scan(&world).await.unwrap();
    assert!(
        stats["created"].as_u64().unwrap_or(0) >= 2,
        "both files indexed: {stats}"
    );

    // Same query shape as Dashboard search and MCP filesystem_search.
    let found = ipc::send_request(
        ipc::CORE_PIPE,
        "query_objects",
        json!({"provider_id": "filesystem", "path_contains": "control-needle"}),
    )
    .await
    .unwrap();
    let objs: Vec<Object> = serde_json::from_value(found["result"].clone()).unwrap();
    assert_eq!(objs.len(), 1, "exactly the control file: {objs:?}");
    assert!(
        objs[0]
            .external_id
            .as_deref()
            .map(|p| p.ends_with("control-needle.txt"))
            .unwrap_or(false),
        "external id points at the real file"
    );

    // Negative control: unknown name finds nothing (honest empty, not error).
    let missing: Value = ipc::send_request(
        ipc::CORE_PIPE,
        "query_objects",
        json!({"provider_id": "filesystem", "path_contains": "no-such-file-zzz"}),
    )
    .await
    .unwrap();
    assert_eq!(missing["ok"], true);
    assert_eq!(missing["result"].as_array().unwrap().len(), 0);

    _core_srv.abort();
}
