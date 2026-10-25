//! SpaceTrace Slice 2: Provider scan emits correlated trace linked to Core mutations.
//!
//! Proves Reality -> Provider scan -> Core mutation chain shares one
//! scan correlation, without changing scan semantics.

use filesystem_provider::scanner::Scanner;
use space_core::{ipc, Storage};
use std::path::PathBuf;

/// Cross-process exclusion for the GLOBAL core pipe (see
/// support/global_lock.rs, mirrored from space-core tests).
#[path = "support/global_lock.rs"]
mod global_lock;

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join("spaceos-st-slice2").join(name);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_emits_correlated_trace_linked_to_core_mutations() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let dir = tmp("scan-trace");
    let db = dir.join("core.db");
    let world = dir.join("world");
    std::fs::create_dir_all(&world).unwrap();
    let storage = Storage::open(&db).unwrap();
    let state = ipc::CoreState::new(storage);
    let state2 = state.clone();
    // Abort at test end: a detached Core on the GLOBAL pipe would steal
    // other suites' connections (Qualification Gate 0 finding).
    let core_srv = tokio::spawn(async move {
        let _ = ipc::run_server(state2, ipc::CORE_PIPE).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    std::fs::write(world.join("a.txt"), "hello slice2").unwrap();
    std::fs::write(world.join("b.txt"), "world slice2").unwrap();

    let scanner = Scanner::new("filesystem");
    let stats = scanner.scan(&world).await.unwrap();
    assert!(
        stats["created"].as_u64().unwrap() >= 2,
        "scan must create >=2 objects: {stats}"
    );

    // SpaceTrace instance held by CoreState shares buffer/correlation
    // with global hooks used by the scanner.
    let st = state
        .spacetrace
        .as_ref()
        .expect("CoreState must hold SpaceTrace");

    // Find scan-start events for this provider.
    let scan_starts = st.provider().query_by_provider("filesystem");
    let scan_starts: Vec<_> = scan_starts
        .iter()
        .filter(|e| format!("{:?}", e.kind) == "ScanStart")
        .collect();
    assert!(!scan_starts.is_empty(), "expected ScanStart trace events");

    // Each scan start must have a matching scan end with same correlation.
    for start in &scan_starts {
        let corr = &start.correlation_id;
        assert!(corr.starts_with("scan-"), "scan correlation prefix: {corr}");
        let chain = st.provider().query_by_correlation(corr);
        let has_end = chain.iter().any(|e| format!("{:?}", e.kind) == "ScanEnd");
        assert!(has_end, "scan {corr} must have ScanEnd");
        // Core mutations from this scan carry the same correlation.
        let core_mutations = chain
            .iter()
            .filter(|e| format!("{:?}", e.layer) == "Core")
            .count();
        assert!(
            core_mutations >= 1,
            "scan {corr} must link >=1 Core mutation traces, got {core_mutations}"
        );
    }

    // Semantic state unchanged: objects + FileCreated events exist in Core.
    let objs = ipc::send_request(
        ipc::CORE_PIPE,
        "query_objects",
        serde_json::json!({"provider_id": "filesystem"}),
    )
    .await
    .unwrap();
    let objs: Vec<space_core::Object> = serde_json::from_value(objs["result"].clone()).unwrap();
    assert!(objs.len() >= 2, "semantic objects must exist");

    let evs = ipc::send_request(ipc::CORE_PIPE, "query_events", serde_json::json!({}))
        .await
        .unwrap();
    let evs: Vec<space_core::Event> = serde_json::from_value(evs["result"].clone()).unwrap();
    let created: Vec<_> = evs
        .iter()
        .filter(|e| e.event_type == "FileCreated")
        .collect();
    assert!(
        created.len() >= 2,
        "semantic FileCreated events must exist, got {}",
        created.len()
    );
    core_srv.abort();
}
