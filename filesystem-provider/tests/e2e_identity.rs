//! V1.1 Test 1 вЂ” Identity E2E through real scan reconciliation.
use filesystem_provider::scanner::Scanner;
use space_core::{ipc, Object, Storage};
use std::path::PathBuf;

/// Cross-process exclusion for the GLOBAL core pipe (see
/// support/global_lock.rs, mirrored from space-core tests).
#[path = "support/global_lock.rs"]
mod global_lock;

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join("spaceos-v11").join(name);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rename_outside_core_preserves_object_and_relation() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    // Core server with a temp DB; scanner talks to the real CORE_PIPE.
    let dir = tmp("t1");
    let db = dir.join("core.db");
    let world = dir.join("world");
    std::fs::create_dir_all(&world).unwrap();
    let storage = Storage::open(&db).unwrap();
    let state = ipc::CoreState::new(storage);
    // Abort at test end: a detached Core on the GLOBAL pipe would steal
    // other suites' connections (Qualification Gate 0 finding).
    let _core_srv = tokio::spawn(async move {
        let _ = ipc::run_server(state, ipc::CORE_PIPE).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    let f = world.join("a.txt");
    std::fs::write(&f, "hello world").unwrap();

    let scanner = Scanner::new("filesystem");
    let s1 = scanner.scan(&world).await.unwrap();
    assert_eq!(s1["created"].as_u64().unwrap() >= 1, true);

    // find the object for a.txt
    let objs = ipc::send_request(
        ipc::CORE_PIPE,
        "query_objects",
        serde_json::json!({"provider_id":"filesystem"}),
    )
    .await
    .unwrap();
    let objs: Vec<Object> = serde_json::from_value(objs["result"].clone()).unwrap();
    let a = objs
        .iter()
        .find(|o| o.external_id.as_deref() == Some(f.to_string_lossy().as_ref()))
        .expect("a.txt object");
    let project = Object::new("Project", "filesystem", serde_json::json!({"name":"P"}));
    ipc::send_request(ipc::CORE_PIPE, "create_object", serde_json::json!(project))
        .await
        .unwrap();
    ipc::send_request(
        ipc::CORE_PIPE,
        "create_relation",
        serde_json::json!({
            "id": uuid::Uuid::new_v4().to_string(),
            "source_object_id": a.id,
            "relation_type": "belongs_to",
            "target_object_id": project.id,
            "properties": {},
            "created_at": chrono::Utc::now().to_rfc3339(),
            "world_id": "",
            "actor": "filesystem",
        }),
    )
    .await
    .unwrap();

    // rename outside Core
    let g = world.join("b.txt");
    std::fs::rename(&f, &g).unwrap();

    let s2 = scanner.scan(&world).await.unwrap();
    let objs2 = ipc::send_request(
        ipc::CORE_PIPE,
        "query_objects",
        serde_json::json!({"provider_id":"filesystem"}),
    )
    .await
    .unwrap();
    let objs2: Vec<Object> = serde_json::from_value(objs2["result"].clone()).unwrap();
    let same = objs2
        .iter()
        .find(|o| o.external_id.as_deref() == Some(g.to_string_lossy().as_ref()))
        .expect("b.txt object");
    assert_eq!(same.id, a.id, "object id must be preserved across rename");

    let rels = ipc::send_request(
        ipc::CORE_PIPE,
        "query_relations",
        serde_json::json!({"source_object_id": a.id}),
    )
    .await
    .unwrap();
    let rels: Vec<serde_json::Value> = serde_json::from_value(rels["result"].clone()).unwrap();
    assert_eq!(rels.len(), 1, "relation must be preserved");
    assert!(
        s2["renamed"].as_u64().unwrap() >= 1,
        "scanner must detect rename"
    );
    _core_srv.abort();
}
