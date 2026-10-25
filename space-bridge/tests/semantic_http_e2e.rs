//! Real TCP HTTP + WebSocket against real Core IPC, unique pipe and temporary DB.
#![cfg(windows)]
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use futures_util::StreamExt;
use serde_json::{json, Value};
use space_bridge::{handlers, BridgeState};
use space_core::{
    ipc,
    principal::PrincipalRegistry,
    service_registration::{ServiceRegistrarRegistry, SEMANTIC_PROFILE, WORLD_READ_PROFILE},
    Object, Storage,
};
use std::{sync::Arc, time::Duration};

struct Tasks(Vec<tokio::task::JoinHandle<()>>);
impl Drop for Tasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

async fn request(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    path: &str,
    body: Value,
    origin: Option<&str>,
    expected: u16,
) -> Value {
    let mut req = client
        .request(method.parse().unwrap(), format!("{base}{path}"))
        .json(&body);
    if let Some(origin) = origin {
        req = req.header("Origin", origin);
    }
    let response = req.send().await.unwrap();
    let status = response.status().as_u16();
    let text = response.text().await.unwrap();
    println!("{method} {path} -> {status} {text}");
    assert_eq!(status, expected, "{text}");
    serde_json::from_str(&text).unwrap_or(json!({"text": text}))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn semantic_tcp_and_websocket_vertical_slice() {
    assert!(std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some());
    let dir = tempfile::tempdir().unwrap();
    assert!(dir
        .path()
        .starts_with("C:\\spaceos\\test-tmp"));
    let db = dir.path().join("isolated.db");
    let pipe = format!(r"\\.\pipe\spaceos-semantic-http-{}", uuid::Uuid::new_v4());
    assert_ne!(pipe, ipc::CORE_PIPE);
    // Only this test binary's environment is changed. Never fallback to global IPC.
    std::env::set_var("SPACEOS_BRIDGE_CORE_PIPE", &pipe);
    let world = "integration-world";
    let registrar_token = URL_SAFE_NO_PAD.encode([71u8; 32]);
    let read_registrar_token = URL_SAFE_NO_PAD.encode([72u8; 32]);
    let principals = PrincipalRegistry::default();
    let mut registrars = ServiceRegistrarRegistry::default();
    registrars
        .insert(
            SEMANTIC_PROFILE,
            [71u8; 32],
            &principals,
            &ipc::core_token(),
        )
        .unwrap();
    registrars
        .insert(WORLD_READ_PROFILE, [72u8; 32], &principals, &ipc::core_token())
        .unwrap();
    let core = ipc::CoreState::new_with_semantic_world_and_identities(
        Storage::open(&db).unwrap(),
        Some(world),
        &format!(r"\\.\pipe\spaceos-http-fs-unused-{}", uuid::Uuid::new_v4()),
        &format!(
            r"\\.\pipe\spaceos-http-browser-unused-{}",
            uuid::Uuid::new_v4()
        ),
        principals,
        registrars,
        ipc::core_token(),
    );
    let provider = Object::new(
        "File",
        "filesystem",
        json!({"world_id": world, "name": "provider"}),
    );
    core.storage
        .lock()
        .unwrap()
        .create_object(&provider)
        .unwrap();
    let mut tasks = Tasks(vec![]);
    let c = core.clone();
    let p = pipe.clone();
    tasks.0.push(tokio::spawn(async move {
        ipc::run_server(c, &p).await.unwrap();
    }));
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if ipc::send_request(&pipe, "ping", json!({})).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
    let state = Arc::new(BridgeState::new_with_semantic_world_and_registrars(
        Some(world),
        Some(registrar_token.clone()),
        None,
    ));
    space_bridge::ipc_client::configure_isolated_read_service(
        read_registrar_token,
        world.to_string(),
    )
    .unwrap();
    let router = handlers::create_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tasks.0.push(tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    }));
    let base = format!("http://{addr}");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let origin = Some("http://127.0.0.1:8765");
    let context = request(
        &client,
        &base,
        "GET",
        "/api/context",
        json!({}),
        origin,
        200,
    )
    .await;
    assert_eq!(context["context"]["semantic_world_id"], world);
    assert!(!context.to_string().contains("actor_token"));
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    let init = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(init.to_text().unwrap().contains("init"));
    let create = |name: &str, corr: &str| json!({"world_id": world, "object_type": "Note", "properties": {"name": name}, "correlation_id": corr});
    let a = request(
        &client,
        &base,
        "POST",
        "/api/semantic/objects",
        create("A", "create-a"),
        origin,
        200,
    )
    .await;
    let b = request(
        &client,
        &base,
        "POST",
        "/api/semantic/objects",
        create("B", "create-b"),
        origin,
        200,
    )
    .await;
    let aid = a["result"]["object"]["id"].as_str().unwrap();
    let bid = b["result"]["object"]["id"].as_str().unwrap();
    let relation = json!({"world_id":world,"source_id":aid,"target_id":bid,"relation_type":"related_to","properties":{},"correlation_id":"rel"});
    let r = request(
        &client,
        &base,
        "POST",
        "/api/semantic/relations",
        relation.clone(),
        origin,
        200,
    )
    .await;
    let patch = json!({"object_id":aid,"world_id":world,"properties_patch":{"name":"A updated"},"correlation_id":"patch"});
    request(
        &client,
        &base,
        "PATCH",
        &format!("/api/semantic/objects/{aid}"),
        patch.clone(),
        origin,
        200,
    )
    .await;
    let mut received = vec![];
    tokio::time::timeout(Duration::from_secs(8), async {
        while received.len() < 4 {
            let message = ws.next().await.unwrap().unwrap();
            let value: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
            if value["type"] == "semantic_event" {
                println!("WS {value}");
                received.push(value["event"].clone());
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        received
            .iter()
            .map(|e| e["event_type"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![
            "ObjectCreated",
            "ObjectCreated",
            "RelationCreated",
            "ObjectUpdated"
        ]
    );
    assert_eq!(received[0]["object_ids"][0], aid);
    assert_eq!(
        received[2]["payload"]["relation_id"],
        r["result"]["relation"]["id"]
    );
    // A mutation originating outside Bridge must also reach its WS consumer.
    let reg = ipc::send_request(
        &pipe,
        "register_service_actor",
        json!({
            "profile":SEMANTIC_PROFILE,"registrar_token":registrar_token,
            "name":"space-world-user","worlds":[world],
            "capabilities":space_core::service_registration::SEMANTIC_CAPS,
        }),
    )
    .await
    .unwrap();
    assert_eq!(reg["ok"], true, "{reg}");
    let direct = ipc::send_request(&pipe,"patch_semantic_object",json!({
        "actor":"space-world-user","actor_token":reg["result"]["token"],
        "command":{"object_id":aid,"world_id":world,"properties_patch":{"ipc":true},"correlation_id":"direct-ipc"}
    })).await.unwrap();
    assert_eq!(direct["ok"], true);
    let message = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let delivered: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    assert_eq!(delivered["event"]["correlation_id"], "direct-ipc");
    println!("WS external IPC: {delivered}");
    let before = core.storage.lock().unwrap().max_seq().unwrap();
    request(
        &client,
        &base,
        "POST",
        "/api/semantic/relations",
        relation.clone(),
        origin,
        409,
    )
    .await;
    let mut rollback_patch = patch.clone();
    rollback_patch["properties_patch"] = json!({"name":"must roll back"});
    request(
        &client,
        &base,
        "PATCH",
        &format!("/api/semantic/objects/{aid}"),
        rollback_patch,
        origin,
        409,
    )
    .await;
    for field in ["actor", "actor_token"] {
        let mut bad = create("spoof", field);
        bad[field] = json!("forged");
        request(
            &client,
            &base,
            "POST",
            "/api/semantic/objects",
            bad,
            origin,
            400,
        )
        .await;
    }
    let mut cross = create("cross", "cross");
    cross["world_id"] = json!("other");
    request(
        &client,
        &base,
        "POST",
        "/api/semantic/objects",
        cross,
        origin,
        403,
    )
    .await;
    request(
        &client,
        &base,
        "POST",
        "/api/semantic/objects",
        create("bad", "origin"),
        Some("http://evil.example"),
        403,
    )
    .await;
    request(
        &client,
        &base,
        "POST",
        "/api/semantic/objects",
        create("duplicate", "create-a"),
        origin,
        409,
    )
    .await;
    let mut pp = patch.clone();
    pp["object_id"] = json!(provider.id);
    pp["correlation_id"] = json!("provider-patch");
    request(
        &client,
        &base,
        "PATCH",
        &format!("/api/semantic/objects/{}", provider.id),
        pp,
        origin,
        400,
    )
    .await;
    for kind in [
        "contains",
        "describes",
        "produces",
        "works_on",
        "mission_has_step",
    ] {
        let mut bad = relation.clone();
        bad["relation_type"] = json!(kind);
        bad["correlation_id"] = json!(kind);
        request(
            &client,
            &base,
            "POST",
            "/api/semantic/relations",
            bad,
            origin,
            400,
        )
        .await;
    }
    let mut invalid = create("bad", "invalid");
    invalid["properties"] = json!([]);
    request(
        &client,
        &base,
        "POST",
        "/api/semantic/objects",
        invalid,
        origin,
        400,
    )
    .await;
    let malformed = client
        .post(format!("{base}/api/semantic/objects"))
        .header("Content-Type", "application/json")
        .body("{")
        .send()
        .await
        .unwrap();
    println!("malformed JSON -> {}", malformed.status());
    assert_eq!(malformed.status().as_u16(), 400);
    let after = core
        .storage
        .lock()
        .unwrap()
        .query_events(&space_core::query::EventQuery {
            after_seq: Some(before),
            ..Default::default()
        })
        .unwrap();
    // Direct registration rotated the cached Bridge token: its retry may
    // record MutationDenied, but no rejected operation may record success.
    assert!(after.iter().all(|e| e.event_type == "MutationDenied"));
    assert!(
        tokio::time::timeout(Duration::from_millis(1200), ws.next())
            .await
            .is_err(),
        "duplicate or rollback success event"
    );
    let stored = space_bridge::ipc_client::scoped_read_request("query_objects", json!({}))
        .await
        .unwrap();
    assert_eq!(stored["result"].as_array().unwrap().len(), 3);
    assert_eq!(
        stored["result"]
            .as_array()
            .unwrap()
            .iter()
            .find(|o| o["id"] == aid)
            .unwrap()["properties"]["name"],
        "A updated"
    );
    let reopened = Storage::open(&db).unwrap();
    assert_eq!(
        reopened
            .get_object(&provider.id)
            .unwrap()
            .unwrap()
            .properties,
        provider.properties
    );
    let persisted = reopened
        .query_events(&space_core::query::EventQuery::default())
        .unwrap();
    for delivered in &received {
        assert!(persisted
            .iter()
            .any(|e| e.id == delivered["id"].as_str().unwrap()
                && e.seq == delivered["seq"].as_u64().unwrap()));
    }
    assert_eq!(
        reopened.query_relations(&Default::default()).unwrap().len(),
        1
    );
    assert_eq!(
        reopened.get_object(aid).unwrap().unwrap().properties["world_id"],
        world
    );
    assert!(reopened
        .get_relation(r["result"]["relation"]["id"].as_str().unwrap())
        .unwrap()
        .is_some());
    assert_eq!(
        core.policy.worlds_of("space-world-user"),
        vec![world.to_string()]
    );
    let mut caps=core.policy.capabilities_of("space-world-user");caps.sort();
    assert_eq!(caps,vec!["mission.command","semantic.object.create","semantic.object.patch","semantic.relation.create"]);
    println!(
        "PERSISTENCE: reopened DB, objects/relationship/events verified. Pipe={pipe} HTTP={base}"
    );
    let router = handlers::create_router(Arc::new(BridgeState::new_with_semantic_world(None)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let disabled = format!("http://{}", listener.local_addr().unwrap());
    tasks.0.push(tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    }));
    request(
        &client,
        &disabled,
        "POST",
        "/api/semantic/objects",
        create("disabled", "disabled"),
        origin,
        503,
    )
    .await;
    for (tag, credential) in [
        ("missing", None),
        ("invalid", Some("invalid-registrar".to_string())),
    ] {
        let state = Arc::new(BridgeState::new_with_semantic_world_and_registrars(
            Some(world),
            credential,
            None,
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unavailable = format!("http://{}", listener.local_addr().unwrap());
        tasks.0.push(tokio::spawn(async move {
            axum::serve(listener, handlers::create_router(state))
                .await
                .unwrap();
        }));
        let denied = request(
            &client,
            &unavailable,
            "POST",
            "/api/semantic/objects",
            create(tag, tag),
            origin,
            503,
        )
        .await;
        assert!(!denied.to_string().contains(&registrar_token));
    }
    // Origin is a browser cross-origin guard, not local-client authentication.
    request(
        &client,
        &base,
        "POST",
        "/api/semantic/objects",
        create("native", "native"),
        None,
        200,
    )
    .await;
    println!("SECURITY LIMIT: a native local client without Origin is accepted by design.");
    let absent_pipe = format!(r"\\.\pipe\spaceos-absent-{}", uuid::Uuid::new_v4());
    std::env::set_var("SPACEOS_BRIDGE_CORE_PIPE", &absent_pipe);
    let state = Arc::new(BridgeState::new_with_semantic_world_and_registrars(
        Some(world),
        Some(registrar_token.clone()),
        None,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unavailable = format!("http://{}", listener.local_addr().unwrap());
    tasks.0.push(tokio::spawn(async move {
        axum::serve(listener, handlers::create_router(state))
            .await
            .unwrap();
    }));
    request(
        &client,
        &unavailable,
        "POST",
        "/api/semantic/objects",
        create("offline", "offline"),
        origin,
        503,
    )
    .await;
    std::env::set_var("SPACEOS_BRIDGE_CORE_PIPE", &pipe);
    ws.close(None).await.unwrap();
}
