#![cfg(windows)]

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};
use space_core::{ipc, policy::Policy, principal::PrincipalRegistry, Object, Storage};
use std::hint::black_box;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::windows::named_pipe::ClientOptions;

const WORLD_A: &str = "stage4-2-world-a";
const WORLD_B: &str = "stage4-2-world-b";

fn privileged_fixture_token() -> String {
    URL_SAFE_NO_PAD.encode([41u8; 32])
}

fn temp_db(tag: &str) -> std::path::PathBuf {
    let directory = std::env::temp_dir().join("spaceos-stage4-2-identity");
    std::fs::create_dir_all(&directory).unwrap();
    directory.join(format!("{tag}-{}.db", uuid::Uuid::new_v4()))
}

fn object(id: &str, world: &str) -> Object {
    let mut object = Object::new("Note", "space-world-user", json!({"world_id": world}));
    object.id = id.to_string();
    object
}

async fn start_core(
    pipe: &str,
    db: &std::path::Path,
) -> (tokio::task::JoinHandle<()>, std::sync::Arc<ipc::CoreState>) {
    let storage = Storage::open(db).unwrap();
    if storage.get_object("center").unwrap().is_none() {
        storage.create_object(&object("center", WORLD_A)).unwrap();
    }
    let mut principals = PrincipalRegistry::default();
    principals
        .insert(
            "system",
            [41u8; 32],
            ["agent.register", "agent.grant.semantic.object.create", "agent.grant.semantic.read"],
            [WORLD_A, WORLD_B],
        )
        .unwrap();
    let state = ipc::CoreState::new_with_semantic_world_and_provider_pipes_and_principals(
        storage,
        Some(WORLD_A),
        ipc::PROVIDER_PIPE,
        ipc::BROWSER_PROVIDER_PIPE,
        principals,
    );
    let server_state = state.clone();
    let server_pipe = pipe.to_string();
    let server = tokio::spawn(async move {
        let _ = ipc::run_server(server_state, &server_pipe).await;
    });
    tokio::time::sleep(Duration::from_millis(250)).await;
    (server, state)
}

async fn register(pipe: &str, name: &str, caps: &[&str], worlds: &[&str]) -> String {
    let response = ipc::send_request(
        pipe,
        "register_agent",
        json!({
            "principal": "system",
            "principal_token": privileged_fixture_token(),
            "name": name,
            "capabilities": caps,
            "worlds": worlds,
        }),
    )
    .await
    .unwrap();
    assert_eq!(response["ok"], true, "{response}");
    response["result"]["token"].as_str().unwrap().to_string()
}

fn context_params(actor: &str, token: &str, world: &str) -> Value {
    json!({
        "actor": actor,
        "actor_token": token,
        "query": {"object_id": "center", "world_id": world, "limit": 10}
    })
}

async fn raw_line(pipe: &str, mut request: String) -> Value {
    let mut backoff = 5;
    let stream = loop {
        match ClientOptions::new().open(pipe) {
            Ok(stream) => break stream,
            Err(_) if backoff <= 320 => {
                tokio::time::sleep(Duration::from_millis(backoff)).await;
                backoff *= 2;
            }
            Err(error) => panic!("open isolated pipe: {error}"),
        }
    };
    let (reader, mut writer) = tokio::io::split(stream);
    request.push('\n');
    writer.write_all(request.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();
    let mut lines = BufReader::new(reader).lines();
    serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap()
}

async fn raw_request(pipe: &str, method: &str, params: Value) -> Value {
    raw_line(
        pipe,
        serde_json::to_string(&json!({"method": method, "params": params})).unwrap(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn token_binding_replay_rotation_and_restart_lifecycle() {
    let pipe = format!(r"\\.\pipe\spaceos-stage4-2-life-{}", uuid::Uuid::new_v4());
    let db = temp_db("lifecycle");
    let (server, _) = start_core(&pipe, &db).await;
    let token_a = register(&pipe, "agent-a", &["semantic.read"], &[WORLD_A]).await;
    let token_b = register(&pipe, "agent-b", &["semantic.read"], &[WORLD_A]).await;

    for token in ["", "incorrect"] {
        let denied = raw_request(
            &pipe,
            "get_object_context",
            context_params("agent-a", token, WORLD_A),
        )
        .await;
        assert_eq!(denied["ok"], false, "{denied}");
    }

    let wrong_actor = raw_request(
        &pipe,
        "get_object_context",
        context_params("agent-b", &token_a, WORLD_A),
    )
    .await;
    assert_eq!(wrong_actor["ok"], false, "{wrong_actor}");

    let wrong_world = raw_request(
        &pipe,
        "get_object_context",
        context_params("agent-a", &token_a, WORLD_B),
    )
    .await;
    assert_eq!(wrong_world["ok"], false, "{wrong_world}");

    // Bearer replay is currently supported: the same live token can be used
    // repeatedly until rotation or Core restart.
    for _ in 0..2 {
        let replay = raw_request(
            &pipe,
            "get_object_context",
            context_params("agent-a", &token_a, WORLD_A),
        )
        .await;
        assert_eq!(replay["ok"], true, "{replay}");
    }

    // A token cannot be reused as a different actor, even when that actor is
    // registered and has the same world grants.
    let token_reuse_as_a = raw_request(
        &pipe,
        "get_object_context",
        context_params("agent-a", &token_b, WORLD_A),
    )
    .await;
    assert_eq!(token_reuse_as_a["ok"], false, "{token_reuse_as_a}");

    // Re-registration is the only currently available revocation surrogate:
    // last writer wins and the old token immediately stops authenticating.
    let rotated = register(&pipe, "agent-a", &["semantic.read"], &[WORLD_A]).await;
    assert_ne!(token_a, rotated);
    let old_after_rotation = raw_request(
        &pipe,
        "get_object_context",
        context_params("agent-a", &token_a, WORLD_A),
    )
    .await;
    assert_eq!(old_after_rotation["ok"], false, "{old_after_rotation}");
    let new_after_rotation = raw_request(
        &pipe,
        "get_object_context",
        context_params("agent-a", &rotated, WORLD_A),
    )
    .await;
    assert_eq!(new_after_rotation["ok"], true, "{new_after_rotation}");

    server.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (restarted, _) = start_core(&pipe, &db).await;
    let old_after_restart = raw_request(
        &pipe,
        "get_object_context",
        context_params("agent-a", &rotated, WORLD_A),
    )
    .await;
    assert_eq!(old_after_restart["ok"], false, "{old_after_restart}");
    restarted.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn model_a_transport_combinations_and_semantic_write_gate() {
    let pipe = format!(r"\\.\pipe\spaceos-stage4-2-model-{}", uuid::Uuid::new_v4());
    let db = temp_db("model-a");
    let (server, state) = start_core(&pipe, &db).await;
    let reader = register(&pipe, "reader", &["semantic.read"], &[WORLD_A]).await;

    let missing_transport = raw_request(
        &pipe,
        "get_object_context",
        context_params("reader", &reader, WORLD_A),
    )
    .await;
    assert_eq!(missing_transport["ok"], true, "{missing_transport}");

    let mut invalid_transport_params = context_params("reader", &reader, WORLD_A);
    invalid_transport_params["auth"] = json!("invalid-core-transport-token");
    let invalid_transport =
        raw_request(&pipe, "get_object_context", invalid_transport_params).await;
    assert_eq!(invalid_transport["ok"], true, "{invalid_transport}");

    let mut bad_actor_token = context_params("reader", "invalid-actor-token", WORLD_A);
    bad_actor_token["auth"] = json!(state.core_token.clone());
    let valid_transport_bad_actor = raw_request(&pipe, "get_object_context", bad_actor_token).await;
    assert_eq!(
        valid_transport_bad_actor["ok"], false,
        "{valid_transport_bad_actor}"
    );

    for auth in [None, Some("invalid-core-transport-token")] {
        let mut core_params = context_params("cli", "", WORLD_A);
        if let Some(auth) = auth {
            core_params["auth"] = json!(auth);
        }
        let denied = raw_request(&pipe, "get_object_context", core_params).await;
        assert_eq!(denied["ok"], false, "{denied}");
    }

    let semantic_token = register(
        &pipe,
        "space-world-user",
        &["semantic.object.create"],
        &[WORLD_A],
    )
    .await;
    let command = json!({
        "world_id": WORLD_A,
        "object_type": "Note",
        "properties": {"name": "Stage 4.2"},
        "correlation_id": format!("stage4-2-{}", uuid::Uuid::new_v4()),
    });
    let denied_write = raw_request(
        &pipe,
        "create_semantic_object",
        json!({
            "actor": "space-world-user",
            "actor_token": "invalid-actor-token",
            "auth": state.core_token,
            "command": command,
        }),
    )
    .await;
    assert_eq!(denied_write["ok"], false, "{denied_write}");

    let allowed_write = raw_request(
        &pipe,
        "create_semantic_object",
        json!({
            "actor": "space-world-user",
            "actor_token": semantic_token,
            "auth": "invalid-but-ignored-for-agent",
            "command": {
                "world_id": WORLD_A,
                "object_type": "Note",
                "properties": {"name": "Stage 4.2 allowed"},
                "correlation_id": format!("stage4-2-{}", uuid::Uuid::new_v4()),
            },
        }),
    )
    .await;
    assert_eq!(allowed_write["ok"], true, "{allowed_write}");

    let malformed = raw_line(&pipe, "{malformed-json".to_string()).await;
    assert_eq!(malformed["ok"], false, "{malformed}");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscription_authentication_and_world_scope_matrix() {
    let pipe = format!(r"\\.\pipe\spaceos-stage4-2-sub-{}", uuid::Uuid::new_v4());
    let db = temp_db("subscription");
    let (server, _) = start_core(&pipe, &db).await;
    let token_a = register(&pipe, "subscriber-a", &["semantic.read"], &[WORLD_A]).await;
    let token_b = register(&pipe, "subscriber-b", &["semantic.read"], &[WORLD_B]).await;
    let no_read = register(&pipe, "subscriber-no-read", &[], &[WORLD_A]).await;

    for params in [
        json!({"subscriber": "subscriber-a", "world_id": WORLD_A}),
        json!({"subscriber": "subscriber-a", "world_id": WORLD_A, "actor_token": "bad"}),
        json!({"subscriber": "subscriber-b", "world_id": WORLD_B, "actor_token": token_a}),
        json!({"subscriber": "subscriber-a", "world_id": WORLD_B, "actor_token": token_a}),
        json!({"subscriber": "unknown", "world_id": WORLD_A, "actor_token": token_b}),
        json!({"subscriber": "subscriber-no-read", "world_id": WORLD_A, "actor_token": no_read}),
        json!({"subscriber": "subscriber-a", "actor_token": token_a}),
    ] {
        let denied = raw_request(&pipe, "subscribe", params).await;
        assert_eq!(denied["ok"], false, "{denied}");
    }

    let allowed = raw_request(
        &pipe,
        "subscribe",
        json!({
            "subscriber": "subscriber-a",
            "world_id": WORLD_A,
            "actor_token": token_a,
            "auth": "invalid-but-ignored-for-agent",
        }),
    )
    .await;
    assert_eq!(allowed["ok"], true, "{allowed}");
    server.abort();
}

fn summarize(samples: &mut [Duration]) -> (u128, u128, u128) {
    samples.sort_unstable();
    let median = samples[samples.len() / 2].as_nanos();
    let p95 = samples[(samples.len() * 95 / 100).min(samples.len() - 1)].as_nanos();
    let max = samples.last().unwrap().as_nanos();
    (median, p95, max)
}

#[test]
fn authentication_cost_model_a_and_synthetic_model_b() {
    const BATCHES: usize = 20;
    const ITERATIONS: usize = 50_000;
    let policy = Policy::new();
    let actor_token = policy
        .register("perf-agent", "system", vec![], vec![WORLD_A.to_string()])
        .unwrap();
    let core_token = "isolated-core-token";
    let mut model_a = Vec::with_capacity(BATCHES);
    let mut model_b = Vec::with_capacity(BATCHES);

    for _ in 0..BATCHES {
        let started = Instant::now();
        for _ in 0..ITERATIONS {
            assert!(black_box(policy.authenticate(
                black_box("perf-agent"),
                Some(black_box(actor_token.as_str()))
            )));
        }
        model_a.push(started.elapsed() / ITERATIONS as u32);

        let started = Instant::now();
        for _ in 0..ITERATIONS {
            let transport_ok = black_box(core_token) == black_box("isolated-core-token");
            assert!(
                black_box(transport_ok)
                    && black_box(policy.authenticate(
                        black_box("perf-agent"),
                        Some(black_box(actor_token.as_str()))
                    ))
            );
        }
        model_b.push(started.elapsed() / ITERATIONS as u32);
    }

    let (a_median, a_p95, a_max) = summarize(&mut model_a);
    let (b_median, b_p95, b_max) = summarize(&mut model_b);
    println!(
        "stage4.2 auth microbenchmark profile=debug batches={BATCHES} iterations_per_batch={ITERATIONS} model_a_ns median={a_median} p95={a_p95} max={a_max} synthetic_model_b_ns median={b_median} p95={b_p95} max={b_max}"
    );

    let mut unique_registration = Vec::with_capacity(BATCHES);
    let mut rotation = Vec::with_capacity(BATCHES);
    for batch in 0..BATCHES {
        let policy = Policy::new();
        let started = Instant::now();
        for index in 0..1_000 {
            assert!(policy
                .register(
                    &format!("agent-{batch}-{index}"),
                    "system",
                    vec!["object.create".to_string()],
                    vec![WORLD_A.to_string()],
                )
                .is_some());
        }
        unique_registration.push(started.elapsed() / 1_000);

        let policy = Policy::new();
        let started = Instant::now();
        for _ in 0..1_000 {
            assert!(policy
                .register(
                    "rotating-agent",
                    "system",
                    vec!["object.create".to_string()],
                    vec![WORLD_A.to_string()],
                )
                .is_some());
        }
        rotation.push(started.elapsed() / 1_000);
    }
    let (reg_median, reg_p95, reg_max) = summarize(&mut unique_registration);
    let (rot_median, rot_p95, rot_max) = summarize(&mut rotation);
    println!(
        "stage4.2 registration microbenchmark profile=debug batches={BATCHES} registrations_per_batch=1000 unique_ns median={reg_median} p95={reg_p95} max={reg_max} rotation_ns median={rot_median} p95={rot_p95} max={rot_max}"
    );
}
