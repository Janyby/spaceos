//! Independent lab Core and Bridge processes, real named pipe and TCP.
#![cfg(windows)]

use futures_util::StreamExt;
use serde_json::{json, Value};
use space_core::ipc;
use std::os::windows::process::CommandExt;
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};

struct Fixture(Child);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_fixture(binary: &str, vars: &[(&str, &str)]) -> Fixture {
    let root = r"C:\spaceos";
    let mut command = Command::new(binary);
    command
        .env_clear()
        .env("SystemRoot", std::env::var("SystemRoot").unwrap())
        .env("SPACEOS_TEST_ISOLATION_ROOT", root)
        .env(
            "SPACEOS_CORE_TOKEN",
            std::env::var("SPACEOS_CORE_TOKEN").unwrap(),
        )
        .env("TEMP", format!(r"{root}\test-tmp"))
        .env("TMP", format!(r"{root}\test-tmp"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .creation_flags(0x0800_0000);
    for (key, value) in vars {
        command.env(key, value);
    }
    Fixture(command.spawn().unwrap())
}

async fn post(client: &reqwest::Client, base: &str, path: &str, body: Value) -> Value {
    let response = client
        .post(format!("{base}{path}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "{path}: {}",
        response.status()
    );
    response.json().await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_processes_register_and_mutate_without_system() {
    assert_eq!(
        std::env::var("SPACEOS_TEST_ISOLATION_ROOT").unwrap(),
        r"C:\spaceos"
    );
    let root = r"C:\spaceos";
    let suffix = uuid::Uuid::new_v4();
    let pipe = format!(r"\\.\pipe\spaceos-4-3a4-process-{suffix}");
    let db = format!(r"{root}\test-tmp\process-{suffix}.db");
    let world = "synthetic-process-world";
    let core = spawn_fixture(
        env!("CARGO_BIN_EXE_isolated-core-fixture"),
        &[
            ("SPACEOS_FIXTURE_CORE_PIPE", &pipe),
            ("SPACEOS_FIXTURE_DB", &db),
            ("SPACEOS_FIXTURE_WORLD", world),
        ],
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if ipc::send_request(&pipe, "ping", json!({})).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    assert_ne!(port, 8765);
    let port_text = port.to_string();
    let bridge = spawn_fixture(
        env!("CARGO_BIN_EXE_isolated-bridge-fixture"),
        &[
            ("SPACEOS_BRIDGE_CORE_PIPE", &pipe),
            ("SPACEOS_FIXTURE_HTTP_PORT", &port_text),
            ("SPACEOS_FIXTURE_WORLD", world),
        ],
    );
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(response) = client.get(format!("{base}/api/context")).send().await {
                if response.status().is_success() {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws"))
        .await
        .unwrap();
    let _ = ws.next().await.unwrap().unwrap();
    let a = post(&client, &base, "/api/semantic/objects", json!({
        "world_id":world,"object_type":"Note","properties":{"name":"A"},"correlation_id":"process-a"
    })).await;
    let b = post(&client, &base, "/api/semantic/objects", json!({
        "world_id":world,"object_type":"Note","properties":{"name":"B"},"correlation_id":"process-b"
    })).await;
    let aid = a["result"]["object"]["id"].as_str().unwrap();
    let bid = b["result"]["object"]["id"].as_str().unwrap();
    post(
        &client,
        &base,
        "/api/semantic/relations",
        json!({
            "world_id":world,"source_id":aid,"target_id":bid,"relation_type":"related_to",
            "properties":{},"correlation_id":"process-relation"
        }),
    )
    .await;
    let response = client
        .patch(format!("{base}/api/semantic/objects/{aid}"))
        .json(&json!({
            "object_id":aid,"world_id":world,"properties_patch":{"name":"A patched"},
            "correlation_id":"process-patch"
        }))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let mut events = Vec::new();
    tokio::time::timeout(Duration::from_secs(8), async {
        while events.len() < 4 {
            let message = ws.next().await.unwrap().unwrap();
            let value: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
            if value["type"] == "semantic_event" {
                events.push(value["event"].clone());
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        events
            .iter()
            .map(|e| e["event_type"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "ObjectCreated",
            "ObjectCreated",
            "RelationCreated",
            "ObjectUpdated"
        ]
    );
    // World reads use Bridge's dedicated scoped read actor, never an
    // anonymous direct Core request from this integration test.
    let objects: Value = client.post(format!("{base}/api/objects/query"))
        .json(&json!({"query": {"world_id": world}}))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(objects["objects"].as_array().unwrap().len(), 2);
    let relations: Value = client.post(format!("{base}/api/relations/query"))
        .json(&json!({"query": {"world_id": world}}))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(relations["relations"].as_array().unwrap().len(), 1);
    let events_read: Value = client.get(format!("{base}/api/events?world_id={world}"))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(events_read["events"].as_array().unwrap().len(), 4);
    let forged = ipc::send_request(
        &pipe,
        "register_agent",
        json!({
            "principal":"system","name":"forged","capabilities":[],"worlds":[]
        }),
    )
    .await
    .unwrap();
    assert_eq!(forged["ok"], false);
    let reopened = space_core::Storage::open(&db).unwrap();
    assert!(reopened.get_object(aid).unwrap().is_some());
    assert!(reopened.get_object(bid).unwrap().is_some());
    drop(reopened);

    // Core restart invalidates the in-memory actor token. The still-running
    // Bridge must re-register once and complete the next semantic operation.
    drop(core);
    let _restarted_core = spawn_fixture(
        env!("CARGO_BIN_EXE_isolated-core-fixture"),
        &[
            ("SPACEOS_FIXTURE_CORE_PIPE", &pipe),
            ("SPACEOS_FIXTURE_DB", &db),
            ("SPACEOS_FIXTURE_WORLD", world),
        ],
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if ipc::send_request(&pipe, "ping", json!({})).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let after_core_restart = post(
        &client,
        &base,
        "/api/semantic/objects",
        json!({
            "world_id":world,"object_type":"Note","properties":{"name":"after Core restart"},
            "correlation_id":"process-core-restart"
        }),
    )
    .await;
    assert_eq!(
        after_core_restart["result"]["event"]["actor"],
        "space-world-user"
    );

    // A new Bridge process establishes its own registrar session without
    // ever receiving a system principal credential.
    ws.close(None).await.unwrap();
    drop(bridge);
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let restart_port = socket.local_addr().unwrap().port();
    drop(socket);
    let restart_port_text = restart_port.to_string();
    let _restarted_bridge = spawn_fixture(
        env!("CARGO_BIN_EXE_isolated-bridge-fixture"),
        &[
            ("SPACEOS_BRIDGE_CORE_PIPE", &pipe),
            ("SPACEOS_FIXTURE_HTTP_PORT", &restart_port_text),
            ("SPACEOS_FIXTURE_WORLD", world),
        ],
    );
    let restarted_base = format!("http://127.0.0.1:{restart_port}");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(response) = client
                .get(format!("{restarted_base}/api/context"))
                .send()
                .await
            {
                if response.status().is_success() {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    post(
        &client,
        &restarted_base,
        "/api/semantic/objects",
        json!({
            "world_id":world,"object_type":"Note","properties":{"name":"after Bridge restart"},
            "correlation_id":"process-bridge-restart"
        }),
    )
    .await;
    let reopened = space_core::Storage::open(&db).unwrap();
    assert_eq!(
        reopened.query_objects(&Default::default()).unwrap().len(),
        4
    );
}
