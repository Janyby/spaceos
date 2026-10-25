//! Isolated real-Core-IPC adversarial principal checks.
#![cfg(windows)]

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};
use space_core::{ipc, principal::PrincipalRegistry, ActionRequest, Storage};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::windows::named_pipe::ClientOptions;

async fn raw(pipe: &str, line: &str) -> Value {
    let mut delay = 5;
    let stream = loop {
        match ClientOptions::new().open(pipe) {
            Ok(s) => break s,
            Err(_) if delay <= 320 => {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                delay *= 2;
            }
            Err(e) => panic!("isolated Core pipe unavailable: {e}"),
        }
    };
    let (reader, mut writer) = tokio::io::split(stream);
    writer.write_all(line.as_bytes()).await.unwrap();
    writer.write_all(b"\n").await.unwrap();
    writer.flush().await.unwrap();
    let mut lines = BufReader::new(reader).lines();
    serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn privileged_identity_is_not_transport_identity() {
    assert!(std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some());
    assert!(std::env::var_os("SPACEOS_CORE_TOKEN").is_some());
    let suffix = uuid::Uuid::new_v4();
    let dir = std::env::temp_dir().join(format!("spaceos-4-3a2-{suffix}"));
    assert!(dir.starts_with("C:\\spaceos\\test-tmp"));
    std::fs::create_dir_all(&dir).unwrap();
    let pipe = format!(r"\\.\pipe\spaceos-4-3a2-core-{suffix}");
    let mut principals = PrincipalRegistry::default();
    principals
        .insert(
            "system",
            [21u8; 32],
            [
                "agent.register",
                "agent.grant.action.read",
                "agent.grant.semantic.read",
                "semantic.read",
                "action.create",
                "dispatch.filesystem.create",
                "provider.register",
                "subscription.subscribe",
                "subscription.unsubscribe",
                "observation.report",
            ],
            ["world-a"],
        )
        .unwrap();
    principals
        .insert(
            "filesystem",
            [22u8; 32],
            ["observation.report", "provider.register"],
            ["world-a"],
        )
        .unwrap();
    let system_secret = URL_SAFE_NO_PAD.encode([21u8; 32]);
    let filesystem_secret = URL_SAFE_NO_PAD.encode([22u8; 32]);
    let state = ipc::CoreState::new_with_semantic_world_and_provider_pipes_and_principals(
        Storage::open(dir.join("fixture.db")).unwrap(),
        Some("world-a"),
        &format!(r"\\.\pipe\spaceos-4-3a2-fs-unused-{suffix}"),
        &format!(r"\\.\pipe\spaceos-4-3a2-browser-unused-{suffix}"),
        principals,
    );
    let server_state = state.clone();
    let server_pipe = pipe.clone();
    let server =
        tokio::spawn(async move { ipc::run_server(server_state, &server_pipe).await.unwrap() });

    let action = ActionRequest::new(
        "create",
        json!({"path": dir.join("never.txt"), "content":"x"}),
        "filesystem",
        "system",
    );
    let transport_only = ipc::send_request(&pipe, "request_action", json!(action))
        .await
        .unwrap();
    assert_eq!(
        transport_only["result"]["status"], "Denied",
        "{transport_only}"
    );
    let mut invalid = json!(action);
    invalid["principal_token"] = json!("invalid");
    let invalid_result = ipc::send_request(&pipe, "request_action", invalid)
        .await
        .unwrap();
    assert_eq!(invalid_result["result"]["status"], "Denied");
    let mut wrong_actor = json!(action);
    wrong_actor["actor"] = json!("filesystem");
    wrong_actor["principal_token"] = json!(system_secret);
    let wrong_actor_result = ipc::send_request(&pipe, "request_action", wrong_actor)
        .await
        .unwrap();
    assert_eq!(wrong_actor_result["result"]["status"], "Denied");
    let mut too_much = json!(action);
    too_much["action"] = json!("delete");
    too_much["principal_token"] = json!(system_secret);
    let excess_capability = ipc::send_request(&pipe, "request_action", too_much)
        .await
        .unwrap();
    assert_eq!(excess_capability["result"]["status"], "Denied");
    let mut conflict = json!(action);
    conflict["actor_id"] = json!("filesystem");
    conflict["principal_token"] = json!(system_secret);
    let conflict_result = ipc::send_request(&pipe, "request_action", conflict)
        .await
        .unwrap();
    assert_eq!(conflict_result["result"]["status"], "Denied");
    assert!(!dir.join("never.txt").exists());

    let transport_register = ipc::send_request(&pipe, "register_agent", json!({
        "principal":"system", "name":"fixture-agent", "capabilities":["action.read","semantic.read"], "worlds":["world-a"]
    })).await.unwrap();
    assert_eq!(transport_register["ok"], false);
    let wrong_privileged = ipc::send_request(
        &pipe,
        "register_agent",
        json!({
            "principal":"system", "principal_token":filesystem_secret, "name":"fixture-agent",
            "capabilities":["action.read","semantic.read"], "worlds":["world-a"]
        }),
    )
    .await
    .unwrap();
    assert_eq!(wrong_privileged["ok"], false);
    let registration = ipc::send_request(
        &pipe,
        "register_agent",
        json!({
            "principal":"system", "principal_token":system_secret, "name":"fixture-agent",
            "capabilities":["action.read","semantic.read"], "worlds":["world-a"]
        }),
    )
    .await
    .unwrap();
    assert_eq!(registration["ok"], true, "{registration}");
    let agent_token = registration["result"]["token"].as_str().unwrap();
    let unauthorized = ActionRequest::new(
        "create",
        json!({"path":dir.join("agent-denied.txt")}),
        "filesystem",
        "fixture-agent",
    );
    let mut unauthorized = json!(unauthorized);
    unauthorized["actor_token"] = json!(agent_token);
    let unauthorized_result = ipc::send_request(&pipe, "request_action", unauthorized.clone())
        .await
        .unwrap();
    assert_eq!(unauthorized_result["result"]["status"], "Denied");
    assert!(!dir.join("agent-denied.txt").exists());
    unauthorized["principal_token"] = json!(system_secret);
    let mixed_result = ipc::send_request(&pipe, "request_action", unauthorized)
        .await
        .unwrap();
    assert_eq!(mixed_result["result"]["status"], "Denied");

    let no_read=state.policy.register("fixture-no-read","system",vec![],vec!["world-a".into()]).unwrap();
    let no_read_subscription=raw(&pipe,&json!({"method":"subscribe","params":{
        "subscriber":"fixture-no-read","actor_token":no_read,"world_id":"world-a"}}).to_string()).await;
    assert_eq!(no_read_subscription["ok"],false,"membership without semantic.read must be denied");
    let valid_subscription = raw(
        &pipe,
        &json!({"method":"subscribe", "params":{
            "subscriber":"fixture-agent", "actor_token":agent_token,
            "auth":"intentionally-wrong", "world_id":"world-a"
        }})
        .to_string(),
    )
    .await;
    assert_eq!(valid_subscription["ok"], true, "{valid_subscription}");
    let wrong_world = raw(
        &pipe,
        &json!({"method":"subscribe", "params":{
            "subscriber":"fixture-agent", "actor_token":agent_token,
            "auth":"intentionally-wrong", "world_id":"world-b"
        }})
        .to_string(),
    )
    .await;
    assert_eq!(wrong_world["ok"], false);
    let system_subscription = raw(
        &pipe,
        &json!({"method":"subscribe", "params":{
            "subscriber":"system", "principal_token":system_secret, "world_id":"world-a"
        }})
        .to_string(),
    )
    .await;
    assert_eq!(system_subscription["ok"], true, "{system_subscription}");
    let system_wrong_world = raw(
        &pipe,
        &json!({"method":"subscribe", "params":{
            "subscriber":"system", "principal_token":system_secret, "world_id":"world-b"
        }})
        .to_string(),
    )
    .await;
    assert_eq!(system_wrong_world["ok"], false);

    let provider_forge = ipc::send_request(
        &pipe,
        "register_provider",
        json!({
            "id":"filesystem", "name":"forged", "capabilities":[], "principal":"filesystem"
        }),
    )
    .await
    .unwrap();
    assert_eq!(provider_forge["ok"], false);
    let provider_registration = ipc::send_request(
        &pipe,
        "register_provider",
        json!({
            "id":"filesystem", "name":"isolated-provider", "capabilities":[],
            "principal":"filesystem", "principal_token":filesystem_secret
        }),
    )
    .await
    .unwrap();
    assert_eq!(provider_registration["ok"], true, "{provider_registration}");
    let wrong_provider_registration = ipc::send_request(
        &pipe,
        "register_provider",
        json!({
            "id":"browser", "name":"forged", "capabilities":[],
            "principal":"filesystem", "principal_token":filesystem_secret
        }),
    )
    .await
    .unwrap();
    assert_eq!(wrong_provider_registration["ok"], false);
    let observation_forge = ipc::send_request(
        &pipe,
        "report_observation",
        json!({
            "actor":"filesystem", "world_id":"world-a"
        }),
    )
    .await
    .unwrap();
    assert_eq!(observation_forge["ok"], false);
    let event_forge = ipc::send_request(
        &pipe,
        "record_event",
        json!({
            "actor":"system", "event_type":"Forged"
        }),
    )
    .await
    .unwrap();
    assert_eq!(event_forge["ok"], false);
    let malformed = raw(&pipe, "{not-json").await;
    assert_eq!(malformed["ok"], false);
    for denied in [
        &transport_only,
        &invalid_result,
        &wrong_actor_result,
        &transport_register,
        &provider_forge,
    ] {
        let rendered = denied.to_string();
        assert!(!rendered.contains(&system_secret));
        assert!(!rendered.contains(&filesystem_secret));
        assert!(!rendered.contains(agent_token));
    }
    server.abort();
    let _ = server.await;
    drop(state);
    // SQLite may still be held by process-global test hooks on Windows.
    // Fixture remains only under the lab-local TEMP tree.
}
