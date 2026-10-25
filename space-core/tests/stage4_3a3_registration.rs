//! Real named-pipe checks for the fixed-profile Bridge registrar.
#![cfg(windows)]

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};
use space_core::{
    ipc,
    principal::PrincipalRegistry,
    service_registration::{
        ServiceRegistrarRegistry, DASHBOARD_CAPS, DASHBOARD_PROFILE, MCP_CAPS, MCP_PROFILE,
        SEMANTIC_CAPS, SEMANTIC_PROFILE,
    },
    ActionRequest, Storage,
};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::windows::named_pipe::ClientOptions;

fn registration(secret: &str, world: &str) -> Value {
    json!({
        "profile": SEMANTIC_PROFILE,
        "name": "space-world-user",
        "worlds": [world],
        "capabilities": SEMANTIC_CAPS,
        "registrar_token": secret,
    })
}

async fn raw(pipe: &str, input: &str) -> Value {
    let stream = ClientOptions::new().open(pipe).unwrap();
    let (reader, mut writer) = tokio::io::split(stream);
    writer.write_all(input.as_bytes()).await.unwrap();
    writer.write_all(b"\n").await.unwrap();
    writer.flush().await.unwrap();
    let mut lines = BufReader::new(reader).lines();
    serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registrar_is_not_system_and_cannot_expand_profile() {
    let root = std::env::var("SPACEOS_TEST_ISOLATION_ROOT").unwrap();
    assert_eq!(root, "C:\\spaceos");
    assert!(std::env::var_os("SPACEOS_CORE_TOKEN").is_some());
    let suffix = uuid::Uuid::new_v4();
    let dir = std::env::temp_dir().join(format!("spaceos-4-3a3-{suffix}"));
    assert!(dir.starts_with("C:\\spaceos\\test-tmp"));
    std::fs::create_dir_all(&dir).unwrap();
    let pipe = format!(r"\\.\pipe\spaceos-4-3a3-{suffix}");
    let world = "synthetic-world";
    let system_token = URL_SAFE_NO_PAD.encode([81u8; 32]);
    let semantic_token = URL_SAFE_NO_PAD.encode([82u8; 32]);
    let dashboard_token = URL_SAFE_NO_PAD.encode([83u8; 32]);
    let mcp_token = URL_SAFE_NO_PAD.encode([85u8; 32]);
    let transport = ipc::core_token();
    let mut principals = PrincipalRegistry::default();
    principals
        .insert(
            "system",
            [81u8; 32],
            [
                "agent.register",
                "agent.grant.action.create",
                "agent.grant.semantic.object.create",
            ],
            [world],
        )
        .unwrap();
    let mut registrars = ServiceRegistrarRegistry::default();
    registrars
        .insert(SEMANTIC_PROFILE, [82u8; 32], &principals, &transport)
        .unwrap();
    registrars
        .insert(DASHBOARD_PROFILE, [83u8; 32], &principals, &transport)
        .unwrap();
    registrars
        .insert(MCP_PROFILE, [85u8; 32], &principals, &transport)
        .unwrap();
    let core = ipc::CoreState::new_with_semantic_world_and_identities(
        Storage::open(dir.join("isolated.db")).unwrap(),
        Some(world),
        &format!(r"\\.\pipe\spaceos-fs-unused-{suffix}"),
        &format!(r"\\.\pipe\spaceos-browser-unused-{suffix}"),
        principals,
        registrars,
        transport,
    );
    let server_state = core.clone();
    let server_pipe = pipe.clone();
    let server =
        tokio::spawn(async move { ipc::run_server(server_state, &server_pipe).await.unwrap() });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if ipc::send_request(&pipe, "ping", json!({})).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let generic = ipc::send_request(&pipe, "register_agent", json!({
        "principal":"system", "name":"forged", "worlds":[world], "capabilities":["action.create"]
    })).await.unwrap();
    assert_eq!(generic["ok"], false);
    let missing = ipc::send_request(&pipe, "register_service_actor", json!({
        "profile":SEMANTIC_PROFILE,"name":"space-world-user","worlds":[world],"capabilities":SEMANTIC_CAPS
    })).await.unwrap();
    assert_eq!(missing["ok"], false);
    let mut invalid = registration("invalid", world);
    assert_eq!(
        ipc::send_request(&pipe, "register_service_actor", invalid.clone())
            .await
            .unwrap()["ok"],
        false
    );
    invalid["registrar_token"] = json!(dashboard_token);
    assert_eq!(
        ipc::send_request(&pipe, "register_service_actor", invalid)
            .await
            .unwrap()["ok"],
        false
    );
    for bad in [
        {
            let mut r = registration(&semantic_token, world);
            r["name"] = json!("system");
            r
        },
        {
            let mut r = registration(&semantic_token, world);
            r["name"] = json!("other-agent");
            r
        },
        registration(&semantic_token, "other-world"),
        {
            let mut r = registration(&semantic_token, world);
            r["capabilities"] = json!([
                "semantic.object.create",
                "semantic.object.patch",
                "semantic.relation.create",
                "action.create"
            ]);
            r
        },
        {
            let mut r = registration(&semantic_token, world);
            r["capabilities"] = json!(["semantic.object.create"]);
            r
        },
        {
            let mut r = registration(&semantic_token, world);
            r["principal_token"] = json!(system_token);
            r
        },
        {
            let mut r = registration(&semantic_token, world);
            r["worlds"] = json!("wrong-type");
            r
        },
    ] {
        let result = ipc::send_request(&pipe, "register_service_actor", bad)
            .await
            .unwrap();
        assert_eq!(result["ok"], false, "{result}");
    }
    assert!(!core.policy.is_registered("space-world-user"));

    let first = ipc::send_request(
        &pipe,
        "register_service_actor",
        registration(&semantic_token, world),
    )
    .await
    .unwrap();
    assert_eq!(first["ok"], true, "{first}");
    let old_actor_token = first["result"]["token"].as_str().unwrap().to_owned();
    assert_eq!(
        core.policy.capabilities_of("space-world-user"),
        SEMANTIC_CAPS
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        core.policy.worlds_of("space-world-user"),
        vec![world.to_owned()]
    );
    for (profile, name, credential, caps) in [
        (
            DASHBOARD_PROFILE,
            "space-bridge",
            &dashboard_token,
            DASHBOARD_CAPS,
        ),
        (MCP_PROFILE, "space-bridge-mcp", &mcp_token, MCP_CAPS),
    ] {
        let response = ipc::send_request(
            &pipe,
            "register_service_actor",
            json!({
                "profile":profile,"name":name,"worlds":[],"capabilities":caps,
                "registrar_token":credential,
            }),
        )
        .await
        .unwrap();
        assert_eq!(response["ok"], true, "{response}");
        assert!(core
            .policy
            .authenticate(name, response["result"]["token"].as_str()));
        assert_eq!(
            core.policy.capabilities_of(name),
            caps.iter().map(|cap| cap.to_string()).collect::<Vec<_>>()
        );
        assert!(core.policy.worlds_of(name).is_empty());
    }
    let second = ipc::send_request(
        &pipe,
        "register_service_actor",
        registration(&semantic_token, world),
    )
    .await
    .unwrap();
    assert_eq!(second["ok"], true);
    let new_actor_token = second["result"]["token"].as_str().unwrap().to_owned();
    assert_ne!(old_actor_token, new_actor_token);
    assert!(!core
        .policy
        .authenticate("space-world-user", Some(&old_actor_token)));
    assert!(core
        .policy
        .authenticate("space-world-user", Some(&new_actor_token)));
    assert!(!core
        .policy
        .may_access_world("space-world-user", "other-world"));
    assert!(!core.policy.can("space-world-user", "action.create"));

    let create = |actor_token: &str, scope: &str| {
        json!({
            "actor":"space-world-user", "actor_token":actor_token,
            "command":{"world_id":scope,"object_type":"Note","properties":{"name":"test"},"correlation_id":uuid::Uuid::new_v4().to_string()}
        })
    };
    assert_eq!(
        ipc::send_request(
            &pipe,
            "create_semantic_object",
            create(&old_actor_token, world)
        )
        .await
        .unwrap()["ok"],
        false
    );
    assert_eq!(
        ipc::send_request(
            &pipe,
            "create_semantic_object",
            create(&new_actor_token, "other-world")
        )
        .await
        .unwrap()["ok"],
        false
    );
    assert_eq!(
        ipc::send_request(
            &pipe,
            "create_semantic_object",
            create(&new_actor_token, world)
        )
        .await
        .unwrap()["ok"],
        true
    );
    let registrar_as_actor = ipc::send_request(&pipe, "create_semantic_object", json!({
        "actor":"space-world-user", "registrar_token":semantic_token,
        "command":{"world_id":world,"object_type":"Note","properties":{},"correlation_id":"registrar-direct"}
    })).await.unwrap();
    assert_eq!(registrar_as_actor["ok"], false);
    let registrar_as_system = ipc::send_request(&pipe, "register_agent", json!({
        "principal":"system","principal_token":semantic_token,"name":"forged","worlds":[],"capabilities":[]
    })).await.unwrap();
    assert_eq!(registrar_as_system["ok"], false);
    let event = ipc::send_request(&pipe, "record_event", json!({
        "id":uuid::Uuid::new_v4().to_string(), "event_type":"forged", "actor":"system",
        "principal_token":semantic_token, "timestamp":chrono::Utc::now().to_rfc3339(), "payload":{}
    })).await.unwrap();
    assert_eq!(event["ok"], false);
    let provider = ipc::send_request(&pipe, "register_provider", json!({
        "principal":"system", "principal_token":semantic_token,"id":"filesystem","name":"forged","capabilities":[]
    })).await.unwrap();
    assert_eq!(provider["ok"], false);
    let action = ActionRequest::new(
        "create",
        json!({"path":dir.join("must-not-exist.txt")}),
        "filesystem",
        "system",
    );
    let mut action = json!(action);
    action["principal_token"] = json!(semantic_token);
    let dispatch = ipc::send_request(&pipe, "request_action", action)
        .await
        .unwrap();
    assert_eq!(dispatch["result"]["status"], "Denied");
    assert!(!dir.join("must-not-exist.txt").exists());

    let (left, right) = tokio::join!(
        ipc::send_request(
            &pipe,
            "register_service_actor",
            registration(&semantic_token, world)
        ),
        ipc::send_request(
            &pipe,
            "register_service_actor",
            registration(&semantic_token, world)
        ),
    );
    let (left, right) = (left.unwrap(), right.unwrap());
    assert_eq!(left["ok"], true);
    assert_eq!(right["ok"], true);
    let left_token = left["result"]["token"].as_str().unwrap();
    let right_token = right["result"]["token"].as_str().unwrap();
    assert_ne!(left_token, right_token);
    assert!(
        core.policy
            .authenticate("space-world-user", Some(left_token))
            ^ core
                .policy
                .authenticate("space-world-user", Some(right_token))
    );

    let mut registration_us = Vec::new();
    for _ in 0..64 {
        let start = Instant::now();
        let response = ipc::send_request(
            &pipe,
            "register_service_actor",
            registration(&semantic_token, world),
        )
        .await
        .unwrap();
        assert_eq!(response["ok"], true, "{response}");
        registration_us.push(start.elapsed().as_micros());
    }
    registration_us.sort_unstable();
    println!(
        "registration debug IPC latency, 64 iterations, 1 actor: median={}us p95={}us max={}us",
        registration_us[32], registration_us[60], registration_us[63]
    );
    assert_eq!(
        core.policy.capabilities_of("space-world-user"),
        SEMANTIC_CAPS
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
    );

    let administrative_change = ipc::send_request(
        &pipe,
        "register_agent",
        json!({
            "principal":"system","principal_token":system_token,
            "name":"space-world-user","worlds":[world],"capabilities":["action.create"]
        }),
    )
    .await
    .unwrap();
    assert_eq!(administrative_change["ok"], true);
    let conflict = ipc::send_request(
        &pipe,
        "register_service_actor",
        registration(&semantic_token, world),
    )
    .await
    .unwrap();
    assert_eq!(conflict["ok"], false);
    assert_eq!(conflict["error"], "registration conflict");
    assert_eq!(
        core.policy.capabilities_of("space-world-user"),
        vec!["action.create"]
    );
    let malformed = raw(&pipe, "{not json").await;
    assert_eq!(malformed["ok"], false);
    for response in [
        &generic,
        &missing,
        &registrar_as_system,
        &event,
        &provider,
        &dispatch,
        &conflict,
        &malformed,
    ] {
        let text = response.to_string();
        assert!(!text.contains(&semantic_token));
        assert!(!text.contains(&system_token));
    }
    server.abort();

    let mut missing_world_registrars = ServiceRegistrarRegistry::default();
    missing_world_registrars
        .insert(
            SEMANTIC_PROFILE,
            [82u8; 32],
            &PrincipalRegistry::default(),
            &ipc::core_token(),
        )
        .unwrap();
    let absent_world = ipc::CoreState::new_with_semantic_world_and_identities(
        Storage::open(dir.join("missing-world.db")).unwrap(),
        None,
        &format!(r"\\.\pipe\spaceos-fs-unused2-{suffix}"),
        &format!(r"\\.\pipe\spaceos-browser-unused2-{suffix}"),
        PrincipalRegistry::default(),
        missing_world_registrars,
        ipc::core_token(),
    );
    let second_pipe = format!(r"\\.\pipe\spaceos-4-3a3-no-world-{suffix}");
    let state = absent_world.clone();
    let pipe_name = second_pipe.clone();
    let second_server =
        tokio::spawn(async move { ipc::run_server(state, &pipe_name).await.unwrap() });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if ipc::send_request(&second_pipe, "ping", json!({}))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let missing_config = ipc::send_request(
        &second_pipe,
        "register_service_actor",
        registration(&semantic_token, world),
    )
    .await
    .unwrap();
    assert_eq!(missing_config["ok"], false);
    assert_eq!(
        missing_config["error"],
        "semantic world configuration unavailable"
    );
    assert!(!absent_world.policy.is_registered("space-world-user"));
    second_server.abort();

    let rotated_token = URL_SAFE_NO_PAD.encode([84u8; 32]);
    let mut rotated_registrars = ServiceRegistrarRegistry::default();
    rotated_registrars
        .insert(
            SEMANTIC_PROFILE,
            [84u8; 32],
            &PrincipalRegistry::default(),
            &ipc::core_token(),
        )
        .unwrap();
    let rotated_core = ipc::CoreState::new_with_semantic_world_and_identities(
        Storage::open(dir.join("rotated.db")).unwrap(),
        Some(world),
        &format!(r"\\.\pipe\spaceos-fs-unused3-{suffix}"),
        &format!(r"\\.\pipe\spaceos-browser-unused3-{suffix}"),
        PrincipalRegistry::default(),
        rotated_registrars,
        ipc::core_token(),
    );
    let rotated_pipe = format!(r"\\.\pipe\spaceos-4-3a3-rotated-{suffix}");
    let state = rotated_core.clone();
    let pipe_name = rotated_pipe.clone();
    let rotated_server =
        tokio::spawn(async move { ipc::run_server(state, &pipe_name).await.unwrap() });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if ipc::send_request(&rotated_pipe, "ping", json!({}))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        ipc::send_request(
            &rotated_pipe,
            "register_service_actor",
            registration(&semantic_token, world)
        )
        .await
        .unwrap()["ok"],
        false
    );
    let after_rotation = ipc::send_request(
        &rotated_pipe,
        "register_service_actor",
        registration(&rotated_token, world),
    )
    .await
    .unwrap();
    assert_eq!(after_rotation["ok"], true);
    assert!(!rotated_core
        .policy
        .authenticate("space-world-user", Some(&old_actor_token)));
    rotated_server.abort();
}
