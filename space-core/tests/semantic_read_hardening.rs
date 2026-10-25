use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use serde_json::json;
use space_core::{
    ipc, principal::PrincipalRegistry, Event, Object, ObjectContextQuery, Relation, Storage,
};

const WORLD_A: &str = "stage4-world-a";
const WORLD_B: &str = "stage4-world-b";

fn temp_db(tag: &str) -> std::path::PathBuf {
    let directory = std::env::temp_dir().join("spaceos-stage4-hardening");
    std::fs::create_dir_all(&directory).unwrap();
    directory.join(format!("{tag}-{}.db", uuid::Uuid::new_v4()))
}

fn object(id: &str, world: &str, provider: &str) -> Object {
    let mut object = Object::new(
        "Note",
        provider,
        json!({"world_id": world, "name": format!("name:{id}")}),
    );
    object.id = id.to_string();
    object
}

fn relation(id: &str, source: &str, target: &str, world: &str, offset_ms: i64) -> Relation {
    let mut relation = Relation::new(source, "related_to", target);
    relation.id = id.to_string();
    relation.world_id = world.to_string();
    relation.created_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
        + ChronoDuration::milliseconds(offset_ms);
    relation
}

#[test]
fn exact_limits_empty_neighborhood_unicode_and_validation_boundaries() {
    let db = temp_db("limits");
    let mut storage = Storage::open(&db).unwrap();
    let unicode_id = "Объект-世界-🚀";
    storage
        .create_object(&object(unicode_id, WORLD_A, "space-world-user"))
        .unwrap();
    storage
        .create_object(&object("neighbor", WORLD_A, "filesystem"))
        .unwrap();

    let empty = storage
        .get_object_context(&ObjectContextQuery {
            object_id: unicode_id.to_string(),
            world_id: WORLD_A.to_string(),
            limit: None,
        })
        .unwrap()
        .unwrap();
    assert_eq!(empty.object.id, unicode_id);
    assert_eq!(empty.object.provider_id, "space-world-user");
    assert_eq!(empty.applied_limit, 50);
    assert!(empty.relations.is_empty());
    assert!(!empty.truncated);

    let tied_time = 0;
    for index in (0..101).rev() {
        storage
            .create_relation(&relation(
                &format!("edge-{index:03}"),
                unicode_id,
                "neighbor",
                WORLD_A,
                tied_time,
            ))
            .unwrap();
    }

    for (limit, expected_len, truncated) in [(1, 1, true), (50, 50, true), (100, 100, true)] {
        let context = storage
            .get_object_context(&ObjectContextQuery {
                object_id: unicode_id.to_string(),
                world_id: WORLD_A.to_string(),
                limit: Some(limit),
            })
            .unwrap()
            .unwrap();
        assert_eq!(context.applied_limit, limit);
        assert_eq!(context.relations.len(), expected_len);
        assert_eq!(context.truncated, truncated);
        assert_eq!(context.relations[0].id, "edge-000");
        assert_eq!(
            context.relations[expected_len - 1].id,
            format!("edge-{:03}", expected_len - 1)
        );
    }

    let default_page = storage
        .get_object_context(&ObjectContextQuery {
            object_id: unicode_id.to_string(),
            world_id: WORLD_A.to_string(),
            limit: None,
        })
        .unwrap()
        .unwrap();
    assert_eq!(default_page.applied_limit, 50);
    assert_eq!(default_page.relations.len(), 50);
    assert!(default_page.truncated);

    for query in [
        ObjectContextQuery {
            object_id: "x".repeat(513),
            world_id: WORLD_A.to_string(),
            limit: Some(1),
        },
        ObjectContextQuery {
            object_id: unicode_id.to_string(),
            world_id: "  ".to_string(),
            limit: Some(1),
        },
        ObjectContextQuery {
            object_id: unicode_id.to_string(),
            world_id: WORLD_A.to_string(),
            limit: Some(101),
        },
    ] {
        assert!(storage.get_object_context(&query).is_err(), "{query:?}");
    }
}

#[test]
fn malformed_legacy_rows_fail_closed_and_failed_transactions_release_cleanly() {
    let db = temp_db("malformed");
    {
        let storage = Storage::open(&db).unwrap();
        storage
            .create_object(&object("center", WORLD_A, "filesystem"))
            .unwrap();
        storage
            .create_object(&object("neighbor", WORLD_A, "browser"))
            .unwrap();
        storage
            .create_relation(&relation("broken-time", "center", "neighbor", WORLD_A, 1))
            .unwrap();
    }

    let connection = rusqlite::Connection::open(&db).unwrap();
    let null_result = connection.execute(
        "UPDATE relations SET created_at = NULL WHERE id = 'broken-time'",
        [],
    );
    assert!(null_result.is_err());
    connection
        .execute(
            "UPDATE relations SET created_at = 'not-rfc3339' WHERE id = 'broken-time'",
            [],
        )
        .unwrap();
    drop(connection);

    let mut storage = Storage::open(&db).unwrap();
    let query = ObjectContextQuery {
        object_id: "center".to_string(),
        world_id: WORLD_A.to_string(),
        limit: Some(10),
    };
    let error = storage.get_object_context(&query).unwrap_err().to_string();
    assert!(!error.is_empty());

    // The failed read transaction must have rolled back on drop. A separate
    // connection can repair the isolated fixture, after which the same
    // Storage connection remains usable.
    let connection = rusqlite::Connection::open(&db).unwrap();
    connection
        .execute(
            "UPDATE relations SET created_at = '2026-01-01T00:00:00+00:00' WHERE id = 'broken-time'",
            [],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE objects SET properties = 'not-json' WHERE id = 'neighbor'",
            [],
        )
        .unwrap();
    drop(connection);

    let json_error = storage.get_object_context(&query).unwrap_err().to_string();
    assert!(!json_error.is_empty());

    let connection = rusqlite::Connection::open(&db).unwrap();
    connection
        .execute(
            "UPDATE objects SET properties = ?1 WHERE id = 'neighbor'",
            rusqlite::params![json!({"world_id": WORLD_A}).to_string()],
        )
        .unwrap();
    drop(connection);
    let recovered = storage.get_object_context(&query).unwrap().unwrap();
    assert_eq!(recovered.relations.len(), 1);
}

#[test]
fn sqlite_lock_is_returned_as_an_error_without_panicking() {
    let db = temp_db("locked");
    let mut storage = Storage::open(&db).unwrap();
    storage
        .create_object(&object("center", WORLD_A, "filesystem"))
        .unwrap();

    let lock = rusqlite::Connection::open(&db).unwrap();
    lock.execute_batch("BEGIN EXCLUSIVE;").unwrap();
    let result = storage.get_object_context(&ObjectContextQuery {
        object_id: "center".to_string(),
        world_id: WORLD_A.to_string(),
        limit: Some(10),
    });
    assert!(result.is_err());
    lock.execute_batch("ROLLBACK;").unwrap();

    assert!(storage
        .get_object_context(&ObjectContextQuery {
            object_id: "center".to_string(),
            world_id: WORLD_A.to_string(),
            limit: Some(10),
        })
        .unwrap()
        .is_some());
}

#[test]
fn event_cursor_is_the_global_visible_sequence_not_a_world_local_cursor() {
    let db = temp_db("cursor");
    let mut storage = Storage::open(&db).unwrap();
    storage
        .create_object(&object("center", WORLD_A, "space-world-user"))
        .unwrap();
    for (world, kind) in [(WORLD_A, "WorldAEvent"), (WORLD_B, "WorldBEvent")] {
        let mut event = Event::new(kind, "system", json!({}));
        event.world_id = world.to_string();
        storage.record_event(&event).unwrap();
    }
    let context = storage
        .get_object_context(&ObjectContextQuery {
            object_id: "center".to_string(),
            world_id: WORLD_A.to_string(),
            limit: Some(1),
        })
        .unwrap()
        .unwrap();
    assert_eq!(context.event_cursor, 2);
}

#[test]
fn malformed_negative_event_sequence_is_rejected_instead_of_wrapping() {
    let db = temp_db("negative-cursor");
    {
        let storage = Storage::open(&db).unwrap();
        storage
            .create_object(&object("center", WORLD_A, "space-world-user"))
            .unwrap();
    }
    let connection = rusqlite::Connection::open(&db).unwrap();
    connection
        .execute(
            "INSERT INTO events
             (seq, id, event_type, actor, provider_id, object_ids, payload, timestamp, correlation_id, world_id)
             VALUES (-1, 'negative-seq', 'MalformedLegacy', 'system', NULL, '[]', '{}',
                     '2026-01-01T00:00:00+00:00', NULL, ?1)",
            rusqlite::params![WORLD_A],
        )
        .unwrap();
    drop(connection);

    let mut storage = Storage::open(&db).unwrap();
    let result = storage.get_object_context(&ObjectContextQuery {
        object_id: "center".to_string(),
        world_id: WORLD_A.to_string(),
        limit: Some(1),
    });
    assert!(result.is_err());
}

#[cfg(windows)]
async fn raw_pipe_line(pipe: &str, mut request: String) -> serde_json::Value {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::windows::named_pipe::ClientOptions;

    let stream = loop {
        match ClientOptions::new().open(pipe) {
            Ok(stream) => break stream,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
        }
    };
    let (reader, mut writer) = tokio::io::split(stream);
    request.push('\n');
    writer.write_all(request.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();
    let mut lines = BufReader::new(reader).lines();
    serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap()
}

#[cfg(windows)]
async fn raw_pipe_request(
    pipe: &str,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    raw_pipe_line(
        pipe,
        serde_json::to_string(&json!({"method": method, "params": params})).unwrap(),
    )
    .await
}

#[cfg(windows)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ipc_rejects_transport_only_and_requires_privileged_read_identity() {
    let pipe = format!(
        r"\\.\pipe\spaceos-stage4-hardening-{}",
        uuid::Uuid::new_v4()
    );
    let db = temp_db("transport-auth");
    let storage = Storage::open(&db).unwrap();
    storage
        .create_object(&object("center", WORLD_A, "filesystem"))
        .unwrap();
    let mut principals = PrincipalRegistry::default();
    principals
        .insert("cli", [103u8; 32], ["semantic.read"], [WORLD_A])
        .unwrap();
    let state = ipc::CoreState::new_with_semantic_world_and_provider_pipes_and_principals(
        storage,
        Some(WORLD_A),
        ipc::PROVIDER_PIPE,
        ipc::BROWSER_PROVIDER_PIPE,
        principals,
    );
    let expected_core_token = state.core_token.clone();
    let server_state = state.clone();
    let server_pipe = pipe.clone();
    let server = tokio::spawn(async move {
        let _ = ipc::run_server(server_state, &server_pipe).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    let base = json!({
        "actor": "cli",
        "query": {"object_id": "center", "world_id": WORLD_A, "limit": 10}
    });
    let missing = raw_pipe_request(&pipe, "get_object_context", base.clone()).await;
    assert_eq!(missing["ok"], false, "{missing}");
    assert!(missing["error"].as_str().unwrap().contains("denied"));

    let mut invalid_params = base.clone();
    invalid_params["auth"] = json!("invalid-transport-token");
    let invalid = raw_pipe_request(&pipe, "get_object_context", invalid_params).await;
    assert_eq!(invalid["ok"], false, "{invalid}");

    let mut valid_params = base;
    valid_params["auth"] = json!(expected_core_token);
    let generic_only = raw_pipe_request(&pipe, "get_object_context", valid_params.clone()).await;
    assert_eq!(generic_only["ok"], false, "{generic_only}");
    valid_params["principal_token"] = json!(URL_SAFE_NO_PAD.encode([103u8; 32]));
    let valid = raw_pipe_request(&pipe, "get_object_context", valid_params).await;
    assert_eq!(valid["ok"], true, "{valid}");
    assert_eq!(valid["result"]["object"]["provider_id"], "filesystem");

    // DTO unknown fields are rejected before the Storage lock is acquired.
    let unknown = raw_pipe_request(
        &pipe,
        "get_object_context",
        json!({
            "actor": "cli",
            "auth": state.core_token,
            "unexpected": true,
            "query": {"object_id": "center", "world_id": WORLD_A, "limit": 10}
        }),
    )
    .await;
    assert_eq!(unknown["ok"], false, "{unknown}");

    let malformed_json = raw_pipe_line(&pipe, "{not-json".to_string()).await;
    assert_eq!(malformed_json["ok"], false, "{malformed_json}");
    assert!(malformed_json["error"]
        .as_str()
        .unwrap()
        .contains("bad json"));
    server.abort();
}

#[cfg(windows)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registered_agent_auth_does_not_currently_validate_transport_auth() {
    let pipe = format!(
        r"\\.\pipe\spaceos-stage4-agent-auth-{}",
        uuid::Uuid::new_v4()
    );
    let db = temp_db("agent-transport-auth");
    let storage = Storage::open(&db).unwrap();
    storage
        .create_object(&object("center", WORLD_A, "space-world-user"))
        .unwrap();
    let mut principals = PrincipalRegistry::default();
    principals
        .insert(
            "system",
            [104u8; 32],
            ["agent.register", "agent.grant.semantic.read"],
            [WORLD_A],
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
    let server_pipe = pipe.clone();
    let server = tokio::spawn(async move {
        let _ = ipc::run_server(server_state, &server_pipe).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    let registration = ipc::send_request(
        &pipe,
        "register_agent",
        json!({
            "principal": "system",
            "principal_token": URL_SAFE_NO_PAD.encode([104u8; 32]),
            "name": "stage4-hardening-reader",
            "capabilities": ["semantic.read"],
            "worlds": [WORLD_A]
        }),
    )
    .await
    .unwrap();
    let actor_token = registration["result"]["token"].as_str().unwrap();
    let response = raw_pipe_request(
        &pipe,
        "get_object_context",
        json!({
            "actor": "stage4-hardening-reader",
            "actor_token": actor_token,
            "auth": "deliberately-invalid-transport-token",
            "query": {"object_id": "center", "world_id": WORLD_A, "limit": 10}
        }),
    )
    .await;

    // Model A agents authenticate with actor_token even when the general
    // transport credential is wrong. Privileged callers use principal_token.
    assert_eq!(response["ok"], true, "{response}");
    server.abort();
}
