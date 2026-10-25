use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::{TimeZone, Utc};
use serde_json::json;
use space_core::{
    ipc, principal::PrincipalRegistry,
    service_registration::{ServiceRegistrarRegistry, WORLD_READ_ACTOR, WORLD_READ_PROFILE},
    Event, Object, ObjectContextQuery, Relation,
    RelationDirection, Storage,
};

const WORLD_A: &str = "stage4-world-a";
const WORLD_B: &str = "stage4-world-b";

fn temp_db(tag: &str) -> std::path::PathBuf {
    let directory = std::env::temp_dir().join("spaceos-stage4");
    std::fs::create_dir_all(&directory).unwrap();
    directory.join(format!("{tag}-{}.db", uuid::Uuid::new_v4()))
}

fn object(id: &str, world: &str, provider: &str) -> Object {
    let mut object = Object::new(
        "Note",
        provider,
        json!({"world_id": world, "private_test_value": "not projected to summaries"}),
    );
    object.id = id.to_string();
    object
}

fn relation(
    id: &str,
    source: &str,
    relation_type: &str,
    target: &str,
    world: &str,
    second: u32,
) -> Relation {
    let mut relation = Relation::new(source, relation_type, target);
    relation.id = id.to_string();
    relation.world_id = world.to_string();
    relation.created_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, second).unwrap();
    relation
}

#[test]
fn context_preserves_direction_duplicates_self_edges_and_cursor() {
    let db = temp_db("shape");
    let mut storage = Storage::open(&db).unwrap();
    for object in [
        object("center", WORLD_A, "space-world-user"),
        object("out", WORLD_A, "filesystem"),
        object("in", WORLD_A, "browser"),
        object("cross-world", WORLD_B, "filesystem"),
    ] {
        storage.create_object(&object).unwrap();
    }
    for relation in [
        relation("r-out-1", "center", "related_to", "out", WORLD_A, 1),
        relation("r-out-2", "center", "related_to", "out", WORLD_A, 2),
        relation("r-in", "in", "mentions", "center", WORLD_A, 3),
        relation("r-self", "center", "related_to", "center", WORLD_A, 4),
        // Legacy APIs can construct inconsistent cross-world edges. The new
        // context query must not disclose the other-world endpoint.
        relation("r-cross", "center", "related_to", "cross-world", WORLD_A, 5),
    ] {
        storage.create_relation(&relation).unwrap();
    }
    let mut event = Event::new("FixtureCommitted", "system", json!({}));
    event.world_id = WORLD_A.to_string();
    storage.record_event(&event).unwrap();

    let context = storage
        .get_object_context(&ObjectContextQuery {
            object_id: "center".to_string(),
            world_id: WORLD_A.to_string(),
            limit: Some(100),
        })
        .unwrap()
        .unwrap();

    assert_eq!(context.object.id, "center");
    assert_eq!(context.world_id, WORLD_A);
    assert_eq!(context.event_cursor, 1);
    assert!(!context.truncated);
    assert_eq!(
        context
            .relations
            .iter()
            .map(|relation| relation.id.as_str())
            .collect::<Vec<_>>(),
        vec!["r-out-1", "r-out-2", "r-in", "r-self"]
    );
    assert_eq!(
        context
            .relations
            .iter()
            .map(|relation| relation.direction.clone())
            .collect::<Vec<_>>(),
        vec![
            RelationDirection::Outgoing,
            RelationDirection::Outgoing,
            RelationDirection::Incoming,
            RelationDirection::SelfRelation,
        ]
    );
    assert_eq!(context.relations[0].neighbor.as_ref().unwrap().id, "out");
    assert_eq!(context.relations[1].neighbor.as_ref().unwrap().id, "out");
    assert_eq!(context.relations[2].neighbor.as_ref().unwrap().id, "in");
    assert_eq!(context.relations[3].neighbor.as_ref().unwrap().id, "center");
    let serialized = serde_json::to_value(&context).unwrap();
    assert!(serialized.to_string().find("private_test_value").is_none());
    assert!(context
        .relations
        .iter()
        .all(|relation| relation.id != "r-cross"));

    let bounded = storage
        .get_object_context(&ObjectContextQuery {
            object_id: "center".to_string(),
            world_id: WORLD_A.to_string(),
            limit: Some(2),
        })
        .unwrap()
        .unwrap();
    assert_eq!(bounded.applied_limit, 2);
    assert_eq!(bounded.relations.len(), 2);
    assert!(bounded.truncated);

    assert!(storage
        .get_object_context(&ObjectContextQuery {
            object_id: "missing".to_string(),
            world_id: WORLD_A.to_string(),
            limit: None,
        })
        .unwrap()
        .is_none());
    assert!(storage
        .get_object_context(&ObjectContextQuery {
            object_id: "center".to_string(),
            world_id: WORLD_B.to_string(),
            limit: None,
        })
        .unwrap()
        .is_none());
    assert!(storage
        .get_object_context(&ObjectContextQuery {
            object_id: "center".to_string(),
            world_id: WORLD_A.to_string(),
            limit: Some(0),
        })
        .is_err());
}

#[test]
fn grandfathered_orphan_is_explicit_without_fabricating_an_object() {
    let db = temp_db("orphan");
    {
        let storage = Storage::open(&db).unwrap();
        storage
            .create_object(&object("center", WORLD_A, "filesystem"))
            .unwrap();
    }
    // Isolated malformed legacy fixture: production writes keep FK enabled.
    let connection = rusqlite::Connection::open(&db).unwrap();
    connection
        .execute_batch("PRAGMA foreign_keys=OFF;")
        .unwrap();
    connection
        .execute(
            "INSERT INTO relations
             (id, source_object_id, relation_type, target_object_id, properties, created_at, world_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                "legacy-orphan",
                "center",
                "related_to",
                "missing-neighbor",
                "{}",
                "2026-01-01T00:00:00+00:00",
                WORLD_A,
            ],
        )
        .unwrap();
    drop(connection);

    let mut storage = Storage::open(&db).unwrap();
    let context = storage
        .get_object_context(&ObjectContextQuery {
            object_id: "center".to_string(),
            world_id: WORLD_A.to_string(),
            limit: None,
        })
        .unwrap()
        .unwrap();
    assert_eq!(context.relations.len(), 1);
    assert_eq!(context.relations[0].id, "legacy-orphan");
    assert!(context.relations[0].neighbor.is_none());
}

async fn start_core_with_fixture(pipe: &str, db: &std::path::Path) -> std::sync::Arc<ipc::CoreState> {
    let storage = Storage::open(db).unwrap();
    storage
        .create_object(&object("center", WORLD_A, "space-world-user"))
        .unwrap();
    storage
        .create_object(&object("a-provider", WORLD_A, "filesystem"))
        .unwrap();
    storage
        .create_relation(&relation("a-edge", "center", "related_to", "a-provider", WORLD_A, 0))
        .unwrap();
    let mut a_event = Event::new("AEvent", "space-world-user", json!({"visible": true}));
    a_event.world_id = WORLD_A.to_string();
    storage.record_event(&a_event).unwrap();
    let mut approved_legacy = Object::new("Note", "filesystem", json!({"name": "approved legacy"}));
    approved_legacy.id = "legacy-approved".into();
    storage.create_object(&approved_legacy).unwrap();
    let mut private_legacy = Object::new("Note", "filesystem", json!({"name": "private legacy"}));
    private_legacy.id = "legacy-private".into();
    storage.create_object(&private_legacy).unwrap();
    storage
        .create_relation(&relation("legacy-private-edge", "legacy-approved", "related_to", "legacy-private", "", 0))
        .unwrap();
    let mut legacy_event = Event::new("LegacyPrivate", "filesystem", json!({"private": true}));
    legacy_event.world_id = "".into();
    storage.record_event(&legacy_event).unwrap();
    storage
        .create_object(&object("foreign", WORLD_B, "filesystem"))
        .unwrap();
    storage
        .create_object(&object("foreign-peer", WORLD_B, "filesystem"))
        .unwrap();
    storage
        .create_relation(&relation(
            "foreign-edge",
            "foreign",
            "related_to",
            "foreign-peer",
            WORLD_B,
            0,
        ))
        .unwrap();
    let mut foreign_event = Event::new("ForeignEvent", "filesystem", json!({"private": true}));
    foreign_event.world_id = WORLD_B.to_string();
    storage.record_event(&foreign_event).unwrap();
    let mut principals = PrincipalRegistry::default();
    principals
        .insert(
            "system",
            [101u8; 32],
            ["agent.register", "agent.grant.semantic.read"],
            [WORLD_A],
        )
        .unwrap();
    principals
        .insert("cli", [102u8; 32], ["semantic.read"], [WORLD_A])
        .unwrap();
    let mut registrars = ServiceRegistrarRegistry::default();
    registrars
        .insert(WORLD_READ_PROFILE, [110u8; 32], &principals, &ipc::core_token())
        .unwrap();
    let state = ipc::CoreState::new_with_semantic_world_and_identities(
        storage,
        Some(WORLD_A),
        ipc::PROVIDER_PIPE,
        ipc::BROWSER_PROVIDER_PIPE,
        principals,
        registrars,
        ipc::core_token(),
    );
    state.set_isolated_legacy_read_ids(&["legacy-approved"]).unwrap();
    let pipe = pipe.to_string();
    let server_state = state.clone();
    tokio::spawn(async move {
        let _ = ipc::run_server(server_state, &pipe).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    state
}

async fn register_reader(pipe: &str) -> String {
    let response = ipc::send_request(
        pipe,
        "register_agent",
        json!({
            "principal": "system",
            "principal_token": URL_SAFE_NO_PAD.encode([101u8; 32]),
            "name": "stage4-reader",
            "capabilities": ["semantic.read"],
            "worlds": [WORLD_A],
        }),
    )
    .await
    .unwrap();
    assert_eq!(response["ok"], true, "{response}");
    response["result"]["token"].as_str().unwrap().to_string()
}

async fn context_request(
    pipe: &str,
    actor: &str,
    actor_token: &str,
    world: &str,
) -> serde_json::Value {
    ipc::send_request(
        pipe,
        "get_object_context",
        json!({
            "actor": actor,
            "actor_token": actor_token,
            "query": {
                "object_id": "center",
                "world_id": world,
                "limit": 10,
            }
        }),
    )
    .await
    .unwrap()
}

// The I.3 negative was recorded before the I.4 gate. Retain the exact seven
// methods as a normal regression: no anonymous response may expose World B.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_reads_require_authenticated_world_scope() {
    let pipe = format!(r"\\.\pipe\spaceos-i4-legacy-{}", uuid::Uuid::new_v4());
    let db = temp_db("legacy-leak");
    start_core_with_fixture(&pipe, &db).await;
    let mut leaked = Vec::new();
    for (method, params) in [
        ("get_object", json!({"id": "foreign"})),
        ("get_relation", json!({"id": "foreign-edge"})),
        ("query_objects", json!({"world_id": WORLD_B})),
        ("query_relations", json!({"world_id": WORLD_B})),
        ("list_relations", json!({})),
        ("query_events", json!({"world_id": WORLD_B})),
        ("get_world", json!({"world_id": WORLD_B})),
    ] {
        let response = ipc::send_request(&pipe, method, params).await.unwrap();
        if response["ok"] == true {
            leaked.push(method);
        }
    }
    assert!(
        leaked.is_empty(),
        "anonymous Core IPC leaked through {leaked:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_service_and_model_a_enforce_world_and_legacy_allowlist() {
    let pipe = format!(r"\\.\pipe\spaceos-i4-read-{}", uuid::Uuid::new_v4());
    let db = temp_db("i4-read-scope");
    let state = start_core_with_fixture(&pipe, &db).await;
    let actor_token = register_reader(&pipe).await;
    let read = |method: &'static str, params: serde_json::Value| {
        let pipe = pipe.clone();
        async move { ipc::send_request(&pipe, method, params).await.unwrap() }
    };
    let actor_a = read(
        "query_objects",
        json!({"actor": "stage4-reader", "actor_token": actor_token, "world_id": WORLD_A}),
    ).await;
    assert_eq!(actor_a["ok"], true, "{actor_a}");
    let actor_ids: Vec<&str> = actor_a["result"].as_array().unwrap().iter()
        .filter_map(|object| object["id"].as_str()).collect();
    assert!(actor_ids.contains(&"center") && actor_ids.contains(&"a-provider"));
    assert!(!actor_ids.contains(&"foreign") && !actor_ids.contains(&"legacy-approved"));
    for (method, extra) in [
        ("get_object", json!({"id": "foreign"})),
        ("get_relation", json!({"id": "foreign-edge"})),
        ("query_events", json!({"world_id": WORLD_B})),
        ("query_objects", json!({"world_id": WORLD_B})),
    ] {
        let mut params = extra;
        params["actor"] = json!("stage4-reader");
        params["actor_token"] = json!(actor_token);
        if params.get("world_id").is_none() { params["world_id"] = json!(WORLD_A); }
        let denied = read(method, params).await;
        assert_eq!(denied["ok"], false, "{method}: {denied}");
    }
    let registration = read(
        "register_service_actor",
        json!({
            "profile": WORLD_READ_PROFILE,
            "name": WORLD_READ_ACTOR,
            "capabilities": ["semantic.read"],
            "worlds": [WORLD_A],
            "registrar_token": URL_SAFE_NO_PAD.encode([110u8; 32]),
        }),
    ).await;
    assert_eq!(registration["ok"], true, "{registration}");
    let token = registration["result"]["token"].as_str().unwrap();
    let visible = read("query_objects", json!({"actor": WORLD_READ_ACTOR, "actor_token": token})).await;
    assert_eq!(visible["ok"], true, "{visible}");
    let ids: Vec<&str> = visible["result"].as_array().unwrap().iter()
        .filter_map(|object| object["id"].as_str()).collect();
    assert!(ids.contains(&"center") && ids.contains(&"a-provider") && ids.contains(&"legacy-approved"));
    assert!(!ids.contains(&"legacy-private") && !ids.contains(&"foreign"));
    let allowed_legacy = read("get_object", json!({"actor": WORLD_READ_ACTOR, "actor_token": token, "id": "legacy-approved"})).await;
    assert_eq!(allowed_legacy["ok"], true, "{allowed_legacy}");
    let private_legacy = read("get_object", json!({"actor": WORLD_READ_ACTOR, "actor_token": token, "id": "legacy-private"})).await;
    assert_eq!(private_legacy["ok"], false, "{private_legacy}");
    let relations = read("query_relations", json!({"actor": WORLD_READ_ACTOR, "actor_token": token})).await;
    assert_eq!(relations["ok"], true, "{relations}");
    let relation_ids: Vec<&str> = relations["result"].as_array().unwrap().iter()
        .filter_map(|relation| relation["id"].as_str()).collect();
    assert!(relation_ids.contains(&"a-edge"));
    assert!(!relation_ids.contains(&"foreign-edge") && !relation_ids.contains(&"legacy-private-edge"));
    let events = read("query_events", json!({"actor": WORLD_READ_ACTOR, "actor_token": token})).await;
    assert_eq!(events["ok"], true, "{events}");
    assert!(events["result"].as_array().unwrap().iter().any(|event| event["event_type"] == "AEvent"));
    assert!(!events["result"].as_array().unwrap().iter().any(|event| event["event_type"] == "ForeignEvent" || event["event_type"] == "LegacyPrivate"));
    let foreign = read("get_world", json!({"actor": WORLD_READ_ACTOR, "actor_token": token, "world_id": WORLD_B})).await;
    assert_eq!(foreign["ok"], false, "{foreign}");
    let forged = read("query_objects", json!({"actor": "system", "actor_token": token, "world_id": WORLD_B})).await;
    assert_eq!(forged["ok"], false, "{forged}");
    let generic = read("query_objects", json!({"actor": WORLD_READ_ACTOR, "auth": state.core_token, "world_id": WORLD_A})).await;
    assert_eq!(generic["ok"], false, "{generic}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ipc_context_requires_bound_identity_and_exact_world() {
    let pipe = format!(r"\\.\pipe\spaceos-stage4-read-{}", uuid::Uuid::new_v4());
    let db = temp_db("ipc-auth");
    start_core_with_fixture(&pipe, &db).await;
    let token = register_reader(&pipe).await;

    let allowed = context_request(&pipe, "stage4-reader", &token, WORLD_A).await;
    assert_eq!(allowed["ok"], true, "{allowed}");
    assert_eq!(allowed["result"]["object"]["id"], "center");

    let no_cap_registration = ipc::send_request(
        &pipe,
        "register_agent",
        json!({
            "principal": "system",
            "principal_token": URL_SAFE_NO_PAD.encode([101u8; 32]),
            "name": "stage4-no-read-cap",
            "capabilities": [],
            "worlds": [WORLD_A],
        }),
    )
    .await
    .unwrap();
    assert_eq!(no_cap_registration["ok"], true, "{no_cap_registration}");
    let no_cap_token = no_cap_registration["result"]["token"].as_str().unwrap();
    let no_cap_read = context_request(&pipe, "stage4-no-read-cap", no_cap_token, WORLD_A).await;
    assert_eq!(no_cap_read["ok"], false, "{no_cap_read}");

    let wrong_world = context_request(&pipe, "stage4-reader", &token, WORLD_B).await;
    assert_eq!(wrong_world["ok"], false, "{wrong_world}");
    assert!(wrong_world["error"].as_str().unwrap().contains("denied"));

    let wrong_token = context_request(&pipe, "stage4-reader", "wrong", WORLD_A).await;
    assert_eq!(wrong_token["ok"], false, "{wrong_token}");

    let missing_token = context_request(&pipe, "stage4-reader", "", WORLD_A).await;
    assert_eq!(missing_token["ok"], false, "{missing_token}");

    let unknown_actor = context_request(&pipe, "unknown-reader", "none", WORLD_A).await;
    assert_eq!(unknown_actor["ok"], false, "{unknown_actor}");

    // Transport authentication alone no longer confers CLI identity.
    let generic_cli = context_request(&pipe, "cli", "", WORLD_A).await;
    assert_eq!(generic_cli["ok"], false, "{generic_cli}");
    let core_principal = ipc::send_request(
        &pipe,
        "get_object_context",
        json!({
            "actor":"cli", "principal_token":URL_SAFE_NO_PAD.encode([102u8; 32]),
            "query":{"object_id":"center","world_id":WORLD_A,"limit":10}
        }),
    )
    .await
    .unwrap();
    assert_eq!(core_principal["ok"], true, "{core_principal}");

    let malformed = ipc::send_request(
        &pipe,
        "get_object_context",
        json!({
            "actor": "stage4-reader",
            "actor_token": token,
            "query": {"object_id": "", "world_id": WORLD_A, "limit": 10}
        }),
    )
    .await
    .unwrap();
    assert_eq!(malformed["ok"], false, "{malformed}");
    assert!(malformed["error"].as_str().unwrap().contains("object_id"));
}
