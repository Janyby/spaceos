//! Four separate guarded processes: Core, Bridge, Filesystem, Browser.
//! No production endpoint, credential, database, CDP or filesystem root.
#![cfg(windows)]

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use futures_util::StreamExt;
use serde_json::{json, Value};
use space_core::{
    ipc,
    query::{EventQuery, ObjectQuery, RelationQuery},
    ActionRequest, Event, Object, Observation, Storage,
};
use std::io::Read;
use std::os::windows::process::CommandExt;
use std::{
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Fixture(Child);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn(binary: &str, vars: &[(&str, &str)]) -> Fixture {
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
        .stderr(Stdio::piped())
        .creation_flags(0x0800_0000);
    for (key, value) in vars {
        command.env(key, value);
    }
    Fixture(command.spawn().unwrap())
}

fn assert_running(fixture: &mut Fixture, name: &str) {
    std::thread::sleep(Duration::from_millis(200));
    if let Some(status) = fixture.0.try_wait().unwrap() {
        let mut stderr = String::new();
        if let Some(mut pipe) = fixture.0.stderr.take() {
            let _ = pipe.read_to_string(&mut stderr);
        }
        panic!("{name} exited {status}: {stderr}");
    }
}

fn assert_exited(fixture: &mut Fixture, name: &str) {
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        fixture.0.try_wait().unwrap().is_some(),
        "{name} did not fail closed"
    );
}

async fn wait_for<F, Fut>(stage: &str, mut probe: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if probe().await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timeout waiting for {stage}"));
}

fn file_observation(
    path: &str,
    instance_id: &str,
    instance_token: &str,
    principal_token: &str,
) -> Value {
    let mut object = Object::new("File", "filesystem", json!({"path":path}));
    object.external_id = Some(path.to_owned());
    let mut event = Event::new(
        "FileCreated",
        "filesystem",
        json!({"path":path,"object_id":object.id}),
    );
    event.provider_id = Some("filesystem".into());
    event.object_ids = vec![object.id.clone()];
    let obs = Observation {
        observation_id: uuid::Uuid::new_v4().to_string(),
        world_id: String::new(),
        facts: vec![],
        objects: vec![object],
        relations: vec![],
        delete_ids: vec![],
        event,
    };
    let mut value = serde_json::to_value(obs).unwrap();
    value["actor"] = json!("filesystem");
    value["principal_token"] = json!(principal_token);
    value["instance_id"] = json!(instance_id);
    value["instance_token"] = json!(instance_token);
    value
}

async fn read_world_a(pipe: &str, method: &str, mut params: Value, world: &str) -> Value {
    params["actor"] = json!("cli");
    params["principal_token"] = json!(URL_SAFE_NO_PAD.encode([111u8; 32]));
    params["world_id"] = json!(world);
    let response = ipc::send_request(pipe, method, params).await.unwrap();
    assert_eq!(response["ok"], true, "scoped fixture read {method}: {response}");
    response
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn guarded_four_process_runtime() {
    let started = Instant::now();
    assert_eq!(
        std::env::var("SPACEOS_TEST_ISOLATION_ROOT").unwrap(),
        r"C:\spaceos"
    );
    let root = r"C:\spaceos\test-tmp";
    let suffix = uuid::Uuid::new_v4();
    let fixture_dir = format!(r"{root}\full-{suffix}");
    let fs_root = format!(r"{fixture_dir}\files");
    let fs_root_b = format!(r"{fixture_dir}\files-b");
    std::fs::create_dir_all(&fs_root).unwrap();
    std::fs::create_dir_all(&fs_root_b).unwrap();
    std::fs::write(format!(r"{fs_root}\observed.txt"), b"synthetic fixture").unwrap();
    let db = format!(r"{fixture_dir}\core.db");
    let core_pipe = format!(r"\\.\pipe\spaceos-4-3a4-process-{suffix}");
    let fs_pipe = format!(r"\\.\pipe\spaceos-4-3a4-fs-{suffix}");
    let browser_pipe = format!(r"\\.\pipe\spaceos-4-3a4-browser-{suffix}");
    let fs_dispatch = URL_SAFE_NO_PAD.encode([95u8; 32]);
    let browser_dispatch = URL_SAFE_NO_PAD.encode([96u8; 32]);
    let fs_principal = URL_SAFE_NO_PAD.encode([92u8; 32]);
    let fs_instance = URL_SAFE_NO_PAD.encode([81u8; 32]);
    let fs_instance_b = URL_SAFE_NO_PAD.encode([82u8; 32]);
    let browser_principal = URL_SAFE_NO_PAD.encode([93u8; 32]);
    let system_principal = URL_SAFE_NO_PAD.encode([94u8; 32]);
    let world = "synthetic-process-world";

    let fake_cdp = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let cdp_port = fake_cdp.local_addr().unwrap().port();
    assert_ne!(cdp_port, 9222);
    assert_ne!(cdp_port, 8765);
    let cdp_url = format!("http://127.0.0.1:{cdp_port}");
    let cdp_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = fake_cdp.accept().await.unwrap();
            tokio::spawn(async move {
                let mut request = [0u8; 1024];
                let _ = stream.read(&mut request).await;
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n[]").await;
            });
        }
    });

    let spawn_core = |mode: &str| {
        spawn(
            env!("CARGO_BIN_EXE_isolated-core-fixture"),
            &[
                ("SPACEOS_FIXTURE_CORE_PIPE", &core_pipe),
                ("SPACEOS_FIXTURE_DB", &db),
                ("SPACEOS_FIXTURE_WORLD", world),
                ("SPACEOS_FIXTURE_FULL_STACK", "1"),
                ("SPACEOS_FIXTURE_FS_ROOT", &fs_root),
                ("SPACEOS_FIXTURE_FS_ROOT_B", &fs_root_b),
                ("SPACEOS_FIXTURE_BINDING_MODE", mode),
                ("SPACEOS_FILESYSTEM_PROVIDER_PIPE", &fs_pipe),
                ("SPACEOS_BROWSER_PROVIDER_PIPE", &browser_pipe),
                ("SPACEOS_DISPATCH_KEY_FILESYSTEM", &fs_dispatch),
                ("SPACEOS_DISPATCH_KEY_BROWSER", &browser_dispatch),
            ],
        )
    };
    let mut core = spawn_core("valid");
    assert_running(&mut core, "Core");
    wait_for("Core ping", || async {
        ipc::send_request(&core_pipe, "ping", json!({}))
            .await
            .is_ok()
    })
    .await;
    let core_ready_ms = started.elapsed().as_millis();

    let fs_exe = std::env::var("SPACEOS_FIXTURE_FS_EXE").unwrap();
    let browser_exe = std::env::var("SPACEOS_FIXTURE_BROWSER_EXE").unwrap();
    assert!(fs_exe.starts_with(r"C:\spaceos\target\debug\"));
    assert!(browser_exe.starts_with(r"C:\spaceos\target\debug\"));
    let mut missing_fs_credential = spawn(
        &fs_exe,
        &[
            ("SPACEOS_CORE_PIPE", &core_pipe),
            ("SPACEOS_FILESYSTEM_PROVIDER_PIPE", &fs_pipe),
            ("SPACEOS_DISPATCH_KEY_FILESYSTEM", &fs_dispatch),
            ("SPACEOS_ROOT", &fs_root),
        ],
    );
    assert_exited(&mut missing_fs_credential, "Filesystem without principal");
    let mut invalid_fs_credential = spawn(
        &fs_exe,
        &[
            ("SPACEOS_CORE_PIPE", &core_pipe),
            ("SPACEOS_FILESYSTEM_PROVIDER_PIPE", &fs_pipe),
            ("SPACEOS_PROVIDER_PRINCIPAL_TOKEN", "invalid"),
            ("SPACEOS_DISPATCH_KEY_FILESYSTEM", &fs_dispatch),
            ("SPACEOS_ROOT", &fs_root),
        ],
    );
    assert_exited(
        &mut invalid_fs_credential,
        "Filesystem with malformed principal",
    );
    let mut missing_browser_credential = spawn(
        &browser_exe,
        &[
            ("SPACEOS_CORE_PIPE", &core_pipe),
            ("SPACEOS_BROWSER_PROVIDER_PIPE", &browser_pipe),
            ("SPACEOS_DISPATCH_KEY_BROWSER", &browser_dispatch),
            ("SPACEOS_BROWSER_CDP_BASE", &cdp_url),
        ],
    );
    assert_exited(&mut missing_browser_credential, "Browser without principal");
    let filesystem = spawn(
        &fs_exe,
        &[
            ("SPACEOS_CORE_PIPE", &core_pipe),
            ("SPACEOS_FILESYSTEM_PROVIDER_PIPE", &fs_pipe),
            ("SPACEOS_PROVIDER_PRINCIPAL_TOKEN", &fs_principal),
            ("SPACEOS_PROVIDER_INSTANCE_ID", "fixture-fs-A"),
            ("SPACEOS_PROVIDER_INSTANCE_TOKEN", &fs_instance),
            ("SPACEOS_DISPATCH_KEY_FILESYSTEM", &fs_dispatch),
            ("SPACEOS_ROOT", &fs_root),
            ("SPACEOS_FIXTURE_DISABLE_PROCESS_OBSERVER", "1"),
        ],
    );
    let browser = spawn(
        &browser_exe,
        &[
            ("SPACEOS_CORE_PIPE", &core_pipe),
            ("SPACEOS_BROWSER_PROVIDER_PIPE", &browser_pipe),
            ("SPACEOS_PROVIDER_PRINCIPAL_TOKEN", &browser_principal),
            ("SPACEOS_DISPATCH_KEY_BROWSER", &browser_dispatch),
            ("SPACEOS_BROWSER_CDP_BASE", &cdp_url),
            ("SPACEOS_POLL_SECS", "1"),
        ],
    );
    wait_for("Provider registration", || async {
        let Ok(response) = ipc::send_request(&core_pipe, "list_providers", json!({})).await else {
            return false;
        };
        let providers = response["result"].as_array();
        providers.is_some_and(|items| {
            items.iter().any(|v| v["id"] == "filesystem")
                && items.iter().any(|v| v["id"] == "browser")
        })
    })
    .await;
    let providers_ready_ms = started.elapsed().as_millis();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_port = listener.local_addr().unwrap().port();
    drop(listener);
    assert_ne!(http_port, 8765);
    let port_text = http_port.to_string();
    let bridge = spawn(
        env!("CARGO_BIN_EXE_isolated-bridge-fixture"),
        &[
            ("SPACEOS_BRIDGE_CORE_PIPE", &core_pipe),
            ("SPACEOS_FIXTURE_HTTP_PORT", &port_text),
            ("SPACEOS_FIXTURE_WORLD", world),
        ],
    );
    let base = format!("http://127.0.0.1:{http_port}");
    let client = reqwest::Client::new();
    wait_for("Bridge context", || async {
        client
            .get(format!("{base}/api/context"))
            .send()
            .await
            .is_ok_and(|v| v.status().is_success())
    })
    .await;
    let bridge_ready_ms = started.elapsed().as_millis();
    let (mut websocket, _) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{http_port}/ws"))
            .await
            .unwrap();
    let _ = websocket.next().await.unwrap().unwrap();

    let create = |name: &'static str| {
        client.post(format!("{base}/api/semantic/objects"))
        .json(&json!({"world_id":world,"object_type":"Note","properties":{"name":name},"correlation_id":format!("full-{name}-{suffix}")}))
    };
    let a: Value = create("A").send().await.unwrap().json().await.unwrap();
    let b: Value = create("B").send().await.unwrap().json().await.unwrap();
    assert_eq!(a["ok"], true, "{a}");
    assert_eq!(b["ok"], true, "{b}");
    let aid = a["result"]["object"]["id"].as_str().unwrap();
    let bid = b["result"]["object"]["id"].as_str().unwrap();
    let relation: Value = client.post(format!("{base}/api/semantic/relations"))
        .json(&json!({"world_id":world,"source_id":aid,"target_id":bid,"relation_type":"related_to","properties":{},"correlation_id":format!("full-relation-{suffix}")}))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(relation["ok"], true, "{relation}");
    let patch: Value = client.patch(format!("{base}/api/semantic/objects/{aid}"))
        .json(&json!({"object_id":aid,"world_id":world,"properties_patch":{"name":"A patched"},"correlation_id":format!("full-patch-{suffix}")}))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(patch["ok"], true, "{patch}");
    let mut semantic_events = 0;
    tokio::time::timeout(Duration::from_secs(8), async {
        while semantic_events < 4 {
            let message = websocket.next().await.unwrap().unwrap();
            let value: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
            if value["type"] == "semantic_event" {
                semantic_events += 1;
            }
        }
    })
    .await
    .unwrap();

    // Watcher-originated FileCreated must traverse Core commit -> Bridge WS.
    let watched_path = format!(r"{fs_root}\watched.txt");
    std::fs::write(&watched_path, b"watcher fixture").unwrap();
    let observed_ws = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let message = websocket.next().await.unwrap().unwrap();
            let value: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
            if value["type"] == "semantic_event"
                && value["event"]["event_type"] == "FileCreated"
                && value["event"]["payload"]["path"] == watched_path
            {
                break value;
            }
        }
    })
    .await
    .expect("FileCreated WebSocket delivery");
    assert_eq!(observed_ws["event"]["world_id"], world);

    let world_b = "synthetic-process-world-b";
    let file_b = format!(r"{fs_root_b}\instance-b.txt");
    std::fs::write(&file_b, b"instance B fixture").unwrap();
    let unregistered_b = file_observation(&file_b, "fixture-fs-B", &fs_instance_b, &fs_principal);
    assert_eq!(
        ipc::send_request(&core_pipe, "report_observation", unregistered_b)
            .await
            .unwrap()["ok"],
        false
    );
    assert!(ipc::send_request(
        &core_pipe,
        "get_object_by_external",
        json!({"provider_id":"filesystem", "external_id":file_b})
    )
    .await
    .unwrap()["result"]
        .is_null());
    let register_b = ipc::send_request(
        &core_pipe,
        "register_provider_instance",
        json!({
            "actor":"filesystem", "principal_token":fs_principal,
            "instance_id":"fixture-fs-B", "instance_token":fs_instance_b
        }),
    )
    .await
    .unwrap();
    assert_eq!(register_b["ok"], true, "{register_b}");
    let forged_registration = ipc::send_request(
        &core_pipe,
        "register_provider_instance",
        json!({
            "actor":"filesystem", "principal_token":fs_principal,
            "instance_id":"fixture-fs-B", "instance_token":fs_instance
        }),
    )
    .await
    .unwrap();
    assert_eq!(forged_registration["ok"], false);
    let denied_before = Storage::open(&db).unwrap().query_events(&EventQuery::default()).unwrap().len();
    let mut forged = file_observation(&file_b, "fixture-fs-A", &fs_instance, &fs_principal);
    assert_eq!(
        ipc::send_request(&core_pipe, "report_observation", forged.clone())
            .await
            .unwrap()["ok"],
        false
    );
    forged["instance_id"] = json!("fixture-fs-B");
    assert_eq!(
        ipc::send_request(&core_pipe, "report_observation", forged.clone())
            .await
            .unwrap()["ok"],
        false
    );
    forged["instance_token"] = json!(fs_instance_b);
    forged["instance_id"] = json!("unknown-instance");
    assert_eq!(
        ipc::send_request(&core_pipe, "report_observation", forged.clone())
            .await
            .unwrap()["ok"],
        false
    );
    forged["instance_id"] = json!("fixture-fs-B");
    forged["principal_token"] = json!(std::env::var("SPACEOS_CORE_TOKEN").unwrap());
    assert_eq!(
        ipc::send_request(&core_pipe, "report_observation", forged)
            .await
            .unwrap()["ok"],
        false
    );
    let denied_after = Storage::open(&db).unwrap().query_events(&EventQuery::default()).unwrap().len();
    assert_eq!(
        denied_after, denied_before,
        "invalid observations wrote domain events"
    );
    let valid_b = file_observation(&file_b, "fixture-fs-B", &fs_instance_b, &fs_principal);
    let b_id = valid_b["objects"][0]["id"].as_str().unwrap().to_owned();
    let b_result = ipc::send_request(&core_pipe, "report_observation", valid_b)
        .await
        .unwrap();
    assert_eq!(b_result["ok"], true, "{b_result}");
    // The World-A read principal must not be granted World B merely for a
    // test assertion. Inspect disposable SQLite after Core committed instead.
    let persisted_b = Storage::open(&db).unwrap().get_object(&b_id).unwrap().unwrap();
    assert_eq!(persisted_b.properties["world_id"], world_b);

    let generic_bypass = ipc::send_request(
        &core_pipe,
        "register_agent",
        json!({
            "principal":"system","name":"forged","capabilities":["action.stat"],"worlds":[]
        }),
    )
    .await
    .unwrap();
    assert_eq!(generic_bypass["ok"], false);
    let registration = ipc::send_request(
        &core_pipe,
        "register_agent",
        json!({
            "principal":"system","principal_token":system_principal,"name":"fixture-actor",
            "capabilities":["action.stat","action.get_tabs"],"worlds":[]
        }),
    )
    .await
    .unwrap();
    assert_eq!(registration["ok"], true, "{registration}");
    let actor_token = registration["result"]["token"].as_str().unwrap();
    let mut stat = ActionRequest::new(
        "stat",
        json!({"path":format!(r"{fs_root}\observed.txt")}),
        "filesystem",
        "fixture-actor",
    );
    stat.actor_token = actor_token.to_string();
    let stat_result = ipc::send_request(&core_pipe, "request_action", json!(stat))
        .await
        .unwrap();
    assert_eq!(
        stat_result["result"]["status"], "Completed",
        "{stat_result}"
    );
    let mut tabs = ActionRequest::new("get_tabs", json!({}), "browser", "fixture-actor");
    tabs.actor_token = actor_token.to_string();
    let tabs_result = ipc::send_request(&core_pipe, "request_action", json!(tabs))
        .await
        .unwrap();
    assert_eq!(
        tabs_result["result"]["status"], "Completed",
        "{tabs_result}"
    );

    let old_fs_instance = ipc::send_request(&fs_pipe, "ping", json!({}))
        .await
        .unwrap()["result"]["instance_id"]
        .as_str()
        .unwrap()
        .to_string();
    let old_browser_instance = ipc::send_request(&browser_pipe, "ping", json!({}))
        .await
        .unwrap()["result"]["instance_id"]
        .as_str()
        .unwrap()
        .to_string();
    drop(filesystem);
    drop(browser);
    let filesystem = spawn(
        &fs_exe,
        &[
            ("SPACEOS_CORE_PIPE", &core_pipe),
            ("SPACEOS_FILESYSTEM_PROVIDER_PIPE", &fs_pipe),
            ("SPACEOS_PROVIDER_PRINCIPAL_TOKEN", &fs_principal),
            ("SPACEOS_PROVIDER_INSTANCE_ID", "fixture-fs-A"),
            ("SPACEOS_PROVIDER_INSTANCE_TOKEN", &fs_instance),
            ("SPACEOS_DISPATCH_KEY_FILESYSTEM", &fs_dispatch),
            ("SPACEOS_ROOT", &fs_root),
            ("SPACEOS_FIXTURE_DISABLE_PROCESS_OBSERVER", "1"),
        ],
    );
    let browser = spawn(
        &browser_exe,
        &[
            ("SPACEOS_CORE_PIPE", &core_pipe),
            ("SPACEOS_BROWSER_PROVIDER_PIPE", &browser_pipe),
            ("SPACEOS_PROVIDER_PRINCIPAL_TOKEN", &browser_principal),
            ("SPACEOS_DISPATCH_KEY_BROWSER", &browser_dispatch),
            ("SPACEOS_BROWSER_CDP_BASE", &cdp_url),
            ("SPACEOS_POLL_SECS", "1"),
        ],
    );
    wait_for("Filesystem restart", || async {
        ipc::send_request(&fs_pipe, "ping", json!({}))
            .await
            .ok()
            .and_then(|v| v["result"]["instance_id"].as_str().map(str::to_owned))
            .is_some_and(|id| id != old_fs_instance)
    })
    .await;
    wait_for("Browser restart", || async {
        ipc::send_request(&browser_pipe, "ping", json!({}))
            .await
            .ok()
            .and_then(|v| v["result"]["instance_id"].as_str().map(str::to_owned))
            .is_some_and(|id| id != old_browser_instance)
    })
    .await;
    let mut stat_after_restart = ActionRequest::new(
        "stat",
        json!({"path":format!(r"{fs_root}\observed.txt")}),
        "filesystem",
        "fixture-actor",
    );
    stat_after_restart.actor_token = actor_token.to_string();
    let stat_after_restart =
        ipc::send_request(&core_pipe, "request_action", json!(stat_after_restart))
            .await
            .unwrap();
    assert_eq!(
        stat_after_restart["result"]["status"], "Completed",
        "{stat_after_restart}"
    );
    let mut tabs_after_restart =
        ActionRequest::new("get_tabs", json!({}), "browser", "fixture-actor");
    tabs_after_restart.actor_token = actor_token.to_string();
    let tabs_after_restart =
        ipc::send_request(&core_pipe, "request_action", json!(tabs_after_restart))
            .await
            .unwrap();
    assert_eq!(
        tabs_after_restart["result"]["status"], "Completed",
        "{tabs_after_restart}"
    );

    let current_fs_instance = ipc::send_request(&fs_pipe, "ping", json!({}))
        .await
        .unwrap()["result"]["instance_id"]
        .as_str()
        .unwrap()
        .to_string();
    drop(filesystem);
    let wrong_dispatch = URL_SAFE_NO_PAD.encode([97u8; 32]);
    let mismatched_filesystem = spawn(
        &fs_exe,
        &[
            ("SPACEOS_CORE_PIPE", &core_pipe),
            ("SPACEOS_FILESYSTEM_PROVIDER_PIPE", &fs_pipe),
            ("SPACEOS_PROVIDER_PRINCIPAL_TOKEN", &fs_principal),
            ("SPACEOS_PROVIDER_INSTANCE_ID", "fixture-fs-A"),
            ("SPACEOS_PROVIDER_INSTANCE_TOKEN", &fs_instance),
            ("SPACEOS_DISPATCH_KEY_FILESYSTEM", &wrong_dispatch),
            ("SPACEOS_ROOT", &fs_root),
            ("SPACEOS_FIXTURE_DISABLE_PROCESS_OBSERVER", "1"),
        ],
    );
    wait_for("mismatched Filesystem startup", || async {
        ipc::send_request(&fs_pipe, "ping", json!({}))
            .await
            .ok()
            .and_then(|v| v["result"]["instance_id"].as_str().map(str::to_owned))
            .is_some_and(|id| id != current_fs_instance)
    })
    .await;
    let mut mismatched_stat = ActionRequest::new(
        "stat",
        json!({"path":format!(r"{fs_root}\observed.txt")}),
        "filesystem",
        "fixture-actor",
    );
    mismatched_stat.actor_token = actor_token.to_string();
    let mismatched_result = ipc::send_request(&core_pipe, "request_action", json!(mismatched_stat))
        .await
        .unwrap();
    assert_eq!(
        mismatched_result["result"]["status"], "Failed",
        "{mismatched_result}"
    );
    drop(mismatched_filesystem);
    let filesystem = spawn(
        &fs_exe,
        &[
            ("SPACEOS_CORE_PIPE", &core_pipe),
            ("SPACEOS_FILESYSTEM_PROVIDER_PIPE", &fs_pipe),
            ("SPACEOS_PROVIDER_PRINCIPAL_TOKEN", &fs_principal),
            ("SPACEOS_PROVIDER_INSTANCE_ID", "fixture-fs-A"),
            ("SPACEOS_PROVIDER_INSTANCE_TOKEN", &fs_instance),
            ("SPACEOS_DISPATCH_KEY_FILESYSTEM", &fs_dispatch),
            ("SPACEOS_ROOT", &fs_root),
            ("SPACEOS_FIXTURE_DISABLE_PROCESS_OBSERVER", "1"),
        ],
    );
    wait_for("Filesystem key recovery", || async {
        ipc::send_request(&fs_pipe, "ping", json!({}))
            .await
            .ok()
            .and_then(|v| v["result"]["instance_id"].as_str().map(str::to_owned))
            .is_some_and(|id| id != current_fs_instance)
    })
    .await;
    let mut recovered_stat = ActionRequest::new(
        "stat",
        json!({"path":format!(r"{fs_root}\observed.txt")}),
        "filesystem",
        "fixture-actor",
    );
    recovered_stat.actor_token = actor_token.to_string();
    let recovered_result = ipc::send_request(&core_pipe, "request_action", json!(recovered_stat))
        .await
        .unwrap();
    assert_eq!(
        recovered_result["result"]["status"], "Completed",
        "{recovered_result}"
    );

    let direct = ipc::send_request(
        &fs_pipe,
        "action",
        json!(ActionRequest::new(
            "stat",
            json!({"path":format!(r"{fs_root}\observed.txt")}),
            "filesystem",
            "fixture-actor"
        )),
    )
    .await
    .unwrap();
    assert_eq!(direct["ok"], false);
    let forged_observation = ipc::send_request(&core_pipe, "report_observation", json!({"actor":"filesystem","observation_id":"forged","world_id":"","facts":[],"objects":[],"relations":[],"delete_ids":[],"event":{"id":"forged"}})).await.unwrap();
    assert_eq!(forged_observation["ok"], false);
    let events = read_world_a(&core_pipe, "query_events", json!({"limit":100}), world).await;
    // ActionCompleted may be unscoped system telemetry and must not leak to
    // this World-A reader. Check durable SQLite separately below.
    assert!(events["result"].as_array().is_some_and(|items| items
        .iter()
        .any(|e| e["event_type"] == "FileCreated" && e["world_id"] == world)));
    assert!(Storage::open(&db).unwrap().query_events(&EventQuery::default()).unwrap()
        .iter().any(|e| e.event_type == "ActionCompleted"));
    let objects = read_world_a(&core_pipe, "query_objects", json!({}), world).await;
    assert!(objects["result"]
        .as_array()
        .is_some_and(|items| items.iter().any(|o| o["id"] == aid)));
    assert!(std::fs::metadata(&db).is_ok());

    // Actual Core process restart: registration is ephemeral, while the
    // trusted configuration and immutable authority pin are reconstructed.
    drop(core);
    core = spawn_core("valid");
    wait_for("restarted Core", || async {
        ipc::send_request(&core_pipe, "ping", json!({}))
            .await
            .is_ok()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1600)).await;
    let restart_path = format!(r"{fs_root}\after-core-restart.txt");
    std::fs::write(&restart_path, b"post-restart fixture").unwrap();
    wait_for(
        "FileCreated after Core restart and Provider re-registration",
        || async {
            Storage::open(&db).ok().and_then(|storage|
                storage.get_object_by_external("filesystem", &restart_path).ok().flatten()
            ).is_some_and(|object| object.properties["world_id"] == world)
        },
    )
    .await;
    let invalid_reregistration = ipc::send_request(
        &core_pipe,
        "register_provider_instance",
        json!({
            "actor":"filesystem", "principal_token":fs_principal,
            "instance_id":"fixture-fs-A", "instance_token":URL_SAFE_NO_PAD.encode([83u8;32])
        }),
    )
    .await
    .unwrap();
    assert_eq!(invalid_reregistration["ok"], false);
    println!("STAGE4_3A6_RESTART valid=true provider_reregistration=true world={world}");

    drop(core);
    core = spawn_core("missing");
    wait_for("Core without A binding", || async {
        ipc::send_request(&core_pipe, "ping", json!({}))
            .await
            .is_ok()
    })
    .await;
    let missing_path = format!(r"{fs_root}\missing-binding.txt");
    std::fs::write(&missing_path, b"missing binding fixture").unwrap();
    let missing_request =
        file_observation(&missing_path, "fixture-fs-A", &fs_instance, &fs_principal);
    assert_eq!(
        ipc::send_request(&core_pipe, "report_observation", missing_request)
            .await
            .unwrap()["ok"],
        false
    );
    let missing_row = ipc::send_request(
        &core_pipe,
        "get_object_by_external",
        json!({"provider_id":"filesystem", "external_id":missing_path}),
    )
    .await
    .unwrap();
    assert!(missing_row["result"].is_null());
    println!("STAGE4_3A6_RESTART missing_binding_denied=true");

    drop(core);
    core = spawn_core("changed");
    wait_for("Core with changed A binding", || async {
        ipc::send_request(&core_pipe, "ping", json!({}))
            .await
            .is_ok()
    })
    .await;
    let changed_registration = ipc::send_request(
        &core_pipe,
        "register_provider_instance",
        json!({
            "actor":"filesystem", "principal_token":fs_principal,
            "instance_id":"fixture-fs-A", "instance_token":fs_instance
        }),
    )
    .await
    .unwrap();
    assert_eq!(changed_registration["ok"], false, "{changed_registration}");
    let changed_path = format!(r"{fs_root}\changed-binding.txt");
    std::fs::write(&changed_path, b"changed binding fixture").unwrap();
    let changed_request =
        file_observation(&changed_path, "fixture-fs-A", &fs_instance, &fs_principal);
    assert_eq!(
        ipc::send_request(&core_pipe, "report_observation", changed_request)
            .await
            .unwrap()["ok"],
        false
    );
    assert!(ipc::send_request(
        &core_pipe,
        "get_object_by_external",
        json!({"provider_id":"filesystem", "external_id":changed_path})
    )
    .await
    .unwrap()["result"]
        .is_null());
    println!("STAGE4_3A6_RESTART changed_binding_denied=true");

    drop(core);
    core = spawn_core("rotated");
    wait_for("Core with rotated A credential", || async {
        ipc::send_request(&core_pipe, "ping", json!({}))
            .await
            .is_ok()
    })
    .await;
    let old_credential = ipc::send_request(
        &core_pipe,
        "register_provider_instance",
        json!({
            "actor":"filesystem", "principal_token":fs_principal,
            "instance_id":"fixture-fs-A", "instance_token":fs_instance
        }),
    )
    .await
    .unwrap();
    assert_eq!(old_credential["ok"], false);
    let rotated_token = URL_SAFE_NO_PAD.encode([84u8; 32]);
    let rotated_registration = ipc::send_request(
        &core_pipe,
        "register_provider_instance",
        json!({
            "actor":"filesystem", "principal_token":fs_principal,
            "instance_id":"fixture-fs-A", "instance_token":rotated_token
        }),
    )
    .await
    .unwrap();
    assert_eq!(rotated_registration["ok"], true, "{rotated_registration}");
    let rotated_path = format!(r"{fs_root}\rotated-credential.txt");
    std::fs::write(&rotated_path, b"rotated credential fixture").unwrap();
    let old_observation =
        file_observation(&rotated_path, "fixture-fs-A", &fs_instance, &fs_principal);
    assert_eq!(
        ipc::send_request(&core_pipe, "report_observation", old_observation)
            .await
            .unwrap()["ok"],
        false
    );
    let rotated_observation =
        file_observation(&rotated_path, "fixture-fs-A", &rotated_token, &fs_principal);
    assert_eq!(
        ipc::send_request(&core_pipe, "report_observation", rotated_observation)
            .await
            .unwrap()["ok"],
        true
    );
    println!("STAGE4_3A6_RESTART old_credential_denied=true rotated_credential_accepted=true");

    drop(core);
    core = spawn_core("valid");
    wait_for("restored Core", || async {
        ipc::send_request(&core_pipe, "ping", json!({}))
            .await
            .is_ok()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1600)).await;
    let restored_path = format!(r"{fs_root}\restored-binding.txt");
    std::fs::write(&restored_path, b"restored binding fixture").unwrap();
    wait_for("restored A binding FileCreated", || async {
        Storage::open(&db).ok().and_then(|storage|
            storage.get_object_by_external("filesystem", &restored_path).ok().flatten()
        ).is_some_and(|object| object.properties["world_id"] == world)
    })
    .await;

    drop(bridge);
    drop(browser);
    drop(filesystem);
    drop(core);
    cdp_task.abort();
    let storage = Storage::open(&db).unwrap();
    let saved_a = storage.get_object(aid).unwrap().unwrap();
    assert_eq!(saved_a.properties["name"], "A patched");
    let saved_objects = storage.query_objects(&ObjectQuery::default()).unwrap();
    let observed_path = format!(r"{fs_root}\observed.txt");
    assert!(saved_objects
        .iter()
        .any(|o| o.external_id.as_deref() == Some(observed_path.as_str())
            && o.properties["world_id"] == world
            && o.provider_id == "filesystem"));
    let saved_relations = storage.query_relations(&RelationQuery::default()).unwrap();
    assert!(saved_relations.iter().any(|r| r.source_object_id == aid
        && r.target_object_id == bid
        && r.relation_type == "related_to"));
    let saved_events = storage.query_events(&EventQuery::default()).unwrap();
    assert!(saved_events
        .iter()
        .any(|e| e.event_type == "FileCreated" && e.world_id == world));
    let file_count = saved_objects
        .iter()
        .filter(|o| {
            o.provider_id == "filesystem" && o.r#type == "File" && o.properties["world_id"] == world
        })
        .count();
    let file_event_count = saved_events
        .iter()
        .filter(|e| {
            e.event_type == "FileCreated"
                && e.world_id == world
                && e.provider_id.as_deref() == Some("filesystem")
        })
        .count();
    assert_eq!(file_count, 5, "unexpected duplicate or missing File object");
    assert_eq!(
        file_event_count, 5,
        "unexpected duplicate or missing FileCreated event"
    );
    assert_eq!(
        saved_objects
            .iter()
            .filter(|o| o.provider_id == "filesystem"
                && o.r#type == "File"
                && o.properties["world_id"] == world_b)
            .count(),
        1
    );
    assert!(
        saved_events
            .iter()
            .filter(|e| e.actor == "space-world-user")
            .count()
            >= 4
    );
    println!(
        "STAGE4_3A5_SQLITE world={world} file_objects={file_count} file_events={file_event_count} ws_filecreated=true"
    );
    println!(
        "STAGE4_3A5_TIMING core_ms={core_ready_ms} providers_ms={providers_ready_ms} bridge_ms={bridge_ready_ms} total_ms={}",
        started.elapsed().as_millis()
    );
}
