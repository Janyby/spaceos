//! Trust boundary adversarial tests. These use a raw client that does NOT
//! inject the core token, unlike space_core::ipc::send_request which is for
//! trusted local clients.
use serde_json::json;
use space_core::{ipc, Storage};

async fn raw_call(pipe: &str, method: &str, params: serde_json::Value) -> serde_json::Value {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    #[cfg(windows)]
    {
        let stream = tokio::net::windows::named_pipe::ClientOptions::new()
            .open(pipe)
            .unwrap();
        let (r, mut w) = tokio::io::split(stream);
        let req = json!({"method": method, "params": params});
        let mut s = serde_json::to_string(&req).unwrap();
        s.push('\n');
        w.write_all(s.as_bytes()).await.unwrap();
        w.flush().await.unwrap();
        let mut lines = BufReader::new(r).lines();
        let l = lines.next_line().await.unwrap().unwrap_or_default();
        return serde_json::from_str(&l).unwrap();
    }
    #[cfg(not(windows))]
    {
        let stream = tokio::net::TcpStream::connect("127.0.0.1:7777")
            .await
            .unwrap();
        let (r, mut w) = stream.into_split();
        let req = json!({"method": method, "params": params});
        let mut s = serde_json::to_string(&req).unwrap();
        s.push('\n');
        w.write_all(s.as_bytes()).await.unwrap();
        w.flush().await.unwrap();
        let mut lines = BufReader::new(r).lines();
        let l = lines.next_line().await.unwrap().unwrap_or_default();
        return serde_json::from_str(&l).unwrap();
    }
}

async fn start_core(pipe: &str) {
    let db = std::env::temp_dir().join("spaceos-v13");
    std::fs::create_dir_all(&db).unwrap();
    let fname = db.join(format!("{}.db", pipe.replace('\\', "_").replace(':', "_")));
    let _ = std::fs::remove_file(&fname);
    let storage = Storage::open(&fname).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    tokio::spawn(async move {
        let _ = ipc::run_server(s2, &p).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fake_system_principal_denied_on_register_agent() {
    start_core(r"\\.\pipe\spaceos-v13-sys").await;
    let r = raw_call(
        r"\\.\pipe\spaceos-v13-sys",
        "register_agent",
        json!({"principal":"system","name":"evil","capabilities":["*"]}),
    )
    .await;
    assert_eq!(r["ok"], false);
    assert!(r["error"].as_str().unwrap().contains("denied"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fake_filesystem_principal_denied_on_register_provider() {
    start_core(r"\\.\pipe\spaceos-v13-prov").await;
    let r = raw_call(
        r"\\.\pipe\spaceos-v13-prov",
        "register_provider",
        json!({"id":"evil","name":"evil","capabilities":[]}),
    )
    .await;
    assert_eq!(r["ok"], false);
    assert!(r["error"].as_str().unwrap().contains("denied"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fake_agent_with_system_principal_denied() {
    start_core(r"\\.\pipe\spaceos-v13-agent").await;
    let r = raw_call(
        r"\\.\pipe\spaceos-v13-agent",
        "register_agent",
        json!({"principal":"system","name":"victim","capabilities":["object.create"]}),
    )
    .await;
    assert_eq!(r["ok"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_spoof_denied() {
    start_core(r"\\.\pipe\spaceos-v13-spoof").await;
    // register a trusted agent first via a legitimate client; raw must fail afterwards
    let ok = ipc::send_request(
        r"\\.\pipe\spaceos-v13-spoof",
        "register_agent",
        json!({"principal":"system","name":"trusted","capabilities":["object.create"]}),
    )
    .await
    .unwrap();
    assert_eq!(ok["ok"], true);
    let spoof = raw_call(r"\\.\pipe\spaceos-v13-spoof", "create_object", json!({"id": uuid::Uuid::new_v4().to_string(), "type":"Project","properties":{"name":"X"},"provider_id":"trusted","external_id":null,"created_at": chrono::Utc::now().to_rfc3339(), "updated_at": chrono::Utc::now().to_rfc3339()})).await;
    assert_eq!(spoof["ok"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capability_spoof_denied() {
    start_core(r"\\.\pipe\spaceos-v13-cap").await;
    // try to register a fake agent with all capabilities using raw system principal
    let r = raw_call(r"\\.\pipe\spaceos-v13-cap", "register_agent", json!({"principal":"system","name":"fake","capabilities":["object.create","object.delete","event.record"]})).await;
    assert_eq!(r["ok"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_agent_impersonation_denied_without_auth() {
    start_core(r"\\.\pipe\spaceos-v13-xagent").await;
    let ok = ipc::send_request(
        r"\\.\pipe\spaceos-v13-xagent",
        "register_agent",
        json!({"principal":"system","name":"real","capabilities":["object.create"]}),
    )
    .await
    .unwrap();
    assert_eq!(ok["ok"], true);
    // raw caller claiming real but not providing auth is denied
    let r = raw_call(r"\\.\pipe\spaceos-v13-xagent", "create_object", json!({"id": uuid::Uuid::new_v4().to_string(), "type":"Project","properties":{},"provider_id":"real","external_id":null,"created_at": chrono::Utc::now().to_rfc3339(), "updated_at": chrono::Utc::now().to_rfc3339()})).await;
    assert_eq!(r["ok"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unauthorized_agent_registration_denied() {
    start_core(r"\\.\pipe\spaceos-v13-reg").await;
    let r = raw_call(
        r"\\.\pipe\spaceos-v13-reg",
        "register_agent",
        json!({"principal":"cli","name":"cliuser","capabilities":["object.create"]}),
    )
    .await;
    assert_eq!(r["ok"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unauthorized_provider_registration_denied() {
    start_core(r"\\.\pipe\spaceos-v13-reg2").await;
    let r = raw_call(
        r"\\.\pipe\spaceos-v13-reg2",
        "register_provider",
        json!({"id":"p","name":"p","capabilities":[]}),
    )
    .await;
    assert_eq!(r["ok"], false);
}
