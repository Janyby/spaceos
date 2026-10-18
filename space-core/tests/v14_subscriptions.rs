//! V1.4 subscription semantics: auth, isolation, filtering, lifecycle.
//! Filtering is enforced server-side BEFORE delivery; tests assert what
//! actually arrives on the subscriber pipe, never post-hoc filtering.
//! Windows-only: subscriptions ride named-pipe connections.
#![cfg(windows)]
use serde_json::{json, Value};
use space_core::{ipc, Storage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp_db(pipe: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-v14");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{}.db", pipe.replace('\\', "_").replace(':', "_")))
}

async fn start_core(pipe: &str) {
    let db = tmp_db(pipe);
    let _ = std::fs::remove_file(&db);
    let storage = Storage::open(&db).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    tokio::spawn(async move {
        let _ = ipc::run_server(s2, &p).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
}

async fn call(pipe: &str, method: &str, params: Value) -> Value {
    ipc::send_request(pipe, method, params).await.unwrap()
}

async fn call_retry(pipe: &str, method: &str, params: Value) -> Value {
    for _ in 0..60 {
        match ipc::send_request(pipe, method, params.clone()).await {
            Ok(v) => return v,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    }
    call(pipe, method, params).await
}

async fn raw_call(pipe: &str, method: &str, params: Value) -> Value {
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
    serde_json::from_str(&l).unwrap()
}

async fn register(pipe: &str, name: &str, caps: &[&str], worlds: &[&str]) -> String {
    let r = call(
        pipe,
        "register_agent",
        json!({"principal":"system","name":name,"capabilities":caps,"worlds":worlds}),
    )
    .await;
    assert_eq!(r["ok"], true, "register {name}");
    r["result"]["token"].as_str().unwrap().to_string()
}

fn with_token(mut p: Value, tok: &str) -> Value {
    p["actor_token"] = json!(tok);
    p
}

fn agent_event(event_type: &str, actor: &str, world: &str, objects: Vec<String>) -> Value {
    let mut e = space_core::Event::new(event_type, actor, json!({}));
    e.world_id = world.to_string();
    e.object_ids = objects;
    json!(e)
}

struct SubConn {
    lines: tokio::io::Lines<
        BufReader<tokio::io::ReadHalf<tokio::net::windows::named_pipe::NamedPipeClient>>,
    >,
    sub_id: String,
}

async fn subscribe(pipe: &str, params: Value) -> (SubConn, Value) {
    let stream = tokio::net::windows::named_pipe::ClientOptions::new()
        .open(pipe)
        .unwrap();
    let (r, mut w) = tokio::io::split(stream);
    let req = json!({"method": "subscribe", "params": params});
    let mut s = serde_json::to_string(&req).unwrap();
    s.push('\n');
    w.write_all(s.as_bytes()).await.unwrap();
    w.flush().await.unwrap();
    let mut lines = BufReader::new(r).lines();
    let ack = lines.next_line().await.unwrap().unwrap_or_default();
    let ack: Value = serde_json::from_str(&ack).unwrap();
    let sub_id = ack["result"]["subscription_id"]
        .as_str()
        .unwrap_or("")
        .to_string();
    (SubConn { lines, sub_id }, ack)
}

async fn recv_event(conn: &mut SubConn, ms: u64) -> Option<Value> {
    match tokio::time::timeout(std::time::Duration::from_millis(ms), conn.lines.next_line()).await {
        Ok(Ok(Some(l))) => serde_json::from_str(&l).ok(),
        _ => None,
    }
}

// ---- 1. authenticated subscriber registration ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_subscriber_registration() {
    let pipe = r"\\.\pipe\spaceos-v14-sub1";
    start_core(pipe).await;
    let tok = register(pipe, "sub-a", &["event.record"], &["WA"]).await;
    let params = with_token(json!({"subscriber":"sub-a","world_id":"WA"}), &tok);
    let (_c, ack) = subscribe(pipe, params).await;
    assert_eq!(ack["ok"], true);
    assert!(ack["result"]["subscription_id"].as_str().is_some());
}

// ---- 2. subscriber spoof denied ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_spoof_denied() {
    let pipe = r"\\.\pipe\spaceos-v14-sub2";
    start_core(pipe).await;
    // unknown name, no token
    let r = raw_call(pipe, "subscribe", json!({"subscriber":"ghost"})).await;
    // raw subscribe gets an ack-or-error line; error expected
    assert_eq!(r["ok"], false);
    // registered name, wrong token
    let _tok = register(pipe, "real-a", &["event.record"], &["WA"]).await;
    let r2 = raw_call(
        pipe,
        "subscribe",
        json!({"subscriber":"real-a","world_id":"WA","actor_token":"wrong"}),
    )
    .await;
    assert_eq!(r2["ok"], false);
}

// ---- 3. cross-agent subscription denied ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_agent_subscription_denied() {
    let pipe = r"\\.\pipe\spaceos-v14-sub3";
    start_core(pipe).await;
    let tok_a = register(pipe, "agent-a", &["event.record"], &["WA"]).await;
    let _tok_b = register(pipe, "agent-b", &["event.record"], &["WB"]).await;
    // A tries to subscribe AS B with A's token
    let r = raw_call(
        pipe,
        "subscribe",
        json!({"subscriber":"agent-b","world_id":"WB","actor_token": tok_a}),
    )
    .await;
    assert_eq!(r["ok"], false);
}

// ---- 4. unauthorized cross-world subscription denied ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unauthorized_cross_world_subscription_denied() {
    let pipe = r"\\.\pipe\spaceos-v14-sub4";
    start_core(pipe).await;
    let tok = register(pipe, "scoped-a", &["event.record"], &["WA"]).await;
    let r = raw_call(
        pipe,
        "subscribe",
        json!({"subscriber":"scoped-a","world_id":"WB","actor_token": tok}),
    )
    .await;
    assert_eq!(r["ok"], false);
    // audit event recorded
    let evs = call(
        pipe,
        "query_events",
        json!({"event_type":"SubscriptionDenied"}),
    )
    .await;
    assert!(!evs["result"].as_array().unwrap().is_empty());
}

// ---- 5/6. world-scoped subscription: A gets, B does not ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn world_scoped_subscription_isolation() {
    let pipe = r"\\.\pipe\spaceos-v14-sub56";
    start_core(pipe).await;
    let tok_a = register(pipe, "w-a", &["event.record"], &["WA"]).await;
    let tok_b = register(pipe, "w-b", &["event.record"], &["WB"]).await;
    let (mut ca, ack_a) = subscribe(
        pipe,
        with_token(json!({"subscriber":"w-a","world_id":"WA"}), &tok_a),
    )
    .await;
    let (mut cb, ack_b) = subscribe(
        pipe,
        with_token(json!({"subscriber":"w-b","world_id":"WB"}), &tok_b),
    )
    .await;
    assert_eq!(ack_a["ok"], true);
    assert_eq!(ack_b["ok"], true);

    let ev = agent_event("ObjectCreated", "w-a", "WA", vec![]);
    call(pipe, "record_event", with_token(ev, &tok_a)).await;

    let got_a = recv_event(&mut ca, 2000).await;
    assert!(got_a.is_some(), "subscriber A must receive WA event");
    assert_eq!(got_a.unwrap()["event"]["world_id"], "WA");
    // B must receive NOTHING: server-side filter, zero bytes on its pipe.
    let got_b = recv_event(&mut cb, 800).await;
    assert!(got_b.is_none(), "subscriber B must NOT receive WA event");
}

// ---- 7. world_id integrity action -> event -> subscriber ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn world_id_integrity() {
    let pipe = r"\\.\pipe\spaceos-v14-sub7";
    start_core(pipe).await;
    let tok = register(pipe, "integ-a", &["event.record"], &["W1"]).await;
    let (mut c, _) = subscribe(
        pipe,
        with_token(json!({"subscriber":"integ-a","world_id":"W1"}), &tok),
    )
    .await;
    let mut ev = space_core::Event::new("ActionCompleted", "integ-a", json!({"x":1}));
    ev.world_id = "W1".into();
    ev.correlation_id = Some("corr-1".into());
    call(pipe, "record_event", with_token(json!(ev), &tok)).await;
    let got = recv_event(&mut c, 2000).await.expect("event");
    assert_eq!(got["event"]["world_id"], "W1");
    assert_eq!(got["event"]["actor"], "integ-a");
    assert_eq!(got["event"]["correlation_id"], "corr-1");
}

// ---- 8. mixed-world isolation: only matching events arrive ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_world_subscriber_isolation() {
    let pipe = r"\\.\pipe\spaceos-v14-sub8";
    start_core(pipe).await;
    let tok_a = register(pipe, "m-a", &["event.record"], &["WA"]).await;
    let tok_b = register(pipe, "m-b", &["event.record"], &["WB"]).await;
    let (mut ca, _) = subscribe(
        pipe,
        with_token(json!({"subscriber":"m-a","world_id":"WA"}), &tok_a),
    )
    .await;
    for (actor, tok, w) in [("m-a", tok_a.as_str(), "WA"), ("m-b", tok_b.as_str(), "WB")] {
        let ev = agent_event("ObjectCreated", actor, w, vec![]);
        call(pipe, "record_event", with_token(ev, tok)).await;
    }
    let first = recv_event(&mut ca, 2000).await.expect("one event");
    assert_eq!(first["event"]["world_id"], "WA");
    let second = recv_event(&mut ca, 600).await;
    assert!(second.is_none(), "no WB event may reach WA subscriber");
}

// ---- 9. event-type filter ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_type_filter() {
    let pipe = r"\\.\pipe\spaceos-v14-sub9";
    start_core(pipe).await;
    let tok = register(pipe, "t-a", &["event.record"], &["W"]).await;
    let (mut c, _) = subscribe(
        pipe,
        with_token(
            json!({"subscriber":"t-a","world_id":"W","event_types":["FileChanged"]}),
            &tok,
        ),
    )
    .await;
    call(
        pipe,
        "record_event",
        with_token(agent_event("ObjectCreated", "t-a", "W", vec![]), &tok),
    )
    .await;
    call(
        pipe,
        "record_event",
        with_token(agent_event("RelationCreated", "t-a", "W", vec![]), &tok),
    )
    .await;
    call(
        pipe,
        "record_event",
        with_token(agent_event("FileChanged", "t-a", "W", vec![]), &tok),
    )
    .await;
    let got = recv_event(&mut c, 2000).await.expect("FileChanged");
    assert_eq!(got["event"]["event_type"], "FileChanged");
    assert!(recv_event(&mut c, 600).await.is_none());
}

// ---- 10. object filter ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn object_filter() {
    let pipe = r"\\.\pipe\spaceos-v14-sub10";
    start_core(pipe).await;
    let tok = register(pipe, "o-a", &["event.record"], &["W"]).await;
    let (mut c, _) = subscribe(
        pipe,
        with_token(
            json!({"subscriber":"o-a","world_id":"W","object_ids":["OBJ-X"]}),
            &tok,
        ),
    )
    .await;
    call(
        pipe,
        "record_event",
        with_token(
            agent_event("FileChanged", "o-a", "W", vec!["OBJ-Y".into()]),
            &tok,
        ),
    )
    .await;
    call(
        pipe,
        "record_event",
        with_token(
            agent_event("FileChanged", "o-a", "W", vec!["OBJ-X".into()]),
            &tok,
        ),
    )
    .await;
    let got = recv_event(&mut c, 2000).await.expect("OBJ-X event");
    assert_eq!(got["event"]["object_ids"][0], "OBJ-X");
    assert!(recv_event(&mut c, 600).await.is_none());
}

// ---- 11. combined world + type + object filter ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn combined_filter() {
    let pipe = r"\\.\pipe\spaceos-v14-sub11";
    start_core(pipe).await;
    let tok = register(pipe, "c-a", &["event.record"], &["W"]).await;
    let (mut c, _) = subscribe(
        pipe,
        with_token(
            json!({"subscriber":"c-a","world_id":"W","event_types":["FileChanged"],"object_ids":["OX"]}),
            &tok,
        ),
    )
    .await;
    // wrong type
    call(
        pipe,
        "record_event",
        with_token(
            agent_event("ObjectCreated", "c-a", "W", vec!["OX".into()]),
            &tok,
        ),
    )
    .await;
    // right type, wrong object
    call(
        pipe,
        "record_event",
        with_token(
            agent_event("FileChanged", "c-a", "W", vec!["OY".into()]),
            &tok,
        ),
    )
    .await;
    // match
    call(
        pipe,
        "record_event",
        with_token(
            agent_event("FileChanged", "c-a", "W", vec!["OX".into()]),
            &tok,
        ),
    )
    .await;
    let got = recv_event(&mut c, 2000).await.expect("matching event");
    assert_eq!(got["event"]["event_type"], "FileChanged");
    assert_eq!(got["event"]["object_ids"][0], "OX");
    assert!(recv_event(&mut c, 600).await.is_none());
}

// ---- concurrency: 5 worlds x agents x subscriptions ----
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_worlds_agents_subscriptions() {
    let pipe = r"\\.\pipe\spaceos-v14-conc5";
    start_core(pipe).await;
    let mut conns = vec![];
    let mut toks = vec![];
    for w in 0..5 {
        let name = format!("conc-a-{w}");
        let world = format!("CW{w}");
        let tok = register(pipe, &name, &["event.record"], &[world.as_str()]).await;
        let (c, ack) = subscribe(
            pipe,
            with_token(json!({"subscriber": name, "world_id": world}), &tok),
        )
        .await;
        assert_eq!(ack["ok"], true);
        conns.push(c);
        toks.push((name, world, tok));
    }
    // all agents record concurrently into their own worlds
    let mut handles = vec![];
    for (name, world, tok) in toks.clone() {
        let p = pipe.to_string();
        handles.push(tokio::spawn(async move {
            let ev = agent_event("ObjectCreated", &name, &world, vec![]);
            call_retry(&p, "record_event", with_token(ev, &tok)).await
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    // each subscriber gets exactly its own world's event, nothing else
    for ((name, world, _), conn) in toks.iter().zip(conns.iter_mut()) {
        let got = recv_event(conn, 3000)
            .await
            .unwrap_or_else(|| panic!("{name} must receive its event"));
        assert_eq!(got["event"]["world_id"], world.as_str(), "{name} isolation");
        assert!(
            recv_event(conn, 400).await.is_none(),
            "{name} gets nothing else"
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscription_cancel_stops_delivery() {
    let pipe = r"\\.\pipe\spaceos-v14-sub1213";
    start_core(pipe).await;
    let tok = register(pipe, "x-a", &["event.record"], &["W"]).await;
    let (mut c, ack) = subscribe(
        pipe,
        with_token(json!({"subscriber":"x-a","world_id":"W"}), &tok),
    )
    .await;
    let sub_id = ack["result"]["subscription_id"]
        .as_str()
        .unwrap()
        .to_string();
    // live before cancel
    call(
        pipe,
        "record_event",
        with_token(agent_event("ObjectCreated", "x-a", "W", vec![]), &tok),
    )
    .await;
    assert!(recv_event(&mut c, 2000).await.is_some());
    // cancel (test 12)
    let un = call(
        pipe,
        "unsubscribe",
        with_token(json!({"subscription_id": sub_id, "subscriber":"x-a"}), &tok),
    )
    .await;
    assert_eq!(un["ok"], true);
    // double-cancel is an error, not a panic (part of 14-style discipline)
    let un2 = call(
        pipe,
        "unsubscribe",
        with_token(json!({"subscription_id": sub_id, "subscriber":"x-a"}), &tok),
    )
    .await;
    assert_eq!(un2["ok"], false);
    // after cancel: nothing arrives (test 13)
    call(
        pipe,
        "record_event",
        with_token(agent_event("ObjectCreated", "x-a", "W", vec![]), &tok),
    )
    .await;
    assert!(recv_event(&mut c, 800).await.is_none());
}

// ---- 14. invalid subscription id ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_subscription_id() {
    let pipe = r"\\.\pipe\spaceos-v14-sub14";
    start_core(pipe).await;
    let r = call(
        pipe,
        "unsubscribe",
        json!({"subscription_id":"nope","subscriber":"cli"}),
    )
    .await;
    assert_eq!(r["ok"], false);
}

// ---- 15. duplicate subscriptions are independent ----
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_subscriptions_independent() {
    let pipe = r"\\.\pipe\spaceos-v14-sub15";
    start_core(pipe).await;
    let tok = register(pipe, "d-a", &["event.record"], &["W"]).await;
    let (mut c1, ack1) = subscribe(
        pipe,
        with_token(json!({"subscriber":"d-a","world_id":"W"}), &tok),
    )
    .await;
    let (mut c2, ack2) = subscribe(
        pipe,
        with_token(json!({"subscriber":"d-a","world_id":"W"}), &tok),
    )
    .await;
    assert_ne!(
        ack1["result"]["subscription_id"].as_str(),
        ack2["result"]["subscription_id"].as_str()
    );
    call(
        pipe,
        "record_event",
        with_token(agent_event("ObjectCreated", "d-a", "W", vec![]), &tok),
    )
    .await;
    assert!(recv_event(&mut c1, 2000).await.is_some());
    assert!(recv_event(&mut c2, 2000).await.is_some());
    // cancel one; other still live
    let id1 = ack1["result"]["subscription_id"]
        .as_str()
        .unwrap()
        .to_string();
    let un = call(
        pipe,
        "unsubscribe",
        with_token(json!({"subscription_id": id1, "subscriber":"d-a"}), &tok),
    )
    .await;
    assert_eq!(un["ok"], true);
    call(
        pipe,
        "record_event",
        with_token(agent_event("ObjectCreated", "d-a", "W", vec![]), &tok),
    )
    .await;
    assert!(recv_event(&mut c1, 800).await.is_none());
    assert!(recv_event(&mut c2, 2000).await.is_some());
}
