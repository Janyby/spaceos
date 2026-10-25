//! M7 — Mixed lifecycle race (ONE causal scenario).
//!
//! Hypothesis: object deletion (with relation cascade) racing a view
//! disconnect/reconnect leaves dangling relations or a resync that
//! disagrees with Core.
//! What may break: relations pointing at deleted objects; reconnected
//! view seeing stale references; resync/query disagreeing with delivery.
//! Failure boundary: Core cascade + subscription resync contract.
//! Why it may break: cascade and disconnect interleave; the reconnected
//! subscriber's world image may predate the delete.
//! Exact workload: object P + object C + relation P-contains-C; one
//! subscriber; burst 10 events; DROP subscriber; delete P (cascade) +
//! 5 more events; reconnect fresh; drain new deliveries; resync via
//! query and cross-check every relation endpoint exists.
//! Dataset: temp DB + temp pipe per run.
//! Fixed threshold: 0 dangling relations; resync shows P gone, relation
//! gone, C present; reconnected delivery receives post-delete events.
//! Expected behavior: HELD.
//! Measurement: dangling count, resync membership, delivery count.
//! Pass criterion: all exact as above.
//! Failure criterion: any dangling relation OR resync/disagree OR
//! reconnect delivery failure.
//! Reproduction protocol: 3 identical runs.
//! (Single scenario per §M7 — no chaos mix.)

use serde_json::{json, Value};
use space_core::{ipc, Object, Relation, Storage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp_db(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-fm7");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{tag}.db"))
}

async fn start_core(pipe: &str, db: &std::path::Path) {
    let storage = Storage::open(db).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    tokio::spawn(async move {
        let _ = ipc::run_server(s2, &p).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
}

async fn register(pipe: &str, name: &str, caps: &[&str], worlds: &[&str]) -> String {
    let r = ipc::send_request(
        pipe,
        "register_agent",
        json!({"principal":"system","name":name,"capabilities":caps,"worlds":worlds}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true, "register {name}: {r}");
    r["result"]["token"].as_str().unwrap().to_string()
}

struct SubConn {
    lines: tokio::io::Lines<
        BufReader<tokio::io::ReadHalf<tokio::net::windows::named_pipe::NamedPipeClient>>,
    >,
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
    (SubConn { lines }, serde_json::from_str(&ack).unwrap())
}

async fn recv_n(conn: &mut SubConn, n: usize, ms_each: u64) -> Vec<Value> {
    let mut out = vec![];
    for _ in 0..n {
        match tokio::time::timeout(
            std::time::Duration::from_millis(ms_each),
            conn.lines.next_line(),
        )
        .await
        {
            Ok(Ok(Some(l))) => out.push(serde_json::from_str(&l).unwrap()),
            _ => break,
        }
    }
    out
}

async fn call(pipe: &str, tok: &str, method: &str, mut params: Value) -> Value {
    params["actor_token"] = json!(tok);
    ipc::send_request(pipe, method, params).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m7_delete_race_disconnect_reconnect() {
    let pipe_owned = format!(r"\\.\pipe\spaceos-fm7-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let db = tmp_db("m7");
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;

    let tok = register(
        pipe,
        "agent-m7",
        &[
            "object.create",
            "object.delete",
            "relation.create",
            "event.record",
        ],
        &["WA"],
    )
    .await;

    // Setup: P + C + relation.
    let mut p = Object::new(
        "Project",
        "agent-m7",
        json!({"name": "P", "world_id": "WA"}),
    );
    p.id = "m7-P".to_string();
    assert_eq!(
        call(pipe, &tok, "create_object", json!(p)).await["ok"],
        true
    );
    let mut c = Object::new("File", "agent-m7", json!({"name": "C", "world_id": "WA"}));
    c.id = "m7-C".to_string();
    assert_eq!(
        call(pipe, &tok, "create_object", json!(c)).await["ok"],
        true
    );
    let mut r = Relation::new("m7-P", "contains", "m7-C");
    r.id = "m7-rel".to_string();
    r.world_id = "WA".to_string();
    let mut rp = json!(r);
    rp["actor"] = json!("agent-m7");
    assert_eq!(call(pipe, &tok, "create_relation", rp).await["ok"], true);

    // Subscriber live for initial burst.
    let (mut sub, ack) = subscribe(
        pipe,
        json!({"subscriber": "agent-m7", "actor_token": tok, "world_id": "WA"}),
    )
    .await;
    assert_eq!(ack["ok"], true);
    for i in 0..10 {
        let mut e = space_core::Event::new("Tick", "agent-m7", json!({"i": i}));
        e.id = format!("m7-pre-{i:02}");
        e.world_id = "WA".to_string();
        assert_eq!(call(pipe, &tok, "record_event", json!(e)).await["ok"], true);
    }
    let got = recv_n(&mut sub, 10, 5000).await;
    assert_eq!(got.len(), 10, "live view gets pre-delete burst");

    // Disconnect mid-lifecycle, then delete with cascade + more events.
    drop(sub);
    assert_eq!(
        call(
            pipe,
            &tok,
            "delete_object",
            json!({"id": "m7-P", "actor": "agent-m7", "actor_token": tok})
        )
        .await["ok"],
        true
    );
    for i in 0..5 {
        let mut e = space_core::Event::new("Tick", "agent-m7", json!({"i": 100 + i}));
        e.id = format!("m7-post-{i:02}");
        e.world_id = "WA".to_string();
        assert_eq!(call(pipe, &tok, "record_event", json!(e)).await["ok"], true);
    }

    // Reconnect: fresh subscription receives post-delete flow.
    let (mut sub2, ack) = subscribe(
        pipe,
        json!({"subscriber": "agent-m7", "actor_token": tok, "world_id": "WA"}),
    )
    .await;
    assert_eq!(ack["ok"], true);
    // Fresh subscriber gets only events published after ITS subscribe...
    // publish 3 more to prove the channel is live.
    for i in 0..3 {
        let mut e = space_core::Event::new("Tick", "agent-m7", json!({"i": 200 + i}));
        e.id = format!("m7-re-{i:02}");
        e.world_id = "WA".to_string();
        assert_eq!(call(pipe, &tok, "record_event", json!(e)).await["ok"], true);
    }
    let got2 = recv_n(&mut sub2, 3, 5000).await;
    assert_eq!(got2.len(), 3, "reconnected view receives live flow");

    // Resync via query: P gone, relation gone, C present, no dangling.
    let objs = ipc::send_request(pipe, "query_objects", json!({}))
        .await
        .unwrap();
    let objs: Vec<Object> = serde_json::from_value(objs["result"].clone()).unwrap();
    assert!(objs.iter().all(|o| o.id != "m7-P"), "P deleted");
    assert!(
        objs.iter().any(|o| o.id == "m7-C"),
        "C survives (only relations cascade)"
    );
    let rels = ipc::send_request(pipe, "query_relations", json!({}))
        .await
        .unwrap();
    let rels: Vec<Relation> = serde_json::from_value(rels["result"].clone()).unwrap();
    let ids: std::collections::HashSet<&str> = objs.iter().map(|o| o.id.as_str()).collect();
    for r in &rels {
        assert!(
            ids.contains(r.source_object_id.as_str()) && ids.contains(r.target_object_id.as_str()),
            "M7 zero dangling relations, violator {}",
            r.id
        );
    }
    assert!(
        rels.iter().all(|r| r.id != "m7-rel"),
        "cascade removed the relation"
    );
    eprintln!("M7: delete+reconnect consistent, 0 dangling, resync agrees");
}
