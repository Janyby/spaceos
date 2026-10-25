//! Real isolated processes, named-pipe subscriptions, HTTP, WS and disposable SQLite.
#![cfg(windows)]
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use futures_util::StreamExt;
use serde_json::{json,Value};
use space_core::{ipc,Event,Object,Observation};
use std::{process::{Child,Command,Stdio},time::Duration,os::windows::process::CommandExt};
use tokio::io::{AsyncBufReadExt,AsyncWriteExt,BufReader};
const LAB:&str=r"C:\spaceos";
const A:&str="synthetic-process-world";
const B:&str="synthetic-process-world-b";
struct Process(Child);
impl Drop for Process { fn drop(&mut self){let _=self.0.kill();let _=self.0.wait();} }
fn spawn(exe:&str,vars:&[(&str,&str)],log:&std::path::Path)->Process {
    assert!(std::path::Path::new(exe).starts_with(format!(r"{LAB}\target\debug")));
    let mut cmd=Command::new(exe);
    cmd.env_clear().env("SystemRoot",std::env::var("SystemRoot").unwrap())
        .env("SPACEOS_TEST_ISOLATION_ROOT",LAB).env("SPACEOS_CORE_TOKEN",ipc::core_token())
        .env("TEMP",format!(r"{LAB}\test-tmp")).env("TMP",format!(r"{LAB}\test-tmp"))
        .stdin(Stdio::null()).stdout(Stdio::from(std::fs::File::create(log).unwrap()))
        .stderr(Stdio::from(std::fs::File::create(log.with_extension("stderr")).unwrap())).creation_flags(0x08000000);
    for (k,v) in vars {cmd.env(k,v);}
    Process(cmd.spawn().unwrap())
}
async fn call(pipe:&str,method:&str,p:Value)->Value { ipc::send_request(pipe,method,p).await.unwrap() }
async fn ready(pipe:&str){for _ in 0..60 {if ipc::send_request(pipe,"ping",json!({})).await.is_ok(){return;}tokio::time::sleep(Duration::from_millis(50)).await;}panic!("isolated Core not ready");}
type Pipe=tokio::net::windows::named_pipe::NamedPipeClient;
struct Subscription { lines:tokio::io::Lines<BufReader<tokio::io::ReadHalf<Pipe>>>, _writer:tokio::io::WriteHalf<Pipe> }
async fn subscribe(pipe:&str,p:Value)->(Subscription,Value) {
    let socket=tokio::net::windows::named_pipe::ClientOptions::new().open(pipe).unwrap();
    let (r,mut w)=tokio::io::split(socket);
    w.write_all(format!("{}\n",json!({"method":"subscribe","params":p})).as_bytes()).await.unwrap();
    let mut lines=BufReader::new(r).lines();
    let ack:Value=serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    (Subscription{lines,_writer:w},ack)
}
async fn note(sub:&mut Subscription)->Option<Value> {
    match tokio::time::timeout(Duration::from_millis(900),sub.lines.next_line()).await {
        Ok(Ok(Some(line)))=>Some(serde_json::from_str::<Value>(&line).unwrap()["event"].clone()),_=>None
    }
}
async fn register(pipe:&str,name:&str,caps:Value,world:&str)->String {
    let r=call(pipe,"register_agent",json!({"principal":"system","principal_token":URL_SAFE_NO_PAD.encode([94;32]),
        "name":name,"capabilities":caps,"worlds":[world]})).await;
    assert_eq!(r["ok"],true,"scoped agent registration");r["result"]["token"].as_str().unwrap().into()
}
async fn emit(pipe:&str,actor:&str,token:&str,world:&str)->Value {
    let mut event=Event::new("I5DynamicEvent",actor,json!({"synthetic":true}));event.world_id=world.into();
    let mut p=json!(event);p["actor_token"]=json!(token);
    let r=call(pipe,"record_event",p).await;assert_eq!(r["ok"],true);r
}
async fn ws_event<S>(ws:&mut tokio_tungstenite::WebSocketStream<S>,cid:&str)->Value
where S:tokio::io::AsyncRead+tokio::io::AsyncWrite+Unpin {
    let event = tokio::time::timeout(Duration::from_secs(8),async {
        while let Some(Ok(m))=ws.next().await {if m.is_text(){let v:Value=serde_json::from_str(m.to_text().unwrap()).unwrap();
            if v["type"]=="semantic_event" {assert_ne!(v["event"]["world_id"],B);if v["event"]["correlation_id"]==cid{return v["event"].clone();}}}}
        panic!("socket closed before expected event")
    }).await.expect("positive WebSocket delivery timeout");
    assert!(tokio::time::timeout(Duration::from_millis(650),ws.next()).await.is_err(),"unexpected duplicate/foreign WebSocket notification");
    event
}
fn observation(path:&str,instance:&str,secret:u8)->Value {
    let mut o=Object::new("File","filesystem",json!({"path":path}));o.external_id=Some(path.into());
    let mut e=Event::new("FileCreated","filesystem",json!({"path":path,"object_id":o.id}));
    e.provider_id=Some("filesystem".into());e.object_ids=vec![o.id.clone()];
    let obs=Observation{observation_id:uuid::Uuid::new_v4().to_string(),world_id:String::new(),facts:vec![],objects:vec![o],relations:vec![],delete_ids:vec![],event:e};
    let mut p=json!(obs);p["actor"]=json!("filesystem");p["principal_token"]=json!(URL_SAFE_NO_PAD.encode([92;32]));
    p["instance_id"]=json!(instance);p["instance_token"]=json!(URL_SAFE_NO_PAD.encode([secret;32]));p
}

#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn dynamic_worlds_revocation_restart_and_callers() {
    assert_eq!(std::env::var("SPACEOS_TEST_ISOLATION_ROOT").unwrap(),LAB);
    let run=std::path::PathBuf::from(format!(r"{LAB}\test-tmp\i5-{}",uuid::Uuid::new_v4()));
    std::fs::create_dir_all(run.join("a")).unwrap();std::fs::create_dir_all(run.join("b")).unwrap();
    let root_a=run.join("a").to_str().unwrap().to_owned();let root_b=run.join("b").to_str().unwrap().to_owned();
    let db=run.join("core.db").to_str().unwrap().to_owned();let public=run.join("fixture-public.json").to_str().unwrap().to_owned();
    let actors=run.join("actors.json").to_str().unwrap().to_owned();
    // Fresh synthetic credentials are test fixtures; values never enter evidence or HTTP responses.
    let login=run.join("login.txt");std::fs::write(&login,"I5-synthetic-explicit-login-proof-48-bytes-minimum").unwrap();
    let login=login.to_str().unwrap().to_owned();
    let pipe=format!(r"\\.\pipe\spaceos-4-3a4-process-i5-{}",uuid::Uuid::new_v4());
    let fs_pipe=format!(r"\\.\pipe\spaceos-4-3a4-i5-unused-fs-{}",uuid::Uuid::new_v4());
    let br_pipe=format!(r"\\.\pipe\spaceos-4-3a4-i5-unused-browser-{}",uuid::Uuid::new_v4());
    let port=18873u16;let port_text=port.to_string();
    // An exact, dedicated port; refusal to bind is a failure, never production fallback.
    drop(std::net::TcpListener::bind(("127.0.0.1",port)).expect("I5 port must be free"));
    let core_exe=env!("CARGO_BIN_EXE_isolated-core-fixture");let bridge_exe=env!("CARGO_BIN_EXE_isolated-bridge-fixture");
    let core_vars=[("SPACEOS_FIXTURE_CORE_PIPE",pipe.as_str()),("SPACEOS_FIXTURE_DB",db.as_str()),("SPACEOS_FIXTURE_WORLD",A),
        ("SPACEOS_FIXTURE_FULL_STACK","1"),("SPACEOS_FIXTURE_I4X","1"),("SPACEOS_FIXTURE_I4X_PUBLIC",public.as_str()),
        ("SPACEOS_FIXTURE_I4X_CREDENTIALS",actors.as_str()),("SPACEOS_FIXTURE_FS_ROOT",root_a.as_str()),("SPACEOS_FIXTURE_FS_ROOT_B",root_b.as_str()),
        ("SPACEOS_FILESYSTEM_PROVIDER_PIPE",fs_pipe.as_str()),("SPACEOS_BROWSER_PROVIDER_PIPE",br_pipe.as_str())];
    let mut core=spawn(core_exe,&core_vars,&run.join("core.log"));ready(&pipe).await;
    let mut bridge=spawn(bridge_exe,&[("SPACEOS_BRIDGE_CORE_PIPE",&pipe),("SPACEOS_FIXTURE_WORLD",A),("SPACEOS_FIXTURE_HTTP_PORT",&port_text),
        ("SPACEOS_FIXTURE_I4X","1"),("SPACEOS_FIXTURE_I4X_LOGIN_FILE",&login)],&run.join("bridge.log"));
    let client=reqwest::Client::new();let base=format!("http://127.0.0.1:{port}");
    for _ in 0..80 {if client.get(format!("{base}/api/health")).send().await.is_ok(){break;}tokio::time::sleep(Duration::from_millis(50)).await;}
    let denied=client.get(format!("{base}/api/events")).send().await.unwrap();assert_eq!(denied.status(),401);
    let auth:Value=client.post(format!("{base}/api/lab-auth/login")).header("Origin",&base)
        .json(&json!({"proof":std::fs::read_to_string(&login).unwrap(),"world_id":A})).send().await.unwrap().json().await.unwrap();
    assert_eq!(auth["ok"],true);
    // Cookie comes from a separate login response, never from JSON.
    let response=client.post(format!("{base}/api/lab-auth/login")).header("Origin",&base)
        .json(&json!({"proof":std::fs::read_to_string(&login).unwrap(),"world_id":A})).send().await.unwrap();
    let cookie=response.headers()["set-cookie"].to_str().unwrap().split(';').next().unwrap().to_owned();
    let auth:Value=response.json().await.unwrap();let csrf=auth["csrf"].as_str().unwrap();
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut request=format!("ws://127.0.0.1:{port}/ws").into_client_request().unwrap();
    request.headers_mut().insert("Origin",base.parse().unwrap());request.headers_mut().insert("Cookie",cookie.parse().unwrap());
    let (mut ws,_)=tokio_tungstenite::connect_async(request.clone()).await.unwrap();
    let init=ws.next().await.unwrap().unwrap();assert_eq!(serde_json::from_str::<Value>(init.to_text().unwrap()).unwrap()["type"],"init");
    let token_a=register(&pipe,"i5-live-a",json!(["semantic.read","event.record"]),A).await;
    let token_b=register(&pipe,"i5-live-b",json!(["semantic.read","event.record"]),B).await;
    let (mut a,ack)=subscribe(&pipe,json!({"subscriber":"i5-live-a","actor_token":token_a,"world_id":A})).await;assert_eq!(ack["ok"],true);
    let (mut b,ack)=subscribe(&pipe,json!({"subscriber":"i5-live-b","actor_token":token_b,"world_id":B})).await;assert_eq!(ack["ok"],true);
    for p in [json!({"subscriber":"i5-live-a","actor_token":token_a}),json!({"subscriber":"i5-live-a","actor_token":token_a,"world_id":B}),
        json!({"subscriber":"system","principal_token":URL_SAFE_NO_PAD.encode([94;32])})] {
        let (_,ack)=subscribe(&pipe,p).await;assert_eq!(ack["ok"],false);
    }
    let no_read=register(&pipe,"i5-no-read",json!([]),A).await;
    let (_,ack)=subscribe(&pipe,json!({"subscriber":"i5-no-read","actor_token":no_read,"world_id":A})).await;assert_eq!(ack["ok"],false);
    let mut seq_a=vec![];let mut seq_b=vec![];
    for _ in 0..3 {
        emit(&pipe,"i5-live-a",&token_a,A).await;
        let ea=note(&mut a).await.expect("World A positive event");assert_eq!(ea["world_id"],A);seq_a.push(ea["seq"].as_u64().unwrap());
        assert!(note(&mut b).await.is_none(),"B leaked A");
        emit(&pipe,"i5-live-b",&token_b,B).await;
        let eb=note(&mut b).await.expect("World B positive event");assert_eq!(eb["world_id"],B);seq_b.push(eb["seq"].as_u64().unwrap());
        assert!(note(&mut a).await.is_none(),"A leaked B");
    }
    assert!(seq_a.windows(2).all(|w|w[0]<w[1]));assert!(seq_b.windows(2).all(|w|w[0]<w[1]));
    let recorded=emit(&pipe,"i5-live-a",&token_a,A).await;let once=note(&mut a).await.unwrap();
    let mut replay=recorded["result"].clone();replay["actor_token"]=json!(token_a);
    assert_eq!(call(&pipe,"record_event",replay).await["ok"],true);
    assert!(note(&mut a).await.is_none(),"immutable event redelivery must not notify twice");
    assert!(once["seq"].as_u64().unwrap()>seq_a[2]);
    // A semantic mutation while the SAME WebSocket remains open.
    let cid=format!("i5-http-{}",uuid::Uuid::new_v4());
    // Mission is now a protected Core command type, not a generic object.
    let created_response=client.post(format!("{base}/api/missions")).header("Origin",&base).header("Cookie",&cookie).header("x-spaceos-csrf",csrf)
        .json(&json!({"goal":"I5 mission","workspace":run,"assignee":"synthetic","command_id":cid}))
        .send().await.unwrap();assert_eq!(created_response.status(),200);
    let created:Value=created_response.json().await.unwrap();
    assert_eq!(created["mission"]["properties"]["state"],"created");
    assert_eq!(created["mission"]["properties"]["world_id"],A);
    let ev=ws_event(&mut ws,&cid).await;assert!(ev["seq"].as_u64().unwrap()>seq_a[2]);
    let id=created["mission"]["id"].as_str().unwrap();
    std::env::set_var("SPACEOS_BRIDGE_CORE_PIPE",&pipe);
    space_bridge::ipc_client::configure_isolated_read_service(URL_SAFE_NO_PAD.encode([110;32]),A.into()).unwrap();
    space_bridge::ipc_client::configure_isolated_mcp_read_service(URL_SAFE_NO_PAD.encode([112;32]),A.into()).unwrap();
    let mission=space_bridge::mission::get_view(id).await.unwrap();assert_eq!(mission.mission["id"],id);
    let mcp=space_bridge::ipc_client::mcp_read_request("query_objects",json!({})).await.unwrap();
    assert!(mcp["result"].as_array().unwrap().iter().all(|o|o["properties"]["world_id"]==A));
    assert!(space_bridge::ipc_client::mcp_read_request("query_objects",json!({"world_id":B})).await.is_err());
    for (credential,world,caps) in [(URL_SAFE_NO_PAD.encode([110;32]),A,json!(["semantic.read"])),
        (URL_SAFE_NO_PAD.encode([112;32]),B,json!(["semantic.read"])),
        (URL_SAFE_NO_PAD.encode([112;32]),A,json!(["semantic.read","object.update"]))] {
        assert_eq!(call(&pipe,"register_service_actor",json!({"profile":"mcp_read","name":"space-mcp-reader",
            "worlds":[world],"capabilities":caps,"registrar_token":credential})).await["ok"],false);
    }
    let r=call(&pipe,"register_provider",json!({"principal":"filesystem","principal_token":URL_SAFE_NO_PAD.encode([92;32]),"id":"filesystem","name":"I5 synthetic filesystem","capabilities":[]})).await;assert_eq!(r["ok"],true);
    for (instance,key) in [("fixture-fs-A",81u8),("fixture-fs-B",82u8)] {
        let r=call(&pipe,"register_provider_instance",json!({"actor":"filesystem","principal_token":URL_SAFE_NO_PAD.encode([92;32]),"instance_id":instance,"instance_token":URL_SAFE_NO_PAD.encode([key;32])})).await;
        assert_eq!(r["ok"],true);
    }
    let file_a=format!(r"{root_a}\file-a.txt");std::fs::write(&file_a,b"I5 synthetic").unwrap();
    let obs=observation(&file_a,"fixture-fs-A",81);let file_cid=obs["event"]["id"].as_str().unwrap().to_owned();
    let r=call(&pipe,"report_observation",obs).await;assert_eq!(r["ok"],true);
    // FileCreated is delivered through the real Bridge event pump too.
    let file_event=tokio::time::timeout(Duration::from_secs(5),async {loop {let m=ws.next().await.unwrap().unwrap();if m.is_text(){let v:Value=serde_json::from_str(m.to_text().unwrap()).unwrap();if v["event"]["id"]==file_cid {break v;}}}}).await.unwrap();assert_eq!(file_event["event"]["world_id"],A);
    // The independent subscription still has the earlier Mission command event.
    assert_eq!(note(&mut a).await.unwrap()["event_type"],"MissionCreated");
    assert_eq!(note(&mut a).await.unwrap()["event_type"],"FileCreated");
    let file_b=format!(r"{root_b}\file-b.txt");std::fs::write(&file_b,b"I5 synthetic B").unwrap();
    assert_eq!(call(&pipe,"report_observation",observation(&file_b,"fixture-fs-B",82)).await["ok"],true);
    let event_b=note(&mut b).await.unwrap();assert_eq!(event_b["world_id"],B);assert_eq!(event_b["event_type"],"FileCreated");
    assert!(note(&mut a).await.is_none(),"World B FileCreated leaked to A");
    let params=json!({"actor":"filesystem","principal_token":URL_SAFE_NO_PAD.encode([92;32]),"instance_id":"fixture-fs-A","instance_token":URL_SAFE_NO_PAD.encode([81;32]),"source_root":root_a});
    let own=call(&pipe,"query_provider_objects",params.clone()).await;assert_eq!(own["ok"],true);assert_eq!(own["result"]["objects"].as_array().unwrap().len(),1);
    assert_eq!(call(&pipe,"query_objects",json!({"actor":"filesystem","principal_token":URL_SAFE_NO_PAD.encode([92;32]),"world_id":A})).await["ok"],false,
        "reconciliation capability must not grant generic World reads");
    let mut forged=params.clone();forged["source_root"]=json!(root_b);assert_eq!(call(&pipe,"query_provider_objects",forged).await["ok"],false);
    let mut forged=params.clone();forged["instance_id"]=json!("fixture-fs-B");assert_eq!(call(&pipe,"query_provider_objects",forged).await["ok"],false);
    let mut forged=params.clone();forged["principal_token"]=json!(ipc::core_token());assert_eq!(call(&pipe,"query_provider_objects",forged).await["ok"],false);
    let cache=tokio::sync::Mutex::new(None);
    for (tool,args) in [("filesystem_list",json!({"dir":root_a})),("filesystem_search",json!({"q":"file-a"})),
        ("spaceos_actions",json!({})),("spaceos_trace",json!({"action_id":cid}))] {
        let tool_result=space_bridge::mcp::call_tool(&cache,tool,&args).await;
        assert!(tool_result["isError"].is_null() || tool_result["isError"] == false,"real Core-backed MCP dispatcher read failed");
        let text=tool_result["content"][0]["text"].as_str().expect("MCP text result");
        let payload:Value=serde_json::from_str(text).expect("MCP result JSON");
        if tool == "filesystem_list" || tool == "filesystem_search" { assert_eq!(payload["count"],1); }
        if tool == "spaceos_trace" { assert_eq!(payload["events"].as_array().unwrap().len(),1); }
        assert!(!tool_result.to_string().contains("file-b.txt"));
    }
    let mcp_fixture=run.join("mcp-read.json");
    std::fs::write(&mcp_fixture,json!({"world_id":A,"registrar":URL_SAFE_NO_PAD.encode([112;32])}).to_string()).unwrap();
    let mut mcp=tokio::process::Command::new(env!("CARGO_BIN_EXE_spaceos-mcp"))
        .env_clear().env("SystemRoot",std::env::var("SystemRoot").unwrap())
        .env("SPACEOS_TEST_ISOLATION_ROOT",LAB).env("SPACEOS_CORE_TOKEN",ipc::core_token())
        .env("SPACEOS_BRIDGE_CORE_PIPE",&pipe).env("SPACEOS_MCP_READ_FIXTURE_FILE",&mcp_fixture)
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).kill_on_drop(true).creation_flags(0x08000000).spawn().unwrap();
    let mut input=mcp.stdin.take().unwrap();let mut output=BufReader::new(mcp.stdout.take().unwrap()).lines();
    input.write_all(format!("{}\n",json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"filesystem_search","arguments":{"q":"file-a"}}})).as_bytes()).await.unwrap();
    let rpc:Value=serde_json::from_str(&tokio::time::timeout(Duration::from_secs(10),output.next_line()).await.unwrap().unwrap().unwrap()).unwrap();
    assert!(rpc["result"]["isError"].is_null());let payload:Value=serde_json::from_str(rpc["result"]["content"][0]["text"].as_str().unwrap()).unwrap();assert_eq!(payload["count"],1);
    drop(input);assert!(mcp.wait().await.unwrap().success());
    // Runtime Model A client and its protected lab-only HTTP compatibility UI.
    let runtime_exe=format!(r"{LAB}\target\debug\spaceos.exe");
    let output=Command::new(&runtime_exe).env_clear().env("SystemRoot",std::env::var("SystemRoot").unwrap())
        .env("SPACEOS_TEST_ISOLATION_ROOT",LAB).env("SPACEOS_CORE_TOKEN",ipc::core_token()).env("SPACEOS_CORE_PIPE",&pipe)
        .env("SPACEOS_RUNTIME_ACTOR","i5-live-a").env("SPACEOS_RUNTIME_ACTOR_TOKEN",&token_a).env("SPACEOS_RUNTIME_WORLD_ID",A)
        .arg("objects").creation_flags(0x08000000).output().unwrap();
    assert!(output.status.success());let cli_objects:Value=serde_json::from_slice(&output.stdout).unwrap();
    assert!(cli_objects.as_array().unwrap().iter().all(|o|o["properties"]["world_id"]==A));
    let denied_output=Command::new(&runtime_exe).env_clear().env("SystemRoot",std::env::var("SystemRoot").unwrap())
        .env("SPACEOS_TEST_ISOLATION_ROOT",LAB).env("SPACEOS_CORE_TOKEN",ipc::core_token()).env("SPACEOS_CORE_PIPE",&pipe)
        .env("SPACEOS_RUNTIME_ACTOR","i5-live-a").env("SPACEOS_RUNTIME_ACTOR_TOKEN",&token_a).env("SPACEOS_RUNTIME_WORLD_ID",B)
        .arg("objects").creation_flags(0x08000000).output().unwrap();assert!(!denied_output.status.success());
    let ui_key="I5-synthetic-browser-caller-lab-key";
    let mut ui_process=Process(Command::new(&runtime_exe).env_clear().env("SystemRoot",std::env::var("SystemRoot").unwrap())
        .env("SPACEOS_TEST_ISOLATION_ROOT",LAB).env("SPACEOS_CORE_TOKEN",ipc::core_token()).env("SPACEOS_CORE_PIPE",&pipe)
        .env("SPACEOS_RUNTIME_ACTOR","i5-live-a").env("SPACEOS_RUNTIME_ACTOR_TOKEN",&token_a).env("SPACEOS_RUNTIME_WORLD_ID",A)
        .env("SPACEOS_RUNTIME_UI_LAB_KEY",ui_key).args(["ui","--port","18874"])
        .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).creation_flags(0x08000000).spawn().unwrap());
    let ui_url="http://127.0.0.1:18874/api/objects";
    for _ in 0..60 {if client.get(ui_url).send().await.is_ok(){break;}tokio::time::sleep(Duration::from_millis(40)).await;}
    assert_eq!(client.get(ui_url).send().await.unwrap().status(),401);
    let ui_objects:Value=client.get(ui_url).bearer_auth(ui_key).send().await.unwrap().json().await.unwrap();
    assert!(ui_objects.as_array().unwrap().len()>0);assert!(ui_objects.as_array().unwrap().iter().all(|o|o["properties"]["world_id"]==A));
    assert!(!ui_objects.to_string().contains(&token_a));
    ui_process.0.kill().unwrap();ui_process.0.wait().unwrap();
    // Rotate the original reader and remove its read capability. Queued notes must be discarded.
    register(&pipe,"i5-live-a",json!(["event.record"]),A).await;
    emit(&pipe,"i5-live-b",&token_b,B).await;
    while note(&mut a).await.is_some() {} // notifications delivered before rotation are allowed.
    let (_,ack)=subscribe(&pipe,json!({"subscriber":"i5-live-a","actor_token":token_a,"world_id":A})).await;assert_eq!(ack["ok"],false);
    let renewed=register(&pipe,"i5-live-a",json!(["event.record","semantic.read"]),A).await;
    let (mut renewed_sub,ack)=subscribe(&pipe,json!({"subscriber":"i5-live-a","actor_token":renewed,"world_id":A})).await;assert_eq!(ack["ok"],true);
    emit(&pipe,"i5-live-a",&renewed,A).await;assert!(note(&mut a).await.is_none());assert_eq!(note(&mut renewed_sub).await.unwrap()["world_id"],A);
    ws.close(None).await.unwrap();let (mut ws2,_)=tokio_tungstenite::connect_async(request.clone()).await.unwrap();ws2.next().await.unwrap().unwrap();
    let cid2=format!("i5-reconnect-{}",uuid::Uuid::new_v4());
    let response=client.post(format!("{base}/api/semantic/objects")).header("Origin",&base).header("Cookie",&cookie).header("x-spaceos-csrf",csrf)
        .json(&json!({"world_id":A,"object_type":"Note","properties":{},"correlation_id":cid2})).send().await.unwrap();assert!(response.status().is_success());ws_event(&mut ws2,&cid2).await;
    // Real Core process restart on the same synthetic database. Bridge remains alive.
    core.0.kill().unwrap();core.0.wait().unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let mut restart=core_vars.to_vec();restart.push(("SPACEOS_FIXTURE_RESTART","1"));
    core=spawn(core_exe,&restart,&run.join("core-restart.log"));ready(&pipe).await;
    let (_,ack)=subscribe(&pipe,json!({"subscriber":"i5-live-b","actor_token":token_b,"world_id":B})).await;assert_eq!(ack["ok"],false);
    let fresh_b=register(&pipe,"i5-live-b",json!(["event.record","semantic.read"]),B).await;
    let (mut b2,ack)=subscribe(&pipe,json!({"subscriber":"i5-live-b","actor_token":fresh_b,"world_id":B})).await;assert_eq!(ack["ok"],true);
    emit(&pipe,"i5-live-b",&fresh_b,B).await;assert_eq!(note(&mut b2).await.unwrap()["world_id"],B);
    let cid3=format!("i5-restart-{}",uuid::Uuid::new_v4());
    let response=client.post(format!("{base}/api/semantic/objects")).header("Origin",&base).header("Cookie",&cookie).header("x-spaceos-csrf",csrf)
        .json(&json!({"world_id":A,"object_type":"Note","properties":{},"correlation_id":cid3})).send().await.unwrap();assert!(response.status().is_success());
    ws_event(&mut ws2,&cid3).await;
    let diagnostics=std::fs::read_to_string(run.join("bridge.stderr")).unwrap();
    assert!(diagnostics.contains("query_failure"),"real temporary outage must be observable");
    assert!(!diagnostics.contains(csrf));assert!(!diagnostics.contains(&token_a));assert!(!diagnostics.contains(&ipc::core_token()));
    let c=rusqlite_connection(&db);let persisted:u64=c.query_row("SELECT COUNT(*) FROM events WHERE event_type='I5DynamicEvent'",[],|r|r.get(0)).unwrap();assert!(persisted>=8);
    ws2.close(None).await.unwrap();
    client.post(format!("{base}/api/lab-auth/logout")).header("Origin",&base).header("Cookie",&cookie).header("x-spaceos-csrf",csrf).send().await.unwrap();
    assert!(tokio_tungstenite::connect_async(request).await.is_err());
    assert!(bridge.0.try_wait().unwrap().is_none());assert!(core.0.try_wait().unwrap().is_none());
    let core_pid=core.0.id();let bridge_pid=bridge.0.id();
    for (world,seqs) in [(A,&seq_a),(B,&seq_b)] { for seq in seqs {
        let stored:String=c.query_row("SELECT world_id FROM events WHERE seq=?1",[seq],|r|r.get(0)).unwrap();assert_eq!(stored,world);
    }}
    drop(bridge);drop(core);
    std::fs::write(run.join("runtime-public.json"),serde_json::to_vec_pretty(&json!({
        "marker":"SPACEOS-I5-SYNTHETIC","status":"stopped","core_pipe":pipe,
        "http":base,"websocket":format!("ws://127.0.0.1:{port}/ws"),"sqlite":db,
        "world_a":A,"world_b":B,"core_pid_at_completion":core_pid,"bridge_pid":bridge_pid,
        "core_executable":core_exe,"bridge_executable":bridge_exe,
        "credential_fixture_references":{"actors":actors,"login":login,"mcp_read":mcp_fixture},
        "world_a_sequences":seq_a,"world_b_sequences":seq_b,"persisted_dynamic_events":persisted,
        "diagnostic_query_failure_seen":true,"open_code_exercised":false,"production_contact":false
    })).unwrap()).unwrap();
    println!("I5 REAL PASS: Core/Bridge processes; dynamic A/B events; ordered nonduplicate IPC; semantic/FileCreated WS; source-bound provider reads; Mission/MCP reads; rotation; reconnect; restart; SQLite events={persisted}; run={}",run.display());
}
fn rusqlite_connection(path:&str)->rusqlite::Connection {
    rusqlite::Connection::open_with_flags(path,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn cursor_authorization_failure_is_logged_without_init_or_secret() {
    let run=std::path::PathBuf::from(format!(r"{LAB}\test-tmp\i5-cursor-{}",uuid::Uuid::new_v4()));std::fs::create_dir_all(&run).unwrap();
    let pipe=format!(r"\\.\pipe\spaceos-4-3a4-process-i5-cursor-{}",uuid::Uuid::new_v4());
    let db=run.join("core.db").to_str().unwrap().to_owned();
    let proof="I5-cursor-error-synthetic-login-proof";let login=run.join("login.txt");std::fs::write(&login,proof).unwrap();
    let mut core=spawn(env!("CARGO_BIN_EXE_isolated-core-fixture"),&[("SPACEOS_FIXTURE_CORE_PIPE",&pipe),
        ("SPACEOS_FIXTURE_DB",&db),("SPACEOS_FIXTURE_WORLD",A),("SPACEOS_FIXTURE_READER_MODE","deny")],&run.join("core.log"));
    ready(&pipe).await;
    let mut bridge=spawn(env!("CARGO_BIN_EXE_isolated-bridge-fixture"),&[("SPACEOS_BRIDGE_CORE_PIPE",&pipe),("SPACEOS_FIXTURE_WORLD",A),
        ("SPACEOS_FIXTURE_HTTP_PORT","18875"),("SPACEOS_FIXTURE_I4X","1"),("SPACEOS_FIXTURE_I4X_LOGIN_FILE",login.to_str().unwrap())],&run.join("bridge.log"));
    let base="http://127.0.0.1:18875";let client=reqwest::Client::new();
    for _ in 0..60 {if client.get(format!("{base}/api/health")).send().await.is_ok(){break;}tokio::time::sleep(Duration::from_millis(40)).await;}
    let response=client.post(format!("{base}/api/lab-auth/login")).header("Origin",base)
        .json(&json!({"proof":proof,"world_id":A})).send().await.unwrap();
    let cookie=response.headers()["set-cookie"].to_str().unwrap().split(';').next().unwrap().to_owned();
    assert!(!client.get(format!("{base}/api/events")).header("Cookie",&cookie).send().await.unwrap().status().is_success());
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut request="ws://127.0.0.1:18875/ws".into_client_request().unwrap();
    request.headers_mut().insert("Origin",base.parse().unwrap());request.headers_mut().insert("Cookie",cookie.parse().unwrap());
    let (mut socket,_)=tokio_tungstenite::connect_async(request).await.unwrap();
    let first=tokio::time::timeout(Duration::from_secs(5),socket.next()).await.unwrap().unwrap().unwrap();
    assert!(first.is_close(),"authorization failure must never announce initialized stream");
    let log=std::fs::read_to_string(run.join("bridge.stderr")).unwrap();
    assert!(log.contains("cursor_initialization_failure") && log.contains("authorization_failure"));
    for secret in [proof,cookie.as_str(),ipc::core_token().as_str()] {assert!(!log.contains(secret));}
    assert!(core.0.try_wait().unwrap().is_none());assert!(bridge.0.try_wait().unwrap().is_none());
    println!("I5 cursor authorization failure PASS: no init, closed stream, bounded safe diagnostics; run={}",run.display());
}
