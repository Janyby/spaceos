#![cfg(windows)]
use base64::{engine::general_purpose::URL_SAFE_NO_PAD,Engine};
use serde_json::{json,Value};
use space_core::{ipc,principal::PrincipalRegistry,provider_observation::{BrowserRuntimeConfig,browser_tab_observation},service_registration::ServiceRegistrarRegistry,Storage};
use std::{path::{Path,PathBuf},process::{Child,Command,Stdio},time::Duration};
use tokio::io::{AsyncBufReadExt,AsyncWriteExt,BufReader,AsyncReadExt};
fn key(n:u8)->String{URL_SAFE_NO_PAD.encode([n;32])}
fn write_evidence(path:&Path,bytes:&[u8])->std::io::Result<()>{let mut output=std::fs::OpenOptions::new().write(true).create_new(true).open(path)?;std::io::Write::write_all(&mut output,bytes)}
fn cfg()->Value{let path=std::env::var("SPACEOS_BROWSER_REAL_TEST_CONFIG").expect("run through browser-observation-real-test.mjs");serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()}
fn runtime(c:&Value)->BrowserRuntimeConfig{serde_json::from_value(c["runtime"].clone()).unwrap()}
fn entry(r:&BrowserRuntimeConfig,instance:&str,secret:u8)->Value{let mut v=serde_json::to_value(r).unwrap();v["provider"]=json!("browser");v["instance_id"]=json!(instance);v["instance_token"]=json!(key(secret));v}
#[test]
fn browser_core_child(){
 if std::env::var_os("SPACEOS_BROWSER_CORE_CHILD").is_none(){return}
 let c=cfg();let run=PathBuf::from(c["run"].as_str().unwrap());let allowed=std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().join("test-tmp")).unwrap();assert!(std::fs::canonicalize(&run).unwrap().starts_with(allowed));
 let pipe=c["pipe"].as_str().unwrap();assert!(pipe.starts_with(r"\\.\pipe\spaceos-4-3a4-browser-"));
 let mut p=PrincipalRegistry::default();p.insert("browser",[201;32],["provider.register","observation.report","event.record"],["browser-A","browser-B"]).unwrap();p.insert("system",[203;32],["agent.register","agent.grant.semantic.read"],["browser-A"]).unwrap();
 let mut state=ipc::CoreState::new_with_semantic_world_and_identities(Storage::open(run.join("synthetic.db")).unwrap(),None,&format!("{pipe}-unused-fs"),c["provider_pipe"].as_str().unwrap(),p,ServiceRegistrarRegistry::default(),ipc::core_token());
 let a=runtime(&c);let mut b=a.clone();b.source_id="509690cb-51b1-4335-9ce6-f21270143dd1".into();b.world_id="browser-B".into();b.cdp_origin=if a.cdp_origin=="http://127.0.0.1:21989"{"http://127.0.0.1:21990"}else{"http://127.0.0.1:21989"}.into();b.expected_browser_debugger_id="synthetic-other-runtime".into();
 std::sync::Arc::get_mut(&mut state).unwrap().observation_bindings.configure_browser(&json!([entry(&a,"browser-real-A",202),entry(&b,"browser-synthetic-B",205)]).to_string()).unwrap();
 tokio::runtime::Runtime::new().unwrap().block_on(ipc::run_server(state,pipe)).unwrap();
}
struct Owned(Child);impl Drop for Owned{fn drop(&mut self){let _=self.0.kill();let _=self.0.wait();}}
fn core(run:&Path)->Owned{let log=std::fs::OpenOptions::new().write(true).create_new(true).open(run.join(format!("core-{}.log",uuid::Uuid::new_v4()))).unwrap();Owned(Command::new(std::env::current_exe().unwrap()).args(["--exact","browser_core_child","--nocapture"]).env("SPACEOS_BROWSER_CORE_CHILD","1").stdout(Stdio::from(log.try_clone().unwrap())).stderr(Stdio::from(log)).spawn().unwrap())}
fn provider(run:&Path,c:&Value)->Owned{let log=std::fs::OpenOptions::new().write(true).create_new(true).open(run.join(format!("provider-{}.log",uuid::Uuid::new_v4()))).unwrap();Owned(Command::new(env!("CARGO_BIN_EXE_browser-provider")).env("SPACEOS_CORE_PIPE",c["pipe"].as_str().unwrap()).env("SPACEOS_BROWSER_PROVIDER_PIPE",c["provider_pipe"].as_str().unwrap()).env("SPACEOS_BROWSER_CDP_BASE",c["runtime"]["cdp_origin"].as_str().unwrap()).env("SPACEOS_POLL_SECS","1").env("SPACEOS_PROVIDER_ID","browser").env("SPACEOS_BROWSER_OBSERVATION_SOURCE",c["runtime"].to_string()).env("SPACEOS_PROVIDER_PRINCIPAL_TOKEN",key(201)).env("SPACEOS_PROVIDER_INSTANCE_ID","browser-real-A").env("SPACEOS_PROVIDER_INSTANCE_TOKEN",key(202)).env("SPACEOS_DISPATCH_KEY_BROWSER",key(204)).stdout(Stdio::from(log.try_clone().unwrap())).stderr(Stdio::from(log)).spawn().unwrap())}
fn db(run:&Path)->rusqlite::Connection{rusqlite::Connection::open_with_flags(run.join("synthetic.db"),rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()}
fn count(run:&Path,t:&str)->i64{db(run).query_row(&format!("SELECT count(*) FROM {t}"),[],|r|r.get(0)).unwrap()}
async fn req(pipe:&str,method:&str,p:Value)->Value{ipc::send_request(pipe,method,p).await.unwrap()}
async fn ready(pipe:&str){for _ in 0..100{if ipc::send_request(pipe,"ping",json!({})).await.is_ok(){return}tokio::time::sleep(Duration::from_millis(50)).await}panic!("isolated Core not ready")}
async fn send(pipe:&str,obs:space_core::Observation,instance:&str,secret:u8)->Value{let mut v=serde_json::to_value(obs).unwrap();v["actor"]=json!("browser");v["principal_token"]=json!(key(201));v["instance_id"]=json!(instance);v["instance_token"]=json!(key(secret));req(pipe,"report_observation",v).await}
async fn reader(pipe:&str)->String{let r=req(pipe,"register_agent",json!({"principal":"system","principal_token":key(203),"name":"browser-observer","capabilities":["semantic.read"],"worlds":["browser-A"]})).await;assert_eq!(r["ok"],true,"{r}");r["result"]["token"].as_str().unwrap().into()}
async fn read(pipe:&str,token:&str,world:&str)->Value{req(pipe,"query_objects",json!({"actor":"browser-observer","actor_token":token,"world_id":world})).await}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn real_chrome_browser_observation(){
 assert_eq!(std::env::var("SPACEOS_TEST_ISOLATION_ROOT").unwrap(),r"C:\spaceos");assert!(std::env::var("SPACEOS_CORE_TOKEN").unwrap().starts_with("synthetic-"));
 let c=cfg();let run=PathBuf::from(c["run"].as_str().unwrap());let pipe=c["pipe"].as_str().unwrap();let r=runtime(&c);
 let mut core1=core(&run);ready(pipe).await;let token=reader(pipe).await;
 let client=tokio::net::windows::named_pipe::ClientOptions::new().open(pipe).unwrap();let (rd,mut wr)=tokio::io::split(client);let mut lines=BufReader::new(rd).lines();wr.write_all((json!({"method":"subscribe","params":{"subscriber":"browser-observer","actor_token":token,"world_id":"browser-A","event_types":["BrowserTabCreated"]}}).to_string()+"\n").as_bytes()).await.unwrap();let ack:Value=serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();assert_eq!(ack["ok"],true);
 let mut p1=provider(&run,&c);let mut found=None;
 for _ in 0..120{let response=read(pipe,&token,"browser-A").await;if response["ok"]==true {found=response["result"].as_array().unwrap().iter().find(|o|o["properties"]["url"].as_str().is_some_and(|s|s.ends_with("/synthetic"))).cloned();if found.is_some(){break}}tokio::time::sleep(Duration::from_millis(100)).await;}
 let object=found.expect("real Browser Provider did not admit synthetic Chrome target");assert_eq!(object["properties"]["title"],"SpaceOS synthetic BrowserTab");assert!(!object.to_string().contains("?remove"));assert!(!object.to_string().contains("#remove"));assert_eq!(object["type"],"BrowserTab");
 let notification:Value=serde_json::from_str(&tokio::time::timeout(Duration::from_secs(4),lines.next_line()).await.unwrap().unwrap().unwrap()).unwrap();assert_eq!(notification["event"]["actor"],"browser");assert_eq!(notification["event"]["world_id"],"browser-A");
 p1.0.kill().unwrap();p1.0.wait().unwrap();let n=count(&run,"events");let logs=count(&run,"observation_log");assert_eq!(count(&run,"objects"),1);
 let target=object["properties"]["target_id"].as_str().unwrap();let original=browser_tab_observation("browser-real-A",&r,target,"https://example.test/navigated?remove=yes",Some("new title ignored on replay")).unwrap();
 let replay=send(pipe,original.clone(),"browser-real-A",202).await;assert_eq!(replay["result"]["applied"],false,"{replay}");assert_eq!(replay["result"]["object"],object);assert_eq!(count(&run,"events"),n);
 let mut denials=Vec::new();
 for mode in 0..12 {
  let mut o=original.clone();let mut instance="browser-real-A";let mut secret=202;
  match mode{0=>secret=209,1=>secret=201,2=>instance="unknown",3=>o.world_id="browser-B".into(),4=>o.objects[0].id="forged".into(),5=>o.objects[0].properties["html"]=json!("denied"),6=>o.facts.push(json!({"page":"denied"})),7=>o.delete_ids.push("x".into()),8=>o.objects[0].properties["runtime_epoch"]=json!(uuid::Uuid::new_v4().to_string()),9=>o.event.event_type="BrowserTabClosed".into(),10=>{instance="browser-synthetic-B";secret=205;},_=>o.objects.push(o.objects[0].clone())}
  let denied=send(pipe,o,instance,secret).await;assert_eq!(denied["ok"],false,"{denied}");denials.push(denied);
 }
 assert_eq!(count(&run,"events"),n);assert_eq!(count(&run,"observation_log"),logs);assert_eq!(count(&run,"objects"),1);
 let forged=req(pipe,"register_provider",json!({"principal":"browser","principal_token":key(201),"id":format!("observation-source:browser:{}",r.source_id),"name":"forged","capabilities":[]})).await;assert_eq!(forged["ok"],false);denials.push(forged);
 let mut b=r.clone();b.source_id="509690cb-51b1-4335-9ce6-f21270143dd1".into();b.world_id="browser-B".into();b.cdp_origin=if r.cdp_origin=="http://127.0.0.1:21989"{"http://127.0.0.1:21990"}else{"http://127.0.0.1:21989"}.into();b.expected_browser_debugger_id="synthetic-other-runtime".into();
 let reg=req(pipe,"register_provider_instance",json!({"actor":"browser","principal_token":key(201),"instance_id":"browser-synthetic-B","instance_token":key(205),"source_id":b.source_id,"runtime_epoch":b.runtime_epoch,"browser_debugger_id":b.expected_browser_debugger_id})).await;assert_eq!(reg["ok"],true,"{reg}");
 let other=send(pipe,browser_tab_observation("browser-synthetic-B",&b,target,"about:blank",None).unwrap(),"browser-synthetic-B",205).await;assert_eq!(other["ok"],true);assert_ne!(other["result"]["object"]["id"],object["id"]);assert_eq!(read(pipe,&token,"browser-A").await["result"].as_array().unwrap().len(),1);assert_eq!(read(pipe,&token,"browser-B").await["ok"],false);
 assert!(tokio::time::timeout(Duration::from_millis(150),lines.next_line()).await.is_err());
 let mut p2=provider(&run,&c);tokio::time::sleep(Duration::from_millis(1400)).await;
 assert_eq!(read(pipe,&token,"browser-A").await["result"],json!([object]));assert_eq!(count(&run,"events"),n+1);
 // Real signed Provider operation, independent of the Core observation grant.
 let pp=c["provider_pipe"].as_str().unwrap();let ping=req(pp,"ping",json!({})).await;
 let action=space_core::ActionRequest::new("get_tabs",json!({}),"browser","synthetic-signed-reader");let mut command=json!(action);command["dispatch_proof"]=json!(space_core::dispatch::issue(&key(204),ping["result"]["instance_id"].as_str().unwrap(),&action).unwrap());
 let signed_tabs=req(pp,"action",command).await;assert_eq!(signed_tabs["result"]["status"],"Completed","{signed_tabs}");
 assert!(signed_tabs["result"]["detail"]["tabs"].as_array().unwrap().iter().any(|t|t["tab_id"]==object["id"]));
 drop(lines);drop(wr);core1.0.kill().unwrap();core1.0.wait().unwrap();let mut core2=core(&run);ready(pipe).await;
 // Provider p2 stays running while Core restarts. Only its polling path may
 // re-register here; the test itself never registers source A in the new Core.
 let mut recovered=false;for _ in 0..70{let response=send(pipe,original.clone(),"browser-real-A",202).await;if response["ok"]==true{assert_eq!(response["result"]["applied"],false);recovered=true;break}tokio::time::sleep(Duration::from_millis(100)).await;}assert!(recovered,"Provider did not re-register after Core restart");
 let token2=reader(pipe).await;let after=read(pipe,&token2,"browser-A").await;assert_eq!(after["result"],json!([object]));assert_eq!(count(&run,"events"),n+1);
 p2.0.kill().unwrap();p2.0.wait().unwrap();
 // Restart only our dedicated test Chrome on its existing port. Core keeps
 // the original owner-approved epoch; no automatic approval is performed.
 let control=c["control_port"].as_u64().unwrap() as u16;let mut stream=tokio::net::TcpStream::connect(("127.0.0.1",control)).await.unwrap();stream.write_all(format!("POST /fixture-restart HTTP/1.1\r\nHost: 127.0.0.1:{control}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();let mut bytes=Vec::new();tokio::time::timeout(Duration::from_secs(15),stream.read_to_end(&mut bytes)).await.unwrap().unwrap();assert!(String::from_utf8_lossy(&bytes).starts_with("HTTP/1.1 200"));
 let provider=browser_provider::BrowserProvider::with_config(browser_provider::BrowserProviderConfig{cdp_base:r.cdp_origin.clone(),provider_id:"browser".into(),poll_interval_secs:1});
 std::env::set_var("SPACEOS_PROVIDER_PRINCIPAL_TOKEN",key(201));std::env::set_var("SPACEOS_PROVIDER_INSTANCE_ID","browser-real-A");std::env::set_var("SPACEOS_PROVIDER_INSTANCE_TOKEN",key(202));
 let mismatch=provider.verified_observation_cycle(pipe,&r).await.unwrap_err().to_string();assert!(mismatch.contains("unapproved browser runtime"));assert_eq!(count(&run,"events"),n+1);assert_eq!(read(pipe,&token2,"browser-A").await["result"],json!([object]));
 let version:i64=db(&run).query_row("PRAGMA user_version",[],|row|row.get(0)).unwrap();assert_eq!(version,4);
 core2.0.kill().unwrap();core2.0.wait().unwrap();let evidence=json!({"run":run,"pipe":pipe,"object":object,"notification":notification,"replay":replay,"synthetic_other_world":other,"denials":denials,"after_restart":after,"runtime_mismatch":mismatch,"core_schema":version,"signed_provider_tabs":signed_tabs,"provider_restart_same_core":true,"core_restart_provider_stays_running":recovered,"owned_rust_processes_stopped":true,"integrity":db(&run).query_row("PRAGMA integrity_check",[],|row|row.get::<_,String>(0)).unwrap()});
 let artifact=run.join("browser-observation-runtime-evidence.json");let mut evidence=evidence;evidence["evidence_artifact"]=json!(artifact);write_evidence(&artifact,&serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();println!("EVIDENCE_ARTIFACT {}",artifact.display());println!("REAL_CHROME_OBSERVATION_PASS run={} object={}",run.display(),object["id"]);
}
