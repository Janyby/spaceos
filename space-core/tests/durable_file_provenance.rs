//! Actual private-pipe Core child process; synthetic provider client, no live Provider/prod DB.
#![cfg(windows)]
use base64::{engine::general_purpose::URL_SAFE_NO_PAD,Engine};
use rusqlite::{Connection,OpenFlags};
use serde_json::{json,Value};
use space_core::{ipc,principal::PrincipalRegistry,provider_observation::ObservationBindings,
    service_registration::ServiceRegistrarRegistry,Storage,Object,Event,Observation,ProviderInfo};
use std::{path::{Path,PathBuf},process::{Child,Command,Stdio},sync::Arc,time::Duration};
use tokio::io::{AsyncBufReadExt,AsyncWriteExt,BufReader};
fn token(n:u8)->String {URL_SAFE_NO_PAD.encode([n;32])}

#[test]
fn admission_core_child() {
    let Ok(config)=std::env::var("SPACEOS_ADMISSION_CHILD_CONFIG") else{return};
    let c:Value=serde_json::from_slice(&std::fs::read(config).unwrap()).unwrap();
    let allowed=std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().join("test-tmp")).unwrap();
    assert!(std::fs::canonicalize(c["run"].as_str().unwrap()).unwrap().starts_with(&allowed));
    let pipe=c["pipe"].as_str().unwrap();assert!(pipe.starts_with(r"\\.\pipe\spaceos-4-3a4-admission-"));
    let db=PathBuf::from(c["run"].as_str().unwrap()).join("synthetic.db");
    let mut p=PrincipalRegistry::default();
    p.insert("filesystem",[71;32],["provider.register","observation.report","object.create","object.update","object.delete","event.record"],["receipt-A","receipt-B"]).unwrap();
    p.insert("system",[72;32],["agent.register","agent.grant.semantic.read","agent.grant.semantic.relation.create","agent.grant.semantic.object.patch"],["receipt-A","receipt-B"]).unwrap();
    let mut state=ipc::CoreState::new_with_semantic_world_and_identities(Storage::open(db).unwrap(),Some("receipt-A"),
        &format!("{pipe}-unused-fs"),&format!("{pipe}-unused-browser"),p,ServiceRegistrarRegistry::default(),ipc::core_token());
    Arc::get_mut(&mut state).unwrap().observation_bindings=ObservationBindings::from_json(&json!([
        {"provider":"filesystem","instance_id":"receipt-instance-A","instance_token":token(c["instance_key"].as_u64().unwrap() as u8),"root":c["root_a"],"world_id":"receipt-A"},
        {"provider":"filesystem","instance_id":"receipt-instance-B","instance_token":token(82),"root":c["root_b"],"world_id":"receipt-B"}
    ]).to_string()).unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(ipc::run_server(state,pipe)).unwrap();
}

struct Process(Child);
impl Drop for Process {fn drop(&mut self){let _=self.0.kill();let _=self.0.wait();}}
fn start(config:&Path,run:&Path,label:&str)->Process {
    let log=std::fs::File::create(run.join(format!("{label}.log"))).unwrap();
    Process(Command::new(std::env::current_exe().unwrap()).args(["--exact","admission_core_child","--nocapture"])
        .env("SPACEOS_ADMISSION_CHILD_CONFIG",config).stdout(Stdio::from(log.try_clone().unwrap())).stderr(Stdio::from(log)).spawn().unwrap())
}
async fn request(pipe:&str,method:&str,params:Value)->Value {ipc::send_request(pipe,method,params).await.unwrap()}
async fn provider(pipe:&str,method:&str,mut params:Value)->Value {
    params["principal_token"]=json!(token(71));
    if method=="register_provider" {params["principal"]=json!("filesystem")}else{params["actor"]=json!("filesystem")}
    request(pipe,method,params).await
}
async fn ready(pipe:&str){for _ in 0..100{if ipc::send_request(pipe,"ping",json!({})).await.is_ok(){return}tokio::time::sleep(Duration::from_millis(40)).await}panic!("own synthetic Core not ready")}
async fn register(pipe:&str,key:u8){
    let r=provider(pipe,"register_provider",json!(ProviderInfo{id:"filesystem".into(),name:"receipt test".into(),capabilities:vec![],version:None})).await;assert_eq!(r["ok"],true);
    let r=provider(pipe,"register_provider_instance",json!({"instance_id":"receipt-instance-A","instance_token":token(key)})).await;assert_eq!(r["ok"],true);
}
fn observation(root:&Path,name:&str,kind:&str)->Value {
    let path=root.join(name);if kind=="Directory"{std::fs::create_dir_all(&path).unwrap()}else{std::fs::write(&path,b"synthetic provider fact").unwrap()}
    let mut object=Object::new(kind,"filesystem",json!({"path":path}));object.external_id=Some(path.to_string_lossy().into_owned());
    let mut event=Event::new("FileCreated","filesystem",json!({"path":path,"object_id":object.id}));event.provider_id=Some("filesystem".into());event.object_ids=vec![object.id.clone()];
    let mut v=serde_json::to_value(Observation{observation_id:uuid::Uuid::new_v4().to_string(),world_id:String::new(),facts:vec![],objects:vec![object],relations:vec![],delete_ids:vec![],event}).unwrap();
    v["instance_id"]=json!("receipt-instance-A");v["instance_token"]=json!(token(81));v
}
fn db(run:&Path)->Connection{Connection::open_with_flags(run.join("synthetic.db"),OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()}
fn receipts(run:&Path)->Vec<Value>{let c=db(run);let mut s=c.prepare("SELECT object_id,observation_id,event_id,world_id,instance_id,source_binding_digest FROM filesystem_file_admissions ORDER BY object_id").unwrap();s.query_map([],|r|Ok(json!({"object_id":r.get::<_,String>(0)?,"observation_id":r.get::<_,String>(1)?,"event_id":r.get::<_,String>(2)?,"world_id":r.get::<_,String>(3)?,"instance_id":r.get::<_,String>(4)?,"source_binding_digest":r.get::<_,String>(5)?}))).unwrap().collect::<rusqlite::Result<_>>().unwrap()}
fn count(run:&Path,table:&str)->i64{db(run).query_row(&format!("SELECT count(*) FROM {table}"),[],|r|r.get(0)).unwrap()}

async fn agent(pipe:&str,name:&str,caps:&[&str],world:&str)->String {
    let r=request(pipe,"register_agent",json!({"principal":"system","principal_token":token(72),"name":name,"capabilities":caps,"worlds":[world]})).await;
    assert_eq!(r["ok"],true,"{r}");r["result"]["token"].as_str().unwrap().into()
}
async fn semantic(pipe:&str,actor:&str,key:&str,method:&str,command:Value)->Value {
    request(pipe,method,json!({"actor":actor,"actor_token":key,"command":command})).await
}
fn relation_command(source:&str,target:&str)->Value {
    json!({"world_id":"receipt-A","source_id":source,"target_id":target,"relation_type":"related_to","properties":{},"correlation_id":uuid::Uuid::new_v4().to_string()})
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn real_ipc_file_relations_authority_atomicity_and_restart(){
    assert_eq!(std::env::var("SPACEOS_TEST_ISOLATION_ROOT").unwrap(),r"C:\spaceos");
    assert!(std::env::var("SPACEOS_CORE_TOKEN").unwrap().starts_with("synthetic-"));
    let uid=uuid::Uuid::new_v4();let run=Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().join("test-tmp").join(format!("file-relations-{uid}"));
    let a=run.join("source-A");let b=run.join("source-B");std::fs::create_dir_all(&a).unwrap();std::fs::create_dir_all(&b).unwrap();
    let pipe=format!(r"\\.\pipe\spaceos-4-3a4-admission-{uid}");let config=run.join("child.json");
    std::fs::write(&config,serde_json::to_vec(&json!({"run":run,"root_a":a,"root_b":b,"pipe":pipe,"instance_key":81})).unwrap()).unwrap();
    // Fixture creation uses the ordinary semantic storage contract before Core starts.
    let mut store=Storage::open(run.join("synthetic.db")).unwrap();
    let mut tasks=Vec::new();
    for _ in 0..2 {tasks.push(store.create_semantic_object(&space_core::semantic::CreateSemanticObject{
        world_id:"receipt-A".into(),object_type:"Task".into(),properties:json!({"status":"pending"}),correlation_id:uuid::Uuid::new_v4().to_string()}).unwrap().object.id)}
    drop(store);
    let mut child=start(&config,&run,"relations-before");ready(&pipe).await;register(&pipe,81).await;
    let good=agent(&pipe,"relation-agent",&["semantic.read","semantic.relation.create","semantic.object.patch"],"receipt-A").await;
    let no_read=agent(&pipe,"relation-no-read",&["semantic.relation.create"],"receipt-A").await;
    let no_relation=agent(&pipe,"relation-no-create",&["semantic.read"],"receipt-A").await;
    let foreign=agent(&pipe,"relation-foreign",&["semantic.read","semantic.relation.create"],"receipt-B").await;
    let file=observation(&a,"relation.txt","File");assert_eq!(provider(&pipe,"report_observation",file.clone()).await["ok"],true);
    let file_id=file["objects"][0]["id"].as_str().unwrap();
    let before_object=Storage::open(run.join("synthetic.db")).unwrap().get_object(file_id).unwrap().unwrap();
    let before_receipts=receipts(&run);let before_bytes=std::fs::read(a.join("relation.txt")).unwrap();
    let client=tokio::net::windows::named_pipe::ClientOptions::new().open(&pipe).unwrap();let (read,mut write)=tokio::io::split(client);let mut lines=BufReader::new(read).lines();
    write.write_all((json!({"method":"subscribe","params":{"subscriber":"relation-agent","actor_token":good,"world_id":"receipt-A","event_types":["RelationCreated"]}}).to_string()+"\n").as_bytes()).await.unwrap();
    let ack:Value=serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();assert_eq!(ack["ok"],true);
    let mut successes=Vec::new();let mut notifications=Vec::new();
    for (source,target) in [(&tasks[0][..],file_id),(file_id,&tasks[0][..])] {
        let r=semantic(&pipe,"relation-agent",&good,"create_semantic_relation",relation_command(source,target)).await;
        assert_eq!(r["ok"],true,"{r}");assert_eq!(r["result"]["event"]["actor"],"relation-agent");
        let n:Value=serde_json::from_str(&tokio::time::timeout(Duration::from_secs(3),lines.next_line()).await.unwrap().unwrap().unwrap()).unwrap();
        assert_eq!(n["event"]["id"],r["result"]["event"]["id"]);
        assert_eq!(count(&run,"relations"),successes.len() as i64+1); // persisted before notification consumed
        successes.push(r);notifications.push(n);
    }
    let mut denials=Vec::new();
    for (actor,key) in [("relation-no-read",no_read.as_str()),("relation-no-create",no_relation.as_str()),("relation-foreign",foreign.as_str()),("relation-agent","forged-token")] {
        let r=semantic(&pipe,actor,key,"create_semantic_relation",relation_command(&tasks[0],file_id)).await;assert_eq!(r["ok"],false,"{r}");denials.push(r);
    }
    let r=semantic(&pipe,"relation-no-read",&no_read,"create_semantic_relation",relation_command(file_id,&tasks[0])).await;assert_eq!(r["ok"],false);denials.push(r);
    let mut foreign_command=relation_command(&tasks[0],file_id);foreign_command["world_id"]=json!("receipt-B");
    let r=semantic(&pipe,"relation-agent",&good,"create_semantic_relation",foreign_command).await;assert_eq!(r["ok"],false);denials.push(r);
    assert_eq!(count(&run,"relations"),2);
    // Preserve semantic-to-semantic behavior without adding a read grant.
    let semantic_only=semantic(&pipe,"relation-no-read",&no_read,"create_semantic_relation",relation_command(&tasks[0],&tasks[1])).await;
    assert_eq!(semantic_only["ok"],true,"{semantic_only}");let _=lines.next_line().await.unwrap().unwrap();
    let second=observation(&a,"second.txt","File");assert_eq!(provider(&pipe,"report_observation",second.clone()).await["ok"],true);
    let dir=observation(&a,"directory","Directory");assert_eq!(provider(&pipe,"report_observation",dir.clone()).await["ok"],true);
    let r=semantic(&pipe,"relation-agent",&good,"create_semantic_relation",relation_command(&tasks[0],dir["objects"][0]["id"].as_str().unwrap())).await;assert_eq!(r["ok"],false);denials.push(r);
    let mut fake=file["objects"][0].clone();fake["id"]=json!(uuid::Uuid::new_v4().to_string());fake["properties"]["world_id"]=json!("receipt-A");fake["properties"]["trusted"]=json!(true);
    assert_eq!(provider(&pipe,"create_object",fake.clone()).await["ok"],true);
    let mut relabel=dir["objects"][0].clone();relabel["type"]=json!("File");relabel["properties"]["world_id"]=json!("receipt-A");assert_eq!(provider(&pipe,"update_object",relabel.clone()).await["ok"],true);
    // Admit a real File in B using B's own authenticated source and instance.
    assert_eq!(provider(&pipe,"register_provider_instance",json!({"instance_id":"receipt-instance-B","instance_token":token(82)})).await["ok"],true);
    let mut other=observation(&b,"foreign.txt","File");other["instance_id"]=json!("receipt-instance-B");other["instance_token"]=json!(token(82));
    assert_eq!(provider(&pipe,"report_observation",other.clone()).await["ok"],true);
    let baseline=count(&run,"relations");let event_baseline=count(&run,"events");
    for cmd in [relation_command(file_id,second["objects"][0]["id"].as_str().unwrap()),relation_command(&tasks[0],fake["id"].as_str().unwrap()),relation_command(&tasks[0],relabel["id"].as_str().unwrap()),relation_command(&tasks[0],other["objects"][0]["id"].as_str().unwrap()),{
        let mut c=relation_command(&tasks[0],file_id);c["relation_type"]=json!("contains");c
    },relation_command(&tasks[0],"missing-object")] {
        let r=semantic(&pipe,"relation-agent",&good,"create_semantic_relation",cmd).await;assert_eq!(r["ok"],false,"{r}");denials.push(r);
    }
    assert_eq!(count(&run,"events"),event_baseline);assert_eq!(count(&run,"relations"),baseline);
    let patch=semantic(&pipe,"relation-agent",&good,"patch_semantic_object",json!({"world_id":"receipt-A","object_id":file_id,"properties_patch":{"hacked":true},"correlation_id":uuid::Uuid::new_v4().to_string()})).await;
    assert_eq!(patch["ok"],false);denials.push(patch);
    // Duplicate event identity must roll back the newly generated relation row.
    let mut collision=relation_command(&tasks[0],file_id);collision["correlation_id"]=successes[0]["result"]["event"]["id"].clone();
    let r=semantic(&pipe,"relation-agent",&good,"create_semantic_relation",collision).await;assert_eq!(r["ok"],false);denials.push(r);
    assert_eq!(count(&run,"relations"),baseline);assert_eq!(count(&run,"events"),event_baseline);
    assert!(tokio::time::timeout(Duration::from_millis(150),lines.next_line()).await.is_err());
    let after_object=Storage::open(run.join("synthetic.db")).unwrap().get_object(file_id).unwrap().unwrap();
    assert_eq!(serde_json::to_value(&before_object).unwrap(),serde_json::to_value(&after_object).unwrap());assert_eq!(std::fs::read(a.join("relation.txt")).unwrap(),before_bytes);
    assert!(receipts(&run).contains(&before_receipts[0]));
    drop(lines);drop(write);child.0.kill().unwrap();child.0.wait().unwrap();
    child=start(&config,&run,"relations-after");ready(&pipe).await;
    // Model A registrations are intentionally ephemeral; authenticate again after restart.
    let renewed=agent(&pipe,"relation-agent",&["semantic.read","semantic.relation.create","semantic.object.patch"],"receipt-A").await;
    let query=request(&pipe,"query_relations",json!({"actor":"relation-agent","actor_token":renewed,"world_id":"receipt-A"})).await;assert_eq!(query["ok"],true,"{query}");
    for r in &successes {assert!(query["result"].as_array().unwrap().iter().any(|row|row["id"]==r["result"]["relation"]["id"]));}
    assert_eq!(count(&run,"relations"),baseline);
    let persisted=Storage::open(run.join("synthetic.db")).unwrap().get_object(file_id).unwrap().unwrap();assert_eq!(serde_json::to_value(persisted).unwrap(),serde_json::to_value(before_object).unwrap());
    let mut same=file["objects"][0].clone();same["properties"]["world_id"]=json!("receipt-A");assert_eq!(provider(&pipe,"update_object",same).await["ok"],true);
    let r=semantic(&pipe,"relation-agent",&renewed,"create_semantic_relation",relation_command(&tasks[0],file_id)).await;assert_eq!(r["ok"],false);denials.push(r);assert_eq!(count(&run,"relations"),baseline);
    child.0.kill().unwrap();child.0.wait().unwrap();
    let evidence=json!({"run":run,"pipe":pipe,"successes":successes,"notifications":notifications,"denials":denials,"semantic_without_read":semantic_only,
        "restart_query":query,"file_unchanged_before_legacy_update":true,"legacy_update_invalidates":true,"owned_core_stopped":true,
        "integrity":db(&run).query_row("PRAGMA integrity_check",[],|r|r.get::<_,String>(0)).unwrap()});
    std::fs::write(run.join("provider-file-relations-ipc.json"),serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    println!("FILE_RELATIONS_IPC_PASS world=receipt-A file={file_id} run={}",run.display());
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn real_ipc_admission_restart_and_negative_cases(){
    assert_eq!(std::env::var("SPACEOS_TEST_ISOLATION_ROOT").unwrap(),r"C:\spaceos");
    assert!(std::env::var("SPACEOS_CORE_TOKEN").unwrap().starts_with("synthetic-"));
    let uid=uuid::Uuid::new_v4();let run=Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().join("test-tmp").join(format!("durable-file-ipc-{uid}"));
    let a=run.join("source-A");let b=run.join("source-B");std::fs::create_dir_all(&a).unwrap();std::fs::create_dir_all(&b).unwrap();
    let pipe=format!(r"\\.\pipe\spaceos-4-3a4-admission-{uid}");let config=run.join("child.json");let mut c=json!({"run":run,"root_a":a,"root_b":b,"pipe":pipe,"instance_key":81});
    std::fs::write(&config,serde_json::to_vec(&c).unwrap()).unwrap();let mut child=start(&config,&run,"core-before");ready(&pipe).await;register(&pipe,81).await;
    let reg=request(&pipe,"register_agent",json!({"principal":"system","principal_token":token(72),"name":"receipt-observer","capabilities":["semantic.read"],"worlds":["receipt-A"]})).await;assert_eq!(reg["ok"],true);
    let client=tokio::net::windows::named_pipe::ClientOptions::new().open(&pipe).unwrap();let (read,mut write)=tokio::io::split(client);let mut lines=BufReader::new(read).lines();
    write.write_all((json!({"method":"subscribe","params":{"subscriber":"receipt-observer","actor_token":reg["result"]["token"],"world_id":"receipt-A","event_types":["FileCreated"]}}).to_string()+"\n").as_bytes()).await.unwrap();
    let ack:Value=serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();assert_eq!(ack["ok"],true);
    let file=observation(&a,"file.txt","File");let response=provider(&pipe,"report_observation",file.clone()).await;assert_eq!(response["ok"],true);assert_eq!(response["result"]["applied"],true);
    let notification:Value=serde_json::from_str(&tokio::time::timeout(Duration::from_secs(3),lines.next_line()).await.unwrap().unwrap().unwrap()).unwrap();
    assert_eq!(notification["event"]["id"],file["event"]["id"]);assert_eq!(notification["event"]["world_id"],"receipt-A");
    assert_eq!(receipts(&run).len(),1);assert_eq!(receipts(&run)[0]["object_id"],file["objects"][0]["id"]);
    let directory=observation(&a,"directory","Directory");assert_eq!(provider(&pipe,"report_observation",directory.clone()).await["ok"],true);assert_eq!(receipts(&run).len(),1);
    // Consume the successful Directory publication before negative/no-publication checks.
    let _=tokio::time::timeout(Duration::from_secs(3),lines.next_line()).await.unwrap().unwrap().unwrap();
    let baseline_events=count(&run,"events");let baseline_objects=count(&run,"objects");let mut denials=Vec::new();
    for mode in 0..5{
        let mut bad=observation(&a,&format!("denied-{mode}"),"File");
        match mode{0=>bad["instance_token"]=json!(token(99)),1=>bad["world_id"]=json!("receipt-B"),2=>{bad["objects"][0]["external_id"]=json!(b.join("foreign"));},3=>bad["instance_id"]=json!("unknown"),_=>bad["instance_token"]=json!(std::env::var("SPACEOS_CORE_TOKEN").unwrap())}
        let r=provider(&pipe,"report_observation",bad).await;assert_eq!(r["ok"],false);denials.push(r);
    }
    assert_eq!(count(&run,"events"),baseline_events);assert_eq!(count(&run,"objects"),baseline_objects);assert_eq!(receipts(&run).len(),1);
    assert!(tokio::time::timeout(Duration::from_millis(150),lines.next_line()).await.is_err());
    // A legacy provider-labelled object and Directory relabel cannot mint a receipt.
    let mut fake=file["objects"][0].clone();fake["id"]=json!(uuid::Uuid::new_v4().to_string());fake["properties"]["world_id"]=json!("receipt-A");
    assert_eq!(provider(&pipe,"create_object",fake).await["ok"],true);
    let mut relabel=directory["objects"][0].clone();relabel["type"]=json!("File");relabel["properties"]["world_id"]=json!("receipt-A");assert_eq!(provider(&pipe,"update_object",relabel).await["ok"],true);assert_eq!(receipts(&run).len(),1);
    let replay=provider(&pipe,"report_observation",file.clone()).await;assert_eq!(replay["result"]["applied"],false);
    let before=receipts(&run);let before_events=count(&run,"events");drop(lines);drop(write);child.0.kill().unwrap();child.0.wait().unwrap();
    // Actual child Core restart and instance credential rotation, same DB/authority.
    c["instance_key"]=json!(83);std::fs::write(&config,serde_json::to_vec(&c).unwrap()).unwrap();child=start(&config,&run,"core-after");ready(&pipe).await;
    assert_eq!(receipts(&run),before);assert_eq!(count(&run,"events"),before_events);
    assert_eq!(provider(&pipe,"report_observation",file.clone()).await["ok"],false);register(&pipe,83).await;
    assert_eq!(provider(&pipe,"report_observation",file.clone()).await["ok"],false);
    let mut rotated=file.clone();rotated["instance_token"]=json!(token(83));assert_eq!(provider(&pipe,"report_observation",rotated.clone()).await["result"]["applied"],false);assert_eq!(receipts(&run),before);
    // Same-value legacy UPDATE invalidates; replay still cannot recertify.
    let mut same=file["objects"][0].clone();same["properties"]["world_id"]=json!("receipt-A");assert_eq!(provider(&pipe,"update_object",same).await["ok"],true);assert!(receipts(&run).is_empty());
    assert_eq!(provider(&pipe,"report_observation",rotated).await["result"]["applied"],false);assert!(receipts(&run).is_empty());
    child.0.kill().unwrap();child.0.wait().unwrap();
    let evidence=json!({"run":run,"pipe":pipe,"source_world":"receipt-A","event_response":response,"notification":notification,
        "denials":denials,"receipt_before_restart":before,"receipt_after_legacy_update":receipts(&run),"replay_response":replay,
        "restart_verified":true,"rotated_credential_verified":true,"owned_core_stopped":true,
        "integrity":db(&run).query_row("PRAGMA integrity_check",[],|r|r.get::<_,String>(0)).unwrap()});
    let output=run.join("durable-file-provenance-ipc.json");
    std::fs::write(output,serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    println!("DURABLE_FILE_IPC_PASS world=receipt-A file={} event={} run={}",file["objects"][0]["id"],file["event"]["id"],run.display());
}
