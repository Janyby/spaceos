//! Real notify watcher in a child process, separate real Core IPC process,
//! synthetic credentials/roots/database only. Not Bridge/World E2E.
#![cfg(windows)]
use base64::{engine::general_purpose::URL_SAFE_NO_PAD,Engine};
use serde_json::{json,Value};
use space_core::{ipc,principal::PrincipalRegistry,provider_observation::ObservationBindings,service_registration::ServiceRegistrarRegistry,Storage,Object,Event,Observation};
use std::{path::{Path,PathBuf},process::{Child,Command,Stdio},time::{Duration,Instant}};
use tokio::io::{AsyncBufReadExt,AsyncWriteExt,BufReader};
fn key(n:u8)->String {URL_SAFE_NO_PAD.encode([n;32])}

#[test]
fn changed_runtime_child() {
    let Ok(config)=std::env::var("SPACEOS_CHANGED_CHILD_CONFIG") else{return};
    let c:Value=serde_json::from_slice(&std::fs::read(config).unwrap()).unwrap();
    let run=PathBuf::from(c["run"].as_str().unwrap());
    let allowed=std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().join("test-tmp")).unwrap();
    assert!(std::fs::canonicalize(&run).unwrap().starts_with(allowed));
    let pipe=c["pipe"].as_str().unwrap();assert!(pipe.starts_with(r"\\.\pipe\spaceos-4-3a4-changed-"));
    let rt=tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    if std::env::var("SPACEOS_CHANGED_CHILD_MODE").unwrap()=="core" {
        let mut p=PrincipalRegistry::default();
        p.insert("filesystem",[101;32],["provider.register","filesystem.reconcile.read","observation.report","object.update"],["A","B"]).unwrap();
        p.insert("system",[103;32],["agent.register","agent.grant.semantic.read"],["A"]).unwrap();
        let mut state=ipc::CoreState::new_with_semantic_world_and_identities(Storage::open(run.join("synthetic.db")).unwrap(),None,
            &format!("{pipe}-unused-fs"),&format!("{pipe}-unused-browser"),p,ServiceRegistrarRegistry::default(),ipc::core_token());
        std::sync::Arc::get_mut(&mut state).unwrap().observation_bindings=ObservationBindings::from_json(&json!([
            {"provider":"filesystem","instance_id":"changed-A","instance_token":key(102),"root":c["source"],"world_id":"A"},
            {"provider":"filesystem","instance_id":"changed-B","instance_token":key(104),"root":c["foreign"],"world_id":"B"}
        ]).to_string()).unwrap();
        rt.block_on(ipc::run_server(state,pipe)).unwrap();
    } else {
        provider_env();
        let phase=std::env::var("SPACEOS_CHANGED_CHILD_PHASE").unwrap();
        rt.block_on(async {
            ipc::send_provider_principal_request(pipe,"filesystem","register_provider",json!({"id":"filesystem","name":"real synthetic watcher","capabilities":[]})).await.unwrap();
            ipc::send_provider_principal_request(pipe,"filesystem","register_provider_instance",json!({"actor":"filesystem","instance_id":"changed-A","instance_token":key(102)})).await.unwrap();
            let (stop,rx)=tokio::sync::oneshot::channel();
            let root=PathBuf::from(c["source"].as_str().unwrap());let endpoint=pipe.to_string();
            let task=tokio::spawn(async move {filesystem_provider::watcher::watch_on(&root,std::sync::Arc::new(filesystem_provider::scanner::Scanner::new("filesystem")),&endpoint,rx).await});
            tokio::task::yield_now().await;
            assert!(!task.is_finished());
            std::fs::write(run.join(format!("{phase}.ready")),b"watcher armed").unwrap();
            while !run.join(format!("{phase}.stop")).exists(){
                assert!(!task.is_finished());tokio::time::sleep(Duration::from_millis(30)).await;
            }
            stop.send(()).unwrap();task.await.unwrap().unwrap();
        });
    }
}
fn provider_env(){
    std::env::set_var("SPACEOS_PROVIDER_PRINCIPAL_TOKEN",key(101));
    std::env::set_var("SPACEOS_PROVIDER_INSTANCE_ID","changed-A");
    std::env::set_var("SPACEOS_PROVIDER_INSTANCE_TOKEN",key(102));
}
struct Owned(Child);
impl Drop for Owned{fn drop(&mut self){let _=self.0.kill();let _=self.0.wait();}}
fn start(config:&Path,run:&Path,mode:&str,phase:&str)->Owned {
    let log=std::fs::File::create(run.join(format!("{mode}-{phase}.log"))).unwrap();
    Owned(Command::new(std::env::current_exe().unwrap()).args(["--exact","changed_runtime_child","--nocapture"])
        .env("SPACEOS_CHANGED_CHILD_CONFIG",config).env("SPACEOS_CHANGED_CHILD_MODE",mode).env("SPACEOS_CHANGED_CHILD_PHASE",phase)
        .stdout(Stdio::from(log.try_clone().unwrap())).stderr(Stdio::from(log)).spawn().unwrap())
}
fn sql(run:&Path)->rusqlite::Connection {rusqlite::Connection::open_with_flags(run.join("synthetic.db"),rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()}
fn count(run:&Path,table:&str)->i64{sql(run).query_row(&format!("SELECT count(*) FROM {table}"),[],|r|r.get(0)).unwrap()}
fn receipt(run:&Path,id:&str)->Value {
    sql(run).query_row("SELECT object_id,observation_id,event_id,admitted_type,provider_id,world_id,external_id,object_created_at,instance_id,source_root,source_binding_digest FROM filesystem_file_admissions WHERE object_id=?1",[id],|r|{
        let cols=(0..11).map(|i|r.get::<_,String>(i)).collect::<rusqlite::Result<Vec<_>>>()?;Ok(json!(cols))
    }).unwrap()
}
fn object(run:&Path)->Option<Object> {
    let c=sql(run);let mut s=c.prepare("SELECT id,type,properties,provider_id,external_id,created_at,updated_at FROM objects WHERE type='File' AND provider_id='filesystem'").unwrap();
    let result=s.query_map([],|r|Ok(Object{id:r.get(0)?,r#type:r.get(1)?,properties:serde_json::from_str(&r.get::<_,String>(2)?).unwrap(),provider_id:r.get(3)?,external_id:r.get(4)?,created_at:r.get::<_,String>(5)?.parse().unwrap(),updated_at:r.get::<_,String>(6)?.parse().unwrap()})).unwrap().next().map(|r|r.unwrap());
    result
}
async fn ready(pipe:&str){for _ in 0..100{if ipc::send_request(pipe,"ping",json!({})).await.is_ok(){return}tokio::time::sleep(Duration::from_millis(40)).await}panic!("isolated Core not ready")}
async fn wait_marker(run:&Path,phase:&str){for _ in 0..100{if run.join(format!("{phase}.ready")).exists(){return}tokio::time::sleep(Duration::from_millis(40)).await}panic!("isolated watcher not ready")}
async fn stop_provider(p:&mut Owned,run:&Path,phase:&str){std::fs::write(run.join(format!("{phase}.stop")),b"stop").unwrap();let start=Instant::now();loop{if let Some(code)=p.0.try_wait().unwrap(){assert!(code.success());return}assert!(start.elapsed()<Duration::from_secs(5));tokio::time::sleep(Duration::from_millis(30)).await}}
async fn wait_size(run:&Path,size:u64)->Object {for _ in 0..160{if let Some(o)=object(run){if o.properties["meta"]["size"]==size{return o}}tokio::time::sleep(Duration::from_millis(40)).await}panic!("watcher did not persist requested size {size}")}
fn change(base:&Object,path:&Path)->Observation {
    let mut o=base.clone();o.properties["meta"]=filesystem_provider::metadata::stat(path);
    let mut e=Event::new("FileChanged","filesystem",json!({"path":o.external_id,"object_id":o.id}));e.provider_id=Some("filesystem".into());e.object_ids=vec![o.id.clone()];e.world_id="A".into();
    Observation{observation_id:uuid::Uuid::new_v4().to_string(),world_id:"A".into(),facts:vec![],objects:vec![o],relations:vec![],delete_ids:vec![],event:e}
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn real_watcher_cas_authenticated_restart_and_atomicity(){
    assert_eq!(std::env::var("SPACEOS_TEST_ISOLATION_ROOT").unwrap(),r"C:\spaceos");
    assert!(std::env::var("SPACEOS_CORE_TOKEN").unwrap().starts_with("synthetic-"));provider_env();
    let run=Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().join("test-tmp").join(format!("verified-changed-{}",uuid::Uuid::new_v4()));
    let source=run.join("source-A");let foreign=run.join("source-B");std::fs::create_dir_all(&source).unwrap();std::fs::create_dir_all(&foreign).unwrap();
    let pipe=format!(r"\\.\pipe\spaceos-4-3a4-changed-{}",uuid::Uuid::new_v4());let config=run.join("child.json");std::fs::write(&config,serde_json::to_vec(&json!({"run":run,"source":source,"foreign":foreign,"pipe":pipe})).unwrap()).unwrap();
    let mut core=start(&config,&run,"core","before");ready(&pipe).await;
    let reg=ipc::send_request(&pipe,"register_agent",json!({"principal":"system","principal_token":key(103),"name":"changed-reader","capabilities":["semantic.read"],"worlds":["A"]})).await.unwrap();assert_eq!(reg["ok"],true);
    let client=tokio::net::windows::named_pipe::ClientOptions::new().open(&pipe).unwrap();let (read,mut write)=tokio::io::split(client);let mut lines=BufReader::new(read).lines();
    write.write_all((json!({"method":"subscribe","params":{"subscriber":"changed-reader","actor_token":reg["result"]["token"],"world_id":"A","event_types":["FileCreated","FileChanged"]}}).to_string()+"\n").as_bytes()).await.unwrap();
    let ack:Value=serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();assert_eq!(ack["ok"],true);
    let mut watcher=start(&config,&run,"provider","before");wait_marker(&run,"before").await;
    let path=source.join("watched.txt");std::fs::write(&path,b"initial").unwrap();let created=wait_size(&run,7).await;
    let original_receipt=receipt(&run,&created.id);
    let before_modify_seq:i64=sql(&run).query_row("SELECT MAX(seq) FROM events",[],|r|r.get(0)).unwrap();
    std::fs::write(&path,b"a genuine watched modification").unwrap();let modified=wait_size(&run,30).await;
    assert_eq!(created.id,modified.id);assert_eq!(created.created_at,modified.created_at);assert_eq!(modified.properties["world_id"],"A");assert!(modified.updated_at>created.updated_at);assert_eq!(receipt(&run,&created.id),original_receipt);
    let mut notifications=Vec::new();loop{
        let n:Value=serde_json::from_str(&tokio::time::timeout(Duration::from_secs(4),lines.next_line()).await.unwrap().unwrap().unwrap()).unwrap();
        let changed=n["event"]["event_type"]=="FileChanged" && n["event"]["seq"].as_i64().unwrap()>before_modify_seq;
        assert_eq!(n["event"]["world_id"],"A");assert_eq!(n["event"]["actor"],"filesystem");
        let persisted:i64=sql(&run).query_row("SELECT count(*) FROM events WHERE id=?1",[n["event"]["id"].as_str().unwrap()],|r|r.get(0)).unwrap();assert_eq!(persisted,1);
        notifications.push(n);if changed{break}
    }
    stop_provider(&mut watcher,&run,"before").await;
    // Drain successful notify callbacks before asserting no publication on denial.
    loop {
        match tokio::time::timeout(Duration::from_millis(50),lines.next_line()).await {
            Ok(Ok(Some(_))) => {},
            Err(_) => break,
            other => panic!("isolated subscription closed while draining: {other:?}"),
        }
    }
    // Deliberate, separately labelled synthetic IPC observations, not notify evidence.
    let base=object(&run).unwrap();std::fs::write(&path,b"older").unwrap();let stale=change(&base,&path);
    std::fs::write(&path,b"newer synthetic snapshot").unwrap();let mut fresh=change(&base,&path);
    fresh.event.timestamp="1990-01-01T00:00:00Z".parse().unwrap();fresh.event.payload["observed_at"]=json!("1990-01-01T00:00:00Z");
    let success=filesystem_provider::observation::apply_observation(&pipe,&fresh).await.unwrap();assert!(success.applied);
    let committed:Value=serde_json::from_str(&tokio::time::timeout(Duration::from_secs(3),lines.next_line()).await.unwrap().unwrap().unwrap()).unwrap();
    assert_eq!(committed["event"]["id"],fresh.event.id);notifications.push(committed);
    let after=object(&run).unwrap();assert!(after.updated_at>base.updated_at);
    let mut denials=Vec::new();let events=count(&run,"events");let logs=count(&run,"observation_log");
    denials.push(filesystem_provider::observation::apply_observation(&pipe,&stale).await.unwrap_err().to_string());assert!(denials[0].contains("conflict:"));
    let replay=filesystem_provider::observation::apply_observation(&pipe,&fresh).await.unwrap();assert!(!replay.applied);assert_eq!(count(&run,"events"),events);assert_eq!(count(&run,"observation_log"),logs);
    for mode in 0..10 {
        let mut bad=change(&after,&path);provider_env();
        match mode{
            0=>std::env::set_var("SPACEOS_PROVIDER_INSTANCE_TOKEN",key(99)),
            1=>std::env::set_var("SPACEOS_PROVIDER_INSTANCE_TOKEN",key(101)),
            2=>bad.world_id="B".into(),
            3=>bad.objects[0].created_at=chrono::Utc::now(),
            4=>bad.objects[0].properties["unapproved"]=json!(true),
            5=>{bad.objects[0].id=uuid::Uuid::new_v4().to_string();bad.event.object_ids=vec![bad.objects[0].id.clone()];bad.event.payload["object_id"]=json!(bad.objects[0].id)},
            6=>std::env::set_var("SPACEOS_PROVIDER_INSTANCE_ID","unknown"),
            7=>{bad.objects[0].external_id=Some(foreign.join("escape.txt").to_string_lossy().into_owned());bad.objects[0].properties["path"]=json!(bad.objects[0].external_id);bad.event.payload["path"]=json!(bad.objects[0].external_id);std::fs::write(foreign.join("escape.txt"),b"synthetic").unwrap();},
            8=>std::env::set_var("SPACEOS_PROVIDER_PRINCIPAL_TOKEN",key(99)),
            _=>{
                let absent=source.join("unadmitted.txt");std::fs::write(&absent,b"synthetic unadmitted").unwrap();
                bad.objects[0].id=uuid::Uuid::new_v4().to_string();bad.objects[0].external_id=Some(absent.to_string_lossy().into_owned());
                bad.objects[0].properties["path"]=json!(bad.objects[0].external_id);bad.objects[0].properties["meta"]=filesystem_provider::metadata::stat(&absent);
                bad.event.object_ids=vec![bad.objects[0].id.clone()];bad.event.payload["object_id"]=json!(bad.objects[0].id);bad.event.payload["path"]=json!(bad.objects[0].external_id);
            },
        }
        denials.push(filesystem_provider::observation::apply_observation(&pipe,&bad).await.unwrap_err().to_string());
    }
    // Principal proof rejection happens before dispatch in this real IPC path.
    provider_env();assert_eq!(count(&run,"events"),events);assert_eq!(count(&run,"observation_log"),logs);assert_eq!(receipt(&run,&after.id),original_receipt);
    assert_eq!(count(&run,"objects"),1);
    assert!(tokio::time::timeout(Duration::from_millis(100),lines.next_line()).await.is_err());
    let events=count(&run,"events");
    let before_fault=serde_json::to_value(object(&run).unwrap()).unwrap();
    let fault=rusqlite::Connection::open(run.join("synthetic.db")).unwrap();fault.execute_batch("CREATE TRIGGER changed_event_fault BEFORE INSERT ON events WHEN NEW.event_type='FileChanged' BEGIN SELECT RAISE(ABORT,'synthetic changed fault'); END").unwrap();
    denials.push(filesystem_provider::observation::apply_observation(&pipe,&change(&after,&path)).await.unwrap_err().to_string());
    assert_eq!(serde_json::to_value(object(&run).unwrap()).unwrap(),before_fault);assert_eq!(receipt(&run,&after.id),original_receipt);assert_eq!(count(&run,"events"),events);assert_eq!(count(&run,"observation_log"),logs);
    assert!(tokio::time::timeout(Duration::from_millis(100),lines.next_line()).await.is_err());
    fault.execute_batch("DROP TRIGGER changed_event_fault").unwrap();drop(fault);
    drop(lines);drop(write);core.0.kill().unwrap();core.0.wait().unwrap();
    core=start(&config,&run,"core","after");ready(&pipe).await;assert_eq!(receipt(&run,&after.id),original_receipt);
    let mut restarted=start(&config,&run,"provider","after");wait_marker(&run,"after").await;
    std::fs::write(&path,b"restart modification").unwrap();let restarted_object=wait_size(&run,20).await;
    assert_eq!(restarted_object.id,created.id);assert_eq!(restarted_object.created_at,created.created_at);assert!(restarted_object.updated_at>after.updated_at);assert_eq!(receipt(&run,&after.id),original_receipt);
    stop_provider(&mut restarted,&run,"after").await;
    denials.push(filesystem_provider::observation::apply_observation(&pipe,&stale).await.unwrap_err().to_string());assert!(denials.last().unwrap().contains("conflict:"));
    let event_rows:Vec<Value>={let c=sql(&run);let mut stmt=c.prepare("SELECT id,event_type,actor,provider_id,world_id,seq FROM events ORDER BY seq").unwrap();let rows=stmt.query_map([],|r|Ok(json!({"id":r.get::<_,String>(0)?,"type":r.get::<_,String>(1)?,"actor":r.get::<_,String>(2)?,"provider":r.get::<_,Option<String>>(3)?,"world":r.get::<_,String>(4)?,"seq":r.get::<_,i64>(5)?}))).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap();rows};
    core.0.kill().unwrap();core.0.wait().unwrap();
    assert_eq!(count(&run,"objects"),1);
    let evidence=json!({"run":run,"pipe":pipe,"created":created,"modified":modified,"after_cas":after,"after_restart":restarted_object,"receipt":original_receipt,"before_modify_seq":before_modify_seq,
        "notifications":notifications,"events":event_rows,"cas_success":success,"replay":replay,"denials":denials,"owned_children_stopped":true,"integrity":sql(&run).query_row("PRAGMA integrity_check",[],|r|r.get::<_,String>(0)).unwrap()});
    std::fs::write(run.join("file-changed-watcher-ipc.json"),serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    println!("REAL_WATCHER_FILE_CHANGED_PASS run={} file={}",run.display(),created.id);
}
