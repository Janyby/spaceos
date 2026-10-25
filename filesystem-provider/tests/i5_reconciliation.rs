#![cfg(windows)]
use base64::{engine::general_purpose::URL_SAFE_NO_PAD,Engine};
use serde_json::json;
use space_core::{ipc,principal::PrincipalRegistry,provider_observation::ObservationBindings,service_registration::ServiceRegistrarRegistry,Storage};

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn authenticated_source_reads_never_turn_denial_into_absence() {
    let lab=std::env::var("SPACEOS_TEST_ISOLATION_ROOT").unwrap();assert_eq!(lab,r"C:\spaceos");
    let root=std::path::PathBuf::from(&lab).join("test-tmp").join(format!("i5-reconcile-{}",uuid::Uuid::new_v4()));
    let source=root.join("files");std::fs::create_dir_all(&source).unwrap();std::fs::write(source.join("one.txt"),b"synthetic").unwrap();
    let token=URL_SAFE_NO_PAD.encode([42;32]);let instance=URL_SAFE_NO_PAD.encode([43;32]);
    let mut principals=PrincipalRegistry::default();principals.insert("filesystem",[42;32],
        ["provider.register","filesystem.reconcile.read","observation.report","object.update","event.record"],["A"]).unwrap();
    let mut state=ipc::CoreState::new_with_semantic_world_and_identities(Storage::open(&root.join("core.db")).unwrap(),None,
        r"\\.\pipe\spaceos-4-3a4-unused-fs",r"\\.\pipe\spaceos-4-3a4-unused-br",principals,ServiceRegistrarRegistry::default(),ipc::core_token());
    std::sync::Arc::get_mut(&mut state).unwrap().observation_bindings=ObservationBindings::from_json(&json!([{
        "provider":"filesystem","instance_id":"i5-source","instance_token":instance,"root":source,"world_id":"A"}]).to_string()).unwrap();
    let pipe=format!(r"\\.\pipe\spaceos-4-3a4-i5-reconcile-{}",uuid::Uuid::new_v4());let p=pipe.clone();
    let server=tokio::spawn(async move{ipc::run_server(state,&p).await.unwrap()});
    for _ in 0..60 {if ipc::send_request(&pipe,"ping",json!({})).await.is_ok(){break;}tokio::time::sleep(std::time::Duration::from_millis(30)).await;}
    for (method,params) in [("register_provider",json!({"principal":"filesystem","principal_token":token,"id":"filesystem","name":"I5 synthetic","capabilities":[]})),
        ("register_provider_instance",json!({"actor":"filesystem","principal_token":token,"instance_id":"i5-source","instance_token":instance}))] {
        assert_eq!(ipc::send_request(&pipe,method,params).await.unwrap()["ok"],true);
    }
    std::env::set_var("SPACEOS_PROVIDER_PRINCIPAL_TOKEN",&token);
    std::env::set_var("SPACEOS_PROVIDER_INSTANCE_ID","i5-source");std::env::set_var("SPACEOS_PROVIDER_INSTANCE_TOKEN",&instance);
    let scanner=filesystem_provider::scanner::Scanner::new("filesystem");
    // Source root itself is not an admissible child object. Previously the
    // scanner swallowed this Core rejection; it must now reach the caller.
    assert!(scanner.scan_on(&source,&pipe).await.unwrap_err().to_string().contains("source binding"));
    // Admit the child through the existing authenticated Provider path, not
    // a raw storage insert, so read/reconciliation assertions remain real.
    let path=source.join("one.txt");
    let mut file=space_core::Object::new("File","filesystem",json!({"path":path,"meta":filesystem_provider::metadata::stat(&path)}));
    file.external_id=Some(path.to_string_lossy().into_owned());
    let mut event=space_core::Event::new("FileCreated","filesystem",json!({"path":path,"object_id":file.id}));event.provider_id=Some("filesystem".into());event.object_ids=vec![file.id.clone()];
    let obs=space_core::Observation{observation_id:uuid::Uuid::new_v4().to_string(),world_id:String::new(),facts:vec![],objects:vec![file],relations:vec![],delete_ids:vec![],event};
    assert!(filesystem_provider::observation::apply_observation(&pipe,&obs).await.unwrap().applied);
    let mut unsupported=obs.clone();unsupported.observation_id=uuid::Uuid::new_v4().to_string();unsupported.event.id=uuid::Uuid::new_v4().to_string();unsupported.event.event_type="FileUpdated".into();
    assert!(filesystem_provider::observation::apply_observation(&pipe,&unsupported).await.unwrap_err().to_string().contains("unsupported filesystem observation shape"));
    let first=filesystem_provider::reconciliation_read::objects(&pipe,&source,None).await.unwrap();assert_eq!(first.len(),1);
    assert!(scanner.scan_on(&source,&pipe).await.is_err());
    let second=filesystem_provider::reconciliation_read::objects(&pipe,&source,None).await.unwrap();assert_eq!(second.len(),1);assert_eq!(first[0].id,second[0].id);
    assert!(filesystem_provider::reconciliation_read::objects(&pipe,&root,None).await.unwrap_err().to_string().contains("invalid_binding"));
    std::env::set_var("SPACEOS_PROVIDER_INSTANCE_TOKEN",URL_SAFE_NO_PAD.encode([44;32]));
    assert!(scanner.scan_on(&source,&pipe).await.unwrap_err().to_string().contains("authorization_denied"));
    std::env::set_var("SPACEOS_PROVIDER_INSTANCE_TOKEN",&instance);
    std::env::set_var("SPACEOS_PROVIDER_PRINCIPAL_TOKEN",ipc::core_token());
    assert!(scanner.scan_on(&source,&pipe).await.is_err());
    std::env::set_var("SPACEOS_PROVIDER_PRINCIPAL_TOKEN",&token);
    let missing=format!(r"\\.\pipe\spaceos-4-3a4-i5-absent-{}",uuid::Uuid::new_v4());
    assert!(filesystem_provider::reconciliation_read::objects(&missing,&source,None).await.unwrap_err().to_string().contains("transport_error"));
    std::fs::remove_file(source.join("one.txt")).unwrap();
    assert_eq!(filesystem_provider::reconciliation_read::objects(&pipe,&source,None).await.unwrap().len(),1,"missing physical file remains visible for reconciliation");
    // Deliberately damage only this disposable test database to exercise an
    // actual SQLite query failure, rather than mocking a database error.
    let database=rusqlite::Connection::open(root.join("core.db")).unwrap();
    database.execute("DROP TABLE objects",[]).unwrap();
    assert!(filesystem_provider::reconciliation_read::objects(&pipe,&source,None).await.unwrap_err().to_string().contains("database_error"));
    server.abort();println!("I5 reconciliation PASS: scan rejection propagated, child authenticated admission, repeated read identity, authorization denial, invalid source, transport error, missing file != missing semantic object");
}
