use super::*;
use crate::provider_observation::*;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD,Engine};
use serde_json::json;
fn config()->BrowserRuntimeConfig {BrowserRuntimeConfig{source_id:"11c3b28d-9262-4729-b324-11af48111001".into(),runtime_epoch:"11c3b28d-9262-4729-b324-11af48111002".into(),world_id:"A".into(),cdp_origin:"http://127.0.0.1:21989".into(),expected_browser_debugger_id:"approved-runtime".into()}}
fn bindings(c:&BrowserRuntimeConfig)->ObservationBindings {
    let mut b=ObservationBindings::default();let mut entry=serde_json::to_value(c).unwrap();entry["provider"]=json!("browser");entry["instance_id"]=json!("test-browser");entry["instance_token"]=json!(URL_SAFE_NO_PAD.encode([151;32]));b.configure_browser(&json!([entry]).to_string()).unwrap();b
}
fn register(s:&mut Storage,b:&ObservationBindings){let a=b.browser_authority("test-browser",&URL_SAFE_NO_PAD.encode([151;32])).unwrap();s.register_browser_authority(&a).unwrap();b.register_browser("test-browser");}
fn apply(s:&mut Storage,b:&ObservationBindings,o:Observation)->Result<(ObservationOutcome,Object,Event)> {let v=b.verify_browser_tab_created("test-browser",&URL_SAFE_NO_PAD.encode([151;32]),o)?;s.apply_verified_browser_tab_created(&v)}
fn count(s:&Storage,t:&str)->i64{s.conn.query_row(&format!("SELECT count(*) FROM {t}"),[],|r|r.get(0)).unwrap()}
#[test]
fn browser_creation_sanitizes_and_replay_adopts_original() {
    let c=config();let b=bindings(&c);let mut s=Storage::open(":memory:").unwrap();register(&mut s,&b);
    let o=browser_tab_observation("test-browser",&c,"target-A","https://example.test/path?not_saved=1#not_saved",Some("synthetic")).unwrap();
    let (out,object,event)=apply(&mut s,&b,o.clone()).unwrap();assert!(out.applied);assert_eq!(object.properties["url"],"https://example.test/path");assert_eq!(count(&s,"filesystem_file_admissions"),0);
    let new=browser_tab_observation("test-browser",&c,"target-A","https://example.test/new",Some("changed")).unwrap();
    let (out,replayed,_)=apply(&mut s,&b,new).unwrap();assert!(!out.applied);assert_eq!(serde_json::to_value(object).unwrap(),serde_json::to_value(replayed).unwrap());assert_eq!(count(&s,"events"),1);assert_eq!(event.actor,"browser");
}
#[test]
fn browser_identity_url_and_shape_are_narrow() {
    let c=config();let b=bindings(&c);let mut s=Storage::open(":memory:").unwrap();register(&mut s,&b);
    let base=browser_tab_observation("test-browser",&c,"target-A","about:blank",None).unwrap();
    for mode in 0..9 {
        let mut o=base.clone();match mode {0=>o.objects[0].id="forged".into(),1=>o.objects[0].properties["html"]=json!("denied"),2=>o.world_id="B".into(),3=>o.objects[0].properties["runtime_epoch"]=json!(uuid::Uuid::new_v4().to_string()),4=>o.facts.push(json!({"html":"denied"})),5=>o.delete_ids.push("x".into()),6=>o.objects.push(o.objects[0].clone()),7=>o.event.event_type="BrowserTabClosed".into(),_=>o.objects[0].r#type="BrowserPage".into()};assert!(apply(&mut s,&b,o).is_err(),"mode {mode}");
    }
    for u in ["file:///x","javascript:alert(1)","https://user:secret@example.test/","chrome://settings","https://example.test/\n"] {assert!(sanitized_browser_url(u).is_err());}
    assert!(sanitized_browser_url(&format!("https://example.test/{}","x".repeat(8192))).is_err());
    assert!(sanitized_browser_url(&format!("https://example.test/{}","x".repeat(4096))).is_err());
    assert!(browser_tab_observation("test-browser",&c,"target-A","about:blank",Some(&"x".repeat(1025))).is_err());
    let a=browser_tab_identity("test-browser",&c,"same").unwrap();let mut different=c.clone();different.source_id=uuid::Uuid::new_v4().to_string();assert_ne!(a.object_id,browser_tab_identity("test-browser",&different,"same").unwrap().object_id);
    different=c.clone();different.runtime_epoch=uuid::Uuid::new_v4().to_string();assert_ne!(a.object_id,browser_tab_identity("test-browser",&different,"same").unwrap().object_id);
    // Independently calculated with Node crypto and u32 BE byte lengths.
    assert_eq!(a.object_id,"browser-tab-v1-b0f019ed6e53f4ddfefffd8661a0d7af7896e84788191c7d40f85f2514a5cc0b");
    assert_eq!(count(&s,"objects"),0);assert_eq!(count(&s,"events"),0);
}
#[test]
fn browser_pin_atomicity_changed_authority_and_runtime_reuse() {
    let c=config();let b=bindings(&c);let mut s=Storage::open(":memory:").unwrap();let a=b.browser_authority("test-browser",&URL_SAFE_NO_PAD.encode([151;32])).unwrap();
    s.conn.execute_batch("CREATE TRIGGER pin_fault BEFORE INSERT ON providers WHEN NEW.id LIKE 'observation-source:browser:%' BEGIN SELECT RAISE(ABORT,'synthetic pin fault'); END").unwrap();
    assert!(s.register_browser_authority(&a).is_err());assert_eq!(count(&s,"providers"),0);s.conn.execute_batch("DROP TRIGGER pin_fault").unwrap();register(&mut s,&b);assert_eq!(count(&s,"providers"),3);
    for field in 0..4 {let mut changed=c.clone();match field{0=>changed.world_id="B".into(),1=>changed.source_id=uuid::Uuid::new_v4().to_string(),2=>changed.cdp_origin="http://127.0.0.1:21990".into(),_=>changed.expected_browser_debugger_id="replacement".into()};let binding=bindings(&changed);let a=binding.browser_authority("test-browser",&URL_SAFE_NO_PAD.encode([151;32])).unwrap();assert!(s.register_browser_authority(&a).is_err());assert_eq!(count(&s,"providers"),3);}
    let mut approved=c.clone();approved.runtime_epoch=uuid::Uuid::new_v4().to_string();approved.expected_browser_debugger_id="new-approved".into();register(&mut s,&bindings(&approved));assert_eq!(count(&s,"providers"),4);
    s.conn.execute("DELETE FROM providers WHERE id=?1",[a.pins()[1].0.as_str()]).unwrap();assert!(s.register_browser_authority(&a).is_err());
}
#[test]
fn browser_legacy_floor_collisions_and_transaction_rollback() {
    let c=config();let b=bindings(&c);let mut s=Storage::open(":memory:").unwrap();
    let mut legacy=browser_tab_observation("test-browser",&c,"legacy","about:blank",None).unwrap();legacy.event.payload=json!({"tab":legacy.objects[0]});
    s.apply_observation(&legacy).unwrap();register(&mut s,&b);assert!(apply(&mut s,&b,legacy.clone()).is_err());assert_eq!(count(&s,"objects"),1);
    let o=browser_tab_observation("test-browser",&c,"fault","about:blank",None).unwrap();s.conn.execute_batch("CREATE TRIGGER tab_fault BEFORE INSERT ON events WHEN NEW.event_type='BrowserTabCreated' BEGIN SELECT RAISE(ABORT,'synthetic tab fault'); END").unwrap();
    assert!(apply(&mut s,&b,o.clone()).is_err());assert!(s.get_object(&o.objects[0].id).unwrap().is_none());assert_eq!(count(&s,"observation_log"),1);
    s.conn.execute_batch("DROP TRIGGER tab_fault").unwrap();s.record_event(&o.event).unwrap();assert!(apply(&mut s,&b,o).is_err());assert_eq!(count(&s,"objects"),1);
    let collision=browser_tab_observation("test-browser",&c,"log-collision","about:blank",None).unwrap();
    s.conn.execute("INSERT INTO observation_log(observation_id,event_id,applied_at) VALUES (?1,?2,?3)",params![collision.observation_id,legacy.event.id,Utc::now().to_rfc3339()]).unwrap();
    let counts=(count(&s,"objects"),count(&s,"events"),count(&s,"observation_log"));
    assert!(apply(&mut s,&b,collision.clone()).is_err());assert!(s.get_object(&collision.objects[0].id).unwrap().is_none());
    assert_eq!(counts,(count(&s,"objects"),count(&s,"events"),count(&s,"observation_log")));
}
#[test]
fn browser_replay_rejects_relabel_delete_and_survives_reopen() {
    let root=Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().join("test-tmp").join(format!("browser-store-{}",uuid::Uuid::new_v4()));std::fs::create_dir_all(&root).unwrap();let db=root.join("synthetic.db");
    let c=config();let b=bindings(&c);let mut s=Storage::open(&db).unwrap();register(&mut s,&b);let o=browser_tab_observation("test-browser",&c,"tab","about:blank",None).unwrap();let (_,obj,_)=apply(&mut s,&b,o.clone()).unwrap();drop(s);
    let mut s=Storage::open(&db).unwrap();let b=bindings(&c);register(&mut s,&b);assert!(!apply(&mut s,&b,o.clone()).unwrap().0.applied);
    let mut corrupt=obj.clone();corrupt.properties["title"]=json!("legacy edit");s.update_object(&corrupt).unwrap();assert!(apply(&mut s,&b,o.clone()).is_err());
    s.delete_object(&obj.id).unwrap();assert!(apply(&mut s,&b,o).is_err());assert_eq!(count(&s,"objects"),0);assert_eq!(count(&s,"events"),1);
}

#[test]
fn browser_credential_rotation_does_not_change_authority_or_identity() {
    let c=config();let b=bindings(&c);let mut s=Storage::open(":memory:").unwrap();register(&mut s,&b);
    let o=browser_tab_observation("test-browser",&c,"rotated","about:blank",None).unwrap();let (_,object,_)=apply(&mut s,&b,o.clone()).unwrap();
    let mut entry=serde_json::to_value(&c).unwrap();entry["provider"]=json!("browser");entry["instance_id"]=json!("test-browser");entry["instance_token"]=json!(URL_SAFE_NO_PAD.encode([152;32]));
    let mut new=ObservationBindings::default();new.configure_browser(&json!([entry]).to_string()).unwrap();
    assert!(new.browser_authority("test-browser",&URL_SAFE_NO_PAD.encode([151;32])).is_err());
    let a=new.browser_authority("test-browser",&URL_SAFE_NO_PAD.encode([152;32])).unwrap();s.register_browser_authority(&a).unwrap();new.register_browser("test-browser");
    let proof=new.verify_browser_tab_created("test-browser",&URL_SAFE_NO_PAD.encode([152;32]),o).unwrap();let (out,replayed,_)=s.apply_verified_browser_tab_created(&proof).unwrap();
    assert!(!out.applied);assert_eq!(object.id,replayed.id);assert_eq!(object.created_at,replayed.created_at);assert_eq!(count(&s,"providers"),3);assert_eq!(count(&s,"events"),1);
}
