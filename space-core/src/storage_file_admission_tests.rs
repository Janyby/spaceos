use super::*;
use crate::provider_observation::ObservationBindings;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::json;

struct Fixture { storage: Storage, bindings: ObservationBindings, root: std::path::PathBuf }
impl Fixture {
    fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().join("test-tmp")
            .join(format!("file-admission-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let storage = Storage::open(":memory:").unwrap();
        storage.upsert_provider(&ProviderInfo {id:"filesystem".into(),name:"synthetic".into(),capabilities:vec![],version:None}).unwrap();
        let bindings = ObservationBindings::from_json(&json!([{
            "provider":"filesystem","instance_id":"test-instance","instance_token":URL_SAFE_NO_PAD.encode([91;32]),
            "root":root,"world_id":"A"
        }]).to_string()).unwrap();
        assert!(bindings.register_instance("test-instance", &URL_SAFE_NO_PAD.encode([91;32])));
        Self { storage, bindings, root }
    }
    fn observation(&self, name: &str, kind: &str) -> Observation {
        let path=self.root.join(name);
        if kind=="Directory" {std::fs::create_dir_all(&path).unwrap()} else {std::fs::write(&path,b"synthetic").unwrap()}
        let mut object=Object::new(kind,"filesystem",json!({"path":path,"meta":{"test":true}}));
        object.external_id=Some(path.to_string_lossy().into_owned());
        let mut event=Event::new("FileCreated","filesystem",json!({"path":path,"object_id":object.id}));
        event.provider_id=Some("filesystem".into());event.object_ids=vec![object.id.clone()];
        Observation {observation_id:uuid::Uuid::new_v4().to_string(),world_id:String::new(),facts:vec![],objects:vec![object],relations:vec![],delete_ids:vec![],event}
    }
    fn admit(&mut self, obs: Observation) -> Result<ObservationOutcome> {
        let verified=self.bindings.verify_file_created("test-instance",&URL_SAFE_NO_PAD.encode([91;32]),obs,&self.storage)?;
        self.storage.apply_admitted_file_created(&verified)
    }
    fn count(&self, table:&str) -> i64 { self.storage.conn.query_row(&format!("SELECT count(*) FROM {table}"),[],|r|r.get(0)).unwrap() }
    fn change(&self, id:&str, size:u64) -> Observation {
        let mut object=self.storage.get_object(id).unwrap().unwrap();
        object.properties["meta"]=json!({"path":object.external_id,"file_type":"File","size":size,"extension":null,"mime":"text/plain","created":null,"modified":null,"accessed":null});
        let mut event=Event::new("FileChanged","filesystem",json!({"path":object.external_id,"object_id":object.id}));
        event.provider_id=Some("filesystem".into());event.object_ids=vec![object.id.clone()];event.world_id="A".into();
        Observation {observation_id:uuid::Uuid::new_v4().to_string(),world_id:"A".into(),facts:vec![],objects:vec![object],relations:vec![],delete_ids:vec![],event}
    }
    fn apply_change(&mut self, obs:Observation)->Result<ObservationOutcome> {
        let proof=self.bindings.verify_file_changed("test-instance",&URL_SAFE_NO_PAD.encode([91;32]),obs,&self.storage)?;
        self.storage.apply_verified_file_changed(&proof)
    }
}

#[test]
fn verified_change_exact_cas_replay_clock_and_receipt() {
    let mut f=Fixture::new();let created=f.observation("change","File");let id=created.objects[0].id.clone();f.admit(created).unwrap();
    let initial=f.storage.get_object(&id).unwrap().unwrap();let first=f.change(&id,20);let mut stale=first.clone();stale.observation_id=uuid::Uuid::new_v4().to_string();stale.event.id=uuid::Uuid::new_v4().to_string();stale.objects[0].properties["meta"]["size"]=json!(10);
    assert!(f.apply_change(first.clone()).unwrap().applied);let current=f.storage.get_object(&id).unwrap().unwrap();
    assert!(current.updated_at>initial.updated_at);assert_eq!(current.created_at,initial.created_at);assert!(f.storage.has_file_admission(&id).unwrap());
    assert!(f.apply_change(stale).unwrap_err().to_string().contains("conflict:"));assert_eq!(f.count("events"),2);assert_eq!(f.count("observation_log"),2);
    assert!(!f.apply_change(first).unwrap().applied);assert_eq!(f.count("events"),2);
    let mut rollback_clock=f.change(&id,30);rollback_clock.event.timestamp="2000-01-01T00:00:00Z".parse().unwrap();rollback_clock.event.payload["observed_at"]=json!("1990-01-01T00:00:00Z");
    assert!(f.apply_change(rollback_clock).unwrap().applied);assert!(f.storage.get_object(&id).unwrap().unwrap().updated_at>current.updated_at);
    let task=f.storage.create_semantic_object(&crate::semantic::CreateSemanticObject{world_id:"A".into(),object_type:"Task".into(),properties:json!({}),correlation_id:uuid::Uuid::new_v4().to_string()}).unwrap().object;
    f.storage.create_semantic_relation_as(&crate::semantic::CreateSemanticRelation{world_id:"A".into(),source_id:task.id,target_id:id.clone(),relation_type:"related_to".into(),properties:json!({}),correlation_id:uuid::Uuid::new_v4().to_string()},"synthetic-agent").unwrap();
    let same=f.storage.get_object(&id).unwrap().unwrap();f.storage.update_object(&same).unwrap();assert!(f.apply_change(f.change(&id,40)).is_err());
}

#[test]
fn changed_rejections_and_transaction_failures_preserve_everything() {
    let mut f=Fixture::new();let created=f.observation("failure","File");let id=created.objects[0].id.clone();f.admit(created).unwrap();
    let before=serde_json::to_value(f.storage.get_object(&id).unwrap().unwrap()).unwrap();
    for mode in 0..9 {
        let mut change=f.change(&id,20);
        match mode {
            0=>change.objects[0].created_at=Utc::now(),
            1=>change.objects[0].properties["fingerprint"]=json!("forged"),
            2=>change.objects[0].properties["meta"]["size"]=json!(-1),
            3=>change.objects[0].properties["meta"]["error"]=json!("unreadable"),
            4=>change.objects[0].properties["world_id"]=json!("B"),
            5=>change.objects[0].r#type="Directory".into(),
            6=>change.objects[0].id=uuid::Uuid::new_v4().to_string(),
            7=>change.objects[0].properties["meta"]["modified"]=json!("invalid"),
            _=>change.delete_ids.push(id.clone()),
        }
        assert!(f.apply_change(change).is_err(),"mode {mode}");
    }
    f.storage.conn.execute_batch("CREATE TRIGGER change_event_failure BEFORE INSERT ON events WHEN NEW.event_type='FileChanged' BEGIN SELECT RAISE(ABORT,'synthetic event failure'); END").unwrap();
    assert!(f.apply_change(f.change(&id,22)).is_err());
    assert_eq!(serde_json::to_value(f.storage.get_object(&id).unwrap().unwrap()).unwrap(),before);assert!(f.storage.has_file_admission(&id).unwrap());assert_eq!(f.count("events"),1);assert_eq!(f.count("observation_log"),1);
    f.storage.conn.execute_batch("DROP TRIGGER change_event_failure").unwrap();
    let collision=f.change(&id,23);f.storage.record_event(&collision.event).unwrap();assert!(f.apply_change(collision).is_err());assert_eq!(f.count("events"),2);assert_eq!(f.count("observation_log"),1);assert!(f.storage.has_file_admission(&id).unwrap());
    assert_eq!(serde_json::to_value(f.storage.get_object(&id).unwrap().unwrap()).unwrap(),before);
}

#[test]
fn changed_instance_cannot_take_over_another_receipt() {
    let mut f=Fixture::new();let created=f.observation("source","File");let id=created.objects[0].id.clone();f.admit(created).unwrap();
    let other=ObservationBindings::from_json(&json!([{"provider":"filesystem","instance_id":"other","instance_token":URL_SAFE_NO_PAD.encode([92;32]),"root":f.root,"world_id":"A"}]).to_string()).unwrap();
    assert!(other.register_instance("other",&URL_SAFE_NO_PAD.encode([92;32])));
    let proof=other.verify_file_changed("other",&URL_SAFE_NO_PAD.encode([92;32]),f.change(&id,9),&f.storage).unwrap();
    assert!(f.storage.apply_verified_file_changed(&proof).unwrap_err().to_string().contains("source authority"));assert_eq!(f.count("events"),1);assert!(f.storage.has_file_admission(&id).unwrap());
}

#[test]
fn file_version_nanosecond_roundtrip_rollback_and_overflow() {
    let old:chrono::DateTime<Utc>="2300-01-01T00:00:00.123456789Z".parse().unwrap();
    let next=next_file_version(old, "2000-01-01T00:00:00Z".parse().unwrap()).unwrap();
    assert_eq!(next.signed_duration_since(old),chrono::Duration::nanoseconds(1));
    assert_eq!(chrono::DateTime::parse_from_rfc3339(&next.to_rfc3339()).unwrap(),next);
    assert!(next_file_version("9999-12-31T23:59:59.999999999Z".parse().unwrap(),Utc::now()).is_err());
    assert!(next_file_version("2016-12-31T23:59:60Z".parse().unwrap(),Utc::now()).is_err());
    let mut f=Fixture::new();let mut created=f.observation("overflow","File");let id=created.objects[0].id.clone();created.objects[0].updated_at="9999-12-31T23:59:59.999999999Z".parse().unwrap();f.admit(created).unwrap();
    assert!(f.apply_change(f.change(&id,5)).is_err());assert_eq!(f.count("events"),1);assert!(f.storage.has_file_admission(&id).unwrap());
}

#[test]
fn file_change_cas_is_collision_free_after_sqlite_restart() {
    let mut f=Fixture::new();let db=f.root.join("cas.db");f.storage=Storage::open(&db).unwrap();
    f.storage.upsert_provider(&ProviderInfo{id:"filesystem".into(),name:"synthetic".into(),capabilities:vec![],version:None}).unwrap();
    let mut created=f.observation("nanosecond","File");let id=created.objects[0].id.clone();
    created.objects[0].updated_at="2300-01-01T00:00:00.123456789Z".parse().unwrap();f.admit(created).unwrap();
    let first=f.change(&id,5);let mut stale=first.clone();stale.observation_id=uuid::Uuid::new_v4().to_string();stale.event.id=uuid::Uuid::new_v4().to_string();
    f.apply_change(first).unwrap();let version=f.storage.get_object(&id).unwrap().unwrap().updated_at;
    assert_eq!(version.to_rfc3339(),"2300-01-01T00:00:00.123456790+00:00");
    drop(f.storage);f.storage=Storage::open(&db).unwrap();assert_eq!(f.storage.get_object(&id).unwrap().unwrap().updated_at,version);
    assert!(f.apply_change(stale).unwrap_err().to_string().contains("conflict:"));
    f.apply_change(f.change(&id,6)).unwrap();assert_eq!(f.storage.get_object(&id).unwrap().unwrap().updated_at.signed_duration_since(version),chrono::Duration::nanoseconds(1));
    assert!(f.storage.has_file_admission(&id).unwrap());assert_eq!(f.count("events"),3);
}

#[test]
fn verified_file_and_directory_have_distinct_durable_admission() {
    let mut f=Fixture::new();let file=f.observation("one.txt","File");let id=file.objects[0].id.clone();
    assert!(f.admit(file).unwrap().applied);assert!(f.storage.has_file_admission(&id).unwrap());
    let tuple:(String,String,String)=f.storage.conn.query_row("SELECT instance_id,source_root,world_id FROM filesystem_file_admissions",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    assert_eq!(tuple,("test-instance".into(),std::fs::canonicalize(&f.root).unwrap().to_string_lossy().into_owned(),"A".into()));
    let dir=f.observation("directory","Directory");let id=dir.objects[0].id.clone();assert!(f.admit(dir).unwrap().applied);
    assert!(!f.storage.has_file_admission(&id).unwrap());assert_eq!(f.count("filesystem_file_admissions"),1);
}

#[test]
fn generic_forgery_and_directory_relabelling_never_mint() {
    let mut f=Fixture::new();let mut obs=f.observation("forged","File");obs.world_id="A".into();obs.event.world_id="A".into();
    obs.objects[0].properties["world_id"]=json!("A");obs.objects[0].properties["trusted"]=json!(true);
    assert!(f.storage.apply_observation(&obs).unwrap().applied);
    assert!(!f.storage.has_file_admission(&obs.objects[0].id).unwrap());
    let dir=f.observation("dir","Directory");let id=dir.objects[0].id.clone();f.admit(dir).unwrap();
    let mut renamed=f.storage.get_object(&id).unwrap().unwrap();renamed.r#type="File".into();f.storage.update_object(&renamed).unwrap();
    assert!(!f.storage.has_file_admission(&id).unwrap());assert_eq!(f.count("filesystem_file_admissions"),0);
}

#[test]
fn every_object_update_invalidates_even_same_values() {
    for mode in 0..5 {
        let mut f=Fixture::new();let obs=f.observation("update","File");let id=obs.objects[0].id.clone();f.admit(obs).unwrap();
        let mut object=f.storage.get_object(&id).unwrap().unwrap();
        match mode {
            0=>f.storage.create_object(&object).unwrap(),
            1=>{object.r#type="Directory".into();f.storage.update_object(&object).unwrap()},
            2=>f.storage.transaction(|tx|tx.create_object(&object)).unwrap(),
            3=>{let mut o=f.observation("update","File");o.objects=vec![object];f.storage.apply_observation(&o).unwrap();},
            _=>{f.storage.conn.execute("UPDATE objects SET properties=properties WHERE id=?1",params![id]).unwrap();}
        }
        assert!(!f.storage.has_file_admission(&id).unwrap(),"mode {mode}");
        assert_eq!(f.count("filesystem_file_admissions"),0);
    }
}

#[test]
fn rollback_preserves_receipt_and_delete_reuse_cannot_revive_it() {
    let mut f=Fixture::new();let obs=f.observation("rollback","File");let id=obs.objects[0].id.clone();f.admit(obs.clone()).unwrap();
    let object=f.storage.get_object(&id).unwrap().unwrap();
    let r:Result<()>=f.storage.transaction(|tx|{tx.create_object(&object)?;anyhow::bail!("synthetic rollback")});assert!(r.is_err());
    assert!(f.storage.has_file_admission(&id).unwrap());
    let r:Result<()>=f.storage.transaction(|tx|{tx.delete_object_cascade(&id)?;anyhow::bail!("synthetic rollback")});assert!(r.is_err());
    assert!(f.storage.has_file_admission(&id).unwrap());
    f.storage.delete_object(&id).unwrap();assert_eq!(f.count("filesystem_file_admissions"),0);
    assert!(!f.admit(obs.clone()).unwrap().applied);assert!(f.storage.get_object(&id).unwrap().is_none());
    f.storage.create_object(&object).unwrap();assert!(!f.storage.has_file_admission(&id).unwrap());
    assert!(!f.admit(obs.clone()).unwrap().applied);assert!(!f.storage.has_file_admission(&id).unwrap());
    f.storage.delete_object(&id).unwrap();let mut fresh=obs;fresh.observation_id=uuid::Uuid::new_v4().to_string();fresh.event.id=uuid::Uuid::new_v4().to_string();
    assert!(f.admit(fresh).unwrap().applied);assert!(f.storage.has_file_admission(&id).unwrap());
}

#[test]
fn replacement_and_transactional_delete_remove_receipts() {
    let mut f=Fixture::new();let obs=f.observation("replace","File");let id=obs.objects[0].id.clone();f.admit(obs).unwrap();
    f.storage.conn.execute("INSERT OR REPLACE INTO objects SELECT * FROM objects WHERE id=?1",params![id]).unwrap();
    assert!(!f.storage.has_file_admission(&id).unwrap());
    let obs=f.observation("delete","File");let id=obs.objects[0].id.clone();f.admit(obs).unwrap();
    f.storage.transaction(|tx|tx.delete_object_cascade(&id)).unwrap();assert_eq!(f.count("filesystem_file_admissions"),0);
}

#[test]
fn event_and_observation_collisions_are_atomic() {
    let mut f=Fixture::new();let obs=f.observation("collision","File");let id=obs.objects[0].id.clone();
    f.storage.record_event(&obs.event).unwrap();assert!(f.admit(obs).is_err());
    assert!(f.storage.get_object(&id).unwrap().is_none());assert_eq!(f.count("observation_log"),0);assert_eq!(f.count("filesystem_file_admissions"),0);
    let obs=f.observation("legit","File");f.admit(obs.clone()).unwrap();
    let mut forged=f.observation("other","File");forged.observation_id=obs.observation_id.clone();assert!(f.admit(forged).is_err());
    assert_eq!(f.count("observation_log"),1);assert_eq!(f.count("filesystem_file_admissions"),1);
}

#[test]
fn receipt_insert_failure_rolls_back_object_event_and_log() {
    let mut f=Fixture::new();f.storage.conn.execute_batch("CREATE TRIGGER synthetic_receipt_fault BEFORE INSERT ON filesystem_file_admissions BEGIN SELECT RAISE(ABORT,'synthetic receipt fault'); END").unwrap();
    let obs=f.observation("fault","File");assert!(f.admit(obs).is_err());
    for table in ["objects","events","observation_log","filesystem_file_admissions"] {assert_eq!(f.count(table),0,"{table}");}
}

#[test]
fn replay_never_recertifies_and_conflicting_type_is_denied() {
    let mut f=Fixture::new();let obs=f.observation("replay","File");let id=obs.objects[0].id.clone();f.admit(obs.clone()).unwrap();
    assert!(!f.admit(obs.clone()).unwrap().applied);assert_eq!(f.count("events"),1);
    std::fs::remove_file(f.root.join("replay")).unwrap();
    std::fs::create_dir(f.root.join("replay")).unwrap();
    let mut conflict=obs.clone();conflict.objects[0].r#type="Directory".into();
    assert!(f.admit(conflict).is_err());assert!(f.storage.has_file_admission(&id).unwrap());
    std::fs::remove_dir(f.root.join("replay")).unwrap();
    std::fs::write(f.root.join("replay"),b"synthetic").unwrap();
    let object=f.storage.get_object(&id).unwrap().unwrap();f.storage.update_object(&object).unwrap();
    assert!(!f.admit(obs.clone()).unwrap().applied);assert_eq!(f.count("filesystem_file_admissions"),0);
    let mut fresh=obs;fresh.observation_id=uuid::Uuid::new_v4().to_string();fresh.event.id=uuid::Uuid::new_v4().to_string();assert!(f.admit(fresh).is_err());
}

#[test]
fn broken_schema_fails_closed_including_legacy_writes() {
    for mode in 0..6 {
        let mut f=Fixture::new();let obs=f.observation("schema","File");let id=obs.objects[0].id.clone();f.admit(obs.clone()).unwrap();
        match mode {
            0=>f.storage.conn.execute_batch("DROP TRIGGER invalidate_filesystem_file_admission").unwrap(),
            1=>f.storage.conn.execute_batch("DROP TRIGGER invalidate_filesystem_file_admission; CREATE TRIGGER invalidate_filesystem_file_admission AFTER UPDATE ON objects BEGIN SELECT 1; END").unwrap(),
            2=>f.storage.conn.execute_batch("ALTER TABLE filesystem_file_admissions RENAME COLUMN instance_id TO broken_instance").unwrap(),
            3=>f.storage.conn.execute_batch("PRAGMA foreign_keys=OFF").unwrap(),
            4=>f.storage.conn.execute_batch("PRAGMA user_version=6").unwrap(),
            _=>f.storage.conn.execute_batch("DROP TABLE filesystem_file_admissions").unwrap(),
        }
        assert!(f.storage.has_file_admission(&id).is_err());
        let object=f.storage.get_object(&id).unwrap().unwrap();assert!(f.storage.update_object(&object).is_err());
        assert!(f.admit(obs).is_err());
    }
}

#[test]
fn migration_v3_has_no_backfill_and_restart_keeps_new_receipts() {
    let mut f=Fixture::new();let db=f.root.join("synthetic.db");
    let old=Storage::open(&db).unwrap();old.conn.execute_batch("DROP TABLE action_authorities; DROP TRIGGER invalidate_filesystem_file_admission; DROP TABLE filesystem_file_admissions; PRAGMA user_version=3").unwrap();
    old.conn.execute("INSERT INTO objects VALUES ('legacy','File','{\"world_id\":\"A\",\"trusted\":true}','filesystem','legacy','old','old')",[]).unwrap();drop(old);
    f.storage=Storage::open(&db).unwrap();assert_eq!(f.storage.user_version().unwrap(),SCHEMA_VERSION);assert!(!f.storage.has_file_admission("legacy").unwrap());
    f.storage.upsert_provider(&ProviderInfo{id:"filesystem".into(),name:"synthetic".into(),capabilities:vec![],version:None}).unwrap();
    let obs=f.observation("persist","File");let id=obs.objects[0].id.clone();f.admit(obs.clone()).unwrap();
    drop(f.storage);f.storage=Storage::open(&db).unwrap();assert!(f.storage.has_file_admission(&id).unwrap());assert!(!f.admit(obs).unwrap().applied);
    assert_eq!(f.count("filesystem_file_admissions"),1);assert_eq!(f.storage.integrity_check().unwrap(),"ok");
}

#[test]
fn migration_v4_preserves_admission_objects_relations_and_events() {
    let mut f=Fixture::new();let db=f.root.join("migration-v4.db");f.storage=Storage::open(&db).unwrap();
    f.storage.upsert_provider(&ProviderInfo{id:"filesystem".into(),name:"synthetic".into(),capabilities:vec![],version:None}).unwrap();
    let obs=f.observation("preserved","File");let id=obs.objects[0].id.clone();f.admit(obs).unwrap();
    let task=f.storage.create_semantic_object(&crate::CreateSemanticObject{world_id:"A".into(),object_type:"Task".into(),properties:json!({"name":"migration"}),correlation_id:uuid::Uuid::new_v4().to_string()}).unwrap();
    let relation=f.storage.create_semantic_relation(&crate::CreateSemanticRelation{world_id:"A".into(),source_id:task.object.id,target_id:id.clone(),relation_type:"related_to".into(),properties:json!({}),correlation_id:uuid::Uuid::new_v4().to_string()}).unwrap();
    let mission=f.storage.mission_command(&serde_json::from_value(json!({"world_id":"A","command_id":uuid::Uuid::new_v4().to_string(),"operation":{"op":"create","goal":"Preserve migration history","workspace":"synthetic","assignee":"unassigned"}})).unwrap()).unwrap().mission;
    let runtime=crate::provider_observation::BrowserRuntimeConfig{source_id:uuid::Uuid::new_v4().to_string(),runtime_epoch:uuid::Uuid::new_v4().to_string(),world_id:"A".into(),cdp_origin:"http://127.0.0.1:21989".into(),expected_browser_debugger_id:"migration-runtime".into()};
    let mut browser=ObservationBindings::default();let mut entry=serde_json::to_value(&runtime).unwrap();let browser_key=URL_SAFE_NO_PAD.encode([153;32]);entry["provider"]=json!("browser");entry["instance_id"]=json!("migration-browser");entry["instance_token"]=json!(browser_key);
    browser.configure_browser(&json!([entry]).to_string()).unwrap();let authority=browser.browser_authority("migration-browser",&browser_key).unwrap();f.storage.register_browser_authority(&authority).unwrap();
    let file=serde_json::to_value(f.storage.get_object(&id).unwrap()).unwrap();let events=f.count("events");
    f.storage.conn.execute_batch("DROP TABLE action_authorities; PRAGMA user_version=4").unwrap();drop(f.storage);
    f.storage=Storage::open(&db).unwrap();assert_eq!(f.storage.user_version().unwrap(),5);assert!(f.storage.has_file_admission(&id).unwrap());
    assert_eq!(serde_json::to_value(f.storage.get_object(&id).unwrap()).unwrap(),file);assert_eq!(f.count("events"),events);
    assert!(f.storage.query_relations(&Default::default()).unwrap().iter().any(|r|r.id==relation.relation.id));
    assert_eq!(serde_json::to_value(f.storage.get_object(&mission.id).unwrap().unwrap()).unwrap(),serde_json::to_value(mission).unwrap());
    f.storage.validate_browser_authority(&authority).unwrap();
    assert_eq!(f.storage.integrity_check().unwrap(),"ok");
}

#[test]
fn failed_migration_does_not_advance_version_or_repair_unknown_table() {
    let f=Fixture::new();f.storage.conn.execute_batch("DROP TRIGGER invalidate_filesystem_file_admission; DROP TABLE filesystem_file_admissions; CREATE TABLE filesystem_file_admissions (unknown TEXT); PRAGMA user_version=3").unwrap();
    assert!(f.storage.migrate().is_err());assert_eq!(f.storage.user_version().unwrap(),3);
}

#[test]
fn file_relation_receipt_validation_is_transactional_and_fail_closed() {
    let mut f=Fixture::new();let obs=f.observation("relation","File");let file=obs.objects[0].id.clone();f.admit(obs).unwrap();
    let task=f.storage.create_semantic_object(&crate::semantic::CreateSemanticObject{world_id:"A".into(),object_type:"Task".into(),properties:json!({}),correlation_id:uuid::Uuid::new_v4().to_string()}).unwrap().object;
    let command=crate::semantic::CreateSemanticRelation{world_id:"A".into(),source_id:task.id,target_id:file,relation_type:"related_to".into(),properties:json!({}),correlation_id:uuid::Uuid::new_v4().to_string()};
    // Fault after relation INSERT but before receipt-independent success event.
    f.storage.conn.execute_batch("CREATE TRIGGER synthetic_relation_event_failure BEFORE INSERT ON events WHEN NEW.event_type='RelationCreated' BEGIN SELECT RAISE(ABORT,'synthetic event fault'); END").unwrap();
    let before=f.count("events");assert!(f.storage.create_semantic_relation_as(&command,"synthetic-agent").is_err());
    assert_eq!(f.count("relations"),0);assert_eq!(f.count("events"),before);assert_eq!(f.count("filesystem_file_admissions"),1);
    f.storage.conn.execute_batch("DROP TRIGGER synthetic_relation_event_failure; DROP TRIGGER invalidate_filesystem_file_admission").unwrap();
    assert!(f.storage.create_semantic_relation_as(&command,"synthetic-agent").is_err());assert_eq!(f.count("relations"),0);
}
