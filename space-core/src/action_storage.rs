//! Private durable action authority. Generic events/objects cannot mint a row.
use super::*;
use crate::action::{ActionCompletion, ActionStatus, DispatchRequest};
use serde::{Deserialize, Serialize};

const TABLE: &str = "CREATE TABLE action_authorities (
    action_id TEXT PRIMARY KEY NOT NULL,
    correlation_id TEXT NOT NULL,
    actor TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    world_id TEXT NOT NULL,
    source_root TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    source_digest TEXT NOT NULL,
    operation_json TEXT NOT NULL,
    operation_digest TEXT NOT NULL CHECK(length(operation_digest)=64),
    performer TEXT NOT NULL CHECK(length(performer)>0),
    binding_id TEXT NOT NULL UNIQUE CHECK(length(binding_id)>0),
    state TEXT NOT NULL CHECK(state IN ('reserved','started','completed','failed')),
    terminal_json TEXT,
    terminal_digest TEXT,
    terminal_event_id TEXT UNIQUE REFERENCES events(id),
    CHECK((state IN ('reserved','started') AND terminal_json IS NULL AND terminal_digest IS NULL AND terminal_event_id IS NULL)
       OR (state IN ('completed','failed') AND terminal_json IS NOT NULL AND terminal_digest IS NOT NULL AND terminal_event_id IS NOT NULL))
)";

pub(crate) fn digest(bytes: &[u8]) -> String {
    ring::digest::digest(&ring::digest::SHA256, bytes).as_ref().iter().map(|b|format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::CompletionBinding;
    fn authority() -> ActionAuthority {
        let mut r=crate::ActionRequest::new("scan",serde_json::json!({"path":"synthetic"}),"filesystem","synthetic-agent");
        r.actor_token="synthetic-private-bearer".into();
        let mut operation=DispatchRequest::from(&r);
        let binding_id=uuid::Uuid::new_v4().to_string();
        operation.completion=Some(CompletionBinding {binding_id:binding_id.clone(),instance_id:"instance".into(),source_root:"synthetic".into(),world_id:"world-A".into(),source_digest:"binding".into()});
        ActionAuthority {operation,performer:"performer".into(),binding_id,state:"reserved".into()}
    }
    fn completion(a:&ActionAuthority)->ActionCompletion {ActionCompletion {action_id:a.operation.id.clone(),binding_id:a.binding_id.clone(),performer:a.performer.clone(),outcome_id:uuid::Uuid::new_v4().to_string(),status:ActionStatus::Completed,detail:serde_json::json!({"synthetic":true})}}
    #[test]
    fn fresh_schema_and_credential_free_private_reservation() {
        let s=Storage::open(":memory:").unwrap();let a=authority();s.reserve_action(&a).unwrap();
        assert_eq!(s.user_version().unwrap(),5);assert!(s.reserve_action(&a).is_err());
        let op:String=s.conn.query_row("SELECT operation_json FROM action_authorities",[],|r|r.get(0)).unwrap();
        assert!(!op.contains("actor_token") && !op.contains("synthetic-private-bearer"));
        let fake=Event::new("ActionStarted","synthetic",serde_json::json!({"action_id":"fake"}));s.record_event(&fake).unwrap();
        assert!(s.action_authority("fake").unwrap().is_none());
    }
    #[test]
    fn started_and_terminal_are_atomic_and_exact_replays_are_noops() {
        let mut s=Storage::open(":memory:").unwrap();let a=authority();let c=completion(&a);s.reserve_action(&a).unwrap();
        assert!(s.finish_action(&c,true).unwrap_err().to_string().contains("Started not confirmed"));
        s.conn.execute_batch("CREATE TRIGGER fail_started BEFORE INSERT ON events WHEN NEW.event_type='ActionStarted' BEGIN SELECT RAISE(ABORT,'synthetic'); END").unwrap();
        assert!(s.start_action(&a.operation.id,&serde_json::json!({})).is_err());assert_eq!(s.action_authority(&a.operation.id).unwrap().unwrap().state,"reserved");
        s.conn.execute_batch("DROP TRIGGER fail_started").unwrap();s.start_action(&a.operation.id,&serde_json::json!({})).unwrap();
        s.conn.execute_batch("CREATE TRIGGER fail_terminal BEFORE INSERT ON events WHEN NEW.event_type='ActionCompleted' BEGIN SELECT RAISE(ABORT,'synthetic'); END").unwrap();
        assert!(s.finish_action(&c,true).is_err());assert_eq!(s.action_authority(&a.operation.id).unwrap().unwrap().state,"started");
        s.conn.execute_batch("DROP TRIGGER fail_terminal").unwrap();
        let e=s.finish_action(&c,true).unwrap().unwrap();assert_eq!(e.actor,a.operation.actor);assert_eq!(e.world_id,"world-A");assert!(e.seq>0);
        assert!(s.finish_action(&c,true).unwrap().is_none());
        let mut conflict=c.clone();conflict.status=ActionStatus::Failed;assert!(s.finish_action(&conflict,true).is_err());
        conflict=c.clone();conflict.performer="other".into();assert!(s.finish_action(&conflict,true).is_err());
    }
    #[test]
    fn migration_v4_restart_pending_replay_and_rollback() {
        let root=std::env::temp_dir().join(format!("action-v5-{}",uuid::Uuid::new_v4()));std::fs::create_dir_all(&root).unwrap();let db=root.join("synthetic.db");
        let mut s=Storage::open(&db).unwrap();let legacy=Event::new("ActionStarted","legacy",serde_json::json!({}));s.record_event(&legacy).unwrap();
        s.conn.execute_batch("DROP TABLE action_authorities; PRAGMA user_version=4").unwrap();drop(s);
        s=Storage::open(&db).unwrap();assert_eq!(s.user_version().unwrap(),5);assert_eq!(s.query_events(&Default::default()).unwrap().len(),1);assert!(s.action_authority(&legacy.id).unwrap().is_none());
        let a=authority();let c=completion(&a);s.reserve_action(&a).unwrap();s.start_action(&a.operation.id,&serde_json::json!({})).unwrap();drop(s);
        s=Storage::open(&db).unwrap();assert_eq!(s.action_authority(&a.operation.id).unwrap().unwrap().state,"started");s.finish_action(&c,true).unwrap();drop(s);
        s=Storage::open(&db).unwrap();assert!(s.finish_action(&c,true).unwrap().is_none());assert_eq!(s.integrity_check().unwrap(),"ok");
        s.conn.execute_batch("DROP TABLE action_authorities; CREATE TABLE action_authorities (forged TEXT); PRAGMA user_version=4").unwrap();
        assert!(s.migrate().is_err());assert_eq!(s.user_version().unwrap(),4);assert_eq!(s.query_events(&Default::default()).unwrap().len(),3);
        // Preserve only this newly generated synthetic evidence; no external cleanup.
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ActionAuthority {
    pub operation: DispatchRequest,
    pub performer: String,
    pub binding_id: String,
    pub state: String,
}

impl Storage {
    pub(super) fn migrate_action_authority(&self) -> Result<()> {
        verify_file_admission_schema(&self.conn)?;
        let tx = self.conn.unchecked_transaction()?;
        tx.execute_batch(TABLE)?; // A pre-existing lookalike is not adopted.
        tx.pragma_update(None,"user_version",5)?;
        tx.commit()?;
        Ok(())
    }
    pub(super) fn verify_action_schema(&self) -> Result<()> {
        anyhow::ensure!(self.user_version()? == 5, "action authority requires schema v5");
        let sql:Option<String> = self.conn.query_row("SELECT sql FROM sqlite_master WHERE type='table' AND name='action_authorities'",[],|r|r.get(0)).optional()?;
        anyhow::ensure!(sql.as_deref().map(str::trim)==Some(TABLE.trim()),"action authority schema unavailable");
        anyhow::ensure!(self.conn.query_row("PRAGMA foreign_keys",[],|r|r.get::<_,i64>(0))?==1,"action authority requires foreign keys");
        Ok(())
    }
    pub(crate) fn reserve_action(&self, a: &ActionAuthority) -> Result<()> {
        self.verify_action_schema()?;
        anyhow::ensure!(!a.operation.id.is_empty() && a.operation.id.len()<=256 && !a.performer.is_empty()
            && a.state=="reserved", "invalid action reservation");
        let op=serde_json::to_string(&a.operation)?;
        let b=a.operation.completion.as_ref();
        self.conn.execute("INSERT INTO action_authorities
            (action_id,correlation_id,actor,provider_id,world_id,source_root,instance_id,source_digest,operation_json,operation_digest,performer,binding_id,state)
            VALUES (?1,?12,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,'reserved')",
            params![a.operation.id,a.operation.actor,a.operation.provider_id,
                b.map(|b|b.world_id.as_str()).unwrap_or_else(||a.operation.target.get("world_id").or_else(||a.operation.target.pointer("/runtime/world_id")).and_then(serde_json::Value::as_str).unwrap_or("")),
                b.map(|b|b.source_root.as_str()).unwrap_or(""), b.map(|b|b.instance_id.as_str()).unwrap_or_else(||a.operation.target.get("instance_id").and_then(serde_json::Value::as_str).unwrap_or("")),
                b.map(|b|b.source_digest.clone()).unwrap_or_else(||digest(a.operation.target.get("runtime").unwrap_or(&serde_json::Value::Null).to_string().as_bytes())),op,digest(op.as_bytes()),a.performer,a.binding_id,a.operation.correlation_id])?;
        Ok(())
    }
    pub(crate) fn action_authority(&self, id: &str) -> Result<Option<ActionAuthority>> {
        self.verify_action_schema()?;
        let row: Option<(String,String,String,String,String)> = self.conn.query_row(
            "SELECT operation_json,operation_digest,performer,binding_id,state FROM action_authorities WHERE action_id=?1",
            params![id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
        row.map(|(op,hash,performer,binding_id,state)|{
            anyhow::ensure!(digest(op.as_bytes())==hash,"corrupt action authority");
            Ok(ActionAuthority {operation:serde_json::from_str(&op)?,performer,binding_id,state})
        }).transpose()
    }
    pub(crate) fn start_action(&mut self, id: &str, detail: &serde_json::Value) -> Result<Event> {
        let a=self.action_authority(id)?.ok_or_else(||anyhow::anyhow!("unknown action"))?;
        self.transaction(|tx| {
            anyhow::ensure!(tx.conn.execute("UPDATE action_authorities SET state='started' WHERE action_id=?1 AND state='reserved'",params![id])?==1,"action not reserved");
            let mut e=Event::new("ActionStarted",&a.operation.actor,detail.clone());
            e.provider_id=Some(a.operation.provider_id.clone());e.correlation_id=Some(a.operation.correlation_id.clone());
            e.world_id=a.operation.completion.as_ref().map(|b|b.world_id.clone()).unwrap_or_default();
            anyhow::ensure!(tx.record_event_if_new(&e)?,"Started event collision");e.seq=tx.event_seq(&e.id)?;Ok(e)
        })
    }
    pub(crate) fn finish_action(&mut self, c:&ActionCompletion, async_callback:bool) -> Result<Option<Event>> {
        let a=self.action_authority(&c.action_id)?.ok_or_else(||anyhow::anyhow!("unknown action"))?;
        anyhow::ensure!(a.performer==c.performer && a.binding_id==c.binding_id,"completion binding mismatch");
        anyhow::ensure!(matches!(c.status,ActionStatus::Completed|ActionStatus::Failed),"invalid terminal outcome");
        uuid::Uuid::parse_str(&c.outcome_id)?;
        let data=serde_json::to_string(c)?;
        anyhow::ensure!(data.len()<=65536,"completion too large");let hash=digest(data.as_bytes());
        self.transaction(|tx| {
            let (state,old):(String,Option<String>)=tx.conn.query_row("SELECT state,terminal_json FROM action_authorities WHERE action_id=?1",params![c.action_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
            if matches!(state.as_str(),"completed"|"failed") {
                anyhow::ensure!(old.as_deref()==Some(&data),"conflicting terminal completion");return Ok(None);
            }
            anyhow::ensure!(state==if async_callback {"started"} else {"reserved"},"completion not ready: Started not confirmed");
            let kind=if c.status==ActionStatus::Completed {"Completed"} else {"Failed"};
            let mut e=Event::new(format!("{}{kind}",if async_callback {"Action"} else {"Dispatch"}),&a.operation.actor,c.detail.clone());
            e.payload["performer_instance"]=serde_json::json!(a.performer);
            e.payload["action"]=serde_json::json!(a.operation.action);
            e.payload["outcome_id"]=serde_json::json!(c.outcome_id);
            e.id=format!("action-terminal:{}",a.binding_id);e.provider_id=Some(a.operation.provider_id.clone());e.correlation_id=Some(a.operation.correlation_id.clone());
            e.world_id=a.operation.completion.as_ref().map(|b|b.world_id.clone()).unwrap_or_else(||a.operation.target.get("world_id").or_else(||a.operation.target.pointer("/runtime/world_id")).and_then(serde_json::Value::as_str).unwrap_or("").into());
            anyhow::ensure!(tx.record_event_if_new(&e)?,"terminal event collision");e.seq=tx.event_seq(&e.id)?;
            anyhow::ensure!(tx.conn.execute("UPDATE action_authorities SET state=?2,terminal_json=?3,terminal_digest=?4,terminal_event_id=?5 WHERE action_id=?1 AND state=?6",
                params![c.action_id,kind.to_lowercase(),data,hash,e.id,state])?==1,"completion transition conflict");Ok(Some(e))
        })
    }
}
