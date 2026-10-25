use super::*;
use crate::mission::{require, text, MissionCommand, MissionOperation as Op, MissionResult};
use serde_json::{json, Value};

fn state(o: &Object) -> &str { o.properties["state"].as_str().unwrap_or("") }
fn owned(tx: &TransactionStorage<'_>, id: &str, world: &str, kind: &str) -> Result<Object> {
    let o = tx.get_object(id)?.ok_or_else(|| crate::mission::denied("not_found", "mission object not found"))?;
    require(tx.semantic_ownership(id)? == Some((SEMANTIC_ACTOR.into(), world.into()))
        && o.properties["world_id"] == world && o.provider_id == SEMANTIC_ACTOR && o.external_id.is_none(),
        "authorization_denied", "mission ownership or World denied")?;
    require(o.r#type == kind, "validation_error", "incorrect mission object type")?;
    Ok(o)
}

// Legacy manually executed Missions have no attempt. Once an executor attempt
// exists, a manual PASS must not override a failed/interrupted or stale attempt.
fn require_successful_attempt(tx: &TransactionStorage<'_>, mission: &Object, world: &str) -> Result<()> {
    if let Some(id) = mission.properties.get("attempt_id") {
        let id=id.as_str().filter(|s|!s.is_empty()).ok_or_else(||crate::mission::denied("conflict","invalid current attempt"))?;
        require(mission.properties["execution_status"]=="succeeded","conflict","successful current execution required")?;
        let mut stmt=tx.conn.prepare("SELECT id FROM objects WHERE type='MissionAttempt' AND json_extract(properties,'$.attempt_id')=?1")?;
        let ids:Vec<String>=stmt.query_map(params![id],|r|r.get(0))?.collect::<rusqlite::Result<_>>()?;
        require(ids.len()==1,"conflict","current attempt missing or ambiguous")?;
        let attempt=owned(tx,&ids[0],world,"MissionAttempt")?;
        require(attempt.properties["mission_id"]==mission.id && attempt.properties["status"]=="succeeded"
            && attempt.properties["opencode_session_id"]==mission.properties["opencode_session_id"]
            && attempt.properties["model"]==mission.properties["execution_model"],"conflict","successful attempt binding required")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn command(id:&str,operation:Value)->MissionCommand {serde_json::from_value(json!({"world_id":"A","command_id":id,"operation":operation})).unwrap()}
    fn create()->MissionCommand {command("create",json!({"op":"create","goal":"Inspect project tasks","workspace":"synthetic","assignee":"unassigned"}))}
    #[test]
    fn lifecycle_dedup_cas_and_ownership() {
        let mut s=Storage::open(":memory:").unwrap();let c=create();let first=s.mission_command(&c).unwrap();
        assert_eq!(first.event.world_id,"A");assert_eq!(first.mission.provider_id,SEMANTIC_ACTOR);assert!(first.event.seq>0);
        assert!(!s.mission_command(&c).unwrap().applied);
        let start=command("start",json!({"op":"transition","mission_id":first.mission.id,"expected_state":"created","action":"start"}));
        assert_eq!(state(&s.mission_command(&start).unwrap().mission),"running");
        assert_eq!(state(&s.mission_command(&c).unwrap().mission),"running");
        let mut changed=start.clone();changed.command_id="stale".into();assert!(s.mission_command(&changed).is_err());
        changed.command_id="cross".into();changed.world_id="B".into();assert!(s.mission_command(&changed).is_err());
        let pause=command("pause",json!({"op":"transition","mission_id":first.mission.id,"expected_state":"running","action":"pause"}));
        assert_eq!(state(&s.mission_command(&pause).unwrap().mission),"paused");
        let resume=command("resume",json!({"op":"transition","mission_id":first.mission.id,"expected_state":"paused","action":"resume"}));
        assert_eq!(s.mission_command(&resume).unwrap().mission.properties["execution_status"],"not_launched");
        let fail=command("fail",json!({"op":"transition","mission_id":first.mission.id,"expected_state":"running","action":"fail","reason":"Executor unavailable"}));
        assert_eq!(state(&s.mission_command(&fail).unwrap().mission),"failed");
    }
    #[test]
    fn atomic_event_failure_and_partial_plan_rollback() {
        let mut s=Storage::open(":memory:").unwrap();
        s.conn.execute_batch("CREATE TRIGGER fail_mission_event BEFORE INSERT ON events WHEN NEW.event_type='MissionCreated' BEGIN SELECT RAISE(ABORT,'synthetic event failure'); END;").unwrap();
        assert!(s.mission_command(&create()).is_err());assert!(s.query_objects(&Default::default()).unwrap().is_empty());assert_eq!(s.max_seq().unwrap(),0);
        s.conn.execute_batch("DROP TRIGGER fail_mission_event").unwrap();let m=s.mission_command(&create()).unwrap().mission;
        let p=command("plan",json!({"op":"plan","mission_id":m.id,"expected_state":"created","steps":[{"title":"valid","detail":"","requires_approval":false},{"title":"","detail":"","requires_approval":false}]}));
        assert!(s.mission_command(&p).is_err());assert_eq!(s.query_objects(&Default::default()).unwrap().len(),1);assert_eq!(s.max_seq().unwrap(),1);
    }
    #[test]
    fn strict_fields_and_generic_patch_cannot_bypass_lifecycle() {
        assert!(serde_json::from_value::<MissionCommand>(json!({"world_id":"A","command_id":"x","operation":{"op":"create","goal":"g","workspace":"w","assignee":"a","state":"completed"}})).is_err());
        let mut s=Storage::open(":memory:").unwrap();let m=s.mission_command(&create()).unwrap().mission;
        assert!(s.patch_semantic_object(&PatchSemanticObject {object_id:m.id,world_id:"A".into(),properties_patch:json!({"state":"completed"}),correlation_id:"bypass".into()}).is_err());
        assert!(s.create_semantic_object(&CreateSemanticObject {world_id:"A".into(),object_type:"Mission".into(),properties:json!({}),correlation_id:"forged".into()}).is_err());
        let mut c=create();if let Op::Create {ref mut goal,..}=c.operation {*goal="changed".into()};assert!(s.mission_command(&c).is_err());
    }

    fn execution(mission:&Object)->crate::mission::MissionExecution {
        crate::mission::MissionExecution {world_id:"A".into(),mission_id:mission.id.clone(),attempt_id:"attempt-a".into(),
            agent_id:"independent-agent".into(),opencode_session_id:"ses_isolated".into(),status:"registered".into(),admitted:0,
            correlation_id:uuid::Uuid::new_v4().to_string(),result:String::new(),model:None}
    }
    fn running(s:&mut Storage)->Object {
        let m=s.mission_command(&create()).unwrap().mission;
        s.mission_command(&command("start",json!({"op":"transition","mission_id":m.id,"expected_state":"created","action":"start"}))).unwrap().mission
    }
    #[test]
    fn execution_binding_count_and_terminal_are_authoritative() {
        let mut s=Storage::open(":memory:").unwrap();let m=running(&mut s);let mut c=execution(&m);
        let a=s.mission_execution(&c).unwrap();assert_eq!(a.object.r#type,"MissionAttempt");assert!(a.event.seq>0);
        c.correlation_id=uuid::Uuid::new_v4().to_string();assert!(s.mission_execution(&c).is_err());
        c.status="running".into();c.admitted=30;assert!(s.mission_execution(&c).is_ok());
        c.admitted=29;c.correlation_id=uuid::Uuid::new_v4().to_string();assert!(s.mission_execution(&c).is_err());
        c.admitted=30;c.agent_id="impostor".into();assert!(s.mission_execution(&c).is_err());c.agent_id="independent-agent".into();
        c.status="timeout".into();assert!(s.mission_execution(&c).is_ok());
        c.status="succeeded".into();c.correlation_id=uuid::Uuid::new_v4().to_string();assert!(s.mission_execution(&c).is_err());
        assert_eq!(s.get_object(&m.id).unwrap().unwrap().properties["execution_status"],"timeout");
        assert!(s.patch_semantic_object(&PatchSemanticObject {world_id:"A".into(),object_id:a.object.id,properties_patch:json!({"admitted":0}),correlation_id:"reset".into()}).is_err());
    }
    #[test]
    fn execution_and_event_commit_or_rollback_together() {
        let mut s=Storage::open(":memory:").unwrap();let m=running(&mut s);let before=s.max_seq().unwrap();
        s.conn.execute_batch("CREATE TRIGGER fail_attempt_event BEFORE INSERT ON events WHEN NEW.event_type='MissionExecutionChanged' BEGIN SELECT RAISE(ABORT,'synthetic failure'); END;").unwrap();
        assert!(s.mission_execution(&execution(&m)).is_err());
        assert_eq!(s.query_objects(&Default::default()).unwrap().len(),1);assert_eq!(s.max_seq().unwrap(),before);
        assert_eq!(s.get_object(&m.id).unwrap().unwrap().properties["execution_status"],"not_launched");
    }
    #[test]
    fn model_selection_is_immutable_per_attempt_and_explicit_on_retry() {
        use crate::mission::ExecutionModel;
        let mut s=Storage::open(":memory:").unwrap();let m=running(&mut s);let mut c=execution(&m);
        c.model=Some(ExecutionModel{provider_id:"provider-a".into(),model_id:"model-a".into()});
        let first=s.mission_execution(&c).unwrap();assert_eq!(first.object.properties["model"]["model_id"],"model-a");
        c.correlation_id=uuid::Uuid::new_v4().to_string();c.status="running".into();c.model.as_mut().unwrap().model_id="model-b".into();
        assert!(s.mission_execution(&c).is_err());
        c.model.as_mut().unwrap().model_id="model-a".into();c.status="failed".into();c.admitted=4;s.mission_execution(&c).unwrap();
        c.attempt_id="explicit-new-attempt".into();c.opencode_session_id="ses_new".into();c.status="registered".into();c.admitted=0;
        c.model.as_mut().unwrap().model_id="model-b".into();c.correlation_id=uuid::Uuid::new_v4().to_string();
        let second=s.mission_execution(&c).unwrap();assert_ne!(first.object.id,second.object.id);
        assert_eq!(second.object.properties["model"]["model_id"],"model-b");
        let old=s.get_object(&first.object.id).unwrap().unwrap();assert_eq!(old.properties["admitted"],4);assert_eq!(old.properties["status"],"failed");
    }
    #[test]
    fn verification_is_attempt_bound_nonempty_and_deduplicated() {
        let mut s=Storage::open(":memory:").unwrap();
        let mut missions=Vec::new();
        for label in ["A","B"] {
            let m=s.mission_command(&command(&format!("create-{label}"),json!({"op":"create","goal":label,"workspace":"synthetic","assignee":"test"}))).unwrap().mission;
            s.mission_command(&command(&format!("start-{label}"),json!({"op":"transition","mission_id":m.id,"expected_state":"created","action":"start"}))).unwrap();
            let mut x=execution(&m);x.attempt_id=format!("attempt-{label}");x.opencode_session_id=format!("session-{label}");
            s.mission_execution(&x).unwrap();x.status="succeeded".into();x.correlation_id=uuid::Uuid::new_v4().to_string();s.mission_execution(&x).unwrap();
            s.mission_command(&command(&format!("begin-{label}"),json!({"op":"transition","mission_id":m.id,"expected_state":"running","action":"begin_verification"}))).unwrap();
            missions.push(m.id);
        }
        let before_a=s.get_object(&missions[0]).unwrap().unwrap();let seq=s.max_seq().unwrap();
        for (attempt,note) in [(Value::Null,"proof"),(json!("attempt-A"),"proof"),(json!("attempt-B"),"   ")] {
            assert!(s.mission_command(&command(&uuid::Uuid::new_v4().to_string(),json!({"op":"verify","mission_id":missions[1],"expected_state":"verifying","expected_attempt_id":attempt,"passed":true,"method":"manual-check","detail":{"note":note}}))).is_err());
            assert_eq!(s.max_seq().unwrap(),seq);
        }
        let verify=command("verify-B",json!({"op":"verify","mission_id":missions[1],"expected_state":"verifying","expected_attempt_id":"attempt-B","passed":true,"method":"manual-check","detail":{"note":"B evidence"}}));
        assert!(s.mission_command(&verify).unwrap().applied);
        assert!(!s.mission_command(&verify).unwrap().applied);
        let mut duplicate=verify.clone();duplicate.command_id="duplicate-B".into();assert!(s.mission_command(&duplicate).is_err());
        assert_eq!(s.max_seq().unwrap(),seq+1);
        assert_eq!(serde_json::to_value(s.get_object(&missions[0]).unwrap().unwrap()).unwrap(),serde_json::to_value(before_a).unwrap());
    }
    #[test]
    fn executor_success_requires_verified_completion_and_preserves_attempt() {
        let mut s=Storage::open(":memory:").unwrap();let m=running(&mut s);let mut x=execution(&m);
        let a=s.mission_execution(&x).unwrap().object;
        x.status="succeeded".into();x.result="real result".into();x.correlation_id=uuid::Uuid::new_v4().to_string();s.mission_execution(&x).unwrap();
        s.mission_command(&command("verifying",json!({"op":"transition","mission_id":m.id,"expected_state":"running","action":"begin_verification"}))).unwrap();
        let complete=command("complete",json!({"op":"transition","mission_id":m.id,"expected_state":"verifying","action":"complete","result":"verified result"}));
        assert!(s.mission_command(&complete).is_err());
        s.mission_command(&command("verify",json!({"op":"verify","mission_id":m.id,"expected_state":"verifying","expected_attempt_id":x.attempt_id,"passed":true,"method":"actual trace check","detail":{"attempt_id":x.attempt_id}}))).unwrap();
        assert_eq!(state(&s.mission_command(&complete).unwrap().mission),"completed");
        assert!(!s.mission_command(&complete).unwrap().applied);
        let attempt=s.get_object(&a.id).unwrap().unwrap();assert_eq!(attempt.properties["status"],"succeeded");assert_eq!(attempt.properties["opencode_session_id"],x.opencode_session_id);
    }
    #[test]
    fn unsuccessful_attempts_cannot_be_verified_or_completed_even_with_pass_claim() {
        for status in ["registered","running","failed","timeout","interrupted"] {
            let mut s=Storage::open(":memory:").unwrap();let m=running(&mut s);let mut x=execution(&m);s.mission_execution(&x).unwrap();
            if status!="registered" {x.status=status.into();x.correlation_id=uuid::Uuid::new_v4().to_string();s.mission_execution(&x).unwrap();}
            s.mission_command(&command("begin",json!({"op":"transition","mission_id":m.id,"expected_state":"running","action":"begin_verification"}))).unwrap();
            let seq=s.max_seq().unwrap();
            assert!(s.mission_command(&command("false-pass",json!({"op":"verify","mission_id":m.id,"expected_state":"verifying","expected_attempt_id":x.attempt_id,"passed":true,"method":"claim","detail":{}}))).is_err());
            let error=s.mission_command(&command("false-complete",json!({"op":"transition","mission_id":m.id,"expected_state":"verifying","action":"complete","result":"claim"}))).unwrap_err();
            assert!(error.to_string().contains("successful current execution required"));assert_eq!(s.max_seq().unwrap(),seq);
        }
    }
    #[test]
    fn cancellation_and_unfinished_steps_block_successful_executor_completion() {
        let mut s=Storage::open(":memory:").unwrap();let m=s.mission_command(&create()).unwrap().mission;
        s.mission_command(&command("plan",json!({"op":"plan","mission_id":m.id,"expected_state":"created","steps":[{"title":"unfinished","detail":"","requires_approval":false}]}))).unwrap();
        s.mission_command(&command("start",json!({"op":"transition","mission_id":m.id,"expected_state":"planned","action":"start"}))).unwrap();
        let mut x=execution(&m);s.mission_execution(&x).unwrap();x.status="succeeded".into();x.correlation_id=uuid::Uuid::new_v4().to_string();s.mission_execution(&x).unwrap();
        s.mission_command(&command("begin",json!({"op":"transition","mission_id":m.id,"expected_state":"running","action":"begin_verification"}))).unwrap();
        s.mission_command(&command("verify",json!({"op":"verify","mission_id":m.id,"expected_state":"verifying","expected_attempt_id":x.attempt_id,"passed":true,"method":"trace","detail":{}}))).unwrap();
        assert!(s.mission_command(&command("incomplete",json!({"op":"transition","mission_id":m.id,"expected_state":"verifying","action":"complete","result":"x"}))).unwrap_err().to_string().contains("unfinished"));
        s.mission_command(&command("cancel",json!({"op":"transition","mission_id":m.id,"expected_state":"verifying","action":"cancel"}))).unwrap();
        assert!(s.mission_command(&command("late",json!({"op":"transition","mission_id":m.id,"expected_state":"verifying","action":"complete","result":"x"}))).is_err());
        assert_eq!(state(&s.get_object(&m.id).unwrap().unwrap()),"cancelled");
    }
}
fn steps(tx: &TransactionStorage<'_>, id: &str, world: &str) -> Result<Vec<Object>> {
    let mut stmt = tx.conn.prepare("SELECT target_object_id FROM relations WHERE source_object_id=?1 AND relation_type='mission_has_step' AND world_id=?2 ORDER BY id")?;
    let ids: Vec<String> = stmt.query_map(params![id,world], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
    ids.iter().map(|s| owned(tx,s,world,"MissionStep")).collect()
}
fn replay_event(tx: &TransactionStorage<'_>, command_id: &str) -> Result<Option<Event>> {
    let mut stmt = tx.conn.prepare("SELECT seq,id,event_type,actor,provider_id,object_ids,payload,timestamp,correlation_id,world_id FROM events WHERE correlation_id=?1 AND event_type LIKE 'Mission%' AND actor=?2")?;
    let mut rows = stmt.query(params![command_id,SEMANTIC_ACTOR])?;
    if let Some(r) = rows.next()? {
        Ok(Some(Event { seq:r.get::<_,i64>(0)? as u64,id:r.get(1)?,event_type:r.get(2)?,actor:r.get(3)?,provider_id:r.get(4)?,
            object_ids:serde_json::from_str(&r.get::<_,String>(5)?)?,payload:serde_json::from_str(&r.get::<_,String>(6)?)?,
            timestamp:chrono::DateTime::parse_from_rfc3339(&r.get::<_,String>(7)?)?.with_timezone(&Utc),correlation_id:r.get(8)?,world_id:r.get(9)? }))
    } else { Ok(None) }
}
impl Storage {
    /// Existing object/event storage; no additional table or schema migration.
    /// Only the authenticated owner Mission service may report execution.
    pub fn mission_execution(&mut self, c:&crate::mission::MissionExecution)->Result<SemanticObjectResult> {
        if let Some(model)=&c.model { model.validate()?; }
        for (value,name,max) in [(&c.world_id,"world",200),(&c.mission_id,"mission",160),
            (&c.attempt_id,"attempt",160),(&c.agent_id,"agent",160),(&c.opencode_session_id,"session",160),
            (&c.correlation_id,"correlation",160)] { text(value,name,max)?; }
        require(c.admitted<=30 && c.result.len()<=32000,"validation_error","execution limits exceeded")?;
        require(matches!(c.status.as_str(),"registered"|"running"|"succeeded"|"failed"|"timeout"|"interrupted"),"validation_error","invalid execution status")?;
        self.transaction_with_correlation(&c.correlation_id,|tx| {
            let mut mission=owned(tx,&c.mission_id,&c.world_id,"Mission")?;
            let mut stmt=tx.conn.prepare("SELECT id FROM objects WHERE type='MissionAttempt' AND json_extract(properties,'$.attempt_id')=?1")?;
            let ids:Vec<String>=stmt.query_map(params![c.attempt_id],|row|row.get(0))?.collect::<rusqlite::Result<_>>()?;
            require(ids.len()<=1,"conflict","ambiguous execution attempt")?;
            let mut attempt=if let Some(id)=ids.first() {
                let object=owned(tx,id,&c.world_id,"MissionAttempt")?;
                for (field,value) in [("mission_id",&c.mission_id),("agent_id",&c.agent_id),("opencode_session_id",&c.opencode_session_id)] {
                    require(object.properties[field].as_str()==Some(value),"authorization_denied","immutable attempt binding mismatch")?;
                }
                require(c.status!="registered","conflict","attempt cannot be restarted or reset")?;
                require(object.properties.get("model").cloned().unwrap_or(Value::Null)==json!(c.model),"conflict","attempt model cannot change")?;
                require(matches!(object.properties["status"].as_str(),Some("registered"|"running")),"conflict","attempt already terminal")?;
                require(c.admitted>=object.properties["admitted"].as_u64().unwrap_or(31) as u32,"conflict","attempt budget cannot decrease")?;
                object
            } else {
                require(c.status=="registered" && c.admitted==0 && state(&mission)=="running","conflict","new attempt requires running Mission and fresh zero-count registration")?;
                let active:i64=tx.conn.query_row("SELECT count(*) FROM objects WHERE type='MissionAttempt' AND json_extract(properties,'$.mission_id')=?1 AND json_extract(properties,'$.status') IN ('registered','running')",params![c.mission_id],|row|row.get(0))?;
                require(active==0,"conflict","Mission already has an active attempt")?;
                let object=Object::new("MissionAttempt",SEMANTIC_ACTOR,json!({"world_id":c.world_id,"mission_id":c.mission_id,"attempt_id":c.attempt_id,"agent_id":c.agent_id,"opencode_session_id":c.opencode_session_id,"model":c.model}));
                tx.insert_semantic_object(&object,SEMANTIC_ACTOR,&c.world_id)?;object
            };
            attempt.properties["status"]=json!(c.status);attempt.properties["admitted"]=json!(c.admitted);attempt.properties["result"]=json!(c.result);
            attempt.updated_at=Utc::now();tx.update_semantic_properties(&attempt.id,&attempt.properties,attempt.updated_at)?;
            mission.properties["execution_status"]=json!(c.status);mission.properties["attempt_id"]=json!(c.attempt_id);
            mission.properties["execution_model"]=json!(c.model);
            mission.properties["execution_result"]=json!(c.result);
            mission.properties["opencode_session_id"]=json!(c.opencode_session_id);mission.updated_at=Utc::now();
            tx.update_semantic_properties(&mission.id,&mission.properties,mission.updated_at)?;
            let mut event=Event::new("MissionExecutionChanged",SEMANTIC_ACTOR,json!({"semantic_operation":"mission_execution","mission_id":c.mission_id,"attempt_id":c.attempt_id,"agent_id":c.agent_id,"opencode_session_id":c.opencode_session_id,"status":c.status,"admitted":c.admitted,"model":c.model}));
            event.id=c.correlation_id.clone();event.correlation_id=Some(c.attempt_id.clone());event.world_id=c.world_id.clone();event.object_ids=vec![mission.id,attempt.id.clone()];
            require(tx.record_event_if_new(&event)?,"conflict","execution event already committed")?;event.seq=tx.event_seq(&event.id)?;
            Ok(SemanticObjectResult {object:attempt,event})
        })
    }

    /// Authorization is enforced by IPC before entering this transaction. No observation path.
    pub fn mission_command(&mut self, cmd: &MissionCommand) -> Result<MissionResult> {
        text(&cmd.command_id,"command_id",160)?;
        text(&cmd.world_id,"world_id",200)?;
        require(serde_json::to_vec(cmd)?.len() <= 128*1024,"validation_error","mission command too large")?;
        let digest = ring::digest::digest(&ring::digest::SHA256, &serde_json::to_vec(cmd)?);
        let fingerprint: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
        self.transaction_with_correlation(&cmd.command_id, |tx| {
            if let Some(event) = replay_event(tx,&cmd.command_id)? {
                require(event.world_id == cmd.world_id && event.provider_id.as_deref() == Some("space-bridge")
                    && event.payload["fingerprint"] == fingerprint,"conflict","command_id already used with different payload")?;
                let id = event.payload["mission_id"].as_str().ok_or_else(|| anyhow::anyhow!("invalid persisted mission event"))?;
                return Ok(MissionResult {mission:owned(tx,id,&cmd.world_id,"Mission")?,event,applied:false,command_id:cmd.command_id.clone()});
            }
            let mut mission = if let Some((id,expected)) = cmd.operation.target() {
                let m = owned(tx,id,&cmd.world_id,"Mission")?;
                require(state(&m) == expected,"conflict","Mission state changed")?;
                m
            } else { Object::new("Mission",SEMANTIC_ACTOR,json!({"world_id":cmd.world_id,"state":"created","execution_status":"not_launched","owner":SEMANTIC_ACTOR})) };
            let mut affected = vec![mission.id.clone()];
            let event_type = match &cmd.operation {
                Op::Create {goal,workspace,assignee} => {
                    text(goal,"goal",16000)?;text(workspace,"workspace",4000)?;
                    require(assignee.len() <= 1000,"validation_error","assignee too long")?;
                    mission.properties["goal"]=json!(goal);mission.properties["workspace"]=json!(workspace);mission.properties["assignee"]=json!(assignee);
                    tx.insert_semantic_object(&mission,SEMANTIC_ACTOR,&cmd.world_id)?;
                    "MissionCreated"
                },
                Op::Plan {steps:specs,..} => {
                    require(state(&mission)=="created","conflict","plan requires created Mission")?;
                    require(!specs.is_empty() && specs.len()<=100,"validation_error","plan requires 1..100 steps")?;
                    require(steps(tx,&mission.id,&cmd.world_id)?.is_empty(),"conflict","Mission already has steps")?;
                    for (index,s) in specs.iter().enumerate() {
                        text(&s.title,"step title",2000)?;require(s.detail.len()<=16000,"validation_error","step detail too long")?;
                        let step=Object::new("MissionStep",SEMANTIC_ACTOR,json!({"world_id":cmd.world_id,"mission_id":mission.id,"index":index,"title":s.title,"detail":s.detail,"requires_approval":s.requires_approval,"approved":false,"state":"pending"}));
                        tx.insert_semantic_object(&step,SEMANTIC_ACTOR,&cmd.world_id)?;
                        let mut rel=Relation::new(&mission.id,"mission_has_step",&step.id);rel.world_id=cmd.world_id.clone();
                        tx.insert_semantic_relation(&rel)?;affected.push(step.id);
                    }
                    mission.properties["state"]=json!("planned");"MissionPlanned"
                },
                Op::Transition {action,reason,result,..} => {
                    require(reason.is_none() || action=="fail" || action=="cancel","validation_error","reason not allowed for this transition")?;
                    require(result.is_none() || action=="complete","validation_error","result not allowed for this transition")?;
                    let old=state(&mission);
                    let to=match action.as_str() {
                        "start" if matches!(old,"created"|"planned") => "running",
                        "pause" if matches!(old,"running"|"awaiting_approval") => "paused",
                        "resume" if matches!(old,"paused"|"failed") => "running",
                        "request_approval" if old=="running" => "awaiting_approval",
                        "approve" if old=="awaiting_approval" => "running",
                        "begin_verification" if matches!(old,"running"|"awaiting_approval") => "verifying",
                        "complete" if old=="verifying" => "completed",
                        "fail" if matches!(old,"running"|"awaiting_approval"|"verifying") => "failed",
                        "cancel" if matches!(old,"created"|"planned"|"running"|"paused"|"awaiting_approval"|"verifying"|"failed") => "cancelled",
                        _ => return Err(crate::mission::denied("conflict","unsupported Mission transition")),
                    };
                    if action=="complete" {
                        require_successful_attempt(tx,&mission,&cmd.world_id)?;
                        require(mission.properties["verification"]["passed"]==true,"conflict","successful verification required")?;
                        text(result.as_deref().unwrap_or(""),"completion result",16000)?;
                        require(steps(tx,&mission.id,&cmd.world_id)?.iter().all(|s| matches!(state(s),"completed"|"skipped")),"conflict","unfinished or failed steps")?;
                        mission.properties["result"]=json!(result);
                    }
                    if action=="fail" {text(reason.as_deref().unwrap_or(""),"failure reason",16000)?;}
                    if let Some(r)=reason { require(r.len()<=16000,"validation_error","reason too long")?;mission.properties["reason"]=json!(r); }
                    if matches!(action.as_str(),"resume"|"start") {mission.properties.as_object_mut().unwrap().remove("verification");}
                    if action=="cancel" {
                        for mut s in steps(tx,&mission.id,&cmd.world_id)? {
                            if !matches!(state(&s),"completed"|"skipped") {s.properties["state"]=json!("skipped");tx.update_semantic_properties(&s.id,&s.properties,Utc::now())?;affected.push(s.id);}
                        }
                    }
                    mission.properties["state"]=json!(to);"MissionTransitioned"
                },
                Op::Step {step_id,expected_step_state,to,evidence,result,action_ids,..} => {
                    require(state(&mission)=="running","conflict","step changes require running Mission")?;
                    let mut s=owned(tx,step_id,&cmd.world_id,"MissionStep")?;
                    require(s.properties["mission_id"]==mission.id && steps(tx,&mission.id,&cmd.world_id)?.iter().any(|x|x.id==s.id),"authorization_denied","step does not belong to Mission")?;
                    require(state(&s)==expected_step_state,"conflict","step state changed")?;
                    let valid=matches!((state(&s),to.as_str()),("pending","running")|("running","awaiting_approval")|("awaiting_approval","running")|("running","completed")|("running","failed")|("pending","skipped")|("failed","running"));
                    require(valid,"conflict","unsupported step transition")?;
                    if state(&s)=="awaiting_approval" && to=="running" {s.properties["approved"]=json!(true);}
                    if to=="completed" {
                        require(!evidence.is_null() && !result.is_null(),"validation_error","completion evidence and result required")?;
                        require(s.properties["requires_approval"]!=true || s.properties["approved"]==true,"conflict","step approval required")?;
                    }
                    for id in action_ids {
                        let world:Option<String>=tx.conn.query_row("SELECT world_id FROM events WHERE correlation_id=?1 AND event_type IN ('ActionCompleted','ActionFailed') ORDER BY seq DESC LIMIT 1",params![id],|r|r.get(0)).optional()?;
                        require(world.as_deref()==Some(&cmd.world_id),"authorization_denied","action evidence lacks authoritative matching World")?;
                    }
                    s.properties["state"]=json!(to);s.properties["evidence"]=evidence.clone();s.properties["result"]=result.clone();s.properties["action_ids"]=json!(action_ids);
                    tx.update_semantic_properties(&s.id,&s.properties,Utc::now())?;affected.push(s.id);"MissionStepTransitioned"
                },
                Op::Verify {passed,method,detail,expected_attempt_id,..} => {
                    require(state(&mission)=="verifying","conflict","verification requires verifying state")?;text(method,"verification method",1000)?;
                    require(mission.properties["attempt_id"].as_str()==expected_attempt_id.as_deref(),"conflict","verification attempt changed; refresh Mission")?;
                    if method=="manual-check" {text(detail["note"].as_str().unwrap_or(""),"manual verification note",16000)?;}
                    let verification=json!({"passed":passed,"method":method,"detail":detail});
                    require(mission.properties.get("verification")!=Some(&verification),"conflict","verification already recorded")?;
                    if *passed { require_successful_attempt(tx,&mission,&cmd.world_id)?; }
                    mission.properties["verification"]=verification;"MissionVerified"
                },
            };
            if !matches!(cmd.operation,Op::Create {..}) {
                mission.updated_at=Utc::now();tx.update_semantic_properties(&mission.id,&mission.properties,mission.updated_at)?;
            }
            let mut event=Event::new(event_type,SEMANTIC_ACTOR,json!({"semantic_operation":"mission_command","mission_id":mission.id,"command_id":cmd.command_id,"fingerprint":fingerprint,"operation":cmd.operation,"execution_status":"not_launched"}));
            event.world_id=cmd.world_id.clone();event.provider_id=Some("space-bridge".into());event.object_ids=affected;event.correlation_id=Some(cmd.command_id.clone());
            require(tx.record_event_if_new(&event)?,"conflict","event id collision")?;event.seq=tx.event_seq(&event.id)?;
            Ok(MissionResult {mission,event,applied:true,command_id:cmd.command_id.clone()})
        })
    }
}
