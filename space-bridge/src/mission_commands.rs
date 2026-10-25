//! HTTP adapters to the specific Core Mission contract, not observation claims.
use axum::{extract::{State, Path}, http::{HeaderMap,StatusCode}, Json};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use crate::state::BridgeState;
type Api = Result<Json<Value>, (StatusCode,Json<Value>)>;
fn error(code:&str, message:&str) -> (StatusCode,Json<Value>) {
    let status=match code {"authorization_denied"=>StatusCode::FORBIDDEN,"configuration_error"=>StatusCode::SERVICE_UNAVAILABLE,
        "conflict"=>StatusCode::CONFLICT,"not_found"=>StatusCode::NOT_FOUND,"validation_error"=>StatusCode::BAD_REQUEST,_=>StatusCode::INTERNAL_SERVER_ERROR};
    (status,Json(json!({"ok":false,"error_code":code,"error":message})))
}
fn origin(s:&BridgeState,h:&HeaderMap)->Result<(),(StatusCode,Json<Value>)> {
    if h.get("origin").and_then(|v|v.to_str().ok())==Some(s.agent_origin()) {Ok(())}
    else {Err(error("authorization_denied","exact laboratory Origin required"))}
}
#[derive(Deserialize)] #[serde(deny_unknown_fields)]
pub struct Create {goal:String,workspace:String,#[serde(default)]assignee:String,#[serde(default)]command_id:Option<String>}
#[derive(Deserialize)] #[serde(deny_unknown_fields)]
pub struct Plan {steps:Vec<space_core::mission::StepSpec>,#[serde(default)]command_id:Option<String>,#[serde(default)]expected_state:Option<String>}
#[derive(Deserialize)] #[serde(deny_unknown_fields)]
pub struct Transition {to:String,#[serde(default)]extra:Value,#[serde(default)]command_id:Option<String>,#[serde(default)]expected_state:Option<String>}
#[derive(Deserialize)] #[serde(deny_unknown_fields)]
pub struct Verify {passed:bool,method:String,detail:Value,#[serde(default)]expected_attempt_id:Option<String>,#[serde(default)]command_id:Option<String>,#[serde(default)]expected_state:Option<String>}
#[derive(Deserialize)] #[serde(deny_unknown_fields)]
pub struct Step {to:String,#[serde(default)]extra:Value,#[serde(default)]command_id:Option<String>,#[serde(default)]expected_state:Option<String>,#[serde(default)]expected_step_state:Option<String>}
fn parse<T:serde::de::DeserializeOwned>(v:Value)->Result<T,(StatusCode,Json<Value>)> {
    serde_json::from_value(v).map_err(|_|error("validation_error","invalid Mission fields"))
}
async fn expected(id:&str, supplied:Option<String>)->Result<String,(StatusCode,Json<Value>)> {
    if let Some(s)=supplied {return Ok(s)}
    // Old clients have no expected_state; capture current state and still use
    // Core's transactional CAS. New clients should always supply their state.
    let v=crate::mission::get_view(id).await.map_err(|_|error("runtime_error","authorized Mission read failed"))?;
    v.mission["properties"]["state"].as_str().map(str::to_owned).ok_or_else(||error("validation_error","Mission has no state"))
}
async fn submit(s:&BridgeState,operation:Value,id:Option<String>)->Api {
    let world=s.semantic_world_id().map_err(|_|error("configuration_error","Mission World not configured"))?;
    dispatch(s,json!({"world_id":world,"command_id":id.unwrap_or_else(||uuid::Uuid::new_v4().to_string()),"operation":operation})).await
}
async fn dispatch(s:&BridgeState,cmd:Value)->Api {
    let _:space_core::mission::MissionCommand=parse(cmd.clone())?;
    let response=s.semantic_command("mission_command",cmd.clone()).await.map_err(|e| {
        if e.to_string().starts_with("semantic agent registration denied:") {
            error("authorization_denied","Mission service registration denied")
        } else if e.to_string()=="semantic registrar credential missing" {
            error("configuration_error","Mission registrar not configured")
        } else { error("runtime_error","authenticated Mission transport failed") }
    })?;
    if response["ok"]!=true {return Err(error(response["error_code"].as_str().unwrap_or("runtime_error"),response["error"].as_str().unwrap_or("Mission command failed")))}
    // Existing lifecycle cancellation/pause must also close a live Gateway
    // admission lease. Do this only after the requested Core CAS commits.
    if cmd["operation"]["op"]=="transition" && matches!(cmd["operation"]["action"].as_str(),Some("cancel"|"pause"|"fail"|"complete")) {
        let mut slot=s.mission_attempt.lock().await;
        if let Some(attempt)=slot.as_mut() {
            if cmd["operation"]["mission_id"]==attempt.binding.mission_id && attempt.snapshot()["outcome"].is_null() {
                attempt.stop(s,"interrupted").await.map_err(|_|error("runtime_error","lifecycle committed; execution interruption reporting failed"))?;
            }
        }
    }
    let result=&response["result"];
    let id=result["mission"]["id"].as_str().ok_or_else(||error("runtime_error","invalid Core Mission result"))?;
    let view=crate::mission::get_view(id).await.map_err(|_|error("runtime_error","command committed; authorized Mission view unavailable, retry same command_id"))?;
    let mut output=view.to_json();output["applied"]=result["applied"].clone();output["command_id"]=result["command_id"].clone();output["committed_event"]=result["event"].clone();output["execution_status"]=view.mission["properties"]["execution_status"].clone();
    Ok(Json(output))
}
pub async fn command(State(s):State<Arc<BridgeState>>,h:HeaderMap,Json(v):Json<Value>)->Api {origin(&s,&h)?;dispatch(&s,v).await}
pub async fn create(State(s):State<Arc<BridgeState>>,h:HeaderMap,Json(v):Json<Value>)->Api {
    origin(&s,&h)?;let p:Create=parse(v)?;submit(&s,json!({"op":"create","goal":p.goal,"workspace":p.workspace,"assignee":p.assignee}),p.command_id).await
}
pub async fn plan(State(s):State<Arc<BridgeState>>,Path(id):Path<String>,h:HeaderMap,Json(v):Json<Value>)->Api {
    origin(&s,&h)?;let p:Plan=parse(v)?;let e=expected(&id,p.expected_state).await?;
    submit(&s,json!({"op":"plan","mission_id":id,"expected_state":e,"steps":p.steps}),p.command_id).await
}
pub async fn transition(State(s):State<Arc<BridgeState>>,Path(id):Path<String>,h:HeaderMap,Json(v):Json<Value>)->Api {
    origin(&s,&h)?;let p:Transition=parse(v)?;let e=expected(&id,p.expected_state).await?;
    let action=match p.to.as_str() {"running"=>match e.as_str(){"created"|"planned"=>"start","paused"|"failed"=>"resume","awaiting_approval"=>"approve",_=>"unsupported"},"paused"=>"pause","awaiting_approval"=>"request_approval","verifying"=>"begin_verification","completed"=>"complete","failed"=>"fail","cancelled"=>"cancel",_=>"unsupported"};
    let mut op=json!({"op":"transition","mission_id":id,"expected_state":e,"action":action});
    if !p.extra.is_null() {
        let extra=p.extra.as_object().ok_or_else(||error("validation_error","extra must be object"))?;
        for (k,v) in extra {if !matches!(k.as_str(),"reason"|"result") {return Err(error("validation_error","unapproved Mission property"))}op[k]=v.clone();}
    }
    submit(&s,op,p.command_id).await
}
pub async fn verify(State(s):State<Arc<BridgeState>>,Path(id):Path<String>,h:HeaderMap,Json(v):Json<Value>)->Api {
    origin(&s,&h)?;let p:Verify=parse(v)?;let e=expected(&id,p.expected_state).await?;
    let verified=submit(&s,json!({"op":"verify","mission_id":id,"expected_state":e,"expected_attempt_id":p.expected_attempt_id,"passed":p.passed,"method":p.method,"detail":p.detail}),p.command_id).await?;
    if p.passed {
        let _launch_guard=s.mission_attempt.lock().await;
        let view=crate::mission::get_view(&id).await.map_err(|_|error("runtime_error","verified Mission read failed"))?;
        if let Some(attempt)=view.mission["properties"]["attempt_id"].as_str() {
            crate::agent_gateway::reconcile_mission_outcome(&s,&id,attempt).await
                .map_err(|_|error("conflict","verification persisted; completion conditions not satisfied"))?;
            let view=crate::mission::get_view(&id).await.map_err(|_|error("runtime_error","completed Mission read failed"))?;
            return Ok(Json(view.to_json()));
        }
    }
    Ok(verified)
}
pub async fn step(State(s):State<Arc<BridgeState>>,Path((id,step_id)):Path<(String,String)>,h:HeaderMap,Json(v):Json<Value>)->Api {
    origin(&s,&h)?;
    let p:Step=parse(v)?;let e=expected(&id,p.expected_state).await?;
    let es=if let Some(es)=p.expected_step_state {es} else {
        let view=crate::mission::get_view(&id).await.map_err(|_|error("runtime_error","authorized step read failed"))?;
        view.steps.iter().find(|s|s["id"]==step_id).and_then(|s|s["properties"]["state"].as_str()).ok_or_else(||error("not_found","step not found"))?.to_owned()
    };
    let mut op=json!({"op":"step","mission_id":id,"step_id":step_id,"expected_state":e,"expected_step_state":es,"to":p.to});
    if !p.extra.is_null() {for(k,v)in p.extra.as_object().ok_or_else(||error("validation_error","extra must be object"))? {
        if !matches!(k.as_str(),"evidence"|"result"|"action_ids") {return Err(error("validation_error","unapproved step property"))}op[k]=v.clone();
    }}
    submit(&s,op,p.command_id).await
}
pub async fn proposal_unavailable()->Api {Err((StatusCode::NOT_IMPLEMENTED,Json(json!({"ok":false,"error_code":"not_implemented","error":"Mission proposal persistence requires an approved scoped command"}))))}

#[derive(Deserialize)] #[serde(deny_unknown_fields)] pub struct Execute {model:space_core::mission::ExecutionModel}
#[derive(Deserialize)] #[serde(deny_unknown_fields)] pub struct Empty {}
pub async fn execute(State(s):State<Arc<BridgeState>>,Path(id):Path<String>,h:HeaderMap,Json(v):Json<Value>)->Api {
    origin(&s,&h)?;let p:Execute=parse(v)?;
    p.model.validate().map_err(|_|error("validation_error","invalid execution model identifier"))?;
    crate::agent_gateway::start_owned_mission(s,&id,p.model).await.map(Json)
        .map_err(|e|{tracing::warn!(error=%e,"isolated Mission launch failed");error("runtime_error","isolated Mission launch denied or unavailable; no automatic retry")})
}
pub async fn interrupt(State(s):State<Arc<BridgeState>>,Path(id):Path<String>,h:HeaderMap,Json(v):Json<Value>)->Api {
    origin(&s,&h)?;let _:Empty=parse(v)?;
    let mut slot=s.mission_attempt.lock().await;
    let a=slot.as_mut().ok_or_else(||error("not_found","no active Mission attempt"))?;
    if a.binding.mission_id!=id {return Err(error("authorization_denied","Mission attempt mismatch"))}
    a.stop(&s,"interrupted").await.map_err(|_|error("runtime_error","attempt interruption reporting failed"))?;
    Ok(Json(a.snapshot()))
}

pub async fn execution_status(State(s):State<Arc<BridgeState>>,Path(id):Path<String>)->Api {
    let slot=s.mission_attempt.lock().await;
    let a=slot.as_ref().ok_or_else(||error("not_found","no execution in this Gateway"))?;
    if a.binding.mission_id!=id {return Err(error("not_found","Mission execution not found"))}
    Ok(Json(a.snapshot()))
}

pub async fn execution_models(State(s):State<Arc<BridgeState>>)->Api {
    crate::agent_gateway::isolated_execution_models(s).await.map(Json)
        .map_err(|_|error("runtime_error","isolated OpenCode catalog unavailable"))
}
