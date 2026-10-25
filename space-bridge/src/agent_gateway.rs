//! Server-side client for an already running OpenCode v2 HTTP server.
//!
//! Legacy chat connects to an existing server; the explicit MVP laboratory
//! path additionally owns one disposable stdio runtime. Credentials stay server-side;
//! browser clients only see the sanitized Bridge API. Only loopback OpenCode
//! URLs are accepted so the agent control surface cannot be pointed at an
//! external network service by configuration error.

use anyhow::{anyhow, Context, Result};
use reqwest::{Client, RequestBuilder, Url};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::time::Duration;

const URL_ENV: &str = "OPENCODE_SERVER_URL";
const USER_ENV: &str = "OPENCODE_SERVER_USERNAME";
const PASSWORD_ENV: &str = "OPENCODE_SERVER_PASSWORD";

/// Immutable, trusted startup binding, not deserialized from tool arguments.
/// This initial MVP lease is ephemeral: a Gateway restart cannot recreate it.
#[derive(Clone, serde::Serialize)]
pub struct AttemptBinding {
    pub mission_id: String,
    pub attempt_id: String,
    pub world_id: String,
    pub agent_id: String,
    pub opencode_session_id: String,
}

#[derive(Clone)]
pub struct MissionRuntimeConfig {
    pub home_root: std::path::PathBuf,
    pub catalog: std::path::PathBuf,
    pub agent_id: String,
    pub actor_token: String,
    pub max_duration: Duration,
}

pub async fn isolated_execution_models(state:std::sync::Arc<crate::BridgeState>)->Result<Value> {
    let cfg=state.mission_runtime.clone().context("isolated Mission runtime not configured")?;
    let slot=state.mission_attempt.lock().await;
    if slot.as_ref().is_some_and(|a|!a.cleanup_complete) {
        let models=state.mission_models.read().await.clone();
        anyhow::ensure!(!models.is_empty(),"catalog unavailable during active attempt");
        return Ok(json!({"models":models,"source":"OpenCode","cached":true}));
    }
    if existing_cli_enabled() {
        let gateway=AgentGateway::existing_cli(None)?;
        let info=gateway.info().await?;
        anyhow::ensure!(info["version"]=="2.1.3","unexpected existing OpenCode version");
        let models=gateway.execution_models().await?;
        *state.mission_models.write().await=models.clone();
        return Ok(json!({"models":models,"source":"OpenCode existing environment","cached":false}));
    }
    let root=std::fs::canonicalize(r"C:\spaceos\test-tmp")?;
    let parent=std::fs::canonicalize(&cfg.home_root)?;
    anyhow::ensure!(parent.starts_with(&root)&&parent!=root,"unsafe catalog home");
    let home=parent.join(format!("catalog-{}",uuid::Uuid::new_v4()));std::fs::create_dir(&home)?;
    let mut runtime=OwnedOpenCode::launch_isolated(&home,&cfg.catalog,mission_opencode_config())?;
    let models=runtime.gateway.execution_models().await?;
    let cleanup=runtime.shutdown("ses_catalog_idle").await?;
    *state.mission_models.write().await=models.clone();
    Ok(json!({"models":models,"source":"OpenCode","cached":false,"cleanup":cleanup}))
}

/// The existing Gateway owns launch, session, MCP binding and monitor. No
/// external executor/broker or user-selected runtime credentials are involved.
pub async fn start_owned_mission(state:std::sync::Arc<crate::BridgeState>,mission_id:&str,model:space_core::mission::ExecutionModel)->Result<Value> {
    model.validate()?;
    let cfg=state.mission_runtime.clone().context("isolated Mission runtime not configured")?;
    anyhow::ensure!(std::env::var("SPACEOS_TEST_ISOLATION_ROOT").ok().as_deref()==Some(r"C:\spaceos"),"isolated execution only");
    let root=std::fs::canonicalize(r"C:\spaceos\test-tmp")?;
    let home_root=std::fs::canonicalize(&cfg.home_root)?;
    anyhow::ensure!(home_root.starts_with(&root)&&home_root!=root,"unsafe executor home");
    anyhow::ensure!(cfg.max_duration>Duration::ZERO && cfg.max_duration<=Duration::from_secs(600),"invalid execution budget");
    let mut slot=state.mission_attempt.lock().await;
    anyhow::ensure!(slot.as_ref().is_none_or(|a|a.outcome.is_some()&&a.cleanup_complete),"another disposable attempt is active or cleaning up");
    let view=crate::mission::get_view(mission_id).await?;
    let properties=&view.mission["properties"];
    let expected=properties["state"].as_str().context("Mission has no state")?;
    let retry=matches!(properties["execution_status"].as_str(),Some("failed"|"timeout"|"interrupted"));
    anyhow::ensure!(matches!(expected,"created"|"planned") || (retry&&matches!(expected,"running"|"failed"|"paused")),"Mission cannot start/retry in its current state");
    let goal=properties["goal"].as_str().context("Mission goal missing")?.to_owned();
    let world=state.semantic_world_id()?.to_owned();
    let attempt_id=uuid::Uuid::new_v4().to_string();let began=std::time::Instant::now();
    let home=home_root.join(&attempt_id);std::fs::create_dir(&home)?;
    let mut runtime=MissionOpenCode::connect(&home,&cfg.catalog)?;
    let info=runtime.gateway().info().await?;
    if let MissionOpenCode::Owned(owned)=&runtime {
        let expected_tmp=std::fs::canonicalize(home.join("tmp"))?;
        anyhow::ensure!(info["version"]=="2.0.26" && info["pid"].as_u64()==Some(owned.pid() as u64)
            && info["paths"]["tmp"].as_str().and_then(|p|std::fs::canonicalize(p).ok()).is_some_and(|p|p.starts_with(&expected_tmp)),"runtime identity/isolation mismatch");
    }
    std::fs::write(home.join("runtime-identity.json"),serde_json::to_vec_pretty(&json!({"version":info["version"],"pid":info["pid"],"existing_environment":existing_cli_enabled(),"mission_directory":home.join("project")}))?)?;
    let public_models=runtime.gateway().execution_models().await?;
    *state.mission_models.write().await=public_models.clone();
    std::fs::write(home.join("runtime-model-catalog.json"),serde_json::to_vec_pretty(&public_models)?)?;
    let selected_public=public_models.iter().find(|m|m["provider_id"]==model.provider_id && m["model_id"]==model.model_id)
        .context("selected tool-capable model unavailable or unauthorized in OpenCode; no fallback")?;
    std::fs::write(home.join("runtime-model.json"),serde_json::to_vec_pretty(&json!({"info":info,"selected_model":selected_public}))?)?;
    let session=runtime.gateway().create_disposable(home.join("project").to_str().context("non-UTF8 project path")?,
        json!({"id":model.model_id,"providerID":model.provider_id}),
        json!({"mission_id":mission_id,"attempt_id":attempt_id,"world_id":world,"agent_id":cfg.agent_id,"model":model})).await?;
    let session_id=session["data"]["id"].as_str().context("OpenCode session ID missing")?.to_owned();
    runtime.claim_session(&session_id);
    anyhow::ensure!(session["data"]["model"]["id"]==model.model_id && session["data"]["model"]["providerID"]==model.provider_id,"OpenCode did not preserve selected model");
    if expected!="running" {
        let action=if retry {"resume"}else{"start"};
        let start=state.semantic_command("mission_command",json!({"world_id":world,"command_id":uuid::Uuid::new_v4().to_string(),
            "operation":{"op":"transition","mission_id":mission_id,"expected_state":expected,"action":action}})).await?;
        anyhow::ensure!(start["ok"]==true,"Core rejected Mission start/retry");
    }
    let binding=AttemptBinding {mission_id:mission_id.into(),attempt_id,world_id:world,agent_id:cfg.agent_id,
        opencode_session_id:session_id.clone()};
    let credential=format!("{}{}",uuid::Uuid::new_v4().simple(),uuid::Uuid::new_v4().simple());
    let duration=cfg.max_duration.checked_sub(began.elapsed()).context("attempt startup deadline expired")?;
    let mut attempt=BoundAttempt::new(binding.clone(),credential.clone(),cfg.actor_token,duration)?;
    attempt.model=Some(model.clone());
    attempt.begin_persistence(&state).await?;
    let file=home.join("mcp-attempt.secret.json");
    std::fs::write(&file,serde_json::to_vec(&json!({"endpoint":"http://127.0.0.1:21973/internal/mission-mcp","credential":credential}))?)?;
    let configured=runtime.gateway().configure_mcp(json!({"type":"local","command":[r"C:\spaceos\target\debug\spaceos-mcp.exe"],
        "cwd":home.join("project"),"environment":{"SPACEOS_TEST_ISOLATION_ROOT":r"C:\spaceos","SPACEOS_MCP_ATTEMPT_FILE":file},
        "codemode":false,"protocol":"legacy"})).await;
    if configured.is_err() {attempt.stop(&state,"failed").await?;let _=runtime.shutdown(&session_id).await;return Err(anyhow!("Mission MCP configuration failed"))}
    let mcp_deadline=std::time::Instant::now()+Duration::from_secs(20);
    let mut connected=false;
    while std::time::Instant::now()<mcp_deadline && std::time::Instant::now()<attempt.deadline {
        if let Ok(status)=runtime.gateway().mcp_status().await {
            // Version-pinned API exposes connected entries. No session creation
            // or pending status is mistaken for a connected tool transport.
            if status["data"].as_array().is_some_and(|items|items.iter().any(|entry|
                entry["name"]=="spaceos" && entry["status"]["status"]=="connected")) {connected=true;break}
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !connected {attempt.stop(&state,"failed").await?;let _=runtime.shutdown(&session_id).await;anyhow::bail!("isolated MCP did not connect")}
    // Install the lease before admitting the prompt. MCP configuration does
    // only initialize/tools/list, which cannot mutate or allocate a budget.
    *slot=Some(attempt);drop(slot);
    if let Err(error)=runtime.gateway().prompt(&session_id,&goal).await {
        let mut slot=state.mission_attempt.lock().await;slot.as_mut().unwrap().stop(&state,"failed").await?;
        let _=runtime.shutdown(&session_id).await;return Err(error)
    }
    let output=json!({"accepted":true,"binding":binding,"model":model,"execution_status":"running"});
    let clone=state.clone();tokio::spawn(async move {
        match monitor_mission_attempt(clone,runtime).await {
            Ok(_)=>tracing::info!("isolated Mission attempt stopped and reported"),
            Err(_)=>tracing::error!("isolated Mission monitoring/reporting failed; no automatic retry"),
        }
    });
    Ok(output)
}

pub struct BoundAttempt {
    pub binding: AttemptBinding,
    credential: String,
    actor_token: String,
    deadline: std::time::Instant,
    admitted: u32,
    outcome: Option<String>,
    persisted: bool,
    calls: Vec<Value>,
    model: Option<space_core::mission::ExecutionModel>,
    result: String,
    cleanup_complete: bool,
}

impl BoundAttempt {
    pub fn new(binding: AttemptBinding, credential: String, actor_token: String, duration: Duration) -> Result<Self> {
        anyhow::ensure!(!binding.mission_id.is_empty() && !binding.attempt_id.is_empty()
            && !binding.world_id.is_empty() && !binding.agent_id.is_empty()
            && !binding.opencode_session_id.is_empty(), "incomplete trusted attempt binding");
        anyhow::ensure!(credential.len() >= 32 && !actor_token.is_empty(), "invalid attempt credentials");
        anyhow::ensure!(duration > Duration::ZERO && duration <= Duration::from_secs(600), "attempt deadline exceeds maximum");
        Ok(Self {binding,credential,actor_token,deadline:std::time::Instant::now()+duration,admitted:0,outcome:None,persisted:false,calls:Vec::new(),model:None,result:String::new(),cleanup_complete:false})
    }

    async fn report(&self, state:&crate::BridgeState, status:&str, result:&str) -> Result<()> {
        let response=state.semantic_command("mission_execution",json!({
            "world_id":self.binding.world_id,"mission_id":self.binding.mission_id,
            "attempt_id":self.binding.attempt_id,"agent_id":self.binding.agent_id,
            "opencode_session_id":self.binding.opencode_session_id,"status":status,
            "admitted":self.admitted,"model":self.model,"result":result,"correlation_id":uuid::Uuid::new_v4().to_string()
        })).await?;
        anyhow::ensure!(response["ok"]==true,"authoritative attempt report denied");Ok(())
    }

    pub async fn begin_persistence(&mut self, state:&crate::BridgeState)->Result<()> {
        anyhow::ensure!(!self.persisted,"attempt already initialized");
        self.report(state,"registered","").await?;self.persisted=true;Ok(())
    }

    pub async fn end(&mut self,state:&crate::BridgeState,status:&str,result:&str)->Result<()> {
        if std::time::Instant::now() >= self.deadline {
            self.stop(state,"timeout").await?;
            anyhow::bail!("attempt expired; late completion rejected");
        }
        self.finish(status)?;
        self.report(state,status,result).await
    }

    pub async fn stop(&mut self,state:&crate::BridgeState,status:&str)->Result<()> {
        self.stop_with_result(state,status,"").await
    }

    pub async fn stop_with_result(&mut self,state:&crate::BridgeState,status:&str,result:&str)->Result<()> {
        if self.outcome.is_some() {return Ok(())}
        anyhow::ensure!(matches!(status,"timeout"|"interrupted"|"failed"),"invalid stop status");
        self.close(status);self.result=result.into();self.report(state,status,result).await
    }

    fn authenticates(&self, presented: &str) -> bool {
        // Do not leak the binding credential through errors or derived Debug.
        presented.len() == self.credential.len() && presented.bytes().zip(self.credential.bytes())
            .fold(0u8, |acc,(a,b)| acc | (a ^ b)) == 0
    }

    pub fn finish(&mut self, outcome: &str) -> Result<()> {
        anyhow::ensure!(self.outcome.is_none(), "attempt already terminal");
        if std::time::Instant::now() >= self.deadline {
            self.outcome = Some("timeout".into());
            anyhow::bail!("attempt deadline expired; late completion rejected");
        }
        anyhow::ensure!(matches!(outcome,"succeeded"|"failed"|"interrupted"), "invalid terminal outcome");
        self.outcome=Some(outcome.into()); Ok(())
    }

    pub fn close(&mut self, outcome: &str) {
        if self.outcome.is_none() { self.outcome=Some(outcome.into()); }
    }

    pub fn snapshot(&self) -> Value {
        json!({"binding":self.binding,"admitted":self.admitted,"limit":30,
            "outcome":self.outcome,"expired":std::time::Instant::now() >= self.deadline,"calls":self.calls,"model":self.model,"result":self.result,"cleanup_complete":self.cleanup_complete})
    }

    /// Hold the attempt mutex across dispatch. Counting, validation and closing
    /// cannot race with another MCP connection or deadline/cancellation latch.
    pub async fn dispatch(&mut self, state:&crate::BridgeState, credential: &str, name: &str, args: Value) -> Result<Value> {
        anyhow::ensure!(self.authenticates(credential), "invalid attempt credential");
        anyhow::ensure!(self.persisted,"attempt not registered in Core");
        if std::time::Instant::now() >= self.deadline {
            self.stop(state,"timeout").await?;
            anyhow::bail!("attempt deadline expired");
        }
        anyhow::ensure!(self.outcome.is_none(), "attempt closed");
        anyhow::ensure!(self.admitted < 30, "attempt tool-call ceiling reached");
        self.admitted+=1; // Count failed, malformed and Core-denied tools too.
        self.calls.push(json!({"invocation":self.admitted,"name":name,"arguments":args}));
        if let Err(error)=self.report(state,"running","").await {
            self.close("failed");return Err(error);
        }
        if std::time::Instant::now() >= self.deadline {
            self.stop(state,"timeout").await?;
            anyhow::bail!("attempt expired before tool dispatch");
        }
        let obj=args.as_object().context("tool arguments must be an object")?;
        let allowed: &[&str] = match name {
            "spaceos_query_objects" | "spaceos_query_relations" => &[],
            "spaceos_get_object" => &["id"],
            "spaceos_patch_object" => &["object_id","properties_patch"],
            "spaceos_create_relation" => &["source_id","target_id","relation_type","properties"],
            _ => anyhow::bail!("unsupported attempt tool"),
        };
        anyhow::ensure!(obj.keys().all(|k| allowed.contains(&k.as_str())), "unapproved tool arguments");
        let (method,mut params)=match name {
            "spaceos_query_objects" => ("query_objects",json!({"world_id":self.binding.world_id})),
            "spaceos_query_relations" => ("query_relations",json!({"world_id":self.binding.world_id})),
            "spaceos_get_object" => ("get_object",json!({"world_id":self.binding.world_id,"id":args["id"]})),
            "spaceos_patch_object" => ("patch_semantic_object",json!({"command":{
                "world_id":self.binding.world_id,"object_id":args["object_id"],"properties_patch":args["properties_patch"],
                "correlation_id":uuid::Uuid::new_v4().to_string()}})),
            _ => ("create_semantic_relation",json!({"command":{
                "world_id":self.binding.world_id,"source_id":args["source_id"],"target_id":args["target_id"],
                "relation_type":args["relation_type"],"properties":args.get("properties").cloned().unwrap_or(json!({})),
                "correlation_id":uuid::Uuid::new_v4().to_string()}})),
        };
        params["actor"]=json!(self.binding.agent_id);
        params["actor_token"]=json!(self.actor_token);
        let remaining=self.deadline.saturating_duration_since(std::time::Instant::now());
        let response=tokio::time::timeout(remaining,crate::ipc_client::core_request(method,params)).await
            .context("attempt deadline expired during Core dispatch")??;
        if let Some(call)=self.calls.last_mut() {call["core_method"]=json!(method);call["core_response"]=response.clone();}
        anyhow::ensure!(response["ok"]==true, "Core rejected scoped agent operation");
        Ok(json!({"binding":self.binding,"invocation":self.admitted,"data":response["result"]}))
    }
}

#[derive(Clone)]
pub struct AgentGateway {
    client: Client,
    base_url: Url,
    username: Option<String>,
    password: Option<String>,
    /// Supported CLI handles normal service authentication internally. No
    /// Desktop credential is read, copied or passed to Bridge.
    cli: Option<std::path::PathBuf>,
    location: Option<std::path::PathBuf>,
}

fn existing_cli_enabled()->bool { std::env::var("SPACEOS_EXISTING_OPENCODE").ok().as_deref()==Some("1") }

/// A shared Desktop process is never owned by SpaceOS. Only its new Mission
/// session and location-scoped MCP entry may be interrupted/disconnected.
enum MissionOpenCode {
    Owned(OwnedOpenCode),
    Existing {gateway:AgentGateway,home:std::path::PathBuf,session:Option<String>},
}
impl MissionOpenCode {
    fn connect(home:&std::path::Path,catalog:&std::path::Path)->Result<Self> {
        if !existing_cli_enabled() {return Ok(Self::Owned(OwnedOpenCode::launch_isolated(home,catalog,mission_opencode_config())?))}
        let project=home.join("project");std::fs::create_dir(&project)?;
        // Project-local agent controls only. Normal user configuration,
        // provider settings and credential stores are neither replaced nor copied.
        let config=mission_opencode_config();
        std::fs::write(project.join("opencode.json"),serde_json::to_vec_pretty(&config)?)?;
        Ok(Self::Existing {gateway:AgentGateway::existing_cli(Some(project))?,home:home.into(),session:None})
    }
    fn gateway(&self)->&AgentGateway {match self {Self::Owned(r)=>&r.gateway,Self::Existing{gateway,..}=>gateway}}
    fn home(&self)->&std::path::Path {match self {Self::Owned(r)=>&r.home,Self::Existing{home,..}=>home}}
    fn claim_session(&mut self,id:&str) {if let Self::Existing{session,..}=self {*session=Some(id.into());}}
    fn has_exited(&mut self)->Result<bool> {match self {Self::Owned(r)=>r.has_exited(),Self::Existing{..}=>Ok(false)}}
    async fn shutdown(&mut self,id:&str)->Result<Value> {
        if let Self::Owned(r)=self {return r.shutdown(id).await}
        let Self::Existing{gateway,session,..}=self else {unreachable!()};
        anyhow::ensure!(session.as_deref()==Some(id),"refusing to interrupt an unowned session");
        let interrupt=tokio::time::timeout(Duration::from_secs(15),gateway.interrupt(id)).await;
        let interrupted=matches!(interrupt,Ok(Ok(_)));
        let mut stopped=false;
        let end=std::time::Instant::now()+Duration::from_secs(15);
        while std::time::Instant::now()<end {
            if let Ok(Ok(info))=tokio::time::timeout(Duration::from_secs(5),gateway.session(id)).await {
                if matches!(info["data"]["outcome"].as_str(),Some("succeeded"|"failed"|"interrupted")) {stopped=true;break}
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let removed=gateway.cli_json("DELETE","experimental/mcp/spaceos",None,true).await.is_ok();
        anyhow::ensure!(stopped&&removed,"Mission session cleanup unconfirmed; shared OpenCode was NOT terminated");
        Ok(json!({"existing_environment":true,"interrupt_request_succeeded":interrupted,"session_stopped":stopped,"mcp_removed":removed,"desktop_terminated":false,"forced_owned_tree":false}))
    }
}

#[derive(Debug)]
pub struct SessionSnapshot {
    pub sessions: Vec<Value>,
    pub active: Value,
}

/// OS-specific ownership is kept out of Mission/Core semantics. The child
/// handle and stdio lease always belong to this Gateway, never a discovered PID.
pub struct OwnedOpenCode {
    child: std::process::Child,
    lease: Option<std::process::ChildStdin>,
    pub gateway: AgentGateway,
    home: std::path::PathBuf,
}

impl OwnedOpenCode {
    pub fn launch_isolated(home:&std::path::Path,catalog:&std::path::Path,config:Value)->Result<Self> {
        use std::{io::{BufRead,BufReader},process::{Command,Stdio}};
        let lab=std::path::Path::new(r"C:\spaceos");
        anyhow::ensure!(std::env::var("SPACEOS_TEST_ISOLATION_ROOT").ok().as_deref()==lab.to_str(),"isolated launch only");
        let root=std::fs::canonicalize(lab.join("test-tmp"))?;
        let home=std::fs::canonicalize(home)?;
        anyhow::ensure!(home.starts_with(&root)&&home!=root,"unsafe OpenCode home");
        // The lab configuration is explicit and no personal profiles/env are inherited.
        for folder in ["home","config","data","state","cache","tmp","appdata","localappdata","programdata","project"] {std::fs::create_dir_all(home.join(folder))?;}
        let password=format!("{}{}",uuid::Uuid::new_v4().simple(),uuid::Uuid::new_v4().simple());
        let mut command=Command::new(r"C:\Users\Gamer\AppData\Roaming\npm\node_modules\@opencode\cli\bin\opencode.exe");
        command.args(["serve","--stdio","--hostname","127.0.0.1","--port","21974"])
            .current_dir(home.join("project")).env_clear();
        for name in ["SystemRoot","WINDIR","PATH"] {if let Some(v)=std::env::var_os(name) {command.env(name,v);}}
        for (name,folder) in [("HOME","home"),("USERPROFILE","home"),("APPDATA","appdata"),("LOCALAPPDATA","localappdata"),
            ("PROGRAMDATA","programdata"),("XDG_CONFIG_HOME","config"),("XDG_DATA_HOME","data"),
            ("XDG_STATE_HOME","state"),("XDG_CACHE_HOME","cache"),("TEMP","tmp"),("TMP","tmp"),
            ("OPENCODE_CONFIG_DIR","config")] {command.env(name,home.join(folder));}
        command.env("OPENCODE_PASSWORD",&password).env("OPENCODE_CONFIG_PROJECT_DISABLE","true")
            .env("OPENCODE_DISABLE_MODELS_FETCH","true").env("OPENCODE_MODELS_PATH",catalog)
            .env("OPENCODE_FILEWATCHER_DISABLE","true").env("OPENCODE_CONFIG_CONTENT",config.to_string())
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::from(std::fs::File::create(home.join("opencode.stderr.log"))?));
        #[cfg(windows)] {use std::os::windows::process::CommandExt;command.creation_flags(0x08000000);}
        let mut child=command.spawn().context("launch owned isolated OpenCode")?;
        let lease=child.stdin.take();let stdout=child.stdout.take().context("OpenCode stdout lease missing")?;
        let (tx,rx)=std::sync::mpsc::channel();
        std::thread::spawn(move||{let mut lines=BufReader::new(stdout).lines();if let Some(line)=lines.next(){let _=tx.send(line);}for _ in lines {}});
        let line=match rx.recv_timeout(Duration::from_secs(20)) {Ok(Ok(line))=>line,_=>{drop(lease);let _=child.kill();let _=child.wait();anyhow::bail!("owned OpenCode startup failed")}};
        let url=serde_json::from_str::<Value>(&line).ok().and_then(|v|v["url"].as_str().map(str::to_owned));
        if url.as_deref()!=Some("http://127.0.0.1:21974") && url.as_deref()!=Some("http://127.0.0.1:21974/") {
            drop(lease);let _=child.kill();let _=child.wait();anyhow::bail!("unexpected isolated OpenCode endpoint")
        }
        Ok(Self {child,lease,home,gateway:AgentGateway::new("http://127.0.0.1:21974",Some("opencode".into()),Some(password))?})
    }

    pub fn pid(&self)->u32 {self.child.id()}
    pub fn has_exited(&mut self)->Result<bool> {Ok(self.child.try_wait()?.is_some())}

    fn force_owned_tree(&mut self)->Result<()> {
        if self.child.try_wait()?.is_some() {return Ok(())}
        #[cfg(windows)] {
            use std::os::windows::process::CommandExt;
            let taskkill=std::path::PathBuf::from(std::env::var_os("SystemRoot").context("SystemRoot missing")?).join("System32/taskkill.exe");
            let result=std::process::Command::new(taskkill).args(["/PID",&self.child.id().to_string(),"/T","/F"])
                .creation_flags(0x08000000).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).status()?;
            anyhow::ensure!(result.success() || self.child.try_wait()?.is_some(),"owned process tree termination failed");
        }
        #[cfg(not(windows))] { self.child.kill()?; }
        let _=self.child.wait();Ok(())
    }

    pub async fn shutdown(&mut self, session_id:&str)->Result<Value> {
        let interrupt=tokio::time::timeout(Duration::from_secs(3),self.gateway.interrupt(session_id)).await;
        let interrupted=matches!(interrupt,Ok(Ok(_)));
        let grace=std::time::Instant::now()+Duration::from_secs(3);
        let mut active=true;
        while std::time::Instant::now()<grace && self.child.try_wait()?.is_none() {
            if let Ok(Ok(snapshot))=tokio::time::timeout(Duration::from_millis(500),self.gateway.active()).await {
                active=session_is_active(&snapshot,session_id);if !active {break}
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // EOF ends the private server and its owned MCP children.
        drop(self.lease.take());
        let end=std::time::Instant::now()+Duration::from_secs(3);
        while std::time::Instant::now()<end && self.child.try_wait()?.is_none() {tokio::time::sleep(Duration::from_millis(100)).await;}
        let forced=self.child.try_wait()?.is_none();
        if forced {self.force_owned_tree()?;}
        anyhow::ensure!(self.child.try_wait()?.is_some(),"owned OpenCode is still running");
        Ok(json!({"pid":self.child.id(),"interrupt_request_succeeded":interrupted,"session_active_before_lease_close":active,"forced_owned_tree":forced,"process_stopped":true}))
    }
}

impl Drop for OwnedOpenCode {
    fn drop(&mut self) {drop(self.lease.take());let _=self.force_owned_tree();}
}

pub fn mission_permissions()->Value {
    let mut rules=vec![json!({"action":"*","resource":"*","effect":"deny"})];
    for name in ["spaceos_query_objects","spaceos_query_relations","spaceos_get_object","spaceos_patch_object","spaceos_create_relation"] {
        rules.push(json!({"action":format!("spaceos_{name}"),"resource":"*","effect":"allow"}));
    }
    json!(rules)
}

pub fn mission_opencode_config()->Value {
    json!({"update":"disable","share":"disabled","snapshots":false,"default_agent":"spaceos-mission",
        "permissions":mission_permissions(),"agents":{"spaceos-mission":{"mode":"primary","steps":20,
        "system":"Operate only through the provided SpaceOS semantic MCP tools. Do not use shell, filesystem, network or subagents. Be truthful about failures.",
        "permissions":mission_permissions()}}})
}

/// Monitor a single owned disposable execution. Terminal state comes from the
/// actual Session.outcome, never HTTP acceptance, text presence or idle alone.
pub async fn monitor_owned_attempt(state:std::sync::Arc<crate::BridgeState>,runtime:OwnedOpenCode)->Result<Value> {
    monitor_mission_attempt(state,MissionOpenCode::Owned(runtime)).await
}

async fn monitor_mission_attempt(state:std::sync::Arc<crate::BridgeState>,mut runtime:MissionOpenCode)->Result<Value> {
    let (session,deadline)={let slot=state.mission_attempt.lock().await;let a=slot.as_ref().context("no attempt")?;
        (a.binding.opencode_session_id.clone(),a.deadline)};
    let mut terminal="failed".to_owned();let mut final_session=Value::Null;
    let mut monitor_error=None;
    loop {
        if let Some(outcome)=state.mission_attempt.lock().await.as_ref().and_then(|a|a.outcome.clone()) {terminal=outcome;break}
        if std::time::Instant::now()>=deadline {terminal="timeout".into();break}
        if runtime.has_exited()? {monitor_error=Some("owned OpenCode exited unexpectedly");break}
        let remaining=deadline.saturating_duration_since(std::time::Instant::now());
        match tokio::time::timeout(remaining,runtime.gateway().session(&session)).await {
            Ok(Ok(info))=>{
                if let Some(outcome)=info["data"]["outcome"].as_str() {
                    if matches!(outcome,"succeeded"|"failed"|"interrupted") {terminal=outcome.into();final_session=info;break}
                }
            },
            Err(_)=>{terminal="timeout".into();break},
            Ok(Err(_))=>{monitor_error=Some("OpenCode session status unavailable");break},
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Close admission BEFORE interruption/lease shutdown. Once latched, no
    // later Session.outcome can overwrite timeout/cancellation/failure.
    let newly_closed={let mut slot=state.mission_attempt.lock().await;let attempt=slot.as_mut().context("attempt disappeared")?;
        if attempt.outcome.is_none() {
            if std::time::Instant::now()>=deadline {terminal="timeout".into();}
            attempt.close(&terminal);true
        } else {terminal=attempt.outcome.clone().unwrap();false}
    };
    let messages=match tokio::time::timeout(Duration::from_secs(15),runtime.gateway().assistant_messages(&session)).await {
        Ok(Ok(messages))=>json!(messages),_=>Value::Null,
    };
    if final_session.is_null() {final_session=json!({});}
    final_session["assistant_messages"]=messages;
    let mut summary=final_session.to_string();
    if summary.len()>28000 {let mut end=28000;while !summary.is_char_boundary(end) {end-=1}summary.truncate(end);}
    let state_result={let mut slot=state.mission_attempt.lock().await;let attempt=slot.as_mut().context("attempt disappeared")?;
        attempt.result=summary.clone();
        if newly_closed {attempt.report(&state,&terminal,&summary).await} else {Ok(())}
    };
    // Cleanup runs even if authoritative persistence is temporarily unavailable.
    let cleanup=runtime.shutdown(&session).await?;
    let lifecycle_result={
        let slot=state.mission_attempt.lock().await;
        let attempt=slot.as_ref().context("attempt disappeared before lifecycle reconciliation")?;
        if state_result.is_ok() {
            Some(reconcile_mission_outcome(&state,&attempt.binding.mission_id,&attempt.binding.attempt_id).await)
        } else { None }
    };
    let snapshot={let mut slot=state.mission_attempt.lock().await;
        let attempt=slot.as_mut().context("attempt disappeared during cleanup")?;
        anyhow::ensure!(attempt.binding.opencode_session_id==session,"attempt changed during cleanup");
        attempt.cleanup_complete=true;Some(attempt.snapshot())
    };
    state_result?;
    let lifecycle=match lifecycle_result {
        Some(Ok(value))=>value,
        Some(Err(error))=>{tracing::warn!(error=%error,"Mission terminal lifecycle reconciliation failed");json!({"ok":false,"error":error.to_string()})},
        None=>Value::Null,
    };
    let evidence=json!({"attempt":snapshot,"session":final_session,"cleanup":cleanup,"monitor_error":monitor_error,"lifecycle":lifecycle});
    std::fs::write(runtime.home().join("gateway-result.json"),serde_json::to_vec_pretty(&evidence)?)?;
    Ok(evidence)
}

fn terminal_lifecycle_action(status:&str,lifecycle:&str,verified:bool)->Option<&'static str> {
    match (status,lifecycle,verified) {
        ("succeeded","running",_)=>Some("begin_verification"),
        ("succeeded","verifying",true)=>Some("complete"),
        ("failed"|"timeout","running"|"awaiting_approval"|"verifying",_)=>Some("fail"),
        ("interrupted","running"|"awaiting_approval",_)=>Some("pause"),
        _=>None,
    }
}

/// Project a persisted terminal attempt through existing scoped Core commands.
/// Callers serialize against launch using mission_attempt. Never creates PASS.
pub async fn reconcile_mission_outcome(state:&crate::BridgeState,mission_id:&str,attempt_id:&str)->Result<Value> {
    let world=state.semantic_world_id()?;
    let view=crate::mission::get_view(mission_id).await?;
    let p=&view.mission["properties"];
    anyhow::ensure!(p["world_id"]==world && p["attempt_id"]==attempt_id,"stale attempt or World during reconciliation");
    let attempts=crate::ipc_client::query_objects(json!({"type":"MissionAttempt","world_id":world})).await?;
    let rows:Vec<_>=attempts.iter().filter(|a|a["properties"]["attempt_id"]==attempt_id).collect();
    anyhow::ensure!(rows.len()==1,"persisted attempt missing or ambiguous");
    let a=&rows[0]["properties"];
    anyhow::ensure!(a["world_id"]==world && a["mission_id"]==mission_id && a["opencode_session_id"]==p["opencode_session_id"]
        && a["model"]==p["execution_model"] && a["status"]==p["execution_status"],"persisted terminal binding mismatch");
    let status=a["status"].as_str().context("attempt status missing")?;
    let lifecycle=p["state"].as_str().context("Mission state missing")?;
    let Some(action)=terminal_lifecycle_action(status,lifecycle,p["verification"]["passed"]==true) else {
        return Ok(json!({"ok":true,"state":lifecycle,"execution_status":status,"transitioned":false}));
    };
    let mut operation=json!({"op":"transition","mission_id":mission_id,"expected_state":lifecycle,"action":action});
    if action=="fail" {operation["reason"]=json!(format!("Executor attempt {attempt_id}: {status}"));}
    if action=="complete" {
        let result=p["execution_result"].as_str().unwrap_or("");
        anyhow::ensure!(!result.trim().is_empty(),"verified execution result missing");
        let parsed=serde_json::from_str::<Value>(result).ok();
        let answer=parsed.as_ref().and_then(|v|v["assistant_messages"].as_array()).into_iter().flatten()
            .flat_map(|m|m["content"].as_array().into_iter().flatten())
            .filter(|part|part["type"]=="text").filter_map(|part|part["text"].as_str()).collect::<Vec<_>>().join("\n");
        let mut answer=if answer.trim().is_empty(){format!("Verified successful executor attempt {attempt_id}; full result retained in MissionAttempt.")}else{answer};
        if answer.len()>15000 {let mut end=15000;while !answer.is_char_boundary(end){end-=1}answer.truncate(end);}
        operation["result"]=json!(answer);
    }
    let response=state.semantic_command("mission_command",json!({"world_id":world,
        "command_id":format!("attempt-{attempt_id}-{action}"),"operation":operation})).await?;
    anyhow::ensure!(response["ok"]==true,"Core rejected terminal lifecycle transition: {}",response["error"]);
    Ok(json!({"ok":true,"state":response["result"]["mission"]["properties"]["state"],"execution_status":status,"transitioned":true,"event":response["result"]["event"]}))
}

pub async fn reconcile_persisted_missions(state:&crate::BridgeState)->Result<()> {
    for mission in crate::ipc_client::query_objects(json!({"type":"Mission"})).await? {
        if let (Some(id),Some(attempt))=(mission["id"].as_str(),mission["properties"]["attempt_id"].as_str()) {
            reconcile_mission_outcome(state,id,attempt).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod terminal_lifecycle_tests {
    use super::terminal_lifecycle_action as action;
    #[test] fn success_waits_for_verification(){assert_eq!(action("succeeded","running",false),Some("begin_verification"));assert_eq!(action("succeeded","verifying",false),None);assert_eq!(action("succeeded","verifying",true),Some("complete"));}
    #[test] fn failure_cannot_complete(){for s in ["failed","timeout"]{assert_eq!(action(s,"running",true),Some("fail"));assert_eq!(action(s,"verifying",true),Some("fail"));}}
    #[test] fn interruption_preserves_cancellation_and_pause(){assert_eq!(action("interrupted","running",true),Some("pause"));for s in ["cancelled","paused","completed"]{assert_eq!(action("interrupted",s,true),None);assert_eq!(action("succeeded",s,true),None);}}
    #[test] fn awaiting_approval_is_not_automatically_approved(){assert_eq!(action("succeeded","awaiting_approval",true),None);}
}

impl AgentGateway {
    fn existing_cli(location:Option<std::path::PathBuf>)->Result<Self> {
        anyhow::ensure!(existing_cli_enabled() && std::env::var("SPACEOS_TEST_ISOLATION_ROOT").ok().as_deref()==Some(r"C:\spaceos"),"existing OpenCode requires explicit lab authorization");
        let exe=std::path::PathBuf::from(r"C:\Users\Gamer\AppData\Roaming\ai.opencode.desktop\cli\2.1.3-x64\opencode-cli.exe");
        anyhow::ensure!(exe.is_file(),"normal installed OpenCode CLI unavailable");
        let mut gateway=Self::new("http://127.0.0.1/",None,None)?;
        gateway.cli=Some(exe);gateway.location=location;
        Ok(gateway)
    }

    async fn cli_json(&self,method:&str,path:&str,body:Option<Value>,scoped:bool)->Result<Value> {
        let exe=self.cli.as_ref().context("CLI transport unavailable")?;
        // This allowlist deliberately excludes credentials, global config,
        // global session lists, server lifecycle and arbitrary API access.
        anyhow::ensure!(matches!(path,"info"|"model"|"mcp"|"session"|"experimental/mcp/spaceos")
            || (path.starts_with("session/ses_")&&!path.contains("..")),"unsupported Mission CLI operation");
        let mut url=Url::parse(&format!("http://127.0.0.1/api/{path}"))?;
        if scoped {if let Some(location)=&self.location {url.query_pairs_mut().append_pair("location[directory]",location.to_str().context("non-UTF8 Mission location")?);}}
        let request_path=format!("{}{}",url.path(),url.query().map(|q|format!("?{q}")).unwrap_or_default());
        let mut command=tokio::process::Command::new(exe);
        command.args(["api",method,&request_path]).kill_on_drop(true)
            .stdin(std::process::Stdio::null()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::null());
        // No isolated profile, auth store override, credential read or client
        // impersonation. Native CLI resolves its normal existing environment.
        if let Some(location)=&self.location {command.current_dir(location);}else {command.current_dir(r"C:\spaceos");}
        if let Some(body)=body {command.args(["--data",&body.to_string()]);}
        #[cfg(windows)] {command.creation_flags(0x08000000);}
        let output=tokio::time::timeout(Duration::from_secs(15),command.output()).await
            .context("normal OpenCode CLI request timed out")??;
        anyhow::ensure!(output.status.success(),"normal OpenCode CLI API request failed (response suppressed)");
        if output.stdout.iter().all(u8::is_ascii_whitespace) {return Ok(Value::Null)}
        let value:Value=serde_json::from_slice(&output.stdout).context("invalid normal OpenCode CLI response")?;
        if path=="info" {
            anyhow::ensure!(value["version"]=="2.1.3","unexpected normal OpenCode runtime");
            let expected=std::env::var("SPACEOS_EXISTING_OPENCODE_PID").ok().and_then(|p|p.parse::<u64>().ok()).context("expected Desktop runtime PID missing")?;
            anyhow::ensure!(value["pid"].as_u64()==Some(expected),"Desktop runtime identity changed; owner revalidation required");
        }
        Ok(value)
    }

    /// Disposable sessions use an explicit model and location, never managed
    /// service discovery or a user's previous session.
    pub async fn create_disposable(&self, directory: &str, model: Value, metadata: Value) -> Result<Value> {
        if self.cli.is_some() {return self.cli_json("POST","session",Some(json!({
            "title":"SpaceOS Mission attempt","location":{"directory":directory},
            "model":model,"metadata":metadata,"agent":"spaceos-mission","permissions":mission_permissions()
        })),false).await}
        self.json_response(self.request(reqwest::Method::POST, "session")?.json(&json!({
            "title":"SpaceOS isolated Mission attempt", "location":{"directory":directory},
            "model":model, "metadata":metadata, "agent":"spaceos-mission",
            "permissions":mission_permissions()
        })), "create disposable session").await
    }

    pub async fn models(&self)->Result<Value> {
        if self.cli.is_some() {return self.cli_json("GET","model",None,true).await}
        self.json_response(self.request(reqwest::Method::GET,"model")?,"models").await
    }
    pub async fn execution_models(&self)->Result<Vec<Value>> {
        let until=std::time::Instant::now()+Duration::from_secs(15);
        let mut raw=self.models().await?;
        while raw["data"].as_array().is_some_and(Vec::is_empty) && std::time::Instant::now()<until {
            tokio::time::sleep(Duration::from_millis(250)).await;raw=self.models().await?;
        }
        let rows=raw["data"].as_array().context("invalid OpenCode model catalog")?;
        Ok(rows.iter().filter(|m|m["enabled"]==true && m["capabilities"]["tools"]==true && m["status"]!="deprecated")
            .filter(|m|m["id"].as_str().is_some()&&m["providerID"].as_str().is_some())
            .map(|m|json!({"provider_id":m["providerID"],"model_id":m["id"],"name":m["name"],"tools":true,"cost":m["cost"]})).collect())
    }
    pub async fn mcp_status(&self)->Result<Value> {
        if self.cli.is_some() {return self.cli_json("GET","mcp",None,true).await}
        self.json_response(self.request(reqwest::Method::GET,"mcp")?,"MCP status").await
    }
    pub async fn info(&self)->Result<Value> {
        if self.cli.is_some() {return self.cli_json("GET","info",None,false).await}
        self.json_response(self.request(reqwest::Method::GET,"info")?,"runtime info").await
    }


    pub async fn configure_mcp(&self, config: Value) -> Result<Value> {
        if self.cli.is_some() {return self.cli_json("PUT","experimental/mcp/spaceos",Some(json!({"config":config})),true).await}
        self.json_response(self.request(reqwest::Method::PUT, "experimental/mcp/spaceos")?
            .json(&json!({"config":config})), "configure isolated MCP").await
    }

    pub async fn session(&self, id: &str) -> Result<Value> {
        validate_session_id(id)?;
        if self.cli.is_some() {return self.cli_json("GET",&format!("session/{id}"),None,false).await}
        self.json_response(self.request(reqwest::Method::GET, &format!("session/{id}"))?, "session outcome").await
    }

    pub async fn interrupt(&self, id: &str) -> Result<Value> {
        validate_session_id(id)?;
        if self.cli.is_some() {return self.cli_json("POST",&format!("session/{id}/interrupt"),None,false).await}
        self.json_response(self.request(reqwest::Method::POST, &format!("session/{id}/interrupt"))?, "interrupt").await
    }

    pub async fn delete_disposable(&self, id: &str) -> Result<Value> {
        validate_session_id(id)?;
        self.json_response(self.request(reqwest::Method::DELETE, &format!("session/{id}"))?, "delete disposable session").await
    }

    pub fn from_env() -> Result<Self> {
        // The owner authorized NEW Mission sessions, not browsing unrelated
        // Desktop history or extracting the managed-service credential.
        anyhow::ensure!(!existing_cli_enabled(),"existing OpenCode access is Mission-only; unrelated session browsing is disabled");
        let explicit_url = std::env::var(URL_ENV).ok().filter(|v| !v.is_empty());
        let (raw, discovered_password) = match explicit_url {
            Some(url) => (url, None),
            None => discover_managed_service().map_err(|e| {
                anyhow!(
                    "OpenCode Server is not configured and managed-service discovery failed: {e}. Set {URL_ENV} to its loopback API URL"
                )
            })?,
        };
        let username = std::env::var(USER_ENV)
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| discovered_password.as_ref().map(|_| "opencode".to_string()));
        let password = std::env::var(PASSWORD_ENV)
            .ok()
            .filter(|v| !v.is_empty())
            .or(discovered_password);
        Self::new(&raw, username, password)
    }

    pub fn new(raw_url: &str, username: Option<String>, password: Option<String>) -> Result<Self> {
        let mut base_url = Url::parse(raw_url).context("invalid OPENCODE_SERVER_URL")?;
        if base_url.scheme() != "http" {
            return Err(anyhow!("OPENCODE_SERVER_URL must use http on loopback"));
        }
        if !base_url.username().is_empty() || base_url.password().is_some() {
            return Err(anyhow!(
                "credentials must not be embedded in OPENCODE_SERVER_URL"
            ));
        }
        let is_loopback = base_url
            .host_str()
            .map(|host| {
                host.eq_ignore_ascii_case("localhost")
                    || host
                        .parse::<std::net::IpAddr>()
                        .map(|ip| ip.is_loopback())
                        .unwrap_or(false)
            })
            .unwrap_or(false);
        if !is_loopback {
            return Err(anyhow!(
                "OpenCode Server must be bound to a loopback address"
            ));
        }
        if matches!(base_url.path(), "" | "/") {
            base_url.set_path("/api/");
        } else if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(15))
            .build()
            .context("build OpenCode HTTP client")?;
        Ok(Self {
            client,
            base_url,
            username,
            password,
            cli:None,
            location:None,
        })
    }

    fn request(&self, method: reqwest::Method, path: &str) -> Result<RequestBuilder> {
        let url = self.base_url.join(path).context("build OpenCode API URL")?;
        let mut request = self.client.request(method, url);
        if let Some(password) = &self.password {
            request = request.basic_auth(
                self.username.as_deref().unwrap_or("opencode"),
                Some(password),
            );
        }
        Ok(request)
    }

    async fn json_response(&self, request: RequestBuilder, operation: &str) -> Result<Value> {
        let response = request
            .send()
            .await
            .with_context(|| format!("OpenCode Server unavailable during {operation}"))?;
        let status = response.status();
        if !status.is_success() {
            // The remote service may echo credentials or prompt material in
            // an error body. Never relay that untrusted body to World/logs.
            let hint = if status == reqwest::StatusCode::UNAUTHORIZED {
                " (check OPENCODE_SERVER_USERNAME and OPENCODE_SERVER_PASSWORD)"
            } else {
                ""
            };
            return Err(anyhow!("OpenCode {operation} failed: HTTP {status}{hint}"));
        }
        let text = response.text().await.unwrap_or_default();
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text)
            .with_context(|| format!("OpenCode {operation} returned invalid JSON"))
    }

    pub async fn sessions(&self) -> Result<SessionSnapshot> {
        let sessions = self
            .json_response(
                self.request(reqwest::Method::GET, "session?limit=100&order=desc")?,
                "session list",
            )
            .await?;
        let active = self.active().await?;
        Ok(SessionSnapshot {
            sessions: unwrap_data_array(&sessions),
            active,
        })
    }

    pub async fn active(&self) -> Result<Value> {
        let value = self
            .json_response(
                self.request(reqwest::Method::GET, "session/active")?,
                "active sessions",
            )
            .await?;
        Ok(value.get("data").cloned().unwrap_or(value))
    }

    pub async fn assistant_messages(&self, session_id: &str) -> Result<Vec<Value>> {
        validate_session_id(session_id)?;
        let path = format!("session/{session_id}/message?limit=20&order=desc&type=assistant");
        if self.cli.is_some() {return Ok(unwrap_data_array(&self.cli_json("GET",&path,None,false).await?))}
        let value = self
            .json_response(self.request(reqwest::Method::GET, &path)?, "message list")
            .await?;
        Ok(unwrap_data_array(&value))
    }

    pub async fn prompt(&self, session_id: &str, text: &str) -> Result<Value> {
        validate_session_id(session_id)?;
        if text.trim().is_empty() {
            return Err(anyhow!("message is empty"));
        }
        if text.chars().count() > 32_000 {
            return Err(anyhow!("message is too long (maximum 32000 characters)"));
        }
        let path = format!("session/{session_id}/prompt");
        if self.cli.is_some() {return self.cli_json("POST",&path,Some(json!({"text":text})),false).await}
        self.json_response(
            self.request(reqwest::Method::POST, &path)?
                .json(&json!({ "text": text })),
            "prompt",
        )
        .await
    }
}

/// Discover OpenCode v2's already-running managed service. The registration
/// contains the loopback URL and its generated Basic Auth password. It is read
/// only inside Bridge and is never returned by the Agent Gateway API.
fn discover_managed_service() -> Result<(String, Option<String>)> {
    let state_root = if let Some(path) = std::env::var_os("XDG_STATE_HOME") {
        std::path::PathBuf::from(path)
    } else {
        let home = std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
            .ok_or_else(|| anyhow!("user home directory is unavailable"))?;
        std::path::PathBuf::from(home).join(".local").join("state")
    };
    let registration = state_root.join("opencode").join("service.json");
    let text = std::fs::read_to_string(&registration)
        .with_context(|| format!("cannot read {}", registration.display()))?;
    let value: Value = serde_json::from_str(&text)
        .with_context(|| format!("invalid JSON in {}", registration.display()))?;
    let url = value
        .get("url")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("managed service registration has no URL"))?
        .to_string();
    let password = value
        .get("password")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .map(str::to_owned);
    Ok((url, password))
}

pub fn message_ids(messages: &[Value]) -> HashSet<String> {
    messages
        .iter()
        .filter_map(|m| {
            m.get("id")
                .or_else(|| m.get("info").and_then(|i| i.get("id")))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect()
}

pub fn newest_response(messages: &[Value], previous: &HashSet<String>) -> Option<(String, String)> {
    messages.iter().find_map(|message| {
        let id = message
            .get("id")
            .or_else(|| message.get("info").and_then(|i| i.get("id")))
            .and_then(Value::as_str)?;
        if previous.contains(id) {
            return None;
        }
        let mut texts = Vec::new();
        collect_text_parts(message, &mut texts);
        let text = texts.join("\n").trim().to_string();
        (!text.is_empty()).then(|| (id.to_string(), text))
    })
}

pub fn session_is_active(active: &Value, session_id: &str) -> bool {
    active.get(session_id).is_some()
        || active
            .as_array()
            .map(|items| {
                items.iter().any(|v| {
                    v.get("id")
                        .or_else(|| v.get("sessionID"))
                        .and_then(Value::as_str)
                        == Some(session_id)
                })
            })
            .unwrap_or(false)
}

fn unwrap_data_array(value: &Value) -> Vec<Value> {
    value
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
        .cloned()
        .unwrap_or_default()
}

fn collect_text_parts(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if map.get("type").and_then(Value::as_str) == Some("text") {
                if let Some(text) = map.get("text").and_then(Value::as_str) {
                    out.push(text.to_string());
                    return;
                }
            }
            for child in map.values() {
                collect_text_parts(child, out);
            }
        }
        Value::Array(items) => {
            for child in items {
                collect_text_parts(child, out);
            }
        }
        _ => {}
    }
}

fn validate_session_id(id: &str) -> Result<()> {
    if id.is_empty()
        || !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
    {
        return Err(anyhow!("invalid OpenCode session id"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_loopback_server() {
        assert!(AgentGateway::new("http://example.com/api", None, None).is_err());
        assert!(AgentGateway::new("https://127.0.0.1/api", None, None).is_err());
        assert!(AgentGateway::new("http://user:secret@127.0.0.1/api", None, None).is_err());
    }

    #[test]
    fn extracts_only_new_text_response() {
        let messages = vec![
            json!({"id":"new", "parts":[{"type":"text","text":"SpaceOS is healthy"}]}),
            json!({"id":"old", "parts":[{"type":"text","text":"old"}]}),
        ];
        let previous = HashSet::from(["old".to_string()]);
        assert_eq!(
            newest_response(&messages, &previous),
            Some(("new".into(), "SpaceOS is healthy".into()))
        );
    }

    #[tokio::test]
    async fn talks_to_v2_http_contract() {
        use axum::{
            routing::{get, post},
            Json, Router,
        };

        async fn sessions() -> Json<Value> {
            Json(json!({"data":[{"id":"ses_test","title":"SpaceOS"}],"cursor":{}}))
        }
        async fn active() -> Json<Value> {
            Json(json!({"data":{}}))
        }
        async fn messages() -> Json<Value> {
            Json(
                json!({"data":[{"id":"msg_2","type":"assistant","parts":[{"type":"text","text":"SpaceOS is healthy"}]}],"cursor":{}}),
            )
        }
        async fn prompt(Json(body): Json<Value>) -> Json<Value> {
            Json(json!({"data":{"id":"inbox_1","text":body["text"]}}))
        }

        let app = Router::new()
            .route("/api/session", get(sessions))
            .route("/api/session/active", get(active))
            .route("/api/session/:id/message", get(messages))
            .route("/api/session/:id/prompt", post(prompt));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let gateway = AgentGateway::new(&format!("http://{addr}/api"), None, None).unwrap();

        let snapshot = gateway.sessions().await.unwrap();
        assert_eq!(snapshot.sessions[0]["id"], "ses_test");
        assert_eq!(
            gateway
                .prompt("ses_test", "Покажи состояние SpaceOS")
                .await
                .unwrap()["data"]["text"],
            "Покажи состояние SpaceOS"
        );
        let response = gateway.assistant_messages("ses_test").await.unwrap();
        assert_eq!(
            newest_response(&response, &HashSet::new()).unwrap().1,
            "SpaceOS is healthy"
        );
        server.abort();
    }

    #[tokio::test]
    async fn shared_runtime_refuses_unowned_session_cleanup() {
        let gateway=AgentGateway::new("http://127.0.0.1:1/",None,None).unwrap();
        let mut runtime=MissionOpenCode::Existing{gateway,home:std::path::PathBuf::new(),session:Some("ses_owned".into())};
        let error=runtime.shutdown("ses_somebody_else").await.unwrap_err().to_string();
        assert!(error.contains("unowned"));
    }

    #[tokio::test]
    async fn native_cli_refuses_credential_and_global_configuration_routes() {
        let mut gateway=AgentGateway::new("http://127.0.0.1:1/",None,None).unwrap();
        gateway.cli=Some(std::path::PathBuf::from("must-not-be-executed"));
        for route in ["credential","experimental/config","location/reload","session/active","session/../credential"] {
            let error=gateway.cli_json("GET",route,None,false).await.unwrap_err().to_string();
            assert!(error.contains("unsupported"),"{route}: {error}");
        }
    }

    #[tokio::test]
    async fn remote_error_body_is_not_exposed() {
        use axum::{http::StatusCode, routing::get, Router};
        let app = Router::new().route(
            "/api/session",
            get(|| async { (StatusCode::BAD_GATEWAY, "synthetic-secret-must-not-leak") }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let gateway = AgentGateway::new(&format!("http://{addr}/api"), None, None).unwrap();
        let error = gateway.sessions().await.unwrap_err().to_string();
        assert!(error.contains("502"));
        assert!(!error.contains("synthetic-secret"));
        server.abort();
    }
}
