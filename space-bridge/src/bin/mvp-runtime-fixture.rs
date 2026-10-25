//! H1-only launch adapter. All handlers and authorization are unchanged I.5 libraries.
use std::{path::PathBuf, sync::Arc, time::Duration};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json,Value};
use space_core::{ipc, principal::PrincipalRegistry, provider_observation::ObservationBindings,
    service_registration::{ServiceRegistrarRegistry,SEMANTIC_PROFILE,WORLD_READ_PROFILE,MCP_READ_PROFILE}, Storage};

const LAB:&str=r"C:\spaceos";
fn key(config:&Value,name:&str)->String{config["keys"][name].as_str().expect("missing synthetic key").into()}
fn bytes(config:&Value,name:&str)->[u8;32]{URL_SAFE_NO_PAD.decode(key(config,name)).unwrap().try_into().unwrap()}

#[tokio::main]
async fn main()->anyhow::Result<()> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::WARN).with_writer(std::io::stderr).init();
    anyhow::ensure!(std::env::var("SPACEOS_TEST_ISOLATION_ROOT")?==LAB,"wrong laboratory");
    let config_path=PathBuf::from(std::env::var("MVP2_CONFIG")?);
    let allowed=std::fs::canonicalize(format!(r"{LAB}\test-tmp"))?;
    anyhow::ensure!(std::fs::canonicalize(config_path.parent().unwrap())?.starts_with(&allowed),"unsafe config");
    let c:Value=serde_json::from_slice(&std::fs::read(config_path)?)?;
    let world=c["world"].as_str().unwrap();
    let pipe=c["pipe"].as_str().unwrap();
    anyhow::ensure!(pipe.starts_with(r"\\.\pipe\spaceos-4-3a4-process-mvp2b1-"),"unsafe pipe");
    match std::env::args().nth(1).as_deref(){
        Some("gateway-check")=>{
            space_bridge::ipc_client::configure_isolated_read_service(key(&c,"reader"),world.into())?;
            let state=Arc::new(space_bridge::BridgeState::new_with_semantic_world_and_registrars(Some(world),Some(key(&c,"semantic")),Some(key(&c,"dashboard"))));
            let home=PathBuf::from(c["run"].as_str().unwrap()).join(format!("owned-opencode-{}",uuid::Uuid::new_v4()));std::fs::create_dir(&home)?;
            let mut runtime=space_bridge::agent_gateway::OwnedOpenCode::launch_isolated(&home,std::path::Path::new(c["catalog"].as_str().unwrap()),space_bridge::agent_gateway::mission_opencode_config())?;
            let info=runtime.gateway.info().await?;
            let session=runtime.gateway.create_disposable(home.join("project").to_str().unwrap(),json!({"id":"fixture-idle-no-inference","providerID":"synthetic"}),json!({"purpose":"idle-deadline-no-model-call"})).await?;
            let session_id=session["data"]["id"].as_str().ok_or_else(||anyhow::anyhow!("missing disposable session"))?.to_owned();
            let a=&c["attempt"];
            let mut attempt=space_bridge::agent_gateway::BoundAttempt::new(space_bridge::agent_gateway::AttemptBinding {
                mission_id:a["mission_id"].as_str().unwrap().into(),attempt_id:a["attempt_id"].as_str().unwrap().into(),
                world_id:world.into(),agent_id:a["agent_id"].as_str().unwrap().into(),opencode_session_id:session_id.clone()
            },a["credential"].as_str().unwrap().into(),a["actor_token"].as_str().unwrap().into(),Duration::from_millis(1200))?;
            attempt.begin_persistence(&state).await?;*state.mission_attempt.lock().await=Some(attempt);
            let started=std::time::Instant::now();
            if c["manual_interrupt"]==true {let clone=state.clone();tokio::spawn(async move {tokio::time::sleep(Duration::from_millis(200)).await;let mut slot=clone.mission_attempt.lock().await;slot.as_mut().unwrap().stop(&clone,"interrupted").await.unwrap();});}
            let result=space_bridge::agent_gateway::monitor_owned_attempt(state,runtime).await?;
            println!("{}",json!({"info":info,"session":session,"result":result,"elapsed_ms":started.elapsed().as_millis(),"model_prompted":false}));Ok(())
        },
        Some("core")=>{
            let db=PathBuf::from(c["db"].as_str().unwrap());
            anyhow::ensure!(std::fs::canonicalize(db.parent().unwrap())?.starts_with(&allowed),"unsafe database");
            let source=std::fs::canonicalize(c["fsRoot"].as_str().unwrap())?;
            anyhow::ensure!(source.starts_with(&allowed)&&source!=allowed,"unsafe source");
            let mut p=PrincipalRegistry::default();
            p.insert("filesystem",bytes(&c,"filesystem"),["provider.register","observation.report","event.record","object.update","filesystem.reconcile.read"],[world]).map_err(anyhow::Error::msg)?;
            p.insert("browser",bytes(&c,"browser"),["provider.register","observation.report","event.record"],[world]).map_err(anyhow::Error::msg)?;
            let mut r=ServiceRegistrarRegistry::default();
            for (profile,name) in [(SEMANTIC_PROFILE,"semantic"),(WORLD_READ_PROFILE,"reader"),(MCP_READ_PROFILE,"mcp"),(space_core::service_registration::DASHBOARD_PROFILE,"dashboard")]{r.insert(profile,bytes(&c,name),&p,&ipc::core_token()).map_err(anyhow::Error::msg)?;}
            let mut state=ipc::CoreState::new_with_semantic_world_and_identities(Storage::open(db)?,Some(world),c["fsPipe"].as_str().unwrap(),c["browserPipe"].as_str().unwrap(),p,r,ipc::core_token());
            Arc::get_mut(&mut state).unwrap().observation_bindings=ObservationBindings::from_json(&json!([{
                "provider":"filesystem","instance_id":"h1-filesystem","instance_token":key(&c,"instance"),"root":source,"world_id":world
            }]).to_string())?;
            let mut readers=serde_json::Map::new();
            for who in ["A","B","coordinator"] {
                let name=format!("mvp2-reader-{who}");
                let token=state.policy.register(&name,"system",vec!["semantic.read".into(),"action.stat".into(),"action.read".into(),"action.get_tabs".into()],vec![world.into()]).unwrap();
                readers.insert(who.into(),json!({"actor":name,"token":token}));
            }
            std::fs::write(PathBuf::from(c["run"].as_str().unwrap()).join("readers.secret.json"),serde_json::to_vec(&readers)?)?;
            ipc::run_server(state,pipe).await
        },
        Some("bridge")=>{
            space_bridge::ipc_client::configure_isolated_read_service(key(&c,"reader"),world.into())?;
            space_bridge::ipc_client::configure_isolated_mcp_read_service(key(&c,"mcp"),world.into())?;
            let mut state=space_bridge::BridgeState::new_with_semantic_world_and_registrars(Some(world),Some(key(&c,"semantic")),Some(key(&c,"dashboard")));
            if c["scoped_browser"] == true { state.configure_scoped_browser(key(&c,"browser_world")); }
            let port=c["port"].as_u64().unwrap() as u16;
            anyhow::ensure!(port==21973,"unexpected MVP2B2 HTTP port");
            state.set_isolated_agent_origin(port)?;
            state.set_isolated_lab_login(key(&c,"login"),Duration::from_secs(7200))?;
            if let Some(execution)=c.get("execution") {
                state.mission_runtime=Some(space_bridge::agent_gateway::MissionRuntimeConfig {
                    home_root:PathBuf::from(execution["home_root"].as_str().unwrap()),
                    catalog:PathBuf::from(execution["catalog"].as_str().unwrap()),
                    agent_id:execution["agent_id"].as_str().unwrap().into(),
                    actor_token:execution["actor_token"].as_str().unwrap().into(),
                    max_duration:Duration::from_millis(execution["duration_ms"].as_u64().unwrap_or(600000)),
                });
            }
            state.interrupt_orphan_attempts().await?;
            space_bridge::agent_gateway::reconcile_persisted_missions(&state).await?;
            if let Some(a)=c.get("attempt") {
                let binding=space_bridge::agent_gateway::AttemptBinding {
                    mission_id:a["mission_id"].as_str().unwrap().into(),
                    attempt_id:a["attempt_id"].as_str().unwrap().into(),
                    world_id:world.into(), agent_id:a["agent_id"].as_str().unwrap().into(),
                    opencode_session_id:a["opencode_session_id"].as_str().unwrap().into(),
                };
                let mut attempt=space_bridge::agent_gateway::BoundAttempt::new(
                    binding,a["credential"].as_str().unwrap().into(),a["actor_token"].as_str().unwrap().into(),
                    Duration::from_millis(a["duration_ms"].as_u64().unwrap_or(600_000)))?;
                match attempt.begin_persistence(&state).await {
                    Ok(())=>*state.mission_attempt.lock().await=Some(attempt),
                    Err(_)=>eprintln!("isolated attempt initialization denied; no automatic retry or budget reset"),
                }
            }
            axum::serve(tokio::net::TcpListener::bind(("127.0.0.1",port)).await?,space_bridge::handlers::create_router(Arc::new(state))).await?;
            Ok(())
        },
        Some("mission-diagnostic")=>{
            let r=ipc::send_request(pipe,"register_service_actor",json!({"profile":"dashboard","name":"space-bridge","capabilities":space_core::service_registration::DASHBOARD_CAPS,"worlds":[],"registrar_token":key(&c,"dashboard")})).await?;
            let token=r["result"]["token"].as_str().ok_or_else(||anyhow::anyhow!("dashboard fixture registration denied"))?;
            let result=space_bridge::mission::create_mission("space-bridge",token,"Inspect synthetic project documents and relationships, produce an evidence-linked task plan and report completion or failure",c["fsRoot"].as_str().unwrap(),"isolated-agent").await;
            println!("{}",match result{Ok((v,a))=>json!({"ok":true,"applied":a,"view":v.to_json()}),Err(e)=>json!({"ok":false,"error":e.to_string()})});Ok(())
        },_=>anyhow::bail!("expected core, bridge or mission-diagnostic")
    }
}
