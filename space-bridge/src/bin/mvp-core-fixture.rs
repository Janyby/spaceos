//! Isolated diagnostic launcher linked to the unchanged MVP-2 Core library.
//! Fixture-only bootstrap is not an agent authorization bypass.
use std::path::PathBuf;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};
use space_core::{ipc, principal::PrincipalRegistry, service_registration::{ServiceRegistrarRegistry,SEMANTIC_PROFILE,WORLD_READ_PROFILE}, Storage, CreateSemanticObject};
const LAB:&str=r"C:\spaceos";
#[tokio::main]
async fn main()->anyhow::Result<()> {
    anyhow::ensure!(std::env::var("SPACEOS_TEST_ISOLATION_ROOT")?==LAB,"wrong candidate");
    let path=PathBuf::from(std::env::var("MVP2B1_CONFIG")?);
    let allowed=std::fs::canonicalize(PathBuf::from(LAB).join("test-tmp"))?;
    anyhow::ensure!(std::fs::canonicalize(&path)?.starts_with(&allowed),"unsafe fixture config");
    let c:Value=serde_json::from_slice(&std::fs::read(&path)?)?;
    let pipe=c["pipe"].as_str().unwrap();
    anyhow::ensure!(pipe.starts_with(r"\\.\pipe\spaceos-4-3a4-process-mvp2b1-"),"unsafe Core pipe");
    let db=PathBuf::from(c["db"].as_str().unwrap());
    anyhow::ensure!(std::fs::canonicalize(db.parent().unwrap())?.starts_with(&allowed),"unsafe SQLite path");
    let world=c["world"].as_str().unwrap();
    let bytes=|name:&str|->anyhow::Result<[u8;32]> {Ok(URL_SAFE_NO_PAD.decode(c["keys"][name].as_str().unwrap())?.try_into().map_err(|_|anyhow::anyhow!("invalid fixture key"))?)};
    let mut principals=PrincipalRegistry::default();
    let mut fixture_grants=vec!["agent.register","agent.grant.semantic.read","agent.grant.semantic.object.patch","agent.grant.semantic.relation.create"];
    if c.get("providers").is_some() { fixture_grants.extend(["agent.grant.action.stat","agent.grant.action.get_tabs","agent.grant.action.scan"]); }
    principals.insert("system",bytes("system")?,fixture_grants,[world]).map_err(anyhow::Error::msg)?;
    // Optional full-product verification fixture. Authority remains synthetic,
    // explicit and confined to this fixture's already-validated test root/World.
    if let Some(provider) = c.get("providers") {
        let source = std::fs::canonicalize(provider["root"].as_str().ok_or_else(||anyhow::anyhow!("missing fixture source"))?)?;
        anyhow::ensure!(source.starts_with(&allowed) && source != allowed,"unsafe provider source");
        principals.insert("filesystem",bytes("filesystem")?,["provider.register","filesystem.reconcile.read","observation.report","object.update","event.record"],[world]).map_err(anyhow::Error::msg)?;
        let browser_worlds=if provider.get("foreign_browser_source").is_some(){vec![world,c["other_world"].as_str().unwrap()]}else{vec![world]};
        principals.insert("browser",bytes("browser")?,["provider.register","observation.report","event.record"],browser_worlds).map_err(anyhow::Error::msg)?;
    }
    let transport=c["keys"]["transport"].as_str().unwrap().to_owned();
    let mut registrars=ServiceRegistrarRegistry::default();
    registrars.insert(SEMANTIC_PROFILE,bytes("semantic")?,&principals,&transport).map_err(anyhow::Error::msg)?;
    registrars.insert(WORLD_READ_PROFILE,bytes("reader")?,&principals,&transport).map_err(anyhow::Error::msg)?;
    if c["scoped_browser"] == true {
        registrars.insert(space_core::service_registration::BROWSER_WORLD_PROFILE,bytes("browser_world")?,&principals,&transport).map_err(anyhow::Error::msg)?;
    }
    let mut storage=Storage::open(db)?;
    if c["scoped_browser"] == true && !path.parent().unwrap().join("seeds.json").exists() {
        let runtime: space_core::provider_observation::BrowserRuntimeConfig = serde_json::from_value(c["providers"]["browser_source"].clone())?;
        let forged = space_core::provider_observation::browser_tab_observation("vertical-browser", &runtime, "forged-history-target", "about:blank", Some("Unadmitted historical fixture"))?;
        storage.create_object(&forged.objects[0])?; // Deliberately no verified admission/log/cache.
    }
    let mut seeds=serde_json::Map::new();
    let seed_file=path.parent().unwrap().join("seeds.json");
    if seed_file.is_file() {
        let prior:Value=serde_json::from_slice(&std::fs::read(&seed_file)?)?;
        for (label,scope) in [("task_a",world),("summary_a",world),("foreign_task",c["other_world"].as_str().unwrap())] {
            let id=prior[label]["id"].as_str().ok_or_else(||anyhow::anyhow!("restart seed missing"))?;
            let object=storage.get_object(id)?.ok_or_else(||anyhow::anyhow!("restart object missing"))?;
            anyhow::ensure!(object.properties["world_id"]==scope,"restart seed World mismatch");
        }
    } else {
    for (label,scope) in [("task_a",world),("summary_a",world),("foreign_task",c["other_world"].as_str().unwrap())] {
        let result=storage.create_semantic_object(&CreateSemanticObject {world_id:scope.into(),object_type:"Note".into(),properties:json!({"title":label,"status":"unfinished"}),correlation_id:format!("fixture-seed-{label}")})?;
        seeds.insert(label.into(),serde_json::to_value(result.object)?);
    }
    std::fs::write(path.parent().unwrap().join("seeds.json"),serde_json::to_vec_pretty(&seeds)?)?;
    }
    let mut state=ipc::CoreState::new_with_semantic_world_and_identities(storage,Some(world),c["fs_pipe"].as_str().unwrap(),c["browser_pipe"].as_str().unwrap(),principals,registrars,transport);
    if let Some(provider) = c.get("providers") {
        let mut bindings=space_core::provider_observation::ObservationBindings::from_json(&json!([{
            "provider":"filesystem","instance_id":"vertical-filesystem",
            "instance_token":c["keys"]["instance"],"root":provider["root"],"world_id":world
        }]).to_string())?;
        // Optional synthetic BrowserTabCreated fixture; no default source,
        // inherited production binding, or automatic Chrome epoch approval.
        if let Some(browser) = provider.get("browser_source") {
            let mut binding=browser.clone();
            binding["provider"]=json!("browser");
            binding["instance_id"]=json!("vertical-browser");
            binding["instance_token"]=c["keys"]["browser_instance"].clone();
            let mut entries=vec![binding];
            if let Some(foreign)=provider.get("foreign_browser_source") {
                let mut b=foreign.clone();b["provider"]=json!("browser");b["instance_id"]=json!("corrections-browser-B");b["instance_token"]=c["keys"]["browser_instance_b"].clone();entries.push(b);
            }
            bindings.configure_browser(&serde_json::to_string(&entries)?)?;
        }
        bindings.validate_distinct_principals(&state.privileged,&c["keys"]["transport"].as_str().unwrap()).map_err(anyhow::Error::msg)?;
        std::sync::Arc::get_mut(&mut state).unwrap().observation_bindings=bindings;
    }
    ipc::run_server(state,pipe).await
}
