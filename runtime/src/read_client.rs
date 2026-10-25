//! Runtime uses an explicitly provisioned Model A identity, never cli/system.
use anyhow::{Context, Result};
use serde_json::{json, Value};
use space_core::ipc;

pub fn identity() -> Result<(String, String, String)> {
    let actor = std::env::var("SPACEOS_RUNTIME_ACTOR").context("runtime actor missing")?;
    let token = std::env::var("SPACEOS_RUNTIME_ACTOR_TOKEN").context("runtime credential missing")?;
    let world = std::env::var("SPACEOS_RUNTIME_WORLD_ID").context("runtime World missing")?;
    anyhow::ensure!(!actor.trim().is_empty() && !space_core::policy::Policy::is_core_principal(&actor)
        && !token.trim().is_empty() && !world.trim().is_empty(), "invalid runtime scoped identity");
    Ok((actor, token, world))
}
pub fn envelope(mut params: Value, subscriber: bool) -> Result<Value> {
    let (actor, token, world) = identity()?;
    if let Some(requested) = params.get("world_id") { anyhow::ensure!(requested.as_str() == Some(&world), "runtime World mismatch"); }
    params[if subscriber {"subscriber"} else {"actor"}] = json!(actor);
    params["actor_token"] = json!(token);
    params["world_id"] = json!(world);
    Ok(params)
}
pub fn pipe() -> Result<String> { ipc::configured_provider_pipe("SPACEOS_CORE_PIPE", ipc::CORE_PIPE) }
pub async fn read(method: &str, params: Value) -> Result<Value> {
    let mut params = envelope(params, false)?;
    if method == "get_world" { params["id"] = params["world_id"].clone(); }
    let response = ipc::send_request(&pipe()?, method, params).await.context("runtime Core transport failure")?;
    anyhow::ensure!(response["ok"] == true, "runtime Core read denied or failed");
    response.get("result").cloned().context("runtime Core response invalid")
}
