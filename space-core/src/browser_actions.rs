//! Scoped Browser actions, inside the existing IPC/dispatch trust boundary.
use super::*;
use crate::action::{BrowserWorldRequest, BrowserWorldTarget};

fn canonical(state: &CoreState, target: &BrowserWorldTarget, object: &Object) -> Result<()> {
    let p = &object.properties;
    let a = state.observation_bindings.browser_action_authority(&target.runtime.world_id)?;
    anyhow::ensure!(a.instance_id == target.instance_id && a.runtime == target.runtime, "Browser binding changed");
    let id = crate::provider_observation::browser_tab_identity(&a.instance_id, &a.runtime,
        p["target_id"].as_str().ok_or_else(||anyhow::anyhow!("missing Browser target"))?)?;
    anyhow::ensure!(object.r#type == "BrowserTab" && object.provider_id == "browser"
        && object.id == id.object_id && object.external_id.as_deref() == Some(&id.external_id)
        && p["world_id"] == a.runtime.world_id && p["source_id"] == a.runtime.source_id
        && p["instance_id"] == a.instance_id && p["runtime_epoch"] == a.runtime.runtime_epoch
        && p["target_type"] == "page", "Browser canonical authority mismatch");
    let storage = state.storage.lock().unwrap();
    storage.validate_browser_authority(&a)?;
    let stored = storage.get_object(&object.id)?.ok_or_else(||anyhow::anyhow!("Browser snapshot missing"))?;
    anyhow::ensure!(serde_json::to_value(stored)? == serde_json::to_value(object)?, "Browser snapshot changed or forged");
    Ok(())
}

fn authorized(state: &CoreState, params: &Value, world: &str, action: &str) -> Result<()> {
    let actor = service_registration::BROWSER_WORLD_ACTOR;
    anyhow::ensure!(state.semantic_world_id.as_ref().ok().map(String::as_str) == Some(world)
        && state.policy.may_access_world(actor, world)
        && principal_authorized(state, params, actor, "semantic.read", Some(world))
        && principal_authorized(state, params, actor, "action.get_tabs", Some(world))
        && principal_authorized(state, params, actor, &format!("action.{action}"), Some(world)), "denied: Browser World authentication or capability");
    Ok(())
}

fn event(state: &CoreState, req: &ActionRequest, world: &str, ids: Vec<String>, kind: &str, payload: Value) -> Result<()> {
    let mut e = Event::new(kind, req.actor.clone(), payload);
    e.world_id = world.into(); e.object_ids = ids; e.provider_id = Some("browser".into());
    e.correlation_id = Some(req.id.clone());
    let (inserted, seq) = state.storage.lock().unwrap().record_event_outcome(&e)?;
    anyhow::ensure!(inserted, "Browser action event collision"); e.seq = seq;
    publish_committed_event(state, &e); Ok(())
}

async fn execute(state: &Arc<CoreState>, params: &Value, req: &ActionRequest) -> Result<ActionResult> {
    let input: BrowserWorldRequest = serde_json::from_value(req.target.clone())?;
    anyhow::ensure!(input.scope_version == 1 && req.provider_id == "browser"
        && matches!(req.action.as_str(), "get_tabs" | "screenshot"), "denied: unsupported scoped Browser operation");
    input.options.validate()?;
    authorized(state, params, &input.world_id, &req.action)?;
    let a = state.observation_bindings.browser_action_authority(&input.world_id)?;
    state.storage.lock().unwrap().validate_browser_authority(&a)?;
    let mut target = BrowserWorldTarget { scope_version: 1, instance_id: a.instance_id,
        runtime: a.runtime, object: None, options: input.options };
    match req.action.as_str() {
        "get_tabs" => anyhow::ensure!(input.object_id.is_none(), "get_tabs does not take an object"),
        _ => {
            let id = input.object_id.as_deref().ok_or_else(||anyhow::anyhow!("canonical object_id required"))?;
            let object = state.storage.lock().unwrap().get_object(id)?.ok_or_else(||anyhow::anyhow!("Browser object unavailable"))?;
            canonical(state, &target, &object)?; target.object = Some(object);
        }
    }
    let ids = target.object.iter().map(|o|o.id.clone()).collect::<Vec<_>>();
    event(state, req, &input.world_id, ids.clone(), "ActionRequested", json!({"action":req.action}))?;
    let outcome: Result<Value> = async {
        // Private, signed source-bound live enumeration; never forwarded unfiltered.
        let mut live_req = req.clone(); live_req.id = uuid::Uuid::new_v4().to_string();
        live_req.action = "get_tabs".into();
        let mut live_target = target.clone(); live_target.object = None;
        live_req.target = serde_json::to_value(&live_target)?;
        let live = forward_action_to_provider(state, &live_req, &req.id, &state.filesystem_provider_pipe, &state.browser_provider_pipe).await?;
        anyhow::ensure!(live.status == ActionStatus::Completed, "Browser live resolution failed");
        anyhow::ensure!(live.detail["authority"] == serde_json::to_value(&live_target)?, "Browser response authority mismatch");
        let rows = live.detail["tabs"].as_array().ok_or_else(||anyhow::anyhow!("invalid Browser live response"))?;
        let mut accepted = Vec::new();
        let mut seen = HashSet::new();
        for row in rows {
            let object: Object = serde_json::from_value(row["canonical"].clone())?;
            anyhow::ensure!(seen.insert(object.id.clone()), "ambiguous Browser live handle");
            if canonical(state, &target, &object).is_ok() { accepted.push(object); }
        }
        authorized(state, params, &input.world_id, &req.action)?;
        if req.action == "get_tabs" {
            let tabs = accepted.iter().map(|o|json!({"tab_id":o.id,"target_id":o.properties["target_id"],
                "world_id":input.world_id,"instance_id":target.instance_id,"source_id":target.runtime.source_id,
                "runtime_epoch":target.runtime.runtime_epoch,"title":o.properties["title"],"url":o.properties["url"]})).collect::<Vec<_>>();
            return Ok(json!({"tabs":tabs,"scoped":true,"performer_instance":live.detail["performer_instance"]}));
        }
        let object = target.object.as_ref().unwrap();
        anyhow::ensure!(accepted.iter().filter(|o|serde_json::to_value(o).ok()==serde_json::to_value(object).ok()).count() == 1,
            "Browser canonical live handle unavailable");
        canonical(state, &target, object)?;
        let mut signed = req.clone(); signed.target = serde_json::to_value(&target)?;
        let result = forward_action_to_provider(state, &signed, &req.id, &state.filesystem_provider_pipe, &state.browser_provider_pipe).await?;
        anyhow::ensure!(result.status == ActionStatus::Completed, "Browser screenshot failed");
        anyhow::ensure!(result.detail["authority"] == serde_json::to_value(&target)?, "Browser screenshot response mismatch");
        anyhow::ensure!(result.detail["data"].as_str().is_some_and(|d|!d.is_empty()), "empty Browser screenshot");
        Ok(json!({"data":result.detail["data"],"format":"jpeg","object_id":object.id,"scoped":true,"performer_instance":result.detail["performer_instance"]}))
    }.await;
    let (status, detail, kind) = match outcome {
        Ok(detail) => (ActionStatus::Completed, detail, "ActionCompleted"),
        Err(error) => (ActionStatus::Failed, json!({"error":error.to_string()}), "ActionFailed"),
    };
    // Never persist screenshots/base64 or credentials in action history.
    event(state, req, &input.world_id, ids, kind, json!({"action":req.action,"status":status,"error":detail.get("error"),"performer_instance":detail.get("performer_instance")}))?;
    Ok(ActionResult { action_id:req.id.clone(), status, detail })
}

pub(super) async fn handle(state: &Arc<CoreState>, params: Value) -> Value {
    let req: ActionRequest = match serde_json::from_value(params.clone()) {
        Ok(req)=>req, Err(_)=>return json!({"ok":false,"error":"invalid Browser action request"}),
    };
    match execute(state, &params, &req).await {
        Ok(result)=>json!({"ok":true,"result":result}),
        Err(error)=>json!({"ok":false,"error":error.to_string()}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scoped_actor_requires_each_capability_and_exact_world() {
        let world = "550e8400-e29b-41d4-a716-446655440000";
        let actor = service_registration::BROWSER_WORLD_ACTOR;
        for missing in ["semantic.read", "action.get_tabs", "action.screenshot"] {
            let state = CoreState::new_with_semantic_world(Storage::open(":memory:").unwrap(), Some(world));
            let caps = service_registration::BROWSER_WORLD_CAPS.iter().filter(|c|**c != missing).map(|c|c.to_string()).collect();
            let token = state.policy.register_scoped_service(actor, caps, vec![world.into()]).unwrap();
            let params = json!({"actor":actor,"actor_token":token});
            assert!(authorized(&state, &params, world, "screenshot").is_err());
        }
        let state = CoreState::new_with_semantic_world(Storage::open(":memory:").unwrap(), Some(world));
        assert!(state.policy.register(actor,"system",vec![],vec![world.into()]).is_none());
        let token = state.policy.register_scoped_service(actor, service_registration::BROWSER_WORLD_CAPS.iter().map(|c|c.to_string()).collect(),vec![world.into()]).unwrap();
        assert!(authorized(&state,&json!({"actor":actor,"actor_token":token}),world,"screenshot").is_ok());
        assert!(authorized(&state,&json!({"actor":actor,"actor_token":token}),"other-world","screenshot").is_err());
    }
    #[test]
    fn browser_public_target_rejects_raw_identity_and_authority() {
        for value in [json!({"scope_version":1,"world_id":"w","target_id":"raw"}),
            json!({"scope_version":1,"world_id":"w","runtime":{}})] {
            assert!(serde_json::from_value::<BrowserWorldRequest>(value).is_err());
        }
        assert!(crate::action::BrowserShotOptions { format:"png".into(),quality:62 }.validate().is_err());
        assert!(crate::action::BrowserShotOptions { format:"jpeg".into(),quality:0 }.validate().is_err());
    }
}
