use anyhow::Result;
use serde_json::json;
use space_core::{ipc, ActionRequest, Event, Object, Relation, Space};

/// Demo agent: uses only the normal Core IPC API. No privileged path.
///
/// SpaceTrace instrumentation (Slice 5): the agent emits runtime/decision/
/// action/responsibility spans into its process-local trace buffer. Cross-
/// process linkage happens via `correlation_id` threaded through non-event
/// IPC calls — Core's own SpaceTrace records the matching half. No trace data
/// is shipped between processes (no broker, spec §34); each side keeps
/// its own buffer. Agent token is never placed in trace payloads (§24):
/// `agent_instance_id` is a fresh per-run UUID, not the token.
#[tokio::main]
async fn main() -> Result<()> {
    // Process-local SpaceTrace; hooks are no-ops if init fails (I1/I17).
    let _st = spacetrace::SpaceTrace::new().ok();
    if let Some(ref st) = _st {
        spacetrace::init_global_hooks(st.buffer(), st.correlation());
    }
    // Runtime identity: local deterministic script => type "local", Task format.
    // Format is a Runtime characteristic, NOT an Agent type (gate §3).
    let runtime_instance_id = uuid::Uuid::new_v4().to_string();
    let agent_instance_id = uuid::Uuid::new_v4().to_string();
    let agent_id = "agent:SpaceOS Demo Agent";
    let responsibility_id = "R-mission-v1";
    let mission_corr = format!("mission-{}", uuid::Uuid::new_v4());
    std::env::set_var("SPACEOS_MISSION_CORR", &mission_corr);
    let hooks = spacetrace::hooks::global_hooks();
    hooks.runtime_start(
        &mission_corr,
        "local",
        &runtime_instance_id,
        agent_id,
        Some("Task"),
    );
    // agent-demo registers its capabilities as a trusted bootstrap step.
    // The returned agent token binds all subsequent calls to this identity.
    let reg = ipc::send_request(ipc::CORE_PIPE, "register_agent", json!({
        "principal": "system", "name": "agent-demo",
        "capabilities": ["object.create","object.update","object.delete","relation.create","relation.delete","event.record","action.create","space.create"],
        "worlds": []
    })).await?;
    let agent_token = reg["result"]["token"].as_str().unwrap_or("").to_string();
    if agent_token.is_empty() {
        anyhow::bail!("agent-demo registration denied");
    }
    std::env::set_var("SPACEOS_AGENT_TOKEN", &agent_token);
    hooks.agent_register(
        &mission_corr,
        agent_id,
        Some("local"),
        Some(&runtime_instance_id),
    );
    hooks.agent_responsibility_change(&mission_corr, agent_id, responsibility_id, "assigned");
    hooks.agent_responsibility_change(&mission_corr, agent_id, responsibility_id, "started");

    let mission = "Prepare result for SpaceOS V1";

    // 1. Space (actor carried alongside: Space has no actor field of its own).
    let space = Space::new("SpaceOS Development", json!({}));
    let mut space_params = json!(space);
    space_params["actor"] = json!("agent-demo");
    let space = call("create_space", space_params).await?;
    let world_id = space["id"].as_str().unwrap().to_string();
    println!("space: {}", world_id);

    // 2. Objects
    let project = obj("Project", "SpaceOS V1", &world_id);
    let mut agent = obj("Agent", "SpaceOS Demo Agent", &world_id);
    // Phase 2: stable Agent identity across runs. The definition (name)
    // is the id; a rerun upserts the same row instead of minting a
    // duplicate (V1.5 C3 bug). Runtime state (token) is NOT the identity.
    // Note: deliberately independent of the per-run world_id below.
    agent.id = "agent:SpaceOS Demo Agent".to_string();
    agent.properties["mission"] = json!(mission);
    agent.properties["status"] = json!("running");
    agent.properties["capabilities"] = json!([
        "create_object",
        "create_relation",
        "record_event",
        "query_objects",
        "query_relations",
        "request_action"
    ]);
    let agent = call("create_object", json!(agent)).await?;
    let document = obj("Document", "Architecture", &world_id);
    let file = obj("File", "main.rs", &world_id);
    let project = call("create_object", json!(project)).await?;
    let document = call("create_object", json!(document)).await?;
    let file = call("create_object", json!(file)).await?;

    let ev = |t: &str, payload: serde_json::Value| {
        let mut e = Event::new(t, "agent-demo", payload);
        e.provider_id = Some("agent-demo".into());
        e
    };

    // 3. Relations
    let r1 = Relation::new(
        agent["id"].as_str().unwrap(),
        "works_on",
        project["id"].as_str().unwrap(),
    );
    let r2 = Relation::new(
        document["id"].as_str().unwrap(),
        "describes",
        project["id"].as_str().unwrap(),
    );
    let r3 = Relation::new(
        project["id"].as_str().unwrap(),
        "contains",
        file["id"].as_str().unwrap(),
    );
    for r in [&r1, &r2, &r3] {
        call("create_relation", json!(r)).await?;
    }

    // Events
    call(
        "record_event",
        json!(ev("AgentStarted", json!({"mission": mission}))),
    )
    .await?;
    call(
        "record_event",
        json!(ev("ActionStarted", json!({"action": "prepare_result"}))),
    )
    .await?;
    call(
        "record_event",
        json!(ev(
            "ObjectCreated",
            json!({"type": "Project", "id": project["id"]})
        )),
    )
    .await?;
    call(
        "record_event",
        json!(ev("RelationCreated", json!({"relation_type": "works_on"}))),
    )
    .await?;

    // 4. Mission: create Result
    spacetrace::hooks::global_hooks().agent_responsibility_change(
        &std::env::var("SPACEOS_MISSION_CORR").unwrap_or_default(),
        agent_id,
        responsibility_id,
        "progressed",
    );
    let result = obj("Result", "V1 Result", &world_id);
    let result = call("create_object", json!(result)).await?;
    let r4 = Relation::new(
        agent["id"].as_str().unwrap(),
        "produces",
        result["id"].as_str().unwrap(),
    );
    call("create_relation", json!(r4)).await?;
    call(
        "record_event",
        json!(ev(
            "ObjectCreated",
            json!({"type": "Result", "id": result["id"]})
        )),
    )
    .await?;
    call(
        "record_event",
        json!(ev("RelationCreated", json!({"relation_type": "produces"}))),
    )
    .await?;

    // 5. Action through policy/provider (same V0 pipeline)
    let mission_corr = std::env::var("SPACEOS_MISSION_CORR").unwrap_or_default();
    // Result path follows the served filesystem root: the provider gates
    // writes to its allowed roots, so the demo honors SPACEOS_RESULT_FILE
    // (default keeps the historic location for standalone runs).
    let result_path = std::env::var("SPACEOS_RESULT_FILE")
        .unwrap_or_else(|_| r"C:\spaceos-test\v1-result.txt".to_string());
    let hooks = spacetrace::hooks::global_hooks();
    hooks.agent_decision(
        &mission_corr,
        agent_id,
        &agent_instance_id,
        Some(responsibility_id),
        "prepare_result",
        json!({"action": "create", "target": result_path.clone()}),
    );
    let mut action = ActionRequest::new(
        "create",
        json!({"path": result_path, "content": "V1 Result"}),
        "filesystem",
        "agent-demo",
    );
    action.actor_token = agent_token.clone();
    hooks.agent_action(
        &mission_corr,
        agent_id,
        &agent_instance_id,
        Some(responsibility_id),
        &action.id,
        "create",
        &result_path,
    );
    let action_resp = ipc::send_request(ipc::CORE_PIPE, "request_action", json!(action)).await?;
    let status = action_resp["result"]["status"].as_str().unwrap_or("?");
    println!("action: {status}");
    if status != "Completed" {
        anyhow::bail!(
            "agent-demo filesystem action failed: status={status} detail={}",
            action_resp["result"]["detail"]
        );
    }

    call(
        "record_event",
        json!(ev("ActionCompleted", json!({"action": "create"}))),
    )
    .await?;
    call("record_event", json!(ev("AgentStopped", json!({})))).await?;

    // status -> completed
    let mut agent_obj: Object = serde_json::from_value(agent.clone())?;
    agent_obj.properties["status"] = json!("completed");
    call("update_object", json!(agent_obj)).await?;

    let mission_corr = std::env::var("SPACEOS_MISSION_CORR").unwrap_or_default();
    spacetrace::hooks::global_hooks().agent_responsibility_change(
        &mission_corr,
        agent_id,
        responsibility_id,
        "completed",
    );
    spacetrace::hooks::global_hooks().runtime_stop(
        &mission_corr,
        "local",
        &runtime_instance_id,
        agent_id,
        Some("Task"),
    );
    println!("agent-demo done");
    Ok(())
}

fn obj(t: &str, name: &str, world_id: &str) -> Object {
    let mut o = Object::new(t, "agent-demo", json!({"name": name, "world_id": world_id}));
    o.external_id = Some(format!("{world_id}:{t}:{name}"));
    o
}

async fn call(method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
    // agent-demo threads its agent token through every mutation.
    let token = std::env::var("SPACEOS_AGENT_TOKEN").unwrap_or_default();
    let mut params = params;
    if !token.is_empty() {
        params["actor_token"] = json!(token);
    }
    // X1 finding: Object carries provider_id and Space params carry an
    // explicit actor, but Relation has neither — the gate then sees
    // "anonymous" and denies. Bind the demo identity explicitly so every
    // mutation authenticates as agent-demo (no Core change: the gate is
    // correct to deny anonymous).
    if params.get("actor").is_none()
        && params.get("actor_id").is_none()
        && params.get("provider_id").is_none()
    {
        params["actor"] = json!("agent-demo");
    }
    // Thread mission correlation so Core-side mutation traces link to the
    // agent-side decision/action spans (cross-process join key). Skipped for
    // record_event: Event.correlation_id is semantic (trace key derives from
    // it), so injecting would change event semantics with zero trace benefit.
    if method != "record_event" && params.get("correlation_id").is_none() {
        if let Ok(corr) = std::env::var("SPACEOS_MISSION_CORR") {
            if !corr.is_empty() {
                params["correlation_id"] = json!(corr);
            }
        }
    }
    let resp = ipc::send_request(ipc::CORE_PIPE, method, params).await?;
    if resp["ok"].as_bool() == Some(true) {
        Ok(resp["result"].clone())
    } else {
        anyhow::bail!(resp["error"].as_str().unwrap_or("error").to_string())
    }
}
