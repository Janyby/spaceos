//! Mission views and legacy workflow helpers.
//!
//! MVP-2 HTTP writes use mission_commands -> Core mission_command. The old
//! observation-based mutators below remain for compatibility diagnostics and
//! the not-yet-migrated MCP caller; Core denies them. They are NOT an executor
//! and must not be used to bypass the dedicated scoped command contract.
//!
//! A Mission is NOT a new table, queue, or history: it is a domain
//! [`Object`] of type `Mission` plus `MissionStep` objects linked by a
//! `mission_has_step` [`Relation`], with every state change applied by
//! Core through one atomic `report_observation` (objects -> relations ->
//! event -> observation log). Providers, Dashboard and MCP never write
//! domain rows directly; the transition validators below run in the
//! bridge (HTTP + MCP share them), and Core persists the result.
//!
//! Step -> Action linkage uses plain id lists inside step properties plus
//! the shared `correlation_id` event chain. It deliberately does NOT use
//! [`Relation`] rows pointing at action ids: relations have a
//! `relations -> objects` FK, and actions are events, not objects -- such
//! a row would roll the whole transaction back.
//!
//! Lifecycle (Mission):
//! `created -> planned -> running <-> awaiting_approval -> verifying ->
//!  completed | failed -> running (resume) | cancelled (terminal)`
//! Lifecycle (Step):
//! `pending -> running <-> awaiting_approval -> completed | failed ->
//!  running (retry) | skipped`
//!
//! Re-delivery safety is two-layered: Core dedups by `observation_id`
//! (`observation_log`), and the API layer treats "already in the target
//! state" as a no-op success (`applied: false`, nothing re-executed).

use serde_json::{json, Value};

/// Domain object types and the step-link relation type.
pub const MISSION_TYPE: &str = "Mission";
pub const STEP_TYPE: &str = "MissionStep";
pub const HAS_STEP_REL: &str = "mission_has_step";

// Mission states.
pub const ST_CREATED: &str = "created";
pub const ST_PLANNED: &str = "planned";
pub const ST_RUNNING: &str = "running";
pub const ST_AWAITING: &str = "awaiting_approval";
pub const ST_VERIFYING: &str = "verifying";
pub const ST_COMPLETED: &str = "completed";
pub const ST_FAILED: &str = "failed";
pub const ST_CANCELLED: &str = "cancelled";

// Step states.
pub const STEP_PENDING: &str = "pending";
pub const STEP_RUNNING: &str = "running";
pub const STEP_AWAITING: &str = "awaiting_approval";
pub const STEP_COMPLETED: &str = "completed";
pub const STEP_FAILED: &str = "failed";
pub const STEP_SKIPPED: &str = "skipped";

// Mission domain events (all carry correlation_id = mission id).
pub const EV_CREATED: &str = "MissionCreated";
pub const EV_PLANNED: &str = "MissionPlanned";
pub const EV_STARTED: &str = "MissionStarted";
pub const EV_STEP_STARTED: &str = "StepStarted";
pub const EV_STEP_COMPLETED: &str = "StepCompleted";
pub const EV_STEP_FAILED: &str = "StepFailed";
pub const EV_APPROVAL_REQUESTED: &str = "MissionApprovalRequested";
pub const EV_APPROVED: &str = "MissionApproved";
pub const EV_VERIFYING: &str = "MissionVerificationStarted";
pub const EV_VERIFIED: &str = "MissionVerified";
pub const EV_COMPLETED: &str = "MissionCompleted";
pub const EV_FAILED: &str = "MissionFailed";
pub const EV_CANCELLED: &str = "MissionCancelled";
pub const EV_RESUMED: &str = "MissionResumed";
pub const EV_PROPOSAL: &str = "MissionProposalRecorded";

/// Allowed mission transitions (from -> to).
fn mission_allowed(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        (ST_CREATED, ST_PLANNED)
            | (ST_PLANNED, ST_RUNNING)
            | (ST_RUNNING, ST_AWAITING)
            | (ST_AWAITING, ST_RUNNING)
            | (ST_RUNNING, ST_VERIFYING)
            | (ST_AWAITING, ST_VERIFYING)
            | (ST_VERIFYING, ST_COMPLETED)
            | (ST_VERIFYING, ST_FAILED)
            | (ST_RUNNING, ST_FAILED)
            | (ST_FAILED, ST_RUNNING)
            | (ST_CREATED, ST_CANCELLED)
            | (ST_PLANNED, ST_CANCELLED)
            | (ST_RUNNING, ST_CANCELLED)
            | (ST_AWAITING, ST_CANCELLED)
            | (ST_FAILED, ST_CANCELLED)
    )
}

/// Allowed step transitions (from -> to).
fn step_allowed(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        (STEP_PENDING, STEP_RUNNING)
            | (STEP_RUNNING, STEP_AWAITING)
            | (STEP_AWAITING, STEP_RUNNING)
            | (STEP_RUNNING, STEP_COMPLETED)
            | (STEP_RUNNING, STEP_FAILED)
            | (STEP_FAILED, STEP_RUNNING)
            | (STEP_PENDING, STEP_SKIPPED)
            | (STEP_RUNNING, STEP_SKIPPED)
            | (STEP_FAILED, STEP_SKIPPED)
            | (STEP_AWAITING, STEP_SKIPPED)
    )
}

fn is_terminal(state: &str) -> bool {
    matches!(state, ST_COMPLETED | ST_FAILED | ST_CANCELLED)
}

fn is_step_terminal(state: &str) -> bool {
    matches!(state, STEP_COMPLETED | STEP_FAILED | STEP_SKIPPED)
}

/// Serializes mutating transitions bridge-wide. Transitions are
/// read-validate-write against Core: without this, two concurrent
/// transitions could interleave reads and the loser would overwrite the
/// winner's patch (lost update). Note what this does NOT do: it does
/// not order dependent transitions — callers must still sequence them
/// (await confirmation before sending the next step). An out-of-order
/// `completed` arriving while the step is still `pending` is honestly
/// rejected; the caller retries after observing the new state.
/// Mission operations are human/agent-paced, so one lock for all of
/// them costs nothing measurable.
fn tx_lock() -> &'static tokio::sync::Mutex<()> {
    static TX: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    TX.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Full mission view: mission object + ordered steps + mission events.
#[derive(Debug, Clone)]
pub struct MissionView {
    pub mission: Value,
    pub steps: Vec<Value>,
    pub events: Vec<Value>,
}

impl MissionView {
    pub fn state(&self) -> &str {
        self.mission
            .get("properties")
            .and_then(|p| p.get("state"))
            .and_then(|s| s.as_str())
            .unwrap_or("")
    }

    pub fn to_json(&self) -> Value {
        let total = self.steps.len();
        let done = self
            .steps
            .iter()
            .filter(|s| {
                is_step_terminal(
                    s.get("properties")
                        .and_then(|p| p.get("state"))
                        .and_then(|v| v.as_str())
                        .unwrap_or(""),
                )
            })
            .count();
        json!({
            "mission": self.mission,
            "steps": self.steps,
            "events": self.events,
            "progress": {"total": total, "done": done},
        })
    }
}

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn mission_obj(
    id: &str,
    goal: &str,
    workspace: &str,
    initiator: &str,
    assignee: &str,
    state: &str,
    extra: Value,
) -> Value {
    let mut props = json!({
        "goal": goal,
        "workspace": workspace,
        "initiator": initiator,
        "assignee": assignee,
        "state": state,
        "created_at": extra.get("created_at").cloned().unwrap_or(json!(now_iso())),
        "updated_at": now_iso(),
    });
    for (k, v) in extra.as_object().cloned().unwrap_or_default() {
        if k != "created_at" {
            props[k] = v;
        }
    }
    json!({
        "id": id,
        "type": MISSION_TYPE,
        "properties": props,
        "provider_id": "space-bridge",
        "external_id": null,
        "created_at": props.get("created_at").cloned().unwrap_or(json!(now_iso())),
        "updated_at": now_iso(),
    })
}

fn step_obj(
    id: &str,
    mission_id: &str,
    index: usize,
    title: &str,
    detail: &str,
    req_appr: bool,
) -> Value {
    json!({
        "id": id,
        "type": STEP_TYPE,
        "properties": {
            "mission_id": mission_id,
            "index": index,
            "title": title,
            "detail": detail,
            "state": STEP_PENDING,
            "requires_approval": req_appr,
            "approved": false,
            "action_ids": [],
            "evidence": null,
            "result": null,
            "proposal": null,
            "updated_at": now_iso(),
        },
        "provider_id": "space-bridge",
        "external_id": null,
        "created_at": now_iso(),
        "updated_at": now_iso(),
    })
}

/// Send one atomic observation to Core as `actor` with `actor_token`.
/// Requires the `observation.report` capability (bridge agents request it
/// at registration; no Core change needed).
async fn report(actor: &str, token: &str, obs: Value) -> anyhow::Result<Value> {
    let mut params = obs;
    params["actor"] = json!(actor);
    params["actor_token"] = json!(token);
    let resp = crate::ipc_client::core_request("report_observation", params).await?;
    if resp.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        anyhow::bail!("report_observation rejected: {resp}");
    }
    Ok(resp
        .get("result")
        .cloned()
        .unwrap_or(json!({"applied": true})))
}

fn observation(
    observation_id: String,
    objects: Vec<Value>,
    relations: Vec<Value>,
    event_type: &str,
    actor: &str,
    mission_id: &str,
    payload: Value,
) -> Value {
    json!({
        "observation_id": observation_id,
        "world_id": "",
        "facts": [],
        "objects": objects,
        "relations": relations,
        "delete_ids": [],
        "event": {
            "id": uuid::Uuid::new_v4().to_string(),
            "event_type": event_type,
            "actor": actor,
            "provider_id": "space-bridge",
            "object_ids": objects.iter().filter_map(|o| o.get("id").and_then(|v| v.as_str()).map(|s| s.to_string())).collect::<Vec<_>>(),
            "payload": payload,
            "timestamp": now_iso(),
            "correlation_id": mission_id,
            "world_id": "",
            "seq": 0,
        },
    })
}

async fn fetch_mission(id: &str) -> anyhow::Result<Value> {
    let objs = crate::ipc_client::query_objects(json!({"type": MISSION_TYPE})).await?;
    objs.into_iter()
        .find(|o| o.get("id").and_then(|v| v.as_str()) == Some(id))
        .ok_or_else(|| anyhow::anyhow!("mission not found: {id}"))
}

async fn fetch_steps(mission_id: &str) -> anyhow::Result<Vec<Value>> {
    let rels = crate::ipc_client::query_relations(
        json!({"relation_type": HAS_STEP_REL, "source_object_id": mission_id}),
    )
    .await?;
    let mut targets: Vec<String> = rels
        .into_iter()
        .filter_map(|r| {
            r.get("target_object_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .collect();
    targets.sort();
    targets.dedup();
    if targets.is_empty() {
        return Ok(vec![]);
    }
    let objs = crate::ipc_client::query_objects(json!({"type": STEP_TYPE})).await?;
    let mut steps: Vec<Value> = objs
        .into_iter()
        .filter(|o| {
            o.get("id")
                .and_then(|v| v.as_str())
                .map(|s| targets.contains(&s.to_string()))
                .unwrap_or(false)
        })
        .collect();
    steps.sort_by_key(|s| {
        s.get("properties")
            .and_then(|p| p.get("index"))
            .and_then(|v| v.as_u64())
            .unwrap_or(u64::MAX)
    });
    Ok(steps)
}

async fn fetch_mission_events(mission_id: &str) -> anyhow::Result<Vec<Value>> {
    let resp = crate::ipc_client::world_read_request(
        "query_events",
        json!({}),
    )
    .await?;
    // Read service supplies the authoritative World scope. Never guess scope
    // for legacy events. Mission command correlation identifies commands, not
    // the aggregate; filter only the already-authorized result.
    let events: Vec<Value> = serde_json::from_value(resp["result"].clone())?;
    Ok(events.into_iter().filter(|e| e["payload"]["mission_id"].as_str() == Some(mission_id)).collect())
}

/// Load the full view (mission + steps + events) straight from Core.
pub async fn get_view(mission_id: &str) -> anyhow::Result<MissionView> {
    Ok(MissionView {
        mission: fetch_mission(mission_id).await?,
        steps: fetch_steps(mission_id).await?,
        events: fetch_mission_events(mission_id).await?,
    })
}

fn step_by_id<'a>(steps: &'a [Value], step_id: &str) -> Option<&'a Value> {
    steps
        .iter()
        .find(|s| s.get("id").and_then(|v| v.as_str()) == Some(step_id))
}

/// Create a Mission (`created`). Idempotent on redelivered `mission_id`
/// only in the sense that ids are caller-fresh UUIDs; repeats with the
/// same id return the existing mission untouched.
pub async fn create_mission(
    actor: &str,
    token: &str,
    goal: &str,
    workspace: &str,
    assignee: &str,
) -> anyhow::Result<(MissionView, bool)> {
    if goal.trim().is_empty() {
        anyhow::bail!("goal must not be empty");
    }
    if workspace.trim().is_empty() {
        anyhow::bail!("workspace must not be empty");
    }
    let _tx = tx_lock().lock().await;
    let id = format!("mission-{}", uuid::Uuid::new_v4());
    let obj = mission_obj(&id, goal, workspace, actor, assignee, ST_CREATED, json!({}));
    let obs_id = format!("{id}-created");
    let obs = observation(
        obs_id,
        vec![obj],
        vec![],
        EV_CREATED,
        actor,
        &id,
        json!({"mission_id": id, "goal": goal, "workspace": workspace}),
    );
    let outcome = report(actor, token, obs).await?;
    let applied = outcome
        .get("applied")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    Ok((get_view(&id).await?, applied))
}

/// Attach a plan: steps + `mission_has_step` relations, `created->planned`.
pub async fn plan_mission(
    actor: &str,
    token: &str,
    mission_id: &str,
    steps: Vec<(String, String, bool)>,
) -> anyhow::Result<(MissionView, bool)> {
    if steps.is_empty() {
        anyhow::bail!("plan needs at least one step");
    }
    let _tx = tx_lock().lock().await;
    let view = get_view(mission_id).await?;
    if view.state() == ST_PLANNED {
        return Ok((view, false));
    }
    if view.state() != ST_CREATED {
        anyhow::bail!(
            "plan allowed only from created/planned, mission is {}",
            view.state()
        );
    }
    let created_at = view
        .mission
        .get("properties")
        .and_then(|p| p.get("created_at"))
        .cloned()
        .unwrap_or(json!(now_iso()));
    let goal = view
        .mission
        .get("properties")
        .and_then(|p| p.get("goal"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let workspace = view
        .mission
        .get("properties")
        .and_then(|p| p.get("workspace"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let initiator = view
        .mission
        .get("properties")
        .and_then(|p| p.get("initiator"))
        .and_then(|v| v.as_str())
        .unwrap_or(actor)
        .to_string();
    let assignee = view
        .mission
        .get("properties")
        .and_then(|p| p.get("assignee"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let mut objects = vec![mission_obj(
        mission_id,
        &goal,
        &workspace,
        &initiator,
        &assignee,
        ST_PLANNED,
        json!({"created_at": created_at}),
    )];
    let mut relations = vec![];
    let mut titles = vec![];
    for (i, (title, detail, req)) in steps.iter().enumerate() {
        if title.trim().is_empty() {
            anyhow::bail!("step {} needs a title", i + 1);
        }
        let sid = format!("{mission_id}-step-{}", i + 1);
        objects.push(step_obj(&sid, mission_id, i + 1, title, detail, *req));
        relations.push(json!({
            "id": format!("{mission_id}-has-{sid}"),
            "source_object_id": mission_id,
            "relation_type": HAS_STEP_REL,
            "target_object_id": sid,
            "properties": {"index": i + 1},
            "created_at": now_iso(),
            "world_id": "",
        }));
        titles.push(json!({"step_id": sid, "title": title}));
    }
    let obs = observation(
        format!("{mission_id}-planned-{}", uuid::Uuid::new_v4()),
        objects,
        relations,
        EV_PLANNED,
        actor,
        mission_id,
        json!({"mission_id": mission_id, "steps": titles}),
    );
    let outcome = report(actor, token, obs).await?;
    let applied = outcome
        .get("applied")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    Ok((get_view(mission_id).await?, applied))
}

/// Mission-level transition with validation. Terminal states are sticky:
/// repeating the current state is a no-op (`applied: false`).
/// `complete` additionally requires a passed verification on record.
pub async fn transition_mission(
    actor: &str,
    token: &str,
    mission_id: &str,
    to: &str,
    extra: Value,
) -> anyhow::Result<(MissionView, bool)> {
    let _tx = tx_lock().lock().await;
    let view = get_view(mission_id).await?;
    let from = view.state().to_string();
    if from == to {
        return Ok((view, false));
    }
    if !mission_allowed(&from, to) {
        anyhow::bail!("mission transition {from} -> {to} not allowed");
    }
    if to == ST_COMPLETED {
        let passed = view
            .mission
            .get("properties")
            .and_then(|p| p.get("verification"))
            .and_then(|v| v.get("passed"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !passed {
            anyhow::bail!("complete requires a passed verification (run verify first)");
        }
    }
    let mut objects = vec![with_state(view.mission.clone(), to, &extra)];
    // Cancelling skips every non-terminal step in the same transaction:
    // already-executed work is untouched, nothing new may start.
    if to == ST_CANCELLED {
        for s in &view.steps {
            let st = s
                .get("properties")
                .and_then(|p| p.get("state"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !is_step_terminal(st) {
                objects.push(with_step_state(s.clone(), STEP_SKIPPED, &json!({})));
            }
        }
    }
    let ev = match to {
        ST_RUNNING if from == ST_FAILED => EV_RESUMED,
        ST_RUNNING if from == ST_AWAITING => EV_APPROVED,
        ST_RUNNING => EV_STARTED,
        ST_AWAITING => EV_APPROVAL_REQUESTED,
        ST_VERIFYING => EV_VERIFYING,
        ST_COMPLETED => EV_COMPLETED,
        ST_FAILED => EV_FAILED,
        ST_CANCELLED => EV_CANCELLED,
        _ => EV_STARTED,
    };
    let obs = observation(
        format!("{mission_id}-{to}-{}", uuid::Uuid::new_v4()),
        objects,
        vec![],
        ev,
        actor,
        mission_id,
        json!({"mission_id": mission_id, "from": from, "to": to, "detail": extra}),
    );
    let outcome = report(actor, token, obs).await?;
    let applied = outcome
        .get("applied")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    Ok((get_view(mission_id).await?, applied))
}

fn with_state(mut mission: Value, state: &str, extra: &Value) -> Value {
    if let Some(p) = mission
        .get_mut("properties")
        .and_then(|v| v.as_object_mut())
    {
        p.insert("state".to_string(), json!(state));
        p.insert("updated_at".to_string(), json!(now_iso()));
        if let Some(o) = extra.as_object() {
            for (k, v) in o {
                p.insert(k.clone(), v.clone());
            }
        }
    }
    mission["updated_at"] = json!(now_iso());
    mission
}

fn with_step_state(mut step: Value, state: &str, extra: &Value) -> Value {
    if let Some(p) = step.get_mut("properties").and_then(|v| v.as_object_mut()) {
        p.insert("state".to_string(), json!(state));
        p.insert("updated_at".to_string(), json!(now_iso()));
        if let Some(o) = extra.as_object() {
            for (k, v) in o {
                p.insert(k.clone(), v.clone());
            }
        }
    }
    step["updated_at"] = json!(now_iso());
    step
}

/// Record a file-change proposal: snapshots current content via the
/// normal Core read path, stores old+new side by side. No write happens
/// here; the write still goes through `filesystem/write` with conflict
/// stamps after approval.
pub async fn propose_step_change(
    actor: &str,
    token: &str,
    current: &Value,
    mission_id: &str,
    step_id: &str,
    path: &str,
    new_content: &str,
) -> anyhow::Result<(MissionView, bool)> {
    let _tx = tx_lock().lock().await;
    let view = get_view(mission_id).await?;
    let step = step_by_id(&view.steps, step_id)
        .ok_or_else(|| anyhow::anyhow!("step not found: {step_id}"))?;
    let detail = current
        .get("result")
        .and_then(|r| r.get("detail"))
        .or_else(|| current.get("detail"))
        .unwrap_or(current);
    let old_content = detail
        .get("content")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let proposal = json!({
        "path": path,
        "old_content": old_content,
        "new_content": new_content,
        "base_modified": detail.get("modified").cloned().unwrap_or(json!(null)),
        "base_size": detail.get("size").cloned().unwrap_or(json!(null)),
        "recorded_at": now_iso(),
        "recorded_by": actor,
    });
    let updated = with_step_state(
        step.clone(),
        current_step_state(step),
        &json!({"proposal": proposal}),
    );
    let obs = observation(
        format!("{mission_id}-{step_id}-proposal-{}", uuid::Uuid::new_v4()),
        vec![updated],
        vec![],
        EV_PROPOSAL,
        actor,
        mission_id,
        json!({"mission_id": mission_id, "step_id": step_id, "path": path}),
    );
    let outcome = report(actor, token, obs).await?;
    let applied = outcome
        .get("applied")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    Ok((get_view(mission_id).await?, applied))
}

fn current_step_state(step: &Value) -> &str {
    step.get("properties")
        .and_then(|p| p.get("state"))
        .and_then(|v| v.as_str())
        .unwrap_or(STEP_PENDING)
}

/// Step transition with validation + the approval gate: a step with
/// `requires_approval` completes only after `approved: true` (set by the
/// `awaiting_approval -> running` approval). Repeating a terminal state is
/// a no-op; retrying a failed step re-opens it without touching history.
pub async fn transition_step(
    actor: &str,
    token: &str,
    mission_id: &str,
    step_id: &str,
    to: &str,
    extra: Value,
) -> anyhow::Result<(MissionView, bool)> {
    let _tx = tx_lock().lock().await;
    let view = get_view(mission_id).await?;
    // Terminal missions are sticky but harmless to re-observe: a
    // redelivered or late step command is a no-op success, never a
    // re-execution and never an error masking the true state.
    if is_terminal(view.state()) {
        return Ok((view, false));
    }
    let step = step_by_id(&view.steps, step_id)
        .ok_or_else(|| anyhow::anyhow!("step not found: {step_id}"))?
        .clone();
    let from = current_step_state(&step).to_string();
    if from == to {
        return Ok((view, false));
    }
    if !step_allowed(&from, to) {
        anyhow::bail!("step transition {from} -> {to} not allowed");
    }
    let props = step.get("properties").cloned().unwrap_or(json!({}));
    let requires_approval = props
        .get("requires_approval")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let approved = props
        .get("approved")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if to == STEP_COMPLETED && requires_approval && !approved {
        anyhow::bail!("step requires approval first (request_approval, then approve)");
    }
    let mut patch = json!({});
    if to == STEP_RUNNING && from == STEP_AWAITING {
        patch["approved"] = json!(true);
    }
    if let Some(o) = extra.as_object() {
        for (k, v) in o {
            patch[k.clone()] = v.clone();
        }
    }
    // Keep action linkage auditable: append, never rewrite.
    if let (Some(dst), Some(src)) = (patch.get_mut("action_ids"), props.get("action_ids")) {
        let mut ids: Vec<Value> = src.as_array().cloned().unwrap_or_default();
        for v in dst.as_array().cloned().unwrap_or_default() {
            if !ids.contains(&v) {
                ids.push(v);
            }
        }
        patch["action_ids"] = json!(ids);
    } else if props.get("action_ids").is_some() && patch.get("action_ids").is_none() {
        patch["action_ids"] = props["action_ids"].clone();
    }
    let updated = with_step_state(step, to, &patch);
    let ev = match to {
        STEP_RUNNING => EV_STEP_STARTED,
        STEP_COMPLETED => EV_STEP_COMPLETED,
        STEP_FAILED => EV_STEP_FAILED,
        _ => EV_STEP_STARTED,
    };
    // A step starting implicitly starts a planned mission (first work).
    let mut objects = vec![updated];
    let mut mission_ev_extra = json!({});
    if to == STEP_RUNNING && view.state() == ST_PLANNED {
        objects.push(with_state(view.mission.clone(), ST_RUNNING, &json!({})));
        mission_ev_extra = json!({"mission_auto_started": true});
    }
    let obs = observation(
        format!("{mission_id}-{step_id}-{to}-{}", uuid::Uuid::new_v4()),
        objects,
        vec![],
        ev,
        actor,
        mission_id,
        json!({"mission_id": mission_id, "step_id": step_id, "from": from, "to": to, "detail": patch, "mission": mission_ev_extra}),
    );
    let outcome = report(actor, token, obs).await?;
    let applied = outcome
        .get("applied")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    Ok((get_view(mission_id).await?, applied))
}

/// Record verification evidence on the mission (`verifying` must be the
/// current state; use the mission transition to get there). A passed
/// verification unlocks `complete`.
pub async fn record_verification(
    actor: &str,
    token: &str,
    mission_id: &str,
    passed: bool,
    method: &str,
    detail: Value,
) -> anyhow::Result<(MissionView, bool)> {
    let _tx = tx_lock().lock().await;
    let view = get_view(mission_id).await?;
    if view.state() != ST_VERIFYING {
        anyhow::bail!(
            "verification recorded only in verifying state (mission is {})",
            view.state()
        );
    }
    let updated = with_state(
        view.mission.clone(),
        ST_VERIFYING,
        &json!({"verification": {"passed": passed, "method": method, "detail": detail, "recorded_at": now_iso(), "recorded_by": actor}}),
    );
    let obs = observation(
        format!("{mission_id}-verification-{}", uuid::Uuid::new_v4()),
        vec![updated],
        vec![],
        EV_VERIFIED,
        actor,
        mission_id,
        json!({"mission_id": mission_id, "passed": passed, "method": method}),
    );
    let outcome = report(actor, token, obs).await?;
    let applied = outcome
        .get("applied")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    Ok((get_view(mission_id).await?, applied))
}

/// List mission summaries (id, goal, workspace, state, progress).
pub async fn list_missions() -> anyhow::Result<Vec<Value>> {
    let objs = crate::ipc_client::query_objects(json!({"type": MISSION_TYPE})).await?;
    let mut out: Vec<Value> = Vec::new();
    for m in objs {
        let id = m
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let steps = fetch_steps(&id).await.unwrap_or_default();
        let total = steps.len();
        let done = steps
            .iter()
            .filter(|s| {
                is_step_terminal(
                    s.get("properties")
                        .and_then(|p| p.get("state"))
                        .and_then(|v| v.as_str())
                        .unwrap_or(""),
                )
            })
            .count();
        let p = m.get("properties").cloned().unwrap_or(json!({}));
        out.push(json!({
            "mission_id": id,
            "goal": p.get("goal").cloned().unwrap_or(json!("")),
            "workspace": p.get("workspace").cloned().unwrap_or(json!("")),
            "state": p.get("state").cloned().unwrap_or(json!("")),
            "assignee": p.get("assignee").cloned().unwrap_or(json!("")),
            "updated_at": m.get("updated_at").cloned().unwrap_or(json!("")),
            "execution_status": p.get("execution_status").cloned().unwrap_or(json!("unknown_legacy")),
            "execution_model": p.get("execution_model").cloned().unwrap_or(Value::Null),
            "execution_result": p.get("execution_result").cloned().unwrap_or(Value::Null),
            "progress": {"total": total, "done": done},
        }));
    }
    out.sort_by(|a, b| {
        b.get("updated_at")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .cmp(a.get("updated_at").and_then(|v| v.as_str()).unwrap_or(""))
    });
    Ok(out)
}
