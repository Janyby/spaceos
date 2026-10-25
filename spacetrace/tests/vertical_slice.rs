//! Vertical Slice 1: World -> Agent -> Responsibility -> Action -> Policy -> Core
//! -> Provider -> Reality -> Observation -> Core Event -> View
//!
//! Proves a single correlated trace chain across layers without changing
//! Core semantics. Uses SpaceTrace hooks directly (no Core dependency).

use serde_json::json;
use spacetrace::{SpaceTrace, TraceLayer};

/// Intra-binary exclusion (see support/global_lock.rs): each test
/// installs its own SpaceTrace into the process-global hooks
/// (last-wins overwrite, decision #22) while asserting on its OWN
/// provider — parallel execution misdelivers traces across tests.
#[path = "support/global_lock.rs"]
mod global_lock;

#[test]
fn vertical_slice_full_chain_correlated() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let st = SpaceTrace::new().unwrap();
    spacetrace::init_global_hooks(st.buffer(), st.correlation());

    let correlation_id = "vs1-corr-001";
    let world_id = "world-vs1";
    let agent_id = "agent:A17";
    let agent_instance = "inst-A17-001";
    let responsibility_id = "R8";
    let action_id = "act-001";
    let provider_id = "filesystem";

    let hooks = spacetrace::hooks::global_hooks();
    assert!(
        spacetrace::hooks_active(),
        "hooks must be active after init"
    );

    // 1. Agent runtime starts (Codex, Task format)
    hooks.runtime_start(
        correlation_id,
        "Codex",
        "rt-inst-001",
        agent_id,
        Some("Task"),
    );

    // 2. Agent registers
    hooks.agent_register(correlation_id, agent_id, Some("Codex"), Some("rt-inst-001"));

    // 3. Responsibility assigned -> started
    hooks.agent_responsibility_change(correlation_id, agent_id, responsibility_id, "assigned");
    hooks.agent_responsibility_change(correlation_id, agent_id, responsibility_id, "started");

    // 4. Agent decision -> action
    hooks.agent_decision(
        correlation_id,
        agent_id,
        agent_instance,
        Some(responsibility_id),
        "create_file",
        json!({"target": "/tmp/vs1.txt"}),
    );
    hooks.agent_action(
        correlation_id,
        agent_id,
        agent_instance,
        Some(responsibility_id),
        action_id,
        "create",
        "/tmp/vs1.txt",
    );

    // 5. Policy check
    hooks.core_policy_check(
        correlation_id,
        agent_id,
        "object.create",
        true,
        Some(world_id),
    );

    // 6. Core mutation start -> commit
    hooks.core_mutation_start(
        correlation_id,
        "create_object",
        agent_id,
        Some(world_id),
        json!({"id": "obj-001"}),
    );
    hooks.core_mutation_commit(correlation_id, "create_object", agent_id, Some(world_id), 2);

    // 7. Storage transaction begin -> commit
    hooks.storage_transaction_begin(correlation_id, 1);
    hooks.storage_transaction_commit(correlation_id, 1);

    // 8. Provider action invoke -> complete
    hooks.provider_action_invoke(
        correlation_id,
        provider_id,
        "create",
        "/tmp/vs1.txt",
        agent_id,
    );
    hooks.provider_action_complete(correlation_id, provider_id, "create", "Completed", 5);

    // 9. Reality change -> watcher -> Core event
    hooks.reality_change(correlation_id, provider_id, "create", "/tmp/vs1.txt");
    hooks.provider_watcher_event(
        correlation_id,
        provider_id,
        "create",
        "/tmp/vs1.txt",
        Some("fp-abc"),
    );
    hooks.core_event_published(
        "evt-001",
        Some(correlation_id.to_string()),
        "FileCreated",
        agent_id,
        world_id,
        Some(provider_id.to_string()),
    );

    // 10. Subscription delivery -> view poll/render
    hooks.core_subscription_delivery(
        "sub-001",
        "viewer-1",
        "evt-001",
        Some(correlation_id.to_string()),
        agent_id,
        world_id,
        3,
    );
    hooks.view_poll("vs1-view-poll-001", "tree-view", world_id, "viewer-1");
    hooks.view_render("vs1-view-poll-001", "tree-view", world_id, 12);

    // Verify correlation: all events linked by correlation_id
    let by_corr = st.provider().query_by_correlation(correlation_id);
    assert!(
        by_corr.len() >= 15,
        "expected >=15 trace events for full chain, got {}",
        by_corr.len()
    );

    // Verify agent index
    let by_agent = st.provider().query_by_agent(agent_id);
    assert!(!by_agent.is_empty(), "agent index must have events");

    // Verify world index
    let by_world = st.provider().query_by_world(world_id);
    assert!(!by_world.is_empty(), "world index must have events");

    // Verify action index
    let by_action = st.provider().query_by_action(action_id);
    assert!(!by_action.is_empty(), "action index must have events");

    // Verify provider index
    let by_provider = st.provider().query_by_provider(provider_id);
    assert!(!by_provider.is_empty(), "provider index must have events");

    // Verify responsibility index
    let by_resp = st.provider().query_by_responsibility(responsibility_id);
    assert!(!by_resp.is_empty(), "responsibility index must have events");

    // Verify runtime separation: agent_id != runtime
    let by_runtime = st.provider().query_by_runtime_type("Codex");
    assert!(!by_runtime.is_empty(), "runtime index must have events");

    // Verify layers present (View uses separate poll correlation - check via world)
    let layers: std::collections::HashSet<String> =
        by_corr.iter().map(|e| format!("{:?}", e.layer)).collect();
    for expected in ["Agent", "Runtime", "Core", "Storage", "Provider", "Reality"] {
        assert!(
            layers.contains(expected),
            "layer {expected} missing in {layers:?}"
        );
    }
    // View events use their own poll correlation but same world
    let by_world = st.provider().query_by_world(world_id);
    let view_events: Vec<_> = by_world
        .iter()
        .filter(|e| e.layer == TraceLayer::View)
        .collect();
    assert!(
        !view_events.is_empty(),
        "View layer must have events in world {world_id}"
    );
}

#[test]
fn runtime_replacement_preserves_identity() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let st = SpaceTrace::new().unwrap();
    spacetrace::init_global_hooks(st.buffer(), st.correlation());
    let hooks = spacetrace::hooks::global_hooks();

    let agent_id = "agent:A17";
    let responsibility_id = "R8";
    let corr1 = "rt-replace-001";
    let corr2 = "rt-replace-002";

    // Runtime = Codex
    hooks.runtime_start(corr1, "Codex", "rt-codex-001", agent_id, Some("Task"));
    hooks.agent_action(
        corr1,
        agent_id,
        "inst-001",
        Some(responsibility_id),
        "act-001",
        "create",
        "/tmp/a.txt",
    );
    hooks.runtime_stop(corr1, "Codex", "rt-codex-001", agent_id, Some("Task"));

    // Runtime = Hermes (same agent, same responsibility)
    hooks.runtime_start(
        corr2,
        "Hermes",
        "rt-hermes-001",
        agent_id,
        Some("Universal"),
    );
    hooks.agent_action(
        corr2,
        agent_id,
        "inst-002",
        Some(responsibility_id),
        "act-002",
        "create",
        "/tmp/b.txt",
    );
    hooks.runtime_stop(
        corr2,
        "Hermes",
        "rt-hermes-001",
        agent_id,
        Some("Universal"),
    );

    // Runtime = dots
    let corr3 = "rt-replace-003";
    hooks.runtime_start(corr3, "dots", "rt-dots-001", agent_id, Some("Persistent"));
    hooks.agent_action(
        corr3,
        agent_id,
        "inst-003",
        Some(responsibility_id),
        "act-003",
        "create",
        "/tmp/c.txt",
    );

    // Agent identity preserved across runtimes
    // corr1: start+action+stop (3 with agent_id), corr2: start+action+stop (3), corr3: start+action (2)
    let by_agent = st.provider().query_by_agent(agent_id);
    assert_eq!(by_agent.len(), 8, "all 8 events linked to same agent_id");

    // Responsibility preserved
    let by_resp = st.provider().query_by_responsibility(responsibility_id);
    assert_eq!(by_resp.len(), 3, "3 actions linked to same responsibility");

    // Runtimes distinguished (only start/stop carry runtime_type; actions carry agent+responsibility)
    assert_eq!(st.provider().query_by_runtime_type("Codex").len(), 2);
    assert_eq!(st.provider().query_by_runtime_type("Hermes").len(), 2);
    assert_eq!(st.provider().query_by_runtime_type("dots").len(), 1); // start only (no stop yet)

    // Runtime formats distinguished but agent identity unchanged (gate §3)
    assert_eq!(st.provider().query_by_runtime_format("Task").len(), 2);
    assert_eq!(st.provider().query_by_runtime_format("Universal").len(), 2);
    assert_eq!(st.provider().query_by_runtime_format("Persistent").len(), 1);
}

#[test]
fn mcp_a2a_remain_protocols_not_agents() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let st = SpaceTrace::new().unwrap();
    spacetrace::init_global_hooks(st.buffer(), st.correlation());
    let hooks = spacetrace::hooks::global_hooks();

    let agent_id = "agent:A17";
    let corr_mcp = "mcp-001";
    let corr_a2a = "a2a-001";

    // MCP: Agent Runtime -> MCP -> Provider
    hooks.runtime_mcp(corr_mcp, "rt-001", agent_id, "read_file", "filesystem");

    // A2A: Agent -> A2A -> Other Agent
    hooks.runtime_a2a(corr_a2a, "rt-001", agent_id, "agent:A22", "review_code");

    // Protocols indexed separately from agents
    let by_mcp = st.provider().query_by_protocol("MCP");
    assert_eq!(by_mcp.len(), 1);
    assert_eq!(by_mcp[0].agent_id.as_deref(), Some(agent_id));

    let by_a2a = st.provider().query_by_protocol("A2A");
    assert_eq!(by_a2a.len(), 1);

    // Agent identity unchanged by protocol use
    let by_agent = st.provider().query_by_agent(agent_id);
    assert_eq!(
        by_agent.len(),
        2,
        "both protocol interactions linked to same agent"
    );
}

#[test]
fn trace_failure_does_not_affect_core() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    // Hooks without init are no-ops (I1, I3, I17)
    // This test uses a fresh process-state assumption: global hooks may already
    // be initialized by other tests, so we test the no-panic property instead.
    let hooks = spacetrace::hooks::global_hooks();
    // These must never panic, even under contention
    hooks.core_mutation_start("c", "create_object", "a", Some("w"), json!({}));
    hooks.core_mutation_commit("c", "create_object", "a", Some("w"), 1);
    hooks.error("c", TraceLayer::Core, "test error", json!({}));
    // If we reach here, trace failure isolation holds (no panic propagated)
}

#[test]
fn provider_neutrality_same_model() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let st = SpaceTrace::new().unwrap();
    spacetrace::init_global_hooks(st.buffer(), st.correlation());
    let hooks = spacetrace::hooks::global_hooks();

    for provider in [
        "filesystem",
        "git",
        "database",
        "web",
        "agent",
        "spacetrace",
    ] {
        let corr = format!("prov-{provider}-001");
        hooks.provider_scan_start(&corr, provider, "/test");
        hooks.provider_scan_end(&corr, provider, json!({"created": 1}), 10);
    }

    // Same trace model for all providers (I26, I27)
    for provider in [
        "filesystem",
        "git",
        "database",
        "web",
        "agent",
        "spacetrace",
    ] {
        let events = st.provider().query_by_provider(provider);
        assert_eq!(
            events.len(),
            2,
            "provider {provider} must have 2 events (start+end)"
        );
        for e in &events {
            assert_eq!(e.layer, TraceLayer::Provider);
        }
    }
}
