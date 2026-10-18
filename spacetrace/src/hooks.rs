//! Hook registry — Core/Provider integration points for trace emission.
//!
//! These hooks are called by SpaceOS components (Core, Providers, Agent)
//! to emit trace events. SpaceTrace remains optional — hooks are no-ops
//! if SpaceTrace is not initialized.

use crate::{CorrelationIndex, TraceBuffer, TraceEvent, TraceKind, TraceLayer};
use parking_lot::RwLock;
use std::sync::{Arc, LazyLock};

/// Global hook registry. Initialized once at startup.
pub struct HookRegistry {
    inner: RwLock<Option<HookRegistryInner>>,
}

struct HookRegistryInner {
    buffer: Arc<TraceBuffer>,
    correlation: Arc<CorrelationIndex>,
}

impl HookRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(None),
        }
    }

    /// Initialize the hook registry with SpaceTrace components.
    /// Call once at startup after creating SpaceTrace.
    pub fn initialize(&self, buffer: Arc<TraceBuffer>, correlation: Arc<CorrelationIndex>) {
        *self.inner.write() = Some(HookRegistryInner {
            buffer,
            correlation,
        });
    }

    /// Check if hooks are active.
    pub fn is_active(&self) -> bool {
        self.inner.read().is_some()
    }

    // ===== Core Hooks =====

    /// Called when a mutation starts (before policy/storage).
    pub fn core_mutation_start(
        &self,
        correlation_id: &str,
        _method: &str,
        actor: &str,
        world_id: Option<&str>,
        payload: serde_json::Value,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Core,
                TraceKind::MutationStart,
                correlation_id,
                payload,
            )
            .with_agent_id(actor)
            .with_world_id(world_id.unwrap_or(""))
            .with_action_id(correlation_id)
        });
    }

    /// Called when a mutation commits successfully.
    pub fn core_mutation_commit(
        &self,
        correlation_id: &str,
        method: &str,
        actor: &str,
        world_id: Option<&str>,
        duration_ms: u64,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Core,
                TraceKind::MutationCommit,
                correlation_id,
                serde_json::json!({"method": method, "duration_ms": duration_ms}),
            )
            .with_agent_id(actor)
            .with_world_id(world_id.unwrap_or(""))
            .with_action_id(correlation_id)
        });
    }

    /// Called when a mutation rolls back (error).
    pub fn core_mutation_rollback(
        &self,
        correlation_id: &str,
        method: &str,
        actor: &str,
        world_id: Option<&str>,
        error: &str,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Core,
                TraceKind::MutationRollback,
                correlation_id,
                serde_json::json!({"method": method, "error": error}),
            )
            .with_agent_id(actor)
            .with_world_id(world_id.unwrap_or(""))
            .with_action_id(correlation_id)
        });
    }

    /// Called when Core publishes an Event to subscribers.
    pub fn core_event_published(
        &self,
        event_id: &str,
        correlation_id: Option<String>,
        event_type: &str,
        actor: &str,
        world_id: &str,
        provider_id: Option<String>,
    ) {
        let correlation_id_for_action = correlation_id.clone().unwrap_or_default();
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Core,
                TraceKind::EventPublished,
                correlation_id.unwrap_or_else(|| event_id.to_string()),
                serde_json::json!({"event_type": event_type, "event_id": event_id}),
            )
            .with_agent_id(actor)
            .with_world_id(world_id)
            .with_action_id(correlation_id_for_action)
            .with_provider_id(provider_id.unwrap_or_default())
        });
    }

    /// Called when an event is delivered to a subscriber.
    pub fn core_subscription_delivery(
        &self,
        subscription_id: &str,
        subscriber: &str,
        event_id: &str,
        correlation_id: Option<String>,
        _actor: &str,
        world_id: &str,
        latency_ms: u64,
    ) {
        let correlation_id_for_action = correlation_id.clone().unwrap_or_default();
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Core,
                TraceKind::SubscriptionDelivery,
                correlation_id.unwrap_or_else(|| event_id.to_string()),
                serde_json::json!({"subscription_id": subscription_id, "subscriber": subscriber, "latency_ms": latency_ms}),
            )
            .with_agent_id(subscriber)
            .with_world_id(world_id)
            .with_action_id(correlation_id_for_action)
        });
    }

    /// Called when Policy checks a capability.
    pub fn core_policy_check(
        &self,
        correlation_id: &str,
        actor: &str,
        capability: &str,
        allowed: bool,
        world_id: Option<&str>,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Core,
                TraceKind::PolicyCheck,
                correlation_id,
                serde_json::json!({"capability": capability, "allowed": allowed}),
            )
            .with_agent_id(actor)
            .with_world_id(world_id.unwrap_or(""))
            .with_action_id(correlation_id)
        });
    }

    // ===== IPC Hooks =====

    /// Called when IPC request is received.
    pub fn ipc_request_received(
        &self,
        correlation_id: &str,
        method: &str,
        actor: &str,
        world_id: Option<&str>,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::IPC,
                TraceKind::RequestReceived,
                correlation_id,
                serde_json::json!({"method": method}),
            )
            .with_agent_id(actor)
            .with_world_id(world_id.unwrap_or(""))
        });
    }

    /// Called when IPC request is dispatched to handler.
    pub fn ipc_request_dispatched(&self, correlation_id: &str, method: &str, actor: &str) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::IPC,
                TraceKind::RequestDispatched,
                correlation_id,
                serde_json::json!({"method": method}),
            )
            .with_agent_id(actor)
        });
    }

    /// Called when IPC response is sent.
    pub fn ipc_response_sent(
        &self,
        correlation_id: &str,
        method: &str,
        actor: &str,
        ok: bool,
        duration_ms: u64,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::IPC,
                TraceKind::ResponseSent,
                correlation_id,
                serde_json::json!({"method": method, "ok": ok, "duration_ms": duration_ms}),
            )
            .with_agent_id(actor)
        });
    }

    // ===== Storage Hooks =====

    /// Called when a Storage transaction begins.
    pub fn storage_transaction_begin(&self, correlation_id: &str, mutation_count: usize) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Storage,
                TraceKind::TransactionBegin,
                correlation_id,
                serde_json::json!({"mutation_count": mutation_count}),
            )
        });
    }

    /// Called when a Storage transaction commits.
    pub fn storage_transaction_commit(&self, correlation_id: &str, duration_ms: u64) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Storage,
                TraceKind::TransactionCommit,
                correlation_id,
                serde_json::json!({"duration_ms": duration_ms}),
            )
        });
    }

    /// Called when a Storage transaction rolls back.
    pub fn storage_transaction_rollback(&self, correlation_id: &str, error: &str) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Storage,
                TraceKind::TransactionRollback,
                correlation_id,
                serde_json::json!({"error": error}),
            )
        });
    }

    // ===== Provider Hooks =====

    /// Called when a Provider scan starts.
    pub fn provider_scan_start(&self, correlation_id: &str, provider_id: &str, path: &str) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Provider,
                TraceKind::ScanStart,
                correlation_id,
                serde_json::json!({"path": path}),
            )
            .with_provider_id(provider_id)
        });
    }

    /// Called when a Provider scan ends.
    pub fn provider_scan_end(
        &self,
        correlation_id: &str,
        provider_id: &str,
        stats: serde_json::Value,
        duration_ms: u64,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Provider,
                TraceKind::ScanEnd,
                correlation_id,
                serde_json::json!({"stats": stats, "duration_ms": duration_ms}),
            )
            .with_provider_id(provider_id)
        });
    }

    /// Called when a Watcher event is processed (create/update/delete/rename).
    pub fn provider_watcher_event(
        &self,
        correlation_id: &str,
        provider_id: &str,
        event_type: &str,
        path: &str,
        fingerprint: Option<&str>,
    ) {
        self.emit_if_active(|_inner| {
            let mut payload = serde_json::json!({"event_type": event_type, "path": path});
            if let Some(fp) = fingerprint {
                payload["fingerprint"] = serde_json::json!(fp);
            }
            TraceEvent::new(
                TraceLayer::Provider,
                TraceKind::WatcherEvent,
                correlation_id,
                payload,
            )
            .with_provider_id(provider_id)
        });
    }

    /// Called when a Provider Action is invoked.
    pub fn provider_action_invoke(
        &self,
        correlation_id: &str,
        provider_id: &str,
        action: &str,
        target: &str,
        actor: &str,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Provider,
                TraceKind::ActionInvoke,
                correlation_id,
                serde_json::json!({"action": action, "target": target}),
            )
            .with_provider_id(provider_id)
            .with_agent_id(actor)
            .with_action_id(correlation_id)
        });
    }

    /// Called when a Provider Action completes.
    pub fn provider_action_complete(
        &self,
        correlation_id: &str,
        provider_id: &str,
        action: &str,
        status: &str,
        duration_ms: u64,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Provider,
                TraceKind::ActionComplete,
                correlation_id,
                serde_json::json!({"action": action, "status": status, "duration_ms": duration_ms}),
            )
            .with_provider_id(provider_id)
            .with_action_id(correlation_id)
        });
    }

    // ===== Agent Hooks =====

    /// Called when an Agent registers.
    pub fn agent_register(
        &self,
        correlation_id: &str,
        agent_id: &str,
        runtime_type: Option<&str>,
        runtime_instance_id: Option<&str>,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Agent,
                TraceKind::AgentRegister,
                correlation_id,
                serde_json::json!({}),
            )
            .with_agent_id(agent_id)
            .with_runtime_type(runtime_type.unwrap_or("unknown"))
            .with_runtime_instance_id(runtime_instance_id.unwrap_or(""))
        });
    }

    /// Called when an Agent makes a decision (internal to runtime).
    pub fn agent_decision(
        &self,
        correlation_id: &str,
        agent_id: &str,
        agent_instance_id: &str,
        responsibility_id: Option<&str>,
        _decision: &str,
        payload: serde_json::Value,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Agent,
                TraceKind::AgentDecision,
                correlation_id,
                payload,
            )
            .with_agent_id(agent_id)
            .with_agent_instance_id(agent_instance_id)
            .with_responsibility_id(responsibility_id.unwrap_or(""))
        });
    }

    /// Called when an Agent invokes an Action.
    pub fn agent_action(
        &self,
        correlation_id: &str,
        agent_id: &str,
        agent_instance_id: &str,
        responsibility_id: Option<&str>,
        action_id: &str,
        action_type: &str,
        target: &str,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Agent,
                TraceKind::AgentAction,
                correlation_id,
                serde_json::json!({"action_type": action_type, "target": target}),
            )
            .with_agent_id(agent_id)
            .with_agent_instance_id(agent_instance_id)
            .with_responsibility_id(responsibility_id.unwrap_or(""))
            .with_action_id(action_id)
        });
    }

    /// Called when Responsibility state changes.
    pub fn agent_responsibility_change(
        &self,
        correlation_id: &str,
        agent_id: &str,
        responsibility_id: &str,
        state: &str,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Agent,
                TraceKind::ResponsibilityChange,
                correlation_id,
                serde_json::json!({"state": state}),
            )
            .with_agent_id(agent_id)
            .with_responsibility_id(responsibility_id)
        });
    }

    // ===== Runtime Hooks =====

    /// Called when an Agent Runtime starts.
    /// runtime_format: Task | Persistent | Universal (or custom). Optional;
    /// None = unknown. Format is a Runtime characteristic, NOT an Agent type.
    pub fn runtime_start(
        &self,
        correlation_id: &str,
        runtime_type: &str,
        runtime_instance_id: &str,
        agent_id: &str,
        runtime_format: Option<&str>,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Runtime,
                TraceKind::RuntimeStart,
                correlation_id,
                serde_json::json!({}),
            )
            .with_agent_id(agent_id)
            .with_runtime_type(runtime_type)
            .with_runtime_instance_id(runtime_instance_id)
            .with_runtime_format(runtime_format.unwrap_or("unknown"))
        });
    }

    /// Called when an Agent Runtime stops.
    pub fn runtime_stop(
        &self,
        correlation_id: &str,
        runtime_type: &str,
        runtime_instance_id: &str,
        agent_id: &str,
        runtime_format: Option<&str>,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Runtime,
                TraceKind::RuntimeStop,
                correlation_id,
                serde_json::json!({}),
            )
            .with_agent_id(agent_id)
            .with_runtime_type(runtime_type)
            .with_runtime_instance_id(runtime_instance_id)
            .with_runtime_format(runtime_format.unwrap_or("unknown"))
        });
    }

    /// Called when MCP protocol interaction occurs.
    pub fn runtime_mcp(
        &self,
        correlation_id: &str,
        runtime_instance_id: &str,
        agent_id: &str,
        tool: &str,
        provider: &str,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Runtime,
                TraceKind::ProtocolMCP,
                correlation_id,
                serde_json::json!({"tool": tool, "provider": provider}),
            )
            .with_agent_id(agent_id)
            .with_runtime_instance_id(runtime_instance_id)
            .with_protocol("MCP")
            .with_provider_id(provider)
        });
    }

    /// Called when A2A protocol interaction occurs.
    pub fn runtime_a2a(
        &self,
        correlation_id: &str,
        runtime_instance_id: &str,
        sender_agent_id: &str,
        receiver_agent_id: &str,
        delegation: &str,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Runtime,
                TraceKind::ProtocolA2A,
                correlation_id,
                serde_json::json!({"receiver_agent_id": receiver_agent_id, "delegation": delegation}),
            )
            .with_agent_id(sender_agent_id)
            .with_runtime_instance_id(runtime_instance_id)
            .with_protocol("A2A")
        });
    }

    // ===== View Hooks =====

    /// Called when a View polls for updates.
    pub fn view_poll(&self, correlation_id: &str, view_id: &str, world_id: &str, subscriber: &str) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::View,
                TraceKind::ViewPoll,
                correlation_id,
                serde_json::json!({"view_id": view_id, "subscriber": subscriber}),
            )
            .with_agent_id(subscriber)
            .with_world_id(world_id)
        });
    }

    /// Called when a View renders an update.
    pub fn view_render(
        &self,
        correlation_id: &str,
        view_id: &str,
        world_id: &str,
        latency_ms: u64,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::View,
                TraceKind::ViewRender,
                correlation_id,
                serde_json::json!({"view_id": view_id, "latency_ms": latency_ms}),
            )
            .with_world_id(world_id)
        });
    }

    // ===== Reality Hooks =====

    /// Called when Reality changes (fs notify, git push, etc.).
    pub fn reality_change(
        &self,
        correlation_id: &str,
        provider_id: &str,
        change_type: &str,
        path: &str,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                TraceLayer::Reality,
                TraceKind::RealityChange,
                correlation_id,
                serde_json::json!({"change_type": change_type, "path": path}),
            )
            .with_provider_id(provider_id)
        });
    }

    // ===== Error Hook =====

    /// Called when an error occurs in any layer.
    pub fn error(
        &self,
        correlation_id: &str,
        layer: TraceLayer,
        error: &str,
        context: serde_json::Value,
    ) {
        self.emit_if_active(|_inner| {
            TraceEvent::new(
                layer,
                TraceKind::Error,
                correlation_id,
                serde_json::json!({"error": error, "context": context}),
            )
        });
    }

    /// Internal: emit if hooks are active.
    fn emit_if_active(&self, f: impl FnOnce(&HookRegistryInner) -> TraceEvent) {
        let inner = self.inner.read();
        if let Some(inner) = inner.as_ref() {
            let event = f(inner);
            inner.correlation.index(&event);
            let _ = inner.buffer.push(event);
        }
    }
}

impl Default for HookRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Global hook registry instance.
static GLOBAL_HOOKS: LazyLock<HookRegistry> = LazyLock::new(|| HookRegistry::new());

/// Get the global hook registry.
pub fn global_hooks() -> &'static HookRegistry {
    &GLOBAL_HOOKS
}

/// Initialize global hooks with SpaceTrace components.
pub fn init_global_hooks(buffer: Arc<TraceBuffer>, correlation: Arc<CorrelationIndex>) {
    GLOBAL_HOOKS.initialize(buffer, correlation);
}

/// Check if global hooks are active.
pub fn hooks_active() -> bool {
    GLOBAL_HOOKS.is_active()
}
