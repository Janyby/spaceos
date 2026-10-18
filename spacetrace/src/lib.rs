//! SpaceTrace — Observability subsystem for SpaceOS.
//!
//! SpaceTrace observes SpaceOS but is NOT the owner of SpaceOS state.
//!
//! Core invariants (per spec §35):
//! - SpaceTrace ≠ Core
//! - SpaceTrace failure ≠ Core failure
//! - World state remains authoritative in Core
//! - Provider boundaries remain intact
//! - Semantic Action ≠ Storage Transaction
//! - Core does not depend on SpaceTrace

pub mod buffer;
pub mod correlation;
#[cfg(windows)]
pub mod etw;
pub mod hooks;
pub mod provider;
pub mod trace_event;
pub mod verdict;

// Re-export commonly used types
pub use buffer::{BufferConfig, BufferStats, TraceBuffer};
pub use correlation::{CorrelationIndex, CorrelationStats};
pub use hooks::{HookRegistry, global_hooks, hooks_active, init_global_hooks};
pub use provider::{SpaceTraceProvider, ipc as provider_ipc};
pub use verdict::{
    GapFlags, OsEvidence, ProcessInstance, SemanticEvidence, Verdict, VerdictStatus, classify,
    verdict_event,
};
// TraceEvent is defined in this crate root, not in trace_event module

use anyhow::Result;
// SpaceTrace does not depend on space_core to avoid cyclic dependency.
// Hooks use serde_json::Value and string identifiers instead of concrete space_core types.

/// Main SpaceTrace handle. Cheap to clone (Arc internally).
#[derive(Clone)]
pub struct SpaceTrace {
    inner: std::sync::Arc<Inner>,
}

struct Inner {
    buffer: std::sync::Arc<buffer::TraceBuffer>,
    correlation: std::sync::Arc<correlation::CorrelationIndex>,
    hooks: hooks::HookRegistry,
    provider: provider::SpaceTraceProvider,
}

impl SpaceTrace {
    /// Create a new SpaceTrace instance.
    pub fn new() -> Result<Self> {
        let buffer = std::sync::Arc::new(buffer::TraceBuffer::new()?);
        let correlation = std::sync::Arc::new(correlation::CorrelationIndex::new());
        let hooks = hooks::HookRegistry::new();
        let provider =
            provider::SpaceTraceProvider::with_components(buffer.clone(), correlation.clone());

        Ok(Self {
            inner: std::sync::Arc::new(Inner {
                buffer,
                correlation,
                hooks,
                provider,
            }),
        })
    }

    /// Get the trace buffer for writing trace events.
    pub fn buffer(&self) -> std::sync::Arc<buffer::TraceBuffer> {
        self.inner.buffer.clone()
    }

    /// Get the correlation index for queries.
    pub fn correlation(&self) -> std::sync::Arc<correlation::CorrelationIndex> {
        self.inner.correlation.clone()
    }

    /// Get the hook registry for Core/Provider integration.
    pub fn hooks(&self) -> &hooks::HookRegistry {
        &self.inner.hooks
    }

    /// Get the SpaceTrace Provider capability interface.
    pub fn provider(&self) -> &provider::SpaceTraceProvider {
        &self.inner.provider
    }
}

impl Default for SpaceTrace {
    fn default() -> Self {
        Self::new().expect("SpaceTrace initialization failed")
    }
}

/// High-level trace event emitted by SpaceTrace hooks.
///
/// NOT a semantic Core Event. High-frequency, optional, linked via correlation_id.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TraceEvent {
    pub trace_id: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub layer: TraceLayer,
    pub kind: TraceKind,
    pub correlation_id: String,
    pub world_id: Option<String>,
    pub agent_id: Option<String>,
    pub agent_instance_id: Option<String>,
    pub responsibility_id: Option<String>,
    pub action_id: Option<String>,
    pub provider_id: Option<String>,
    pub runtime_type: Option<String>,
    pub runtime_instance_id: Option<String>,
    /// Runtime format: Task | Persistent | Universal (or custom).
    /// Characteristic of the Runtime, NOT a semantic Agent type (gate §3).
    pub runtime_format: Option<String>,
    pub protocol: Option<String>,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TraceLayer {
    Core,
    IPC,
    Storage,
    Provider,
    Agent,
    Runtime,
    View,
    Reality,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TraceKind {
    // Core
    MutationStart,
    MutationCommit,
    MutationRollback,
    EventPublished,
    SubscriptionDelivery,
    PolicyCheck,
    // IPC
    RequestReceived,
    RequestDispatched,
    ResponseSent,
    // Storage
    TransactionBegin,
    TransactionCommit,
    TransactionRollback,
    // Provider
    ScanStart,
    ScanEnd,
    WatcherEvent,
    ActionInvoke,
    ActionComplete,
    // Agent
    AgentRegister,
    AgentDecision,
    AgentAction,
    ResponsibilityChange,
    // Runtime
    RuntimeStart,
    RuntimeStop,
    ProtocolMCP,
    ProtocolA2A,
    // View
    ViewPoll,
    ViewRender,
    // Reality
    RealityChange,
    // Correlation
    CorrelationLink,
    // Error
    Error,
}
