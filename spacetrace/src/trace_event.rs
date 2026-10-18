//! TraceEvent serialization and helpers.

use crate::{TraceEvent, TraceKind, TraceLayer};
use serde_json::Value;
use uuid::Uuid;

impl TraceEvent {
    /// Create a new trace event with generated trace_id and current timestamp.
    pub fn new(
        layer: TraceLayer,
        kind: TraceKind,
        correlation_id: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self {
            trace_id: Uuid::new_v4().to_string(),
            timestamp: chrono::Utc::now(),
            layer,
            kind,
            correlation_id: correlation_id.into(),
            world_id: None,
            agent_id: None,
            agent_instance_id: None,
            responsibility_id: None,
            action_id: None,
            provider_id: None,
            runtime_type: None,
            runtime_instance_id: None,
            runtime_format: None,
            protocol: None,
            payload,
        }
    }

    /// Builder-style setters for optional correlation fields.
    pub fn with_world_id(mut self, world_id: impl Into<String>) -> Self {
        self.world_id = Some(world_id.into());
        self
    }

    pub fn with_agent_id(mut self, agent_id: impl Into<String>) -> Self {
        self.agent_id = Some(agent_id.into());
        self
    }

    pub fn with_agent_instance_id(mut self, agent_instance_id: impl Into<String>) -> Self {
        self.agent_instance_id = Some(agent_instance_id.into());
        self
    }

    pub fn with_responsibility_id(mut self, responsibility_id: impl Into<String>) -> Self {
        self.responsibility_id = Some(responsibility_id.into());
        self
    }

    pub fn with_action_id(mut self, action_id: impl Into<String>) -> Self {
        self.action_id = Some(action_id.into());
        self
    }

    pub fn with_provider_id(mut self, provider_id: impl Into<String>) -> Self {
        self.provider_id = Some(provider_id.into());
        self
    }

    pub fn with_runtime_type(mut self, runtime_type: impl Into<String>) -> Self {
        self.runtime_type = Some(runtime_type.into());
        self
    }

    pub fn with_runtime_instance_id(mut self, runtime_instance_id: impl Into<String>) -> Self {
        self.runtime_instance_id = Some(runtime_instance_id.into());
        self
    }

    /// Runtime format: Task | Persistent | Universal (or custom).
    /// Characteristic of the Runtime, NOT a semantic Agent type.
    pub fn with_runtime_format(mut self, runtime_format: impl Into<String>) -> Self {
        self.runtime_format = Some(runtime_format.into());
        self
    }

    pub fn with_protocol(mut self, protocol: impl Into<String>) -> Self {
        self.protocol = Some(protocol.into());
        self
    }
}
