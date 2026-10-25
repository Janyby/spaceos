//! SpaceTrace Provider — exposes trace capabilities via Provider API.
//!
//! Capabilities:
//! - trace.observe
//! - trace.query
//! - trace.correlate
//! - trace.metrics

use crate::{BufferStats, CorrelationIndex, CorrelationStats, TraceBuffer, TraceEvent};
use serde_json::{Value, json};
use std::sync::Arc;

/// SpaceTrace Provider capability interface.
pub struct SpaceTraceProvider {
    buffer: Arc<TraceBuffer>,
    correlation: Arc<CorrelationIndex>,
}

impl SpaceTraceProvider {
    pub fn new() -> Self {
        // Placeholder — real instance created by SpaceTrace::new()
        Self {
            buffer: Arc::new(TraceBuffer::new().unwrap()),
            correlation: Arc::new(CorrelationIndex::new()),
        }
    }

    pub fn with_components(buffer: Arc<TraceBuffer>, correlation: Arc<CorrelationIndex>) -> Self {
        Self {
            buffer,
            correlation,
        }
    }

    /// Get trace buffer stats.
    pub fn buffer_stats(&self) -> BufferStats {
        self.buffer.stats()
    }

    /// Get correlation index stats.
    pub fn correlation_stats(&self) -> CorrelationStats {
        self.correlation.stats()
    }

    /// Query trace events by correlation_id.
    pub fn query_by_correlation(&self, correlation_id: &str) -> Vec<TraceEvent> {
        self.correlation
            .by_correlation(correlation_id)
            .into_iter()
            .filter_map(|tid| self.correlation.get_event(&tid))
            .collect()
    }

    /// Query trace events by world_id.
    pub fn query_by_world(&self, world_id: &str) -> Vec<TraceEvent> {
        self.correlation
            .by_world(world_id)
            .into_iter()
            .filter_map(|tid| self.correlation.get_event(&tid))
            .collect()
    }

    /// Query trace events by agent_id.
    pub fn query_by_agent(&self, agent_id: &str) -> Vec<TraceEvent> {
        self.correlation
            .by_agent(agent_id)
            .into_iter()
            .filter_map(|tid| self.correlation.get_event(&tid))
            .collect()
    }

    /// Query trace events by agent_instance_id.
    pub fn query_by_agent_instance(&self, agent_instance_id: &str) -> Vec<TraceEvent> {
        self.correlation
            .by_agent_instance(agent_instance_id)
            .into_iter()
            .filter_map(|tid| self.correlation.get_event(&tid))
            .collect()
    }

    /// Query trace events by responsibility_id.
    pub fn query_by_responsibility(&self, responsibility_id: &str) -> Vec<TraceEvent> {
        self.correlation
            .by_responsibility(responsibility_id)
            .into_iter()
            .filter_map(|tid| self.correlation.get_event(&tid))
            .collect()
    }

    /// Query trace events by action_id.
    pub fn query_by_action(&self, action_id: &str) -> Vec<TraceEvent> {
        self.correlation
            .by_action(action_id)
            .into_iter()
            .filter_map(|tid| self.correlation.get_event(&tid))
            .collect()
    }

    /// Query trace events by provider_id.
    pub fn query_by_provider(&self, provider_id: &str) -> Vec<TraceEvent> {
        self.correlation
            .by_provider(provider_id)
            .into_iter()
            .filter_map(|tid| self.correlation.get_event(&tid))
            .collect()
    }

    /// Query trace events by runtime_type.
    pub fn query_by_runtime_type(&self, runtime_type: &str) -> Vec<TraceEvent> {
        self.correlation
            .by_runtime_type(runtime_type)
            .into_iter()
            .filter_map(|tid| self.correlation.get_event(&tid))
            .collect()
    }

    /// Query trace events by runtime_instance_id.
    pub fn query_by_runtime_instance(&self, runtime_instance_id: &str) -> Vec<TraceEvent> {
        self.correlation
            .by_runtime_instance(runtime_instance_id)
            .into_iter()
            .filter_map(|tid| self.correlation.get_event(&tid))
            .collect()
    }

    /// Query trace events by runtime_format (Task | Persistent | Universal | custom).
    pub fn query_by_runtime_format(&self, runtime_format: &str) -> Vec<TraceEvent> {
        self.correlation
            .by_runtime_format(runtime_format)
            .into_iter()
            .filter_map(|tid| self.correlation.get_event(&tid))
            .collect()
    }

    /// Query trace events by protocol.
    pub fn query_by_protocol(&self, protocol: &str) -> Vec<TraceEvent> {
        self.correlation
            .by_protocol(protocol)
            .into_iter()
            .filter_map(|tid| self.correlation.get_event(&tid))
            .collect()
    }

    /// Get all correlations in a world.
    pub fn correlations_in_world(&self, world_id: &str) -> Vec<String> {
        self.correlation.correlations_in_world(world_id)
    }

    /// Get all agents in a world.
    pub fn agents_in_world(&self, world_id: &str) -> Vec<String> {
        self.correlation.agents_in_world(world_id)
    }

    /// Get trace event by trace_id.
    pub fn get_event(&self, trace_id: &str) -> Option<TraceEvent> {
        self.correlation.get_event(trace_id)
    }

    /// Get all stats as JSON.
    pub fn all_stats(&self) -> Value {
        json!({
            "buffer": self.buffer_stats(),
            "correlation": self.correlation_stats(),
        })
    }

    /// Provider capability list.
    pub fn capabilities() -> Vec<&'static str> {
        vec![
            "trace.observe",
            "trace.query",
            "trace.correlate",
            "trace.metrics",
        ]
    }
}

impl Default for SpaceTraceProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// Provider IPC handler for SpaceTrace.
pub mod ipc {
    use super::SpaceTraceProvider;
    use serde_json::{Value, json};

    /// Handle SpaceTrace Provider IPC methods. Synchronous: in-memory
    /// index lookups only, never blocks on IO (Send-safe for Core tasks).
    pub fn handle(provider: &SpaceTraceProvider, method: &str, params: Value) -> Value {
        match method {
            "stats" => json!({"ok": true, "result": provider.all_stats()}),
            "query" => {
                let correlation_id = params.get("correlation_id").and_then(|v| v.as_str());
                let world_id = params.get("world_id").and_then(|v| v.as_str());
                let agent_id = params.get("agent_id").and_then(|v| v.as_str());
                let agent_instance_id = params.get("agent_instance_id").and_then(|v| v.as_str());
                let responsibility_id = params.get("responsibility_id").and_then(|v| v.as_str());
                let action_id = params.get("action_id").and_then(|v| v.as_str());
                let provider_id = params.get("provider_id").and_then(|v| v.as_str());
                let runtime_type = params.get("runtime_type").and_then(|v| v.as_str());
                let runtime_instance_id =
                    params.get("runtime_instance_id").and_then(|v| v.as_str());
                let runtime_format = params.get("runtime_format").and_then(|v| v.as_str());
                let protocol = params.get("protocol").and_then(|v| v.as_str());
                let trace_id = params.get("trace_id").and_then(|v| v.as_str());

                let events = if let Some(id) = correlation_id {
                    provider.query_by_correlation(id)
                } else if let Some(id) = world_id {
                    provider.query_by_world(id)
                } else if let Some(id) = agent_id {
                    provider.query_by_agent(id)
                } else if let Some(id) = agent_instance_id {
                    provider.query_by_agent_instance(id)
                } else if let Some(id) = responsibility_id {
                    provider.query_by_responsibility(id)
                } else if let Some(id) = action_id {
                    provider.query_by_action(id)
                } else if let Some(id) = provider_id {
                    provider.query_by_provider(id)
                } else if let Some(id) = runtime_type {
                    provider.query_by_runtime_type(id)
                } else if let Some(id) = runtime_instance_id {
                    provider.query_by_runtime_instance(id)
                } else if let Some(id) = runtime_format {
                    provider.query_by_runtime_format(id)
                } else if let Some(id) = protocol {
                    provider.query_by_protocol(id)
                } else if let Some(id) = trace_id {
                    provider.get_event(id).into_iter().collect()
                } else {
                    vec![]
                };

                json!({"ok": true, "result": events})
            }
            "correlations_in_world" => {
                let world_id = params
                    .get("world_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                json!({"ok": true, "result": provider.correlations_in_world(world_id)})
            }
            "agents_in_world" => {
                let world_id = params
                    .get("world_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                json!({"ok": true, "result": provider.agents_in_world(world_id)})
            }
            "capabilities" => json!({"ok": true, "result": SpaceTraceProvider::capabilities()}),
            other => json!({"ok": false, "error": format!("unknown method: {other}")}),
        }
    }
}
