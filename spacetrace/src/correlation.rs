//! Correlation index — query trace events by semantic identifiers.
//!
//! Separate from Core's correlation_id; maintains additional indices
//! for Agent, Responsibility, Action, Runtime, Provider, World.

use crate::TraceEvent;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};

/// Multi-dimensional correlation index for trace queries.
pub struct CorrelationIndex {
    // Primary: correlation_id -> trace_ids
    by_correlation: RwLock<HashMap<String, HashSet<String>>>,
    // Secondary indices
    by_world: RwLock<HashMap<String, HashSet<String>>>,
    by_agent: RwLock<HashMap<String, HashSet<String>>>,
    by_agent_instance: RwLock<HashMap<String, HashSet<String>>>,
    by_responsibility: RwLock<HashMap<String, HashSet<String>>>,
    by_action: RwLock<HashMap<String, HashSet<String>>>,
    by_provider: RwLock<HashMap<String, HashSet<String>>>,
    by_runtime_type: RwLock<HashMap<String, HashSet<String>>>,
    by_runtime_instance: RwLock<HashMap<String, HashSet<String>>>,
    by_runtime_format: RwLock<HashMap<String, HashSet<String>>>,
    by_protocol: RwLock<HashMap<String, HashSet<String>>>,
    by_trace_id: RwLock<HashMap<String, TraceEvent>>, // for direct lookup
}

impl CorrelationIndex {
    pub fn new() -> Self {
        Self {
            by_correlation: RwLock::new(HashMap::new()),
            by_world: RwLock::new(HashMap::new()),
            by_agent: RwLock::new(HashMap::new()),
            by_agent_instance: RwLock::new(HashMap::new()),
            by_responsibility: RwLock::new(HashMap::new()),
            by_action: RwLock::new(HashMap::new()),
            by_provider: RwLock::new(HashMap::new()),
            by_runtime_type: RwLock::new(HashMap::new()),
            by_runtime_instance: RwLock::new(HashMap::new()),
            by_runtime_format: RwLock::new(HashMap::new()),
            by_protocol: RwLock::new(HashMap::new()),
            by_trace_id: RwLock::new(HashMap::new()),
        }
    }

    /// Index a trace event across all dimensions.
    pub fn index(&self, event: &TraceEvent) {
        let trace_id = &event.trace_id;

        // Primary
        self.by_correlation
            .write()
            .entry(event.correlation_id.clone())
            .or_default()
            .insert(trace_id.clone());

        // Secondaries
        if let Some(w) = &event.world_id {
            self.by_world
                .write()
                .entry(w.clone())
                .or_default()
                .insert(trace_id.clone());
        }
        if let Some(a) = &event.agent_id {
            self.by_agent
                .write()
                .entry(a.clone())
                .or_default()
                .insert(trace_id.clone());
        }
        if let Some(ai) = &event.agent_instance_id {
            self.by_agent_instance
                .write()
                .entry(ai.clone())
                .or_default()
                .insert(trace_id.clone());
        }
        if let Some(r) = &event.responsibility_id {
            self.by_responsibility
                .write()
                .entry(r.clone())
                .or_default()
                .insert(trace_id.clone());
        }
        if let Some(a) = &event.action_id {
            self.by_action
                .write()
                .entry(a.clone())
                .or_default()
                .insert(trace_id.clone());
        }
        if let Some(p) = &event.provider_id {
            self.by_provider
                .write()
                .entry(p.clone())
                .or_default()
                .insert(trace_id.clone());
        }
        if let Some(rt) = &event.runtime_type {
            self.by_runtime_type
                .write()
                .entry(rt.clone())
                .or_default()
                .insert(trace_id.clone());
        }
        if let Some(ri) = &event.runtime_instance_id {
            self.by_runtime_instance
                .write()
                .entry(ri.clone())
                .or_default()
                .insert(trace_id.clone());
        }
        if let Some(rf) = &event.runtime_format {
            self.by_runtime_format
                .write()
                .entry(rf.clone())
                .or_default()
                .insert(trace_id.clone());
        }
        if let Some(p) = &event.protocol {
            self.by_protocol
                .write()
                .entry(p.clone())
                .or_default()
                .insert(trace_id.clone());
        }

        // Direct lookup
        self.by_trace_id
            .write()
            .insert(trace_id.clone(), event.clone());
    }

    /// Remove a trace event from all indices (for buffer drain/retention).
    pub fn remove(&self, event: &TraceEvent) {
        let trace_id = &event.trace_id;

        self.by_correlation
            .write()
            .entry(event.correlation_id.clone())
            .or_default()
            .remove(trace_id);

        if let Some(w) = &event.world_id {
            self.by_world
                .write()
                .entry(w.clone())
                .or_default()
                .remove(trace_id);
        }
        if let Some(a) = &event.agent_id {
            self.by_agent
                .write()
                .entry(a.clone())
                .or_default()
                .remove(trace_id);
        }
        if let Some(ai) = &event.agent_instance_id {
            self.by_agent_instance
                .write()
                .entry(ai.clone())
                .or_default()
                .remove(trace_id);
        }
        if let Some(r) = &event.responsibility_id {
            self.by_responsibility
                .write()
                .entry(r.clone())
                .or_default()
                .remove(trace_id);
        }
        if let Some(a) = &event.action_id {
            self.by_action
                .write()
                .entry(a.clone())
                .or_default()
                .remove(trace_id);
        }
        if let Some(p) = &event.provider_id {
            self.by_provider
                .write()
                .entry(p.clone())
                .or_default()
                .remove(trace_id);
        }
        if let Some(rt) = &event.runtime_type {
            self.by_runtime_type
                .write()
                .entry(rt.clone())
                .or_default()
                .remove(trace_id);
        }
        if let Some(ri) = &event.runtime_instance_id {
            self.by_runtime_instance
                .write()
                .entry(ri.clone())
                .or_default()
                .remove(trace_id);
        }
        if let Some(rf) = &event.runtime_format {
            self.by_runtime_format
                .write()
                .entry(rf.clone())
                .or_default()
                .remove(trace_id);
        }
        if let Some(p) = &event.protocol {
            self.by_protocol
                .write()
                .entry(p.clone())
                .or_default()
                .remove(trace_id);
        }

        self.by_trace_id.write().remove(trace_id);
    }

    /// Get trace IDs by correlation_id.
    pub fn by_correlation(&self, correlation_id: &str) -> Vec<String> {
        self.by_correlation
            .read()
            .get(correlation_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get trace IDs by world_id.
    pub fn by_world(&self, world_id: &str) -> Vec<String> {
        self.by_world
            .read()
            .get(world_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get trace IDs by agent_id.
    pub fn by_agent(&self, agent_id: &str) -> Vec<String> {
        self.by_agent
            .read()
            .get(agent_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get trace IDs by agent_instance_id.
    pub fn by_agent_instance(&self, agent_instance_id: &str) -> Vec<String> {
        self.by_agent_instance
            .read()
            .get(agent_instance_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get trace IDs by responsibility_id.
    pub fn by_responsibility(&self, responsibility_id: &str) -> Vec<String> {
        self.by_responsibility
            .read()
            .get(responsibility_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get trace IDs by action_id.
    pub fn by_action(&self, action_id: &str) -> Vec<String> {
        self.by_action
            .read()
            .get(action_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get trace IDs by provider_id.
    pub fn by_provider(&self, provider_id: &str) -> Vec<String> {
        self.by_provider
            .read()
            .get(provider_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get trace IDs by runtime_type.
    pub fn by_runtime_type(&self, runtime_type: &str) -> Vec<String> {
        self.by_runtime_type
            .read()
            .get(runtime_type)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get trace IDs by runtime_instance_id.
    pub fn by_runtime_instance(&self, runtime_instance_id: &str) -> Vec<String> {
        self.by_runtime_instance
            .read()
            .get(runtime_instance_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get trace IDs by runtime_format (Task | Persistent | Universal | custom).
    pub fn by_runtime_format(&self, runtime_format: &str) -> Vec<String> {
        self.by_runtime_format
            .read()
            .get(runtime_format)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get trace IDs by protocol.
    pub fn by_protocol(&self, protocol: &str) -> Vec<String> {
        self.by_protocol
            .read()
            .get(protocol)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get full TraceEvent by trace_id.
    pub fn get_event(&self, trace_id: &str) -> Option<TraceEvent> {
        self.by_trace_id.read().get(trace_id).cloned()
    }

    /// Get all unique correlation_ids for a world.
    pub fn correlations_in_world(&self, world_id: &str) -> Vec<String> {
        let trace_ids = self.by_world(world_id);
        let mut corr = HashSet::new();
        for tid in trace_ids {
            if let Some(evt) = self.by_trace_id.read().get(&tid) {
                corr.insert(evt.correlation_id.clone());
            }
        }
        corr.into_iter().collect()
    }

    /// Get all unique agents for a world.
    pub fn agents_in_world(&self, world_id: &str) -> Vec<String> {
        let trace_ids = self.by_world(world_id);
        let mut agents = HashSet::new();
        let by_trace = self.by_trace_id.read();
        for tid in trace_ids {
            if let Some(evt) = by_trace.get(&tid) {
                if let Some(a) = &evt.agent_id {
                    agents.insert(a.clone());
                }
            }
        }
        agents.into_iter().collect()
    }

    /// Stats for monitoring.
    pub fn stats(&self) -> CorrelationStats {
        CorrelationStats {
            unique_correlations: self.by_correlation.read().len(),
            unique_worlds: self.by_world.read().len(),
            unique_agents: self.by_agent.read().len(),
            unique_agent_instances: self.by_agent_instance.read().len(),
            unique_responsibilities: self.by_responsibility.read().len(),
            unique_actions: self.by_action.read().len(),
            unique_providers: self.by_provider.read().len(),
            unique_runtime_types: self.by_runtime_type.read().len(),
            unique_runtime_instances: self.by_runtime_instance.read().len(),
            unique_runtime_formats: self.by_runtime_format.read().len(),
            unique_protocols: self.by_protocol.read().len(),
            total_events: self.by_trace_id.read().len(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CorrelationStats {
    pub unique_correlations: usize,
    pub unique_worlds: usize,
    pub unique_agents: usize,
    pub unique_agent_instances: usize,
    pub unique_responsibilities: usize,
    pub unique_actions: usize,
    pub unique_providers: usize,
    pub unique_runtime_types: usize,
    pub unique_runtime_instances: usize,
    pub unique_runtime_formats: usize,
    pub unique_protocols: usize,
    pub total_events: usize,
}

impl Default for CorrelationIndex {
    fn default() -> Self {
        Self::new()
    }
}
