use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Object;

pub const DEFAULT_CONTEXT_RELATION_LIMIT: usize = 50;
pub const MAX_CONTEXT_RELATION_LIMIT: usize = 100;
const MAX_CANONICAL_ID_LEN: usize = 512;
const MAX_WORLD_ID_LEN: usize = 200;

/// Authenticated read envelope. The transport authentication value is injected
/// by the Core IPC client; agent identity remains bound by `actor_token`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticReadEnvelope<T> {
    pub actor: String,
    #[serde(default)]
    pub actor_token: String,
    /// Privileged Core-principal proof, independent of transport auth.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_token: Option<String>,
    pub query: T,
    /// Transport-layer authentication injected by `send_request`.
    #[serde(default)]
    pub auth: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectContextQuery {
    pub object_id: String,
    pub world_id: String,
    #[serde(default)]
    pub limit: Option<usize>,
}

impl ObjectContextQuery {
    pub fn validate(&self) -> anyhow::Result<usize> {
        validate_bounded_nonempty(&self.object_id, "object_id", MAX_CANONICAL_ID_LEN)?;
        validate_bounded_nonempty(&self.world_id, "world_id", MAX_WORLD_ID_LEN)?;
        let limit = self.limit.unwrap_or(DEFAULT_CONTEXT_RELATION_LIMIT);
        if !(1..=MAX_CONTEXT_RELATION_LIMIT).contains(&limit) {
            anyhow::bail!("limit must contain 1..={MAX_CONTEXT_RELATION_LIMIT} relations");
        }
        Ok(limit)
    }
}

fn validate_bounded_nonempty(value: &str, field: &str, max: usize) -> anyhow::Result<()> {
    if value.trim().is_empty() || value.len() > max {
        anyhow::bail!("{field} must contain 1..={max} bytes");
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RelationDirection {
    Incoming,
    Outgoing,
    SelfRelation,
}

/// Deliberately narrow object identity for context responses. Arbitrary object
/// properties and provider external ids are not copied into neighbor summaries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectSummary {
    pub id: String,
    pub r#type: String,
    pub provider_id: String,
    pub world_id: String,
}

impl ObjectSummary {
    pub(crate) fn from_object(object: &Object, world_id: &str) -> Self {
        Self {
            id: object.id.clone(),
            r#type: object.r#type.clone(),
            provider_id: object.provider_id.clone(),
            world_id: world_id.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextRelation {
    pub id: String,
    pub source_object_id: String,
    pub relation_type: String,
    pub target_object_id: String,
    pub created_at: DateTime<Utc>,
    pub world_id: String,
    pub direction: RelationDirection,
    /// `None` is possible only for a grandfathered legacy orphan. New writes
    /// remain protected by SQLite foreign keys.
    pub neighbor: Option<ObjectSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectContext {
    pub object: ObjectSummary,
    pub relations: Vec<ContextRelation>,
    pub applied_limit: usize,
    pub truncated: bool,
    /// Highest committed event sequence visible in the same SQLite read
    /// transaction. This is a read watermark, not a global snapshot id and not
    /// a claim that every legacy/provider state change is event-sourced.
    pub event_cursor: u64,
    pub world_id: String,
}
