use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Event, Object, Relation};

pub const SEMANTIC_ACTOR: &str = "space-world-user";
pub const SEMANTIC_OBJECT_CREATE_CAP: &str = "semantic.object.create";
pub const SEMANTIC_OBJECT_PATCH_CAP: &str = "semantic.object.patch";
pub const SEMANTIC_RELATION_CREATE_CAP: &str = "semantic.relation.create";
pub const USER_RELATION_TYPE: &str = "related_to";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSemanticObject {
    pub world_id: String,
    pub object_type: String,
    pub properties: serde_json::Value,
    pub correlation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchSemanticObject {
    pub object_id: String,
    pub world_id: String,
    pub properties_patch: serde_json::Value,
    pub correlation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSemanticRelation {
    pub world_id: String,
    pub source_id: String,
    pub target_id: String,
    pub relation_type: String,
    pub properties: serde_json::Value,
    pub correlation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticCommandEnvelope<T> {
    pub actor: String,
    pub actor_token: String,
    pub command: T,
    /// Transport-layer auth (added by send_request). Ignored by handler.
    #[serde(default)]
    pub auth: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticObjectResult {
    pub object: Object,
    pub event: Event,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticRelationResult {
    pub relation: Relation,
    pub event: Event,
}

/// Read and validate the fail-closed semantic World configuration.
/// World ids are plain Strings in the existing model (legacy empty
/// strings are unscoped; Space ids are UUIDs but world_id itself is
/// String). We enforce only non-empty + bounded length here — never
/// impose UUID validation where the existing contract is String.
pub fn semantic_world_id_from_env() -> Result<String, String> {
    match std::env::var("SPACEOS_SEMANTIC_WORLD_ID") {
        Ok(value) => validate_semantic_world_id(&value),
        Err(_) => Err(
            "semantic mutation API is unavailable: SPACEOS_SEMANTIC_WORLD_ID is not set"
                .to_string(),
        ),
    }
}

pub fn validate_semantic_world_id(value: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(
            "semantic mutation API is unavailable: SPACEOS_SEMANTIC_WORLD_ID is empty"
                .to_string(),
        );
    }
    if value.len() > 200 {
        return Err(
            "semantic mutation API is unavailable: SPACEOS_SEMANTIC_WORLD_ID exceeds 200 characters"
                .to_string(),
        );
    }
    Ok(value.to_string())
}

pub fn merge_properties(target: &mut serde_json::Value, patch: &serde_json::Value) {
    match patch {
        serde_json::Value::Object(patch_map) => {
            if !target.is_object() {
                *target = serde_json::json!({});
            }
            let target_map = target.as_object_mut().expect("object initialized above");
            for (key, value) in patch_map {
                if value.is_null() {
                    target_map.remove(key);
                } else {
                    merge_properties(
                        target_map
                            .entry(key.clone())
                            .or_insert(serde_json::Value::Null),
                        value,
                    );
                }
            }
        }
        value => *target = value.clone(),
    }
}

pub fn validate_properties(value: &serde_json::Value, field: &str) -> anyhow::Result<()> {
    let map = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("{field} must be a JSON object"))?;
    if map.contains_key("world_id") {
        anyhow::bail!("{field}.world_id is managed by Core and cannot be supplied");
    }
    Ok(())
}

pub fn validate_correlation_id(value: &str) -> anyhow::Result<()> {
    if value.trim().is_empty() || value.len() > 200 {
        anyhow::bail!("correlation_id must contain 1..=200 characters");
    }
    Ok(())
}
