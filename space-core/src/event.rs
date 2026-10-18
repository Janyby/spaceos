use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    pub event_type: String,
    pub actor: String,
    pub provider_id: Option<String>,
    pub object_ids: Vec<String>,
    pub payload: serde_json::Value,
    pub timestamp: DateTime<Utc>,
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub world_id: String,
    /// Durable cursor assigned by SQLite (`events.seq`, starts at 1).
    /// `0` means "not yet persisted" (freshly constructed events).
    /// Additive field: old JSON without it still parses.
    #[serde(default)]
    pub seq: u64,
}

impl Event {
    pub fn new(
        event_type: impl Into<String>,
        actor: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            event_type: event_type.into(),
            actor: actor.into(),
            provider_id: None,
            object_ids: vec![],
            payload,
            timestamp: Utc::now(),
            correlation_id: None,
            world_id: String::new(),
            seq: 0,
        }
    }
}
