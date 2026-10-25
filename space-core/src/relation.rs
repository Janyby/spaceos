use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relation {
    pub id: String,
    pub source_object_id: String,
    pub relation_type: String,
    pub target_object_id: String,
    pub properties: serde_json::Value,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub world_id: String,
}

impl Relation {
    pub fn new(
        source: impl Into<String>,
        relation_type: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            source_object_id: source.into(),
            relation_type: relation_type.into(),
            target_object_id: target.into(),
            properties: serde_json::json!({}),
            created_at: Utc::now(),
            world_id: String::new(),
        }
    }
}
