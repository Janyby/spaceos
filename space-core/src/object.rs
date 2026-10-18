use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Object {
    pub id: String,
    pub r#type: String,
    pub properties: serde_json::Value,
    pub provider_id: String,
    pub external_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Object {
    pub fn new(
        r#type: impl Into<String>,
        provider_id: impl Into<String>,
        properties: serde_json::Value,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            r#type: r#type.into(),
            properties,
            provider_id: provider_id.into(),
            external_id: None,
            created_at: now,
            updated_at: now,
        }
    }
}
