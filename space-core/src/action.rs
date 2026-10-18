use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ActionStatus {
    Requested,
    Started,
    Completed,
    Failed,
    Denied,
}

fn now_utc() -> DateTime<Utc> {
    Utc::now()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionRequest {
    pub id: String,
    pub action: String,
    pub target: serde_json::Value,
    pub provider_id: String,
    pub actor: String,
    /// Timestamp of the request. Defaults to now when the sender omits it
    /// (e.g. the HTTP bridge builds minimal action JSON); still always
    /// serialized so stored/traced requests keep the field.
    #[serde(default = "now_utc")]
    pub requested_at: DateTime<Utc>,
    /// Agent token binding this request to `actor`. Core principals
    /// ("system"/"filesystem"/"cli") authenticate with the core token
    /// carried in the request envelope instead.
    #[serde(default)]
    pub actor_token: String,
}

impl ActionRequest {
    pub fn new(
        action: impl Into<String>,
        target: serde_json::Value,
        provider_id: impl Into<String>,
        actor: impl Into<String>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            action: action.into(),
            target,
            provider_id: provider_id.into(),
            actor: actor.into(),
            requested_at: Utc::now(),
            actor_token: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionResult {
    pub action_id: String,
    pub status: ActionStatus,
    pub detail: serde_json::Value,
}
