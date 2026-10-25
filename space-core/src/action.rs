use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Public scoped request: never accepts a raw CDP target or caller authority.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorldRequest {
    pub scope_version: u8,
    pub world_id: String,
    #[serde(default)]
    pub object_id: Option<String>,
    #[serde(default)]
    pub options: BrowserShotOptions,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserShotOptions {
    #[serde(default = "jpeg")]
    pub format: String,
    #[serde(default = "quality")]
    pub quality: u8,
}
fn jpeg() -> String { "jpeg".into() }
fn quality() -> u8 { 62 }
impl Default for BrowserShotOptions {
    fn default() -> Self { Self { format: jpeg(), quality: quality() } }
}
impl BrowserShotOptions {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.format == "jpeg" && (1..=100).contains(&self.quality), "unsupported screenshot options");
        Ok(())
    }
}
/// Core-created target. Covered in full by the existing dispatch MAC.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserWorldTarget {
    pub scope_version: u8,
    pub instance_id: String,
    pub runtime: crate::provider_observation::BrowserRuntimeConfig,
    pub object: Option<crate::Object>,
    pub options: BrowserShotOptions,
}

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
    /// ("system"/"filesystem"/"cli") authenticate with their separate
    /// principal_token; the transport credential is not principal identity.
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

/// Internal v2 operation. Caller credentials deliberately have no field here.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispatchRequest {
    pub id: String,
    pub correlation_id: String,
    pub action: String,
    pub target: serde_json::Value,
    pub provider_id: String,
    pub actor: String,
    pub requested_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion: Option<CompletionBinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CompletionBinding {
    pub binding_id: String,
    pub instance_id: String,
    pub source_root: String,
    pub world_id: String,
    pub source_digest: String,
}

impl From<&ActionRequest> for DispatchRequest {
    fn from(r: &ActionRequest) -> Self {
        Self { id:r.id.clone(), correlation_id:r.id.clone(), action:r.action.clone(), target:r.target.clone(),
            provider_id:r.provider_id.clone(), actor:r.actor.clone(), requested_at:r.requested_at,
            completion:None }
    }
}
impl From<&DispatchRequest> for DispatchRequest {
    fn from(r: &DispatchRequest) -> Self { r.clone() }
}
impl DispatchRequest {
    pub fn from_provider_params(params: &serde_json::Value) -> Result<Self, serde_json::Error> {
        let mut value = params.clone();
        if let Some(map) = value.as_object_mut() { map.remove("auth"); map.remove("dispatch_proof"); }
        serde_json::from_value(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionCompletion {
    pub action_id: String,
    pub binding_id: String,
    pub performer: String,
    pub outcome_id: String,
    pub status: ActionStatus,
    pub detail: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionResult {
    pub action_id: String,
    pub status: ActionStatus,
    pub detail: serde_json::Value,
}
