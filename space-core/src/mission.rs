//! Specific scoped Mission commands. Execution is deliberately not claimed here.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::{Event, Object};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionModel { pub provider_id:String, pub model_id:String }

impl ExecutionModel {
    pub fn validate(&self)->anyhow::Result<()> {
        for value in [&self.provider_id,&self.model_id] {
            require(!value.trim().is_empty() && value.len()<=240 && !value.chars().any(char::is_control),"validation_error","invalid execution model identifier")?;
        }
        Ok(())
    }
}

/// Server-side execution reporting, deliberately not a browser MissionOperation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MissionExecution {
    pub world_id: String, pub mission_id: String, pub attempt_id: String,
    pub agent_id: String, pub opencode_session_id: String,
    pub status: String, pub admitted: u32, pub correlation_id: String,
    #[serde(default)] pub result: String,
    #[serde(default)] pub model: Option<ExecutionModel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MissionCommand {
    pub world_id: String,
    pub command_id: String,
    pub operation: MissionOperation,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum MissionOperation {
    Create { goal: String, workspace: String, assignee: String },
    Plan { mission_id: String, expected_state: String, steps: Vec<StepSpec> },
    Transition { mission_id: String, expected_state: String, action: String,
        #[serde(default)] reason: Option<String>, #[serde(default)] result: Option<String> },
    Step { mission_id: String, expected_state: String, step_id: String,
        expected_step_state: String, to: String, #[serde(default)] evidence: Value,
        #[serde(default)] result: Value, #[serde(default)] action_ids: Vec<String> },
    Verify { mission_id: String, expected_state: String, #[serde(default)] expected_attempt_id: Option<String>, passed: bool, method: String, detail: Value },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepSpec { pub title: String, pub detail: String, pub requires_approval: bool }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MissionResult { pub mission: Object, pub event: Event, pub applied: bool, pub command_id: String }

#[derive(Debug)]
pub struct MissionError { pub code: &'static str, pub message: String }
impl std::fmt::Display for MissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.message) }
}
impl std::error::Error for MissionError {}
pub fn denied(code: &'static str, message: &str) -> anyhow::Error {
    MissionError { code, message: message.into() }.into()
}
pub fn require(ok: bool, code: &'static str, message: &str) -> anyhow::Result<()> {
    if ok { Ok(()) } else { Err(denied(code, message)) }
}
pub fn text(value: &str, name: &str, max: usize) -> anyhow::Result<()> {
    require(!value.trim().is_empty() && value.len() <= max, "validation_error", &format!("invalid {name}"))
}
impl MissionOperation {
    pub fn target(&self) -> Option<(&str, &str)> {
        match self {
            Self::Create { .. } => None,
            Self::Plan {mission_id, expected_state,..} | Self::Transition {mission_id,expected_state,..}
            | Self::Step {mission_id,expected_state,..} | Self::Verify {mission_id,expected_state,..}
                => Some((mission_id, expected_state)),
        }
    }
}
