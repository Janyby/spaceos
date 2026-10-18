pub mod actions;
pub mod browser;
pub mod format;
pub mod identity;
pub mod ipc;
pub mod metadata;
pub mod observation;
pub mod performer;
pub mod roots;
pub mod scanner;
pub mod watcher;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatInfo {
    pub physical_type: String,
    pub format: String,
    pub semantic_type: String,
    pub detected_by: String,
    pub confidence: f64,
}
