//! Source-bound reconciliation. A denied/malformed read is never an empty snapshot.
use anyhow::{Context, Result};
use serde_json::json;
use space_core::{ipc, Object};
use std::path::Path;

pub async fn objects(pipe: &str, root: &Path, external: Option<&str>) -> Result<Vec<Object>> {
    let (instance, token) = ipc::provider_instance_credential().context("authorization_denied")?;
    let credential = ipc::provider_principal_credential().context("authorization_denied")?;
    let mut params = json!({"actor":"filesystem","principal_token":credential,"source_root":root,"instance_id":instance,"instance_token":token});
    if let Some(path) = external { params["external_id"] = json!(path); }
    // Keep Core's structured error code; the generic provider write helper
    // intentionally turns rejected responses into Err and would lose this distinction.
    let response = ipc::send_request(pipe, "query_provider_objects", params)
        .await.context("transport_error")?;
    if response["ok"] != true {
        let code = match response["error_code"].as_str() {
            Some("invalid_binding") => "invalid_binding",
            Some("database_error") => "database_error",
            _ => "authorization_denied",
        };
        anyhow::bail!("{code}");
    }
    serde_json::from_value(response["result"]["objects"].clone()).context("invalid_response")
}
