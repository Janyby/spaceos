use serde_json::{json, Value};
use std::path::Path;

pub fn stat(path: &Path) -> Value {
    match std::fs::metadata(path) {
        Ok(md) => {
            let ext = path
                .extension()
                .and_then(|e| e.to_str())
                .map(|s| s.to_string());
            json!({
                "path": path.to_string_lossy(),
                "file_type": if md.is_dir() { "Directory" } else { "File" },
                "size": md.len(),
                "extension": ext,
                "mime": mime_from_path(path),
                "created": md.created().ok().map(systemtime_rfc3339),
                "modified": md.modified().ok().map(systemtime_rfc3339),
                "accessed": md.accessed().ok().map(systemtime_rfc3339),
            })
        }
        Err(e) => json!({"path": path.to_string_lossy(), "error": e.to_string()}),
    }
}

fn systemtime_rfc3339(t: std::time::SystemTime) -> String {
    let dt: chrono::DateTime<chrono::Utc> = t.into();
    // Nanos precision: optimistic concurrency (`expected_modified`) must
    // distinguish rapid successive writes; seconds truncation made a
    // sneaky write within the same second invisible (roots_gate proved it).
    dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, false)
}

pub fn mime_from_path(path: &Path) -> String {
    mime_guess::from_path(path)
        .first_raw()
        .unwrap_or("application/octet-stream")
        .to_string()
}
