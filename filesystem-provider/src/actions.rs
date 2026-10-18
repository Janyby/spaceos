use anyhow::Result;
use serde_json::{json, Value};
use std::path::Path;

use crate::performer::performer_json;

/// Attach performer identity to a successful action detail. The detail
/// travels untouched through `ActionResult` into the Core `ActionCompleted`
/// event payload — no new IPC contract, no Core change.
fn with_performer(mut detail: Value) -> Value {
    detail["performer"] = performer_json();
    detail
}

/// Maximum bytes returned by the `read` action (explicit error above it).
pub const READ_MAX_BYTES: u64 = 256 * 1024;

/// Execute filesystem actions. The watcher/observation path records the
/// resulting Reality changes; these actions only change Windows state.
///
/// Path allowlisting (allowed roots) is enforced by the caller
/// (`serve_lines`), which knows the daemon's effective roots; `perform`
/// itself stays a pure filesystem operation.
pub async fn perform(action: &str, target: &Value) -> Result<Value> {
    match action {
        "create" => {
            let path = target["path"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing path"))?;
            // Never overwrite silently: an existing file must be edited
            // through `write`, so accidents stay loud and auditable.
            if Path::new(path).exists() {
                anyhow::bail!("already exists (use write to modify): {path}");
            }
            std::fs::write(path, target["content"].as_str().unwrap_or(""))?;
            Ok(with_performer(json!({"created": path})))
        }
        "read" => {
            let path = target["path"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing path"))?;
            let md =
                std::fs::metadata(path).map_err(|e| anyhow::anyhow!("cannot stat {path}: {e}"))?;
            if md.is_dir() {
                anyhow::bail!("{path} is a directory, not a file");
            }
            if md.len() > READ_MAX_BYTES {
                anyhow::bail!(
                    "file too large ({} bytes, limit {}): {path}",
                    md.len(),
                    READ_MAX_BYTES
                );
            }
            let bytes = std::fs::read(path)?;
            let content = String::from_utf8(bytes)
                .map_err(|_| anyhow::anyhow!("not a UTF-8 text file: {path}"))?;
            // `modified` feeds optimistic concurrency: callers pass it back
            // as `expected_modified` on write to detect races.
            let modified = crate::metadata::stat(Path::new(path))
                .get("modified")
                .cloned()
                .unwrap_or(Value::Null);
            Ok(with_performer(json!({
                "path": path,
                "size": md.len(),
                "modified": modified,
                "content": content,
            })))
        }
        "rename" => {
            let from = target["from"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing from"))?;
            let to = target["to"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing to"))?;
            std::fs::rename(from, to)?;
            // try to preserve identity: update the object's external_id if we know provider
            Ok(with_performer(json!({"renamed": from, "to": to})))
        }
        "move" => {
            let from = target["from"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing from"))?;
            let to = target["to"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing to"))?;
            std::fs::rename(from, to)?;
            Ok(with_performer(json!({"moved": from, "to": to})))
        }
        "copy" => {
            let from = target["from"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing from"))?;
            let to = target["to"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing to"))?;
            std::fs::copy(from, to)?;
            Ok(with_performer(json!({"copied": from, "to": to})))
        }
        "delete" => {
            let path = target["path"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing path"))?;
            if Path::new(path).is_dir() {
                std::fs::remove_dir_all(path)?;
            } else {
                std::fs::remove_file(path)?;
            }
            Ok(with_performer(json!({"deleted": path})))
        }
        "write" => {
            let path = target["path"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing path"))?;
            // Optimistic concurrency: when the caller passes the stamp it
            // saw on read (`expected_modified` and/or `expected_size`),
            // refuse to overwrite silently if the file changed since. The
            // error keeps the machine-readable `conflict: ` prefix so
            // dashboard and MCP clients can prompt for re-read instead of
            // losing data.
            //
            // Both fields are compared because Windows does not always
            // advance mtime between rapid successive writes (proven: two
            // back-to-back fs::write calls report identical 100ns
            // intervals), so a timestamp alone can miss a sneaky write.
            if target.get("expected_modified").is_some() || target.get("expected_size").is_some() {
                let current = crate::metadata::stat(Path::new(path));
                if let Some(expected) = target.get("expected_modified").and_then(|v| v.as_str()) {
                    let cur_mod = current
                        .get("modified")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if cur_mod != expected {
                        anyhow::bail!(
                            "conflict: {path} changed since read (expected modified {expected:?}, current {cur_mod:?}); re-read before saving"
                        );
                    }
                }
                if let Some(expected_size) = target.get("expected_size").and_then(|v| v.as_u64()) {
                    let cur_size = current
                        .get("size")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(u64::MAX);
                    if cur_size != expected_size {
                        anyhow::bail!(
                            "conflict: {path} changed since read (expected size {expected_size}, current {cur_size}); re-read before saving"
                        );
                    }
                }
            }
            std::fs::write(path, target["content"].as_str().unwrap_or(""))?;
            Ok(with_performer(json!({"wrote": path})))
        }
        other => anyhow::bail!("unknown action {other}"),
    }
}
