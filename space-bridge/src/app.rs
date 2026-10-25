//! Shared application services for the Dashboard (HTTP) and OpenCode (MCP).
//!
//! Both clients operate on the same shapes: object summaries, directory
//! filtering, and action-lifecycle summaries derived from Core events.
//! Provider execution itself always goes through Core `request_action`.

use serde_json::{json, Value};

/// Terminal Core event types for one action's lifecycle.
const TERMINAL: &[&str] = &["ActionCompleted", "ActionFailed", "ActionDenied"];

/// Best-effort tag stripper for page-text previews (no new crates).
/// Drops `<script>`/`<style>` blocks, skips `<...>` markup, decodes a
/// handful of entities, collapses whitespace. Output is lossy by
/// design — a preview, never a faithful document copy. Shared by the
/// HTTP bridge and the MCP server so both surfaces read identically.
pub fn strip_tags(html: &str) -> String {
    let mut out = String::with_capacity(html.len().min(65536));
    let mut chars = html.chars().peekable();
    let mut in_block = false; // inside <script>/<style>…</…>
    while let Some(c) = chars.next() {
        if c == '<' {
            let mut tag = String::new();
            let mut closing = false;
            for c2 in chars.by_ref() {
                if c2 == '>' {
                    break;
                }
                if c2 == '/' && tag.is_empty() {
                    closing = true;
                    continue;
                }
                if c2.is_whitespace() {
                    for c3 in chars.by_ref() {
                        if c3 == '>' {
                            break;
                        }
                    }
                    break;
                }
                tag.push(c2.to_ascii_lowercase());
            }
            if tag == "script" || tag == "style" {
                in_block = !closing;
            }
            if !closing {
                out.push(' ');
            }
            continue;
        }
        if in_block {
            continue;
        }
        if c == '&' {
            let mut ent = String::new();
            for c2 in chars.by_ref().take(12) {
                if c2 == ';' {
                    break;
                }
                ent.push(c2);
            }
            match ent.as_str() {
                "amp" => out.push('&'),
                "lt" => out.push('<'),
                "gt" => out.push('>'),
                "quot" => out.push('"'),
                "nbsp" => out.push(' '),
                _ => {}
            }
            continue;
        }
        out.push(c);
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// One directory entry for the file manager, from a stored object.
pub fn object_summary(o: &Value) -> Value {
    let props = o.get("properties").cloned().unwrap_or(json!({}));
    let meta = props.get("meta").cloned().unwrap_or(json!({}));
    json!({
        "id": o.get("id"),
        "type": o.get("type"),
        "path": props.get("path"),
        "file_type": meta.get("file_type"),
        "size": meta.get("size"),
        "modified": meta.get("modified"),
        "external_id": o.get("external_id"),
    })
}

/// Normalize a directory prefix for containment checks (case-insensitive,
/// trailing separator ensured).
pub fn dir_prefix(dir: &str) -> String {
    let mut d = dir.trim().to_lowercase().replace('/', "\\");
    while d.ends_with('\\') && d.len() > 3 {
        d.pop();
    }
    if !d.ends_with('\\') {
        d.push('\\');
    }
    d
}

/// Keep stored objects whose `properties.path` lies under `dir`, mapped to
/// summaries, sorted by path, truncated to `limit`.
pub fn filter_by_dir(objects: Vec<Value>, dir: &str, limit: usize) -> (Vec<Value>, bool) {
    let prefix = dir_prefix(dir);
    let mut items: Vec<Value> = objects
        .into_iter()
        .filter(|o| {
            o.get("properties")
                .and_then(|p| p.get("path"))
                .and_then(|v| v.as_str())
                .map(|p| p.to_lowercase().replace('/', "\\").starts_with(&prefix))
                .unwrap_or(false)
        })
        .map(|o| object_summary(&o))
        .collect();
    sort_summaries(&mut items);
    let truncated = items.len() > limit;
    items.truncate(limit);
    (items, truncated)
}

/// Map stored objects to summaries, sorted by path, truncated to `limit`.
pub fn summarize(objects: Vec<Value>, limit: usize) -> (Vec<Value>, bool) {
    let mut items: Vec<Value> = objects.into_iter().map(|o| object_summary(&o)).collect();
    sort_summaries(&mut items);
    let truncated = items.len() > limit;
    items.truncate(limit);
    (items, truncated)
}

fn sort_summaries(items: &mut [Value]) {
    items.sort_by(|a, b| {
        a.get("path")
            .and_then(|v| v.as_str())
            .cmp(&b.get("path").and_then(|v| v.as_str()))
    });
}

/// Lifecycle of one action from its Core events (oldest first):
/// Queued (only Requested) -> Running (Started, no terminal yet) ->
/// Completed | Failed | Denied | Unknown (no events at all).
pub fn action_summary(action_id: &str, events: &[Value]) -> Value {
    let mut status = "Unknown";
    let mut terminal: Option<&Value> = None;
    let mut requested = false;
    let mut started = false;
    for e in events {
        match e.get("event_type").and_then(|v| v.as_str()) {
            Some("ActionRequested") => requested = true,
            Some("ActionStarted") => started = true,
            Some(t) if TERMINAL.contains(&t) => terminal = Some(e),
            _ => {}
        }
    }
    if let Some(t) = terminal {
        status = match t.get("event_type").and_then(|v| v.as_str()) {
            Some("ActionCompleted") => "Completed",
            Some("ActionDenied") => "Denied",
            _ => "Failed",
        };
    } else if started || requested {
        status = if started { "Running" } else { "Queued" };
    }
    json!({
        "action_id": action_id,
        "status": status,
        "terminal": terminal.cloned().unwrap_or(Value::Null),
        "event_count": events.len(),
    })
}

/// Compact history row for one action id and its events. Carries the
/// audit fields the Activity view needs: operation (Core action name),
/// resource (primary target: url/path/target_id), correlation id (= the
/// action id), and the terminal error/result message when present.
pub fn history_row(action_id: &str, events: &[Value]) -> Value {
    let first = events.first();
    let summary = action_summary(action_id, events);
    let requested = events
        .iter()
        .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("ActionRequested"));
    let operation = requested
        .and_then(|e| e.get("payload"))
        .and_then(|p| p.get("action"))
        .cloned()
        .unwrap_or(Value::Null);
    let resource = requested
        .and_then(|e| e.get("payload"))
        .and_then(|p| p.get("target"))
        .map(|t| {
            for k in ["url", "path", "target_id", "dir", "q"] {
                if let Some(v) = t.get(k).and_then(|v| v.as_str()) {
                    return Value::String(v.to_string());
                }
            }
            Value::Null
        })
        .unwrap_or(Value::Null);
    let terminal = summary.get("terminal").cloned().unwrap_or(Value::Null);
    let message = terminal
        .get("payload")
        .map(|p| {
            for k in ["error", "job", "stats", "navigated", "created", "wrote"] {
                if let Some(v) = p.get(k) {
                    if !v.is_null() {
                        return v.clone();
                    }
                }
            }
            Value::Null
        })
        .unwrap_or(Value::Null);
    json!({
        "action_id": action_id,
        "correlation_id": action_id,
        "status": summary["status"],
        "actor": first.and_then(|e| e.get("actor")).cloned().unwrap_or(Value::Null),
        "provider_id": first.and_then(|e| e.get("provider_id")).cloned().unwrap_or(Value::Null),
        "operation": operation,
        "resource": resource,
        "message": message,
        "first_seen": first.and_then(|e| e.get("timestamp")).cloned().unwrap_or(Value::Null),
        "last_seen": events.last().and_then(|e| e.get("timestamp")).cloned().unwrap_or(Value::Null),
        "event_count": events.len(),
    })
}

/// Group a flat event list (any order) into per-action history rows,
/// newest action first. Only `Action*` event types are considered.
pub fn action_history(events: Vec<Value>, limit: usize) -> Vec<Value> {
    use std::collections::BTreeMap;
    let mut by_id: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    for e in events.iter().filter(|e| {
        e.get("event_type")
            .and_then(|v| v.as_str())
            .map(|t| t.starts_with("Action"))
            .unwrap_or(false)
    }) {
        if let Some(id) = e.get("correlation_id").and_then(|v| v.as_str()) {
            by_id.entry(id.to_string()).or_default().push(e);
        }
    }
    let mut rows: Vec<Value> = by_id
        .iter()
        .map(|(id, evs)| {
            let mut sorted: Vec<Value> = evs.iter().map(|e| (*e).clone()).collect();
            sorted.sort_by(|a, b| {
                a.get("timestamp")
                    .and_then(|v| v.as_str())
                    .cmp(&b.get("timestamp").and_then(|v| v.as_str()))
            });
            history_row(id, &sorted)
        })
        .collect();
    // Newest first by last_seen.
    rows.sort_by(|a, b| {
        b.get("last_seen")
            .and_then(|v| v.as_str())
            .cmp(&a.get("last_seen").and_then(|v| v.as_str()))
    });
    rows.truncate(limit);
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(t: &str, corr: &str, ts: &str) -> Value {
        json!({
            "event_type": t,
            "correlation_id": corr,
            "timestamp": ts,
            "actor": "a",
        })
    }

    #[test]
    fn lifecycle_states() {
        assert_eq!(action_summary("x", &[])["status"], "Unknown");
        assert_eq!(
            action_summary("x", &[ev("ActionRequested", "x", "1")])["status"],
            "Queued"
        );
        assert_eq!(
            action_summary(
                "x",
                &[
                    ev("ActionRequested", "x", "1"),
                    ev("ActionStarted", "x", "2")
                ]
            )["status"],
            "Running"
        );
        assert_eq!(
            action_summary(
                "x",
                &[
                    ev("ActionRequested", "x", "1"),
                    ev("ActionStarted", "x", "2"),
                    ev("ActionCompleted", "x", "3"),
                ]
            )["status"],
            "Completed"
        );
        assert_eq!(
            action_summary("x", &[ev("ActionDenied", "x", "1")])["status"],
            "Denied"
        );
    }

    #[test]
    fn dir_prefix_rules() {
        assert_eq!(dir_prefix("C:/data"), "c:\\data\\");
        assert_eq!(dir_prefix("C:\\data\\\\"), "c:\\data\\");
    }

    #[test]
    fn action_history_groups_and_sorts() {
        // Two actions with interleaved events; expect newest-first, correct grouping.
        let events = vec![
            ev("ActionRequested", "a1", "10:00:00"),
            ev("ActionRequested", "a2", "10:00:05"),
            ev("ActionCompleted", "a1", "10:00:10"),
            ev("ActionCompleted", "a2", "10:00:08"),
        ];
        let rows = action_history(events, 50);
        assert_eq!(rows.len(), 2, "two actions grouped: {rows:?}");
        // a2 newer last_seen than a1 -> a2 first.
        assert_eq!(rows[0]["actor"], "a");
        assert_eq!(rows[1]["actor"], "a");
        // Status reflects the terminal event.
        let a2_status = rows
            .iter()
            .find(|r| r["action_id"] == "a2")
            .map(|r| r["status"].as_str())
            .unwrap();
        assert_eq!(a2_status, Some("Completed"));
        let a1_status = rows
            .iter()
            .find(|r| r["action_id"] == "a1")
            .map(|r| r["status"].as_str())
            .unwrap();
        assert_eq!(a1_status, Some("Completed"));
    }

    #[test]
    fn action_history_filters_non_action_events() {
        // Non-Action* events must not create spurious rows.
        let events = vec![
            json!({"event_type": "SystemBoot", "correlation_id": "x", "timestamp": "1"}),
            ev("ActionRequested", "good", "2"),
            ev("ActionCompleted", "good", "3"),
        ];
        let rows = action_history(events, 50);
        assert_eq!(rows.len(), 1, "only the Action* event grouped: {rows:?}");
        assert_eq!(rows[0]["action_id"], "good");
    }

    #[test]
    fn action_history_limits_results() {
        // Many actions but limit=2: only newest 2 returned.
        let mut events = vec![];
        for i in 0..10 {
            let ts = format!("10:00:0{}", i);
            events.push(ev("ActionRequested", &format!("a{}", i), &ts));
            events.push(ev("ActionCompleted", &format!("a{}", i), &ts));
        }
        let rows = action_history(events, 2);
        assert_eq!(rows.len(), 2, "limited to 2: {rows:?}");
        // Newest first by last_seen.
        assert_ne!(rows[0]["action_id"], rows[1]["action_id"]);
    }

    #[test]
    fn filter_by_dir_nested_and_missing() {
        let objects = vec![
            json!({"id": "1", "properties": {"path": "C:\\data\\a.txt"}, "type": "file"}),
            json!({"id": "2", "properties": {"path": "C:\\data\\sub\\b.txt"}, "type": "file"}),
            json!({"id": "3", "properties": {"path": "C:\\other\\c.txt"}, "type": "file"}),
            // No path property -> filtered out, never panics.
            json!({"id": "4", "properties": {}}),
        ];
        let (items, truncated) = filter_by_dir(objects, "C:/data", 200);
        assert_eq!(items.len(), 2, "only C:\\data entries: {items:?}");
        assert!(!truncated);
        let paths: Vec<String> = items
            .iter()
            .map(|i| i["path"].as_str().unwrap().to_string())
            .collect();
        assert!(paths.iter().any(|p| p == "C:\\data\\a.txt"));
        assert!(paths.iter().any(|p| p == "C:\\data\\sub\\b.txt"));
    }
}
