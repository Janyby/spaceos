use anyhow::Result;
use serde_json::json;
use space_core::{Event, ipc};
use std::collections::HashMap;

/// Minimal process observation: snapshot process list via sysinfo-like polling.
/// Records ProcessStarted / ProcessExited events. Relations are only
/// created later when supported by actual file observations.
pub async fn watch_processes(provider_id: String, filter: Option<String>, interval_ms: u64) -> Result<()> {
    let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
    let pipe = ipc::configured_provider_pipe("SPACEOS_CORE_PIPE", ipc::CORE_PIPE)?;
    watch_processes_on(provider_id, filter, interval_ms, &pipe, rx).await
}

/// Same as [`watch_processes`], but against an explicit Core endpoint (tests).
/// Follows the `scan_on` / `watch_on` precedent: full logic lives here.
pub async fn watch_processes_on(
    provider_id: String,
    filter: Option<String>,
    interval_ms: u64,
    pipe: &str,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) -> Result<()> {
    let mut known: HashMap<u32, String> = snapshot_processes();
    loop {
        tokio::select! {
            _ = &mut stop => return Ok(()),
            _ = tokio::time::sleep(std::time::Duration::from_millis(interval_ms)) => {}
        }
        let now = snapshot_processes();
        for (pid, name) in &now {
            if !known.contains_key(pid) {
                if matches_filter(name, &filter) {
                    let mut ev = Event::new("ProcessStarted", "filesystem", json!({"pid": pid, "name": name}));
                    ev.provider_id = Some(provider_id.clone());
                    let _ = ipc::send_provider_principal_request(pipe, "filesystem", "record_event", json!(ev)).await;
                }
            }
        }
        for (pid, name) in &known {
            if !now.contains_key(pid) && matches_filter(name, &filter) {
                let mut ev = Event::new("ProcessExited", "filesystem", json!({"pid": pid, "name": name}));
                ev.provider_id = Some(provider_id.clone());
                let _ = ipc::send_provider_principal_request(pipe, "filesystem", "record_event", json!(ev)).await;
            }
        }
        known = now;
    }
}

fn matches_filter(name: &str, filter: &Option<String>) -> bool {
    match filter {
        Some(f) => name.to_lowercase().contains(&f.to_lowercase()),
        None => true,
    }
}

/// Running processes: pid -> image name.
///
/// Windows: Toolhelp32 wide-char API (`Process32FirstW`/`Process32NextW`).
/// Names arrive as UTF-16 from the OS itself, so the snapshot is
/// independent of the system locale and of the console code page: the
/// previous `tasklist /FO CSV` byte path failed strict UTF-8 decoding on
/// non-English Windows (e.g. CP866 byte 0x8A) and silently produced an
/// empty map, blinding the observer.
#[cfg(windows)]
fn snapshot_processes() -> HashMap<u32, String> {
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, Process32FirstW, Process32NextW, PROCESSENTRY32W,
        TH32CS_SNAPPROCESS,
    };

    let mut map = HashMap::new();
    unsafe {
        let snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
        if snapshot == INVALID_HANDLE_VALUE {
            return map;
        }
        let mut entry = PROCESSENTRY32W {
            dwSize: std::mem::size_of::<PROCESSENTRY32W>() as u32,
            ..Default::default()
        };
        if Process32FirstW(snapshot, &mut entry) != 0 {
            loop {
                let end = entry
                    .szExeFile
                    .iter()
                    .position(|&c| c == 0)
                    .unwrap_or(entry.szExeFile.len());
                let name = String::from_utf16_lossy(&entry.szExeFile[..end]);
                if !name.is_empty() {
                    map.insert(entry.th32ProcessID, name);
                }
                if Process32NextW(snapshot, &mut entry) == 0 {
                    break;
                }
            }
        }
        CloseHandle(snapshot);
    }
    map
}

/// Non-Windows: no portable process-list source is wired here (the observer
/// is a Windows capability); report no observations rather than fail.
#[cfg(not(windows))]
fn snapshot_processes() -> HashMap<u32, String> {
    HashMap::new()
}

/// Apply a provider observation via the Core's report_observation endpoint.
/// Returns the outcome indicating whether the observation was applied (or was a redelivery).
/// Binds actor "filesystem" (core principal, same precedent as the
/// watcher's rows); per-object provider_id keeps true attribution.
pub async fn apply_observation(
    pipe: &str,
    obs: &space_core::Observation,
) -> Result<space_core::ObservationOutcome> {
    let mut params = serde_json::to_value(obs)?;
    params["actor"] = serde_json::json!("filesystem");
    let (instance_id, instance_token) = space_core::ipc::provider_instance_credential()?;
    params["instance_id"] = serde_json::json!(instance_id);
    params["instance_token"] = serde_json::json!(instance_token);
    let response = space_core::ipc::send_provider_principal_request(pipe, "filesystem", "report_observation", params).await?;
    let outcome: space_core::ObservationOutcome = serde_json::from_value(response["result"].clone())?;
    anyhow::ensure!(!outcome.event_id.is_empty() && if outcome.applied { outcome.seq > 0 } else { outcome.seq == 0 }, "invalid observation outcome");
    Ok(outcome)
}

#[cfg(all(test, windows))]
mod tests {
    use super::snapshot_processes;

    /// Locale/code-page independence regression: on non-English Windows the
    /// old `tasklist /FO CSV` byte path failed strict UTF-8 decoding and
    /// silently returned an empty snapshot. The process-list source must
    /// always see real processes regardless of the system code page.
    #[test]
    fn snapshot_processes_sees_own_process_on_any_locale() {
        let procs = snapshot_processes();
        assert!(
            !procs.is_empty(),
            "process snapshot must not be empty on Windows"
        );
        let own = std::process::id();
        let name = procs
            .get(&own)
            .unwrap_or_else(|| panic!("own pid {own} missing from snapshot"));
        assert!(!name.is_empty(), "own process name must decode non-empty");
    }
}
