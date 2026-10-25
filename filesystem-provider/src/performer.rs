//! Performer process identity (Stage 2).
//!
//! The performer is the provider process itself: `actions::perform`
//! executes only in-process `std::fs` calls and never spawns child
//! processes, so no delegation distinction is needed. (If a future action
//! ever delegates to a child, its identity must be captured at the child
//! and never attributed to this process without proof.)
//!
//! Identity = OS pid + authoritative process creation time:
//! - Windows: `GetProcessTimes(GetCurrentProcess())`, FILETIME as decimal.
//!   Units: 100ns ticks. Epoch: 1601-01-01 UTC. Clock system: Windows
//!   process creation (kernel). Do NOT compare directly against Unix,
//!   chrono, or Instant timestamps without an explicit conversion.
//! - The pseudo-handle from `GetCurrentProcess` must not be closed.
//! - Non-Windows (or API failure): pid is still reported, `start_time` is
//!   `None` — explicitly unavailable, never approximated by action time,
//!   registration time, or wall clock.

use serde_json::{json, Value};

/// How the identity below was obtained.
pub const SOURCE_WIN32_PROCESS_TIMES: &str = "win32-GetProcessTimes";
/// Explicit marker: identity could not be obtained (never guessed).
pub const SOURCE_UNAVAILABLE: &str = "unavailable";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PerformerIdentity {
    pub pid: u32,
    /// Decimal FILETIME on Windows; `None` = explicitly unavailable.
    pub start_time: Option<String>,
    pub source: &'static str,
}

#[cfg(windows)]
fn creation_filetime() -> Option<u64> {
    use windows_sys::Win32::Foundation::FILETIME;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessTimes};
    unsafe {
        // Pseudo-handle: valid in this process, must NOT be closed.
        let h = GetCurrentProcess();
        let mut creation: FILETIME = std::mem::zeroed();
        let mut _exit: FILETIME = std::mem::zeroed();
        let mut _kernel: FILETIME = std::mem::zeroed();
        let mut _user: FILETIME = std::mem::zeroed();
        if GetProcessTimes(h, &mut creation, &mut _exit, &mut _kernel, &mut _user) == 0 {
            return None;
        }
        Some(((creation.dwHighDateTime as u64) << 32) | creation.dwLowDateTime as u64)
    }
}

#[cfg(not(windows))]
fn creation_filetime() -> Option<u64> {
    None
}

/// Capture this process's performer identity. Infallible by design:
/// failure surfaces as `start_time: None` + `SOURCE_UNAVAILABLE`.
pub fn current_performer() -> PerformerIdentity {
    match creation_filetime() {
        Some(t) => PerformerIdentity {
            pid: std::process::id(),
            start_time: Some(t.to_string()),
            source: SOURCE_WIN32_PROCESS_TIMES,
        },
        None => PerformerIdentity {
            pid: std::process::id(),
            start_time: None,
            source: SOURCE_UNAVAILABLE,
        },
    }
}

/// Identity block attached to successful action details. Same shape for
/// every action type; `start_time: null` = explicitly unavailable.
pub fn performer_json() -> Value {
    let p = current_performer();
    json!({
        "pid": p.pid,
        "start_time": p.start_time,
        "source": p.source,
    })
}
