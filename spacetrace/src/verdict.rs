//! Evidence contract + verdict classification (Stage 1).
//!
//! Pure data + pure function. `classify` is deterministic: equal inputs
//! give equal verdicts. It reads no files, touches no IPC/OS/Storage,
//! and collects no traces. OS backends (ETW/eBPF) do not exist yet; their
//! future fields are explicitly optional and distinguished from the data
//! available today.
//!
//! Decision table (see docs/VERDICT_RULES.md for the full wording):
//!
//! | # | Condition | Verdict |
//! |---|-----------|---------|
//! | 1 | semantic action not completed | Unknown (`semantic-effect-absent`) |
//! | 2 | known trace loss in window | Unknown (`trace-gap-loss`) |
//! | 3 | semantic chain incomplete | Unknown (`incomplete-chain`) |
//! | 4 | observer rights unconfirmed | Unknown (`observation-rights-unconfirmed`) |
//! | 5 | no OS evidence at all | Unknown (`missing-os-evidence`) |
//! | 6 | >=2 candidates match file identity | Probable (`ambiguous-candidates`) |
//! | 7 | exactly 1 file-identity match + performer bound + window holds | Confirmed |
//! | 8 | 1 match but pid without start-time (or mismatch) | Probable (`pid-reuse-unresolved`) |
//! | 9 | 1 match but performer identity unavailable | Probable (`performer-instance-unbound`) |
//! | 10 | 1 match but window not evaluable | Probable (`window-unevaluated`) |
//! | 11 | no file-identity match; single informational path+window match | Probable (`path-informational-only`) |
//! | 12 | no file-identity match; several path matches | Probable (`ambiguous-candidates`) |
//! | 13 | OS evidence present but nothing matches | Unknown (`os-evidence-no-match`) |
//!
//! Hard rules (no exceptions): timestamps alone never prove causation;
//! pid alone is insufficient (reuse); missing OS evidence is Unknown, never
//! disproof; loss/incompleteness caps at Unknown; a recorded successful
//! action alone never confirms an effect; Confirmed requires independent
//! evidence plus sufficient identification. The performer side is captured
//! since Stage 2 (provider `perform()` records pid + creation time); the OS
//! side and `file_key` still await backends, so Confirmed remains
//! unreachable on real inputs by construction — synthetic tests below use
//! future-shaped inputs to pin the boundary.

use crate::{TraceEvent, TraceKind, TraceLayer};

/// Machine-readable reason codes. Every verdict carries at least one.
pub mod reason {
    pub const SEMANTIC_EFFECT_ABSENT: &str = "semantic-effect-absent";
    pub const TRACE_GAP_LOSS: &str = "trace-gap-loss";
    pub const INCOMPLETE_CHAIN: &str = "incomplete-chain";
    pub const RIGHTS_UNCONFIRMED: &str = "observation-rights-unconfirmed";
    pub const MISSING_OS_EVIDENCE: &str = "missing-os-evidence";
    pub const OS_EVIDENCE_NO_MATCH: &str = "os-evidence-no-match";
    pub const AMBIGUOUS_CANDIDATES: &str = "ambiguous-candidates";
    pub const SAME_ACTION_CHAIN: &str = "same-action-chain";
    pub const FILE_KEY_MATCH: &str = "file-key-match";
    pub const PERFORMER_INSTANCE_BOUND: &str = "performer-instance-bound";
    pub const WINDOW_CONTAINED: &str = "window-contained";
    pub const PID_REUSE_UNRESOLVED: &str = "pid-reuse-unresolved";
    pub const PERFORMER_INSTANCE_UNBOUND: &str = "performer-instance-unbound";
    pub const WINDOW_UNEVALUATED: &str = "window-unevaluated";
    pub const PATH_INFORMATIONAL_ONLY: &str = "path-informational-only";
    pub const FILE_IDENTITY_UNAVAILABLE: &str = "file-identity-unavailable";
}

/// A process instance: pid is meaningless without its start time.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProcessInstance {
    pub pid: u32,
    /// Kernel process start time (opaque string, backend-defined).
    /// `None` = unavailable — never treat pid alone as identity.
    pub start_time: Option<String>,
    /// How the identity was obtained (e.g. "win32-GetProcessTimes").
    /// `None` = provenance not recorded (pre-Stage-2 payloads).
    #[serde(default)]
    pub source: Option<String>,
}

/// Semantic (SpaceOS-side) evidence for one action.
/// Only `action_id`, `correlation_id`, `target`, `provider_id`,
/// `action_completed` are fillable by Core alone; `file_key` awaits a
/// future backend, while `performer` (now with `source`) is filled by the
/// filesystem provider at `perform()` time since Stage 2.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SemanticEvidence {
    pub action_id: String,
    pub correlation_id: String,
    pub agent_instance_id: Option<String>,
    pub target: String,
    pub provider_id: String,
    pub action_completed: bool,
    /// Stable file-object identity when the provider learns to capture it.
    pub file_key: Option<String>,
    /// Performer recorded by the provider at `perform()` time.
    pub performer: Option<ProcessInstance>,
    /// Action window as unix microseconds (start, end), when known.
    pub window_us: Option<(i64, i64)>,
    /// Trace ids backing this evidence (for `trace.query` linkage).
    pub trace_ids: Vec<String>,
}

/// One OS-side observation (ETW backend since Stage 3, eBPF future).
/// `path` is informational only — never identity (paths are unstable).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OsEvidence {
    /// Backend source: "etw" | "ebpf".
    pub source: String,
    pub pid: u32,
    /// Kernel process start time; `None` = unavailable.
    pub process_start_time: Option<String>,
    /// Boot-scoped process instance key (e.g. ETW UniqueProcessKey);
    /// anti-reuse datum when no wall-clock start time exists.
    /// `None` = unavailable. Added in Stage 3, defaulted for compatibility.
    #[serde(default)]
    pub instance_key: Option<String>,
    /// Stable file-object identity (not a path); `None` = unavailable.
    pub file_key: Option<String>,
    /// Informational path at observation time; never matched as identity.
    pub path: Option<String>,
    pub op: String,
    /// Kernel timestamp, unix microseconds; `None` = unavailable.
    pub time_us: Option<i64>,
    /// Opaque backend reference (event id, record number, ...).
    pub backend_ref: String,
}

/// Completeness flags for the observation window.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GapFlags {
    /// Buffer `dropped_count` grew inside the window (or is unknown-high).
    pub trace_dropped: bool,
    /// Observer rights were verified for this window.
    pub observation_rights: bool,
    /// Semantic chain (decision/action/invoke/complete) is complete.
    pub trace_complete: bool,
}

impl Default for GapFlags {
    /// Strict default: gaps assumed until a backend proves otherwise.
    fn default() -> Self {
        Self {
            trace_dropped: false,
            observation_rights: false,
            trace_complete: true,
        }
    }
}

/// Verdict status: honest Unknown beats unproven Confirmed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum VerdictStatus {
    Confirmed,
    Probable,
    UnknownGap,
}

impl VerdictStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            VerdictStatus::Confirmed => "confirmed",
            VerdictStatus::Probable => "probable",
            VerdictStatus::UnknownGap => "unknown-gap",
        }
    }
}

/// Correlation verdict: machine-readable reasons plus references to the
/// backing evidence whenever such references exist.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Verdict {
    pub subject_action_id: String,
    pub status: VerdictStatus,
    /// At least one reason code; fixed push order (deterministic).
    pub reasons: Vec<String>,
    /// Trace ids of the semantic evidence (for `trace.query` linkage).
    pub semantic_refs: Vec<String>,
    /// Opaque backend refs of the matched OS evidence (may be empty).
    pub os_refs: Vec<String>,
}

fn unknown(action_id: &str, reason: &str, sem: &SemanticEvidence) -> Verdict {
    Verdict {
        subject_action_id: action_id.to_string(),
        status: VerdictStatus::UnknownGap,
        reasons: vec![reason.to_string()],
        semantic_refs: sem.trace_ids.clone(),
        os_refs: vec![],
    }
}

/// Deterministic classifier: equal inputs give equal verdicts.
/// No I/O, no clock reads, no global state.
pub fn classify(sem: &SemanticEvidence, os: &[OsEvidence], gaps: &GapFlags) -> Verdict {
    let aid = sem.action_id.as_str();

    // Rule 6: no successful semantic effect, nothing to confirm.
    if !sem.action_completed {
        return unknown(aid, reason::SEMANTIC_EFFECT_ABSENT, sem);
    }
    // Rule 4: known loss caps at Unknown — a missed event could
    // contradict everything we see.
    if gaps.trace_dropped {
        return unknown(aid, reason::TRACE_GAP_LOSS, sem);
    }
    if !gaps.trace_complete {
        return unknown(aid, reason::INCOMPLETE_CHAIN, sem);
    }
    if !gaps.observation_rights {
        return unknown(aid, reason::RIGHTS_UNCONFIRMED, sem);
    }
    // Rule 3: missing OS evidence is Unknown, never disproof.
    if os.is_empty() {
        return unknown(aid, reason::MISSING_OS_EVIDENCE, sem);
    }

    // Strict file-identity match: both sides present and equal.
    // Paths are never matched as identity (unstable across renames).
    let strict_matches: Vec<&OsEvidence> = os
        .iter()
        .filter(|o| match (&sem.file_key, &o.file_key) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        })
        .collect();

    if strict_matches.len() >= 2 {
        return Verdict {
            subject_action_id: aid.to_string(),
            status: VerdictStatus::Probable,
            reasons: vec![reason::AMBIGUOUS_CANDIDATES.to_string()],
            semantic_refs: sem.trace_ids.clone(),
            os_refs: strict_matches
                .iter()
                .map(|o| o.backend_ref.clone())
                .collect(),
        };
    }

    if let Some(m) = strict_matches.first() {
        // Exactly one file-identity match: check performer binding (rule 2)
        // and window containment (rule 1: necessary, never sufficient).
        let pid_ok = match (&sem.performer, m.pid) {
            (Some(p), pid) if p.pid == pid => true,
            _ => false,
        };
        let start_ok = match (&sem.performer, &m.process_start_time) {
            (Some(p), Some(t)) => p.start_time.as_deref() == Some(t.as_str()),
            _ => false,
        };
        let window_ok = match (sem.window_us, m.time_us) {
            (Some((lo, hi)), Some(t)) => lo <= t && t <= hi,
            _ => false,
        };
        if pid_ok && start_ok && window_ok {
            return Verdict {
                subject_action_id: aid.to_string(),
                status: VerdictStatus::Confirmed,
                reasons: vec![
                    reason::SAME_ACTION_CHAIN.to_string(),
                    reason::FILE_KEY_MATCH.to_string(),
                    reason::PERFORMER_INSTANCE_BOUND.to_string(),
                    reason::WINDOW_CONTAINED.to_string(),
                ],
                semantic_refs: sem.trace_ids.clone(),
                os_refs: vec![m.backend_ref.clone()],
            };
        }
        // One match, but identification incomplete: name the exact gap.
        let gap_reason = if !pid_ok {
            // Same file touched by a process we cannot bind to the performer.
            reason::PERFORMER_INSTANCE_UNBOUND
        } else if !start_ok {
            // Rule 2: pid without a matching start time (PID reuse possible).
            reason::PID_REUSE_UNRESOLVED
        } else {
            reason::WINDOW_UNEVALUATED
        };
        return Verdict {
            subject_action_id: aid.to_string(),
            status: VerdictStatus::Probable,
            reasons: vec![reason::FILE_KEY_MATCH.to_string(), gap_reason.to_string()],
            semantic_refs: sem.trace_ids.clone(),
            os_refs: vec![m.backend_ref.clone()],
        };
    }

    // No file-identity match. Informational path coincidence is weak:
    // at most Probable, and only for a single candidate inside the window.
    let path_matches: Vec<&OsEvidence> = os
        .iter()
        .filter(|o| o.path.as_deref() == Some(sem.target.as_str()))
        .collect();
    let window_holds = |o: &OsEvidence| match (sem.window_us, o.time_us) {
        (Some((lo, hi)), Some(t)) => lo <= t && t <= hi,
        _ => false,
    };
    let in_window: Vec<&&OsEvidence> = path_matches.iter().filter(|o| window_holds(o)).collect();
    if in_window.len() == 1 {
        return Verdict {
            subject_action_id: aid.to_string(),
            status: VerdictStatus::Probable,
            reasons: vec![
                reason::PATH_INFORMATIONAL_ONLY.to_string(),
                reason::FILE_IDENTITY_UNAVAILABLE.to_string(),
            ],
            semantic_refs: sem.trace_ids.clone(),
            os_refs: vec![in_window[0].backend_ref.clone()],
        };
    }
    if in_window.len() >= 2 || path_matches.len() >= 2 {
        return Verdict {
            subject_action_id: aid.to_string(),
            status: VerdictStatus::Probable,
            reasons: vec![reason::AMBIGUOUS_CANDIDATES.to_string()],
            semantic_refs: sem.trace_ids.clone(),
            os_refs: in_window.iter().map(|o| o.backend_ref.clone()).collect(),
        };
    }
    unknown(aid, reason::OS_EVIDENCE_NO_MATCH, sem)
}

/// Publish a verdict as an ordinary `CorrelationLink` trace event so the
/// existing `trace.query` path (buffer → index → provider query) serves it
/// with no contract change. Storage, indexing, auth and world-scope are
/// the caller's existing mechanism, not something verdicts bring along.
pub fn verdict_event(
    verdict: &Verdict,
    correlation_id: &str,
    world_id: Option<&str>,
) -> TraceEvent {
    let mut ev = TraceEvent::new(
        TraceLayer::Core,
        TraceKind::CorrelationLink,
        correlation_id,
        serde_json::json!({
            "subject_action_id": verdict.subject_action_id,
            "verdict": verdict.status.as_str(),
            "reasons": verdict.reasons,
            "semantic_refs": verdict.semantic_refs,
            "os_refs": verdict.os_refs,
        }),
    )
    .with_action_id(verdict.subject_action_id.clone());
    if let Some(w) = world_id {
        ev = ev.with_world_id(w);
    }
    ev
}
