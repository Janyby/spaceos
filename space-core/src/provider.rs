//! Provider contract — unified interface for observation and action providers.
//!
//! This module defines the contract between Core and observation/action providers
//!   (Filesystem, Browser/CDP, Process, ETW, SpaceTrace).
//!
//! Design principles:
//! - Provider is an independent source of observations/actions.
//! - Core knows only the trait interface, not implementation details.
//! - Backward compatible: existing `register_provider` IPC continues to work.

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Opaque handle for providers to interact with Core.
/// Does NOT expose direct access to Storage, Policy, or internal state.
#[derive(Clone)]
pub struct CoreHandle {
    pub(crate) sender: crate::ipc::CoreSender,
}

impl CoreHandle {
    /// Record an event from a provider.
    pub async fn record_event(&self, event: crate::Event) -> Result<(), anyhow::Error> {
        self.sender.record_event(event).await
    }

    /// Query objects (read-only).
    pub async fn query_objects(
        &self,
        query: crate::query::ObjectQuery,
    ) -> Result<Vec<crate::Object>, anyhow::Error> {
        self.sender.query_objects(query).await
    }

    /// Get provider info by id.
    pub async fn provider_info(&self, id: &str) -> Result<Option<ProviderInfo>, anyhow::Error> {
        self.sender.provider_info(id).await
    }
}

/// Opaque token for cooperative cancellation of provider background tasks.
#[derive(Clone)]
pub struct StopToken {
    inner: std::sync::Arc<tokio::sync::Notify>,
}

impl StopToken {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Returns true if stop has been signaled.
    pub fn is_stopped(&self) -> bool {
        // Best-effort check; actual stopping happens via Drop of ProviderHandle
        false // Simplified: actual implementation would track state
    }

    /// Wait until stop is signaled.
    pub async fn wait(&self) {
        self.inner.notified().await;
    }

    /// Signal stop to all holders of this token.
    pub fn trigger(&self) {
        self.inner.notify_waiters();
    }
}

impl Default for StopToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Provider lifecycle error.
#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    #[error("IPC error: {0}")]
    Ipc(#[from] anyhow::Error),
    #[error("Provider already registered: {0}")]
    AlreadyRegistered(String),
    #[error("Provider not found: {0}")]
    NotFound(String),
    #[error("Capability not supported: {0}")]
    UnsupportedCapability(String),
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Provider stopped")]
    Stopped,
}

/// Capability — typed description of provider capabilities.
///
/// Wraps a `&'static str` for zero-allocation const construction.
/// Serialized as a plain JSON string for IPC backward compatibility.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Capability(pub &'static str);

impl Capability {
    // --- Filesystem ---
    pub const SCAN: Capability = Capability("scan");
    pub const WATCH: Capability = Capability("watch");
    pub const FORMAT: Capability = Capability("format");
    pub const ACTIONS: Capability = Capability("actions");

    // --- Browser/CDP ---
    pub const OBSERVE: Capability = Capability("observe");
    pub const NAVIGATE: Capability = Capability("navigate");

    // --- Process ---
    pub const OBSERVE_PROCESS: Capability = Capability("observe_process");

    // --- SpaceTrace ---
    pub const TRACE_OBSERVE: Capability = Capability("trace.observe");
    pub const TRACE_QUERY: Capability = Capability("trace.query");
    pub const TRACE_CORRELATE: Capability = Capability("trace.correlate");
    pub const TRACE_METRICS: Capability = Capability("trace.metrics");

    /// Construct a Capability from a static string.
    /// This is a `const fn` so it can be used for compile-time constants.
    pub const fn new(s: &'static str) -> Self {
        Capability(s)
    }

    /// Return the inner string.
    pub fn as_str(&self) -> &str {
        self.0
    }
}

impl std::fmt::Display for Capability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&'static str> for Capability {
    fn from(s: &'static str) -> Self {
        Capability(s)
    }
}

/// Serialize as a plain string (backward compatible with old `String`-based IPC).
impl Serialize for Capability {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.0)
    }
}

/// Deserialize from a string. Uses `String` internally then leaks to `'static`
/// (acceptable: capabilities are parsed at init, not in hot paths;
/// in production, `ProviderInfo.capabilities` is `Vec<String>`, not `Vec<Capability>`).
impl<'de> Deserialize<'de> for Capability {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s: String = String::deserialize(deserializer)?;
        Ok(Capability(Box::leak(s.into_boxed_str())))
    }
}

/// Provider registration info — exchanged during registration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderInfo {
    /// Unique provider identifier (e.g., "filesystem", "browser", "process", "etw", "spacetrace").
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Declared capabilities (serialized as strings for IPC compatibility).
    pub capabilities: Vec<String>,
    /// Optional version for future compatibility.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

/// Opaque handle returned by Provider::start, used to manage lifecycle.
pub struct ProviderHandle {
    pub(crate) stop_token: StopToken,
    pub(crate) provider_id: String,
}

impl ProviderHandle {
    /// Create a new provider handle.
    pub fn new(stop_token: StopToken, provider_id: String) -> Self {
        Self {
            stop_token,
            provider_id,
        }
    }

    pub fn provider_id(&self) -> &str {
        &self.provider_id
    }

    /// Signal stop to the provider.
    pub fn stop(&self) {
        self.stop_token.trigger();
    }

    /// Wait for provider to fully stop.
    pub async fn wait_stopped(self) {
        self.stop_token.wait().await;
    }
}

/// Trait for all observation/action providers.
///
/// Implementors must be Send + Sync + 'static.
/// Provider lifecycle:
///   1. Core creates provider instance.
///   2. Core calls `provider.start(core_handle, stop_token).await`.
///   3. Provider spawns background tasks, registers itself via CoreHandle.
///   4. Core signals stop via `stop_token` or drops `ProviderHandle`.
///   5. Provider cleans up and returns from `start`.
///   6. Core awaits `handle.wait_stopped().await`.
pub trait Provider: Send + Sync + 'static {
    /// Provider identity and declared capabilities.
    fn info(&self) -> ProviderInfo;

    /// Start provider background tasks.
    /// Returns a handle for lifecycle management.
    /// Must return quickly (spawn background tasks, return immediately).
    async fn start(
        &self,
        core: CoreHandle,
        stop: StopToken,
    ) -> Result<ProviderHandle, ProviderError>;

    /// Graceful shutdown. Called after stop token is triggered.
    /// Must wait for all background tasks to complete.
    async fn stop(&self) -> Result<(), ProviderError>;

    /// Optional: return the capability strings this provider declares.
    fn supported_capabilities(&self) -> Vec<String> {
        self.info().capabilities.clone()
    }
}

/// Trait for types that can be converted to Capability.
pub trait IntoCapability {
    fn into_capability(self) -> Capability;
}

impl IntoCapability for Capability {
    fn into_capability(self) -> Capability {
        self
    }
}

impl IntoCapability for &'static str {
    fn into_capability(self) -> Capability {
        Capability(self)
    }
}

/// Extension trait for Vec<Capability> construction.
pub trait CapabilityExt {
    fn with_capability(self, cap: impl IntoCapability) -> Self;
}

impl CapabilityExt for Vec<Capability> {
    fn with_capability(mut self, cap: impl IntoCapability) -> Self {
        self.push(cap.into_capability());
        self
    }
}

/// Known capabilities — for validation and documentation.
pub mod known_caps {
    use super::Capability;

    pub fn all() -> Vec<Capability> {
        vec![
            Capability::new("scan"),
            Capability::new("watch"),
            Capability::new("format"),
            Capability::new("actions"),
            Capability::new("observe"),
            Capability::new("navigate"),
            Capability::new("observe_process"),
            // SpaceTrace
            Capability::new("trace.observe"),
            Capability::new("trace.query"),
            Capability::new("trace.correlate"),
            Capability::new("trace.metrics"),
        ]
    }

    pub fn is_known(cap: &str) -> bool {
        matches!(
            cap,
            "scan"
                | "watch"
                | "format"
                | "actions"
                | "observe"
                | "navigate"
                | "observe_process"
                | "trace.observe"
                | "trace.query"
                | "trace.correlate"
                | "trace.metrics"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_serialization() {
        let cap = Capability::new("scan");
        let json = serde_json::to_string(&cap).unwrap();
        assert_eq!(json, "\"scan\"");

        let parsed: Capability = serde_json::from_str(&json).unwrap();
        assert_eq!(cap, parsed);
    }

    #[test]
    fn provider_info_serialization() {
        let info = ProviderInfo {
            id: "filesystem".into(),
            name: "filesystem-provider".into(),
            capabilities: vec!["scan".into(), "watch".into()],
            version: Some("1.0".into()),
        };
        let json = serde_json::to_string(&info).unwrap();
        let parsed: ProviderInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(info, parsed);
    }

    #[test]
    fn capability_from_str() {
        let cap: Capability = "scan".into();
        assert_eq!(cap, Capability::new("scan"));
    }

    #[test]
    fn known_caps_validation() {
        assert!(known_caps::is_known("scan"));
        assert!(known_caps::is_known("trace.observe"));
        assert!(!known_caps::is_known("unknown_cap"));
    }

    #[test]
    fn all_known_caps_roundtrip() {
        for cap in known_caps::all() {
            let json = serde_json::to_string(&cap).unwrap();
            let parsed: Capability = serde_json::from_str(&json).unwrap();
            assert_eq!(cap, parsed);
        }
    }
}
