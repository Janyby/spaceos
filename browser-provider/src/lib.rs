//! Browser Provider — CDP-based observation and action provider for SpaceOS.
//!
//! This provider connects to a local Chromium/Chrome/Edge instance via the
//! Chrome DevTools Protocol (CDP) and exposes browser tabs, pages, and
//! navigation as SpaceOS objects, events, and actions.

pub mod cdp;
pub mod ipc;
pub mod objects;
pub mod provider;

pub use cdp::{fetch_targets, CdpClient, CdpTarget, DEFAULT_CDP_BASE};
pub use objects::{page_object_type, tab_object_type, BrowserPageObject, BrowserTabObject};
pub use provider::{BrowserProvider, BrowserProviderConfig};
