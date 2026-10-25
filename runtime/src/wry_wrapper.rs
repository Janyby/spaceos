//! Minimal Wry wrapper for SpaceOS — vertical slice (real native window).
//!
//! Feature gate (verified 2026-10-18 by actual `cargo check`, both modes):
//! - With `wry-wrapper`: uses `winit` 0.30 + `wry` 0.45 (optional workspace deps).
//! - Without `wry-wrapper`: only `load_static_fallback()` is compiled; `main.rs`
//!   calls it for the `wry` command and prints the static fallback instead.
//!
//! Verified API facts (from `cargo check -p runtime --features wry-wrapper`):
//! - `winit` 0.30 API: `EventLoop::run_app(&mut impl ApplicationHandler)`;
//!   `WindowBuilder` was removed in 0.30 in favor of `WindowAttributes`
//!   (`Window::default_attributes()`), windows are created via
//!   `ActiveEventLoop::create_window(...)`.
//! - `wry` 0.45 API: `wry::WebViewBuilder::new(&window).with_url(...).build()`.
//!
//! Design rules (from MIGRATION_AUDIT_PLAN.md):
//! - Does NOT block Tokio runtime: Wry event loop runs in `std::thread::spawn`.
//! - Does NOT create second source of truth: loads existing `runtime/static/index.html`
//!   via `http://127.0.0.1:{port}/` (existing `ui::run()` server).
//! - Keeps existing CLI commands intact; never writes directly to SQLite.

#[cfg(feature = "wry-wrapper")]
use winit::{
    application::ApplicationHandler,
    event::WindowEvent,
    event_loop::{ActiveEventLoop, ControlFlow, EventLoop},
    window::Window,
};

/// Minimal app handler for the native window. Both the window AND the
/// WebView are kept alive here: dropping the `wry::WebView` destroys the
/// view (it is not just a handle), which leaves a blank window.
#[cfg(feature = "wry-wrapper")]
struct WryApp {
    url: String,
    window: Option<Window>,
    webview: Option<wry::WebView>,
}

#[cfg(feature = "wry-wrapper")]
impl ApplicationHandler for WryApp {
    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        tracing::info!("wry_wrapper: creating native window (main thread)");
        let attrs = Window::default_attributes()
            .with_title("SpaceOS V1")
            .with_inner_size(winit::dpi::LogicalSize::new(1024.0, 768.0));
        let window = match event_loop.create_window(attrs) {
            Ok(w) => w,
            Err(e) => {
                tracing::error!("wry_wrapper: create_window failed: {}", e);
                event_loop.exit();
                return;
            }
        };
        let url = self.url.clone();
        let webview = match wry::WebViewBuilder::new(&window).with_url(&url).build() {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("wry_wrapper: WebViewBuilder failed: {}", e);
                event_loop.exit();
                return;
            }
        };
        tracing::info!("wry_wrapper: native WebView loaded (URL: {})", url);
        // Keep BOTH alive for the whole loop; the WebView owns the view.
        self.window = Some(window);
        self.webview = Some(webview);
        // Event-driven wait instead of a busy poll loop: a client window must
        // not burn a CPU core.
        event_loop.set_control_flow(ControlFlow::Wait);
    }

    fn window_event(
        &mut self,
        event_loop: &ActiveEventLoop,
        _window_id: winit::window::WindowId,
        event: WindowEvent,
    ) {
        match event {
            WindowEvent::CloseRequested => {
                tracing::info!("wry_wrapper: close requested, exiting event loop");
                event_loop.exit();
            }
            WindowEvent::Destroyed => {
                tracing::info!("wry_wrapper: window destroyed, exiting event loop");
                event_loop.exit();
            }
            _ => {}
        }
    }
}

/// Run the native window event loop ON THE CALLER'S THREAD and return only
/// when the window closes.
///
/// The caller must be the OS main thread: winit 0.30 panics ("Initializing
/// the event loop outside of the main thread is a significant cross-platform
/// compatibility hazard") when the loop is created on any other thread, and
/// `#[tokio::main]`'s worker thread is not the main thread (see `main.rs`).
///
/// - Creates a `winit::EventLoop` and a `winit::Window` (1024x768, title
///   "SpaceOS V1") via `Window::default_attributes()`.
/// - Creates a `wry::WebViewBuilder` pointing at `url`: the live Dashboard
///   of an external stack, or the bundled runtime UI -- the caller
///   (`main.rs`) decides and logs which one.
/// - Clean shutdown on `WindowEvent::CloseRequested` / `Destroyed`.
#[cfg(feature = "wry-wrapper")]
pub fn run_wry_window_blocking(url: String) -> std::result::Result<(), String> {
    tracing::info!("wry_wrapper: launching native window at {}", url);
    let event_loop = EventLoop::new().map_err(|e| format!("EventLoop::new() failed: {e}"))?;
    let mut app = WryApp {
        url,
        window: None,
        webview: None,
    };
    event_loop
        .run_app(&mut app)
        .map_err(|e| format!("event loop error: {e}"))
}

/// Load the existing static HTML file (`runtime/static/index.html`).
/// This does NOT create a second source of truth; it reads the existing
/// `runtime/src/ui.rs` served assets.
///
/// Unused only when the `wry-wrapper` feature is on (the real window path is
/// used instead); kept unconditional because `main.rs` calls it in the
/// no-feature `wry` command path.
#[cfg_attr(feature = "wry-wrapper", allow(dead_code))]
pub fn load_static_fallback() -> String {
    std::fs::read_to_string("C:\\spaceos\\runtime\\static\\index.html")
        .unwrap_or_else(|_| {
            "<html><head><title>SpaceOS</title></head>\
             <body><h2>SpaceOS UI not available</h2></body></html>".into()
        })
}
