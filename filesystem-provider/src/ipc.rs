use crate::{actions, browser, metadata, observation, roots::RootsPolicy, scanner::Scanner};
use anyhow::Result;
use serde_json::{json, Value};
use space_core::{ipc, ActionRequest, ActionResult, ActionStatus, Event, ProviderInfo};
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// Second provider identity: the process observer shares this daemon but
/// registers and reports as its own source (`provider_id = "process"`).
/// Observation-only: capabilities advertise watching, never actions.
pub fn process_provider_info() -> ProviderInfo {
    ProviderInfo {
        id: "process".into(),
        name: "process-observer".into(),
        capabilities: vec!["observe".into()],
        version: None,
    }
}

/// Third provider identity: browser observation shares this daemon but
/// registers and reports as its own source (`provider_id = "browser"`).
/// Observation-only over CDP: capabilities advertise watching, never actions.
///
/// Kept for backward compatibility. The dedicated `browser-provider` daemon
/// is the single source of truth for id "browser" (capabilities
/// ["observe", "navigate"]); this in-daemon observer is therefore OPT-IN
/// via `SPACEOS_BROWSER_OBSERVER=1` and stays off by default so the two
/// daemons never flap the same registration or double-emit observations.
pub fn browser_provider_info() -> ProviderInfo {
    ProviderInfo {
        id: browser::BROWSER_PROVIDER_ID.into(),
        name: "browser-observer".into(),
        capabilities: vec!["observe".into()],
        version: None,
    }
}

/// Whether the legacy in-daemon CDP observer is enabled (opt-in).
fn browser_observer_enabled() -> bool {
    std::env::var("SPACEOS_BROWSER_OBSERVER")
        .map(|v| v == "1")
        .unwrap_or(false)
}

/// Poll interval of the production process observer (tests pass their own).
pub const PROCESS_POLL_MS: u64 = 2000;

pub async fn run_server(provider_id: String, root: Option<std::path::PathBuf>) -> Result<()> {
    // Register with Core once available: the filesystem provider plus the
    // process and browser observers (observation-only sources in this daemon).
    let info = ProviderInfo {
        id: provider_id.clone(),
        name: "filesystem-provider".into(),
        capabilities: vec![
            "scan".into(),
            "watch".into(),
            "format".into(),
            "actions".into(),
        ],
        version: None,
    };
    let proc_info = process_provider_info();
    let browser_info = browser_provider_info();
    let want_browser = browser_observer_enabled();
    let register_id = provider_id.clone();
    tokio::spawn(async move {
        for _ in 0..60 {
            let r = ipc::send_request(ipc::CORE_PIPE, "register_provider", json!(info)).await;
            let rp = ipc::send_request(ipc::CORE_PIPE, "register_provider", json!(proc_info)).await;
            let rb_ok = if want_browser {
                ipc::send_request(ipc::CORE_PIPE, "register_provider", json!(browser_info))
                    .await
                    .is_ok()
            } else {
                true
            };
            if r.is_ok() && rp.is_ok() && rb_ok {
                tracing::info!(
                    "registered with core as {} (+process{})",
                    register_id,
                    if want_browser {
                        ", +browser observer (opt-in)"
                    } else {
                        ""
                    }
                );
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        tracing::warn!("could not register with core");
    });

    // Process observer: same daemon lifecycle as the watcher below
    // (aborting run_server aborts this task too). Needs no FS root.
    tokio::spawn(async move {
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
        if let Err(e) = observation::watch_processes_on(
            "process".into(),
            None,
            PROCESS_POLL_MS,
            ipc::CORE_PIPE,
            rx,
        )
        .await
        {
            tracing::warn!("process observer error: {e}");
        }
    });

    // Browser observer (CDP, observation-only): same daemon lifecycle.
    // Degrades gracefully when no debuggable browser is present.
    // Opt-in only (SPACEOS_BROWSER_OBSERVER=1): the dedicated
    // browser-provider daemon owns id "browser" by default.
    if browser_observer_enabled() {
        tokio::spawn(async move {
            let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
            if let Err(e) = browser::watch_cdp(
                browser::DEFAULT_CDP_BASE,
                browser::BROWSER_PROVIDER_ID.into(),
                ipc::CORE_PIPE,
                rx,
            )
            .await
            {
                tracing::warn!("browser observer error: {e}");
            }
        });
    }

    // One long-lived Scanner shared by the initial scan and the "scan"
    // endpoint: uncertainty evidence (consecutive scan-misses) must
    // accumulate across scans. A fresh Scanner per request would reset
    // the ledger every time and make Phase 1 semantics test-only fiction.
    // Scanner is Sync-safe (short std::sync::Mutex guards, never held
    // across await) so Arc sharing is sufficient, no async mutex needed.
    let scanner = std::sync::Arc::new(Scanner::new(provider_id.clone()));
    // Effective roots: explicit SPACEOS_ALLOWED_ROOTS plus this daemon's
    // own serve root (if any). Unset env = legacy unrestricted mode.
    let roots = Arc::new(crate::roots::effective_roots(root.as_deref()));
    tracing::info!(
        "filesystem allowed roots: {}",
        crate::roots::describe(&roots)
    );
    if let Some(root) = root {
        let root1 = root.clone();
        let scanner_initial = scanner.clone();
        tokio::spawn(async move {
            match scanner_initial.scan(&root1).await {
                Ok(stats) => tracing::info!("initial scan: {stats}"),
                Err(e) => tracing::warn!("initial scan failed: {e}"),
            }
        });
        let root2 = root.clone();
        let scanner_watch = scanner.clone();
        tokio::spawn(async move {
            let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
            if let Err(e) = crate::watcher::watch(&root2, scanner_watch, rx).await {
                tracing::warn!("watcher error: {e}");
            }
        });
    }

    #[cfg(windows)]
    {
        use tokio::net::windows::named_pipe::ServerOptions;
        // Same concurrent-accept treatment as Core (Breakage B3):
        // several pending instances absorb synchronized connect bursts.
        // KillOnDrop: aborting run_server stops the detached loops too,
        // so a restarted server never races stale loops on this pipe.
        struct KillOnDrop {
            n: std::sync::Arc<tokio::sync::Notify>,
        }
        impl Drop for KillOnDrop {
            fn drop(&mut self) {
                self.n.notify_waiters();
            }
        }
        let notify = std::sync::Arc::new(tokio::sync::Notify::new());
        let _guard = KillOnDrop { n: notify.clone() };
        for _ in 0..space_core::ipc::ACCEPT_LOOPS {
            let scanner = scanner.clone();
            let roots = roots.clone();
            let n = notify.clone();
            tokio::spawn(async move {
                loop {
                    let server = match ServerOptions::new().create(ipc::PROVIDER_PIPE) {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::warn!("provider accept create error: {e}");
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            continue;
                        }
                    };
                    tokio::select! {
                        _ = n.notified() => break,
                        r = server.connect() => {
                            if r.is_err() {
                                continue;
                            }
                        }
                    }
                    let scanner = scanner.clone();
                    let roots = roots.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle(server, scanner, roots).await {
                            tracing::warn!("provider connection error: {e}");
                        }
                    });
                }
            });
        }
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        }
    }
    #[cfg(not(windows))]
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:7778").await?;
        loop {
            let (stream, _) = listener.accept().await?;
            let scanner = scanner.clone();
            let roots = roots.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_tcp(stream, scanner, roots).await {
                    tracing::warn!("provider connection error: {e}");
                }
            });
        }
    }
}

#[cfg(windows)]
async fn handle(
    server: tokio::net::windows::named_pipe::NamedPipeServer,
    scanner: std::sync::Arc<Scanner>,
    roots: Arc<RootsPolicy>,
) -> Result<()> {
    let (reader, mut writer) = tokio::io::split(server);
    serve_lines(reader, &mut writer, scanner, roots).await
}

#[cfg(not(windows))]
async fn handle_tcp(
    stream: tokio::net::TcpStream,
    scanner: std::sync::Arc<Scanner>,
    roots: Arc<RootsPolicy>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    serve_lines(reader, &mut writer, scanner, roots).await
}

/// Publish a provider-side terminal event to Core, threading the caller's
/// agent token so the `record_event` policy gate authenticates the
/// requesting actor (same binding agent-demo uses; empty for core actors).
async fn report_event(ev: Event, actor_token: &str) {
    let mut v = json!(ev);
    v["actor_token"] = json!(actor_token);
    let _ = ipc::send_request(ipc::CORE_PIPE, "record_event", v).await;
}

/// Gate action targets against allowed roots (server-side).
/// Unknown actions and missing fields fall through to `perform` (or the
/// scan job) for their own explicit errors.
fn check_action_paths(policy: &RootsPolicy, action: &str, target: &Value) -> Result<(), String> {
    let one = |raw: Option<&str>, for_new: bool| -> Result<(), String> {
        match raw {
            None => Ok(()),
            Some(p) => {
                let r = if for_new {
                    crate::roots::check_new_path(policy, p)
                } else {
                    crate::roots::check_path(policy, p)
                };
                r.map(|_| ()).map_err(|e| e.to_string())
            }
        }
    };
    match action {
        "create" => one(target["path"].as_str(), true),
        "read" | "write" | "delete" | "scan" => one(target["path"].as_str(), false),
        "rename" | "move" | "copy" => {
            one(target["from"].as_str(), false)?;
            one(target["to"].as_str(), true)
        }
        _ => Ok(()),
    }
}

async fn serve_lines<R, W>(
    reader: R,
    writer: &mut W,
    scanner: std::sync::Arc<Scanner>,
    roots: Arc<RootsPolicy>,
) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await? {
        let req: Value = serde_json::from_str(&line)?;
        let method = req["method"].as_str().unwrap_or("");
        let params = req.get("params").cloned().unwrap_or(json!({}));
        let resp = match method {
            "ping" => json!({"ok": true, "result": {"pong": true}}),
            "action" => {
                let req: ActionRequest = serde_json::from_value(params)?;
                match check_action_paths(&roots, &req.action, &req.target) {
                    Err(e) => json!({"ok": true, "result": ActionResult {
                        action_id: req.id, status: ActionStatus::Failed, detail: json!({"error": e}),
                    }}),
                    Ok(()) => {
                        // Long-running scan runs as an async job: answer Started
                        // immediately (bridge/Core have bounded waits) and publish
                        // the terminal ActionCompleted/ActionFailed event with the
                        // same action id as correlation_id when the scan lands.
                        if req.action == "scan" {
                            let scan_id = format!("scan-{}", uuid::Uuid::new_v4());
                            let path = req.target["path"].as_str().unwrap_or("").to_string();
                            let scan_path = path.clone();
                            let scanner_bg = scanner.clone();
                            let action_id = req.id.clone();
                            let actor = req.actor.clone();
                            // Thread the caller's agent token so the terminal event
                            // passes the Core policy gate as the requesting actor
                            // (same binding agent-demo uses; empty for core actors).
                            let actor_token = req.actor_token.clone();
                            let provider = scanner_bg.provider_id.clone();
                            let job = scan_id.clone();
                            tokio::spawn(async move {
                                let started = std::time::Instant::now();
                                match scanner_bg.scan(std::path::Path::new(&scan_path)).await {
                                    Ok(stats) => {
                                        let mut done = Event::new(
                                            "ActionCompleted",
                                            actor.clone(),
                                            json!({"job": job, "stats": stats}),
                                        );
                                        done.provider_id = Some(provider.clone());
                                        done.correlation_id = Some(action_id.clone());
                                        report_event(done, &actor_token).await;
                                        spacetrace::hooks::global_hooks().provider_action_complete(
                                            &action_id,
                                            &provider,
                                            "scan",
                                            "Completed",
                                            started.elapsed().as_millis() as u64,
                                        );
                                    }
                                    Err(e) => {
                                        let mut failed = Event::new(
                                            "ActionFailed",
                                            actor.clone(),
                                            json!({"job": job, "error": e.to_string()}),
                                        );
                                        failed.provider_id = Some(provider.clone());
                                        failed.correlation_id = Some(action_id.clone());
                                        report_event(failed, &actor_token).await;
                                        spacetrace::hooks::global_hooks().provider_action_complete(
                                            &action_id,
                                            &provider,
                                            "scan",
                                            "Failed",
                                            started.elapsed().as_millis() as u64,
                                        );
                                    }
                                }
                            });
                            json!({"ok": true, "result": ActionResult {
                                action_id: req.id, status: ActionStatus::Started, detail: json!({"job": scan_id, "path": path}),
                            }})
                        } else {
                            match actions::perform(&req.action, &req.target).await {
                                Ok(detail) => json!({"ok": true, "result": ActionResult {
                                    action_id: req.id, status: ActionStatus::Completed, detail,
                                }}),
                                Err(e) => json!({"ok": true, "result": ActionResult {
                                    action_id: req.id, status: ActionStatus::Failed, detail: json!({"error": e.to_string()}),
                                }}),
                            }
                        }
                    }
                }
            }
            "scan" => {
                let path = params["path"].as_str().unwrap_or("");
                match crate::roots::check_path(&roots, path) {
                    Err(e) => json!({"ok": false, "error": e.to_string()}),
                    Ok(_) => match scanner.scan(Path::new(path)).await {
                        Ok(stats) => json!({"ok": true, "result": stats}),
                        Err(e) => json!({"ok": false, "error": e.to_string()}),
                    },
                }
            }
            "report_observation" => {
                // Forward to Core: providers never mutate domain tables
                // directly. Core validates + applies atomically.
                // Actor binding: this daemon reports as the "filesystem"
                // core principal (same precedent as watcher/scanner rows,
                // whose provider_id carries attribution); envelope auth
                // proves same-machine origin.
                match serde_json::from_value::<space_core::Observation>(params.clone()) {
                    Ok(obs) => {
                        let fwd = json!({
                            "actor": "filesystem",
                            "observation_id": obs.observation_id,
                            "world_id": obs.world_id,
                            "facts": obs.facts,
                            "objects": obs.objects,
                            "relations": obs.relations,
                            "event": obs.event,
                        });
                        match ipc::send_request(ipc::CORE_PIPE, "report_observation", fwd).await {
                            Ok(resp) => resp,
                            Err(e) => json!({"ok": false, "error": e.to_string()}),
                        }
                    }
                    Err(e) => json!({"ok": false, "error": format!("invalid observation: {e}")}),
                }
            }
            "roots" => {
                json!({"ok": true, "result": crate::roots::describe(&roots)})
            }
            "stat" => {
                let path = params["path"].as_str().unwrap_or("");
                json!({"ok": true, "result": metadata::stat(Path::new(path))})
            }
            other => json!({"ok": false, "error": format!("unknown method {other}")}),
        };
        let mut s = serde_json::to_string(&resp)?;
        s.push('\n');
        writer.write_all(s.as_bytes()).await?;
        writer.flush().await?;
    }
    Ok(())
}
