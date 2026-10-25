use crate::{actions, browser, observation, roots::RootsPolicy, scanner::Scanner};
use anyhow::Result;
use serde_json::{json, Value};
use space_core::dispatch::{DispatchProof, DispatchVerifier};
use space_core::{ipc, action::{ActionCompletion, DispatchRequest}, ActionResult, ActionStatus, ProviderInfo};
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
    anyhow::ensure!(provider_id == "filesystem", "unsupported filesystem Provider identity");
    // This is the existing Core transport credential used by Core dispatch
    // and privileged provider registration. Never include it in responses.
    let transport_token = ipc::core_token();
    ipc::provider_principal_credential()?;
    let (instance_id, instance_token) = ipc::provider_instance_credential()?;
    let core_pipe = ipc::configured_provider_pipe("SPACEOS_CORE_PIPE", ipc::CORE_PIPE)?;
    let provider_pipe = ipc::configured_provider_pipe("SPACEOS_FILESYSTEM_PROVIDER_PIPE", ipc::PROVIDER_PIPE)?;
    let dispatch_key = space_core::dispatch::key_from_env("filesystem").map_err(anyhow::Error::msg)?;
    anyhow::ensure!(dispatch_key != transport_token, "filesystem dispatch credential conflicts with Core transport credential");
    let dispatch = Arc::new(DispatchVerifier::new("filesystem", Some(&dispatch_key)));
    if let Some(lab_root) = std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT") {
        let root = root.as_ref().ok_or_else(|| anyhow::anyhow!("isolated filesystem root required"))?;
        let allowed = std::fs::canonicalize(std::path::PathBuf::from(lab_root).join("test-tmp"))?;
        let actual = std::fs::canonicalize(root)?;
        anyhow::ensure!(actual.starts_with(&allowed) && actual != allowed, "unsafe isolated filesystem root");
    }
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
    anyhow::ensure!(
        !want_browser,
        "in-daemon browser observer requires a separate browser principal; use the dedicated Browser Provider"
    );
    let register_id = provider_id.clone();
    let registration_core_pipe = core_pipe.clone();
    // Register before any scanner/watcher task can emit an observation.
    // Core rejects observations from an unregistered Provider, so starting
    // observers first would create a startup race and lose the initial scan.
    let mut registered = false;
    for _ in 0..60 {
        let r = ipc::send_provider_principal_request(&registration_core_pipe, "filesystem", "register_provider", json!(info)).await;
        let rp = ipc::send_provider_principal_request(&registration_core_pipe, "filesystem", "register_provider", json!(proc_info)).await;
        let ri = if r.is_ok() {
            ipc::send_provider_principal_request(&registration_core_pipe, "filesystem", "register_provider_instance",
                json!({"actor":"filesystem", "instance_id":instance_id, "instance_token":instance_token})).await
        } else {
            Err(anyhow::anyhow!("filesystem Provider type not registered"))
        };
        let rb_ok = if want_browser {
            ipc::send_provider_principal_request(&registration_core_pipe, "browser", "register_provider", json!(browser_info))
                .await
                .is_ok()
        } else {
            true
        };
        if r.is_ok() && rp.is_ok() && ri.is_ok() && rb_ok {
            tracing::info!(
                "registered with core as {} (+process{})",
                register_id,
                if want_browser {
                    ", +browser observer (opt-in)"
                } else {
                    ""
                }
            );
            registered = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    anyhow::ensure!(registered, "could not register filesystem Provider with Core");

    // Core registration is ephemeral: refresh the same authenticated instance
    // after a Core restart. Core's immutable startup bindings prevent a
    // re-registration request from changing sources or Worlds.
    let refresh_pipe = core_pipe.clone();
    let refresh_id = instance_id.clone();
    let refresh_token = instance_token.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let _ = ipc::send_provider_principal_request(&refresh_pipe, "filesystem", "register_provider_instance",
                json!({"actor":"filesystem", "instance_id":refresh_id, "instance_token":refresh_token})).await;
        }
    });

    // Process observer: same daemon lifecycle as the watcher below
    // (aborting run_server aborts this task too). Needs no FS root.
    let process_core_pipe = core_pipe.clone();
    let skip_process_observer = std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some()
        && std::env::var("SPACEOS_FIXTURE_DISABLE_PROCESS_OBSERVER").ok().as_deref() == Some("1");
    if !skip_process_observer {
    tokio::spawn(async move {
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
        if let Err(e) = observation::watch_processes_on(
            "process".into(),
            None,
            PROCESS_POLL_MS,
            &process_core_pipe,
            rx,
        )
        .await
        {
            tracing::warn!("process observer error: {e}");
        }
    });
    }

    // Browser observer (CDP, observation-only): same daemon lifecycle.
    // Degrades gracefully when no debuggable browser is present.
    // Opt-in only (SPACEOS_BROWSER_OBSERVER=1): the dedicated
    // browser-provider daemon owns id "browser" by default.
    if browser_observer_enabled() {
        let browser_core_pipe = core_pipe.clone();
        tokio::spawn(async move {
            let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
            if let Err(e) = browser::watch_cdp(
                browser::DEFAULT_CDP_BASE,
                browser::BROWSER_PROVIDER_ID.into(),
                &browser_core_pipe,
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
        let initial_core_pipe = core_pipe.clone();
        tokio::spawn(async move {
            match scanner_initial.scan_on(&root1, &initial_core_pipe).await {
                Ok(stats) => tracing::info!("initial scan: {stats}"),
                Err(e) => tracing::warn!("initial scan failed: {e}"),
            }
        });
        let root2 = root.clone();
        let scanner_watch = scanner.clone();
        let watcher_core_pipe = core_pipe.clone();
        tokio::spawn(async move {
            let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
            if let Err(e) = crate::watcher::watch_on(&root2, scanner_watch, &watcher_core_pipe, rx).await {
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
            let transport_token = transport_token.clone();
            let dispatch = dispatch.clone();
            let n = notify.clone();
            let provider_pipe = provider_pipe.clone();
            tokio::spawn(async move {
                loop {
                    let server = match ServerOptions::new()
                        .reject_remote_clients(true)
                        .create(&provider_pipe)
                    {
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
                    let transport_token = transport_token.clone();
                    let dispatch = dispatch.clone();
                    tokio::spawn(async move {
                        if let Err(e) =
                            handle(server, scanner, roots, &transport_token, dispatch).await
                        {
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
            let transport_token = transport_token.clone();
            let dispatch = dispatch.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_tcp(stream, scanner, roots, &transport_token, dispatch).await
                {
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
    transport_token: &str,
    dispatch: Arc<DispatchVerifier>,
) -> Result<()> {
    let (reader, mut writer) = tokio::io::split(server);
    serve_lines(
        reader,
        &mut writer,
        scanner,
        roots,
        transport_token,
        dispatch,
    )
    .await
}

#[cfg(not(windows))]
async fn handle_tcp(
    stream: tokio::net::TcpStream,
    scanner: std::sync::Arc<Scanner>,
    roots: Arc<RootsPolicy>,
    transport_token: &str,
    dispatch: Arc<DispatchVerifier>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    serve_lines(
        reader,
        &mut writer,
        scanner,
        roots,
        transport_token,
        dispatch,
    )
    .await
}

fn transport_auth_ok(params: &Value, expected: &str) -> bool {
    params.get("auth").and_then(Value::as_str) == Some(expected)
}

/// Retry only this identical completion, never the external job. Lost responses
/// are resolved by Core's durable terminal digest, not another success event.
async fn report_completion(completion: ActionCompletion, instance: String, instance_token: String) -> Result<()> {
    let core_pipe=ipc::configured_provider_pipe("SPACEOS_CORE_PIPE",ipc::CORE_PIPE)?;
    for _ in 0..60 {
        // Reconstruct ephemeral registration after Core restart using the same
        // independently authenticated instance and existing immutable binding.
        let call=async {
            ipc::send_provider_principal_request(&core_pipe,"filesystem","register_provider_instance",
                json!({"actor":"filesystem","instance_id":instance,"instance_token":instance_token})).await?;
            // Keep the structured retryable response: the generic helper turns
            // every Core rejection into Err and would hide permanent denials.
            ipc::send_request(&core_pipe,"complete_provider_action",
                json!({"actor":"filesystem","principal_token":ipc::provider_principal_credential()?,
                    "instance_id":instance,"instance_token":instance_token,"completion":completion})).await
        };
        match tokio::time::timeout(std::time::Duration::from_secs(5),call).await {
            Ok(Ok(response)) if response["ok"]==true => return Ok(()),
            Ok(Ok(response)) if response["retryable"]!=true => anyhow::bail!("async completion rejected by Core"),
            _ => {}
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    anyhow::bail!("async completion remains unconfirmed after bounded reporting retries")
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
        "read" | "write" | "delete" | "scan" | "stat" => one(target["path"].as_str(), false),
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
    transport_token: &str,
    dispatch: Arc<DispatchVerifier>,
) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await? {
        let req: Value = match serde_json::from_str(&line) {
            Ok(Value::Object(map)) => Value::Object(map),
            _ => {
                writer
                    .write_all(b"{\"ok\":false,\"error\":\"invalid request\"}\n")
                    .await?;
                writer.flush().await?;
                continue;
            }
        };
        let method = req["method"].as_str().unwrap_or("");
        let params = req.get("params").cloned().unwrap_or(json!({}));
        let resp = if method != "ping" && !transport_auth_ok(&params, transport_token) {
            json!({"ok": false, "error": "denied: provider authentication required"})
        } else {
            match method {
                "ping" => {
                    json!({"ok": true, "result": {"pong": true, "instance_id": dispatch.instance_id(),"dispatch_protocol":2,
                        "observation_instance":ipc::provider_instance_credential().ok().map(|v|v.0)}})
                }
                "action" => {
                    match DispatchRequest::from_provider_params(&params) {
                        Err(_) => json!({"ok": false, "error": "invalid action request"}),
                        Ok(req) => {
                            let proof = serde_json::from_value::<DispatchProof>(
                                params["dispatch_proof"].clone(),
                            )
                            .ok();
                            match dispatch.verify(proof.as_ref(), &req) {
                                Err(error) => json!({"ok": false, "error": error}),
                                Ok(()) => {
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
                                                let scan_id = format!("scan-{}",req.completion.as_ref()
                                                    .ok_or_else(||anyhow::anyhow!("scan completion binding required"))?.binding_id);
                                                let path = req.target["path"]
                                                    .as_str()
                                                    .unwrap_or("")
                                                    .to_string();
                                                let scan_path = path.clone();
                                                let scanner_bg = scanner.clone();
                                                let action_id = req.id.clone();
                                                let binding=req.completion.clone().ok_or_else(||anyhow::anyhow!("scan completion binding required"))?;
                                                let (instance,instance_token)=ipc::provider_instance_credential()?;
                                                anyhow::ensure!(instance==binding.instance_id,"scan instance mismatch");
                                                let performer=dispatch.instance_id().to_owned();
                                                let outcome_id=uuid::Uuid::new_v4().to_string();
                                                let provider = scanner_bg.provider_id.clone();
                                                let job = scan_id.clone();
                                                tokio::spawn(async move {
                                                    let started = std::time::Instant::now();
                                                    match scanner_bg
                                                        .scan(std::path::Path::new(&scan_path))
                                                        .await
                                                    {
                                                        Ok(stats) => {
                                                            let done=ActionCompletion {action_id:action_id.clone(),binding_id:binding.binding_id.clone(),performer:performer.clone(),outcome_id:outcome_id.clone(),
                                                                status:ActionStatus::Completed,detail:json!({"job":job,"stats":stats})};
                                                            if report_completion(done,instance.clone(),instance_token.clone()).await.is_err() {
                                                                tracing::warn!(action_id=%action_id,"scan executed; durable completion unconfirmed");
                                                            }
                                                            spacetrace::hooks::global_hooks()
                                                                .provider_action_complete(
                                                                    &action_id,
                                                                    &provider,
                                                                    "scan",
                                                                    "Completed",
                                                                    started.elapsed().as_millis()
                                                                        as u64,
                                                                );
                                                        }
                                                        Err(e) => {
                                                            let failed=ActionCompletion {action_id:action_id.clone(),binding_id:binding.binding_id.clone(),performer:performer.clone(),outcome_id:outcome_id.clone(),
                                                                status:ActionStatus::Failed,detail:json!({"job":job,"error":e.to_string()})};
                                                            if report_completion(failed,instance.clone(),instance_token.clone()).await.is_err() {
                                                                tracing::warn!(action_id=%action_id,"scan failed; durable completion unconfirmed");
                                                            }
                                                            spacetrace::hooks::global_hooks()
                                                                .provider_action_complete(
                                                                    &action_id,
                                                                    &provider,
                                                                    "scan",
                                                                    "Failed",
                                                                    started.elapsed().as_millis()
                                                                        as u64,
                                                                );
                                                        }
                                                    }
                                                });
                                                json!({"ok": true, "result": ActionResult {
                                                    action_id: req.id, status: ActionStatus::Started, detail: json!({"job": scan_id, "path": path}),
                                                }})
                                            } else {
                                                match actions::perform(&req.action, &req.target)
                                                    .await
                                                {
                                                    Ok(detail) => {
                                                        json!({"ok": true, "result": ActionResult {
                                                            action_id: req.id, status: ActionStatus::Completed, detail,
                                                        }})
                                                    }
                                                    Err(e) => {
                                                        json!({"ok": true, "result": ActionResult {
                                                            action_id: req.id, status: ActionStatus::Failed, detail: json!({"error": e.to_string()}),
                                                        }})
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                "scan" => {
                    json!({"ok": false, "error": "scan requires Core request_action dispatch"})
                }
                "report_observation" => {
                    json!({"ok": false, "error": "provider observation proxy disabled; observer reports to Core directly"})
                }
                "roots" => {
                    json!({"ok": true, "result": crate::roots::describe(&roots)})
                }
                "stat" => {
                    json!({"ok": false, "error": "stat requires Core request_action dispatch"})
                }
                other => json!({"ok": false, "error": format!("unknown method {other}")}),
            }
        };
        let mut s = serde_json::to_string(&resp)?;
        s.push('\n');
        writer.write_all(s.as_bytes()).await?;
        writer.flush().await?;
    }
    Ok(())
}

#[cfg(all(test, windows))]
mod security_tests {
    use super::*;
    use space_core::principal::PrincipalRegistry;
    use space_core::{ActionRequest, Storage};
    use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};

    async fn fixture_request(pipe: &str, line: &str) -> Value {
        let mut delay = 5;
        let stream = loop {
            match ClientOptions::new().open(pipe) {
                Ok(stream) => break stream,
                Err(_) if delay <= 160 => {
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    delay *= 2;
                }
                Err(error) => panic!("isolated fixture pipe did not open: {error}"),
            }
        };
        let (reader, mut writer) = tokio::io::split(stream);
        writer.write_all(line.as_bytes()).await.unwrap();
        writer.write_all(b"\n").await.unwrap();
        writer.flush().await.unwrap();
        let mut lines = BufReader::new(reader).lines();
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap()
    }

    async fn request(pipe: &str, method: &str, params: Value) -> Value {
        fixture_request(
            pipe,
            &json!({"method": method, "params": params}).to_string(),
        )
        .await
    }

    fn start_fixture(
        pipe: String,
        roots: Arc<RootsPolicy>,
        token: String,
        dispatch_key: String,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let scanner = Arc::new(Scanner::new("filesystem".to_string()));
            let dispatch = Arc::new(DispatchVerifier::new("filesystem", Some(&dispatch_key)));
            loop {
                let server = ServerOptions::new()
                    .reject_remote_clients(true)
                    .create(&pipe)
                    .unwrap();
                server.connect().await.unwrap();
                handle(
                    server,
                    scanner.clone(),
                    roots.clone(),
                    &token,
                    dispatch.clone(),
                )
                .await
                .unwrap();
            }
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn direct_pipe_denials_and_core_policy_dispatch() {
        let suffix = uuid::Uuid::new_v4();
        let base = std::env::temp_dir().join(format!("spaceos-4-3a-fs-{suffix}"));
        let allowed = base.join("allowed");
        let outside = base.join("outside");
        std::fs::create_dir_all(&allowed).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let read_path = allowed.join("read.txt");
        let outside_path = outside.join("secret.txt");
        std::fs::write(&read_path, "allowed fixture").unwrap();
        std::fs::write(&outside_path, "outside fixture").unwrap();
        let roots = Arc::new(RootsPolicy::Restricted(vec![std::fs::canonicalize(
            &allowed,
        )
        .unwrap()]));
        let token = uuid::Uuid::new_v4().to_string();
        std::env::set_var("SPACEOS_CORE_TOKEN", &token);
        let dispatch_key =
            base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, [7u8; 32]);
        std::env::set_var("SPACEOS_DISPATCH_KEY_FILESYSTEM", &dispatch_key);
        let provider_pipe = format!(r"\\.\pipe\spaceos-4-3a-fs-{suffix}");
        let core_pipe = format!(r"\\.\pipe\spaceos-4-3a-core-{suffix}");
        let provider = start_fixture(
            provider_pipe.clone(),
            roots.clone(),
            token.clone(),
            dispatch_key.clone(),
        );

        let ping = request(&provider_pipe, "ping", json!({})).await;
        assert_eq!(ping["ok"], true);
        let instance = ping["result"]["instance_id"].as_str().unwrap();
        let signed_read = ActionRequest::new(
            "read",
            json!({"path": read_path}),
            "filesystem",
            "test-agent",
        );
        let proof = space_core::dispatch::issue(&dispatch_key, instance, &signed_read).unwrap();
        let mut signed_params = json!(DispatchRequest::from(&signed_read));
        signed_params["auth"] = json!(token);
        signed_params["dispatch_proof"] = json!(proof);
        let signed_result = request(&provider_pipe, "action", signed_params.clone()).await;
        assert_eq!(
            signed_result["result"]["status"], "Completed",
            "{signed_result}"
        );
        assert_eq!(
            signed_result["result"]["detail"]["content"],
            "allowed fixture"
        );
        let replay = request(&provider_pipe, "action", signed_params.clone()).await;
        assert_eq!(replay["ok"], false, "{replay}");
        signed_params["target"]["path"] = json!(outside_path);
        let modified_path = request(&provider_pipe, "action", signed_params).await;
        assert_eq!(modified_path["ok"], false, "{modified_path}");
        let traversal = ActionRequest::new(
            "read",
            json!({"path": allowed.join("..").join("outside").join("secret.txt")}),
            "filesystem",
            "test-agent",
        );
        let traversal_proof =
            space_core::dispatch::issue(&dispatch_key, instance, &traversal).unwrap();
        let mut traversal_params = json!(DispatchRequest::from(&traversal));
        traversal_params["auth"] = json!(token);
        traversal_params["dispatch_proof"] = json!(traversal_proof);
        let traversal_result = request(&provider_pipe, "action", traversal_params).await;
        assert_eq!(
            traversal_result["result"]["status"], "Failed",
            "{traversal_result}"
        );
        for auth in [
            Value::Null,
            json!("wrong"),
            json!(7),
            json!({"token": token}),
        ] {
            let denied = request(
                &provider_pipe,
                "action",
                json!({
                    "id": "forged", "action": "read", "target": {"path": read_path},
                    "provider_id": "filesystem", "actor": "system", "auth": auth,
                }),
            )
            .await;
            assert_eq!(denied["ok"], false, "{denied}");
        }
        let denied_create = allowed.join("denied.txt");
        let direct = request(
            &provider_pipe,
            "action",
            json!({
                "id": "forged-create", "action": "create",
                "target": {"path": denied_create, "content": "no"},
                "provider_id": "filesystem", "actor": "system",
            }),
        )
        .await;
        assert_eq!(direct["ok"], false, "{direct}");
        assert!(!denied_create.exists());
        for method in ["scan", "roots", "stat", "report_observation"] {
            let denied = request(&provider_pipe, method, json!({"path": read_path})).await;
            assert_eq!(denied["ok"], false, "{method}: {denied}");
        }
        let malformed = fixture_request(&provider_pipe, "{not-json").await;
        assert_eq!(malformed["ok"], false);
        let malformed_action = request(&provider_pipe, "action", json!({"auth": token})).await;
        assert_eq!(malformed_action["ok"], false);

        let roots_response = request(&provider_pipe, "roots", json!({"auth": token})).await;
        assert_eq!(roots_response["ok"], true);
        let read = request(
            &provider_pipe,
            "action",
            json!({
                "id": "read", "action": "read", "target": {"path": read_path},
                "provider_id": "filesystem", "actor": "filesystem", "auth": token,
            }),
        )
        .await;
        assert_eq!(read["ok"], false, "{read}");
        let credential_holder_forged_actor = request(
            &provider_pipe,
            "action",
            json!({
                "id": "credential-holder", "action": "read", "target": {"path": read_path},
                "provider_id": "filesystem", "actor": "system", "auth": token,
            }),
        )
        .await;
        assert_eq!(credential_holder_forged_actor["ok"], false);
        let unauthorized_write = request(
            &provider_pipe,
            "action",
            json!({
                "id": "unauthorized-write", "action": "write",
                "target": {"path": read_path, "content": "tamper"},
                "provider_id": "filesystem", "actor": "system", "auth": token,
            }),
        )
        .await;
        assert_eq!(unauthorized_write["ok"], false, "{unauthorized_write}");
        assert_eq!(
            std::fs::read_to_string(&read_path).unwrap(),
            "allowed fixture"
        );
        let allowed_stat = request(
            &provider_pipe,
            "stat",
            json!({"path": read_path, "auth": token}),
        )
        .await;
        assert_eq!(allowed_stat["ok"], false, "{allowed_stat}");
        let outside_stat = request(
            &provider_pipe,
            "stat",
            json!({"path": outside_path, "auth": token}),
        )
        .await;
        assert_eq!(outside_stat["ok"], false, "{outside_stat}");
        let escaped = request(
            &provider_pipe,
            "action",
            json!({
                "id": "traversal", "action": "read",
                "target": {"path": allowed.join("..").join("outside").join("secret.txt")},
                "provider_id": "filesystem", "actor": "filesystem", "auth": token,
            }),
        )
        .await;
        assert_eq!(escaped["ok"], false, "{escaped}");

        let db = base.join("core.db");
        let privileged_token = base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            [11u8; 32],
        );
        let mut principals = PrincipalRegistry::default();
        principals
            .insert(
                "system",
                [11u8; 32],
                [
                    "provider.register",
                    "provider.register.filesystem",
                    "agent.register",
                    "agent.grant.action.create",
                    "agent.grant.action.stat",
                    "agent.grant.action.read",
                    "action.create",
                    "dispatch.filesystem.create",
                ],
                [],
            )
            .unwrap();
        let state = ipc::CoreState::new_with_semantic_world_and_provider_pipes_and_principals(
            Storage::open(&db).unwrap(),
            None,
            &provider_pipe,
            &format!(r"\\.\pipe\spaceos-4-3a-unused-{suffix}"),
            principals,
        );
        let core_state = state.clone();
        let core_path = core_pipe.clone();
        let core = tokio::spawn(async move {
            ipc::run_server(core_state, &core_path).await.unwrap();
        });
        let rejected_registration = fixture_request(
            &core_pipe,
            &json!({
                "method": "register_provider",
                "params": {
                    "id": "filesystem", "name": "fixture", "capabilities": ["actions"],
                    "auth": "invalid",
                },
            })
            .to_string(),
        )
        .await;
        assert_eq!(rejected_registration["ok"], false);
        let provider_registration = ipc::send_request(
            &core_pipe,
            "register_provider",
            json!({"id": "filesystem", "name": "fixture", "capabilities": ["actions"], "principal": "system", "principal_token": privileged_token}),
        )
        .await
        .unwrap();
        assert_eq!(provider_registration["ok"], true, "{provider_registration}");
        // The former generic-token-to-system bypass must now fail before Core
        // can sign a Provider dispatch.
        let core_principal_path = allowed.join("core-principal-limit.txt");
        let privileged = ActionRequest::new(
            "create",
            json!({"path": core_principal_path, "content": "fixture"}),
            "filesystem",
            "system",
        );
        let core_principal_result =
            ipc::send_request(&core_pipe, "request_action", json!(privileged))
                .await
                .unwrap();
        assert_eq!(
            core_principal_result["result"]["status"], "Denied",
            "{core_principal_result}"
        );
        assert!(!core_principal_path.exists());
        let mut forged = json!(privileged);
        forged["principal_token"] = json!("invalid");
        let invalid_privileged = ipc::send_request(&core_pipe, "request_action", forged)
            .await
            .unwrap();
        assert_eq!(invalid_privileged["result"]["status"], "Denied");
        let mut actor_token_conflict = json!(privileged);
        actor_token_conflict["actor_token"] = json!(privileged_token);
        let conflict = ipc::send_request(&core_pipe, "request_action", actor_token_conflict)
            .await
            .unwrap();
        assert_eq!(conflict["result"]["status"], "Denied");
        let mut authorized_system = json!(privileged);
        authorized_system["principal_token"] = json!(privileged_token);
        let authorized_system_result =
            ipc::send_request(&core_pipe, "request_action", authorized_system)
                .await
                .unwrap();
        assert_eq!(
            authorized_system_result["result"]["status"], "Completed",
            "{authorized_system_result}"
        );
        assert_eq!(
            std::fs::read_to_string(&core_principal_path).unwrap(),
            "fixture"
        );
        let registration = ipc::send_request(
            &core_pipe,
            "register_agent",
            json!({
                "principal": "system", "name": "test-agent",
                "capabilities": ["action.create", "action.stat"], "worlds": [], "principal_token": privileged_token,
            }),
        )
        .await
        .unwrap();
        assert_eq!(registration["ok"], true, "{registration}");
        let actor_token = registration["result"]["token"].as_str().unwrap();
        let limited_registration = ipc::send_request(&core_pipe, "register_agent", json!({
            "principal": "system", "name": "limited-agent", "capabilities": ["action.read"], "worlds": [], "principal_token": privileged_token,
        })).await.unwrap();
        assert_eq!(limited_registration["ok"], true);
        let denied_path = allowed.join("limited-denied.txt");
        let mut limited_action = ActionRequest::new(
            "create",
            json!({"path": denied_path, "content": "no"}),
            "filesystem",
            "limited-agent",
        );
        limited_action.actor_token = limited_registration["result"]["token"]
            .as_str()
            .unwrap()
            .to_string();
        let limited_result = ipc::send_request(&core_pipe, "request_action", json!(limited_action))
            .await
            .unwrap();
        assert_eq!(
            limited_result["result"]["status"], "Denied",
            "{limited_result}"
        );
        assert!(!denied_path.exists());
        let mut stat_action = ActionRequest::new(
            "stat",
            json!({"path": read_path}),
            "filesystem",
            "test-agent",
        );
        stat_action.actor_token = actor_token.to_string();
        let through_core_stat = ipc::send_request(&core_pipe, "request_action", json!(stat_action))
            .await
            .unwrap();
        assert_eq!(
            through_core_stat["result"]["status"], "Completed",
            "{through_core_stat}"
        );
        let target_path = allowed.join("from-core.txt");
        let mut action = ActionRequest::new(
            "create",
            json!({"path": target_path, "content": "from policy"}),
            "filesystem",
            "test-agent",
        );
        action.actor_token = "invalid".to_string();
        let denied = ipc::send_request(&core_pipe, "request_action", json!(action))
            .await
            .unwrap();
        assert_eq!(denied["result"]["status"], "Denied", "{denied}");
        assert!(!target_path.exists());
        action.actor_token = actor_token.to_string();
        let accepted = ipc::send_request(&core_pipe, "request_action", json!(action))
            .await
            .unwrap();
        assert_eq!(accepted["result"]["status"], "Completed", "{accepted}");
        assert_eq!(
            std::fs::read_to_string(&target_path).unwrap(),
            "from policy"
        );
        let model_a_path = allowed.join("actor-token-only.txt");
        let mut model_a_action = ActionRequest::new(
            "create",
            json!({"path": model_a_path, "content": "model a"}),
            "filesystem",
            "test-agent",
        );
        model_a_action.actor_token = actor_token.to_string();
        let model_a_result = fixture_request(
            &core_pipe,
            &json!({"method": "request_action", "params": model_a_action}).to_string(),
        )
        .await;
        assert_eq!(
            model_a_result["result"]["status"], "Completed",
            "{model_a_result}"
        );
        assert_eq!(std::fs::read_to_string(&model_a_path).unwrap(), "model a");
        let events = state
            .storage
            .lock()
            .unwrap()
            .query_events(&Default::default())
            .unwrap();
        assert!(events
            .iter()
            .any(|event| event.event_type == "ActionRequested" && event.actor == "test-agent"));
        assert!(events
            .iter()
            .any(|event| event.event_type == "ActionCompleted" && event.actor == "test-agent"));

        provider.abort();
        let _ = provider.await;
        let restarted_provider =
            start_fixture(provider_pipe.clone(), roots, token.clone(), dispatch_key);
        let reconnected = request(&provider_pipe, "roots", json!({"auth": token})).await;
        assert_eq!(reconnected["ok"], true, "{reconnected}");
        core.abort();
        restarted_provider.abort();
        let _ = core.await;
        let _ = restarted_provider.await;
        drop(state);
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[tokio::test]
    async fn absent_fixture_endpoint_fails_to_connect() {
        let pipe = format!(r"\\.\pipe\spaceos-4-3a-absent-{}", uuid::Uuid::new_v4());
        assert!(ClientOptions::new().open(pipe).is_err());
    }
}
