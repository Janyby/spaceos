//! Browser Provider IPC server — handles action requests from Core.

use anyhow::Result;
use serde_json::{json, Value};
use space_core::dispatch::{DispatchProof, DispatchVerifier};
use space_core::{ipc, action::DispatchRequest, ActionResult, ActionStatus};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::warn;

use crate::provider::BrowserProvider;

/// Default pipe path for browser provider on Windows.
#[cfg(windows)]
pub const BROWSER_PROVIDER_PIPE: &str = r"\\.\pipe\spaceos-browser-provider";

/// Run the browser provider IPC server.
pub async fn run_server(provider: BrowserProvider) -> Result<()> {
    let transport_token = ipc::core_token();
    ipc::provider_principal_credential()?;
    let provider_pipe = ipc::configured_provider_pipe("SPACEOS_BROWSER_PROVIDER_PIPE", ipc::BROWSER_PROVIDER_PIPE)?;
    let dispatch_key = space_core::dispatch::key_from_env("browser").map_err(anyhow::Error::msg)?;
    anyhow::ensure!(dispatch_key != transport_token, "browser dispatch credential conflicts with Core transport credential");
    let dispatch = Arc::new(DispatchVerifier::new("browser", Some(&dispatch_key)));
    #[cfg(windows)]
    {
        use tokio::net::windows::named_pipe::ServerOptions;

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

        for _ in 0..ipc::ACCEPT_LOOPS {
            let provider = provider.clone();
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
                            warn!("browser provider accept create error: {e}");
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
                    let provider = provider.clone();
                    let transport_token = transport_token.clone();
                    let dispatch = dispatch.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle(server, provider, &transport_token, dispatch).await {
                            warn!("browser provider connection error: {e}");
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:7779").await?;
        loop {
            let (stream, _) = listener.accept().await?;
            let provider = provider.clone();
            let transport_token = transport_token.clone();
            let dispatch = dispatch.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_tcp(stream, provider, &transport_token, dispatch).await {
                    warn!("browser provider connection error: {e}");
                }
            });
        }
    }
}

#[cfg(windows)]
async fn handle(
    server: tokio::net::windows::named_pipe::NamedPipeServer,
    provider: BrowserProvider,
    transport_token: &str,
    dispatch: Arc<DispatchVerifier>,
) -> Result<()> {
    let (reader, mut writer) = tokio::io::split(server);
    serve_lines(reader, &mut writer, provider, transport_token, dispatch).await
}

#[cfg(not(windows))]
async fn handle_tcp(
    stream: tokio::net::TcpStream,
    provider: BrowserProvider,
    transport_token: &str,
    dispatch: Arc<DispatchVerifier>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    serve_lines(reader, &mut writer, provider, transport_token, dispatch).await
}

fn transport_auth_ok(params: &Value, expected: &str) -> bool {
    params.get("auth").and_then(Value::as_str) == Some(expected)
}

async fn serve_lines<R, W>(
    reader: R,
    writer: &mut W,
    provider: BrowserProvider,
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
                    json!({"ok": true, "result": {"pong": true, "instance_id": dispatch.instance_id(), "browser_world_protocol":1,"dispatch_protocol":2}})
                }
                "action" => match DispatchRequest::from_provider_params(&params) {
                    Err(_) => json!({"ok": false, "error": "invalid action request"}),
                    Ok(req) => match serde_json::from_value::<DispatchProof>(
                        params["dispatch_proof"].clone(),
                    )
                    .ok()
                    .as_ref()
                    .map_or_else(
                        || dispatch.verify(None, &req),
                        |proof| dispatch.verify(Some(proof), &req),
                    ) {
                        Err(error) => json!({"ok": false, "error": error}),
                        Ok(()) => match execute_browser_action(&provider, &req).await {
                            Ok(detail) => json!({"ok": true, "result": ActionResult {
                                action_id: req.id, status: ActionStatus::Completed, detail,
                            }}),
                            Err(e) => json!({"ok": true, "result": ActionResult {
                                action_id: req.id, status: ActionStatus::Failed, detail: json!({"error": e.to_string()}),
                            }}),
                        },
                    },
                },
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

async fn execute_browser_action(provider: &BrowserProvider, req: &DispatchRequest) -> Result<Value> {
    if req.actor == space_core::service_registration::BROWSER_WORLD_ACTOR {
        return provider.execute_world_action(req).await;
    }
    anyhow::ensure!(req.target.get("scope_version").is_none() && req.target.get("runtime").is_none(), "scoped Browser actor required");
    // Use the provider's internal execute_action method
    provider
        .execute_action_for_ipc(&req.action, &req.target)
        .await
}

#[cfg(all(test, windows))]
mod security_tests {
    use super::*;
    use crate::BrowserProviderConfig;
    use space_core::principal::PrincipalRegistry;
    use space_core::{ActionRequest, Storage};
    use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};

    fn start_fixture(
        pipe: String,
        token: String,
        dispatch_key: String,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let dispatch = Arc::new(DispatchVerifier::new("browser", Some(&dispatch_key)));
            let provider = BrowserProvider::with_config(BrowserProviderConfig {
                cdp_base: "http://127.0.0.1:0".to_string(),
                provider_id: "browser".to_string(),
                poll_interval_secs: 1,
            });
            loop {
                let server = ServerOptions::new()
                    .reject_remote_clients(true)
                    .create(&pipe)
                    .unwrap();
                server.connect().await.unwrap();
                handle(server, provider.clone(), &token, dispatch.clone())
                    .await
                    .unwrap();
            }
        })
    }

    async fn raw_request(pipe: &str, line: &str) -> Value {
        let mut delay = 5;
        let stream = loop {
            match ClientOptions::new().open(pipe) {
                Ok(stream) => break stream,
                Err(_) if delay <= 160 => {
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    delay *= 2;
                }
                Err(error) => panic!("isolated browser fixture pipe did not open: {error}"),
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
        raw_request(
            pipe,
            &json!({"method": method, "params": params}).to_string(),
        )
        .await
    }

    fn action(name: &str, auth: Option<Value>, actor: &str) -> Value {
        let mut request = json!({
            "id": "fixture-action", "action": name,
            "target": {"url": "about:blank"},
            "provider_id": "browser", "actor": actor,
        });
        if let Some(auth) = auth {
            request["auth"] = auth;
        }
        request
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn browser_pipe_auth_and_reconnect_without_chromium() {
        let suffix = uuid::Uuid::new_v4();
        let pipe = format!(r"\\.\pipe\spaceos-4-3a-browser-{suffix}");
        let token = uuid::Uuid::new_v4().to_string();
        let dispatch_key =
            base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, [8u8; 32]);
        std::env::set_var("SPACEOS_DISPATCH_KEY_BROWSER", &dispatch_key);
        let mut server = start_fixture(pipe.clone(), token.clone(), dispatch_key.clone());

        let ping = request(&pipe, "ping", json!({})).await;
        assert_eq!(ping["ok"], true);
        let instance = ping["result"]["instance_id"].as_str().unwrap();
        let signed_tabs = ActionRequest::new("get_tabs", json!({}), "browser", "fixture-agent");
        let proof = space_core::dispatch::issue(&dispatch_key, instance, &signed_tabs).unwrap();
        let mut signed_params = json!(DispatchRequest::from(&signed_tabs));
        signed_params["auth"] = json!(token);
        signed_params["dispatch_proof"] = json!(proof);
        let signed_result = request(&pipe, "action", signed_params.clone()).await;
        assert_eq!(
            signed_result["result"]["status"], "Completed",
            "{signed_result}"
        );
        assert_eq!(signed_result["result"]["detail"]["tabs"], json!([]));
        let replay = request(&pipe, "action", signed_params.clone()).await;
        assert_eq!(replay["ok"], false, "{replay}");
        signed_params["action"] = json!("navigate");
        let tampered = request(&pipe, "action", signed_params).await;
        assert_eq!(tampered["ok"], false, "{tampered}");
        for auth in [
            None,
            Some(json!("wrong")),
            Some(json!(7)),
            Some(json!({"token": token})),
        ] {
            let denied = request(&pipe, "action", action("get_tabs", auth, "system")).await;
            assert_eq!(denied["ok"], false, "{denied}");
        }
        let denied_mutation = request(&pipe, "action", action("navigate", None, "system")).await;
        assert_eq!(denied_mutation["ok"], false, "{denied_mutation}");
        let valid_read = request(
            &pipe,
            "action",
            action("get_tabs", Some(json!(token)), "browser"),
        )
        .await;
        assert_eq!(valid_read["ok"], false, "{valid_read}");
        let no_session = request(
            &pipe,
            "action",
            action("navigate", Some(json!(token)), "browser"),
        )
        .await;
        assert_eq!(no_session["ok"], false, "{no_session}");
        let malformed = request(&pipe, "action", json!({"auth": token})).await;
        assert_eq!(malformed["ok"], false);
        let malformed_json = raw_request(&pipe, "{not-json").await;
        assert_eq!(malformed_json["ok"], false);

        std::env::set_var("SPACEOS_CORE_TOKEN", &token);
        let base = std::env::temp_dir().join(format!("spaceos-4-3a-browser-{suffix}"));
        std::fs::create_dir_all(&base).unwrap();
        let core_pipe = format!(r"\\.\pipe\spaceos-4-3a-browser-core-{suffix}");
        let privileged_token = base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            [12u8; 32],
        );
        let mut principals = PrincipalRegistry::default();
        principals
            .insert(
                "system",
                [12u8; 32],
                ["agent.register", "agent.grant.action.get_tabs"],
                [],
            )
            .unwrap();
        let state = ipc::CoreState::new_with_semantic_world_and_provider_pipes_and_principals(
            Storage::open(base.join("core.db")).unwrap(),
            None,
            &format!(r"\\.\pipe\spaceos-4-3a-browser-unused-{suffix}"),
            &pipe,
            principals,
        );
        let core_state = state.clone();
        let core_path = core_pipe.clone();
        let core = tokio::spawn(async move {
            ipc::run_server(core_state, &core_path).await.unwrap();
        });
        let registration = ipc::send_request(
            &core_pipe,
            "register_agent",
            json!({
                "principal": "system", "name": "browser-fixture-agent",
                "capabilities": ["action.get_tabs"], "worlds": [], "principal_token": privileged_token,
            }),
        )
        .await
        .unwrap();
        assert_eq!(registration["ok"], true, "{registration}");
        let mut core_action =
            ActionRequest::new("get_tabs", json!({}), "browser", "browser-fixture-agent");
        core_action.actor_token = "invalid".to_string();
        let denied_at_core = request(&core_pipe, "request_action", json!(core_action)).await;
        assert_eq!(
            denied_at_core["result"]["status"], "Denied",
            "{denied_at_core}"
        );
        core_action.actor_token = registration["result"]["token"]
            .as_str()
            .unwrap()
            .to_string();
        let allowed_at_core = request(&core_pipe, "request_action", json!(core_action)).await;
        assert_eq!(
            allowed_at_core["result"]["status"], "Completed",
            "{allowed_at_core}"
        );
        assert_eq!(allowed_at_core["result"]["detail"]["tabs"], json!([]));
        core.abort();
        let _ = core.await;
        drop(state);
        std::fs::remove_dir_all(&base).unwrap();

        server.abort();
        let _ = server.await;
        server = start_fixture(pipe.clone(), token.clone(), dispatch_key);
        let reconnected = request(
            &pipe,
            "action",
            action("get_tabs", Some(json!(token)), "browser"),
        )
        .await;
        assert_eq!(reconnected["ok"], false, "{reconnected}");
        server.abort();
        let _ = server.await;
    }
}
