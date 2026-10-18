//! Browser Provider IPC server — handles action requests from Core.

use anyhow::Result;
use serde_json::{json, Value};
use space_core::{ipc, ActionRequest, ActionResult, ActionStatus};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::warn;

use crate::provider::BrowserProvider;

/// Default pipe path for browser provider on Windows.
#[cfg(windows)]
pub const BROWSER_PROVIDER_PIPE: &str = r"\\.\pipe\spaceos-browser-provider";

/// Run the browser provider IPC server.
pub async fn run_server(provider: BrowserProvider) -> Result<()> {
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
            let n = notify.clone();
            tokio::spawn(async move {
                loop {
                    let server = match ServerOptions::new().create(BROWSER_PROVIDER_PIPE) {
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
                    tokio::spawn(async move {
                        if let Err(e) = handle(server, provider).await {
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
            tokio::spawn(async move {
                if let Err(e) = handle_tcp(stream, provider).await {
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
) -> Result<()> {
    let (reader, mut writer) = tokio::io::split(server);
    serve_lines(reader, &mut writer, provider).await
}

#[cfg(not(windows))]
async fn handle_tcp(stream: tokio::net::TcpStream, provider: BrowserProvider) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    serve_lines(reader, &mut writer, provider).await
}

async fn serve_lines<R, W>(reader: R, writer: &mut W, provider: BrowserProvider) -> Result<()>
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
                match execute_browser_action(&provider, &req).await {
                    Ok(detail) => json!({"ok": true, "result": ActionResult {
                        action_id: req.id, status: ActionStatus::Completed, detail,
                    }}),
                    Err(e) => json!({"ok": true, "result": ActionResult {
                        action_id: req.id, status: ActionStatus::Failed, detail: json!({"error": e.to_string()}),
                    }}),
                }
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

async fn execute_browser_action(provider: &BrowserProvider, req: &ActionRequest) -> Result<Value> {
    // Use the provider's internal execute_action method
    provider
        .execute_action_for_ipc(&req.action, &req.target)
        .await
}
