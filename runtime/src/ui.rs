use anyhow::Result;
use serde_json::Value;
use space_core::ipc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Minimal local web UI server: static files + /api proxy to Core.
///
/// SpaceTrace Slice 6: each HTTP request emits view_poll at receipt and
/// view_render after the response is built, sharing a per-request
/// `view-<uuid>` correlation. World is unscoped here (UI polls without
/// world_id) and recorded as unknown ("") rather than guessed (I20).
/// Hooks are infallible no-ops when SpaceTrace is uninitialized.
pub async fn run(port: u16) -> Result<()> {
    let _st = spacetrace::SpaceTrace::new().ok();
    if let Some(ref st) = _st {
        spacetrace::init_global_hooks(st.buffer(), st.correlation());
    }
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    tracing::info!("ui on http://127.0.0.1:{port}");
    loop {
        let (mut stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let view_corr = format!("view-{}", uuid::Uuid::new_v4());
            let poll_start = std::time::Instant::now();
            let mut buf = vec![0u8; 8192];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            let path = req
                .lines()
                .next()
                .unwrap_or("")
                .split_whitespace()
                .nth(1)
                .unwrap_or("/")
                .to_string();
            spacetrace::hooks::global_hooks().view_poll(&view_corr, "web-ui", "", "web-viewer");
            let (content_type, body) = match path.as_str() {
                "/api/world" => (
                    "application/json",
                    api("get_world", serde_json::json!({})).await,
                ),
                "/api/objects" => (
                    "application/json",
                    api("query_objects", serde_json::json!({})).await,
                ),
                "/api/relations" => (
                    "application/json",
                    api("query_relations", serde_json::json!({})).await,
                ),
                "/api/events" => (
                    "application/json",
                    api("query_events", serde_json::json!({"limit": 100})).await,
                ),
                "/" | "/index.html" => ("text/html", serve_static("index.html")),
                "/app.js" => ("text/javascript", serve_static("app.js")),
                other => ("text/plain", format!("not found: {other}")),
            };
            spacetrace::hooks::global_hooks().view_render(
                &view_corr,
                "web-ui",
                "",
                poll_start.elapsed().as_millis() as u64,
            );
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes()).await;
            let _ = stream.flush().await;
        });
    }
}

fn serve_static(name: &str) -> String {
    std::fs::read_to_string(format!("C:\\spaceos\\runtime\\static\\{name}"))
        .unwrap_or_else(|_| format!("missing static file {name}"))
}

async fn api(method: &str, params: Value) -> String {
    match ipc::send_request(ipc::CORE_PIPE, method, params).await {
        Ok(v) => serde_json::to_string_pretty(&v["result"]).unwrap_or_else(|_| "null".into()),
        Err(e) => serde_json::json!({"error": e.to_string()}).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Behavior regression: UI still serves HTTP after trace instrumentation.
    // (Trace spans themselves are covered at hook level in spacetrace tests;
    // the server's buffer is process-local to run().)
    #[tokio::test]
    async fn ui_serves_api_after_trace_wiring() {
        let port = 18765u16;
        let srv = tokio::spawn(async move {
            let _ = run(port).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("ui must listen");
        stream
            .write_all(b"GET /api/objects HTTP/1.0\r\n\r\n")
            .await
            .unwrap();
        let mut buf = vec![0u8; 65536];
        let mut out = Vec::new();
        // Server closes connection after response (Connection: close).
        loop {
            match stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => out.extend_from_slice(&buf[..n]),
                Err(_) => break,
            }
        }
        let text = String::from_utf8_lossy(&out);
        assert!(
            text.starts_with("HTTP/1.1 200 OK"),
            "ui must return 200, got: {}",
            &text[..text.len().min(120)]
        );
        srv.abort();
    }
}
