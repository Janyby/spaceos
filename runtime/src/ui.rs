use anyhow::Result;
use serde_json::Value;
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
    // Production browser caller authentication is still an owner decision.
    // This compatibility UI may run only behind an explicit synthetic lab key.
    anyhow::ensure!(std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some(), "runtime UI authentication contract not provisioned");
    anyhow::ensure!(port > 1024 && port != 8765 && port != 9222, "unsafe laboratory runtime UI port");
    crate::read_client::identity()?;
    let access = std::env::var("SPACEOS_RUNTIME_UI_LAB_KEY")?;
    anyhow::ensure!(access.len() >= 32 && !access.contains(['\r','\n']), "invalid laboratory UI access key");
    let _st = spacetrace::SpaceTrace::new().ok();
    if let Some(ref st) = _st {
        spacetrace::init_global_hooks(st.buffer(), st.correlation());
    }
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    tracing::info!("ui on http://127.0.0.1:{port}");
    loop {
        let (mut stream, _) = listener.accept().await?;
        let access = access.clone();
        tokio::spawn(async move {
            let view_corr = format!("view-{}", uuid::Uuid::new_v4());
            let poll_start = std::time::Instant::now();
            let mut buf = vec![0u8; 8192];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            let expected = format!("Bearer {access}");
            let authorized = req.lines().filter_map(|l| l.split_once(':')).any(|(name,value)|
                name.eq_ignore_ascii_case("authorization") && value.trim() == expected);
            if !authorized {
                let body = r#"{"ok":false,"error":"runtime laboratory authorization required"}"#;
                let response = format!("HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
                let _ = stream.write_all(response.as_bytes()).await;
                return;
            }
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
    std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("static").join(name))
        .unwrap_or_else(|_| format!("missing static file {name}"))
}

async fn api(method: &str, params: Value) -> String {
    match crate::read_client::read(method, params).await {
        Ok(v) => serde_json::to_string_pretty(&v).unwrap(),
        Err(_) => serde_json::json!({"ok":false,"error":"runtime scoped read unavailable"}).to_string(),
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
        std::env::set_var("SPACEOS_RUNTIME_ACTOR", "i5-runtime-test");
        std::env::set_var("SPACEOS_RUNTIME_ACTOR_TOKEN", "synthetic-model-a-test-token");
        std::env::set_var("SPACEOS_RUNTIME_WORLD_ID", "i5-runtime-world");
        std::env::set_var("SPACEOS_RUNTIME_UI_LAB_KEY", "i5-synthetic-ui-access-key-32-bytes");
        std::env::set_var("SPACEOS_CORE_PIPE", format!(r"\\.\pipe\spaceos-4-3a4-i5-ui-{}",uuid::Uuid::new_v4()));
        let probe=std::net::TcpListener::bind(("127.0.0.1",0)).unwrap();
        let port=probe.local_addr().unwrap().port(); drop(probe);
        let srv = tokio::spawn(async move {
            let _ = run(port).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        let mut denied=tokio::net::TcpStream::connect(("127.0.0.1",port)).await.unwrap();
        denied.write_all(b"GET /api/objects HTTP/1.0\r\n\r\n").await.unwrap();
        let mut denied_response=String::new();denied.read_to_string(&mut denied_response).await.unwrap();
        assert!(denied_response.starts_with("HTTP/1.1 401 Unauthorized"));
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("ui must listen");
        stream
            .write_all(b"GET /api/objects HTTP/1.0\r\nAuthorization: Bearer i5-synthetic-ui-access-key-32-bytes\r\n\r\n")
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
