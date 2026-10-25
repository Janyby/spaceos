//! Browser observation via CDP (observation-only).
//!
//! Protocol detail stays inside the daemon: Core sees only `record_event`
//! with `provider_id` set by the caller — it never learns CDP. This module
//! binds no pipe and serves no actions, so it cannot collide with the
//! filesystem provider endpoint (routing limitation A1 untouched).
//!
//! Observation-only by construction: the wire protocol used here is
//! read-only (`Page.enable` + inbound events). `navigate_to` below is a
//! test helper for driving deterministic reality, never called by the
//! observer loop itself.

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use space_core::{ipc, Event};
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot::Receiver;

/// Default CDP endpoint of a locally launched browser (production).
pub const DEFAULT_CDP_BASE: &str = "http://127.0.0.1:9222";
/// Provider id registered for this source (mirrors the `process` source).
pub const BROWSER_PROVIDER_ID: &str = "browser";

/// One debuggable page target from `/json/list`.
#[derive(Debug, Clone)]
pub struct CdpTarget {
    pub id: String,
    pub url: String,
    pub ws_url: String,
}

/// Pure constructor: `Page.frameNavigated` -> Core event. Unit-tested
/// without a browser; the observer loop only fills these fields.
pub fn navigated_event(provider_id: &str, frame_url: &str, target_id: &str) -> Event {
    let mut e = Event::new(
        "BrowserNavigated",
        "filesystem",
        json!({"url": frame_url, "target_id": target_id}),
    );
    e.provider_id = Some(provider_id.to_string());
    e
}

/// Pure constructor: `Page.loadEventFired` -> Core event.
pub fn loaded_event(provider_id: &str, target_id: &str, last_url: &str) -> Event {
    let mut e = Event::new(
        "BrowserLoaded",
        "filesystem",
        json!({"url": last_url, "target_id": target_id}),
    );
    e.provider_id = Some(provider_id.to_string());
    e
}

fn split_host_port(base: &str) -> Result<(String, u16)> {
    let no_scheme = base
        .strip_prefix("http://")
        .ok_or_else(|| anyhow::anyhow!("cdp base must be http://host:port"))?;
    let (host, port) = no_scheme
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("cdp base needs :port"))?;
    Ok((host.to_string(), port.parse::<u16>()?))
}

/// Read one HTTP response body framed by Content-Length.
/// Chrome keeps HTTP/1.1 connections alive, so read-to-EOF framing hangs;
/// Content-Length framing works on every Chromium version.
async fn read_http_body(s: &mut tokio::net::TcpStream) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut out = Vec::new();
    let mut buf = vec![0u8; 65536];
    loop {
        if let Some(pos) = out.windows(4).position(|w| w == b"\r\n\r\n") {
            let header = String::from_utf8_lossy(&out[..pos]);
            let len: usize = header
                .lines()
                .find_map(|l| {
                    let (name, value) = l.split_once(':')?;
                    if name.trim().eq_ignore_ascii_case("content-length") {
                        value.trim().parse().ok()
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            let start = pos + 4;
            while out.len() < start + len {
                match s.read(&mut buf).await? {
                    0 => break,
                    n => out.extend_from_slice(&buf[..n]),
                }
            }
            if out.len() < start + len {
                anyhow::bail!("cdp /json/list: truncated body");
            }
            return Ok(out[start..start + len].to_vec());
        }
        if out.len() > 1_048_576 {
            anyhow::bail!("cdp /json/list: headers too large");
        }
        match s.read(&mut buf).await? {
            0 => anyhow::bail!("cdp /json/list: connection closed before headers"),
            n => out.extend_from_slice(&buf[..n]),
        }
    }
}

/// GET /json/list over raw TCP (no HTTP dep needed for one endpoint).
pub async fn fetch_targets(base: &str) -> Result<Vec<CdpTarget>> {
    let (host, port) = split_host_port(base)?;
    let mut s = tokio::net::TcpStream::connect((host.as_str(), port))
        .await
        .context("cdp http connect")?;
    // Chrome 155+ closes HTTP/1.0 connections without a body, so use
    // HTTP/1.1 and frame the body via Content-Length instead of EOF.
    let req = format!(
        "GET /json/list HTTP/1.1\r\nHost: {}:{}\r\nConnection: close\r\n\r\n",
        host, port
    );
    s.write_all(req.as_bytes()).await?;
    let body = read_http_body(&mut s).await?;
    let text = String::from_utf8_lossy(&body);
    let items: Vec<Value> = serde_json::from_str(&text).context("cdp /json/list parse")?;
    Ok(items
        .into_iter()
        .filter(|t| t["type"] == "page")
        .filter_map(|t| {
            Some(CdpTarget {
                id: t["id"].as_str()?.to_string(),
                url: t["url"].as_str().unwrap_or("").to_string(),
                ws_url: t["webSocketDebuggerUrl"].as_str()?.to_string(),
            })
        })
        .collect())
}

/// Drive one navigation on a target's debugger URL (test helper + future
/// action path; NOT used by the observation loop).
pub async fn navigate_to(ws_url: &str, url: &str) -> Result<()> {
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_url)
        .await
        .context("cdp ws connect")?;
    let req = json!({"id": 1, "method": "Page.navigate", "params": {"url": url}});
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&req)?.into(),
    ))
    .await?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if tokio::time::Instant::now() > deadline {
            anyhow::bail!("cdp navigate: no result in 15s");
        }
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("cdp ws closed during navigate"))??;
        if let tokio_tungstenite::tungstenite::Message::Text(t) = msg {
            let v: Value = serde_json::from_str(t.as_str())?;
            if v.get("id") == Some(&json!(1)) {
                if v.get("error").is_some() {
                    anyhow::bail!("cdp navigate denied: {v}");
                }
                return Ok(());
            }
        }
    }
}

/// Observe loop: attach the first page target, enable Page events, forward
/// `frameNavigated` / `loadEventFired` as Core observations. Retries
/// forever when the browser is absent (graceful degradation); stops on
/// `stop`. Reads only — never issues page actions.
pub async fn watch_cdp(
    cdp_base: &str,
    provider_id: String,
    core_pipe: &str,
    mut stop: Receiver<()>,
) -> Result<()> {
    loop {
        if stop.try_recv().is_ok() {
            return Ok(());
        }
        if let Err(e) = run_session(cdp_base, &provider_id, core_pipe, &mut stop).await {
            tracing::warn!("cdp session: {e:#}");
        }
        tokio::select! {
            _ = &mut stop => return Ok(()),
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
        }
    }
}

async fn run_session(
    base: &str,
    provider_id: &str,
    pipe: &str,
    stop: &mut Receiver<()>,
) -> Result<()> {
    let targets = fetch_targets(base).await?;
    let t = targets
        .first()
        .ok_or_else(|| anyhow::anyhow!("cdp: no page target"))?;
    let target_id = t.id.clone();
    let (mut ws, _) = tokio_tungstenite::connect_async(&t.ws_url)
        .await
        .context("cdp ws connect")?;
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        json!({"id": 1, "method": "Page.enable"}).to_string().into(),
    ))
    .await?;
    let mut last_url = t.url.clone();
    loop {
        tokio::select! {
            _ = &mut *stop => return Ok(()),
            msg = ws.next() => {
                let Some(msg) = msg else {
                    anyhow::bail!("cdp ws closed");
                };
                let msg = msg?;
                let tokio_tungstenite::tungstenite::Message::Text(text) = msg else {
                    continue;
                };
                let v: Value = serde_json::from_str(text.as_str())?;
                match v.get("method").and_then(|m| m.as_str()) {
                    Some("Page.frameNavigated") => {
                        let frame = &v["params"]["frame"];
                        let url = frame["url"].as_str().unwrap_or("");
                        last_url = url.to_string();
                        let ev = navigated_event(provider_id, url, &target_id);
                        let _ = ipc::send_provider_principal_request(pipe, "browser", "record_event", json!(ev)).await;
                    }
                    Some("Page.loadEventFired") => {
                        let ev = loaded_event(provider_id, &target_id, &last_url);
                        let _ = ipc::send_provider_principal_request(pipe, "browser", "record_event", json!(ev)).await;
                    }
                    _ => {}
                }
            }
        }
    }
}
