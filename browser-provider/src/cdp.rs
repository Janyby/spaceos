//! CDP Client — low-level Chrome DevTools Protocol communication.

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;

/// Default CDP endpoint of a locally launched browser (production).
pub const DEFAULT_CDP_BASE: &str = "http://127.0.0.1:9222";

/// One debuggable page target from `/json/list`.
#[derive(Debug, Clone)]
pub struct CdpTarget {
    pub id: String,
    pub url: String,
    pub title: Option<String>,
    pub ws_url: String,
    pub target_type: String,
}

/// CDP client for communicating with a browser instance.
pub struct CdpClient {
    host: String,
    port: u16,
}

impl CdpClient {
    /// Validate a specifically approved browser runtime; never follows a peer
    /// supplied host or silently accepts a replacement browser on this port.
    pub async fn pinned_browser(&self, approved:&space_core::provider_observation::BrowserRuntimeConfig)->Result<Arc<Mutex<CdpSession>>> {
        approved.validate()?;
        anyhow::ensure!(approved.cdp_origin==format!("http://{}:{}",self.host,self.port),"browser endpoint mismatch");
        tokio::time::timeout(std::time::Duration::from_secs(10),async {
            let mut stream=TcpStream::connect((&*self.host,self.port)).await?;
            stream.write_all(format!("GET /json/version HTTP/1.1\r\nHost: {}:{}\r\nConnection: close\r\n\r\n",self.host,self.port).as_bytes()).await?;
            let body=read_http_body(&mut stream).await?;
            let version:Value=serde_json::from_slice(&body).map_err(|_|anyhow::anyhow!("invalid browser version response"))?;
            let expected=format!("ws://{}:{}/devtools/browser/{}",self.host,self.port,approved.expected_browser_debugger_id);
            anyhow::ensure!(version["webSocketDebuggerUrl"].as_str()==Some(expected.as_str()),"unapproved browser runtime; owner epoch authorization required");
            let config=tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default().max_message_size(Some(1_048_576)).max_frame_size(Some(1_048_576));
            let (ws,_)=tokio_tungstenite::connect_async_with_config(&expected,Some(config),false).await.map_err(|_|anyhow::anyhow!("approved browser session unavailable"))?;
            Ok(CdpSession::new_shared(ws))
        }).await.map_err(|_|anyhow::anyhow!("browser runtime verification deadline"))?
    }

    pub async fn pinned_targets(&self,approved:&space_core::provider_observation::BrowserRuntimeConfig)->Result<Vec<CdpTarget>> {
        let session=self.pinned_browser(approved).await?;
        let mut guard=session.lock().await;
        let result=guard.send_command("Target.getTargets",None).await?;
        let list=result["targetInfos"].as_array().context("invalid browser targets response")?;
        anyhow::ensure!(list.len()<=1024,"too many browser targets");
        let mut targets=Vec::new();
        for t in list {
            if t["type"]!="page"||t.get("subtype").is_some_and(|v|!v.is_null()&&v!=""){continue;}
            let id=t["targetId"].as_str().context("invalid page target")?;
            targets.push(CdpTarget{id:id.into(),url:t["url"].as_str().context("invalid page URL")?.into(),title:Some(t["title"].as_str().context("invalid page title")?.into()),
                ws_url:format!("ws://{}:{}/devtools/page/{id}",self.host,self.port),target_type:"page".into()});
        }
        guard.ws.close(None).await?;Ok(targets)
    }
    /// Create a new CDP client for the given base URL.
    pub fn new(base_url: &str) -> Result<Self> {
        let (host, port) = split_host_port(base_url)?;
        Ok(Self { host, port })
    }

    /// Fetch all available targets from the browser.
    pub async fn fetch_targets(&self) -> Result<Vec<CdpTarget>> {
        let mut stream = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .context("cdp http connect")?;

        // Chrome 155+ closes HTTP/1.0 connections without a body, so use
        // HTTP/1.1 and frame the body via Content-Length instead of EOF.
        let req = format!(
            "GET /json/list HTTP/1.1\r\nHost: {}:{}\r\nConnection: close\r\n\r\n",
            self.host, self.port
        );
        stream.write_all(req.as_bytes()).await?;

        let body = read_http_body(&mut stream).await?;
        let text = String::from_utf8_lossy(&body);
        let items: Vec<Value> = serde_json::from_str(&text).context("cdp /json/list parse")?;

        Ok(items
            .into_iter()
            .filter(|t| t["type"] == "page")
            .filter_map(|t| {
                Some(CdpTarget {
                    id: t["id"].as_str()?.to_string(),
                    url: t["url"].as_str().unwrap_or("").to_string(),
                    title: t["title"].as_str().map(|s| s.to_string()),
                    ws_url: t["webSocketDebuggerUrl"].as_str()?.to_string(),
                    target_type: t["type"].as_str().unwrap_or("page").to_string(),
                })
            })
            .collect())
    }

    /// Connect to a specific target's WebSocket debugger URL.
    pub async fn connect_target(&self, ws_url: &str) -> Result<Arc<Mutex<CdpSession>>> {
        let (ws, _) = tokio_tungstenite::connect_async(ws_url)
            .await
            .context("cdp ws connect")?;

        Ok(CdpSession::new_shared(ws))
    }
}

/// Active CDP session over WebSocket.
pub struct CdpSession {
    pub ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    next_id: u64,
    pending: HashMap<u64, mpsc::Sender<Value>>,
}

impl CdpSession {
    /// Create a new CdpSession wrapped in Arc<Mutex> for sharing.
    pub fn new_shared(
        ws: tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            ws,
            next_id: 1,
            pending: HashMap::new(),
        }))
    }

    /// Send a CDP command and wait for the response.
    pub async fn send_command(&mut self, method: &str, params: Option<Value>) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;

        let (tx, _rx) = mpsc::channel(1);
        self.pending.insert(id, tx);

        let req = json!({
            "id": id,
            "method": method,
            "params": params.unwrap_or(json!({}))
        });

        self.ws
            .send(Message::Text(serde_json::to_string(&req)?.into()))
            .await?;

        // Wait for response with timeout
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline.into()) => {
                    self.pending.remove(&id);
                    anyhow::bail!("cdp command timeout: {}", method);
                }
                msg = self.ws.next() => {
                    let Some(msg) = msg else {
                        self.pending.remove(&id);
                        anyhow::bail!("cdp ws closed");
                    };
                    let msg = msg?;
                    if let Message::Text(text) = msg {
                        let v: Value = serde_json::from_str(text.as_str())?;
                        if v.get("id") == Some(&json!(id)) {
                            self.pending.remove(&id);
                            if let Some(error) = v.get("error") {
                                anyhow::bail!("cdp error: {}", error);
                            }
                            return Ok(v["result"].clone());
                        } else if let Some(_method) = v.get("method").and_then(|m| m.as_str()) {
                            // Handle events
                            if let Some(tx) = self.pending.get(&0) {
                                let _ = tx.try_send(v);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Enable Page domain events.
    pub async fn enable_page(&mut self) -> Result<()> {
        self.send_command("Page.enable", None).await?;
        Ok(())
    }

    /// Navigate to a URL.
    pub async fn navigate(&mut self, url: &str) -> Result<()> {
        self.send_command("Page.navigate", Some(json!({"url": url})))
            .await?;
        Ok(())
    }

    /// Reload the page.
    pub async fn reload(&mut self) -> Result<()> {
        self.send_command("Page.reload", None).await?;
        Ok(())
    }

    /// Get the document tree (DOM).
    pub async fn get_document(&mut self) -> Result<Value> {
        self.send_command("DOM.getDocument", Some(json!({"depth": -1})))
            .await
    }

    /// Get outer HTML of a node.
    pub async fn get_outer_html(&mut self, node_id: u64) -> Result<String> {
        let result = self
            .send_command("DOM.getOuterHTML", Some(json!({"nodeId": node_id})))
            .await?;
        Ok(result["outerHTML"].as_str().unwrap_or("").to_string())
    }

    /// Run JavaScript in the page context.
    pub async fn evaluate(&mut self, expression: &str) -> Result<Value> {
        self.send_command(
            "Runtime.evaluate",
            Some(json!({"expression": expression, "returnByValue": true})),
        )
        .await
    }

    /// Close the session.
    pub async fn close(mut self) -> Result<()> {
        self.ws.close(None).await?;
        Ok(())
    }
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
async fn read_http_body(stream: &mut TcpStream) -> Result<Vec<u8>> {
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
            anyhow::ensure!(len<=1_048_576,"CDP response too large");
            while out.len() < start + len {
                match stream.read(&mut buf).await? {
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
        match stream.read(&mut buf).await? {
            0 => anyhow::bail!("cdp /json/list: connection closed before headers"),
            n => out.extend_from_slice(&buf[..n]),
        }
    }
}

/// Fetch targets from a CDP endpoint (standalone function for backward compatibility).
pub async fn fetch_targets(base: &str) -> Result<Vec<CdpTarget>> {
    let client = CdpClient::new(base)?;
    client.fetch_targets().await
}
