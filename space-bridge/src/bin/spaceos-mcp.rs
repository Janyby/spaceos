//! spaceos-mcp: SpaceOS MCP server over stdio for OpenCode.
//!
//! Protocol: JSON-RPC 2.0, newline-delimited. stdout carries ONLY protocol
//! messages; all diagnostics go to stderr. A malformed line yields a -32700
//! response; EOF or a broken pipe exits quietly with code 0.

use space_bridge::mcp;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::main]
async fn main() {
    let token_cache = tokio::sync::Mutex::new(None);
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    let mut stdout = tokio::io::stdout();
    eprintln!("spaceos-mcp: ready (protocol {})", mcp::PROTOCOL_VERSION);
    loop {
        let line = match lines.next_line().await {
            Ok(Some(l)) => l,
            Ok(None) => break, // EOF: clean shutdown
            Err(e) => {
                eprintln!("spaceos-mcp: stdin error: {e}");
                break;
            }
        };
        let resp = mcp::handle_line(&token_cache, &line).await;
        if let Some(resp) = resp {
            let mut s = serde_json::to_string(&resp).unwrap_or_else(|_| {
                r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"encode error"}}"#
                    .to_string()
            });
            s.push('\n');
            if stdout.write_all(s.as_bytes()).await.is_err() {
                break; // broken pipe: parent is gone
            }
            if stdout.flush().await.is_err() {
                break;
            }
        }
    }
}
