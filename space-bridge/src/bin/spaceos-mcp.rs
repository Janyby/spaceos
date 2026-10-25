//! spaceos-mcp: SpaceOS MCP server over stdio for OpenCode.
//!
//! Protocol: JSON-RPC 2.0, newline-delimited. stdout carries ONLY protocol
//! messages; all diagnostics go to stderr. A malformed line yields a -32700
//! response; EOF or a broken pipe exits quietly with code 0.

use space_bridge::mcp;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::main]
async fn main() {
    let bound = std::env::var_os("SPACEOS_MCP_ATTEMPT_FILE").map(|file| {
        load_attempt(std::path::Path::new(&file)).unwrap_or_else(|_| {
            eprintln!("spaceos-mcp: invalid isolated attempt configuration"); std::process::exit(1)
        })
    });
    let client=reqwest::Client::builder().timeout(std::time::Duration::from_secs(12)).build().unwrap();
    if let Some(file) = std::env::var_os("SPACEOS_MCP_READ_FIXTURE_FILE") {
        if configure_read_fixture(std::path::Path::new(&file)).is_err() {
            eprintln!("spaceos-mcp: invalid isolated read fixture");
            std::process::exit(1);
        }
    }
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
        let resp = if let Some((endpoint,credential))=&bound {
            mcp::handle_attempt_line(&client,endpoint,credential,&line).await
        } else {mcp::handle_line(&token_cache, &line).await};
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

fn load_attempt(file:&std::path::Path)->anyhow::Result<(String,String)> {
    let root=std::env::var("SPACEOS_TEST_ISOLATION_ROOT")?;
    anyhow::ensure!(root==r"C:\spaceos","wrong laboratory");
    let allowed=std::fs::canonicalize(std::path::Path::new(&root).join("test-tmp"))?;
    let file=std::fs::canonicalize(file)?;
    anyhow::ensure!(file.starts_with(&allowed)&&file!=allowed,"unsafe startup binding");
    let value:serde_json::Value=serde_json::from_slice(&std::fs::read(file)?)?;
    let endpoint=value["endpoint"].as_str().ok_or_else(||anyhow::anyhow!("endpoint missing"))?;
    anyhow::ensure!(endpoint=="http://127.0.0.1:21973/internal/mission-mcp","unsafe Gateway endpoint");
    let credential=value["credential"].as_str().ok_or_else(||anyhow::anyhow!("credential missing"))?;
    anyhow::ensure!(credential.len()>=32,"invalid binding credential");
    Ok((endpoint.into(),credential.into()))
}

fn configure_read_fixture(file: &std::path::Path) -> anyhow::Result<()> {
    let root=std::env::var("SPACEOS_TEST_ISOLATION_ROOT")?;
    anyhow::ensure!(root == r"C:\spaceos", "isolated fixture required");
    let allowed=std::fs::canonicalize(std::path::Path::new(&root).join("test-tmp"))?;
    let file=std::fs::canonicalize(file)?;
    anyhow::ensure!(file.starts_with(&allowed) && file != allowed,"unsafe fixture path");
    let value:serde_json::Value=serde_json::from_slice(&std::fs::read(file)?)?;
    space_bridge::ipc_client::configure_isolated_mcp_read_service(
        value["registrar"].as_str().ok_or_else(||anyhow::anyhow!("fixture credential missing"))?.into(),
        value["world_id"].as_str().ok_or_else(||anyhow::anyhow!("fixture World missing"))?.into())
}
