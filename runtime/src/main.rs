use anyhow::Result;
use serde_json::json;
use space_core::ipc;
mod ui;
mod wry_wrapper;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).cloned().unwrap_or_else(|| "run".into());

    // The native window must own the OS main thread: winit 0.30 panics with
    // "Initializing the event loop outside of the main thread is a
    // significant cross-platform compatibility hazard" otherwise -- and
    // neither `#[tokio::main]`'s worker thread nor a spawned thread is the
    // main thread. So `wry` is handled BEFORE the runtime starts; every
    // other command keeps running on a Tokio runtime exactly as before.
    if cmd == "wry" {
        return run_wry_command(&args);
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main(args))
}

async fn async_main(args: Vec<String>) -> Result<()> {
    let cmd = args.get(1).cloned().unwrap_or_else(|| "run".into());

    match cmd.as_str() {
        "run" => {
            // Optional --root PATH
            let root = args
                .iter()
                .position(|a| a == "--root")
                .and_then(|i| args.get(i + 1))
                .cloned();
            let db = std::env::var("SPACEOS_DB")
                .unwrap_or_else(|_| "C:\\spaceos\\runtime\\core.db".into());
            if let Some(parent) = std::path::Path::new(&db).parent() {
                std::fs::create_dir_all(parent)?;
            }
            let storage = space_core::Storage::open(&db)?;
            let state = space_core::ipc::CoreState::new(storage);
            let core_state = state.clone();
            tokio::spawn(async move {
                if let Err(e) = ipc::run_server(core_state, ipc::CORE_PIPE).await {
                    tracing::error!("core server error: {e}");
                }
            });
            let root_pb = root.map(std::path::PathBuf::from).or_else(|| {
                // default test root
                let p = std::path::PathBuf::from("C:\\spaceos-test");
                std::fs::create_dir_all(&p).ok();
                Some(p)
            });
            filesystem_provider::ipc::run_server("filesystem".into(), root_pb).await
        }
        "objects" => {
            let resp = ipc::send_request(ipc::CORE_PIPE, "query_objects", json!({})).await?;
            println!("{}", serde_json::to_string_pretty(&resp["result"])?);
            Ok(())
        }
        "events" => {
            let mut params = json!({"limit": 50});
            if let Some(t) = args.get(2) {
                params["event_type"] = json!(t);
            }
            let resp = ipc::send_request(ipc::CORE_PIPE, "query_events", params).await?;
            println!("{}", serde_json::to_string_pretty(&resp["result"])?);
            Ok(())
        }
        "relations" => {
            let resp = ipc::send_request(ipc::CORE_PIPE, "query_relations", json!({})).await?;
            println!("{}", serde_json::to_string_pretty(&resp["result"])?);
            Ok(())
        }
        "providers" => {
            let resp = ipc::send_request(ipc::CORE_PIPE, "list_providers", json!({})).await?;
            println!("{}", serde_json::to_string_pretty(&resp["result"])?);
            Ok(())
        }
        "scan" => {
            let path = args
                .get(2)
                .ok_or_else(|| anyhow::anyhow!("usage: spaceos scan <path>"))?;
            let resp = ipc::send_request(
                ipc::PROVIDER_PIPE,
                "scan",
                json!({"path": path, "provider_id": "filesystem"}),
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
            Ok(())
        }
        "stat" => {
            let path = args
                .get(2)
                .ok_or_else(|| anyhow::anyhow!("usage: spaceos stat <path>"))?;
            let resp = ipc::send_request(ipc::PROVIDER_PIPE, "stat", json!({"path": path})).await?;
            println!("{}", serde_json::to_string_pretty(&resp["result"])?);
            Ok(())
        }
        "rename" | "move" | "copy" | "delete" | "write" | "create" => {
            let (action, target) = match cmd.as_str() {
                "rename" | "move" | "copy" => {
                    let from = args.get(2).ok_or_else(|| anyhow::anyhow!("missing from"))?;
                    let to = args.get(3).ok_or_else(|| anyhow::anyhow!("missing to"))?;
                    (cmd.clone(), json!({"from": from, "to": to}))
                }
                "delete" => {
                    let path = args.get(2).ok_or_else(|| anyhow::anyhow!("missing path"))?;
                    (cmd.clone(), json!({"path": path}))
                }
                "write" | "create" => {
                    let path = args.get(2).ok_or_else(|| anyhow::anyhow!("missing path"))?;
                    let content = args.get(3).map(|s| s.as_str()).unwrap_or("");
                    (cmd.clone(), json!({"path": path, "content": content}))
                }
                _ => unreachable!(),
            };
            let req = space_core::ActionRequest::new(action, target, "filesystem", "cli");
            let resp = ipc::send_request(ipc::CORE_PIPE, "request_action", json!(req)).await?;
            println!("{}", serde_json::to_string_pretty(&resp["result"])?);
            Ok(())
        }
        "subscribe" => {
            let event_type = args.get(2).cloned();
            // simple: subscribe then print notifications for 30s
            subscribe_and_print(event_type).await?;
            Ok(())
        }
        // Handled on the OS main thread before the runtime starts (winit
        // requires the main thread); see `main` / `run_wry_command`.
        "wry" => unreachable!("wry is dispatched before the Tokio runtime starts"),
        "ui" => {
            let port = args
                .iter()
                .position(|a| a == "--port")
                .and_then(|i| args.get(i + 1))
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(8765);
            ui::run(port).await
        }
        other => {
            eprintln!("unknown command: {other}");
            eprintln!("usage: spaceos [run|objects|events|relations|providers|scan|stat|rename|move|copy|delete|write|create|subscribe|ui]");
            Ok(())
        }
    }
}

/// Native client window (`spaceos wry`). Runs on the OS main thread (a winit
/// requirement; see `main`). The target UI is explicit and logged:
///   --url URL : load exactly this URL, never start a local server;
///   --port N  : default 8765; if an external SpaceOS stack already answers
///               on the port, load it; otherwise serve the bundled runtime
///               UI locally and load that.
fn run_wry_command(args: &[String]) -> Result<()> {
    let url_arg = args
        .iter()
        .position(|a| a == "--url")
        .and_then(|i| args.get(i + 1))
        .cloned();
    let port = args
        .iter()
        .position(|a| a == "--port")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(8765);
    let url = url_arg
        .clone()
        .unwrap_or_else(|| format!("http://127.0.0.1:{port}/"));

    #[cfg(feature = "wry-wrapper")]
    {
        let external = url_arg.is_some() || tcp_probe(port);
        if external {
            tracing::info!("wry: loading existing SpaceOS UI at {url}");
        } else {
            tracing::info!("wry: no stack on 127.0.0.1:{port}; serving bundled runtime UI");
            // The bundled UI server needs a runtime, but the event loop must
            // stay on this (main) thread, so the server gets its own thread.
            std::thread::spawn(move || {
                if let Ok(rt) = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                {
                    rt.block_on(async {
                        let _ = ui::run(port).await;
                    });
                }
            });
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
        if let Err(e) = wry_wrapper::run_wry_window_blocking(url) {
            return Err(anyhow::anyhow!(e));
        }
    }
    #[cfg(not(feature = "wry-wrapper"))]
    {
        let _ = url;
        tracing::info!("wry command requires 'wry-wrapper' feature; loading static fallback");
        println!("{}", wry_wrapper::load_static_fallback());
    }
    Ok(())
}

/// Blocking TCP probe (this path intentionally has no async runtime).
#[cfg(feature = "wry-wrapper")]
fn tcp_probe(port: u16) -> bool {
    use std::net::{TcpStream, ToSocketAddrs};
    let addr = format!("127.0.0.1:{port}");
    match addr.to_socket_addrs() {
        Ok(mut it) => match it.next() {
            Some(a) => TcpStream::connect_timeout(&a, std::time::Duration::from_millis(500)).is_ok(),
            None => false,
        },
        Err(_) => false,
    }
}

async fn subscribe_and_print(event_type: Option<String>) -> Result<()> {
    #[cfg(windows)]
    {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::windows::named_pipe::ClientOptions;
        let stream = ClientOptions::new().open(ipc::CORE_PIPE)?;
        let (reader, mut writer) = tokio::io::split(stream);
        let params = match event_type {
            Some(t) => json!({"event_type": t}),
            None => json!({}),
        };
        let req = json!({"method": "subscribe", "params": params});
        let mut s = serde_json::to_string(&req)?;
        s.push('\n');
        writer.write_all(s.as_bytes()).await?;
        writer.flush().await?;
        let mut lines = BufReader::new(reader).lines();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let line = tokio::time::timeout_at(deadline.into(), lines.next_line()).await;
            match line {
                Ok(Ok(Some(l))) => println!("{l}"),
                _ => break,
            }
        }
    }
    Ok(())
}
