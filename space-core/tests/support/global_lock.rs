//! Cross-process mutual exclusion for tests touching GLOBAL named pipes.
//!
//! Several suites bind or consume the machine-global `\\.\pipe\spaceos-core`
//! / `spaceos-fs-provider` (in-process servers, provider stubs, real
//! `spaceos.exe` processes) or the shared `C:\spaceos-test` result file.
//! Cargo runs test binaries (and tests within a binary) in parallel, so
//! without exclusion one suite's requests land in another suite's server
//! (observed: break_b7 cross-talk, Qualification Gate 0).
//! This lock serializes exactly that conflicting subset; everything else
//! stays parallel. Test-only, std-only, no product impact.
//!
//! MIRRORED FILE: an identical copy lives in
//! `filesystem-provider/tests/support/global_lock.rs` (integration tests
//! cannot share code across packages except via `#[path]`; keep the two
//! copies byte-identical).

use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// Name of the single global lock (one lock for both pipes + result file:
//  a single coarse lock cannot deadlock; per-pipe ordering bugs are
//  impossible by construction).
fn lock_path() -> PathBuf {
    std::env::temp_dir().join("spaceos-global-ipc.lock")
}

fn pid_alive(pid: u32) -> bool {
    // One child spawn per check, only on the contended path.
    match std::process::Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/NH", "/FO", "CSV"])
        .output()
    {
        Ok(out) => {
            let text = String::from_utf8_lossy(&out.stdout);
            text.lines().any(|l| l.contains(&format!("\"{pid}\"")))
        }
        Err(_) => true, // cannot tell: assume alive, never steal blindly
    }
}

/// Guard: removes the lock file on drop. Panics nowhere.
pub struct GlobalIpcLock {
    path: PathBuf,
}

impl Drop for GlobalIpcLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Registry of spawned server tasks with structured shutdown.
///
/// Detached `tokio::spawn` servers (provider stubs, in-process Cores on
/// GLOBAL pipe names) outlive their test and steal other suites'
/// connections — observed as cross-talk (Qualification Gate 0).
/// Register every server task here; dropping the guard aborts them all.
/// Test-only, no product impact.
#[derive(Default)]
pub struct TaskRegistry {
    handles: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl TaskRegistry {
    /// Spawn and track a task. Takes `self` BY VALUE (as Arc) so call
    /// sites never fight the borrow checker: always call as
    /// `reg.clone().spawn(fut)` where `fut` owns its own clones.
    pub fn spawn(
        self: std::sync::Arc<Self>,
        fut: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        self.handles.lock().unwrap().push(tokio::spawn(fut));
    }

    pub fn abort_all(&self) {
        for h in self.handles.lock().unwrap().iter() {
            h.abort();
        }
    }
}

/// Guard owning a task registry: abort-all on drop.
pub struct ServerGuard {
    reg: std::sync::Arc<TaskRegistry>,
}

impl ServerGuard {
    pub fn new(reg: std::sync::Arc<TaskRegistry>) -> Self {
        Self { reg }
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.reg.abort_all();
    }
}

/// Acquire the machine-global IPC lock, waiting up to `timeout`.
/// Stale locks (dead holder PID) are stolen so killed harnesses can
/// never wedge later runs. Ownership is nonce-verified after writing,
/// so a concurrent stealer can never split the lock between two
/// holders (create-vs-write race closed).
///
/// NOTE: this blocks the calling OS thread (std sleep loop). Call it
/// once per test, outside hot paths; contention windows in this repo
/// are seconds-to-minutes, well within multi-thread runtimes.
pub fn acquire_global_ipc() -> GlobalIpcLock {
    acquire_with_timeout(Duration::from_secs(600))
}

fn acquire_with_timeout(timeout: Duration) -> GlobalIpcLock {
    static CTR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = lock_path();
    // Unique owner id: pid + timestamp + per-call counter. Holders
    // verify ownership after writing (closes the create-vs-write race
    // where a concurrent stealer sees an empty file and deletes ours).
    let mine = format!(
        "{} {} {}",
        std::process::id(),
        now_secs(),
        CTR.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    );
    let start = Instant::now();
    loop {
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(mut f) => {
                if writeln!(f, "{mine}").is_err() {
                    std::thread::sleep(Duration::from_millis(50));
                    continue;
                }
                drop(f);
                // Ownership check: our line must still be there. A
                // concurrent stealer may have replaced it mid-write.
                match std::fs::read_to_string(&path) {
                    Ok(c) if c.trim_end() == mine => {
                        return GlobalIpcLock { path };
                    }
                    _ => {
                        std::thread::sleep(Duration::from_millis(50));
                        continue;
                    }
                }
            }
            Err(_) => {
                if steal_if_stale(&path) {
                    continue;
                }
                if start.elapsed() > timeout {
                    panic!("global IPC lock not acquired in 600s; stale holder?");
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn steal_if_stale(path: &PathBuf) -> bool {
    let content = std::fs::read_to_string(path).unwrap_or_default();
    let mut parts = content.split_whitespace();
    let pid: Option<u32> = parts.next().and_then(|s| s.parse().ok());
    let ts: Option<u64> = parts.next().and_then(|s| s.parse().ok());
    // Empty/unparseable content is NOT garbage to steal: it is a
    // creator mid-write (create_new landed, bytes not flushed yet).
    // Only a file that stays empty past the grace window means the
    // creator died mid-write — use mtime, never content, for that.
    if pid.is_none() {
        let age = std::fs::metadata(path)
            .and_then(|m| m.modified())
            .map(|t| {
                std::time::SystemTime::now()
                    .duration_since(t)
                    .map(|d| d.as_secs())
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        if age > 10 {
            std::fs::remove_file(path).ok();
            return true;
        }
        return false;
    }
    let steal = match (pid, ts) {
        (Some(p), _) if !pid_alive(p) => true,
        _ => false,
    };
    if steal {
        std::fs::remove_file(path).ok();
        return true;
    }
    false
}
