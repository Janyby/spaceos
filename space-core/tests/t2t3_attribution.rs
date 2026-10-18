//! T2->T3 per-sample attribution diagnostics (TEST-ONLY, isolated).
//!
//! Purpose: attribute individual T2->T3 commit samples (especially tail
//! outliers) to the execution conditions at their timestamps, separating
//! CPU-load correlation from SQLite/WAL/fsync-load correlation as far as
//! available tools allow.
//!
//! What T2->T3 IS (verified by reading batch_experiment.rs /
//! wal_experiment.rs / storage.rs, NOT assumed):
//! - t2 = Instant::now() immediately BEFORE the commit call.
//! - t3 = Instant::now() immediately AFTER it returns.
//! - wal_experiment "current"/"wal": direct `Storage::record_event` =
//!   ONE synchronous=FULL SQLite INSERT OR REPLACE + fsync, plus serde
//!   serialization, fault-Mutex check (uncontended), no SpaceTrace hook.
//! - batch_experiment "wal"/"batch-10": `Storage::transaction(|tx|
//!   tx.record_event)` = BEGIN IMMEDIATE + same INSERT + COMMIT fsync,
//!   plus one UUIDv4 for tx correlation + two no-op global_hook calls.
//! - T1->T2 = Event construction only; T3->T4 = growing query_events scan.
//!   Only T2->T3 is ever asserted (p95 < 5000us, 100 samples/mode).
//!
//! Isolation rules honored:
//! - NO production code touched; only public Storage/Event/Space API.
//! - NO threshold change; these suites REPORT ONLY (no assert on p95),
//!   so load modes cannot "fail" — the original tests keep their gates.
//! - NO semantics change: same 100-sample loop, same payload shape
//!   ({"i","data":"x"*100}), same world_id/object_ids pattern.
//! - Run ONE diagnostic at a time:
//!   cargo test -p space-core --test t2t3_attribution <name> -- --exact --nocapture --test-threads=1
//!   so the ONLY load is the controlled neighbor below.
//!
//! What is collected per sample (all directly measured):
//! - idx, t2t3_us, t1t2_us, t3t4_us (Instant deltas, same clock as originals)
//! - wall_ms since measurement start (same t0 as NEIGHBOR markers)
//! - flood_commits: neighbor SQLite commits completed BEFORE this sample
//!   (shared AtomicU64 — quantitative overlap proof, not causation).
//!
//! What is NOT collected (unavailable without production hooks / ETW):
//! - per-commit fsync duration (inside Storage::record_event; adding a
//!   hook there would be a production-code change — forbidden).
//! - CPU preemption / context-switch events (logman/xperf/wpr absent from
//!   PATH on this machine; no ETW backend in tree).
//! - flush/sync timestamps (same reason as fsync).
//! Attribution level is therefore TEMPORAL CORRELATION with neighbor
//! phase + dose comparison across modes. Correlation is never reported
//! as causation.
//!
//! Warmup (T2T3_WARMED_SYNC_ISOLATION): before the first measured sample,
//! sqlite-flood modes wait until `flood_done` exceeds 500. Only the steady
//! commit loops increment that counter — file deletion and schema creation
//! never touch it — so the gate distinguishes init from live flood without
//! fixed sleeps. Warmup time is NOT part of the sample window (logged via
//! WARMUP_DONE / SAMPLE_WINDOW markers, same wall clock).

use serde_json::json;
use space_core::query::EventQuery;
use space_core::{Event, Space, Storage};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

fn tmp_db(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-attr");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{name}.db"))
}

fn fresh_wal(name: &str) -> Storage {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let _ = std::fs::remove_file(tmp_db(&format!("{name}{suffix}")));
    }
    Storage::open_wal(&tmp_db(name)).unwrap()
}

fn fresh_current(name: &str) -> Storage {
    let p = tmp_db(&format!("cur-{name}"));
    let _ = std::fs::remove_file(&p);
    let _ = std::fs::remove_file(tmp_db(&format!("cur-{name}-wal")));
    let _ = std::fs::remove_file(tmp_db(&format!("cur-{name}-shm")));
    Storage::open(&p).unwrap()
}

fn pct_sorted(v: &[i64], p: f64) -> i64 {
    v[((p / 100.0) * (v.len() as f64 - 1.0)).round() as usize]
}

/// Core diagnostic loop (report-only, no gate assert by design).
fn run_attr_tracked(
    label: &str,
    use_tx: bool,
    wal_mode: bool,
    neigh: &str,
    cpu_threads: usize,
    sql_threads: usize,
) {
    let tag = format!("attr-{}-{}-{}", label, neigh, std::process::id());
    let mut s = if wal_mode {
        fresh_wal(&tag)
    } else {
        fresh_current(&tag)
    };
    let sp = Space {
        id: format!("space-{tag}"),
        name: format!("W-{tag}"),
        metadata: json!({}),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    s.create_space(&sp).unwrap();
    // Read-only PRAGMA confirmation on the TARGET connection (outside the
    // measurement window: t0 starts below). Enforces the frozen target.
    let (tjm, tsy) = (
        s.journal_mode().unwrap_or_default(),
        s.synchronous().unwrap_or(-1),
    );
    eprintln!("TARGET_PRAGMA journal_mode={tjm} synchronous={tsy}");

    let par = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    eprintln!(
        "ATTR_BEGIN label={label} neigh={neigh} wal_mode={wal_mode} use_tx={use_tx} cpu_threads={cpu_threads} sql_threads={sql_threads} parallelism={par} tid={:?}",
        std::thread::current().id()
    );

    let stop = Arc::new(AtomicBool::new(false));
    let flood_done = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];
    let t0 = std::time::Instant::now();

    // Neighbor 1: pure-CPU burners. Tight wrapping-add + spin_loop, no
    // syscalls, no allocation, no disk I/O by construction.
    for _ in 0..cpu_threads {
        let stop2 = stop.clone();
        handles.push(std::thread::spawn(move || {
            let mut x = 0u64;
            while !stop2.load(Ordering::Relaxed) {
                x = x.wrapping_add(1);
                std::hint::spin_loop();
            }
            std::hint::black_box(x);
        }));
    }
    if cpu_threads > 0 {
        eprintln!("NEIGHBOR_START kind=cpu threads={cpu_threads} wall_ms=0");
    }

    // Neighbor 2: SQLite flood. Each thread owns a SEPARATE WAL DB file
    // (Storage is !Sync) and hammers direct record_event — same syscall
    // class as the measured commit (INSERT + FULL fsync) on the same SSD
    // and temp-dir filesystem, plus JSON serialization CPU.
    for k in 0..sql_threads {
        let stop2 = stop.clone();
        let done2 = flood_done.clone();
        let fname = format!("{tag}-fl{k}");
        handles.push(std::thread::spawn(move || {
            let fs = fresh_wal(&fname);
            // Read-only PRAGMA confirmation on THIS flood connection.
            let (fjm, fsy) = (
                fs.journal_mode().unwrap_or_default(),
                fs.synchronous().unwrap_or(-1),
            );
            eprintln!("FLOOD_PRAGMA idx={k} journal_mode={fjm} synchronous={fsy}");
            let mut i = 0u64;
            while !stop2.load(Ordering::Relaxed) {
                let mut e = Event::new("Tick", "filesystem", json!({"i": i}));
                e.id = format!("{fname}-{i}");
                e.world_id = "flood".into();
                if fs.record_event(&e).is_ok() {
                    done2.fetch_add(1, Ordering::Relaxed);
                }
                i += 1;
            }
        }));
    }
    if sql_threads > 0 {
        eprintln!("NEIGHBOR_START kind=sqlite threads={sql_threads} wall_ms=0");
    }

    // Warmup gate: steady flood only. See module docs. Skipped when there
    // is no sqlite flood (counter would never advance).
    let (warm_start, warm_wall_ms) = if sql_threads > 0 {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        while flood_done.load(Ordering::Relaxed) <= 500 {
            if std::time::Instant::now() > deadline {
                panic!("warmup: flood did not reach 500 commits in 120s");
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let c = flood_done.load(Ordering::Relaxed);
        let w = t0.elapsed().as_millis();
        eprintln!("WARMUP_DONE flood_start={c} wall_ms={w}");
        (c, w)
    } else {
        eprintln!("WARMUP_SKIPPED reason=no-sqlite-flood");
        (0, 0)
    };

    let mut t2t3_all: Vec<i64> = Vec::with_capacity(100);
    for i in 0..100 {
        let t1 = std::time::Instant::now();
        let mut e = Event::new(
            "FileChanged",
            "filesystem",
            json!({"i": i, "data": "x".repeat(100)}),
        );
        e.id = format!("{tag}-t-{i:03}");
        e.world_id = "lat".into();
        e.object_ids = vec![format!("{tag}-obj-{i:03}")];

        let t2 = std::time::Instant::now();
        if use_tx {
            s.transaction(|tx| {
                tx.record_event(&e).unwrap();
                Ok(())
            })
            .unwrap();
        } else {
            s.record_event(&e).unwrap();
        }
        let t3 = std::time::Instant::now();

        let _ = s
            .query_events(&EventQuery {
                world_id: Some("lat".into()),
                ..Default::default()
            })
            .unwrap();
        let t4 = std::time::Instant::now();

        let t2t3 = (t3 - t2).as_micros() as i64;
        t2t3_all.push(t2t3);
        let wall_ms = t0.elapsed().as_millis();
        let flood = flood_done.load(Ordering::Relaxed);
        eprintln!(
            "SAMPLE idx={i} t2t3_us={t2t3} wall_ms={wall_ms} flood_commits={flood} t1t2_us={} t3t4_us={}",
            (t2 - t1).as_micros(),
            (t4 - t3).as_micros()
        );
    }
    let window_end_ms = t0.elapsed().as_millis();

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }
    let total_flood = flood_done.load(Ordering::Relaxed);
    eprintln!(
        "NEIGHBOR_STOP wall_ms={} flood_commits_total={total_flood}",
        t0.elapsed().as_millis()
    );
    // Sample-window intensity: commits strictly inside the measured loop.
    let window_secs = (window_end_ms as f64 - warm_wall_ms as f64) / 1000.0;
    let window_rate = if window_secs > 0.0 {
        (total_flood as f64 - warm_start as f64) / window_secs
    } else {
        0.0
    };
    eprintln!(
        "SAMPLE_WINDOW secs={window_secs:.3} flood_start={warm_start} flood_end={total_flood} rate_per_s={window_rate:.0}"
    );
    t2t3_all.sort_unstable();
    let p50 = pct_sorted(&t2t3_all, 50.0);
    let p95 = pct_sorted(&t2t3_all, 95.0);
    let p99 = pct_sorted(&t2t3_all, 99.0);
    let max = *t2t3_all.last().unwrap();
    let over = t2t3_all.iter().filter(|&&v| v >= 5000).count();
    eprintln!(
        "ATTR_END label={label} neigh={neigh} n=100 p50={p50} p95={p95} p99={p99} max={max} exceed5000={over} flood_total={total_flood}"
    );
}

#[test]
fn attr_baseline_wal_tx() {
    // Mirrors batch_experiment "wal": WAL + single-event transaction, no neighbor.
    run_attr_tracked("wal-tx", true, true, "none", 0, 0);
}

#[test]
fn attr_baseline_current() {
    // Mirrors wal_experiment "current": rollback-journal direct commit, no neighbor.
    run_attr_tracked("current-direct", false, false, "none", 0, 0);
}

#[test]
fn attr_cpu_wal_tx() {
    // Same commit as wal-tx baseline + 8 pure-CPU burners (no disk I/O).
    run_attr_tracked("wal-tx", true, true, "cpu8", 8, 0);
}

#[test]
fn attr_sqlite_wal_tx() {
    // Same commit + 4 SQLite flood threads (same syscall class, same SSD).
    run_attr_tracked("wal-tx", true, true, "sqlite4", 0, 4);
}

#[test]
fn attr_cpu_current() {
    // Most sensitive commit (rollback journal) + 8 pure-CPU burners.
    run_attr_tracked("current-direct", false, false, "cpu8", 8, 0);
}

#[test]
fn attr_sqlite_current() {
    // Most sensitive commit + 4 SQLite flood threads.
    run_attr_tracked("current-direct", false, false, "sqlite4", 0, 4);
}

/// Open a raw flood connection with WAL + synchronous=OFF (TEST-ONLY).
/// Schema and persistent WAL come from the public Storage API (same DB
/// organization as the FULL flood: same files, same tables); only
/// `synchronous` differs, set explicitly on THIS connection and verified
/// by readback. PRAGMA synchronous is per-connection, so the target and
/// the other flood connections are unaffected.
fn flood_conn_off(fname: &str) -> rusqlite::Connection {
    // Fresh files (same lifecycle as fresh_wal).
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let _ = std::fs::remove_file(tmp_db(&format!("{fname}{suffix}")));
    }
    // Schema + persistent WAL via public API, then drop that connection.
    drop(Storage::open_wal(&tmp_db(fname)).unwrap());
    let conn = rusqlite::Connection::open(tmp_db(fname)).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF;")
        .unwrap();
    let jm: String = conn
        .query_row("PRAGMA journal_mode;", [], |r| r.get(0))
        .unwrap();
    let sy: i64 = conn
        .query_row("PRAGMA synchronous;", [], |r| r.get(0))
        .unwrap();
    assert_eq!(jm, "wal", "flood OFF conn must stay WAL");
    assert_eq!(sy, 0, "flood OFF conn must read back synchronous=OFF");
    conn
}

/// Byte-identical INSERT to Storage::record_event (storage.rs:480-498),
/// minus the (disarmed) fault hooks. Flood events always carry a non-empty
/// world_id ("flood"), so the payload-fallback branch is unreachable here.
fn flood_insert_off(conn: &rusqlite::Connection, e: &Event) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO events (id, event_type, actor, provider_id, object_ids, payload, timestamp, correlation_id, world_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        rusqlite::params![
            e.id,
            e.event_type,
            e.actor,
            e.provider_id,
            serde_json::to_string(&e.object_ids).unwrap(),
            serde_json::to_string(&e.payload).unwrap(),
            e.timestamp.to_rfc3339(),
            e.correlation_id,
            e.world_id,
        ],
    )?;
    Ok(())
}

/// B mode: target FROZEN (file WAL DB, synchronous=FULL, single-event
/// transaction — byte-identical measurement to attr_sqlite_wal_tx);
/// only the 4 flood connections run synchronous=OFF. Same thread count,
/// same SQL statement, same files organization, same 100 samples.
fn run_attr_tracked_flood_off() {
    let label = "wal-tx";
    let neigh = "sqlite4-floodOFF";
    let tag = format!("attr-{label}-{neigh}-{}", std::process::id());
    let mut s = fresh_wal(&tag);
    let sp = Space {
        id: format!("space-{tag}"),
        name: format!("W-{tag}"),
        metadata: json!({}),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    s.create_space(&sp).unwrap();
    // Frozen-target enforcement: abort loudly if the target is not WAL/FULL.
    let (tjm, tsy) = (
        s.journal_mode().unwrap_or_default(),
        s.synchronous().unwrap_or(-1),
    );
    eprintln!("TARGET_PRAGMA journal_mode={tjm} synchronous={tsy}");
    assert_eq!(tjm, "wal", "target must stay WAL");
    assert_eq!(tsy, 2, "target must stay synchronous=FULL");

    let par = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    eprintln!(
        "ATTR_BEGIN label={label} neigh={neigh} wal_mode=true use_tx=true cpu_threads=0 sql_threads=4 parallelism={par} tid={:?}",
        std::thread::current().id()
    );

    let stop = Arc::new(AtomicBool::new(false));
    let flood_done = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];
    let t0 = std::time::Instant::now();

    for k in 0..4usize {
        let stop2 = stop.clone();
        let done2 = flood_done.clone();
        let fname = format!("{tag}-fl{k}");
        handles.push(std::thread::spawn(move || {
            let conn = flood_conn_off(&fname);
            eprintln!("FLOOD_PRAGMA idx={k} journal_mode=wal synchronous=0");
            let mut i = 0u64;
            while !stop2.load(Ordering::Relaxed) {
                let mut e = Event::new("Tick", "filesystem", json!({"i": i}));
                e.id = format!("{fname}-{i}");
                e.world_id = "flood".into();
                if flood_insert_off(&conn, &e).is_ok() {
                    done2.fetch_add(1, Ordering::Relaxed);
                }
                i += 1;
            }
        }));
    }
    eprintln!("NEIGHBOR_START kind=sqlite-syncOFF threads=4 wall_ms=0");

    // Warmup gate (unconditional here: this runner always has 4 flood
    // threads). Same counter semantics as the shared runner.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while flood_done.load(Ordering::Relaxed) <= 500 {
        if std::time::Instant::now() > deadline {
            panic!("warmup: flood did not reach 500 commits in 120s");
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let warm_start = flood_done.load(Ordering::Relaxed);
    let warm_wall_ms = t0.elapsed().as_millis();
    eprintln!("WARMUP_DONE flood_start={warm_start} wall_ms={warm_wall_ms}");

    let mut t2t3_all: Vec<i64> = Vec::with_capacity(100);
    for i in 0..100 {
        let t1 = std::time::Instant::now();
        let mut e = Event::new(
            "FileChanged",
            "filesystem",
            json!({"i": i, "data": "x".repeat(100)}),
        );
        e.id = format!("{tag}-t-{i:03}");
        e.world_id = "lat".into();
        e.object_ids = vec![format!("{tag}-obj-{i:03}")];

        let t2 = std::time::Instant::now();
        s.transaction(|tx| {
            tx.record_event(&e).unwrap();
            Ok(())
        })
        .unwrap();
        let t3 = std::time::Instant::now();

        let _ = s
            .query_events(&EventQuery {
                world_id: Some("lat".into()),
                ..Default::default()
            })
            .unwrap();
        let t4 = std::time::Instant::now();

        let t2t3 = (t3 - t2).as_micros() as i64;
        t2t3_all.push(t2t3);
        let wall_ms = t0.elapsed().as_millis();
        let flood = flood_done.load(Ordering::Relaxed);
        eprintln!(
            "SAMPLE idx={i} t2t3_us={t2t3} wall_ms={wall_ms} flood_commits={flood} t1t2_us={} t3t4_us={}",
            (t2 - t1).as_micros(),
            (t4 - t3).as_micros()
        );
    }
    let window_end_ms = t0.elapsed().as_millis();

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }
    let total_flood = flood_done.load(Ordering::Relaxed);
    eprintln!(
        "NEIGHBOR_STOP wall_ms={} flood_commits_total={total_flood}",
        t0.elapsed().as_millis()
    );
    // Sample-window intensity: commits strictly inside the measured loop.
    let window_secs = (window_end_ms as f64 - warm_wall_ms as f64) / 1000.0;
    let window_rate = if window_secs > 0.0 {
        (total_flood as f64 - warm_start as f64) / window_secs
    } else {
        0.0
    };
    eprintln!(
        "SAMPLE_WINDOW secs={window_secs:.3} flood_start={warm_start} flood_end={total_flood} rate_per_s={window_rate:.0}"
    );
    t2t3_all.sort_unstable();
    let p50 = pct_sorted(&t2t3_all, 50.0);
    let p95 = pct_sorted(&t2t3_all, 95.0);
    let p99 = pct_sorted(&t2t3_all, 99.0);
    let max = *t2t3_all.last().unwrap();
    let over = t2t3_all.iter().filter(|&&v| v >= 5000).count();
    eprintln!(
        "ATTR_END label={label} neigh={neigh} n=100 p50={p50} p95={p95} p99={p99} max={max} exceed5000={over} flood_total={total_flood}"
    );
}

#[test]
fn attr_sqlite_wal_tx_flood_off() {
    // B mode: target WAL/FULL unchanged; flood WAL/OFF.
    run_attr_tracked_flood_off();
}

/// Aggregate slot limiter (TEST-ONLY): caps TOTAL flood commits across all
/// 4 connections to ~800/s without touching SQL or adding transactions.
/// 50ms slots × 40 commits; threads out of budget sleep 1ms and retry.
/// Possible side effects (documented, see report): pacing ripple at the
/// 50ms slot scale; sleeping-vs-fsync-blocked profile differs from FULL by
/// construction (that difference IS the treatment); one short Mutex per
/// admission (~4 threads × ~200 admissions/s — negligible contention).
struct AggSlotLimiter {
    slot_start: std::sync::Mutex<std::time::Instant>,
    slot_used: AtomicU64,
    waited_ms: AtomicU64,
    slot_ms: u64,
    per_slot: u64,
}

impl AggSlotLimiter {
    fn new(slot_ms: u64, per_slot: u64) -> Self {
        Self {
            slot_start: std::sync::Mutex::new(std::time::Instant::now()),
            slot_used: AtomicU64::new(0),
            waited_ms: AtomicU64::new(0),
            slot_ms,
            per_slot,
        }
    }

    /// Block until one aggregate commit token is available.
    fn admit(&self) {
        loop {
            {
                let mut g = self.slot_start.lock().unwrap();
                if g.elapsed().as_millis() >= self.slot_ms as u128 {
                    *g = std::time::Instant::now();
                    self.slot_used.store(0, Ordering::Relaxed);
                }
                if self.slot_used.load(Ordering::Relaxed) < self.per_slot {
                    self.slot_used.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
            self.waited_ms.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// B-rate-matched mode: identical to run_attr_tracked_flood_off except the
/// flood passes through a shared 800/s aggregate limiter. Target, SQL,
/// connections, WAL, warmup gate (>500), N=100 — all unchanged.
fn run_attr_tracked_flood_off_limited() {
    let label = "wal-tx";
    let neigh = "sqlite4-floodOFF-lim800";
    let tag = format!("attr-{label}-{neigh}-{}", std::process::id());
    let mut s = fresh_wal(&tag);
    let sp = Space {
        id: format!("space-{tag}"),
        name: format!("W-{tag}"),
        metadata: json!({}),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    s.create_space(&sp).unwrap();
    let (tjm, tsy) = (
        s.journal_mode().unwrap_or_default(),
        s.synchronous().unwrap_or(-1),
    );
    eprintln!("TARGET_PRAGMA journal_mode={tjm} synchronous={tsy}");
    assert_eq!(tjm, "wal", "target must stay WAL");
    assert_eq!(tsy, 2, "target must stay synchronous=FULL");

    let par = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    eprintln!(
        "ATTR_BEGIN label={label} neigh={neigh} wal_mode=true use_tx=true cpu_threads=0 sql_threads=4 parallelism={par} tid={:?}",
        std::thread::current().id()
    );
    eprintln!("LIMITER kind=slot-aggregate slot_ms=50 per_slot=40 target_per_s=800 band=720-880");

    let stop = Arc::new(AtomicBool::new(false));
    let flood_done = Arc::new(AtomicU64::new(0));
    let limiter = Arc::new(AggSlotLimiter::new(50, 40));
    let mut handles = vec![];
    let t0 = std::time::Instant::now();

    for k in 0..4usize {
        let stop2 = stop.clone();
        let done2 = flood_done.clone();
        let lim2 = limiter.clone();
        let fname = format!("{tag}-fl{k}");
        handles.push(std::thread::spawn(move || {
            let conn = flood_conn_off(&fname);
            eprintln!("FLOOD_PRAGMA idx={k} journal_mode=wal synchronous=0");
            let mut i = 0u64;
            while !stop2.load(Ordering::Relaxed) {
                lim2.admit();
                if stop2.load(Ordering::Relaxed) {
                    break;
                }
                let mut e = Event::new("Tick", "filesystem", json!({"i": i}));
                e.id = format!("{fname}-{i}");
                e.world_id = "flood".into();
                if flood_insert_off(&conn, &e).is_ok() {
                    done2.fetch_add(1, Ordering::Relaxed);
                }
                i += 1;
            }
        }));
    }
    eprintln!("NEIGHBOR_START kind=sqlite-syncOFF-limited threads=4 wall_ms=0");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while flood_done.load(Ordering::Relaxed) <= 500 {
        if std::time::Instant::now() > deadline {
            panic!("warmup: flood did not reach 500 commits in 120s");
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let warm_start = flood_done.load(Ordering::Relaxed);
    let warm_wall_ms = t0.elapsed().as_millis();
    eprintln!("WARMUP_DONE flood_start={warm_start} wall_ms={warm_wall_ms}");

    let mut t2t3_all: Vec<i64> = Vec::with_capacity(100);
    for i in 0..100 {
        let t1 = std::time::Instant::now();
        let mut e = Event::new(
            "FileChanged",
            "filesystem",
            json!({"i": i, "data": "x".repeat(100)}),
        );
        e.id = format!("{tag}-t-{i:03}");
        e.world_id = "lat".into();
        e.object_ids = vec![format!("{tag}-obj-{i:03}")];

        let t2 = std::time::Instant::now();
        s.transaction(|tx| {
            tx.record_event(&e).unwrap();
            Ok(())
        })
        .unwrap();
        let t3 = std::time::Instant::now();

        let _ = s
            .query_events(&EventQuery {
                world_id: Some("lat".into()),
                ..Default::default()
            })
            .unwrap();
        let t4 = std::time::Instant::now();

        let t2t3 = (t3 - t2).as_micros() as i64;
        t2t3_all.push(t2t3);
        let wall_ms = t0.elapsed().as_millis();
        let flood = flood_done.load(Ordering::Relaxed);
        eprintln!(
            "SAMPLE idx={i} t2t3_us={t2t3} wall_ms={wall_ms} flood_commits={flood} t1t2_us={} t3t4_us={}",
            (t2 - t1).as_micros(),
            (t4 - t3).as_micros()
        );
    }
    let window_end_ms = t0.elapsed().as_millis();

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }
    let total_flood = flood_done.load(Ordering::Relaxed);
    eprintln!(
        "NEIGHBOR_STOP wall_ms={} flood_commits_total={total_flood}",
        t0.elapsed().as_millis()
    );
    let window_secs = (window_end_ms as f64 - warm_wall_ms as f64) / 1000.0;
    let window_rate = if window_secs > 0.0 {
        (total_flood as f64 - warm_start as f64) / window_secs
    } else {
        0.0
    };
    eprintln!(
        "SAMPLE_WINDOW secs={window_secs:.3} flood_start={warm_start} flood_end={total_flood} rate_per_s={window_rate:.0}"
    );
    eprintln!(
        "LIMITER_WAIT total_ms={}",
        limiter.waited_ms.load(Ordering::Relaxed)
    );
    t2t3_all.sort_unstable();
    let p50 = pct_sorted(&t2t3_all, 50.0);
    let p95 = pct_sorted(&t2t3_all, 95.0);
    let p99 = pct_sorted(&t2t3_all, 99.0);
    let max = *t2t3_all.last().unwrap();
    let over = t2t3_all.iter().filter(|&&v| v >= 5000).count();
    eprintln!(
        "ATTR_END label={label} neigh={neigh} n=100 p50={p50} p95={p95} p99={p99} max={max} exceed5000={over} flood_total={total_flood}"
    );
}

#[test]
fn attr_sqlite_wal_tx_flood_off_limited() {
    // B-rate-matched: target WAL/FULL unchanged; flood WAL/OFF at ~800/s aggregate.
    run_attr_tracked_flood_off_limited();
}

/// FULL rate-controlled mode: target FROZEN (file WAL DB, synchronous=FULL,
/// single-event transaction); the 4 flood connections stay synchronous=FULL
/// via the public `Storage::open_wal` API and pass through the SAME shared
/// slot limiter as the OFF runner. Only `per_slot` varies:
/// 40/50ms = ~800/s (A800), 10/50ms = ~200/s (A200).
fn run_attr_tracked_full_limited(per_slot: u64, neigh: &str) {
    let label = "wal-tx";
    let tag = format!("attr-{label}-{neigh}-{}", std::process::id());
    let mut s = fresh_wal(&tag);
    let sp = Space {
        id: format!("space-{tag}"),
        name: format!("W-{tag}"),
        metadata: json!({}),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    s.create_space(&sp).unwrap();
    let (tjm, tsy) = (
        s.journal_mode().unwrap_or_default(),
        s.synchronous().unwrap_or(-1),
    );
    eprintln!("TARGET_PRAGMA journal_mode={tjm} synchronous={tsy}");
    assert_eq!(tjm, "wal", "target must stay WAL");
    assert_eq!(tsy, 2, "target must stay synchronous=FULL");

    let target_per_s = per_slot * 20; // 50ms slots
    let band = if target_per_s == 800 {
        "720-880"
    } else {
        "180-220"
    };
    let par = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    eprintln!(
        "ATTR_BEGIN label={label} neigh={neigh} wal_mode=true use_tx=true cpu_threads=0 sql_threads=4 parallelism={par} tid={:?}",
        std::thread::current().id()
    );
    eprintln!("LIMITER kind=slot-aggregate slot_ms=50 per_slot={per_slot} target_per_s={target_per_s} band={band}");

    let stop = Arc::new(AtomicBool::new(false));
    let flood_done = Arc::new(AtomicU64::new(0));
    let limiter = Arc::new(AggSlotLimiter::new(50, per_slot));
    let mut handles = vec![];
    let t0 = std::time::Instant::now();

    for k in 0..4usize {
        let stop2 = stop.clone();
        let done2 = flood_done.clone();
        let lim2 = limiter.clone();
        let fname = format!("{tag}-fl{k}");
        handles.push(std::thread::spawn(move || {
            let fs = fresh_wal(&fname);
            let (fjm, fsy) = (
                fs.journal_mode().unwrap_or_default(),
                fs.synchronous().unwrap_or(-1),
            );
            eprintln!("FLOOD_PRAGMA idx={k} journal_mode={fjm} synchronous={fsy}");
            let mut i = 0u64;
            while !stop2.load(Ordering::Relaxed) {
                lim2.admit();
                if stop2.load(Ordering::Relaxed) {
                    break;
                }
                let mut e = Event::new("Tick", "filesystem", json!({"i": i}));
                e.id = format!("{fname}-{i}");
                e.world_id = "flood".into();
                if fs.record_event(&e).is_ok() {
                    done2.fetch_add(1, Ordering::Relaxed);
                }
                i += 1;
            }
        }));
    }
    eprintln!("NEIGHBOR_START kind=sqlite-syncFULL-limited threads=4 wall_ms=0");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while flood_done.load(Ordering::Relaxed) <= 500 {
        if std::time::Instant::now() > deadline {
            panic!("warmup: flood did not reach 500 commits in 120s");
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let warm_start = flood_done.load(Ordering::Relaxed);
    let warm_wall_ms = t0.elapsed().as_millis();
    eprintln!("WARMUP_DONE flood_start={warm_start} wall_ms={warm_wall_ms}");

    let mut t2t3_all: Vec<i64> = Vec::with_capacity(100);
    for i in 0..100 {
        let t1 = std::time::Instant::now();
        let mut e = Event::new(
            "FileChanged",
            "filesystem",
            json!({"i": i, "data": "x".repeat(100)}),
        );
        e.id = format!("{tag}-t-{i:03}");
        e.world_id = "lat".into();
        e.object_ids = vec![format!("{tag}-obj-{i:03}")];

        let t2 = std::time::Instant::now();
        s.transaction(|tx| {
            tx.record_event(&e).unwrap();
            Ok(())
        })
        .unwrap();
        let t3 = std::time::Instant::now();

        let _ = s
            .query_events(&EventQuery {
                world_id: Some("lat".into()),
                ..Default::default()
            })
            .unwrap();
        let t4 = std::time::Instant::now();

        let t2t3 = (t3 - t2).as_micros() as i64;
        t2t3_all.push(t2t3);
        let wall_ms = t0.elapsed().as_millis();
        let flood = flood_done.load(Ordering::Relaxed);
        eprintln!(
            "SAMPLE idx={i} t2t3_us={t2t3} wall_ms={wall_ms} flood_commits={flood} t1t2_us={} t3t4_us={}",
            (t2 - t1).as_micros(),
            (t4 - t3).as_micros()
        );
    }
    let window_end_ms = t0.elapsed().as_millis();

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }
    let total_flood = flood_done.load(Ordering::Relaxed);
    eprintln!(
        "NEIGHBOR_STOP wall_ms={} flood_commits_total={total_flood}",
        t0.elapsed().as_millis()
    );
    let window_secs = (window_end_ms as f64 - warm_wall_ms as f64) / 1000.0;
    let window_rate = if window_secs > 0.0 {
        (total_flood as f64 - warm_start as f64) / window_secs
    } else {
        0.0
    };
    eprintln!(
        "SAMPLE_WINDOW secs={window_secs:.3} flood_start={warm_start} flood_end={total_flood} rate_per_s={window_rate:.0}"
    );
    eprintln!(
        "LIMITER_WAIT total_ms={}",
        limiter.waited_ms.load(Ordering::Relaxed)
    );
    t2t3_all.sort_unstable();
    let p50 = pct_sorted(&t2t3_all, 50.0);
    let p95 = pct_sorted(&t2t3_all, 95.0);
    let p99 = pct_sorted(&t2t3_all, 99.0);
    let max = *t2t3_all.last().unwrap();
    let over = t2t3_all.iter().filter(|&&v| v >= 5000).count();
    eprintln!(
        "ATTR_END label={label} neigh={neigh} n=100 p50={p50} p95={p95} p99={p99} max={max} exceed5000={over} flood_total={total_flood}"
    );
}

#[test]
fn attr_sqlite_wal_tx_full_lim800() {
    // A800: target WAL/FULL unchanged; flood WAL/FULL at ~800/s aggregate.
    run_attr_tracked_full_limited(40, "sqlite4-floodFULL-lim800");
}

#[test]
fn attr_sqlite_wal_tx_full_lim200() {
    // A200: target WAL/FULL unchanged; flood WAL/FULL at ~200/s aggregate.
    run_attr_tracked_full_limited(10, "sqlite4-floodFULL-lim200");
}
