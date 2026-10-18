//! B2 — Event log growth. Fixed conditions, see docs/BREAKAGE_CAMPAIGN.md.
//! Thresholds (fixed BEFORE the run): 100k completes; write p99 < 50ms;
//! full query at 100k < 5s; reopen < 5s. Higher steps measurement-only
//! with a wall-time stop rule (12 min/step); 1M attempted only if fast.
use space_core::query::EventQuery;
use space_core::{Event, Storage};

fn tmp_db(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-bb2");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{name}.db"))
}

fn percentile(v: &mut [i64], p: f64) -> i64 {
    v.sort_unstable();
    v[((p / 100.0) * (v.len() as f64 - 1.0)).round() as usize]
}

#[test]
fn event_growth_100k() {
    let p = tmp_db("ev100k");
    let _ = std::fs::remove_file(&p);
    let s = Storage::open(&p).unwrap();
    let mut lat = Vec::with_capacity(100_000);
    let t0 = std::time::Instant::now();
    for i in 0..100_000 {
        let e = Event::new("Tick", "bench", serde_json::json!({"i": i}));
        let t = std::time::Instant::now();
        s.record_event(&e).unwrap();
        lat.push(t.elapsed().as_micros() as i64);
    }
    let wall = t0.elapsed();
    let mut l = lat.clone();
    let (p50, p95, p99) = (
        percentile(&mut l.clone(), 50.0),
        percentile(&mut l.clone(), 95.0),
        percentile(&mut l.clone(), 99.0),
    );
    let size = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
    eprintln!(
        "100k events: wall={}ms write p50={}us p95={}us p99={}us throughput={}/s db={}B",
        wall.as_millis(),
        p50,
        p95,
        p99,
        (100_000_000 / wall.as_millis().max(1)),
        size
    );
    assert!(p99 < 50_000, "write p99 < 50ms at 100k");
    // Full read: the ORDER BY timestamp full scan.
    let t0 = std::time::Instant::now();
    let all = s.query_events(&EventQuery::default()).unwrap();
    let qms = t0.elapsed().as_millis();
    eprintln!("full query 100k: {qms}ms rows={}", all.len());
    assert_eq!(all.len(), 100_000);
    assert!(qms < 5000, "full query at 100k < 5s");
    drop(s);
    let t0 = std::time::Instant::now();
    let s2 = Storage::open(&p).unwrap();
    let rms = t0.elapsed().as_millis();
    eprintln!("reopen 100k: {rms}ms");
    assert!(rms < 5000, "reopen < 5s");
    assert_eq!(
        s2.query_events(&EventQuery::default()).unwrap().len(),
        100_000
    );
}

#[test]
fn event_growth_250k_stop_rule() {
    // Measurement-only step. Stop rule: skip (pass vacuously with notice)
    // if the 100k pace projects this step past the 12-minute budget.
    let p = tmp_db("ev250pace");
    let _ = std::fs::remove_file(&p);
    let s = Storage::open(&p).unwrap();
    let t0 = std::time::Instant::now();
    for i in 0..10_000 {
        s.record_event(&Event::new("Tick", "bench", serde_json::json!({"i": i})))
            .unwrap();
    }
    let pace_ms = t0.elapsed().as_millis() as f64 / 10_000.0;
    let projected_min = pace_ms * 250_000.0 / 60_000.0;
    eprintln!("10k pace: {pace_ms:.2}ms/op, 250k projected: {projected_min:.1} min");
    if projected_min > 12.0 {
        eprintln!("STOP RULE: skipping 250k fill, recording max reached = 100k");
        return;
    }
    for i in 10_000..250_000 {
        s.record_event(&Event::new("Tick", "bench", serde_json::json!({"i": i})))
            .unwrap();
    }
    let size = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
    eprintln!("250k events: db={size}B");
    let t0 = std::time::Instant::now();
    let n = s.query_events(&EventQuery::default()).unwrap().len();
    eprintln!("full query 250k: {}ms", t0.elapsed().as_millis());
    assert_eq!(n, 250_000);
}
