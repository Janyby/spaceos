//! SpaceTrace Slice 3: Storage transaction hooks emit begin/commit/rollback
//! with explicit correlation, preserving atomic semantics.
//!
//! NOTE: the three tests serialize on the global IPC lock (see
//! support/global_lock.rs). Each test installs its own SpaceTrace into
//! the process-global hooks (last-wins overwrite, decision #22) while
//! asserting on its OWN provider — parallel execution misdelivers
//! traces across tests (same class as spacetrace_ipc).

use serde_json::json;
use space_core::{Object, Storage};

#[path = "support/global_lock.rs"]
mod global_lock;

fn tmp_db(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-st-slice3");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{name}.db"))
}

fn fresh_storage(name: &str) -> Storage {
    let p = tmp_db(name);
    let _ = std::fs::remove_file(&p);
    Storage::open(&p).unwrap()
}

#[test]
fn transaction_commit_emits_traced_spans() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let st = spacetrace::SpaceTrace::new().unwrap();
    spacetrace::init_global_hooks(st.buffer(), st.correlation());

    let mut s = fresh_storage("tx-commit");
    let corr = "tx-test-commit-001";
    s.transaction_with_correlation(corr, |tx| {
        for i in 0..5 {
            let mut o = Object::new("File", "test", json!({"i": i}));
            o.id = format!("tx-obj-{i}");
            tx.create_object(&o).unwrap();
        }
        Ok(())
    })
    .unwrap();

    let chain = st.provider().query_by_correlation(corr);
    let kinds: Vec<String> = chain.iter().map(|e| format!("{:?}", e.kind)).collect();
    assert!(
        kinds.contains(&"TransactionBegin".to_string()),
        "expected TransactionBegin in {kinds:?}"
    );
    assert!(
        kinds.contains(&"TransactionCommit".to_string()),
        "expected TransactionCommit in {kinds:?}"
    );
    assert!(
        !kinds.contains(&"TransactionRollback".to_string()),
        "no rollback expected in {kinds:?}"
    );

    // Semantic state committed.
    let objs = s
        .query_objects(&space_core::query::ObjectQuery::default())
        .unwrap();
    assert_eq!(objs.len(), 5);
}

#[test]
fn transaction_rollback_emits_traced_span_and_no_state() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let st = spacetrace::SpaceTrace::new().unwrap();
    spacetrace::init_global_hooks(st.buffer(), st.correlation());

    let mut s = fresh_storage("tx-rollback");
    let corr = "tx-test-rollback-001";
    let r: anyhow::Result<()> = s.transaction_with_correlation(corr, |tx| {
        let mut o = Object::new("File", "test", json!({}));
        o.id = "tx-obj-rollback".to_string();
        tx.create_object(&o).unwrap();
        anyhow::bail!("simulated failure");
    });
    assert!(r.is_err());

    let chain = st.provider().query_by_correlation(corr);
    let kinds: Vec<String> = chain.iter().map(|e| format!("{:?}", e.kind)).collect();
    assert!(kinds.contains(&"TransactionBegin".to_string()));
    assert!(kinds.contains(&"TransactionRollback".to_string()));
    assert!(!kinds.contains(&"TransactionCommit".to_string()));

    // Atomic: nothing persisted.
    let objs = s
        .query_objects(&space_core::query::ObjectQuery::default())
        .unwrap();
    assert_eq!(objs.len(), 0);
    assert_eq!(s.integrity_check().unwrap(), "ok");
}

#[test]
fn plain_transaction_generates_tx_correlation() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let st = spacetrace::SpaceTrace::new().unwrap();
    spacetrace::init_global_hooks(st.buffer(), st.correlation());

    let mut s = fresh_storage("tx-plain");
    s.transaction(|tx| {
        let mut o = Object::new("File", "test", json!({}));
        o.id = "tx-obj-plain".to_string();
        tx.create_object(&o).unwrap();
        Ok(())
    })
    .unwrap();

    // Generated tx-<uuid> correlation indexed.
    let stats = st.provider().correlation_stats();
    assert!(stats.total_events >= 2, "begin+commit expected");
}
