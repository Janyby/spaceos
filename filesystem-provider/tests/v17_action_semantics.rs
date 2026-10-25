//! V1.7 Phase 3 — per-type end-effect semantics of the real provider
//! actions. Core delivers at-least-once (see space-core v17_actions);
//! here: what the SECOND application of each type does to Reality.
#![cfg(windows)]
use filesystem_provider::actions::perform;
use serde_json::json;

fn tmp(tag: &str) -> std::path::PathBuf {
    // Each test gets an ISOLATED dir: the previous shared-dir design raced
    // under parallel test threads (one test's remove_dir_all wiped
    // another's files). Qualification Gate 0 finding, harness-side fix.
    let d = std::env::temp_dir().join("spaceos-v17pa").join(tag);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn p(dir: &std::path::Path, n: &str) -> String {
    dir.join(n).to_string_lossy().to_string()
}

#[tokio::test]
async fn create_refuses_to_overwrite() {
    // Spec rule: create never overwrites silently; an existing file must
    // be edited through `write`. The second create fails LOUDLY and the
    // original bytes are preserved.
    let d = tmp("create");
    let t = json!({"path": p(&d, "a.txt"), "content": "hi"});
    assert!(perform("create", &t).await.is_ok());
    let again = perform("create", &t).await;
    assert!(again.is_err(), "re-create must fail instead of overwriting");
    assert!(
        again.unwrap_err().to_string().contains("already exists"),
        "refusal names the safe alternative"
    );
    assert_eq!(std::fs::read_to_string(d.join("a.txt")).unwrap(), "hi");
}

#[tokio::test]
async fn write_is_naturally_idempotent() {
    let d = tmp("write");
    std::fs::write(d.join("a.txt"), "old").unwrap();
    let t = json!({"path": p(&d, "a.txt"), "content": "new"});
    assert!(perform("write", &t).await.is_ok());
    assert!(perform("write", &t).await.is_ok());
    assert_eq!(std::fs::read_to_string(d.join("a.txt")).unwrap(), "new");
}

#[tokio::test]
async fn delete_second_apply_fails_cleanly() {
    let d = tmp("delete");
    std::fs::write(d.join("a.txt"), "x").unwrap();
    let t = json!({"path": p(&d, "a.txt")});
    assert!(perform("delete", &t).await.is_ok());
    // Second delete: target gone. Must fail LOUDLY, not corrupt.
    assert!(
        perform("delete", &t).await.is_err(),
        "delete is not idempotent; failure is explicit"
    );
}

#[tokio::test]
async fn rename_second_apply_fails_cleanly() {
    let d = tmp("rename");
    std::fs::write(d.join("a.txt"), "x").unwrap();
    let t = json!({"from": p(&d, "a.txt"), "to": p(&d, "b.txt")});
    assert!(perform("rename", &t).await.is_ok());
    assert!(
        perform("rename", &t).await.is_err(),
        "rename is not idempotent; source is gone"
    );
    assert!(d.join("b.txt").exists(), "first apply still holds");
}

#[tokio::test]
async fn move_copy_semantics() {
    let d = tmp("movecopy");
    std::fs::write(d.join("a.txt"), "x").unwrap();
    let cp = json!({"from": p(&d, "a.txt"), "to": p(&d, "c.txt")});
    assert!(perform("copy", &cp).await.is_ok());
    assert!(
        perform("copy", &cp).await.is_ok(),
        "copy over existing is benign"
    );
    let mv = json!({"from": p(&d, "a.txt"), "to": p(&d, "m.txt")});
    assert!(perform("move", &mv).await.is_ok());
    assert!(
        perform("move", &mv).await.is_err(),
        "move is not idempotent"
    );
}

#[tokio::test]
async fn unknown_action_rejected() {
    let d = tmp("unknown");
    assert!(
        perform("definitely-not-an-action", &json!({"path": p(&d, "x")}))
            .await
            .is_err()
    );
}
