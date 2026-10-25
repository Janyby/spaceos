//! Roots gate: the real provider daemon refuses paths outside the
//! configured allowed roots, through the real IPC pipe.
//!
//! Isolated harness (post-I.5 Provider identity/IPC contract; see
//! docs/maintenance/SPACEOS_ROOTS_GATE_FIX.md and the companion diagnosis
//! SPACEOS_ROOTS_GATE_DIAGNOSIS.md): every run generates synthetic
//! credentials (provider principal, instance identity, dispatch key, Core
//! transport token), injects the matching Core identities, and serves the
//! test-owned Core plus the real filesystem-provider daemon on per-run
//! unique `spaceos-4-3a4-` pipes. No fixed machine-global pipe and no
//! production credential is ever used. Readiness requires the exact
//! test-owned provider instance to answer `ping` with its live dispatch
//! instance id (the identity Core itself binds dispatch proofs to); a
//! generic `list_providers` response is never accepted as proof. Daemon
//! startup failures surface immediately, and a failed pipe connect names
//! the expected pipe. Provider `action` calls carry per-call dispatch
//! proofs, mirroring how the Core forwards actions. All security
//! assertions of the original roots_gate (steps 1-8) are preserved. The
//! global IPC test lock is still honored so this suite never overlaps a
//! fixed-pipe suite that may still run on the machine.
#![cfg(windows)]

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};
use space_core::{
    dispatch, ipc, principal::PrincipalRegistry, provider_observation::ObservationBindings,
    service_registration::ServiceRegistrarRegistry, ActionRequest, Storage,
};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[path = "support/global_lock.rs"]
mod global_lock;

/// Daemon startup failures land here the moment the daemon task ends.
type DaemonFailures = Arc<Mutex<Vec<String>>>;

/// A synthetic 32-byte credential (isolated-suite credential format: 32
/// raw bytes, base64url-encoded for transport in env/params).
fn credential32() -> [u8; 32] {
    let mut bytes = [0u8; 32];
    let a = uuid::Uuid::new_v4();
    let c = uuid::Uuid::new_v4();
    bytes[..16].copy_from_slice(a.as_bytes());
    bytes[16..].copy_from_slice(c.as_bytes());
    bytes
}

fn b64(bytes: &[u8; 32]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Isolated lab root, plus whether this run had to synthesize the
/// `SPACEOS_TEST_ISOLATION_ROOT` env itself: the canonical test runner
/// provides the repository root; without a runner the test derives it from
/// its own crate location instead of inheriting a production environment.
fn prepare_lab() -> (PathBuf, bool) {
    match std::env::var("SPACEOS_TEST_ISOLATION_ROOT") {
        Ok(lab) if !lab.trim().is_empty() => (PathBuf::from(lab), false),
        _ => {
            let lab = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("the test crate's parent is the repository root")
                .to_path_buf();
            std::env::set_var("SPACEOS_TEST_ISOLATION_ROOT", &lab);
            (lab, true)
        }
    }
}

fn daemon_failure(failures: &DaemonFailures) -> Option<String> {
    failures.lock().unwrap().first().cloned()
}

/// Readiness of the EXACT test-owned provider instance:
///
/// 1. the only Core that can answer on the run-unique core pipe has the
///    filesystem provider registered (no pre-existing process can serve
///    that pipe name), and
/// 2. the provider pipe answers `ping` with a non-empty live dispatch
///    instance id — the per-process identity that Core dispatch binds
///    action proofs to (space-core/src/ipc.rs::forward_action_to_provider;
///    a provider restart yields a new id, so every proof binds to the
///    CURRENT daemon).
///
/// A generic `list_providers` response carries no instance identity and is
/// never accepted alone. Panics name both expected pipes and the first
/// daemon failure instead of timing out silently.
async fn wait_provider_owned(core_pipe: &str, provider_pipe: &str, failures: &DaemonFailures) {
    let timeout = Duration::from_secs(30);
    let start = Instant::now();
    let mut last = "no successful probe yet".to_owned();
    while start.elapsed() < timeout {
        // Child/daemon startup failures surface immediately.
        if let Some(fail) = daemon_failure(failures) {
            panic!(
                "roots_gate harness: test-owned daemon failed during startup: {fail}\
                 \nexpected Core pipe: {core_pipe}\
                 \nexpected provider pipe: {provider_pipe}"
            );
        }
        let mut registered = false;
        match ipc::send_request(core_pipe, "list_providers", json!({})).await {
            Ok(r) => {
                registered = r["result"]
                    .as_array()
                    .map(|a| a.iter().any(|p| p["id"] == "filesystem"))
                    .unwrap_or(false);
            }
            Err(e) => last = format!("core list_providers: {e:#}"),
        }
        let mut instance_ready = false;
        match ipc::send_request(provider_pipe, "ping", json!({})).await {
            Ok(r) => {
                if r["result"]["instance_id"].as_str().unwrap_or("").is_empty() {
                    last = format!("provider ping without dispatch instance id: {r}");
                } else {
                    instance_ready = true;
                }
            }
            Err(e) => last = format!("provider pipe ping: {e:#}"),
        }
        if registered && instance_ready {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!(
        "filesystem provider never ready on the test-owned pipes within 30s\
         \nexpected Core pipe: {core_pipe}\
         \nexpected provider pipe: {provider_pipe}\
         \nlast probe: {last}"
    );
}

/// Dispatch one action through the REAL provider pipe exactly the way the
/// Core forwards actions (space-core/src/ipc.rs::forward_action_to_provider):
/// `ping` for the live dispatch instance id, issue an operation-bound
/// dispatch proof with the run's dispatch key, then send. A FRESH proof is
/// issued per call — the provider verifier rejects missing, stale,
/// replayed, tampered, or foreign-instance proofs. Panics name the
/// expected pipe instead of failing on an opaque `unwrap`.
async fn action_on_pipe(
    provider_pipe: &str,
    dispatch_key: &str,
    action: &str,
    target: Value,
) -> Value {
    let req = ActionRequest::new(action, target, "filesystem", "tester");
    let ping = ipc::send_request(provider_pipe, "ping", json!({}))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "roots_gate harness: expected the test-owned provider instance listening on {provider_pipe} to answer 'ping': {e:#}"
            )
        });
    let instance = ping["result"]["instance_id"]
        .as_str()
        .unwrap_or_else(|| panic!("provider ping without dispatch instance id: {ping}"))
        .to_owned();
    let proof = dispatch::issue(dispatch_key, &instance, &req).unwrap_or_else(|e| {
        panic!("roots_gate: cannot issue dispatch proof for instance {instance}: {e:#}")
    });
    let mut params = json!(req);
    params["dispatch_proof"] = json!(proof);
    let resp = ipc::send_request(provider_pipe, "action", params)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "roots_gate harness: expected the test-owned provider instance listening on {provider_pipe} to answer 'action': {e:#}"
            )
        });
    resp["result"].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn roots_gate_end_to_end() {
    let _ipc_lock = global_lock::acquire_global_ipc();

    let (lab_raw, generated_isolation) = prepare_lab();
    let lab = lab_raw
        .canonicalize()
        .unwrap_or_else(|e| panic!("isolation lab root {lab_raw:?} unreadable: {e:#}"));

    // All test mutations are confined to this per-run base under
    // `<lab>/test-tmp` (the isolated filesystem-root rule the provider
    // enforces at startup). Nothing outside it is touched, and it is
    // removed at cleanup.
    let base = lab
        .join("test-tmp")
        .join(format!("roots-gate-{}", uuid::Uuid::new_v4()));
    let allowed = base.join("allowed");
    let outside = base.join("outside");
    std::fs::create_dir_all(&allowed).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("secret.txt"), "nope").unwrap();

    // Synthetic credentials: provider identity, instance identity, dispatch
    // key, Core transport token — fresh per run, all mutually distinct,
    // never equal to a production credential.
    let principal_secret = credential32();
    let principal_token = b64(&principal_secret);
    let instance_token = b64(&credential32());
    let system_secret = credential32();
    let system_token = b64(&system_secret);
    let dispatch_key = b64(&credential32());
    let instance_id = "roots-gate-fixture";
    let mut generated_envs: Vec<&'static str> = Vec::new();
    // Core transport token: honor the runner's canonical token when one is
    // already configured; otherwise generate one so the test stays
    // self-contained. Isolated tests require an explicit token and must
    // never fall back to the production credential file.
    if std::env::var("SPACEOS_CORE_TOKEN")
        .map(|t| t.trim().is_empty())
        .unwrap_or(true)
    {
        std::env::set_var("SPACEOS_CORE_TOKEN", b64(&credential32()));
        generated_envs.push("SPACEOS_CORE_TOKEN");
    }

    // Per-run unique test pipes (the isolated-IPC `spaceos-4-3a4-` naming):
    // nothing pre-existing on this machine can answer these names, so the
    // test never connects to the production Core pipe or a global one.
    let run_tag = uuid::Uuid::new_v4();
    let core_pipe = format!(r"\\.\pipe\spaceos-4-3a4-roots-gate-{run_tag}-core");
    let fs_pipe = format!(r"\\.\pipe\spaceos-4-3a4-roots-gate-{run_tag}-fs");
    let browser_pipe = format!(r"\\.\pipe\spaceos-4-3a4-roots-gate-{run_tag}-browser");

    // Restrict this process's daemon before it computes roots at startup.
    std::env::set_var(
        "SPACEOS_ALLOWED_ROOTS",
        allowed.to_string_lossy().to_string(),
    );
    // I.5 Provider identity/IPC contract for this run's daemon (mirrors
    // filesystem-provider/tests/i5_reconciliation.rs).
    std::env::set_var("SPACEOS_PROVIDER_PRINCIPAL_TOKEN", &principal_token);
    std::env::set_var("SPACEOS_PROVIDER_INSTANCE_ID", instance_id);
    std::env::set_var("SPACEOS_PROVIDER_INSTANCE_TOKEN", &instance_token);
    std::env::set_var("SPACEOS_DISPATCH_KEY_FILESYSTEM", &dispatch_key);
    // Test-owned pipes for every IPC hop in this process.
    std::env::set_var("SPACEOS_CORE_PIPE", &core_pipe);
    std::env::set_var("SPACEOS_FILESYSTEM_PROVIDER_PIPE", &fs_pipe);
    std::env::set_var("SPACEOS_FIXTURE_DISABLE_PROCESS_OBSERVER", "1");

    // Serve root inside the allowed tree so the initial scan is compliant.
    let serve_root = allowed.join("srv");
    std::fs::create_dir_all(&serve_root).unwrap();

    // Core-side identities injected explicitly (isolated tests never read
    // inherited production principal credentials): "filesystem" registers
    // its provider types/instance and reports observations (same synthetic
    // capability set as i5_reconciliation.rs); "system" delegates
    // `action.create` to the agent used by the final Core-routed step.
    // All credentials are distinct by construction.
    let transport_token = ipc::core_token();
    let mut principals = PrincipalRegistry::default();
    principals
        .insert(
            "filesystem",
            principal_secret,
            [
                "provider.register",
                "filesystem.reconcile.read",
                "observation.report",
                "object.update",
                "event.record",
            ],
            ["A"],
        )
        .unwrap();
    principals
        .insert(
            "system",
            system_secret,
            ["agent.register", "agent.grant.action.create"],
            [],
        )
        .unwrap();

    // Instance identity binding: `register_provider_instance` fails closed
    // ("denied: unknown or unauthenticated Provider instance") unless the
    // Core was started with a binding matching the instance id/token and
    // the served root. Inject ours before serving (same mechanism as
    // i5_reconciliation.rs uses for its synthetic source instance).
    let bindings = ObservationBindings::from_json(&json!([{
        "provider": "filesystem",
        "instance_id": instance_id,
        "instance_token": instance_token,
        "root": serve_root.to_string_lossy(),
        "world_id": "A",
    }])
    .to_string())
    .unwrap_or_else(|e| panic!("roots_gate harness: observation binding rejected: {e:#}"));
    bindings
        .validate_distinct_principals(&principals, &transport_token)
        .unwrap_or_else(|e| panic!("roots_gate harness: synthetic credentials conflict: {e:#}"));

    let db = base.join("core.db");
    let mut state = ipc::CoreState::new_with_semantic_world_and_identities(
        Storage::open(&db).unwrap(),
        None,
        &fs_pipe,      // Core routes filesystem actions to OUR provider pipe.
        &browser_pipe, // Browser observer is opt-in and stays off.
        principals,
        ServiceRegistrarRegistry::default(),
        transport_token,
    );
    Arc::get_mut(&mut state).unwrap().observation_bindings = bindings;

    // Test-owned daemons with immediate startup-failure surfacing: each
    // task reports its outcome into the shared slot; the readiness loop
    // fails fast with that diagnosis instead of timing out on a quiet
    // pipe. Both daemons run in-process (no child processes to manage).
    let daemon_failures: DaemonFailures = Arc::new(Mutex::new(Vec::new()));
    let core_slot = daemon_failures.clone();
    let core_pipe_serve = core_pipe.clone();
    let core_srv = tokio::spawn(async move {
        if let Err(e) = ipc::run_server(state, &core_pipe_serve).await {
            core_slot
                .lock()
                .unwrap()
                .push(format!("Core daemon exited before test end: {e:#}"));
        }
    });
    let prov_slot = daemon_failures.clone();
    let prov_root = serve_root.clone();
    let prov = tokio::spawn(async move {
        match filesystem_provider::ipc::run_server("filesystem".into(), Some(prov_root)).await {
            Err(e) => prov_slot
                .lock()
                .unwrap()
                .push(format!("filesystem provider daemon exited before test end: {e:#}")),
            Ok(()) => prov_slot
                .lock()
                .unwrap()
                .push("filesystem provider daemon exited unexpectedly (clean exit)".to_owned()),
        }
    });

    wait_provider_owned(&core_pipe, &fs_pipe, &daemon_failures).await;

    // Helper closures capturing THIS run's owned pipe + dispatch key.
    let do_action = |action: &str, target: Value| {
        let pipe = fs_pipe.clone();
        let key = dispatch_key.clone();
        let action = action.to_owned();
        async move { action_on_pipe(&pipe, &key, &action, target).await }
    };

    // 1. Roots method reports the restriction honestly.
    let roots = ipc::send_request(&fs_pipe, "roots", json!({}))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "roots_gate harness: expected the test-owned provider instance listening on {fs_pipe} to answer 'roots': {e:#}"
            )
        });
    assert_eq!(roots["ok"], true, "roots: {roots}");
    assert_eq!(roots["result"]["mode"], "restricted", "roots: {roots}");

    // 2. Create inside roots works (direct pipe: isolates the gate).
    let inside = allowed.join("a.txt");
    let r = do_action(
        "create",
        json!({"path": inside.to_string_lossy(), "content": "hi"}),
    )
    .await;
    assert_eq!(r["status"], "Completed", "create inside: {r}");
    assert!(inside.exists());

    // 3. Create outside roots is refused loudly, nothing written.
    let evil = outside.join("evil.txt");
    let r = do_action(
        "create",
        json!({"path": evil.to_string_lossy(), "content": "x"}),
    )
    .await;
    assert_eq!(r["status"], "Failed", "create outside: {r}");
    assert!(
        r["detail"]["error"]
            .as_str()
            .unwrap_or("")
            .contains("outside allowed roots"),
        "diagnosis names the rule: {r}"
    );
    assert!(!evil.exists(), "nothing written outside roots");

    // 4. `..` escape is refused even when the lexical path starts inside.
    let esc = format!(r"{}\..\evil2.txt", allowed.to_string_lossy());
    let r = do_action("create", json!({"path": esc, "content": "x"})).await;
    assert_eq!(r["status"], "Failed", "escape: {r}");

    // 5. Read outside roots is refused; read inside works.
    let r = do_action(
        "read",
        json!({"path": outside.join("secret.txt").to_string_lossy()}),
    )
    .await;
    assert_eq!(r["status"], "Failed", "read outside: {r}");
    let r = do_action("read", json!({"path": inside.to_string_lossy()})).await;
    assert_eq!(r["status"], "Completed", "read inside: {r}");
    assert_eq!(r["detail"]["content"], "hi");

    // 6. Scan outside roots fails fast (no Started job for denied work).
    let r = do_action("scan", json!({"path": outside.to_string_lossy()})).await;
    assert_eq!(r["status"], "Failed", "scan outside: {r}");

    // 7. Optimistic concurrency: stale stamp refused, fresh stamp wins.
    let f = allowed.join("race.txt");
    let r = do_action(
        "create",
        json!({"path": f.to_string_lossy(), "content": "v1"}),
    )
    .await;
    assert_eq!(r["status"], "Completed", "create race.txt: {r}");
    let r = do_action("read", json!({"path": f.to_string_lossy()})).await;
    assert_eq!(r["status"], "Completed", "read race.txt: {r}");
    let stamp = r["detail"]["modified"].as_str().unwrap_or("").to_string();
    assert!(!stamp.is_empty(), "read returns a modified stamp");
    let size = r["detail"]["size"].as_u64().unwrap();
    // Simulate a concurrent change behind our back.
    std::fs::write(&f, "v2-sneaky").unwrap();
    let r = do_action(
        "write",
        json!({"path": f.to_string_lossy(), "content": "v3",
               "expected_modified": stamp, "expected_size": size}),
    )
    .await;
    assert_eq!(r["status"], "Failed", "stale write: {r}");
    assert!(
        r["detail"]["error"]
            .as_str()
            .unwrap_or("")
            .contains("conflict:"),
        "conflict is machine-readable: {r}"
    );
    assert_eq!(std::fs::read_to_string(&f).unwrap(), "v2-sneaky");
    // Re-read, then the write lands.
    let r = do_action("read", json!({"path": f.to_string_lossy()})).await;
    let stamp2 = r["detail"]["modified"].as_str().unwrap().to_string();
    let size2 = r["detail"]["size"].as_u64().unwrap();
    let r = do_action(
        "write",
        json!({"path": f.to_string_lossy(), "content": "v3",
               "expected_modified": stamp2, "expected_size": size2}),
    )
    .await;
    assert_eq!(r["status"], "Completed", "fresh write: {r}");
    assert_eq!(std::fs::read_to_string(&f).unwrap(), "v3");

    // 8. Same denial through the full Core flow (policy + gate composed).
    // Registering the agent requires an authenticated "system" principal:
    // the privileged credential travels as `principal_token` (the Core
    // transport token is deliberately not a principal credential,
    // principal_authenticated in space-core/src/ipc.rs).
    let agent = ipc::send_request(
        &core_pipe,
        "register_agent",
        json!({"principal": "system", "principal_token": system_token,
               "name": "roots-tester",
               "capabilities": ["action.create"], "worlds": []}),
    )
    .await
    .unwrap_or_else(|e| panic!("register_agent on {core_pipe}: {e:#}"));
    let agent_token = agent["result"]["token"]
        .as_str()
        .unwrap_or_else(|| panic!("register_agent: {agent}"))
        .to_string();
    let mut req = ActionRequest::new(
        "create",
        json!({"path": evil.to_string_lossy(), "content": "x"}),
        "filesystem",
        "roots-tester",
    );
    req.actor_token = agent_token;
    let r = ipc::send_request(&core_pipe, "request_action", json!(req))
        .await
        .unwrap_or_else(|e| panic!("request_action on {core_pipe}: {e:#}"));
    // Policy passed (cap present) so Core executed; the provider refused.
    assert_eq!(r["result"]["status"], "Failed", "core-routed denial: {r}");
    assert!(!evil.exists());
    // Strengthened beyond the original: the Failed status must come from
    // the provider's own gate (the same rule as step 3), which proves Core
    // really dispatched the action to the test-owned provider instance
    // (a dispatch/transport failure would be a different error here).
    assert!(
        r["result"]["detail"]["error"]
            .as_str()
            .unwrap_or("")
            .contains("outside allowed roots"),
        "core-routed denial must come from the provider gate, not a dispatch failure: {r}"
    );

    // Belt and braces: no test-owned daemon exited while the gate
    // assertions were running (that would have made some assertion fail
    // for the wrong reason).
    if let Some(fail) = daemon_failure(&daemon_failures) {
        panic!(
            "roots_gate harness: test-owned daemon exited while the gate assertions ran: {fail}"
        );
    }

    // Cleanup: stop only test-owned processes. The daemons are in-process
    // tasks of this very test; aborting them closes the run-unique pipes
    // (no external process names were ever used).
    prov.abort();
    core_srv.abort();
    tokio::time::sleep(Duration::from_millis(100)).await; // let aborts land
    for name in [
        "SPACEOS_PROVIDER_PRINCIPAL_TOKEN",
        "SPACEOS_PROVIDER_INSTANCE_ID",
        "SPACEOS_PROVIDER_INSTANCE_TOKEN",
        "SPACEOS_DISPATCH_KEY_FILESYSTEM",
        "SPACEOS_CORE_PIPE",
        "SPACEOS_FILESYSTEM_PROVIDER_PIPE",
        "SPACEOS_FIXTURE_DISABLE_PROCESS_OBSERVER",
        "SPACEOS_ALLOWED_ROOTS", // removed last, as the original suite did
    ] {
        std::env::remove_var(name);
    }
    for name in generated_envs {
        std::env::remove_var(name);
    }
    let _ = std::fs::remove_dir_all(&base);
    println!(
        "ROOTS_GATE PASS: all original gate assertions held on the test-owned pipes\
         \n  core pipe: {core_pipe}\
         \n  provider pipe: {fs_pipe}\
         \n  instance: {instance_id} (per-run directory removed)"
    );
    if generated_isolation {
        println!("ROOTS_GATE: isolation root was synthesized for this run (no runner env).");
    }
}
