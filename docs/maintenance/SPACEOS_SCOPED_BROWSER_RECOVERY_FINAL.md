# Scoped Browser UI and recovery — final report

Date: 2026-10-25 (Europe/Moscow). **PASS for the executed bounded UI/HTTP/recovery checks. No production/deployment claim.**

## 1. Confirmed causes and exact changes

The approved implementation report correctly identified three limitations: absent mode metadata was treated as legacy in frontend booleans, scoped failures used the general 500 mapper, and stale service credentials were only discarded for a later lazy registration.

Only Bridge/frontend code and focused tests were changed in this task:

| File | Functions / correction |
|---|---|
| `space-bridge/src/state.rs` | `BrowserFailureKind`, `BrowserFailure`, `scoped_core_failure`, `scoped_transport_failure`: scoped-only, conservative error classification. `browser_capabilities`: authoritative server-configured Browser action descriptor. `browser_world_action`, `ensure_browser_world_agent`: separate authenticated registration from execution; refresh only after the exact pre-dispatch authorization error, without resending the action |
| `space-bridge/src/handlers.rs` | `core_err`: typed scoped categories; unknown/legacy errors keep existing behavior. Existing generic Browser response, `get_browser_tabs` and `context_endpoint` expose `browser_capabilities` alongside `browser_mode`. One regression test covers status mapping and unchanged legacy/timeout categories |
| `space-bridge/static/world.html` | Initial Browser mutation controls have static `disabled` attributes. `canBrowser`, `applyBrowserMode`, `loadBrowserAuthority`: unknown/incomplete/invalid descriptor is unavailable, not legacy; load metadata at initialization/login and from successful tab responses; clear permissions on load failure. Sidebar/panel action handlers and `bfShot` require the relevant loaded capability before HTTP. Canonical identity and full tuple checks remain |
| `space-bridge/static/index.html` | Static disabled Browser controls. `shellCanBrowser`, `applyShellBrowserAuthority`, `loadBrowserAuthority`: same explicit ready/capability state; load context without requiring a Get Tabs click. Existing action handlers guard before HTTP; failed tab load disables/removes stale operational controls |
| `tests/scoped_browser_recovery_ui.cjs` | Execute real World/Shell helper functions with controlled DOM doubles; initial HTML disabled assertions and script compilation |
| `tests/vertical_slice_ii_ui.cjs` | Existing extracted-function fixture supplies its capability helper double; existing identity/action assertions retained |
| `tests/vertical_slice_ii_corrections_runtime.cjs` | Optional recovery mode, separate evidence output, actual UI checkpoint, controlled Core/Provider restart, post-restart HTTP checks and subsequent Bridge restart |
| `tests/scoped_browser_actions.cjs` | Optional recovery screenshot output name, preserving the prior stage's image artifact |
| `tests/scoped_browser_recovery_checks.cjs` | Real HTTP status and stale-request/no-event/subsequent-request assertions |
| `tests/scoped_browser_recovery_audit.cjs` | Independent artifact/persistence/hash/cleanup/known-fixture-credential audit |

No Core, Policy, service-registration profile, Provider, observation, Mission, SQLite schema or dispatch-proof source was changed. The pre-existing dirty worktree is not attributed to this task. The audit compares Core/Provider source hashes and prior evidence artifacts against the previous stage's inventory; all matched. Existing fixture executables were rebuilt/relinked in the existing target; production binaries/processes were not replaced.

## 2. Authority loading and compatibility

The server now advertises, for scoped mode:

```json
{"browser_mode":"scoped","browser_capabilities":["action.get_tabs","action.screenshot"]}
```

The same fields appear inside `/api/context`'s `context` and at the top of successful tab/action responses. This describes the server-configured, already-approved action surface; it is not a replacement for Core authentication, current World membership, live authority or availability checks.

Both interfaces require a recognized mode **and** a valid capability array before enabling an operation. Mode alone, unknown mode, missing capabilities, unexpected scoped capabilities or failed metadata loading do not enable legacy behavior. World retains the last recognized scoped identity mode on failure but clears operational permissions. Shell enters unavailable mode. Screenshot/activation handlers cannot issue HTTP based only on an enabled-looking element or a raw target.

Read-only discovery (`Get Tabs` and the metadata request) remains available to load authority; it still goes through existing HTTP authentication and Core checks. Navigation/new-tab/reload/content/activate/close controls remain disabled in scoped mode. A valid legacy descriptor enables only its declared existing actions after loading; no legacy capability was expanded. Initial disabled attributes prevent a pre-JavaScript or slow-response enabling window. Existing address fields and representations remain editable/viewable without granting an action.

## 3. HTTP categories and recovery contract

Classification applies only to typed errors created in the scoped Bridge path. Existing legacy errors are not broadly reclassified from vague strings.

| Condition | HTTP | Evidence |
|---|---:|---|
| Raw-only screenshot input / malformed or unsupported screenshot options | 400 | Real HTTP |
| Unsupported scoped Browser action | 403 | Real HTTP |
| Exact Core authentication/capability denial | 403 | Real stale-token HTTP request |
| Canonical object missing | 404 | Real HTTP |
| Canonical snapshot/authority conflict or missing matching live handle | 409 | Real forged historical handle HTTP request |
| Missing scoped config/registrar/registered source, recognized unavailable transport/protocol | 503 | Implemented classification; category mapping unit-tested, not every outage newly injected over HTTP |
| Recognized upstream screenshot/authority/empty-frame failure | 502 | Implemented classification; category mapping unit-tested, not every Provider failure newly injected through HTTP |
| Existing IPC timeout | 504 | Existing rule preserved and unit-tested; no real timeout injected |
| Unknown Core/internal failure | 500 | Intentionally retained; do not infer permission/absence from an unknown error |

Recovery occurs only for the exact current Core pre-dispatch error `denied: Browser World authentication or capability`:

1. Send the requested action once with the cached scoped actor credential.
2. If that exact authorization error returns, invalidate the cached token **only if it still equals the failed request's token**. A late failure cannot erase a newer concurrent registration.
3. Authenticate through the separate existing `browser_world` registrar and cache the resulting exact-profile identity for subsequent requests.
4. Return the failed request as 403, explicitly stating that identity was refreshed and the action was **not retried**.
5. A later deliberate request uses the refreshed identity. If registration is unavailable, return 503 and never fall back to dashboard.

Validation/conflict/missing-object failures do not churn the actor token. Ambiguous transport failure/timeout does not trigger action replay or the legacy retry path. Authority/profile conflicts during registration cannot be overwritten by this recovery. No new Policy grant or broader registration route was introduced.

## 4. One actual isolated restart workflow

Runtime evidence: `scoped-browser-recovery-runtime.json/.log`.

- Root: `C:\spaceos\test-tmp\vertical-corrections-8HV07i`; existing test-state location, not another laboratory/target tree.
- Origin: `http://127.0.0.1:21973`; synthetic page port 21974; owned Chrome CDP port 21975. No production 8765/9222 or production IPC/database was contacted.
- World: `32048895-0421-4122-b368-43dcf169982b`.
- Canonical tab: `browser-tab-v1-2742c581bbe49ef24eed197372f0d297e6422bdcd344ff4eb8abd5475358f2f9`.
- Real Chrome 155.0.8059.85, owned PID 31044 and profile under this fixture root.
- Initial Core 34896, Bridge 31448, Filesystem 14684, Browser 6464. Controlled restart: Core 23236, Filesystem 19500, Browser 2408. After stale-cache verification, Bridge restarted as 9548. These are historical evidence PIDs, not reusable stop instructions.
- Recorded run: 2026-10-25T03:32:05.103Z–03:35:24.824Z, i.e. 06:32:05–06:35:24 MSK.

Sequence actually performed:

1. Existing real four-process fixture checks: filesystem watcher creation/change and receipt, Browser admission, authenticated Bridge WS, negative/replay/foreign-World isolation, authorized signed Browser actions.
2. Open actual World, record all four unsupported controls disabled before login/authority; synthetic login, canonical search/Focus, real screenshot. Save before evidence; close this tab's timers before the controlled restart so the stale-request test is deterministic.
3. Stop/restart only owned Core/Providers against the same synthetic DB, source and still-owned Chrome. Missing registration denies; valid provider re-registration restores the same tab identity and changes the ephemeral dispatch performer. File receipt and tab snapshot persist.
4. With Bridge still running and its old actor cached, send one canonical screenshot through its real HTTP route. Actual response:

```json
{"ok":false,"error":"scoped Browser authorization denied; identity refreshed for subsequent requests; action not retried"}
```

HTTP **403**. The harness compared all scoped Action event counts immediately before/after: unchanged (`ActionRequested=18`, `ActionCompleted=17`, `ActionFailed=1`). The rejected screenshot therefore did not generate even a requested action/event, nor a fabricated success. Registration refresh itself did not execute a screenshot.

5. Issue a new, separate HTTP screenshot: **200 / Completed**, action `91351237-c514-42dd-89e4-5bebaf327305`, same canonical object, real JPEG 11,745 bytes, performer `8eafa98d-7412-4ecf-8124-6914cabf2393`.
6. Verify real HTTP 400/403/404/409 examples and unchanged canonical snapshot. Restart the owned Bridge too; reopen World, explicitly log into the new lab session, search the same canonical tab and receive a real rendered screenshot again. No seamless session persistence or automatic action replay is claimed.
7. In actual Shell, verify unsupported controls already disabled **before Get Tabs**, then query and display the same canonical tab. Close all created UI tabs and stop all owned runtime processes.

Final UI screenshot terminal event: `35db3c58-b48c-4051-bf4a-af4ca9f469da`, seq 121, actor `space-world-browser`, provider `browser`, exact World above, correlation `e5833dc9-a38b-42de-bd26-722b44bc412f`, performer `8eafa98d-7412-4ecf-8124-6914cabf2393`.

Evidence distinguishes real UI from doubles:

- `SCOPED_BROWSER_RECOVERY_BEFORE.png`, `SCOPED_BROWSER_RECOVERY_AFTER.png`: actual served World and the real synthetic Chrome frame before/after restart.
- `scoped-browser-recovery-before-ui.json`, `scoped-browser-recovery-after-ui.json`: initial disabled controls, same canonical ID, decoded image, UI state; post-restart World WebSocket connected; no captured warn/error console messages.
- `scoped-browser-recovery-shell-ui.json`: actual Shell controls before enumeration and the same tab afterward; no captured warn/error console messages.
- `SCOPED_BROWSER_RECOVERY_PROVIDER_SCREENSHOT.jpg`: real Provider JPEG from this run, separately named to preserve prior evidence.

The frame panel's existing timer produced repeated read-only frames when open; no navigation/update/close lifecycle or artificial model action was substituted. Existing authenticated WS FileChanged/BrowserTabCreated and World isolation assertions passed; the old raw collector was closed by Bridge restart, while the reopened real World established a new authenticated WS connection.

## 5. Commands, tests and inventory

Common Rust environment: existing `CARGO_TARGET_DIR=C:\spaceos\target`, `CARGO_INCREMENTAL=0`, `SPACEOS_TEST_ISOLATION_ROOT=C:\spaceos`, synthetic transport credential (not exported), `TEMP`/`TMP=C:\spaceos\test-tmp`. No production secret was loaded.

```powershell
cargo build --offline --locked -p space-bridge --features mvp-lab-runtime --bin mvp-core-fixture --bin mvp-runtime-fixture -j2
cargo test --offline --locked -p space-bridge --lib --test static_ui -j2 -- --test-threads=1
node tests/scoped_browser_recovery_ui.cjs
node tests/vertical_slice_ii_ui.cjs
node tests/contract_tests.js
node tests/mission_verification_ui.cjs
$env:SPACEOS_SCOPED_BROWSER_RECOVERY_TEST='1'
node tests/vertical_slice_ii_corrections_runtime.cjs
node tests/scoped_browser_recovery_audit.cjs
```

The existing driver waits for `RESTART` under its printed, validated fixture root after pre-restart UI capture, then waits for `STOP` after the post-restart UI check. Those harmless markers were created only inside the exact current test root. It records cleanup and preserves its DB/evidence. No owner/production process was stopped to release a binary.

| Executed suite | Result / log prefix `scoped-browser-recovery-` |
|---|---|
| Bridge library | 44/44, `bridge.log` |
| Existing static UI | 4/4, `bridge.log` |
| New authority helper/initial-disabled/script tests | 20/20, `ui-unit.log`; controlled DOM, not real UI |
| Existing extracted World contracts | 26/26, `existing-ui.log` |
| Frontend contracts | 103/103, `contracts.log`; static/HTTP-mock coverage |
| Mission verification UI | 11/11, `mission-ui.log`; DOM/HTTP doubles, no model Mission |
| Actual Core/Bridge/Provider/SQLite/Chrome/WS/restart HTTP | 72 assertions, `runtime.json/.log` |
| Independent post-run audit | 27/27, `audit.log` / `inventory.json` |

Total focused regression tests: **208 PASS**, separately **72 real runtime assertions** and **27 audit checks**. No new build/test assertion failures occurred in this task. Existing compiler warnings and CRLF advisories remain; `git diff --check` passed on the four implementation files. The previous stage's larger Core/Provider suites were not unnecessarily rerun or claimed as new executions; their protected source hashes remained identical and the actual provider path was exercised again.

Loaded binary SHA-256:

```text
mvp-core-fixture.exe    f206d6570e21bb6fbe66c99401aa19508b9c21654886686d42ab40113e2846ad
mvp-runtime-fixture.exe 298cfa80a5220b13a34dbc3f64be8d7fa32a0e05ed1b5ebe97febfafd565ada4
filesystem-provider.exe 72e28751327be3322e920a6658f8241a573c791723c1857c5f55c3345f8f1798
browser-provider.exe    a29ee38638a634679274a0c40a13e31b64ddf9f98336b3a9cbc070f39a0668dd
```

Paths: existing `C:\spaceos\target\debug`. Both Provider binary hashes match the approved prior run. Source/evidence/image hashes and every loaded PID/path are recorded in `scoped-browser-recovery-inventory.json`. Its audit verifies prior evidence artifact hashes are preserved, Core/Policy/Provider sources match the prior inventory, and dispatch-proof source matches its known prior hash. The manifest excludes its own output and actively written audit log from self-hashing.

Final SQLite integrity: `ok`; canonical Browser snapshot unchanged; one BrowserTabCreated for this canonical tab; all scoped action events in the exact World; original File admission receipt retained. No Mission was executed or reinterpreted as complete. All owned cleanup flags are true; a final process inspection found no Chrome process with this fixture profile path. Free disk was approximately 26.826 GB at entry and 26.713 GB after cleanup (decimal). No target tree/laboratory was duplicated or other laboratory files deleted.

## 6. Remaining limitations

- Error mapping deliberately depends on a bounded set of current Core messages because the approved task does not change the Core wire contract. Unknown errors remain 500; structured Core error codes would require a separate contract decision. 502/503/504 mappings are implemented/unit-tested, but this run does not claim every timeout/outage branch was injected through real HTTP.
- A stale actor's failing request still returns failure. Recovery prepares subsequent requests; it never promises success or replays an uncertain action. If registrar/source/config is unavailable, a later explicit refresh/request must try registration again. Concurrent token replacement handling was source-reviewed, not adversarially stress-tested.
- Full Bridge restart invalidates synthetic in-memory login sessions; explicit lab login is required. The existing general connection label can still call a missing/expired lab session “Bridge unavailable”; that unrelated status-label redesign was not included.
- Capability metadata is server configuration, not a UI security boundary or proof that the Provider is currently live. Core/Provider enforcement remains authoritative. Legacy dashboard is still broad/unscoped; production localhost caller authentication, credential provisioning and ACL decisions remain unresolved. CORS/Origin is not caller authentication.
- No changes to World zoom/navigation, semantic identity, Provider observations, browser lifecycle, Mission, SQLite schema or dispatch proofs. No production database access, permanent credential/profile change, deployment, commit or push.

**Final status: scoped UI readiness PASS; distinguishable HTTP categories PASS; no-replay stale-token recovery PASS; isolated restart/UI PASS; production UNTOUCHED; deployment NOT AUTHORIZED.**
