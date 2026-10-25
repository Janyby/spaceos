# Verified BrowserTabCreated implementation

Date: 2026-10-24. Repository: `C:\spaceos`. Owner-approved bounded contract: `SPACEOS_BROWSER_OBSERVATION_FINAL_DESIGN.md` and the implementation task attachment. This is isolated runtime verification, not production E2E or deployment readiness.

## Verdict

| Area | Result |
|---|---|
| CONTRACT | PASS — bounded authenticated BrowserTabCreated implementation and executed tests described below |
| REAL CHROME | PASS — dedicated Chrome/155.0.8059.85, synthetic profile/page, browser-level Target.getTargets |
| CORE PERSISTENCE | PASS — real Core IPC and SQLite, atomic object/event/log, scoped reads and committed subscription notification |
| IDENTITY / WORLD ISOLATION | PASS — tested credential, identity, source and World negative cases |
| RESTART / REPLAY | PASS — real Provider restart; Core restart with Provider alive; same-port replacement Chrome rejected |
| BRIDGE / WORLD | NOT TESTED — no Bridge, browser World UI, Shell or Bridge WebSocket assertion in this run |
| PRODUCTION | UNTOUCHED — no production database, provider endpoint, personal profile or deployment operation |

These PASS statements concern the specified creation-only contract and executed coverage, not all possible Browser lifecycle or CDP faults. Remaining unexecuted scenarios are listed below.

## 1. Real runtime identity was tested before implementation

Command: `node scripts/browser-observation-probe.mjs`.

Evidence: `docs/maintenance/browser-runtime-identity-probe.json`; fixture directory `test-tmp/browser-runtime-probe-5aae8fac-631a-4418-beaa-d6a15860b9da`.

Executable: `C:\Program Files\Google\Chrome\Application\chrome.exe`; reported Chrome/155.0.8059.85. A dedicated headless profile was launched on loopback CDP port 64424. Three independent browser-level WebSocket reconnects preserved the debugger identity, and Target.getTargets returned the synthetic page. The owned Chrome was closed and relaunched on the same port/profile; its debugger identity changed. Recorded debugger-ID SHA-256 values:

- Initial: `ae45ddfd1b7f8ce8d1a281c308eecbb9b3dc370b79144b749b1a095d97e20517`.
- Replacement: `5864f270081b3e1e62af95243e2ba265103492fc612a1350629dc2f8da12b4da`.

The source UUID is owner configuration, not derived from the port, URL, PID or browser version. The debugger guard detects runtime substitution; it is not cryptographic authentication of a hostile local CDP server or a substitute for operating-system isolation.

## 2. Exact implementation scope

| Changed file | Functions / change |
|---|---|
| `space-core/src/provider_observation.rs` | `ObservationBindings::{from_env,configure_browser,validate_distinct_principals,browser_authority,register_browser,verify_browser_tab_created}`; new strict `BrowserRuntimeConfig`, opaque verified admission/authority types, `browser_tab_identity`, `browser_tab_observation`, `sanitized_browser_url`, reserved-pin helper. Existing filesystem parser and observation/CAS contract preserved. |
| `space-core/src/security_config.rs` | `validate_present_credential_domains` validates optional Browser bindings and includes instance secrets in existing credential-domain separation. |
| `space-core/src/ipc.rs` | Existing `register_provider`, `register_provider_instance`, `report_observation` dispatch branches: reserve Core-owned Browser pins, authenticate explicit Browser instance/source/runtime registration, enforce exact-World principal grants, validate strict wire shape, return canonical object, publish only applied committed events. |
| `space-core/src/storage.rs` | `browser_pin`, `Storage::register_browser_authority`, `Storage::apply_verified_browser_tab_created`; private test-module declaration. Immediate transactions, true INSERT, immutable authority/runtime pins and first-admission floor, strict canonical replay. No DDL or schema version change. |
| `browser-provider/src/main.rs` | Validate explicit Browser source configuration and separate instance credential on startup when verified mode is selected. |
| `browser-provider/src/provider.rs` | `run_standalone`, new `verified_observation_cycle`, `execute_action`, state initialization. Explicit configuration selects a metadata-only discovery loop, registration each cycle, canonical cache adoption; errors clear live state and log a sanitized degraded warning. Signed actions retain their separate gate; configured runtime is checked before actions. |
| `browser-provider/src/cdp.rs` | `CdpClient::{pinned_browser,pinned_targets}`: exact local descriptor/approved debugger URL, bounded browser-level WebSocket, Target.getTargets, page/subtype filtering. Existing HTTP body helper rejects Content-Length over 1 MiB. No page scripts or background page sessions in verified discovery. |
| `browser-provider/src/objects.rs` | `BrowserTabObject::from_canonical` adopts Core identity, metadata and timestamps. Legacy constructor is not silently redefined. |
| `space-core/Cargo.toml`, `Cargo.lock` | Direct existing `url` 2.5.8 dependency for proper URL validation/normalization; lock resolution updated offline. |
| `browser-provider/Cargo.toml` | Test-only workspace rusqlite dependency for actual SQLite evidence. |
| `space-core/src/storage_browser_observation_tests.rs` | Six new storage/security tests, including injected failures, floors, credential rotation, collisions and reopen. |
| `browser-provider/tests/verified_observation.rs` | Real dedicated Chrome / actual Browser executable / isolated Core child / SQLite / authenticated IPC integration and negative cases. |
| `scripts/browser-observation-probe.mjs`, `scripts/browser-observation-real-test.mjs` | Test-owned Chrome lifecycle and synthetic HTTP fixture; no personal profiles or production ports. |

The worktree was already extensively dirty. This inventory describes this task, not every difference against Git HEAD. No unrelated files were reset, reformatted or committed.

## 3. Authority and persistence invariant

Core optional server configuration: `SPACEOS_BROWSER_OBSERVATION_BINDINGS`, strict Browser entries with provider, instance_id, instance_token, source_id, world_id, cdp_origin, runtime_epoch, expected_browser_debugger_id. Provider configuration: `SPACEOS_BROWSER_OBSERVATION_SOURCE` contains only the runtime/source fields; existing principal, instance and dispatch credential loaders are reused. No production values or credentials were selected or provisioned.

Browser instance credentials are separate from transport, privileged principal, filesystem instance and dispatch credentials. Principal authorization still requires `provider.register` and `observation.report` in the exact configured World. An instance token alone is not a principal grant; a principal/transport token is not instance proof. Browser needs no global read. The fixture reader is a separate explicitly registered synthetic semantic.read agent for World browser-A only.

Three reserved internal Provider records pin authority:

- `observation-instance:browser:<instance>` and `observation-source:browser:<source>`: authority fingerprint and immutable initial event-sequence floor.
- `observation-runtime:browser:<source>:<epoch>`: runtime fingerprint including configured debugger guard and the same floor.

Public register_provider cannot overwrite these namespaces, including through the system registration path. Exact re-registration preserves pins/floor; changed instance/source/World/origin authority or same-epoch debugger identity denies. An explicitly configured new epoch can add a new runtime pin; the application never chooses one automatically. Missing/inconsistent pins fail closed. Credential rotation does not change historic identity or World; old instance credentials fail authentication after configuration replacement.

Identity uses u32 big-endian UTF-8 byte-length prefixes and SHA-256 over `["spaceos/browser-tab/v1", "browser", authenticated_instance, configured_source, approved_epoch, raw_target_id]`. The independently calculated Node test vector is asserted in Rust. URL/title/port/PID are not identity inputs. Core independently recomputes and validates the identity.

Core accepts one BrowserTabCreated, one BrowserTab, exact eight metadata properties, no facts/relations/deletes or extra object fields. HTTP/HTTPS or exact about:blank only; URL userinfo/control characters deny. Input URL <=8192 bytes, persisted <=4096, title null or <=1024; normalized observation <=16 KiB. Query and fragment are stripped before persistence. Title and path are not anonymized: dedicated non-personal sources remain necessary.

Inside one existing immediate transaction, all three pins are rechecked, the canonical object is truly INSERTed with Core timestamps, the BrowserTabCreated event is constructed from its complete canonical snapshot, and the observation log is inserted. Any collision/failure rolls everything back. Publication follows commit only. No Browser receipt/table/schema migration was introduced: database remains v4.

Replay authenticates first, checks observation/event identity, seq strictly above the authority's initial floor, event attribution and World, immutable event snapshot versus the full current object, and canonical identity tuple. It returns the original object with applied=false/seq=0 without writes or notification. Newly supplied bounded URL/title are ignored on replay. Legacy-looking rows/events below the floor, standalone event collisions, missing rows and changed canonical snapshots cannot be recertified or healed.

## 4. Actual integration evidence

Command: `node scripts/browser-observation-real-test.mjs`.

Latest passing run: `test-tmp/browser-verified-real-dd37d86c-5e2f-4506-b7a4-6fcc2b8f2186`.

Evidence:

- `docs/maintenance/browser-observation-real-runtime.log` — passing Rust integration command/output.
- `docs/maintenance/browser-observation-runtime-evidence.json` — canonical IPC replies, denial replies, event notification, signed get_tabs result and restart assertions.
- Fixture `fixture.json`, `core-*.log`, `provider-*.log`, synthetic.db — owned processes/config and actual persisted test state. No production credentials are used.

Source A is genuine Chrome → actual Browser Provider executable → authenticated Core implementation in its owned Rust child process → SQLite → authorized IPC query/subscription. Source B is explicitly synthetic IPC, not a second real Chrome.

Actual admitted object:

```text
id = browser-tab-v1-b42f8cf6c4225d544986923d09414a5fc8544538691d5b6ef04bcda203ffa933
external_id = browser-tab-v1:b42f8cf6c4225d544986923d09414a5fc8544538691d5b6ef04bcda203ffa933
type = BrowserTab; provider_id = browser; World = browser-A
instance_id = browser-real-A
source_id = 51bdafdc-d875-4741-b134-4dca0ffba734
runtime_epoch = 7f790338-fb8e-49b4-9b2c-0c71f828b1d5
target_id = D41E74317D548EF5B08A2E564420DE90
url = http://127.0.0.1:50189/synthetic
title = SpaceOS synthetic BrowserTab
created_at = updated_at = 2026-10-24T17:51:38.271624500Z
event_id = browser-tab-created-event-v1:b42f8cf6c4225d544986923d09414a5fc8544538691d5b6ef04bcda203ffa933
event_type = BrowserTabCreated; actor = browser; provider_id = browser
event World = browser-A; committed seq = 1
```

The input URL included `?remove=synthetic#remove`; neither component is in the canonical object/event. The event payload is the Core-owned full object snapshot. Real replay returned `{ok:true, result:{applied:false, seq:0, object:<same canonical object>}}`. Signed Browser get_tabs returned Completed and the canonical ID under the existing `tab_id` field.

Verified restart behavior:

1. Stop/restart actual Browser executable with the same Chrome/Core: identical canonical object, no duplicate events.
2. Restart actual isolated Core process while Browser executable continues polling: Provider re-registers; replay and exact-World read succeed; IDs/timestamps remain unchanged.
3. Close/relaunch only the test-owned Chrome on the same CDP port/profile: old configuration fails with `unapproved browser runtime; owner epoch authorization required`; no automatic epoch, object or success event. Historical tabs remain stored.
4. SQLite reopen and credential rotation are additional storage tests; explicit new-epoch pin approval is synthetic storage coverage, not an operational production approval.

Twelve malformed/unauthorized observation attempts and one reserved registration attempt returned ok=false without growth in object/event/log counts. Cases include wrong/general principal credential as instance proof, unknown/substituted instance, World/source/epoch mismatch, forged object identity, HTML/extra property, facts/deletes, additional object, unsupported lifecycle event. Source B's same target ID creates a different ID in browser-B; World A's reader cannot query B or receive B's creation notification. Integrity check returned ok; schema_version remained 4.

## 5. Exact commands and regression results

Toolchain: rustc 1.99.0 (b940084d7 2026-09-28), cargo 1.99.0 (5f94df478 2026-08-27), Node v24.20.0. All Rust commands used the existing target, offline dependencies and no incremental cache:

```powershell
$env:CARGO_TARGET_DIR='C:\spaceos\target'
$env:CARGO_INCREMENTAL='0'
$env:SPACEOS_TEST_ISOLATION_ROOT='C:\spaceos'
$env:SPACEOS_CORE_TOKEN='synthetic-browser-observation-tests-only'
# Core/FS regression runs also set TEMP and TMP to a fresh UUID directory under C:\spaceos\test-tmp.
cargo check --offline -p space-core -j2
cargo check --offline --locked -p browser-provider -j2
node scripts/browser-observation-probe.mjs
node scripts/browser-observation-real-test.mjs
cargo test --offline --locked -p space-core --lib --test phase_b --test semantic_api --test durable_file_provenance -j2 -- --test-threads=1
cargo test --offline --locked -p filesystem-provider --lib --test i5_reconciliation --test roots_gate --test verified_file_changed -j2 -- --nocapture --test-threads=1
cargo test --offline --locked -p browser-provider --lib -j2 -- --nocapture --test-threads=1
cargo test --offline --locked -p space-core --lib --test security_verification --test stage4_2_identity --test stage4_3a2_identity --test stage4_3a3_registration -j2 -- --test-threads=1
```

The first Core check resolved the newly direct url dependency offline and updated Cargo.lock; subsequent checks/tests used --locked. The real-test script invokes `cargo test --offline --locked -p browser-provider --test verified_observation real_chrome_browser_observation -j2 -- --nocapture --test-threads=1` with its generated fixture.

| Suite / log in docs/maintenance | Actual result |
|---|---|
| browser-observation-core-regressions.log | Core library 51/51; phase_b 12/12; semantic_api 7/7; durable_file_provenance 3/3 raw (2 behavioral + child launcher) |
| browser-observation-security-final.log | Core library 51/51 again, including final collision/bounds assertions; security_verification 4/4 raw (2 behavioral + 2 existing print-only placeholders); stage4_2_identity 4/4; stage4_3a2_identity 1/1; stage4_3a3_registration 1/1 |
| browser-observation-filesystem-regressions.log | Library 8/8; reconciliation 1/1; roots_gate 1/1; verified_file_changed 2/2 raw (1 real watcher/restart scenario + child launcher) |
| browser-observation-browser-regressions.log | Browser library 7/7, including signed IPC authentication/reconnect |
| browser-observation-real-runtime.log | Real Chrome integration 1/1; includes multiple authenticated denial, replay and restart assertions |

Deduplicated behavioral test total: **99 PASS** (Core 80, Filesystem 11, Browser library 7, real Chrome scenario 1). Repeated Core-library executions, child launchers and print-only tests are excluded. The phase-1 Chrome identity probe is additional runtime evidence, not another Rust test. The test named bridge_http_security_tests is a print-only placeholder and is expressly **not** HTTP/Bridge evidence.

Existing semantic File relation, durable receipt, FileChanged exact CAS, Mission transitions, scoped registration, Model A/identity and filesystem root enforcement tests passed. Actual current notify watcher creation/modification and Core/Provider restart were exercised by verified_file_changed; this is not merely an IPC fixture.

Failures encountered and corrected:

- Initial integration compilation lacked test rusqlite dependency; added the narrow dev dependency.
- Existing private Browser state test initializer needed the new optional runtime field; corrected initializer only. See browser-observation-test-check.log.
- Enhanced signed get_tabs test initially asserted `id`, but the unchanged API returns `tab_id`; corrected the test, not the API. Failed run directory: test-tmp/browser-verified-real-4956c1a0-1264-44c9-8e76-8e4cca02e0a4. The top-level runtime log was later overwritten by the passing run; retained child logs are not represented as a complete failure transcript.
- Added observation-log collision fixture initially omitted existing NOT NULL applied_at. Five tests passed, one setup failed (browser-observation-additional-security.log). Added the required synthetic fixture timestamp; final 51/51 library run includes all six new tests and passes. No application contract was weakened.
- Existing async-trait/unused-code compiler warnings remain; no unrelated cleanup attempted.

## 6. Source and binary SHA-256

Baseline pre-implementation hashes remain in section 13 of the approved design. Final hashes below are full current files, including pre-existing worktree content, not claims of clean Git provenance.

```text
Cargo.lock E61B82C9E65E663391BF2DEA50ED9E92A5EDCD43D277C30FEF1DE867DA065AD9
space-core/Cargo.toml A81FCA4268699C1B7AAD5D20C36291DE2E24EDA87C8F7A001DA05311F29A4C05
space-core/src/provider_observation.rs D209FC04188B805998AF358DA932F1DEF1DAE6C92FED75C089504A47ED592607
space-core/src/security_config.rs 08D605D334238F05A2CE96B3229BE784F93382B697C88601938AB2C1AD34D4F6
space-core/src/ipc.rs B745CD513E1D6E6A380BE4A2B51C87161E8CD09DD0E561773DA4678CB50D3336
space-core/src/storage.rs 7553AE1A0C29ED5B0976314FA9704AED05334A7A6067E1A53EA8CC3C7267E8B0
space-core/src/storage_browser_observation_tests.rs A62F3E78A6F022C0234E96B7FF3777ACE0513ACA2B78FE9950AE87AEE3130F43
browser-provider/Cargo.toml 8CBFEDD0B254780B8EE00BA9C24201F1337B6055AB9CFAB102598A06E3A9B343
browser-provider/src/main.rs 7C38E227F9C3FE22AA95D30160AF50F3954AE6C2297122FAAA258D2627742049
browser-provider/src/provider.rs B6BEB87396E7CC269FD0D3E3C7894E17C6D5DA3AB8C9AC0838082A2CA6564E64
browser-provider/src/objects.rs 43C00076F73166DDBC5C8E2E285AD9FDBE41EFFB8913FAE6D3421F5FE6184FF9
browser-provider/src/cdp.rs 61B7A04D8949581ACBBD0645B0DEA7F377AF16C53B60C2F875BD3C72C657221B
browser-provider/tests/verified_observation.rs E2EE4B5D7BABDB9AA909D31A9088879925433C053FC8FB5B0F552AAC8BAC26BF
scripts/browser-observation-probe.mjs 7974310D9D459CB5B38F84C3939647B643B1E6E1CF4AF1EC1453032B5B669AC3
scripts/browser-observation-real-test.mjs D792D6F31FBD843BCC85C1BD8EC7E6530F6FA9168998FA6EB9951848BBEE7B83
target/debug/browser-provider.exe FEAD1C57E9468CC03645C8A11C06FBAF48EEB32B403DEBAEA0F60FAFB85B9723
target/debug/deps/verified_observation-ecbf537efd455f88.exe FE8EC9577B21B1419F6116261CDE32288BBEEE7F8A2230182A67EB4A936BF583
```

The integration child executable contains the actual Core server implementation; no newly built production Core executable or release candidate is claimed. No release binaries were replaced.

## 7. Isolation, limitations and next owner decisions

Only generated synthetic SQLite files and profiles below the existing test-tmp directory were used. Owned Rust children were stopped; fixture scripts close their own Chrome. Post-run process inspection still showed the pre-existing Core PID11956, Browser PID26108 and sibling-lab Filesystem PID14808 at their original executable paths; none was signalled/restarted. No verified_observation child remained. No production pipe, port9222/8765, database, credential file or personal Chrome profile was accessed. Repository source changes are authorized development changes, not production runtime modifications.

Free disk was monitored: 28,732,600,320 bytes before work and 27,312,373,760 bytes after regressions (about25.4 GiB). Only the existing target was used; no target copied, new laboratory created or unrelated files deleted.

Remaining limits:

- Creation snapshots only: navigation/title changes, closes, deletes, rename, repair and historical-tab cleanup are not implemented. Replay never updates metadata. No Browser relation grant or filesystem authority is added.
- Verified standalone mode requires explicit source config; the older in-process CoreHandle/legacy discovery paths are not certified by these results. No claim that all existing Browser action epoch races or legacy record_event authorization gaps are repaired.
- Core subscription evidence is real IPC, **not Bridge WebSocket or World/Shell UI**. Those representations remain NOT TESTED here.
- Source B is synthetic IPC. Actual two-Chrome concurrency, simultaneous creator races, extended CDP outages, all malformed/oversize fake-CDP response shapes, and positive real-Chrome new-epoch approval were not executed. Storage serial transactions/collision tests and real same-port replacement denial cover the stated tested subset, not these unexecuted scenarios.
- Errors propagate as failed admission/cycle, not success. Provider polling intentionally emits only sanitized degraded diagnostics rather than raw payload/CDP URLs. It retries later polling cycles; it never silently changes epochs or weakens authentication.
- Metadata bounds and query/fragment removal reduce exposure but do not make URL paths/titles non-sensitive. Dedicated non-personal sources and exact read/event grants remain mandatory.
- Production source/World/epoch selection, secret provisioning, Windows identities/ACLs and coordinated startup/rollback require a separate owner-approved deployment plan. No automatic epoch rotation, production credentials or configuration were introduced.
- No model request, Mission agent, OpenCode/MCP runtime, production readiness or full-product E2E claim is made.

No commit. No push. No deployment.
