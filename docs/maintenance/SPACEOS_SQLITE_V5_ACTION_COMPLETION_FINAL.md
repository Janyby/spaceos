# SQLite v5 Action authority and credential-free dispatch

Date: 2026-10-25. Repository: `C:\spaceos`. Owner-approved bounded v4 → v5 implementation.

**Implemented and verified in synthetic isolated state. No production readiness/deployment claim.** The real Filesystem process returns Started and reports an authenticated terminal result without caller impersonation. Pending completion survives Core restart; a lost callback response produces no second terminal event. Both Browser and Filesystem use credential-free dispatch protocol v2.

**Evidence preservation is PARTIAL:** three existing regression fixtures regenerated fixed-name historical JSON summaries before their output paths were corrected. This error and the remaining limits are disclosed below; it must not be hidden by the passing functional results.

## 1. Exact implementation scope

The repository was already dirty. This list describes this task, not every difference against Git HEAD. No existing Policy/principal/service capability profiles, Mission/Gateway orchestration, World/Shell HTML, filesystem watcher/action implementations or Browser observation lifecycle were changed.

| File | Functions / changes |
|---|---|
| `space-core/src/action.rs` | Public ActionRequest authentication retained; correct its principal-authentication comment. Add internal `DispatchRequest`, `CompletionBinding`, `ActionCompletion`; explicit credential-free conversion and strict Provider parameter parsing. |
| `space-core/src/dispatch.rs` | `DispatchPayload`, `issue`, `issue_at`, `DispatchVerifier::verify`: sign/verify credential-free requests, protocol version 2; preserve MAC, expiry, nonce replay cache, exact request and performer binding. Add credential-absence / captured-proof-not-actor tests. |
| `space-core/src/storage.rs` | Schema version 5; integrate private action storage; transactional v4→v5 migration; preserve v4 File admission schema checks for versions 4/5 and reject unsupported future versions. |
| `space-core/src/action_storage.rs` (new) | Private table, schema verification, `reserve_action`, `action_authority`, `start_action`, `finish_action`; operation/terminal digests, atomic state/event transitions and exact terminal replay. Storage/migration/fault/restart tests. |
| `space-core/src/ipc.rs` | `forward_action_to_provider`: require v2 advertisement, strip caller credentials, reserve before dispatch, bind scan source/World/instance/performer, persist Started/terminal bookkeeping. `handle_action`: no duplicate Started and no fabricated Failed on uncertain dispatch/persistence errors. Add strict `complete_provider_action` endpoint; authenticate Provider, validate exact job/detail/authority, restore attribution, commit then publish. |
| `space-core/src/browser_actions.rs` | Pass Core state and original outer correlation into the shared dispatch path; existing scoped authorization/live resolution preserved. |
| `space-core/src/provider_observation.rs` | Reuse instance registration fingerprint through `instance_record`; add internal `dispatch_source` for unique registered canonical filesystem source/World binding. Observation admission rules unchanged. |
| `space-core/src/security_config.rs` | Add BROWSER_WORLD registrar role. Factor the same validation body behind an injectable lookup for synthetic tests without reading real environment secrets. Test malformed/short/empty, transport/principal/registrar/dispatch/instance duplicates and valid/missing optional configuration. |
| `filesystem-provider/src/ipc.rs` | Advertise v2 and observation instance; strict credential-free action parsing; bind scan job ID to Core's completion binding. Replace caller-token `report_event` with bounded `report_completion`, authenticating as filesystem plus its existing instance. Re-register the same instance after Core restart; retry only the identical completion, never the scan. |
| `browser-provider/src/ipc.rs`, `provider.rs` | Advertise v2; use internal DispatchRequest in existing signed action/scoped execution paths. No new Browser action or lifecycle. Update direct signed-request test fixtures. |
| `space-core/src/storage_file_admission_tests.rs` | Update the unsupported-future-version fixture to 6; make synthetic v3 fixture genuinely pre-v5. Add v4 migration preservation test for admitted File, semantic object/relation, Mission, Browser authority pins and events. |
| `space-bridge/src/bin/mvp-core-fixture.rs` | Only synthetic registrar fixture: explicitly allow registering a World-scoped test scan agent. No production/permanent profile expansion. |
| `tests/action_v5_runtime.cjs` (new) | Real private IPC proxies observe wire fields and control completion transport, actual scan/restart/lost response/SQL fault/negative tests. No mocked execution result. |
| `tests/action_v5_audit.cjs` (new) | Read-only post-run DB, attribution, digest, binary/source preservation, cleanup-flag and known-synthetic-secret checks; source/binary/artifact manifest. |
| `tests/vertical_slice_ii_corrections_runtime.cjs`, `scoped_browser_actions.cjs` | Optional v5 mode using existing isolated four-process/Chrome fixture; separate v5 artifacts, v2 signed negative fixtures, automatic stop for transport-only verification. Existing non-v5 modes retained. |
| `space-core/tests/durable_file_provenance.rs`, `filesystem-provider/tests/verified_file_changed.rs` | Change structured evidence destinations from shared maintenance filenames to each test's unique synthetic run directory, preventing further historical-summary overwrites. |

## 2. Private table and migration

Exactly one new table; no second semantic model. Full exact DDL is in `action_storage.rs::TABLE` and `action-v5-inventory.json::actionTableSQL`:

```sql
CREATE TABLE action_authorities (
    action_id TEXT PRIMARY KEY NOT NULL,
    correlation_id TEXT NOT NULL,
    actor TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    world_id TEXT NOT NULL,
    source_root TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    source_digest TEXT NOT NULL,
    operation_json TEXT NOT NULL,
    operation_digest TEXT NOT NULL CHECK(length(operation_digest)=64),
    performer TEXT NOT NULL CHECK(length(performer)>0),
    binding_id TEXT NOT NULL UNIQUE CHECK(length(binding_id)>0),
    state TEXT NOT NULL CHECK(state IN ('reserved','started','completed','failed')),
    terminal_json TEXT,
    terminal_digest TEXT,
    terminal_event_id TEXT UNIQUE REFERENCES events(id),
    CHECK((state IN ('reserved','started') AND terminal_json IS NULL AND terminal_digest IS NULL AND terminal_event_id IS NULL)
       OR (state IN ('completed','failed') AND terminal_json IS NOT NULL AND terminal_digest IS NOT NULL AND terminal_event_id IS NOT NULL))
)
```

Core creates a reservation only after caller authorization and protocol/source checks; its INSERT commits before sending the action. Duplicate action IDs and dispatch bindings are not silently reused. The credential-free operation JSON retains original correlation and approved parameters; SHA-256 binds its stored representation. Exact scan source/root/World, stable observation instance and registration fingerprint are separate from the ephemeral dispatch performer.

Legacy unscoped synchronous operations retain their existing authorization semantics; an empty World in such bookkeeping is not a new World grant. Scoped Browser operations retain their signed runtime tuple in the operation. Async scan requires a unique registered source and actual caller World membership (or scoped principal grant); an unscoped legacy dashboard is not silently given scan completion authority.

The v4→v5 DDL and user_version stamp commit in one SQLite transaction. A preexisting lookalike table is rejected, not adopted. No historical action-shaped events are backfilled. Reopening verifies exact table definition, schema version and foreign keys. Tests cover fresh initialization, synthetic v4 migration, rollback/version preservation on a conflicting table, restart and SQLite integrity. Existing File receipts/triggers, Browser pins, objects/relations, events and Mission state are preserved by the tested migration.

Generic record_event, semantic object writes and observations have no entry point into this private table. An arbitrary ActionStarted-looking event cannot mint authority. No reusable caller token is stored in the new record.

## 3. Before/after credential and completion boundary

Previously, Core forwarded ActionRequest.actor_token both outside and inside the signed proof. Filesystem background scan retained it and called generic record_event as the original caller.

Now:

1. Public caller authentication remains at Core. Conversion to DispatchRequest has **no actor_token field**. Both Provider parsers reject an unexpected bearer field rather than ignoring it.
2. Core requires `dispatch_protocol: 2` before action dispatch. Proof version 2 binds exact credential-free fields, original actor attribution, authorized target, nonce, expiry and dispatch performer. Old Providers without v2 advertisement are refused; old v1 proofs/outer public requests are not the new Provider protocol. There is no silent downgrade.
3. Existing transport `auth` remains a transport credential, not caller/system/service identity. No substitute reusable caller/privileged token is inserted into the operation or proof.
4. For scan, Core stores reservation before external execution. Provider returns Started with `scan-<binding_id>`; Core verifies that job and commits Started plus its domain event together. A failure here leaves the reservation unresolved, not completed.
5. Provider background state contains only credential-free operation/binding and its own existing Provider/instance identity credentials. It calls `complete_provider_action` as **filesystem**, never as the requesting actor.
6. Core's completion envelope is narrow: transport/principal/instance authentication and typed completion only. It validates registered instance, current canonical source/World/fingerprint, original binding, operation, live original performer and bounded scan-specific result shape. Caller-supplied World/extra authority fields, wrong instances/performers and unknown actions fail closed.
7. Only Started→Completed/Failed is admitted through the async callback. Core restores original actor/correlation/World from private authority. Terminal row and attributed event commit together, then publication occurs. Exact replay returns `{durable:true,replay:true}` without another event; different outcome identity/content is rejected.

Synchronous authority bookkeeping stores only compact action/status identity, **not screenshot base64 or read response bodies**. The immediate Provider result still reaches the caller unchanged; bookkeeping does not impose the async report's 64 KiB limit on normal filesystem read responses. Existing legacy action-history behavior outside this new table was not redesigned.

Before Started is durable, callback returns a retryable nonterminal response. Provider reporting uses up to 60 attempts, each with a five-second timeout and one-second interval. It retries the same report/outcome ID, not the scan. A lost successful response is resolved by the durable terminal digest. Permanent rejection or exhausted retries produces an explicit unconfirmed-completion diagnostic, not fabricated success. No Provider durable outbox/job resumption was added.

## 4. Real final runtime evidence

Designated final run: **2026-10-25T04:41:55.430Z–2026-10-25T04:42:08.389Z** (07:41:55–07:42:08 MSK).

- Root: `C:\spaceos\test-tmp\vertical-corrections-vcy8HG`; SQLite `core.db` there.
- World: `8f621e7a-f8f2-477b-ba73-3aa411873153`.
- Private UUID-named Core/Provider pipes only; Bridge origin 127.0.0.1:21973, owned synthetic page 21974, owned Chrome CDP 21975. No 8765/9222/production pipe/database connection.
- Initial Core 11948, Bridge 2804, Filesystem 18016, Browser 20316; pending-completion Core restart 8936; later Core 36848, Browser 34928, Filesystem 10884. Chrome 6088, 155.0.8059.85, profile below this run. PIDs are historical evidence, not stop instructions.
- Synthetic scan agent has action.scan only in this World; no permanent agent/profile grant was changed.

Actual action: `8b0d3c53-b15b-4d30-a91a-2d0bc81a15cb`; binding `e9675bfc-8cd8-4c96-8400-db55d5438c05`; original performer `a183c163-f948-447b-8594-479e95edca76`.

The scan of the preexisting synthetic empty child directory `project-files\scan-success` returned Started. Its actual Provider callback was withheld at the private transport proxy while Core's row remained Started and terminal count zero. Core was restarted against the same SQLite file while the same Provider remained alive. Reservation/performer/binding persisted. On release, the proxy forwarded the real callback, then intentionally lost the response. The Provider retried; exactly one terminal event remained.

Actual accepted outcome ID: `748c465b-cf50-402f-9092-dd363d86ad68`, status Completed, stats `created=1, updated=0, renamed=0, removed=0, uncertain=0`. The new observed child is a Directory, not an artificial File admission receipt. The canonical File's receipt remains unchanged.

The designated run recorded 5 callback attempts overall and one deliberately lost response. Tests reject valid-shaped conflicting stats, unknown action, wrong instance credential, substituted performer/source instance and foreign World envelope. A captured actual operation proof is rejected as service authentication; a separate unit test supplies a registered-service positive control and proves neither serialized proof nor MAC authenticates that actor.

Injected reservation INSERT failure prevented a Provider action message. Started-event INSERT failure returned no success and left a reserved row. Callback before Started was retryable with zero terminal events. Terminal-event INSERT failure left Started and no terminal event; the same report succeeded after removing the synthetic fault. No rejected write produced a success notification/event.

Unresolved action `ac1c3d8c-8790-4d89-ba73-56aa760a08b6` deliberately remains reserved after the Started persistence fault and later Provider restart. It was not reassigned or automatically completed. An old callback was rejected after performer replacement.

This proves **Core restart with pending durable completion**, not that a long-running directory traversal or a lost Provider process is seamlessly resumed. The scan had executed before its held callback was released. Execution and confirmed durable completion are explicitly different.

Real Bridge HTTP scoped get_tabs and screenshot succeeded through Core and signed Browser Provider against owned real Chrome. Foreign authority/raw target/unsupported actions were denied. Actual wire proxies observed v2 Browser and Filesystem operations with no actor_token in outer request or nested proof. FileCreated/FileChanged watcher, File receipt, Task→File relation, BrowserTabCreated/replay and exact-World authenticated Bridge WS checks passed. No Qwen request/Mission was repeated. No new interactive World/Shell visual pass is claimed; Browser evidence here is real HTTP/IPC/CDP plus the actual Provider JPEG.

Final SQLite: integrity `ok`, no FK violations, 11 objects, 3 relations, 47 events, one File receipt. All cleanup flags true; post-run inspection found no Chrome process with the final or failed startup fixture profile. The driver's inherited `READY_FOR_ONE_WORLD_MISSION` / `READY_FOR_REAL_UI` labels are inert fixture checkpoints, not evidence of a launched model or a pending live runtime.

## 5. Commands and regression results

Existing target only: `CARGO_TARGET_DIR=C:\spaceos\target`, `CARGO_INCREMENTAL=0`, offline/locked dependencies, `-j2`. Rustc/cargo 1.99.0; Node v24.20.0. Rust tests used explicit synthetic Core transport values, `SPACEOS_TEST_ISOLATION_ROOT=C:\spaceos` and newly generated TEMP/TMP directories under test-tmp. Startup validation tests inject a synthetic configuration lookup; they do not read production variables. Source order in Core main confirms validation precedes Storage::open; a production startup was not attempted.

Commands actually executed, omitting duplicate exploratory invocations:

```powershell
cargo check --offline --locked -p space-core -p filesystem-provider -p browser-provider -j2
cargo build --offline --locked -p space-bridge --features mvp-lab-runtime --bin mvp-core-fixture --bin mvp-runtime-fixture -p filesystem-provider --bin filesystem-provider -p browser-provider --bin browser-provider -j2
cargo test --offline --locked -p space-core -p filesystem-provider -p browser-provider --lib -j2 -- --test-threads=1
cargo test --offline --locked -p space-core --lib -j2 -- --test-threads=1
cargo test --offline --locked -p space-core --lib security_config -j2 -- --test-threads=1
cargo test --offline --locked -p space-core --test durable_file_provenance --test semantic_api --test stage4_3a3_registration -j2 -- --test-threads=1
cargo test --offline --locked -p space-core --test durable_file_provenance -j2 -- --test-threads=1
cargo test --offline --locked -p filesystem-provider --test verified_file_changed -j2 -- --test-threads=1
cargo test --offline --locked -p space-bridge --lib --test static_ui -j2 -- --test-threads=1
node tests/scoped_browser_recovery_ui.cjs
node tests/mission_verification_ui.cjs
$env:SPACEOS_SCOPED_BROWSER_TEST='1'
$env:SPACEOS_ACTION_V5_TEST='1'
node tests/vertical_slice_ii_corrections_runtime.cjs
node tests/action_v5_audit.cjs
```

| Suite / `docs/maintenance/` evidence | Result and actual boundary |
|---|---|
| `action-v5-core-final.log`, `action-v5-startup-final.log` | Core library 59/59; startup subset 2/2 included, not extra tests. Migration/private authority/fault/replay, File/Browser/Mission/Policy/authentication unit coverage. |
| `action-v5-lib-regressions.log` | Browser 7/7, Filesystem 8/8; includes real private IPC with fixture/fake CDP, not standalone real Chrome for those unit cases. |
| `action-v5-core-ipc-regressions.log`, `action-v5-isolated-core-evidence.log` | Durable provenance 3/3 (one guarded child entry point), semantic API 7/7, service registration 1/1. Actual synthetic IPC admission/relations/restart; repeated durable run not counted twice. |
| `action-v5-filechanged-final.log` | 2/2 (one guarded child entry point); actual filesystem watcher, CAS/receipt/authentication/restart/atomicity. |
| `action-v5-bridge-regressions.log` | Bridge 44/44 and static UI 4/4, including EventPump and existing Mission/Gateway contracts. |
| `action-v5-ui-regressions.log`, `action-v5-mission-ui-regressions.log` | 20/20 + 11/11 extracted-helper/DOM-double tests, not fresh manual browser evidence. |
| `action-v5-runtime-final-completion.log`, `action-v5-runtime.json` | 86 actual runtime assertions on designated final binaries. |
| `action-v5-audit.log`, `action-v5-inventory.json` | 17 post-run assertions, actual SQLite/hashes/known synthetic credentials. |

Nonduplicated suite total: **166 runner cases PASS, including two guarded child entry points (164 behavioral cases)**. Runtime 86 and audit 17 are separate evidence categories, not added as unique unit tests. Existing repeated suites/builds are not counted again. Focused tests establish the executed contract, not universal absence of regressions.

### Failures and corrections retained

- Initial check failed because the new Filesystem diagnostics used an unqualified warn macro; qualified it. Initial Core tests used a nonexistent in-memory helper; corrected to the existing `Storage::open(":memory:")`.
- Initial Core result 57/58: a corruption fixture called schema 5 unsupported, which is no longer true. Changed its unsupported version to 6, retaining fail-closed assertions; final 59/59 includes the additional migration preservation test.
- One Cargo invocation repeated --lib and was rejected before tests; corrected the command.
- First proxy runtime hung because a pre-listener connection failure closed without resolving the fixture's ping. `action-v5-runtime-proxy-startup-failure.json` / first log retained. Only exact owned executable/profile/driver identities were stopped; no production process was touched. Proxy now returns an explicit synthetic unavailable response and bounds upstream waiting.
- Earlier real Failed scan control and pre-hardening runs are preserved under `action-v5-runtime-*-control.json` / `*-before-*.json` and corresponding logs/images. Root scan's existing admission denial is not reclassified as successful scan work. Final successful scan uses a legitimate child directory.
- Synchronous bookkeeping was minimized before final verification to avoid retaining screenshot/file bodies or narrowing existing read limits. Earlier synthetic candidate records are not claimed to have this final property.
- First audit's broad “mission” filename regex also selected “admission” tests, which were intentionally changed. Corrected to exact intended implementation basenames; retain `action-v5-audit-selection-failure.log`.
- Regression compilation regenerated the Filesystem executable and hash audit correctly failed. Did not change expected historical hashes to hide this. Rebuilt coordinated candidates and **reran the real runtime on the final files**; final audit matches them. Retain `action-v5-audit-binary-relink-failure.log` and earlier runtime copies.
- Existing compiler warnings remain; focused tracked-source `git diff --check` passed. No unrelated warning/refactor cleanup.
- Final source review found that the generic Provider helper converts all negative Core responses into errors, hiding the retryable flag. Completion reporting now preserves the structured reply while supplying only the existing filesystem principal credential; no new credential/grant. The designated final real run verifies this final implementation.

## 6. Evidence preservation exception

Before detecting their fixed destinations, existing Rust regression fixtures overwrote:

- `docs/maintenance/provider-file-relations-ipc.json`
- `docs/maintenance/durable-file-provenance-ipc.json`
- `docs/maintenance/file-changed-watcher-ipc.json`

Their current contents are genuine new v5 regression results, **not the original JSON snapshots referenced by older reports/hashes**. Original exact JSON bytes were not restored from a verified backup in this task. Original run databases/logs and earlier reports remain; do not represent the historical summary hashes as still matching.

New copies are explicitly named `action-v5-provider-file-relations-ipc.json`, `action-v5-durable-file-provenance-ipc.json`, `action-v5-file-changed-watcher-ipc.json`. The responsible tests now write inside their unique run directories; those changed tests were rerun successfully. No old database/evidence directory was deleted. This is an acknowledged preservation error, not an application/production data corruption claim.

## 7. Hashes, compatibility and remaining limits

Full **19 source/test hashes**, loaded executable observations, artifact hashes, exact DDL and accepted action row: `docs/maintenance/action-v5-inventory.json`. Main source hashes:

```text
space-core/src/action.rs          a0cfb3539aef7d08b02bd72399ca6b86627633f799912d40f8aed4ffde9a3c23
space-core/src/dispatch.rs        25cccee5fc91b12fc6734c1890bb874bceeba7177f2bb91cd921809d95f8f6ba
space-core/src/ipc.rs             574bf9d79014050deb42124257d5a8e6f573a455db7c05f16f0d4296d0a3f1fe
space-core/src/storage.rs         a84fcb7e41ef4bc4a8a12a779fa613a3abda5a182296f6cae9f045021c0ae83c
space-core/src/action_storage.rs  66d27084888a76cb8b2d311c9f41573d5d380d1c50baa60eb62a78aa6360499b
space-core/src/security_config.rs dd902a2e3e3ff689629f8401199531606ec3eaa1a32d47aea899fea2cdd34d9a
```

Final loaded hashes under existing `target/debug`:

```text
mvp-core-fixture.exe    9a6d29e0b72a3c8780221b1a21b7dee233c28f92984f8dcdd07d2c4eccf55fe5
mvp-runtime-fixture.exe 472e3aaff14c5f4fd68599cd0947d7c5d7fe2da17b28f6f157dca16d16ebbee6
filesystem-provider.exe 147edd6328b9f5349e36342beba5fe15aa28c45194ac6f243f86dbd681b8dbb7
browser-provider.exe    3003a77f905cda684c175b84fc731aff2bf480d6da2d4f256914da3350b9fba0
```

These are isolated development candidates, not deployed production executables. Policy/profiles, World/Shell assets, Bridge UI recovery, Mission/Gateway and filesystem watcher/action/read sources matched their preserved baselines. No personal OpenCode/model request/profile/configuration was used or changed.

Compatibility requires coordinated Core/Provider v2 update. Old Providers are refused, not silently accepted. Old in-flight jobs and v4 action events have no trusted receipt and are not recertified. Production rollout would need an approved drain/stop, cold backup and compatible rollback plan; no schema downgrade or production migration was attempted. Unscoped legacy scan does not acquire exact-World completion authority automatically. Ordinary synchronous Browser/Filesystem authentication and signed execution remain covered by focused compatibility tests.

Remaining limits: no durable Provider outbox, automatic job re-execution, cleanup/timeout terminalization or seamless lost-process recovery; no active long-running traversal crossing restart was demonstrated; outages beyond the reporting budget and concurrent credential/registration rotation are not stress-certified. A lost/unconfirmed action stays reserved/started until a separately authorized recovery policy exists. Actual completion WebSocket delivery to World was not newly asserted; Core's new publication is post-commit, existing provider-observation Bridge WS was exercised, and no EventPump allowlist expansion was smuggled into this task. No fresh interactive UI layout pass, ETW validation or real Qwen Mission was needed/performed.

Startup validation is tested with synthetic lookup and source-verified before SQLite; this is not production account/ACL/secret provisioning certification. Existing legacy dashboard/localhost caller-authentication limitations remain. Free disk monitored from approximately 26.69 GB to 26.37 GB decimal; only the existing target and synthetic run directories were used. No unrelated WIP cleanup or production access.

## Final verdict

| Area | Result |
|---|---|
| SQLITE V5 MIGRATION | PASS — executed synthetic migration/preservation/rollback/integrity tests |
| CREDENTIAL-FREE DISPATCH | PASS — actual wire and signed-proof tests, both Providers |
| ASYNC COMPLETION | PASS — real Started and authenticated Completed/Failed controls |
| RESTART / REPLAY | PASS for pending completion, exact replay and performer replacement denial; not job recovery |
| BROWSER ACTIONS | PASS — real scoped HTTP/Core/Provider/owned Chrome get_tabs and screenshot |
| FILESYSTEM REGRESSIONS | PASS — executed focused watcher/CAS/admission/relation/synchronous action tests |
| MISSION / POLICY | PASS — executed focused regressions; no real model Mission repeated |
| HISTORICAL EVIDENCE PRESERVATION | PARTIAL — three fixed-name JSON summaries overwritten as disclosed |
| PRODUCTION | UNTOUCHED |
| DEPLOYMENT | NOT AUTHORIZED / NOT PERFORMED |

No new service, generic impersonation/event delegation, permanent capability change, unrelated frontend work, commit or push.
