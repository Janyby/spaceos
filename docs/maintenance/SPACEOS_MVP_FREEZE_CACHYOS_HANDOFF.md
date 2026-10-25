# Windows MVP freeze and CachyOS handoff

Date: 2026-10-25. Assessment of `C:\spaceos`; no implementation changes.

**Recommendation: proceed to an OWNER-REVIEWED Windows MVP source freeze, subject to complete source selection and private-configuration disposition. The source is not yet frozen or committed.** No newly demonstrated functional blocker prevents the already tested isolated Windows MVP from being preserved and used. Linux runtime compatibility is NOT established; it is the next porting stage, not an additional Windows MVP requirement.

This assessment is not general Windows production approval, a clean-checkout build certification, a deployment instruction or a claim that the current production processes run these candidates.

## 1. Exact MVP boundary

One World / Multiple Representations remains the boundary: Core owns semantic state, authorization and authoritative events; World and Shell query that same model. Providers observe/perform authorized operations; SpaceTrace observes rather than owning semantic state.

| Capability | Verified MVP boundary | Incomplete / experimental / explicitly excluded |
|---|---|---|
| Core semantic World / SQLite | Scoped authenticated semantic create/patch/related_to, read visibility and Mission commands; durable objects/relations/events; schema v5 private action authority, v4 File receipts and Browser authority pins | No global read grant, automatic legacy World migration, historical File backfill or arbitrary provider-object editing |
| World / Shell | Same canonical objects, relationships and Mission state; actual Space/Tree/Graph/Focus inspection; corrected search selection; canonical Browser labels; capability-gated Browser controls; real scoped HTTP/WS provider-event delivery | Bounded Graph overview may omit an unlinked BrowserTab; World/scope/status labels are imperfect; no claim that every canvas gesture, visual size or optional-provider diagnostic is perfected |
| Filesystem | Real watcher FileCreated then verified FileChanged; stable identity, exact Core updated_at CAS, protected receipt, authenticated instance/source/World; Task↔File related_to by an explicitly authorized actor; signed stat and bounded async scan | No approved general rename/move/delete/upsert lifecycle; exact-root scan/reconciliation can be denied; no general project-sync promise; provider-owned File PATCH remains denied |
| Browser | Canonical BrowserTabCreated, authenticated instance/source/epoch/World, replay/restart safety; real scoped get_tabs and screenshot through Bridge→Core→signed Provider, exact live handle; fail-closed UI and stale actor-token re-registration for subsequent requests | Creation snapshot only: no BrowserTabUpdated/navigation/close persistence. Legacy operational controls are not equivalent scoped authority. No personal-profile automation requirement |
| OpenCode/Qwen Mission | Actual World-initiated selected `unsloth/qwen-q8`, genuine model MCP reads/relationship use and Task/Summary patches; attempt binding, 30-call/600-second bounds; Core verifying→owner confirmation→completed; terminal-state persistence across controlled restart | Existing-owner OpenCode setup and hardcoded Windows lab paths/version pin; not a standalone install/executor product. No seamless active executor recovery, silent model fallback or generic autonomous relation grant |
| Policy / Action dispatch | Model A and distinct principals/registrars/instances; exact World checks; credential-free internal dispatch v2, signed parameters, expiry/replay/performer checks; durable scan Started→terminal completion survives tested Core restart | Existing localhost caller-authentication/legacy authority risks remain; not a hostile multi-user production certification. No durable Provider outbox, automatic lost-job rerun or terminalization policy |
| SpaceTrace | Semantic/EventPublished records correlated with actual Core/SQLite events and Mission correlation; portable in-process buffers/index/hooks | Windows ETW code exists, but the end-to-end model-process/OS attribution was NOT verified in Slice II. Linux OS backend is future work, not a freeze requirement |

The Mission agent did not gain `semantic.relation.create`: the Task→File link was established by a separate explicitly authorized synthetic actor. Provider semantic metadata reads are not disk/page-content reads. Flexible Summary properties retained an older `summary` value alongside new `progress`; preserve this recorded ambiguity rather than fabricating a stronger business result.

The local model endpoint `http://127.0.0.1:8888/v1` is an OpenAI-compatible inference endpoint, not OpenCode Server or Core IPC. Its present loaded/healthy state was not probed in this assessment.

## 2. Verified evidence and continuity

Primary reports, all under `C:\spaceos\docs\maintenance`:

| Evidence | Actual result, not rerun here |
|---|---|
| `SPACEOS_FULL_VERTICAL_SLICE_II.md`, `_EVIDENCE.md`, `full-slice-ii-inventory.json` | Real four-process/Chrome workflow, physical File create/change, canonical BrowserTab, Task→File link; six real Qwen MCP calls and owner-verified completed Mission; Core/Bridge restart; actual World/Shell |
| `SPACEOS_VERTICAL_SLICE_II_CORRECTIONS.md` | Subsequent correction of the recorded WS omissions, search exception and canonical Browser representation; 201 focused checks plus real scoped HTTP/WS/UI evidence |
| `SPACEOS_SCOPED_BROWSER_ACTION_IMPLEMENTATION.md`, `SPACEOS_SCOPED_BROWSER_RECOVERY_FINAL.md` | Real scoped get_tabs/screenshot, signer/performer/authority negative cases, capability loading and Core/Bridge restart recovery; do not replay an uncertain screenshot |
| `SPACEOS_SQLITE_V5_ACTION_COMPLETION_FINAL.md`, `action-v5-inventory.json` | 166 runner cases (164 behavioral + 2 guarded child entries), separate 86 runtime assertions and 17 audit checks; actual wire v2, pending completion across Core restart and one terminal outcome after lost callback response |
| `SPACEOS_SOURCE_ARTIFACT_ISOLATION_FINAL.md` | Exact 201-file Rust/build/static inventory and required untracked modules; targeted Gitignore protection, no source cleanup |
| `SPACEOS_EVIDENCE_WRITER_ISOLATION_FINAL.md` | 25 writer checks, 1 extracted Rust writer test, 19 syntax checks; immutable per-run outputs, explicit audit run input, historical bytes preserved |

Counts describe different suites/stages, not one freshly executed product E2E or a deduplicated lifetime test total.

### Reused genuine Mission evidence

Mission `3a984a38-bdff-435f-a89d-2643c045a369`; attempt `f989f770-73b3-40b9-84b5-ffb29b017d0c`; MissionAttempt object `a247a739-8e3a-4fba-9704-9706f42ff163`; World `e0551103-d8c8-4c6a-80f5-033defd5bd6c`.

Ordered model-originated calls:

1. `spaceos_query_relations`;
2. `spaceos_get_object` Evidence;
3. `spaceos_get_object` admitted File;
4. `spaceos_get_object` BrowserTab;
5. `spaceos_patch_object` Task;
6. `spaceos_patch_object` Summary.

Core replies were successful. Actual Mission lifecycle reached verifying, then the owner confirmed the exact attempt through UI with note `q`, producing authoritative completed. No agent self-verification, fabricated query_objects call or second confirmation is claimed.

Fresh read-only comparison now confirms **8/8** recorded Slice-II source hashes still match: Core Mission/storage, Bridge Mission/commands/Gateway/MCP/MCP executable source and SpaceTrace hooks. This justifies reusing the actual Mission evidence without a new Qwen experiment. It does not establish that today's external model/OpenCode runtime is currently available.

Current v5 inventory comparison: **16/19 source hashes unchanged**; the only three differences are the later authorized evidence-writer changes to `tests/action_v5_audit.cjs`, `tests/scoped_browser_actions.cjs` and `tests/vertical_slice_ii_corrections_runtime.cjs`. None is a production authorization or Mission contract change. The full Browser test source also has the documented writer-only edit; it was not fully rerun.

### Evidence integrity exception

The three fixed-name historical `provider-file-relations-ipc.json`, `durable-file-provenance-ipc.json` and `file-changed-watcher-ipc.json` were overwritten during the earlier v5 stage. Their current bytes are later real results, NOT the originals referenced by older hashes. Preserve the exception in the freeze record; do not claim reconstructed originals.

For this assessment, all **802 existing maintenance files** were hashed before writing this new report; the same aggregate was checked afterward:

```text
00642ec64e580afcb261cdf238e33817f2b2d1cb8f799b7c136e2ff4c4f7bd5d
```

These are current-file integrity hashes, not restoration of lost historical bytes. No historical report or image was changed.

## 3. Current Git and source completeness

At `2026-10-25T05:19:44.077Z`:

- Branch `flaky-t1t2t3t4`.
- HEAD `7990bd9f525bbe626ed5be07bf3c964270028ae4`, `fix(bridge-ui): repair world.html inline script and guard static UI assets`.
- **237 tracked paths; 40 tracked unstaged modifications; 899 visible untracked paths; no staged changes.** Creating this report adds one visible untracked path.
- HEAD is an old branch base, NOT the current MVP release. There are no commits isolating the accumulated tasks.
- Cargo metadata resolves **7 packages / 120 targets**, existing target directory only. All packages are currently `0.1.0`; Core/other packages use edition 2021, SpaceTrace uses edition 2024.
- **201 Rust/build/static inputs** plus operational/test sources give **250 source/support candidates**, including **72 currently untracked**. None of the 250 paths is ignored.
- Active module/target closure was established by the source-isolation review. Current metadata confirms target discovery; no clean checkout/compiler pass was substituted for that evidence.

The exact source-isolation inclusion list must be carried forward with its later evidence-writer delta. In particular preserve `C:\spaceos\space-core\src\action_storage.rs`, private as a Rust module but mandatory source; `storage.rs` explicitly includes it. Also preserve the explicit-path Browser/Mission storage/admission test modules. A tracked-only export or HEAD archive is incomplete.

### Required untracked source/support candidates

Exact current list for owner review; scripts are source candidates, **not permission to execute deployment/history tooling**:

```text
C:\spaceos\browser-provider\tests\verified_observation.rs
C:\spaceos\filesystem-provider\src\reconciliation_read.rs
C:\spaceos\filesystem-provider\tests\i5_reconciliation.rs
C:\spaceos\filesystem-provider\tests\verified_file_changed.rs
C:\spaceos\mock\contract_server.js
C:\spaceos\mock\proxy.js
C:\spaceos\runtime\src\read_client.rs
C:\spaceos\scripts\Deploy-Stage2B3.ps1
C:\spaceos\scripts\Inspect-BackupSqlite.cjs
C:\spaceos\scripts\Prepare-WorldRollback.ps1
C:\spaceos\scripts\Read-SpaceOSEnvironment.ps1
C:\spaceos\scripts\Verify-WorldRollback.cjs
C:\spaceos\scripts\browser-observation-probe.mjs
C:\spaceos\scripts\browser-observation-real-test.mjs
C:\spaceos\space-bridge\src\agent_gateway.rs
C:\spaceos\space-bridge\src\bin\isolated-bridge-fixture.rs
C:\spaceos\space-bridge\src\bin\isolated-core-fixture.rs
C:\spaceos\space-bridge\src\bin\mvp-core-fixture.rs
C:\spaceos\space-bridge\src\bin\mvp-runtime-fixture.rs
C:\spaceos\space-bridge\src\event_stream.rs
C:\spaceos\space-bridge\src\mission_commands.rs
C:\spaceos\space-bridge\src\service_credentials.rs
C:\spaceos\space-bridge\tests\full_runtime_security_e2e.rs
C:\spaceos\space-bridge\tests\i5_runtime.rs
C:\spaceos\space-bridge\tests\process_registration_e2e.rs
C:\spaceos\space-bridge\tests\semantic_http_e2e.rs
C:\spaceos\space-core\src\action_storage.rs
C:\spaceos\space-core\src\browser_actions.rs
C:\spaceos\space-core\src\dispatch.rs
C:\spaceos\space-core\src\mission.rs
C:\spaceos\space-core\src\mission_storage.rs
C:\spaceos\space-core\src\principal.rs
C:\spaceos\space-core\src\provider_observation.rs
C:\spaceos\space-core\src\security_config.rs
C:\spaceos\space-core\src\semantic.rs
C:\spaceos\space-core\src\semantic_read.rs
C:\spaceos\space-core\src\service_registration.rs
C:\spaceos\space-core\src\storage_browser_observation_tests.rs
C:\spaceos\space-core\src\storage_file_admission_tests.rs
C:\spaceos\space-core\tests\durable_file_provenance.rs
C:\spaceos\space-core\tests\security_verification.rs
C:\spaceos\space-core\tests\semantic_api.rs
C:\spaceos\space-core\tests\semantic_read.rs
C:\spaceos\space-core\tests\semantic_read_hardening.rs
C:\spaceos\space-core\tests\semantic_read_performance.rs
C:\spaceos\space-core\tests\stage4_2_identity.rs
C:\spaceos\space-core\tests\stage4_3a2_identity.rs
C:\spaceos\space-core\tests\stage4_3a3_registration.rs
C:\spaceos\tests\action_v5_audit.cjs
C:\spaceos\tests\action_v5_runtime.cjs
C:\spaceos\tests\attempt_binding.cjs
C:\spaceos\tests\authorization-positive.cjs
C:\spaceos\tests\check_consolidated_mvp.cjs
C:\spaceos\tests\check_full_vertical_slice.cjs
C:\spaceos\tests\check_full_vertical_slice_ii.cjs
C:\spaceos\tests\check_full_vertical_slice_ii_http.cjs
C:\spaceos\tests\consolidated_mvp_smoke.cjs
C:\spaceos\tests\contract_tests.js
C:\spaceos\tests\evidence_paths.cjs
C:\spaceos\tests\evidence_writer_isolation.cjs
C:\spaceos\tests\full_vertical_slice.cjs
C:\spaceos\tests\full_vertical_slice_ii.cjs
C:\spaceos\tests\mission_verification_ui.cjs
C:\spaceos\tests\scoped_browser_actions.cjs
C:\spaceos\tests\scoped_browser_audit.cjs
C:\spaceos\tests\scoped_browser_recovery_audit.cjs
C:\spaceos\tests\scoped_browser_recovery_checks.cjs
C:\spaceos\tests\scoped_browser_recovery_ui.cjs
C:\spaceos\tests\services.js
C:\spaceos\tests\vertical_slice_ii_corrections_audit.cjs
C:\spaceos\tests\vertical_slice_ii_corrections_runtime.cjs
C:\spaceos\tests\vertical_slice_ii_ui.cjs
```

The 201-file active manifest from the source-isolation report plus its documented Browser test writer hash update, the two mock-contract sources, operational scripts, and the two new evidence helpers/tests define the current inclusion surface. Preserve existing tracked baseline source and tests as well; selecting only modified files is insufficient for a standalone source package.

### Configuration/source package boundaries

Include reviewed Cargo root/member manifests and `Cargo.lock`, active modules, both Bridge HTML assets, Runtime static assets, Rust integration/support tests, Node/mock tests, evidence helpers, and approved reproducibility/maintenance documentation. Historical reports should retain old paths, not be rewritten to new results.

Do NOT package raw `C:\spaceos\test-tmp`, `target`, runtime databases/journals/tokens/private JSON, personal Chrome profiles/cookies, local model caches, OpenCode auth/session files or raw secret fixtures. Do not copy fixture configuration into production defaults. Retain private evidence locally under its current ignored paths; this task neither archives nor deletes it.

Tracked dirty `C:\spaceos\opencode.json` is an unresolved distribution-review item, **not a confirmed leaked credential**. Its contents were not read. Gitignore cannot protect an already tracked local configuration. Owner must decide exclusion from a distributable package versus a separately reviewed sanitized template before commit/publication. Merely leaving its current edits unstaged does not remove the already tracked HEAD version from a Git archive or future repository history; that baseline also requires owner review. No removal or history rewrite is authorized here. Do not blindly stage it or reset the owner's version.

Existing `.gitignore` now excludes test state and SQLite companions; hash `d17dcc7ff1d236e287f1ae5873a377d9a1a905224d33e662dfb24259ee720f7f`. No ignore rule was changed here. Normal Git staging protection is not protection against forced staging or arbitrary archive tools.

Non-secret configuration requirements (names/contracts only; no effective production values inspected):

- Explicit `SPACEOS_SEMANTIC_WORLD_ID`; missing/bad scope fails closed.
- Separate Core transport, principal, per-profile registrar, Provider instance and per-Provider dispatch credentials; Browser World registrar included in startup validation.
- Exact authorized filesystem roots→World/source bindings; separate Browser instance/source/epoch/debugger guard/World.
- Scoped Browser mode and its registrar, not legacy fallback; exact Bridge Origin and existing caller/session/CSRF contract.
- Lab execution marker, private endpoints and per-run DB/config; Mission model catalog/runtime/agent binding supplied by existing guarded fixtures.
- Existing OpenCode environment and explicit selected model for Mission evidence; do not export its authentication to the source package.

## 4. Freeze blockers versus nonblocking limitations

### True source-freeze conditions still requiring owner action

1. **Complete preservation selection.** A Git-only freeze made now would omit required untracked modules/tests. Owner must approve the explicit inclusion manifest and the final source revision/commit. No missing implementation file was found on disk; this is preservation completeness, not a demand for another feature.
2. **Private configuration disposition.** Resolve tracked local OpenCode configuration and review chosen evidence attachments before distributing/committing them. This assessment is deliberately not credential clearance.

No additional confirmed Windows functional code blocker was established by this review. A clean-checkout build cannot be claimed until an owner-approved complete source revision exists. Offline builds additionally need their normal dependency cache; `Cargo.lock` is not dependency vendoring.

### Unfinished but nonblocking for this Windows MVP freeze

- General Windows multi-user production identity/SID/ACL provisioning, accepted legacy IPC/dashboard/caller-authentication risks and coordinated deployment/rollback remain outside this source freeze. Do not describe them as fixed or approve external exposure.
- Unbounded Bridge WS backlog/backpressure remains a material scaling limit, not a demonstrated blocker for this bounded single-owner synthetic workflow.
- Browser lifecycle beyond creation/scoped reads, global Graph expansion, general filesystem lifecycle/reconciliation, stricter Summary ontology and OS/model-process tracing are future scope.
- Active execution/Provider job recovery is not seamless; unresolved authority can remain reserved/started. No automatic retry/reassignment is implied.
- Old audit baselines and one-time maintenance scripts must not be run indiscriminately. The older Browser real-observation test still asserts schema 4, and the older recovery audit contains a pre-v5 dispatch-source hash. These are test-baseline limitations; do not alter assertions just to pass, and do not select them as the current v5 acceptance gate.
- Windows-specific lab launchers are not cross-platform installation tooling. This does not invalidate the demonstrated Windows Mission.
- The three lost historical JSON originals remain an integrity limitation, explicitly disclosed rather than an impossible prerequisite to preserve current sources.

## 5. Smallest Windows acceptance proposal

**Not executed in this assessment.** Existing historical Qwen/real UI evidence is reused because the corresponding source/HTML contracts are unchanged. There is no reason to rerun free-provider experiments or a full model Mission for writer-only changes.

### A. Read-only freeze gate — executed here

Git status/index check, Cargo metadata, all required source paths visible, current source hashes, v5 binary hashes, comparison of the eight Mission/MCP/trace source hashes, and evidence-integrity check. No database opened and no runtime launched.

### B. Small writer/UI checks — existing tests, only when owner wants a fresh acceptance run

```powershell
Set-Location C:\spaceos
node tests/evidence_writer_isolation.cjs
node tests/scoped_browser_recovery_ui.cjs
node tests/mission_verification_ui.cjs
```

Expected scope from retained results: 25 writer checks; 20 Browser UI extracted-helper checks; 11 Mission verification DOM/HTTP-double checks. These are not a real browser E2E. Check counts/results must be recorded anew if executed; none was executed here.

### C. One model-free current v5 runtime slice — optional pre-freeze confidence check

Use the existing coherent v5 debug binaries whose hashes are recorded below. Verify dedicated fixture ports 21973–21976 are available before launching; never substitute production ports/pipes or kill their owners. Existing driver creates fresh unique state/private pipes and synthetic credentials; no historical fixture secret should be copied.

```powershell
$env:SPACEOS_SCOPED_BROWSER_TEST = '1'
$env:SPACEOS_SCOPED_BROWSER_RECOVERY_TEST = '0'
$env:SPACEOS_ACTION_V5_TEST = '1'
node tests/vertical_slice_ii_corrections_runtime.cjs
# Copy the exact new run path printed by this execution, never discover "latest".
node tests/action_v5_audit.cjs --run 'C:\spaceos\test-tmp\<exact-new-run>'
```

This existing driver covers real Core/Bridge/SQLite/Providers/owned Chrome, semantic operations, physical watcher create/change/receipt, Task→File relation, Browser creation/replay/scoped screenshot, authentication/isolation/WS, credential-free dispatch, real scan completion transport and Core/Provider restart. V5 mode does not run Qwen and stops owned processes automatically. It does not newly prove interactive browser layout or Mission execution.

Retained designated final evidence was 86 runtime assertions + 17 audit checks. The evidence-writer isolation stage did not rerun the whole driver; if owner selects this acceptance, failure must be retained rather than bypassed. Audit requires all expected current artifacts inside that exact run; historical baselines remain read-only.

Only if binaries are absent or production implementation source changes should they be rebuilt, into the existing target:

```powershell
$env:CARGO_TARGET_DIR = 'C:\spaceos\target'
$env:CARGO_INCREMENTAL = '0'
cargo build --offline --locked -p space-bridge --features mvp-lab-runtime --bin mvp-core-fixture --bin mvp-runtime-fixture --bin spaceos-mcp -p filesystem-provider --bin filesystem-provider -p browser-provider --bin browser-provider -j2
```

Rebuild invalidates old loaded-binary comparisons; record new hashes and run the selected acceptance on those exact binaries. Do not claim old runtime evidence directly certifies a newly linked executable. No whole-workspace build, ETW elevation, fresh Qwen call, production launch.ps1 run or 72-hour test is required for this bounded source freeze.

## 6. Recommended exact baseline

Recommended owner label: **Windows MVP / SQLite v5 / dispatch v2 / post-evidence-isolation**, optionally tagged later as `mvp-windows-v5-freeze`. This is a proposed label, not an existing tag, commit or SemVer bump. Keep it separate from subsequent CachyOS changes; do not use the old HEAD alone as the release version.

Current tool versions observed read-only:

- rustc `1.99.0 (b940084d7 2026-09-28)`;
- cargo `1.99.0 (5f94df478 2026-08-27)`;
- Node `v24.20.0`.

Historical real runtime: OpenCode 2.1.3, Chrome 155.0.8059.85, existing local `unsloth/qwen-q8`. Those external versions/loaded model were not queried now. Native Wry is optional, not required for the browser-served MVP.

Current source/support fingerprint: 250 sorted `[relative path, SHA256]` records, JSON-encoded:

```text
39255f407b5f5e26038632369192cac12d2011a9aa38c99c2815a1a75e7c3dc2
```

Per-group count/digest (same encoding, group subsequence); this fingerprints inclusion candidates, not private configuration or build outputs:

| Group | Files | SHA-256 |
|---|---:|---|
| root | 18 | `ea63a8a674438c4296d4d42bf1537459e3c6355de5c3a42890276c676c5fac7f` |
| agent-demo | 2 | `5fa6e7572b9c10f23385d901303dad816a405053b09de9a6753320f2dd687f67` |
| browser-provider | 8 | `462aa08042e1cea70c3f658deb255128451af73051d4547a5e01242aa0ce7dcf` |
| filesystem-provider | 43 | `b291ad81f419d995a78316b7f96df7ac3313b0cba40f5824978bce824c6d8057` |
| mock | 2 | `01c71982138237334814e8fc4f296e20d8560e0e5ab54f28c6b160c0fed1baee` |
| runtime | 7 | `e3d6bf7400377f54075987896d9a35df56f0eb8751bee9839c4f953af9ff9493` |
| scripts | 7 | `4a009b1496f903c7c31e986a1a8a899cc7ff52c0fdf66818fafe632c1a4d3230` |
| space-bridge | 35 | `495c8dc8dfa6396ce896653df7028bd9b3c7fa8b8034292ab53f8285a962a81c` |
| space-core | 90 | `78e25739cf0e43730568c60ccc25e8445d23197226b56f427eddaf69d317d08c` |
| spacetrace | 14 | `37c077212daf1c16e43c3e320762684cf19a71d128fed97c28351324974d5855` |
| tests | 24 | `753870de0593fa7aad6fe1a174126088c5ddad418cf1a45d016c36cdb630768b` |

Selected essential file hashes:

```text
e61b82c9e65e663391bf2dea50ed9e92a5edcd43d277c30fef1de867da065ad9 C:\spaceos\Cargo.lock
90ce43bb78951c065d896c671de47300e5abbf765a951b4f888522345dfa735c C:\spaceos\space-bridge\static\index.html
3ae990adcbc35d84b5222e93f225fcf30e46a8744986cd71553a6c4271569cf8 C:\spaceos\space-bridge\static\world.html
66d27084888a76cb8b2d311c9f41573d5d380d1c50baa60eb62a78aa6360499b C:\spaceos\space-core\src\action_storage.rs
a84fcb7e41ef4bc4a8a12a779fa613a3abda5a182296f6cae9f045021c0ae83c C:\spaceos\space-core\src\storage.rs
```

Rechecked loaded v5 candidate binaries — **4/4 match** `action-v5-inventory.json`:

```text
9a6d29e0b72a3c8780221b1a21b7dee233c28f92984f8dcdd07d2c4eccf55fe5 C:\spaceos\target\debug\mvp-core-fixture.exe
472e3aaff14c5f4fd68599cd0947d7c5d7fe2da17b28f6f157dca16d16ebbee6 C:\spaceos\target\debug\mvp-runtime-fixture.exe
147edd6328b9f5349e36342beba5fe15aa28c45194ac6f243f86dbd681b8dbb7 C:\spaceos\target\debug\filesystem-provider.exe
3003a77f905cda684c175b84fc731aff2bf480d6da2d4f256914da3350b9fba0 C:\spaceos\target\debug\browser-provider.exe
```

They are guarded fixture/debug candidates, not a certified production installer. Old Core/Provider v1 and new dispatch v2 cannot be mixed. SQLite v5 downgrade is not approved; any future data migration/rollback is separate from a source freeze.

## 7. CachyOS portability: actual source findings

No Linux build or runtime test was performed. “Portable” below means the source/domain contract is largely platform-neutral, not that CachyOS compilation has passed. Preserve the same Core, schemas, actor attribution, Policy and observation semantics.

| Area | Exact Windows-source/adaptation points | Minimum handoff boundary |
|---|---|---|
| Core domain/storage | `C:\spaceos\space-core\src\storage.rs`, `action_storage.rs`, `mission.rs`, `mission_storage.rs`, `semantic.rs`, `semantic_read.rs`, `policy.rs`, `dispatch.rs` | Keep object/relation/event model, transactional CAS/receipts/action authority and credentials/proofs. Rust/serde/chrono/rusqlite bundled SQLite/ring dependencies need a target build, not a second Core or new schema |
| Core IPC/defaults | `C:\spaceos\space-core\src\ipc.rs::{run_server,send_request,configured_provider_pipe,try_core_token}`; `main.rs::main` | Current Windows named pipes; non-Windows servers have hardcoded TCP 7777/7778/7779. `send_request` non-Windows always connects to 7777, ignores the supplied endpoint and lacks the Windows branch's transport-auth insertion. Do not call these fallbacks a working Linux stack |
| Provider/Bridge IPC | `C:\spaceos\filesystem-provider\src\ipc.rs::{run_server,handle_tcp}`; `C:\spaceos\browser-provider\src\ipc.rs::{run_server,serve_lines}`; `C:\spaceos\space-bridge\src\ipc_client.rs::{core_request,provider_request_endpoint}` | Provider action servers require transport auth plus signed proof. Bridge validates Windows pipe syntax; isolated endpoint guards likewise use pipe prefixes. Coordinate endpoint selection/auth/streaming/shutdown, never disable these checks or route Provider actions back to Core |
| Filesystem observation/path authority | `C:\spaceos\filesystem-provider\src\{watcher.rs,scanner.rs,observation.rs,metadata.rs,roots.rs,performer.rs,reconciliation_read.rs}`; Core `provider_observation.rs::within` | notify/std::fs are reusable, but Linux event/coalescing/permissions/symlink/non-UTF8 behavior must be tested. `roots.rs::contained` unconditionally lowercases paths: incorrect case authority for Linux; Core's `within` already has cfg-specific behavior. Root parsing currently uses semicolons. Preserve exact read-before-stat CAS and error propagation |
| Process/performance identity | `filesystem-provider\src\performer.rs::{creation_filetime,current_performer}`, `observation.rs::snapshot_processes` | Windows GetProcessTimes/ToolHelp; non-Windows start time is explicitly unavailable and process snapshot empty. Define Linux process-instance/time semantics before claiming equivalent OS evidence; never substitute wall clock/PID alone as verified identity |
| Browser/CDP | `C:\spaceos\browser-provider\src\{cdp.rs,provider.rs,ipc.rs,objects.rs,main.rs}`; Core `provider_observation.rs::{BrowserRuntimeConfig,browser_tab_identity}`, `browser_actions.rs` | CDP JSON/HTTP/WS and canonical tuple are reusable. Configure owned Linux Chrome path/profile/loopback endpoint; preserve instance/source/epoch/debugger guard and exact target, fail closed after runtime replacement. No new Browser lifecycle needed |
| Bridge/World/Shell HTTP/WS | `C:\spaceos\space-bridge\src\{handlers.rs,server.rs,state.rs,event_stream.rs,service_credentials.rs}`; `static\world.html`, `static\index.html` | Axum/reqwest/WS/JS representation can remain. Preserve exact Origin and auth/CSRF; serve assets from a known working directory, configure endpoint paths. Existing Unix termination handler in server.rs is useful but not full-stack portability proof |
| Mission/OpenCode/MCP execution | `C:\spaceos\space-bridge\src\agent_gateway.rs::{start_owned_mission,existing_cli,cli_json,OwnedOpenCode::launch_isolated,force_owned_tree}`; `mcp_client.rs::McpClient::connect`; `bin\spaceos-mcp.rs::{load_attempt,configure_read_fixture}` | Hardcoded C:\spaceos test guards/CLI .exe locations, pinned existing Desktop PID/version and Windows taskkill/creation flags require bounded platform adaptation. Linux child.kill is not demonstrated process-tree cleanup. Preserve session ownership, attempt lease/World, model selection, MCP-call/deadline bounds and cancellation |
| Runtime/native UI | `C:\spaceos\runtime\src\{main.rs,read_client.rs,ui.rs,wry_wrapper.rs}`, `runtime\Cargo.toml` | CLI subscription is Windows-only; port its stream. Browser-served UI first. Optional wry/winit requires target-native dependencies/display validation; no Tauri work or native shell requirement for initial Linux MVP |
| Launch/test infrastructure | `C:\spaceos\launch.ps1`, root build/stop/diagnose PowerShell/batch; Bridge fixture bins; `tests\*.cjs`, `scripts\browser-observation-*.mjs` | Replace Windows launch/path/process assumptions in the approved port stage. Preserve isolation guards with configured canonical Linux roots, not deleted checks. Several tests are cfg(windows) or use Windows pipe paths. Keep unique per-run artifacts and explicit audit run selection |
| SpaceTrace | `C:\spaceos\spacetrace\src\{hooks.rs,buffer.rs,correlation.rs,provider.rs,trace_event.rs,verdict.rs}`; cfg(windows) `etw.rs`, Cargo Windows dependencies | Reuse semantic event correlation/buffers. ETW cannot run on CachyOS; no Linux eBPF collector exists merely because a verdict field accepts “ebpf”. Keep OS evidence unavailable until a separately approved implementation/test. Core currently has a Cargo dependency on spacetrace; preserve Core authority and failure separation rather than relying on contrary old comments |

The fallback IPC/auth findings and case-sensitive root check are concrete source-level port blockers, **not a reason to add Linux changes before the Windows freeze**. They also mean a successful cross-compilation alone would not prove a secure functional Linux runtime.

### Recommended porting order

1. **Freeze/preserve the reviewed Windows source first.** Keep evidence/private state separate; record accepted limitations and exact hashes. Do not rewrite historical Windows results as Linux results.
2. **Select Linux local transport and owned paths/credential provisioning.** Recommended design direction: owner-scoped local sockets with filesystem access control while retaining existing application authentication/proofs; alternative explicitly configured loopback TCP with equivalent isolation. Owner approval is required before choosing/implementing this contract. Fix Core/Bridge/both Provider endpoints together; no second service/model.
3. **Build the unchanged domain/storage/security core on CachyOS**, then test synthetic SQLite v5, receipts, timestamp CAS, Mission state and signed dispatch. Use fresh synthetic DBs; do not open/migrate the owner's Windows DB. Keep library dependencies locked initially.
4. **Port Filesystem runtime and path checks**, including case/symlink/permission tests, watcher create/change/restart, receipt preservation, scoped stat and bounded async scan completion. Preserve unsupported lifecycle denials.
5. **Run Bridge + actual browser-served World/Shell** against that Core. Test login/CSRF/exact Origin, scoped queries, semantic mutations, authenticated WS, identity consistency and persistence.
6. **Port the owned Chromium/Browser runtime**, test one BrowserTabCreated/replay and exact scoped get_tabs/screenshot with signed proofs. Do not rebind historical Browser epochs or grant legacy authority to make it work.
7. **Adapt existing OpenCode invocation/MCP paths and cancellation**, verify one model-driven scoped read first, then repeat the existing user-initiated relation-reading Task/Summary Mission with owner verification. Use the owner's chosen working local model configuration, no model download/profile experiment required by this handoff.
8. **Optional native Wry and Linux OS observation later.** Preserve semantic SpaceTrace correlation immediately; ETW equivalence/eBPF is a separately approved observability milestone, not an initial Linux semantic-MVP gate.

Do not reinterpret Windows File external paths, source bindings or Browser epoch identities as Linux authorities. Any later user-data relocation/import needs its own explicit ownership/migration decision; the initial port should use fresh synthetic state.

## 8. Owner decisions before commit / next stage

| Required decision | Recommended option | Alternative / implications |
|---|---|---|
| Source freeze scope and label | Approve current complete post-writer v5 source/test/docs candidate and disclosed limits; future separate freeze commit/tag | Delay for the small optional model-free acceptance, not another architecture expansion. No tag/commit made now |
| Tracked local configuration / chosen evidence attachments | Exclude private runtime/configuration from distributable source; review a sanitized template separately | Keeping local private configuration privately is possible, but does not authorize publishing it or broad staging; its contents remain unreviewed here |
| Evidence acceptance | Reuse unchanged real Mission/UI evidence plus current hash continuity; optionally run one v5 model-free acceptance | A new Qwen Mission is justified only by changed execution contracts or owner request, not source housekeeping |
| CachyOS first-stage scope | Browser-served Core/World/Shell, scoped filesystem/browser and existing Mission; defer native UI/OS collector | Native UI or Linux kernel tracing first expands dependencies/validation without proving the semantic path |
| Linux transport / owned runtime paths and identity provisioning | Approve a bounded transport/path plan before implementation, retaining credential domains and exact Worlds | Configurable loopback TCP is an alternative but must not inherit the broken fixed-port fallback or weaken auth. No account/socket directory/secret store selected here |

General production deployment approval, Windows ACL hardening and Linux OS-observation authority are NOT silently accepted by these decisions.

## 9. Work performed and stop point

Executed only read-only Git/source/report inspection, SHA-256 comparisons, Cargo metadata and tool version/disk queries. No test suite, new build, browser/model/provider/Core process, DB migration or production access. Disk observed: 26,334,347,264 bytes. Only this report was created.

After report creation: original 250-source fingerprint, existing maintenance-file aggregate and Git index hash were checked again. No staging, commit, push, deployment, WIP cleanup, credential read/export, source edit or new laboratory.

**Windows MVP: verified bounded candidate. Source freeze: PENDING OWNER APPROVAL AND COMPLETE SELECTION. CachyOS: ASSESSED, IMPLEMENTATION NOT STARTED. Production deployment: NOT AUTHORIZED.**
