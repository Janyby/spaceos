# Scoped Browser World Actions — implementation and evidence

Report closed: 2026-10-25 (owner timezone: Europe/Moscow).
Runtime evidence retains its original timestamps: final run 2026-10-24 20:31:54.570Z–20:34:10.384Z (23:31:54–23:34:10 MSK).

## 1. Result

**PASS for the bounded isolated get_tabs/screenshot contract. Not production readiness or deployment authorization.**

- A real served World selected a persisted canonical BrowserTab and displayed an actual JPEG captured by the dedicated real Chrome through Bridge → authenticated Core Policy → signed Browser Provider dispatch.
- New profile `browser_world`, actor `space-world-browser`, grants exactly `semantic.read`, `action.get_tabs`, `action.screenshot` in one configured World. No existing profile, Mission grant or filesystem grant was expanded.
- Canonical row, configured/registered observation instance, source, epoch, debugger guard and live Core-adopted provider handle are checked. A public raw target or supplied authority tuple cannot select the action target.
- 260 focused regression tests passed, plus 60 real runtime assertions and 20 post-cleanup artifact/persistence checks. Counts exclude repeated executions and distinguish unit/mocked frontend tests from real HTTP/IPC/Chrome/UI evidence below.
- Real controlled Core/Provider restart was exercised. Unregistered source fails closed; re-registration reconstructs the same canonical identity; the ephemeral signed-dispatch performer changes. Browser creation snapshot and filesystem admission receipt persist.
- All owned fixture processes/tabs stopped. No production endpoints/database/credentials were accessed. No schema migration, new service/laboratory/target tree, commit, push or deployment.

## 2. Exact source changes

The worktree already contained substantial earlier authorized changes. The entire Git diff is **not** attributed to this task. This task's bounded changes are:

| File | Functions / change |
|---|---|
| `space-core/src/service_registration.rs` | `BROWSER_WORLD_PROFILE`, `BROWSER_WORLD_ACTOR`, fixed `BROWSER_WORLD_CAPS`; `actor_for`, `capabilities_for`, `from_env` add a separate registrar domain |
| `space-core/src/policy.rs` | `register` rejects delegated creation/overwrite of the new reserved actor; `is_delegated_agent` recognizes the service identity; existing service authority conflict rules unchanged |
| `space-core/src/action.rs` | Strict `BrowserWorldRequest`, `BrowserShotOptions`, `BrowserWorldTarget`; bounded JPEG options; no change to the outer ActionRequest/proof schema |
| `space-core/src/provider_observation.rs` | `browser_action_authority`: exactly one configured source in the World and registered observation instance; `credential_matches_instance`: reject registrar/instance credential reuse |
| `space-core/src/storage.rs` | Read-only `validate_browser_authority` checks the existing protected instance/source/runtime pins and common admission floor; no new table/receipt/migration |
| `space-core/src/browser_actions.rs` (new internal IPC module) | `authorized`, `canonical`, `execute`, `event`, `handle`: exact auth/World checks; canonical row and live snapshot match; scoped result filtering; durable Action events; two focused unit tests |
| `space-core/src/ipc.rs` | `register_service_actor`: exact configured World/caps, source registration/pins, distinct credential domain; `handle_action`: reserved actor cannot enter legacy dispatch; scoped fields cannot fall through under another actor; `forward_action_to_provider`: require protocol support for scoped actor, validate response action ID, attach actual dispatch performer ID |
| `browser-provider/src/ipc.rs` | Existing ping advertises `browser_world_protocol: 1`; after existing MAC verification, `execute_browser_action` routes reserved actor to scoped enforcement; legacy actors cannot inject scoped targets |
| `browser-provider/src/provider.rs` | `execute_world_action`: full signed tuple/config comparison, pinned live page targets, exact Core-adopted cache snapshot, exact session; only list/screenshot, no retry/current-tab substitution |
| `space-bridge/src/service_credentials.rs` | Separate server-only `SPACEOS_SERVICE_REGISTRAR_TOKEN_BROWSER_WORLD` loading, with existing isolation guard |
| `space-bridge/src/state.rs` | Independent scoped registrar/token cache; explicit mode; `configure_scoped_browser`, `scoped_browser`, `browser_world_action`; Browser calls use new actor only in scoped mode, never legacy fallback or automatic operation retry |
| `space-bridge/src/handlers.rs` | Existing browser/generic routes retain Core routing; expose `browser_mode` in tabs/generic/context response; no new service/route required |
| `space-bridge/static/world.html` | `applyBrowserMode`, `browserLiveTarget`, `openBrowserObject`, `bfRefresh`, `bfSelect`, `bfShot`, `bfRenderTabs`, Browser sidebar: exact full live tuple in scoped mode; canonical `object_id`; no first-tab selection; unavailable controls disabled/omitted; truthful scoped read-only note |
| `space-bridge/static/index.html` | `getBrowserTabs`, `renderTabs`: scoped canonical IDs, no activate/close controls, disable unsupported Browser operations. Shell lists live scoped tabs; no new Shell screenshot feature |
| `space-bridge/src/bin/mvp-core-fixture.rs` | Optional guarded synthetic browser-world registrar and deliberately unadmitted historical BrowserTab fixture; no permanent profile provisioning |
| `space-bridge/src/bin/mvp-runtime-fixture.rs` | Optional explicit synthetic scoped Browser setup; no inherited production credential |
| `tests/vertical_slice_ii_corrections_runtime.cjs` | Opt-in `SPACEOS_SCOPED_BROWSER_TEST=1`, separate evidence filenames, actual authorization checks and controlled owned-process restart; ordinary previous driver mode preserved |
| `tests/scoped_browser_actions.cjs` (new) | Real Core IPC/Bridge HTTP/Provider signed-negative/SQLite assertions; no model requests |
| `tests/vertical_slice_ii_ui.cjs` | Three additional executable extracted-JS contracts; existing panel fixture includes its mode helper double |
| `tests/scoped_browser_audit.cjs` (new) | Independent final persistence/UI/cleanup/hash/known-fixture-secret audit |

All current source, loaded executable and evidence hashes are in `scoped-browser-inventory.json`. No changes were made to the filesystem Provider source, Mission orchestration, OpenCode/Gateway configuration, observation admission behavior or schema. The new Storage method only reads existing Browser pins.

## 3. Contract now enforced

### Registration and credentials

The initial approved subset is fixed to the three capabilities above, not a caller-configurable superset. Existing `DASHBOARD_CAPS`, `space-world-user`, MCP profiles and their existing World rules remain unchanged. No dynamic action-manager configuration was introduced.

Core registers the new actor only for validated `SPACEOS_SEMANTIC_WORLD_ID` and exactly one registered Browser source in that World, with valid durable authority pins. A new secret is independently supplied to Core and Bridge through the existing registrar mechanism. It is not a reader/provider/system/transport credential or an action bearer token. The new registration path also rejects equality with configured observation-instance credentials or Browser/filesystem dispatch keys. Existing registrar insertion rejects principal/transport/other-registrar reuse.

Bridge explicit mode is `SPACEOS_BROWSER_MODE=scoped`; the synthetic fixture uses the explicit constructor method rather than inherited environment secrets. Missing/invalid scoped credential or World/source cannot fall back to dashboard. An unrecognized/non-legacy mode value does not acquire a registrar or silently become legacy. Unset mode retains existing legacy compatibility. Production values/storage/rotation were not configured.

Browser principal/instance use existing `SPACEOS_BROWSER_OBSERVATION_BINDINGS` on Core and existing provider configuration. No additional source grant was invented. Empty/multiple matching sources or absent re-registration deny. The stable observation instance is distinct from the ephemeral `DispatchVerifier` performer ID.

### Request and live resolution

World sends the existing generic action route with canonical input:

```json
{"action":"screenshot","target":{"object_id":"browser-tab-v1-...","options":{"format":"jpeg","quality":62}}}
```

Bridge supplies the actor/token/configured World server-side. Public Core input is versioned World/object/options, not a raw CDP target or caller-supplied instance/source/epoch. Core independently authenticates and checks exact membership plus each capability. The synthetic lab session can be created only for that same configured World; normal cookie/CSRF/Origin checks remain in effect.

Core reads the canonical row and validates type/provider/World, source/instance/epoch/page target, recomputed canonical/external identity and durable authority pins. It obtains a private, signed source-bound live enumeration; returned rows are compared with current Core objects, including the full canonical snapshot. Only matching rows can reach World. Thus unscoped operational enumeration, authorized historical semantic reads and scoped live actions remain separate contracts.

Provider reconstructs its canonical cache objects from records previously adopted from Core's verified observation response, preserving Core timestamps. At execution it checks the signed tuple against configured observation identity and `approved_runtime`, pins the actual Chrome debugger, checks the page target/cache/snapshot and attaches only to the exact target. Screenshot calls `execute_on_session` directly without the legacy retry/current-target fallback. Core verifies the response authority and nonempty image. No storage lock is held across provider/CDP awaits.

This is a **live authority chain**, not a new durable per-tab provenance receipt. The canonical hash alone is not proof. Deliberately seeded unadmitted historical state with a correctly shaped canonical identity could be read as historical semantic data but could not acquire an actionable live handle. Signed tuple/snapshot substitutions were also rejected. No tested forged historical state satisfied the chain. These tests are not a proof against every possible privileged database rewrite; no new historical recertification mechanism was added.

### Events and compatibility

Scoped ActionRequested must persist before dispatch; terminal scoped event must persist before publication/returning success. Actor, World, canonical object IDs and correlation are retained. Images, tokens and signed proofs are not written into action history. Failures after external execution cannot be atomically rolled back with SQLite; a terminal persistence error is surfaced, not reported as success or retried to repair history.

All Browser route variants, including generic `/api/providers/browser/actions`, reach the scoped actor in scoped mode. Unsupported actions reject, not legacy-register. Legacy dashboard behavior remains available in its existing mode and is **not** certified as World-authorized. Existing filesystem dispatch/root/instance/provenance paths remain separate and unchanged.

## 4. Actual runtime and transport evidence

Final fixture root: `C:\spaceos\test-tmp\vertical-corrections-isLaKP`.
This is disposable test state within the existing repository, not another laboratory or Rust target tree.

- Bridge origin `http://127.0.0.1:21973`; synthetic page `http://127.0.0.1:21974/evidence.html`; dedicated Chrome CDP `127.0.0.1:21975`.
- World A `e4ab161e-0f99-449b-9a1a-1bc681840352`; distinct World B/source for negative checks.
- BrowserTab `browser-tab-v1-97874ec832800ff8e1ffbd430df23b5cb6514217472f7dc0c99a9578a0429faa`.
- File `86fc1922-3468-4857-8aeb-db5779b3e10c`; Task→File relation `2cbf597b-266e-491d-9073-55b033e22d1a` established by the existing explicitly scoped synthetic relation actor, not an expanded Mission grant.
- Dedicated real Chrome `155.0.8059.85`, PID 21056, new profile below the fixture root.
- Initial Core 25824, Bridge 26916, Filesystem 34188, Browser 34504; after controlled restart Core 12600, Filesystem 2644, Browser 17224. Bridge and owned Chrome remained during the restart. These are recorded historical PIDs, not reusable shutdown instructions.
- All executable paths, process hashes and explicit owned Chrome command line are preserved in `scoped-browser-runtime.json`.

Actual HTTP results:

- Without lab authentication, `/api/browser/tabs` → **401**.
- Foreign Origin → **403**.
- Authenticated scoped tab list → **200**, `browser_mode: scoped`, `status: Completed`, exactly one admitted live canonical tab with full World/instance/source/epoch columns.
- Authenticated canonical screenshot → **200**, `status: Completed`, nonempty JPEG and the actual dispatch performer ID.
- Raw-only screenshot and all specialized unsupported Browser routes rejected; no legacy fallback.

The test calls with a forged historical canonical row received:

```json
{"ok":true,"result":{"status":"Failed","detail":{"error":"Browser canonical live handle unavailable"}}}
```

The dedicated signed-Provider negative harness owns **synthetic** dispatch keys and sends real MAC-verified requests over the private Provider transport. Stale epoch, foreign source/World, substituted observation instance/raw target and forged cached snapshot all produced Provider `Failed` outcomes. This does not imply an HTTP client can create Core proofs.

Pre-restart performer: `7bbc44e8-655f-432f-a1e9-709ec53e53f8`.
Post-restart performer: `d3a558a6-fe95-4a17-8763-0599c3f4e542`.

Final actual UI screenshot terminal event:

```text
event_id       2a4cf01f-ce29-4b3f-b454-7506cb3dfa05
event_type     ActionCompleted
actor          space-world-browser
provider_id    browser
world_id       e4ab161e-0f99-449b-9a1a-1bc681840352
correlation_id 46ef38e0-5515-41cd-9525-e072824dd41d
seq            61
performer      d3a558a6-fe95-4a17-8763-0599c3f4e542
```

Actual authenticated Bridge WebSocket collector delivered BrowserTabCreated event `browser-tab-created-event-v1:97874ec832800ff8e1ffbd430df23b5cb6514217472f7dc0c99a9578a0429faa` and FileChanged `37a83861-5b27-484d-a3cc-31d2810315ec` in World A. Additional legitimate FileChanged notifications are retained in evidence. Replayed/rejected observations produced no extra write/success notification during the controlled silence check. A committed foreign-World Browser event was absent from the World-A query/WS stream. No event-selection predicate was changed in this task.

Final SQLite integrity_check: `ok`. Exactly one BrowserTabCreated for the selected tab after all operational reads, UI streaming and restart. Full canonical Browser snapshot unchanged. The live UI frame was decoded at 764×485; final actual UI viewport 1280×720, device scale 1. The existing frame timer produced repeated read-only screenshots, not a new lifecycle or a single-shot-only UI claim.

### Real UI evidence, separate from mocks

- `SCOPED_BROWSER_WORLD_FINAL_UI.png`: actual served World displaying the Provider's real synthetic page screenshot on final binaries after restart.
- `scoped-browser-final-ui-evidence.json`: exact canonical ID, decoded image dimensions, four disabled unsupported buttons, zero close controls, empty warn/error console result, matching mode note.
- `SCOPED_BROWSER_PROVIDER_SCREENSHOT.jpg`: real Provider JPEG, not a generated image.
- Earlier successful actual UI run `vertical-corrections-m20fA3`: `scoped-browser-runtime-ui-run.json/.log`, `SCOPED_BROWSER_WORLD_UI.png`, `scoped-browser-ui-evidence.json`.
- `scoped-browser-shell-evidence.json`: actual Shell canonical tab ID and disabled unsupported controls in that earlier run; console warnings/errors empty. Shell was not claimed to implement a new screenshot surface.

Search selection used the real delegated handler and canonical object, then the Browser action was activated using normal keyboard Enter. Pointer clicks on the offscreen sidebar failed in the inherited narrow UI view; no JavaScript handler invocation or injected overlay was substituted. The final wide screenshot verifies the rendered frame; this task is not a comprehensive responsive-layout certification.

## 5. Commands and executed tests

All Cargo commands used existing `CARGO_TARGET_DIR=C:\spaceos\target`, `CARGO_INCREMENTAL=0`, offline/locked resolution and `-j2`. Synthetic security tests used `SPACEOS_TEST_ISOLATION_ROOT=C:\spaceos`, synthetic `SPACEOS_CORE_TOKEN` (value not exported), and `TEMP`/`TMP=C:\spaceos\test-tmp` where required. No production key was loaded.

```powershell
cargo check --offline --locked -p space-bridge -p browser-provider -j2
cargo build --offline --locked -p space-bridge --features mvp-lab-runtime --bin mvp-core-fixture --bin mvp-runtime-fixture -p browser-provider --bin browser-provider -j2
cargo test --offline --locked -p space-core --lib browser_actions -j2 -- --test-threads=1
cargo test --offline --locked -p space-core --lib --test stage4_3a3_registration --test semantic_api -j2 -- --test-threads=1
cargo test --offline --locked -p space-core --test stage4_3a3_registration --test durable_file_provenance -j2 -- --test-threads=1
cargo test --offline --locked -p space-core --lib --test stage4_3a3_registration -j2 -- --test-threads=1
cargo test --offline --locked -p space-bridge --lib --test static_ui -j2 -- --test-threads=1
cargo test --offline --locked -p browser-provider --lib -j2 -- --test-threads=1
cargo test --offline --locked -p filesystem-provider --test verified_file_changed -j2 -- --test-threads=1
node tests/contract_tests.js
node tests/mission_verification_ui.cjs
node tests/vertical_slice_ii_ui.cjs
$env:SPACEOS_SCOPED_BROWSER_TEST='1'
node tests/vertical_slice_ii_corrections_runtime.cjs
node tests/scoped_browser_audit.cjs
```

Also ran Node VM syntax compilation of inline scripts in both actual World/Shell HTML assets, and `git diff --check` on changed tracked implementation files. Existing CRLF advisory/compiler warnings were not refactored away.

| Suite / evidence log | Executed result |
|---|---|
| Core library, `scoped-browser-final-core.log` | 53/53, including scoped auth/DTO, Policy, Browser observation, File admission/CAS and Mission storage tests |
| Semantic API, `scoped-browser-core-regressions.log` | 7/7 (the later registrar invocation in that first combined command failed its temp-root guard; corrected separately) |
| Registrar compatibility, final Core log | 1/1, existing legacy profile behavior retained |
| Durable File provenance/relations, `scoped-browser-registration-provenance-2.log` | 3/3, including real synthetic IPC, atomicity and restart |
| Bridge / static UI, `scoped-browser-bridge.log` | 43/43 + 4/4 |
| Browser Provider, `scoped-browser-provider.log` | 7/7 signed IPC/auth/reconnect and existing behavior |
| Filesystem verified change, `scoped-browser-filechanged-2.log` | 2/2, real watcher/CAS/authentication/restart/atomicity |
| Frontend contracts, `scoped-browser-contracts-final.log` | 103/103 static/HTTP-mock contracts; not real browser assertions |
| Mission UI, `scoped-browser-mission-ui-final.log` | 11/11 DOM/HTTP doubles; not another model Mission |
| Executable extracted World functions, `scoped-browser-ui-unit-final.log` | 26/26, including exact full scoped join, missing tuple denial and unavailable activation |
| Real runtime, `scoped-browser-runtime.json/.log` | 60 assertions, real Core/SQLite/HTTP/Provider/Chrome/WS and controlled restart |
| Post-cleanup audit, `scoped-browser-audit.log`, inventory | 20 checks, actual DB/UI/loaded hashes/cleanup/known-fixture-secret absence |

### Retained failures and limits of coverage

- First build failed because the new Provider method used an unqualified `ipc` import; corrected to `space_core::ipc`. Retained `scoped-browser-build.log`; corrected builds 2/3/final succeeded.
- First Core auth unit invocation omitted its required synthetic transport environment: 1 test failed before the assertion. Corrected invocation passed 2/2, subsequently included in final 53/53.
- First real runtime comparison incorrectly compared timestamp strings `Z` versus equivalent `+00:00`. Exact properties and nanosecond digits matched. Corrected the test's UTC suffix normalization, without discarding timestamp precision. Retained `scoped-browser-runtime-timestamp-fixture-failure.json/.log` and that fixture DB.
- Initial registrar temp-root guard failed; initial durable provenance invocation rejected a synthetic transport value without its expected `synthetic-` prefix. No security guard was weakened. Corrected environment passed.
- An initial combined Cargo command repeated `--lib` and was rejected before tests. Separate commands passed.
- Filesystem regression build initially could not replace the executable while the owned fixture had it open. The owned runtime was stopped normally; rerun passed. No owner/production process was stopped to release that file.
- SQL event-write failure injection, multi-source same-World runtime ambiguity, Chrome restart/port-reuse race and every possible privileged historical rewrite were not newly exercised end-to-end here. New source paths fail closed and existing relevant regressions passed, but no unexecuted negative scenario is marked runtime PASS. External screenshot execution is not atomically reversible with SQLite.

## 6. Hashes and preservation

Final loaded executables:

```text
mvp-core-fixture.exe    208c40f7455e977fb34dde13bbbd4f1565498b91326f1a2bdec0ea49da78b8e6
mvp-runtime-fixture.exe 7879d3a1637a992b38bb0278b42bbb490a199026c8f3984c0cf641c678465831
browser-provider.exe    a29ee38638a634679274a0c40a13e31b64ddf9f98336b3a9cbc070f39a0668dd
filesystem-provider.exe 72e28751327be3322e920a6658f8241a573c791723c1857c5f55c3345f8f1798
```

All executable paths are `C:\spaceos\target\debug\...`. They are test-loaded candidates, not replacements of production processes. The source inventory includes the complete final World/Shell and changed source/test hashes; evidence artifact hashes include the actual images. Manifest and its actively written audit log are deliberately excluded from self-hashing.

Known fixture credential scanning found no configured synthetic keys in public scoped-browser JSON/log artifacts; this is a bounded known-value check, not a universal secret-detection guarantee. Private fixture configuration remains local test evidence and must not be distributed as a public report. No personal OpenCode sessions/auth files or browser profile was read.

Free disk: approximately 27.06 GB at inspection and 26.80 GB at closure (decimal). Only the existing target was used. Earlier successful/failed run evidence and databases were preserved. Final cleanup records Core, Bridge, Filesystem Provider, Browser Provider and dedicated Chrome stopped; created UI tabs were closed.

## 7. Remaining limitations and verdict

| Subsystem | Status |
|---|---|
| Scoped registrar / Core World authorization | PASS for executed fixed-profile positive/negative checks |
| Real scoped get_tabs / signed screenshot / performer attribution | PASS |
| Actual World action/rendered frame | PASS |
| Shell scoped canonical list and unavailable actions | PASS in the separately identified earlier UI run |
| Historical forgery / wrong authority cases exercised | PASS; no admission bypass observed |
| Real Core/Provider restart and canonical/receipt persistence | PASS |
| Existing FileChanged / BrowserTabCreated Bridge WS | PASS |
| Legacy registration, File provenance/relations, Mission regressions | PASS within executed suites |
| Real Qwen/OpenCode Mission | NOT REPEATED; contract did not change |
| Production rollout/readiness | NOT AUTHORIZED / NOT CLAIMED |

Only get_tabs/screenshot are admitted. Legacy dashboard remains broad and unscoped; preserving it is not accepting its risks. Generic localhost HTTP caller authentication outside the synthetic lab remains an unresolved production prerequisite; Origin/CORS alone is not authentication. Lab login/CSRF is not a production login/storage decision. Signed requests rely on the existing trusted provider transport for responses; this task did not add independent response signatures or alter Windows ACLs.

After Core restart Bridge may surface one failed read while clearing its stale actor cache; a later normal refresh re-registers, with no automatic replay of screenshot execution and no legacy fallback. Some invalid/unavailable scoped operations still use existing HTTP 500 error mapping rather than refined 4xx categories. Unsupported buttons may be visible before the first mode-bearing response; Core denies them regardless. Shell supports scoped listing, not a new screenshot feature. These are not hidden authority grants.

Approval for any additional action, source-selection policy, production credential/ACL/caller-auth setup, durable Browser admission framework or deployment remains separate. No schema migration, new architecture layer, production modification, permanent live credential/profile change, commit, push or deployment was performed.
