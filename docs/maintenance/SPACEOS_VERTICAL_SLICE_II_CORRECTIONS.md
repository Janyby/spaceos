# Vertical Slice II — bounded UI and WebSocket corrections

Date: 2026-10-24, Europe/Moscow. Repository: `C:\spaceos`.

## Result

| Subsystem | Status | Executed evidence |
|---|---|---|
| Bridge provider-event delivery | PASS | Actual Core → scoped Bridge query → authenticated WebSocket delivered FileChanged and BrowserTabCreated. Existing semantic-command and FileCreated coverage retained. |
| Rejection/replay notification behavior | PASS | Real authenticated FileChanged/Browser replays returned applied=false/seq0. Rejected observations produced no DB writes or success frames; queued earlier frames were drained before measuring. |
| Exact World isolation/authentication | PASS | Foreign Browser event actually committed to a second World but absent from World A WS/HTTP/Core queries. Anonymous WS401; valid-session foreign-Origin WS403. |
| Search handler | PASS | Executable extracted-handler regression; actual served World opens canonical Focus, closes dropdown and records zero console errors. |
| Canonical BrowserTab representation | PASS | Real Tree/Space/search/Focus preserve one Core ID and snapshot title/URL/World; no virtual live tab objects or duplicate Browser search results. |
| Graph bounds | PASS | Existing bounded fallback unchanged; unrelated BrowserTab intentionally omitted, no Browser relation added. |
| Live-action mapping logic | PASS, unit coverage | Executed production helper/panel functions verify canonical-ID/raw-target/authority matching, mismatch/ambiguity denial, no retargeting after disappearance, and no external_id fallback. |
| Full real UI → CDP action | PARTIAL / not exercised positively | Existing test dashboard registrar is unconfigured; real /api/browser/tabs returns500. Canonical CDP buttons correctly stay unavailable. Separately authorized signed Core get_tabs succeeds. No grants broadened to force a positive UI action. |
| Frontend/Bridge/Browser/Mission regressions | PASS | 201 focused checks plus21 actual-runtime audit checks, detailed below. |
| Qwen Mission | NOT RETESTED | No Mission/MissionAttempt created; no model request. Prior successful workflow/evidence preserved. |
| Production | UNTOUCHED | No production DB, pipe, HTTP/CDP or OpenCode session access; existing owner processes preserved. |

The three requested corrections are implemented and verified within this coverage. This is not a full-product, production-readiness or Browser lifecycle PASS. Live operational-action E2E remains limited by existing fixture provisioning.

## 1. Root causes and exact changes

### Bridge: space-bridge/src/event_stream.rs

`EventPump::poll` previously sent only rows carrying semantic_operation or event_type FileCreated, advancing the cursor over valid FileChanged/BrowserTabCreated rows without sending them. Extended that same predicate with the two exact additional types using matches!. No query, scope, cursor update, initialization, authentication, delivery failure or publication mechanism changed.

Added tests:

- provider_creation_and_change_events_deliver_without_semantic_marker: FileCreated/FileChanged/BrowserTabCreated delivered; unrelated diagnostics still omitted; empty poll does not notify; cursor advances over filtered rows.
- provider_delivery_failure_preserves_cursor_for_retry: both new types retain the cursor on send failure, then deliver successfully on retry.

Existing initialization denial, query failure/recovery/order and bounded diagnostics tests remain passing. Core commit/publication and scoped service-read implementation are unchanged.

### Search: space-bridge/static/world.html

`handleDelegatedClick` used a local boolean hideDrop and then invoked hideDrop(), shadowing the existing function. Renamed only that local flag to shouldHideDrop and retained the real hideDrop() call after selectObj. The executable handler test verifies exact ID, openFocus=true, selection-before-hide and dropdown closure.

### Canonical BrowserTab presentation: same World asset

- `objName` now prefers persisted BrowserTab properties.title/url. `nameFor` no longer compares hashed external_id with live target_id or overwrites the creation label from a live cache.
- `tabObjs` now returns existing Core BrowserTab references from memberObjs('Web'), never synthesized tab:<target> objects. `clusterCounts`, nav/quick-access counters, Web cluster rows and `renderTreeMode` use those canonical identities. Space mini-nodes continue using the existing bounds and one node per ID. Search does not append a second Browser result from the operational cache.
- `renderSidebar` shows canonical World and URL (creation snapshot). It never interprets external_id as a raw CDP target. If no unambiguous live binding exists, it reports that the creation snapshot has no matching live authority and does not offer object-specific CDP buttons.
- Added small `browserLiveTarget` helper: match exact canonical tab_id and explicit properties.target_id; require v1 Browser identity, provider browser and source/epoch/instance/World properties. Any source_id/runtime_epoch/instance_id/world_id supplied by the live row must match. Missing or duplicate matches deny. Exact v1 tab_id binds the instance/source/epoch tuple when the live response lacks separate authority columns. The UI does not invent missing scope, use URL/port/title identity, recompute authorization, or trust a raw target alone.
- Added `openBrowserObject` and two delegated cases carrying the canonical object ID, resolving the current optional overlay immediately before the existing operation. The external hash is never passed as target_id.
- Existing browser panel state has an optional objectId. `openBrowser`/`bfRefresh` retain this semantic binding; if its live match disappears or refresh fails, selection clears rather than falling back to a different tab. Explicit operational tab selection/new-tab success clears the semantic binding; these remain existing live-only operations, not new persistence behavior.
- `renderGraphMode` fallback remains exactly Mission/Files/Projects with existing bounds. Only a comment documents intentional omission of unrelated Web snapshots. No Web cluster was silently added and no Browser relation created.

Operational rows stay in existing W.tabs/B.tabs caches; they are not another semantic model. All actual HTTP actions still use existing Bridge → Core Policy → signed Provider dispatch. UI joins do not grant permissions or authenticate a caller. No Browser Provider or Core/Policy library implementation changed in this task.

### Test-only bootstrap and new tests

- `space-bridge/src/bin/mvp-core-fixture.rs`: optional foreign_browser_source fixture binding, separate synthetic instance credential and explicit second-World observation grant for the test Browser principal. This is guarded by the existing mvp-lab-runtime feature, canonical test-root/config checks and explicit fixture configuration. It changes no production principal profile or registration policy; the World reader remains scoped only to World A.
- `tests/vertical_slice_ii_ui.cjs`: executable extracted handler, canonical render/join and panel-refresh tests with controlled DOM/action doubles.
- `tests/vertical_slice_ii_corrections_runtime.cjs`: real Core/Bridge/Filesystem/Browser/Chrome fixture, typed authenticated replay/denial/second-World observations and authenticated WS collector. No model or Mission execution path is invoked.
- `tests/vertical_slice_ii_corrections_audit.cjs`: independent read-only SQLite + actual HTTP/WS/auth + recorded CUA browser evidence assertions.
- `docs/maintenance/finalize-vertical-corrections.cjs`: source/binary/artifact hashes and known synthetic-secret scan.

Source comparison against the preserved full-slice-ii-inventory.json found changes in only the three previously inventoried files: event_stream.rs, world.html and the test Core launcher. Core/Policy/identity/receipt/Provider/Mission/Gateway/Shell sources remain byte-identical to that baseline. Newly added tests/reports are listed above. Dirty unrelated worktree changes were not reverted.

## 2. Real isolated runtime

Run: `C:\spaceos\test-tmp\vertical-corrections-yYE7Jp`. Actual SQLite: core.db under that directory. Successful run 22:51:31–22:56:57 Moscow time (raw evidence timestamps retain UTC ISO format).

- World A: cbf38c40-c2ca-498f-8597-4d8ff7becb33.
- World B: f6308168-7165-4060-9d74-a01fdea9d8eb.
- HTTP21973; synthetic page21974; dedicated real Chrome CDP21975. No8765/9222 use.
- Private UUID Core/Provider pipes and separate synthetic principal/instance/dispatch credentials; all local secret configuration stays in the run's fixture.secret.json, not this report.
- Real Chrome155.0.8059.85 PID17460, owned non-personal headless profile under this run. Source A is real Chrome discovery. Source B is explicitly synthetic typed IPC, not a second real Chrome or a mocked replacement for source A.

| Actual executable under C:\spaceos\target\debug | PID | SHA-256 |
|---|---|---|
| mvp-core-fixture.exe, actual Core library |21384|725b2b2a5fa53d9d33d458cc7a2dcfa3ac1aff132d1fa3c9b9260c49a59862e5|
| mvp-runtime-fixture.exe, actual Bridge handlers |34308|328f0be7711d1d6d825d3b23c975549583734b6357aae0f8cf5600c800c72f54|
| filesystem-provider.exe |29896|80a040038c2d161acf2ad494b283941ca6464844156c90a5e236b06940ea4ea5|
| browser-provider.exe |31240|6dc8dd11dbc4bac9abc7ca36eea149ca40f4bd7c9f6e12c592ed7dcfd59f87ae|

Only the two changed Rust test launchers were rebuilt, using current Bridge/Core libraries and the existing target. Providers were reused at their verified current-source hashes, unchanged from Slice II. No new target/laboratory or full workspace build.

Canonical File:8a3b1ac5-8e2d-4fa6-82bb-29a835c48f77. Real watcher creation/physical append retained ID/World and v4 receipt, updated metadata to actual disk size. Its committed FileChanged notifications were received with IDs65ed5346-5fd5-405d-a039-4fa648f82a39(seq12),4c4d390e-2953-495f-9764-128043f6b86f(seq13),1f778b09-73c9-4b0a-b4ca-ddae5202c924(seq18). Initial creation can generate additional actual OS metadata-change notifications; these are distinct committed events, not replay duplicates.

Canonical BrowserTab: browser-tab-v1-7430c47a14b0f0bbd8a5e44fc609d056557fffd5f2684de6699be8c332750165. Actual BrowserTabCreated WS event: browser-tab-created-event-v1:7430c47a14b0f0bbd8a5e44fc609d056557fffd5f2684de6699be8c332750165(seq10). Same event ID joins to real SQLite.

Browser source b202b7de-cb81-4d8c-bcbf-a92af9e5fef1; epoch18793553-83c8-4efd-8c71-2ab76ce1077b; instance vertical-browser; raw target118A83D73EDDEB862740ABD5A79B7FE3; title SpaceOS synthetic acceptance page; URL http://127.0.0.1:21974/evidence.html. Core snapshot stays canonical through repeated discovery/replay.

Actual replay replies: FileChanged and BrowserTabCreated both ok=true, applied=false,seq0. Browser reply returns the existing canonical object despite bounded candidate title/URL differences. A forged Browser ID and cross-World File observation return ok=false. After draining earlier legitimate relation frames, measured object/event/observation-log counts and success-frame count did not grow during the rejection/replay window. The final collector contains each provider event ID only once.

Foreign Browser event browser-tab-created-event-v1:66298c9d2eeb84dbcd8d3d9d3b035b418ded407e479e0b9512cdca36486303b0 really exists in SQLite for World B. It is absent from World A's WS, scoped HTTP event/object responses and Core scoped snapshot. Explicit World B query by the World A reader is denied. This tests a committed foreign event, not just a rejected cross-World request.

Authenticated WS upgraded successfully; no-session WS handshake returned401; foreign-Origin handshake with valid cookie returned403. No Origin/session checks disabled. Final SQLite integrity ok:9 objects (7 in World A,2 foreign),3 relations,21 events; no Mission/MissionAttempt. No schema change/migration.

## 3. Real browser evidence

Computer-use skill instructions were read; actual browser interaction used the preferred CUA browser interface, not an ambiguous desktop window. Only created IAB tab8 was used/closed. Existing owner tab and Chrome profiles were untouched.

Reused localhost origin initially loaded the older cached World. DOM script inspection showed the correction markers absent, so that page was not counted as corrected evidence. An explicit test URL `http://127.0.0.1:21973/?corrections=20261024-1` loaded the current asset; both marker checks passed and independently fetched served HTML hash equals the file on disk. No cache policy or production endpoint changed. Owners must reload the current asset when testing; cache persistence remains an operational consideration.

On the verified corrected asset:

- Tree contains exactly one selectable BrowserTab with the canonical Core ID/title.
- Space has exactly one Browser mini-node with that ID, despite separate live-cache support.
- Search by canonical title returns exactly one canonical result, not a duplicate tab:<target> entry.
- Selection opens Focus with the saved title and ID, closes dropdown (computed display none), and produces zero console errors in the recorded corrected-page run.
- Sidebar displays the exact World UUID and URL labelled creation snapshot. The unavailable operational overlay does not offer Show in Chromium/open-frame buttons for this object.
- Graph retains existing bounded membership and the three actual semantic relations. The unrelated BrowserTab is intentionally absent, not duplicated or linked artificially.

Artifacts: `vertical-slice-ii-corrections-browser-ui.json`, `VERTICAL_SLICE_II_CORRECTED_FOCUS.png`. The image is actual UI at the inherited narrow IAB viewport, not a responsive-layout certification or screenshot mock. DOM/accessibility additionally records offscreen metadata and exact identities.

## 4. Commands and results

Common Rust environment: CARGO_TARGET_DIR=C:\spaceos\target, CARGO_INCREMENTAL=0, SPACEOS_TEST_ISOLATION_ROOT=C:\spaceos. No production credentials loaded for tests.

```powershell
cargo build --offline --locked -p space-bridge --features mvp-lab-runtime --bin mvp-core-fixture --bin mvp-runtime-fixture -j2
cargo test --offline --locked -p space-bridge --lib --test static_ui -j2 -- --test-threads=1
cargo test --offline --locked -p browser-provider --lib -j2 -- --test-threads=1
cargo test --offline --locked -p space-core --lib mission_storage::tests -j2 -- --test-threads=1
node tests/contract_tests.js
node tests/mission_verification_ui.cjs
node tests/vertical_slice_ii_ui.cjs
node tests/vertical_slice_ii_corrections_runtime.cjs
node tests/vertical_slice_ii_corrections_audit.cjs
node docs/maintenance/finalize-vertical-corrections.cjs
```

| Suite / maintenance log prefix vertical-slice-ii-corrections- | Result |
|---|---|
| rust.log | Bridge43/43 (including5 EventPump tests), static UI4/4 |
| browser.log | Browser7/7, existing signed IPC/auth/reconnect coverage |
| mission-core.log | Core Mission10/10 |
| frontend.log | Existing static/HTTP-mock frontend contracts103/103 |
| mission-ui.log | Existing verification handler DOM/HTTP-double contracts11/11 |
| ui-unit.log | New executable extracted JS tests23/23 |
| runtime.log/runtime.json | Real Providers/Core/Bridge/Chrome and authenticated WS/replay/rejection/isolation assertions passed |
| audit.log/audit.json | Independent21 checks passed, using actual SQLite/HTTP/WS and actual CUA browser evidence |

Total focused regressions:201 PASS. Unit/mocked frontend checks are not called real browser E2E; genuine transport/UI evidence is separately described above. No Qwen execution was repeated because no Mission/Gateway contract changed. The reused driver contains inert historical model/cleanup fields, but its executed path does not configure a Mission runtime or issue model requests; empty gateway_results and zero Mission/MissionAttempt rows independently confirm this.

Retained setup failures:

- Initial Cargo command accidentally repeated --lib and was rejected before running tests. Corrected commands above passed; no application code change was made for this invocation error.
- First transport fixture compared total frames before already-queued RelationCreated messages had arrived (8 versus5). Counts showed no observation writes; fixed the fixture to await the last legitimate relation before measuring silence. The prior failed evidence/run remain at vertical-slice-ii-corrections-queued-frame-fixture-failure.json/.log and test-tmp/vertical-corrections-xwDXe7. It is not treated as a replay defect or erased.
- Extended panel-function unit fixture initially omitted the bfShot double; recorded ReferenceError, then supplied the controlled timer callback double. Retained ui-fixture-failure.log; production assertions were not weakened. Final23 tests pass.
- Existing compiler warnings remain (async trait/dead code); no unrelated warning cleanup. git diff --check found no whitespace errors in changed implementation files.

## 5. Live operational limit and unchanged security boundaries

Actual signed Core-routed stat and get_tabs succeeded with the test inspector's existing explicitly scoped action grants. The latter returns the real canonical tab_id/raw target_id. But the real World route /api/browser/tabs returns HTTP500: `dashboard service registration denied: service registration credential missing or invalid`. This pre-existing fixture does not provision that dashboard registrar. We did not grant extra authority, reuse a reader token, change a capability profile or inject a fake successful overlay into the real browser.

Consequently positive real World → activation/screenshot/navigation was not exercised. Positive/negative mapping and no-retarget behavior are executable function tests; actual real-UI evidence proves unavailable actions stay unavailable. Any future positive operational test must use an explicitly provisioned existing dashboard identity under a separately authorized fixture configuration, not a Policy bypass. No claim that every legacy Browser action or epoch race is solved.

No BrowserTabUpdated, navigation/title persistence, close/delete synchronization, new Browser relation, new lifecycle or automatic epoch rotation was added. Historical/legacy Browser records remain readable through current queries; without a provable matching v1 live identity they do not gain a new object-specific CDP mapping.

Other Slice II findings remain out of scope: Summary progress/summary ambiguity, generic World scope labels, system/provider health presentation, canvas click investigation, stale Mission diagnostic execution_status fields, ETW/model-process correlation and production credential/ACL/localhost-auth prerequisites. No full-product PASS or deployment authorization follows from these corrections.

## 6. Hashes, cleanup and preservation

Current hashes:

```text
space-bridge/src/event_stream.rs d97b679019754396599b2a13213d182013842c6f27c376066f4a95785ccb2827
space-bridge/src/bin/mvp-core-fixture.rs 34d135a59cb79bd68f126b8428a91c4b61f96131fe45b702020c664306f77d94
space-bridge/static/world.html 2cd5285ad2d1d15691b46b42a11150cc3e34d02875773c6d72a2e0519a17c3a4
```

Complete previous/current source hashes, new test hashes, loaded binary hashes and evidence artifact hashes: `vertical-slice-ii-corrections-inventory.json`. Known synthetic-secret scan found zero matches in exported correction JSON/log files. Local synthetic fixture secrets are not public reports. No production secrets were read/copied/printed.

Owned Core21384, Bridge34308, Filesystem29896, Browser31240 and dedicated Chrome/profile were stopped; the created UI tab was closed. Existing owner Core11956, Browser26108, sibling-lab Filesystem14808 and OpenCode25196 remained at their original executable paths and were not contacted/signalled. Post-run process inspection found no owned Chrome profile process. All cleanup flags true.

Free disk monitored:27,290,529,792 bytes at entry;27,049,611,264 bytes after cleanup. Only existing target used. No laboratory created, previous DB/evidence deleted, schema/Policy/Mission changed, production database opened, commit, push or deployment performed.
