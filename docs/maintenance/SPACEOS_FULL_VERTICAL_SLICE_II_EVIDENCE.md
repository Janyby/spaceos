# Full vertical slice II — execution evidence

Date:2026-10-24. All run times below UTC; screenshots display Moscow time. All hashes SHA-256. Evidence describes isolated integration, never production E2E.

## 1. Runtime and build provenance

Main run: `C:\spaceos\test-tmp\full-slice-ii-NKAHWN`; database: `core.db` in that directory. Start19:24:07; controlled restart ready19:32:04; owned cleanup finished19:36:25. World `e0551103-d8c8-4c6a-80f5-033defd5bd6c`.

```powershell
Set-Location C:\spaceos
$env:CARGO_TARGET_DIR='C:\spaceos\target'
$env:CARGO_INCREMENTAL='0'
cargo build --offline --locked -p space-bridge --features mvp-lab-runtime --bin mvp-core-fixture --bin mvp-runtime-fixture --bin spaceos-mcp -p filesystem-provider --bin filesystem-provider -p browser-provider --bin browser-provider -j2
node tests/full_vertical_slice_ii.cjs
node tests/check_full_vertical_slice_ii.cjs --completed
node tests/check_full_vertical_slice_ii_http.cjs
```

Build PASS in full-slice-ii-build.log,22.46seconds; no new target. rustc1.99.0(b940084d7 2026-09-28), Node24.20.0, OpenCode2.1.3, Chrome155.0.8059.85. Before run free disk26,952,355,840 bytes; after cleanup27,295,232,000 bytes. Available space was monitored; no unrelated cleanup was attempted, and fluctuations do not imply files were deleted by this task.

The guarded test Core/Bridge launchers link actual current production libraries/handlers; only bootstrapping/configuration is synthetic. This is not a replacement server. The full repository was dirty before work; unrelated owner changes were preserved.

## 2. Exact processes and loaded executable hashes

| Role / executable under C:\spaceos\target\debug | Initial PID | Restart PID | Runtime hash |
|---|---|---|---|
| mvp-core-fixture.exe |23604|35064|f2afe7c407bd433c0be858de72730e69397b4a72d014d7a010aec7fdd406fb76|
| mvp-runtime-fixture.exe (real Bridge/Gateway)|16540|20596|56a24ae9013b408b22d5b9396d81dd3bbda0fb0d233d03f533b26fa61b4189dd|
| filesystem-provider.exe |25168|same process|80a040038c2d161acf2ad494b283941ca6464844156c90a5e236b06940ea4ea5|
| browser-provider.exe |35620|same process|6dc8dd11dbc4bac9abc7ca36eea149ca40f4bd7c9f6e12c592ed7dcfd59f87ae|
| spaceos-mcp.exe |owned by attempt MCP connection|no new attempt|4afeaaa9ae7b0ce4d1956f6db3110e6763e8b4c79ecbf22c8382a697f99bb67c|

Dedicated Chrome PID9992: `C:\Program Files\Google\Chrome\Application\chrome.exe`, headless, profile inside this run, explicit loopback CDP21975, synthetic page `http://127.0.0.1:21974/evidence.html`. Exact process arguments and version are retained in full-slice-ii-smoke-evidence.json. It was not the owner's Chrome. No connection to9222.

Existing OpenCode API info was obtained through its supported installed CLI: `C:\Users\Gamer\AppData\Roaming\ai.opencode.desktop\cli\2.1.3-x64\opencode-cli.exe api GET /api/info`. Version2.1.3/PID25196 verified; actual model catalog confirmed enabled tool-capable unsloth/qwen-q8. No auth files, unrelated sessions, profiles or global provider configuration were read/changed.

Pre-existing Core11956 (`C:\spaceos\target\release\space-core.exe`), Browser26108 (`C:\spaceos\target\release\browser-provider.exe`), Filesystem14808 (`C:\spaceos-i5-runtime-lab\target\debug\filesystem-provider.exe`) and OpenCode25196 remained alive at their original paths after cleanup. Owned PIDs23604/16540/25168/35620/35064/20596 were absent. No Chrome process with this run's profile remained. No request to8765 or any production IPC endpoint/database.

## 3. Canonical identities

| Resource | ID |
|---|---|
| Project |d24c2f20-cb70-4a9f-918c-d01ab7cd5f32|
| Task |27b147f7-76a2-436c-9c78-3d77b4f23d76|
| Summary (Note, kind ProgressSummary) |501d0f6f-1ddc-4fc1-89dc-c6365d503247|
| Acceptance Evidence |77ac003e-a4e0-4174-ae5a-c31f025fd837|
| File |392bf2dd-1813-473c-967c-f15648c4a97a|
| BrowserTab |browser-tab-v1-041fc5633ad8d9a7dde8a4280a588020cb7d0f5cc859c4ddf042eeeec0a2c407|
| Project → Task related_to |ec9d5742-75c8-40c2-a38c-d8d5429b0685|
| Task → Evidence related_to |28bb9de2-72d0-4632-ab36-a06e820135f7|
| Task → File related_to |5b1db330-dfa3-4d7a-abe6-2d58e1e4de1f|
| Executed Mission |3a984a38-bdff-435f-a89d-2643c045a369|
| Execution attempt |f989f770-73b3-40b9-84b5-ffb29b017d0c|
| MissionAttempt object |a247a739-8e3a-4fba-9704-9706f42ff163|
| Cancelled unlaunched draft |3bb74c47-134f-400c-af27-dc5da44c36fd|
| Foreign-World guard |a7a26f95-20e7-475d-b6a3-0192e5f14798|

The executed Mission's OpenCode session is ses_e909e189cffezh5GWN3VpQezgi. All initial/final properties, raw synthetic SQLite records, relation IDs, events, binding and model selection are retained in the evidence JSON/database. The unused draft is not erased or counted as another executor attempt.

## 4. Provider evidence

Filesystem principal filesystem; instance vertical-filesystem; root `C:\spaceos\test-tmp\full-slice-ii-NKAHWN\project-files`. The real watcher observed physical creation of acceptance.txt (104bytes), then physical append to136bytes. We waited for a version newer than the pre-append base and matching disk size, not merely any FileChanged. Initial create can itself generate a metadata-change notification.

FileCreated event be830f1d-182b-4a81-82bc-853a8fcd2377(seq11). FileChanged events e18371bf-7944-40fe-94db-9f4380cddc05(seq12) and e91db9a7-3b51-4783-8792-ec56e9df1a36(seq17). Same File ID/external path/created_at/World; authoritative updated_at increases. The retained receipt still references the original FileCreated event/observation, type File, instance, root and World. It was not minted by the relation operation. Exact receipt fields are in full-slice-ii-audit.json and raw final evidence.

Browser principal browser; separate instance vertical-browser. Configured source4c00482b-8436-4df9-b6b6-6857dbe6b834; epochbf485ad2-d5d5-41a3-95f2-2929a0a40cf5; target122F5CD062DA9C99DF63B71C26EA540F. The test fixture explicitly binds the observed debugger identity before Core startup; no trust-on-restart or automatic epoch rotation. Canonical created_at=updated_at=2026-10-24T19:24:09.158524800Z, title SpaceOS synthetic acceptance page, URL http://127.0.0.1:21974/evidence.html, exact World. Event `browser-tab-created-event-v1:041fc5633ad8d9a7dde8a4280a588020cb7d0f5cc859c4ddf042eeeec0a2c407`, seq10. Three durable browser authority/runtime pins persisted. Continuous discovery/replay and real Core restart left the original object/event unchanged. Chrome was not restarted in this slice; the preceding stage separately tested same-port runtime replacement denial.

Real signed Core-routed stat returned Completed with the actual metadata and provider performer PID25168. Signed get_tabs returned Completed with one canonical tab_id plus its raw target_id, title and URL. Repeat operations succeeded after Core restart. These are real provider operations; not evidence of navigation, page-content reading or lifecycle updates.

## 5. Actual model-originated tools and mutations

Ordered Gateway attempt trace:

1. spaceos_query_relations — returned the actual three relations, including Task → File.
2. spaceos_get_object(77ac003e-a4e0-4174-ae5a-c31f025fd837) — synthetic acceptance Evidence result PASS.
3. spaceos_get_object(392bf2dd-1813-473c-967c-f15648c4a97a) — real admitted File metadata.
4. spaceos_get_object(browser-tab-v1-041fc5633ad8d9a7dde8a4280a588020cb7d0f5cc859c4ddf042eeeec0a2c407) — real admitted BrowserTab snapshot.
5. spaceos_patch_object(Task) — status completed, reviewed_evidence_id and observed_file_id.
6. spaceos_patch_object(Summary) — progress, relationship_used, task_id, file_id and browser_tab_id.

All six actual Core replies ok; no relation mutation by canonical-mvp-agent. Its synthetic grant is only semantic.read + semantic.object.patch for the test World. vertical-file-linker, a distinct explicitly authorized test agent, created the File relation with its own credentials. space-world-user and permanent Mission-agent profiles were not changed.

Agent ObjectUpdated events: ed427846-8fba-40da-87aa-fd0cb79f8368(seq30, Task),4e0cb5fe-c083-4839-86fd-0482681a84d3(seq32, Summary). Correct actor canonical-mvp-agent, World and object IDs. RelationCreated63b918e6-9648-41a8-a399-73c2c3d0beca(seq19) attributes Task → File to vertical-file-linker.

Executor status succeeded; Core begin_verification event07ff265f-c0a5-4800-a6bc-5437e6a7bb81(seq34). Owner-confirmed MissionVerified b12c255a-f4c5-44f3-98b6-32c8cc432737(seq35), expected_attempt_id f989f770-73b3-40b9-84b5-ffb29b017d0c, method manual-check, passed=true, note="q", then completion transition seq36. The owner explicitly confirmed performing that UI action. We did not replace the short note with our proposed detailed text.

## 6. HTTP, WebSocket, persistence and SpaceTrace

Authenticated actual Bridge replies:

- POST /api/objects/query:200,10 exact-World objects including the five required resource kinds; foreign guard absent.
- POST /api/relations/query:200,actual relations.
- GET /api/events?limit=500:200,including FileCreated/FileChanged/BrowserTabCreated/MissionVerified.
- GET /api/context:200.
- Anonymous object-query POST:401; authenticated foreign-Origin object-query POST:403. No check disabled.

Evidence: full-slice-ii-http.json/.log. The initial negative test expected403 for anonymous access, but the actual correct unauthenticated status is401; corrected assertion only. Retained full-slice-ii-http-status-expectation.log records that fixture expectation failure.

The separate authenticated exact-Origin raw WS collector really received init and semantic_event frames for FileCreated, relations, Mission execution/verification and both agent ObjectUpdated events. It did **not** receive FileChanged or BrowserTabCreated. These event IDs are present in the same actual GET /api/events and SQLite; EventPump::poll filters them because they lack semantic_operation and are not FileCreated. The original failed assertion is preserved in full-slice-ii-ws-failure.log; the audit explicitly records both requirements FAIL, not mock success.

Collector segments before/after restart are retained separately in full-slice-ii-before-restart.json and full-slice-ii-smoke-evidence.json. The audit combines these immutable actual segments for historical coverage; it does not fabricate notifications. Core/Bridge restart closes old sessions, requiring explicit synthetic reauthentication. No seamless in-flight Mission recovery was tested: the attempt was already terminal.

Final SQLite:11 objects total (10 in the visible World,one foreign guard),3 relations,40 total events,integrity ok. Scoped query/UI event count is31, not the entire SQLite event table. Existing unscoped/other-scope events are not made visible by granting global read. The read-only audit passed24 checks: stable objects/relations, historic events, receipt, provider identity, model tool results, exact attempt verification and foreign guard. Both failed WS requirements remain in its gaps array.

Pre-restart SpaceTrace:29 distinct EventPublished traces join to29 actual Core/SQLite event IDs;10 match the attempt correlation, including execution transitions. BrowserTabCreated has a real EventPublished match despite missing Bridge WS delivery. Trace buffer/trace query are not another semantic database; restart-local history is not promised persistent. No ETW collector/model-process correlation was executed or claimed.

## 7. Actual UI evidence

Created only IAB tabs6(World) and7(Shell popup from +Mission); owner tab1/8765 was not navigated. Dedicated provider Chrome is a separate headless process/profile. UI actions used actual served World/Shell and authenticated Bridge, not a mock. Both test tabs were closed after evidence capture.

Artifacts:

- FULL_SLICE_II_SPACE.png, TREE.png, GRAPH.png, FOCUS.png.
- FULL_SLICE_II_WORLD_COMPLETED.png and WORLD_AFTER_RESTART.png show completed in World Focus.
- FULL_SLICE_II_BROWSER_FOCUS.png shows the canonical BrowserTab Focus.
- FULL_SLICE_II_SHELL_SHARED.png and MISSION_COMPLETED.png plus the actual DOM/accessibility evidence in full-slice-ii-ui.json.

The narrow actual IAB viewport is approximately682×884 (Shell popup667×899); no broad responsive-layout PASS. Some screenshots are scrolled or show an overview rather than all offscreen fields; canonical identity/state assertions additionally use actual DOM/accessibility and Core/SQLite. Graph has9 object nodes after restart; Tree has7 selectable canonical objects; Shell lists10. This difference is explained by the actual render functions and is not claimed to be representation parity. The raw SVG path count is not treated as relation count.

Post-reload World console capture contains four actual `TypeError: hideDrop is not a function` entries at world.html1964; Shell capture has no error entries. No clean-console PASS. Actual search selection changed Focus before throwing, so the selection succeeded but the dropdown remained open. Details are in the gaps report.

## 8. Tests, changes and preservation

```powershell
$env:SPACEOS_TEST_ISOLATION_ROOT='C:\spaceos'
$env:SPACEOS_CORE_TOKEN='synthetic-full-slice-ii-regression-token'
cargo test --offline --locked -p space-bridge --lib --test static_ui -j2 -- --test-threads=1
node tests/mission_verification_ui.cjs
node tests/contract_tests.js
node docs/maintenance/finalize-full-slice-ii.cjs
```

Logs: full-slice-ii-bridge-regressions.log(41+4 PASS), full-slice-ii-verification-contracts.log(11 PASS), full-slice-ii-frontend-contracts.log(103 PASS). Frontend103 uses static parity and an HTTP mock; verification11 uses DOM/HTTP doubles. Neither is mislabeled real browser E2E. Real UI/model/runtime evidence is separately above.

Only task source changes: existing test-only space-bridge/src/bin/mvp-core-fixture.rs optional browser_source injection; new tests/full_vertical_slice_ii.cjs, tests/check_full_vertical_slice_ii.cjs, tests/check_full_vertical_slice_ii_http.cjs; new maintenance inventory generator, evidence and reports. No Core/Provider/Policy/Gateway/Mission/SpaceTrace/UI implementation changed during this verification stage.

Preserved failed preflight: full-slice-ii-preflight-timing-failure.json/.log and test-tmp/full-slice-ii-dpEgGa. The first test mistakenly treated an earlier create-time FileChanged as proof of the later append; fixed the wait to require a newer version plus actual disk size. No model request in that run. All old slice/provider evidence remains intact.

Full source/crate/lock/UI/test/binary/artifact hash inventory: full-slice-ii-inventory.json. World hash d231c34b869a58d002335b06cbb310ba49d6b282054f842ba67965845d1ab575; Shell hash243e72cd32d5b649a5de9745e6cd410f2a9021cdb0bb4d1d68d8f419c1b428db. No frontend edit.

Known synthetic credential scan found zero matches in exported II JSON/log artifacts. Local fixture.secret.json deliberately contains only this run's synthetic credentials and is not a public report. No production credentials were read or copied. Cleanup flags for owned Core/Bridge/Filesystem/Browser/Chrome are all true; process inspection confirmed absence. No commit, push or deployment.
