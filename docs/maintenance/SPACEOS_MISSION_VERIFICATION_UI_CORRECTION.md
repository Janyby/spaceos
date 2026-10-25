# Mission verification UI correction

2026-10-24. Current MVP candidate: `C:\spaceos-mvp2b2-execution-lab`.

## Verdict

**Correction verified in the existing laboratory. Historical second-confirmation root cause remains UNKNOWN.** Do not interpret the repaired gaps as forensic proof of what caused the earlier Mission A confirmation.

No model request, new laboratory, OpenCode profile, schema migration, production application change, commit, push or deployment. Original real-execution database and earlier execution evidence remain unchanged. New reports/helpers are in the existing `C:\spaceos\docs\maintenance` directory.

## Confirmed findings

The old `index.html::missionVerify(missionId, passed)` called native `prompt()`, accepted an empty string, generated a new random command_id each invocation and had no verification-specific in-flight guard. Its button carried an explicit literal mission_id; it did not derive the ID from a shared selected-Mission variable. No attempt_id was submitted.

Bridge `mission_commands::verify` forwarded that path ID, expected lifecycle state and evidence to Core. Core `MissionOperation::Verify` checked ownership/World and lifecycle state but had no expected-attempt comparison and accepted arbitrary detail, including an empty manual-check note. Existing generic command replay protected only reuse of the same command_id; repeated clicks generated different IDs.

Actual old staged Core/Bridge HTTP reproduction on a disposable copy of the two existing Missions: two blank-note FAIL submissions to B both returned 200 and produced two verification events. Mission A remained byte-for-byte unchanged. This establishes empty-note and duplicate-submission defects, **not** cross-Mission redirection.

The embedded browser also produced a real console error from the cached old handler: `Error: prompt() is not supported`, at `index.html:2728`. This proves native-prompt incompatibility in this browser, not that it caused the earlier empty notes or second Mission approval. No native-dialog error was silently converted to a successful verification in this reproduction.

The earlier second confirmation cannot be attributed to duplicate submission, stale selection or handler reuse with the available evidence. Source inspection found no shared selection rewrite, and explicit B requests did not mutate A. Possible timing/input causes remain hypotheses. The original events were not altered to conceal this uncertainty.

## Exact minimal application changes

Four files, all in the existing MVP candidate:

1. `space-bridge/static/index.html`: `missionVerify` and `missionButtons`.
   - Replace native prompt with one small accessible DOM dialog showing full Mission ID, attempt ID and PASS/FAIL outcome.
   - Render the attempt ID on each button and capture an immutable binding when opening; a second Mission cannot replace an open binding.
   - Reject blank/whitespace evidence locally before HTTP.
   - Disable confirmation while saving, guard re-entry, allow cancellation without submission.
   - Preserve the command ID for retry of an unchanged request after an uncertain response; changed evidence gets a new ID.
   - Send expected_attempt_id with the explicit bound Mission path. No selection-dependent targeting or model substitution.
2. `space-bridge/src/mission_commands.rs`: `Verify` DTO and `verify` adapter forward expected_attempt_id unchanged.
3. `space-core/src/mission.rs`: optional expected_attempt_id on the existing Verify operation. No new endpoint/table/authority.
4. `space-core/src/mission_storage.rs`: transactional Verify branch compares expected_attempt_id against the current owned Mission's attempt. Missing/mismatched ID is a conflict for executor-backed Missions; omission/null remains compatible with legacy Missions without an attempt. `manual-check` requires a trimmed nonempty note bounded by 16,000 bytes. Identical already-recorded verification under a different command ID is rejected; exact command replay remains idempotent. Add focused two-Mission unit coverage and update executor-backed tests to supply their attempt IDs.

Core authentication, World ownership, successful-attempt validation, lifecycle CAS, transaction/event atomicity, Policy and immutable attempt history remain in force. Validation occurs inside Core, not just in Bridge or JavaScript. A custom client cannot bypass the attempt comparison using the generic mission_command endpoint.

### Empty-note contract decision

Historically empty notes were valid because detail was unrestricted JSON. There was no pre-existing nonempty manual-note rule to claim was enforced. This correction deliberately narrows **manual-check** only: a human confirmation must supply evidence. Other verification methods retain their existing detail contract. Historical empty records are preserved; there is no migration or retroactive invalidation.

## Test state and preservation

Existing Mission A: `5b9b6015-2569-4c6a-b444-8bd81b2b9426`, attempt `0a1c8e4c-1a8a-4d8d-aecc-87c916470046`.

Existing Mission B: `5d1aad05-4ecf-4baf-a3fc-64957fc8bb12`, attempt `1dafffca-48bc-4b6a-be5a-9de2bbcf131e`.

Same synthetic World and immutable attempts as the prior real execution. Since both authoritative original Missions were already completed, verification tests used disposable SQLite backups inside the same laboratory's existing run directory. Fixtures explicitly restore verifying properties and the event prefix through seq 29, before the historical approvals. This fixture preparation is not a claimed user/Core lifecycle transition and never modifies the original authoritative database/history.

The first clone run incorrectly kept old completion command events while resetting properties. The existing deterministic completion command then replayed instead of changing state. This was a **test-fixture inconsistency**, not a new verified runtime defect. Its before/after evidence is preserved in `SPACEOS_MISSION_VERIFICATION_FIXTURE_COLLISION.json` and `SPACEOS_MISSION_VERIFICATION_UI_FIXTURE_COLLISION.json`. The corrected consistent fixture is `verification-fix/runtime-4BFhOB`.

All prior `SPACEOS_COMPLETE_*` artifact hashes were recorded before testing and matched afterward. Original real-execution SQLite SHA-256 also matched after testing. Before-source copies and new build outputs are under `test-tmp/first-local-mission-AdE4I8/verification-fix`; the previous completion-fix binaries/build manifest were not overwritten.

## Real browser + HTTP + IPC results

Actual laboratory origin: `http://127.0.0.1:21973`. Production 8765/9222 endpoints were not used. Synthetic existing lab login, HttpOnly session cookie and CSRF checks remained enabled. Provider/model settings and normal OpenCode were not accessed or modified.

- Dialogs displayed separate correct A/B attempt bindings. Cancel of A submitted no verification.
- Blank evidence showed `Verification evidence must not be empty.`.
- A genuine double-click on B's Confirm verification generated **one** new MissionVerified event with a nonempty note and B's exact expected_attempt_id.
- B completed through the existing Core command; A's full SQLite object row remained unchanged. Both MissionAttempt rows were unchanged.
- Missing attempt -> HTTP 409; A attempt submitted for B -> 409; whitespace note -> 400; foreign Origin -> 403. These probes produced no events.
- Direct authenticated Core IPC with the foreign/stale attempt was denied without relying on Bridge validation.
- After browser submission: replay same command -> 200 with no mutation; new duplicate -> 409; B attempt submitted for A -> 409. Objects and events stayed identical across these probes.
- The mismatched-attempt checks verify stale/foreign token rejection. No new agent execution or retry was launched to manufacture a live attempt rollover.
- Real browser World and Shell displayed B completed and A verifying. Actual Core/Bridge restart preserved every object/event row and produced no extra events. Reopened World/Shell showed the same persisted B outcome.

Actual corrected UI events in the disposable fixture:

| Seq | UTC timestamp | Event | Mission |
| --- | --- | --- | --- |
| 34 | 2026-10-24T09:50:28.667019800+00:00 | MissionVerified | B |
| 35 | 2026-10-24T09:50:28.728334700+00:00 | MissionTransitioned / complete | B |

Sequence gaps reflect the explicit point-in-time fixture setup, not deleted production events. Full payloads, ID bindings and original-history preservation hashes are in the runtime evidence.

## Executed commands and tests

Incremental rebuild only, reusing existing dependency/native caches without another Rust target directory:

```powershell
node C:\spaceos\docs\maintenance\build-mission-verification.cjs --completion-only
```

Exact compiler commands/source hashes/output hashes: `verification-fix/build.json`. New binary SHA-256:

- Core fixture: `9fc39f572d13c0844229c074246d68ef1700d40282326a2df33d6c4d56fff6d0`.
- Bridge runtime: `4d1fc09ec1353e99de1912884964deaecba082872392514cefcfcddca6d25646`.

Executed with SPACEOS_TEST_ISOLATION_ROOT set to the existing candidate, synthetic test token, and TEMP/TMP inside verification-fix:

```powershell
& C:\spaceos-mvp2b2-execution-lab\test-tmp\first-local-mission-AdE4I8\verification-fix\core-tests.exe --test-threads=1
& C:\spaceos-mvp2b2-execution-lab\test-tmp\first-local-mission-AdE4I8\verification-fix\bridge-tests.exe --test-threads=1
node C:\spaceos\docs\maintenance\test-mission-verification-ui.cjs
node C:\spaceos-mvp2b2-execution-lab\tests\contract_tests.js C:\spaceos-mvp2b2-execution-lab\space-bridge\static\world.html
node C:\spaceos\docs\maintenance\run-mission-verification.cjs
node C:\spaceos\docs\maintenance\check-mission-verification-http.cjs
```

- Core **28/28 PASS**, Bridge **41/41 PASS**; full logs in verification-fix.
- Actual extracted frontend handler unit contracts **11 PASS** (DOM/HTTP doubles, not browser E2E): immutable target, modal exclusion, blank-before-HTTP rejection, one in-flight submission, exact URL/attempt, same-ID uncertain retry, cancellation, stale rendered binding rejection, FAIL preservation, no native prompt.
- Existing World contracts **103/103 PASS**, separate log.
- Real runtime harness **19 checks PASS**, including old-defect reproduction, corrected negative tests, immutable A/attempts, single B event and actual restart equality.
- Post-browser authenticated HTTP/SQLite checks **9 PASS**.

## Evidence and screenshots

- `SPACEOS_MISSION_VERIFICATION_RUNTIME.json`: final real IPC/HTTP responses, raw SQLite snapshots, prior artifact hashes, original DB preservation and owned-process cleanup.
- `SPACEOS_MISSION_VERIFICATION_HTTP.json`: actual UI command payload recovered from the committed event, exact retry/new duplicate/A-B isolation responses.
- `SPACEOS_MISSION_VERIFICATION_UI.json`: actual DOM target and outcome evidence before/after restart, console observations.
- `SPACEOS_MISSION_VERIFICATION_FRONTEND.log`, `SPACEOS_MISSION_VERIFICATION_WORLD_CONTRACTS.log`.
- `SPACEOS_MISSION_VERIFICATION_EMPTY.png`, `BOUND.png`, `SHELL.png`, `WORLD_B.png`, `RESTART_SHELL.png`, `RESTART_WORLD.png` (all share the SPACEOS_MISSION_VERIFICATION_ prefix).

The first navigation loaded cached old HTML; direct HTTP confirmed the corrected source while DOM still showed the old handler. An explicit version query URL loaded the verified corrected asset. This was recorded rather than treating old cached UI as a corrected-build result. Browser console evidence retains the old native-prompt failure. Corrected World console observations were empty; no universal zero-error claim is made.

## Limits and final status

**Verification isolation/duplicate/stale-attempt correction: PASS in the laboratory. Historical unexpected Mission A confirmation attribution: UNKNOWN.** Original investigation remains partially unresolved on that narrow forensic question.

No guarantee of exactly-once user intent across independent devices is claimed: command replay is idempotent, identical duplicate verification is rejected, lifecycle/attempt CAS is authoritative, and the current UI prevents accidental in-flight duplication. Deliberately changed verification evidence is not globally forbidden by this focused change.

Existing unrelated limitations (bounded execution-result truncation/fallback wording and legacy command-event execution_status metadata) were not refactored here. Optional Providers were absent; no Provider operation or full model Mission was claimed in this UI-only correction stage.

Both owned laboratory processes were stopped; test HTTP listener 21973 was released. OpenCode and unrelated processes were not terminated. Approximately 11.20 GiB free disk remained at the final check. No deployment is authorized or performed.
