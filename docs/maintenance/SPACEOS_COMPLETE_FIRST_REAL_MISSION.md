# Complete first real local-model Mission

Date: 2026-10-24. Overall verdict: **PARTIAL**. The terminal-state defect is repaired and a genuine relationship-dependent local-model Mission completed and survived an actual Core/Bridge restart. The manual verification UI audit is not clean; see limitations below. This is laboratory evidence, not production qualification.

## Scope and root cause

Application changes were confined to `C:\spaceos-mvp2b2-execution-lab`. Reports and test helpers are in `C:\spaceos\docs\maintenance`. No new laboratory, OpenCode profile, database schema, executor service, model configuration, production deployment, commit or push was introduced.

The Gateway persisted terminal executor status through `BoundAttempt::report`, but did not reconcile that result with the authoritative Core Mission state machine. Therefore `execution_status=succeeded` could coexist indefinitely with `state=running`.

Core already distinguishes execution success from verified Mission completion. The correction uses its existing `mission_command` contract, not direct SQLite writes or observation injection:

- Succeeded + running -> begin_verification -> verifying.
- Succeeded + verifying + recorded verification PASS -> complete, subject to existing Core step/result checks.
- Failed/timeout -> fail while lifecycle is active.
- Interrupted -> pause while running/awaiting approval.
- Cancelled, paused and terminal Missions are not silently completed. Success never auto-approves an awaiting-approval Mission.
- Attempt/session/model/World bindings must match. Core additionally rejects passed verification or completion of an executor-backed Mission unless its unique, owned, same-World current attempt actually succeeded.
- Legacy manually executed Missions without attempts retain their previous completion contract.

## Exact changed application files/functions

All paths below are relative to the existing MVP2B2 laboratory.

| File | Focused change |
| --- | --- |
| `space-core/src/mission_storage.rs` | `require_successful_attempt`; checks in passed Verify and complete transition; three focused negative/positive test cases. |
| `space-bridge/src/agent_gateway.rs` | Terminal monitor invokes `reconcile_mission_outcome` after persistence and owned cleanup under the existing launch guard; `terminal_lifecycle_action`, deterministic existing Core command IDs, binding validation, diagnostics, `reconcile_persisted_missions`; four mapping tests. |
| `space-bridge/src/mission_commands.rs` | `dispatch` returns authoritative execution_status; `verify` reconciles a passed verification through the existing Core command under the launch guard. |
| `space-bridge/static/world.html` | `loadSide` isolates the optional filesystem roots failure rather than letting it reject all mandatory Core-backed sidebar loads and incorrectly label Bridge unavailable. |
| `src/runtime.rs` | Existing laboratory Bridge launcher performs persisted lifecycle reconciliation after orphan interruption at startup. |
| `src/authorization_fixture.rs` | Existing laboratory Core launcher validates/reuses prior seed IDs on restart instead of duplicating synthetic objects. |

`index.html`, MCP executable and model/provider settings were unchanged. The previously verified provider IPC endpoint-isolation fix in `space-bridge/src/ipc_client.rs` was retained, not introduced by this change. Four before-source snapshots are under `test-tmp/first-local-mission-AdE4I8/completion-fix`; the two small launcher changes do not have equivalent pre-edit snapshots. Final source hashes are recorded in `SPACEOS_COMPLETE_MISSION_ARTIFACTS.json`.

## One genuine execution, no retry/substitution

Normal installed OpenCode 2.1.3: `C:\Users\Gamer\AppData\Roaming\ai.opencode.desktop\cli\2.1.3-x64\opencode-cli.exe`. Existing configuration/authentication was preserved. Model explicitly selected in the real World-opened Mission UI: `unsloth/qwen-q8`, default variant, existing local endpoint `http://127.0.0.1:8888/v1`.

- Mission: `5d1aad05-4ecf-4baf-a3fc-64957fc8bb12`.
- Attempt: `1dafffca-48bc-4b6a-be5a-9de2bbcf131e`.
- New Mission-specific OpenCode session: `ses_e92c82333ffe52UkFLZszohtUt`.
- Authorized World: `first-local-A-9693eb7f-1a31-4943-8e76-879abd5cd16e`.
- Actual MCP actor: `relationship-mission-agent`, exact World, semantic.read/object.patch/relation.create only; no global read-all.
- Existing budget/deadline/cancellation controls retained; admitted calls 4/30, no automatic retry, one model execution only.

The submitted goal required reading existing relations before any mutation, following the Task -> Evidence relation, requiring connected Evidence PASS and target_unchanged=true, preserving Task status and updating the existing summary.

Actual model-originated MCP calls, all successful Core responses:

1. `spaceos_query_objects {}`.
2. `spaceos_query_relations {}`. Returned existing `related_to` edge `e91a97a7-680d-435e-91c0-e7d3d8c2683e`, Task `b42a5b7b-88e6-4bb7-8a76-e154b8f1c2f5` -> Evidence `9d682876-f060-42a4-9893-59537d42f65a`.
3. `spaceos_patch_object` on that Task: relationship_review=verified, reviewed_evidence_id=that Evidence ID, existing status=completed preserved.
4. `spaceos_patch_object` on Summary `2d627802-7371-48b1-8981-b3a4fac9e105`: `Validation passed via evidence ID 9d682876-f060-42a4-9893-59537d42f65a.`

The trace, not just the final prose, establishes relation reading before patching. The relation was not invented or rewritten. The earlier Mission had no relation-read invocation; this new execution does not retroactively change that history.

## Authoritative state and actual events

Executor outcome: succeeded. Core automatically moved the new Mission from running to verifying at `2026-10-24T09:22:38.969Z`, event seq 29. Independent read-only evidence checks then confirmed the connected Evidence acceptance fields, matching task update, persisted summary, two actor-scoped ObjectUpdated events, unchanged relation IDs and unchanged earlier attempt row.

Manual PASS was recorded at `09:23:52.318Z` (seq 30); existing Core complete transition committed at `09:23:52.441Z` (seq 31). Final Mission properties: state=completed, execution_status=succeeded, verification.passed=true, nonempty result.

The earlier Mission `5b9b6015-2569-4c6a-b444-8bd81b2b9426` also received a manual PASS at `09:23:55.748Z` (seq 32) and completion at `09:23:55.857Z` (seq 33). Its prior attempt was preserved byte-for-byte. Attribution of that second approval is unresolved; it must not be described as a deliberately submitted second verification.

Existing Core ownership/Policy checks and domain-event transactions were used throughout. No direct database mutation was used for authoritative Mission transitions.

## Actual restart and UI evidence

Actual isolated Core and Bridge process restart completed at `2026-10-24T09:27:34.605Z`, reusing the same SQLite database and synthetic World. The scoped snapshot before and after restart was identical: 7 objects, 2 relations, 32 visible events. Global SQLite includes the foreign-World guard/event: 8 objects, 2 relations, 33 events. No duplicate seeds, attempts or restart-generated semantic events appeared. SQLite integrity_check returned ok.

World and Shell were reopened/refreshed with the existing synthetic login flow. Both displayed the same authoritative completed Mission and succeeded executor after restart. The World connection label was `Core data received`; optional providers were absent, not falsely reported operational. Captured browser warning/error arrays were empty at the recorded observation, not a claim about all possible browser diagnostics.

Screenshots in this maintenance directory:

- `SPACEOS_COMPLETE_MISSION_01_SELECTED.png`: actual selected model.
- `SPACEOS_COMPLETE_MISSION_02_VERIFYING.png`: succeeded execution awaiting verification.
- `SPACEOS_COMPLETE_MISSION_03_COMPLETED_SHELL.png` and `04_COMPLETED_WORLD.png`.
- `SPACEOS_COMPLETE_MISSION_05_RESTARTED_WORLD.png` and `06_RESTARTED_SHELL.png`: persistence and representation parity.

These are actual browser captures through the identified localhost laboratory tabs, not fabricated UI state or mock screenshots.

## Tests actually executed

Working directory: existing MVP2B2 lab. The incremental build reused existing read-only native/dependency caches under `C:\spaceos\target\debug`; outputs were placed only in the existing run's `completion-fix` directory. No new Cargo target directory or unrelated workspace build.

```powershell
node C:\spaceos\docs\maintenance\build-complete-local-mission.cjs --completion-only
```

Exact rustc arguments and source/output hashes: `test-tmp/first-local-mission-AdE4I8/completion-fix/build.json`. This build intentionally changed Core; `coreSourceUnchanged` is false. An inherited initial console summary said true incorrectly and was corrected in the helper; the build manifest records false.

With SPACEOS_TEST_ISOLATION_ROOT set to the existing laboratory and synthetic test credentials/TEMP:

```powershell
& C:\spaceos-mvp2b2-execution-lab\test-tmp\first-local-mission-AdE4I8\completion-fix\core-tests.exe --test-threads=1
& C:\spaceos-mvp2b2-execution-lab\test-tmp\first-local-mission-AdE4I8\completion-fix\bridge-tests.exe --test-threads=1
node C:\spaceos\docs\maintenance\test-completion-commands.cjs
node C:\spaceos-mvp2b2-execution-lab\tests\contract_tests.js C:\spaceos-mvp2b2-execution-lab\space-bridge\static\world.html
node C:\spaceos\docs\maintenance\check-complete-mission-goal.cjs
node C:\spaceos\docs\maintenance\check-complete-mission-goal.cjs --completed
node C:\spaceos\docs\maintenance\finalize-complete-mission.cjs
```

- Core: **27/27 PASS** (`completion-fix/core-tests.log`).
- Bridge: **41/41 PASS** (`completion-fix/bridge-tests.log`).
- Real isolated IPC/Core command checks: **18 PASS**, including denial of passed verification/completion for registered, running, failed, timeout, interrupted and cancelled attempts; successful verified completion; invalid credential and foreign World rejection. Synthetic execution reports in this test are fixtures, not agent evidence.
- Existing frontend contracts: **103/103 PASS**, `SPACEOS_COMPLETE_MISSION_FRONTEND.log`; contract checks are not real browser/model assertions.
- Genuine Mission independent checks: **11 PASS** before verification and **11 PASS** completed/after restart.
- Final snapshot/cleanup assertions: **6 PASS**, including unchanged earlier attempt, restart equality for objects/relations/events and integrity.

The real runtime harness was `node C:\spaceos\docs\maintenance\run-complete-local-mission.cjs`. Restart/stop markers live in its existing completion-fix directory. It stopped its owned Core/Bridge and removed only its owned temporary OpenCode runtime resources; the normal OpenCode runtime was not terminated. Final stop timestamp: `2026-10-24T09:34:22.454Z`. No laboratory HTTP listener remained on 21973/21974 at the post-stop check.

## Binary and asset SHA-256

| Artifact | SHA-256 |
| --- | --- |
| existing-env-runtime.exe | AE38267555103360F503A9C6D8D1247B3A47C14AF4C448625FC5BEEBBEF1D8B6 |
| authorization-fixture.exe | 3B94B9186D39E9B3708D810C0866C8A83B11F9C779F527BB2E29B67BE0F0A4D7 |
| unchanged mvp2-mcp.exe | CFA85D61301AA722866EF274BE542626E2706135B69E30A105B48A6E7244E345 |
| world.html | 6A6C06C2D4091C20C2E709CCB766962BCEBAFE268CC08F90B15C306FFB73CCB0 |
| unchanged index.html | 729A24868DAD12879B0706A88DCA2FB2A8E9A04AD5F379EDFB8D44413D5E0D56 |

## Evidence references

- `SPACEOS_COMPLETE_LOCAL_MISSION_EVIDENCE.json`: actual Gateway/MCP responses, Core snapshots, events, restart, final SQLite rows and cleanup.
- `SPACEOS_COMPLETE_LOCAL_MISSION_IPC_TESTS.json`: actual synthetic negative/positive IPC responses.
- `SPACEOS_COMPLETE_MISSION_GOAL_CHECK.json`: independent evidence before manual verification.
- `SPACEOS_COMPLETE_MISSION_FINAL_CHECK.json`: completed-state checks, rerun after restart.
- `SPACEOS_COMPLETE_MISSION_UI_EVIDENCE.json`: actual DOM observations, console observations and explicit approval-dialog anomaly.
- `SPACEOS_COMPLETE_MISSION_ARTIFACTS.json`: final source/evidence hashes and restart equality assertions.

The `verification_note` in the independent checker JSON is explanatory evidence generated by the checker, NOT text entered into the browser or persisted as a Core verification note.

## Remaining limitations and failure evidence

1. **Manual verification UI audit PARTIAL.** One explicitly targeted Record pass click timed out in browser input; subsequent dialog inspection found no active prompt. No verification note was typed. Actual Core history shows empty note strings for the new Mission and also the earlier Mission about 3 seconds later. The cause/actor interaction producing the second approval is unproven. History was preserved, not rewritten. Independent evidence supports the new Mission's work, but this is not a clean one-click approval attribution test. The existing blank-note acceptance was not changed without a reproduced cause.
2. **Result serialization truncation.** Existing execution_result is capped/truncated at 28,000 characters and may no longer parse as JSON. The new completion path then stores an attempt-reference result rather than reconstructed assistant text. Its current fallback wording says full result retained in MissionAttempt, which is too strong when that field is truncated; the full raw Gateway artifact and concise semantic Summary are the reliable result evidence. No claim of lossless assistant-result persistence.
3. **Legacy event metadata.** Mission transition/verify events still contain the inherited hardcoded execution_status=not_launched field. Authoritative Mission properties and MissionExecutionChanged records are correct; the HTTP dispatch response was corrected. Consumers must not treat that legacy command-event field as the execution authority.
4. **Optional provider display.** loadSide no longer reports Bridge unavailable for absent filesystem roots. The temporary footer degradation label may be overwritten by a later renderFooter, leaving Roots —. Filesystem/Browser operations were not tested or claimed PASS here.
5. **Verification semantics.** Executor success is not automatic goal verification. Core still requires its existing verification and completed/skipped steps. Restart persistence of completed terminal state was demonstrated, not seamless recovery of an in-flight model request.
6. **Environment boundary.** No production application source, data or model/provider configuration was changed. Reports/test helpers under the explicitly requested existing maintenance directory are new. Existing user OpenCode was used as authorized; it may independently manage unrelated MCP subprocesses, so this report does not claim global OpenCode subprocess isolation. Only this harness's owned runtime resources were stopped.

## Final verdict

| Area | Result |
| --- | --- |
| Executor/lifecycle reconciliation | PASS |
| Genuine local-model relationship read before mutation | PASS |
| Scoped semantic mutations and persisted events | PASS |
| Mission authoritative completed state | PASS |
| Actual restart persistence | PASS |
| World/Shell authoritative state parity | PASS |
| Manual verification UI attribution | PARTIAL |
| Overall | PARTIAL |
| Production deployment | NOT AUTHORIZED / NOT PERFORMED |

Recommended next action: reproduce the native verification prompt/second-approval behavior in this same lab without further model requests, then make only a confirmed UI fix; separately correct bounded result serialization/fallback wording before claiming complete approval and result-history fidelity.
