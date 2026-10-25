# Bounded evidence writer isolation — final report

Date: 2026-10-25. Repository: `C:\spaceos`.
**Verdict: PASS for the bounded active-writer isolation gate.** This is not a new runtime, Mission, Provider or production integration certification. Historical one-time utilities remain executable and require separate review before use.

## 1. Baseline and preservation

Read `SPACEOS_SOURCE_ARTIFACT_ISOLATION_FINAL.md` and `SPACEOS_SQLITE_V5_ACTION_COMPLETION_FINAL.md`; inspected actual active writer paths, their consumers and historical maintenance utilities.

Before editing/testing, at **2026-10-25T04:57:12.235Z**, recorded SHA-256 for **801 existing regular files** recursively under `docs/maintenance`, including reports, JSON, logs, images, source archives and historical utilities. The new report is excluded from the comparison.

Aggregate SHA-256 of JSON-encoded sorted `[path, SHA256]` pairs:

```text
9b6e319ca084d2178da184a551e2857aa1a4b95042a33e188b2aa4c08c3fa2b5
```

**All 801 hashes matched after the writer tests and after creating this report.** No historical output, image, report or maintenance utility was changed, deleted, moved or promoted to a new result. Git index SHA-256 also remained:

```text
acc7b09c01c5d22f21ed88ece3df47023dd3515d581a7baf40473f8c5cea5e3c
```

Three previously overwritten v5-stage historical summaries retain their CURRENT bytes:

| Path under docs/maintenance | Current preserved SHA-256 |
|---|---|
| `durable-file-provenance-ipc.json` | `3fbdd7dace3a3b907bafca57975258941beba6f7e9d144b16001b54a2f1c5813` |
| `file-changed-watcher-ipc.json` | `66d2397361af02a94641a25b94aaa177a51c4c6e8a11397f05c34cf4cc45b1c6` |
| `provider-file-relations-ipc.json` | `f005c952478f275ea6b5c55e324601b94e57ac87d7a58b0a5f14be2b4f5aba2f` |

These are not the lost original pre-v5 bytes/hashes. This task does not reconstruct them or repair the earlier history.

The repository was already WIP. No tracked `opencode.json` contents or production credentials were inspected. No production process, endpoint or database was contacted.

## 2. Bounded implementation

A small test-only filesystem helper, `tests/evidence_paths.cjs`, centralizes canonical run containment, exclusive writes and checkpoint reading. It has no service, database, credential authority, publication/retention framework or new output hierarchy.

- `runDirectory`: require an existing directory strictly beneath canonical repository `test-tmp`; reject the root, outside/sibling directories and missing paths.
- `selectedRun`: require explicit `--run <path>` or `SPACEOS_EVIDENCE_RUN`. No “latest run”, maintenance-directory or historical fallback.
- `artifactPath`: basename-only output, no traversal/nested destinations.
- `writeEvidence`: `wx` exclusive file creation; existing file/symlink is not overwritten. JSON objects record canonical `run` and absolute `evidence_artifact`; binary bytes are unchanged.
- `snapshotWriter`: preserve every repeated save as an immutable numbered checkpoint, such as `action-v5-runtime.000001.json`. Refuse a collision rather than skipping it. Sequence exhaustion fails closed. Each checkpoint records its actual path and current run artifact paths.
- `readEvidence`: read the newest numbered checkpoint for the requested basename, or a single-shot file **within the explicitly selected run only**. Reject an escaping file symlink or mismatched embedded run. This is not a fallback to historical evidence.

Original runner save timing, assertion predicates, authorization calls, test data, model choices, IPC/network/runtime sequencing and production contracts were not redesigned. Fresh runtime JSON basenames gain sequence suffixes because overwriting mutable intermediate reports would lose evidence. One-shot audit, probe and screenshot filenames remain unchanged inside their run.

Existing mutable private fixture configuration, test documents and STOP/RESTART controls remain working state, not immutable evidence; their existing behavior is preserved inside the ignored unique run. Some older auxiliary run-local logs remain append-only streams. No private working state was moved to documentation.

## 3. Active writer inventory and before/after destinations

All “before” maintenance paths below remain historical references only, never new active output destinations.

| Files / functions | Before | After |
|---|---|---|
| `tests/full_vertical_slice.cjs::save` | maintenance `full-slice-smoke-evidence.json` repeatedly replaced | Existing `test-tmp/full-slice-*/full-slice-smoke-evidence.NNNNNN.json` |
| `tests/full_vertical_slice_ii.cjs::save` | maintenance `full-slice-ii-smoke-evidence.json` | Existing `full-slice-ii-*` run, numbered checkpoints |
| `tests/consolidated_mvp_smoke.cjs::save` | maintenance `consolidated-mvp-smoke-evidence.json` | Existing consolidated run, numbered checkpoints |
| `tests/vertical_slice_ii_corrections_runtime.cjs::save` | maintenance corrections/scoped/recovery/v5 runtime JSON, selected by mode | Existing `vertical-corrections-*` run; same mode basename + numbered immutable checkpoints |
| The four runtime drivers' `start` / Chrome log opens | Per-run append-mode .out/.err logs | Same run/log names, exclusive initial creation; restart labels remain distinct |
| `tests/scoped_browser_actions.cjs` screenshot writer | maintenance scoped/recovery/v5 screenshot JPG | `c.run/SCOPED_BROWSER_PROVIDER_SCREENSHOT.jpg`, recovery or v5 counterpart; exclusive |
| `tests/action_v5_runtime.cjs` | No independent evidence file writer; calls supplied `save/check` | Unchanged module; inherited save now writes isolated checkpoints. Synthetic SQLite fault/control operations unchanged |
| `tests/check_full_vertical_slice.cjs` | Reads shared smoke JSON, overwrites `full-slice-audit.json` | Explicit selected run input; exclusive same-named audit there |
| `tests/check_full_vertical_slice_ii.cjs` | Shared smoke/audit JSON | Explicit selected run; `full-slice-ii-audit.json` exclusive |
| `tests/check_full_vertical_slice_ii_http.cjs` | Shared smoke/`full-slice-ii-http.json` | Explicit selected run; exclusive HTTP evidence |
| `tests/check_consolidated_mvp.cjs` | Shared smoke/before-verification/final-check JSON | Explicit selected run; exclusive same-named check JSON |
| `tests/vertical_slice_ii_corrections_audit.cjs` | Shared runtime/UI JSON and `vertical-slice-ii-corrections-audit.json` | Explicit selected run for current runtime/UI inputs and exclusive output |
| `tests/scoped_browser_audit.cjs` | Shared runtime/UI/artifact enumeration/inventory | Explicit selected run for current evidence and artifact enumeration; exclusive inventory |
| `tests/scoped_browser_recovery_audit.cjs` | Shared recovery runtime/UI/artifact/inventory paths | Explicit selected run; exclusive recovery inventory; previous approved inventory remains a read-only historical reference |
| `tests/action_v5_audit.cjs` | Shared v5 runtime/artifacts/inventory | Explicit selected run; exclusive inventory; approved source baselines remain read-only historical references |
| `tests/authorization-positive.cjs::save/snapshot/result` | Fixed maintenance pointer plus overwritable run-local JSON | Pointer/result/failure/snapshots exclusive inside its existing run; IPC/timeline saves numbered checkpoints |
| `tests/attempt_binding.cjs::save` | Overwritable run-local `evidence.json` | Numbered immutable run checkpoints; mutable private fixture configuration unchanged |
| `scripts/browser-observation-probe.mjs` | maintenance `browser-runtime-identity-probe.json` | Existing `browser-runtime-probe-<UUID>` run, exclusive same filename; stdout gives run/artifact |
| `scripts/browser-observation-real-test.mjs` | maintenance runtime log opened with truncating `w` | Existing `browser-verified-real-<UUID>` run; log `wx`, private fixture JSON `wx`; stdout lists log/Rust evidence paths |
| `browser-provider/tests/verified_observation.rs::write_evidence`, `core`, `provider` | Fixed maintenance evidence JSON; UUID logs created truncating | Evidence JSON inside configured synthetic run via `create_new(true)`; UUID logs also exclusive |

The Browser Rust test's existing assertions, including its existing schema-version assertion, were NOT revised to make this writer change pass a full runtime test. The complete Browser test was not executed here.

The three already corrected Rust writers in `space-core/tests/durable_file_provenance.rs` and `filesystem-provider/tests/verified_file_changed.rs` remain unchanged: each uses its UUID-specific run directory, not maintenance destinations. Their run-local JSON writes are not newly certified as exclusive same-run collision guards in this task.

### Exact changed/new source files

18 existing test/driver files modified; 2 new test-only files. No Core/Bridge/Provider production implementation, Policy, Mission, Gateway, schema, grants, frontend, Cargo manifest/lockfile, ignore rule or credential configuration changed in this task.

```text
9db603bd788650cc7370f468c6de82ddb38962339d955cb1582a88bccad1df4f tests/full_vertical_slice.cjs
ce1e3c2d8ddad14174e4c9c12dc8a7e0da211e3e67faf27269948d3af945332b tests/full_vertical_slice_ii.cjs
e4a4e65f59b36934f0d8dbdcb0b410c8503c02684c523c4ba99ff12da4bb1bd3 tests/vertical_slice_ii_corrections_runtime.cjs
a95f733aade48febf20f1bb1b668fc345168d10411e630cef8f7d2bc090495d3 tests/consolidated_mvp_smoke.cjs
acec1926b7124feee972da8e0f78879e7dea2bc3917de95ab4d6991cb587e991 tests/scoped_browser_actions.cjs
a01a854eca8bdea8778208658bfe14cfbaecc17b92769ed767570526f0e351af tests/check_full_vertical_slice.cjs
5973fac3fab02010db918e19ec44945be960e0a03f41d429700daa230732e2fa tests/check_full_vertical_slice_ii.cjs
32a18737659f35ccf2a0ce3c9365ed4cb331998224bca6fc19a3a2091f0c0821 tests/check_full_vertical_slice_ii_http.cjs
adae3929dade49c843909c0946838f6d6dc4a185cad6db237494d6d174b03d3e tests/check_consolidated_mvp.cjs
0b8976810bb79e2b84a3b531a154d4d17d9e61e32bbc3e9abb48cc1c1dd6f749 tests/action_v5_audit.cjs
c7f9c926950fc51b1e8ddbc6f8e84e0f5962906ed458aa80747ce09584f775bf tests/scoped_browser_audit.cjs
1dd992c60311be3839be1acef6e37db6d910b0d9111942f61035940466549638 tests/scoped_browser_recovery_audit.cjs
36957313dc653373cceae5457d12c3fac505381de22f530af23ed736f9c6dbea tests/vertical_slice_ii_corrections_audit.cjs
415ea6bdb9c23d5c8b7c73ac130fdfb4aebeac04e482d443c383501a976d9f86 tests/authorization-positive.cjs
feae0d48f5e48f9f4d25f7491db4dbdeb91aedd436dd3cb7fe31b72ae7c6aa4e tests/attempt_binding.cjs
40a321d8f72775102f5942624c2d3838937ff13516c638dc4b301e4140dd5fc2 scripts/browser-observation-probe.mjs
bee986e2c577ffdde72eca4f704b17a550cb689c881b266c21f002bd64406cec scripts/browser-observation-real-test.mjs
0540d3c297e412a005c36e21572e8122cd085edc716b3157816b6882fee576ff browser-provider/tests/verified_observation.rs
6d04285c02f092d3734ce92b432c89aed87c9b16d159c2bb4c7bd9e438e9ceb6 tests/evidence_paths.cjs
c62bab22487bb6c1d34caa27fce3e39906bb733371902db671f24ba739504512 tests/evidence_writer_isolation.cjs
```

New files are `tests/evidence_paths.cjs` and `tests/evidence_writer_isolation.cjs`; both must be included in any future owner-reviewed test-source package. The prior inclusion manifest remains a historical snapshot, not a claim that these new files already existed.

## 4. Current versus historical inputs

Current runtime summaries, screenshots, UI capture inputs and audit products must now be supplied within the same explicit run. Do not copy private fixtures to maintenance or quietly load an unrelated old run.

Approved comparison baselines are intentionally still read-only:

- v5 audit: `docs/maintenance/scoped-browser-recovery-inventory.json` and `full-slice-ii-inventory.json`;
- recovery audit: prior `scoped-browser-inventory.json` and its historical artifact hashes.

Changing current output directories does not change the expected historical source hashes, event counts or authorization assertions. An old baseline may independently fail against a newer candidate; this task does not weaken those assertions or pretend that an old audit is a new execution.

Example future invocation (NOT executed here):

```powershell
node tests/action_v5_audit.cjs --run C:\spaceos\test-tmp\<the-new-v5-run>
node tests/check_full_vertical_slice_ii.cjs --run C:\spaceos\test-tmp\<the-new-slice-II-run>
# Equivalent explicit input:
$env:SPACEOS_EVIDENCE_RUN = 'C:\spaceos\test-tmp\<the-new-run>'
```

A repeated audit of the same run refuses to replace its existing final inventory/audit file. Do not delete old evidence to make it pass. Use a genuinely new execution run; any need for multiple named audits of one run requires a separate explicit output-name decision.

## 5. Historical maintenance utilities intentionally unchanged

Inspection found **25 top-level `docs/maintenance/*.cjs` utilities**. No active test/script imports or invokes them in the inspected regression workflows. They are source-analysis, one-time consolidation/build, earlier laboratory execution/validation, migration-of-source or report-generation utilities, not the maintained runtime writer path corrected above.

```text
docs/maintenance/analyze.cjs
docs/maintenance/build-complete-local-mission.cjs
docs/maintenance/build-first-local-bridge.cjs
docs/maintenance/build-mission-verification.cjs
docs/maintenance/check-complete-mission-goal.cjs
docs/maintenance/check-mission-verification-http.cjs
docs/maintenance/consolidation-patch.cjs
docs/maintenance/finalize-complete-mission.cjs
docs/maintenance/finalize-consolidation-evidence.cjs
docs/maintenance/finalize-full-slice-ii.cjs
docs/maintenance/finalize-full-slice.cjs
docs/maintenance/finalize-vertical-corrections.cjs
docs/maintenance/finish-reports.cjs
docs/maintenance/inventory.cjs
docs/maintenance/plan-data.cjs
docs/maintenance/prepare-consolidation.cjs
docs/maintenance/run-complete-local-mission.cjs
docs/maintenance/run-first-local-mission.cjs
docs/maintenance/run-mission-verification.cjs
docs/maintenance/test-completion-commands.cjs
docs/maintenance/test-mission-verification-ui.cjs
docs/maintenance/validate-local-mcp.cjs
docs/maintenance/validate-local-model.cjs
docs/maintenance/validate-local-opencode.cjs
docs/maintenance/verify-first-local-mission.cjs
```

**Do not execute these without separate review.** In particular, `build-*`, `run-*`, earlier local-model/MCP validators and completion/verification utilities retain sibling-laboratory paths; consolidation preparers can copy sources, and finalizers can overwrite fixed inventories/reports. They were neither refactored nor run. No active utility from this directory was required for the current regression writer path.

These are the remaining known fixed-destination executable writers outside the approved active set. Their continued existence means the repository is not universally safe against executing arbitrary historical scripts. There is no silent active-runner fallback to them.

## 6. Executed verification

No Core, Chrome, OpenCode, Qwen, network runtime, production endpoint or full integration fixture was launched.

### Node writer regression

```powershell
node tests/evidence_writer_isolation.cjs
```

Final result: **25/25 PASS**. Actual retained final run pair:

```text
C:\spaceos\test-tmp\evidence-writer-A-jhdabc
C:\spaceos\test-tmp\evidence-writer-B-hx0DVM
```

Machine-readable result: `evidence-writer-A-jhdabc/writer-checks.json`. Additional extracted-runner and screenshot test outputs occupy fresh sibling run directories created using the existing runners' own mkdtemp prefixes; no new laboratory or directory hierarchy.

Executed coverage:

- two synthetic runs: distinct JSON/image paths and correct embedded run;
- JSON and binary `EEXIST` collisions leave previous bytes unchanged;
- outside/root/traversal output rejection;
- immutable checkpoints, latest checkpoint reading and first-checkpoint collision rejection;
- mismatched embedded run rejected;
- **actual extracted runner initialization/save code**, not substitute save fixtures: Slice I, Slice II, consolidated MVP and all four corrections/scoped/recovery/v5 modes;
- actual screenshot writer expression: per-run placement and collision;
- actual probe summary writer expression;
- actual async Browser harness log-open expression: exclusive creation and preserved log bytes;
- Browser Rust source contains exclusive receipt output and no maintenance destination;
- all eight current audit/check entrypoints launched **without any run**, each fails before runtime/database work with the explicit-run diagnostic;
- a newly written synthetic private fixture is excluded by existing `/test-tmp/` Gitignore.

Initial test development failure was a malformed JSON collision-test replacement string: helper rejected it during JSON parsing before any write. The synthetic test input was corrected to valid replacement JSON. Existing bytes were not overwritten. Intermediate 22- and 24-check successful runs remain on disk; final 25-check run above is the designated result, not 71 independent tests.

### Standalone Rust writer verification

Extracted the exact `write_evidence` function from the changed Browser integration test into a tiny std-only harness under the designated ignored run. No Cargo workspace/component rebuild or target tree was created.

```powershell
rustc --test test-tmp/evidence-writer-A-jhdabc/rust-writer-check.rs -o test-tmp/evidence-writer-A-jhdabc/rust-writer-check.exe
.\test-tmp\evidence-writer-A-jhdabc\rust-writer-check.exe --nocapture
```

Result: **1/1 PASS**. Actual exclusive creation of two distinct files, repeat write returns `AlreadyExists`, original bytes remain. This verifies the extracted writer primitive, NOT compilation/execution of the full Browser integration test.

### Syntax, source and artifact checks

- `node --check <file>` for 19 changed/new JS/CJS/MJS inputs: **19/19 PASS**.
- Existing assertion/check call-site counts in the modified drivers/checks match their before-edit counts (373 sites across 15 files). This is a preservation diagnostic, not 373 executed tests.
- Compared the previous exact 201-file active Rust/build/static manifest with current bytes: **200/201 unchanged**; the sole change is the authorized `browser-provider/tests/verified_observation.rs` evidence writer. Production implementations, manifests, lockfile and frontend assets match that baseline.
- No active maintenance-output expressions remain in the inspected scripts except explicit read-only baseline references. The v5 transport module has no standalone artifact writer to relocate.
- Final historical-file hash aggregation: **801/801 unchanged**, aggregate above matches before/after.
- Git index hash unchanged; `git diff --cached --name-only` empty.
- Existing test-state ignore protects newly generated private fixtures; no staging occurred.
- Free disk after tests: **26,335,518,720 bytes**. No deletion, pruning, other laboratory access, target duplication or expensive rebuild.

### Coverage limits

Full runtime, full Browser Rust test, genuine CDP screenshot, authenticated IPC, WS, Qwen and Mission integration were not rerun. Writer expressions, filesystem effects and path selection were exercised without those systems. Existing v5/vertical-slice functional results remain historical results, not fresh re-certification.

## 7. Remaining limits

- Historical maintenance utilities still have fixed outputs and sometimes sibling paths; separate review required before execution.
- Older UUID-local durable-provenance/FileChanged writers and auxiliary append-only logs remain local to their runs; universal exclusive-create behavior for every test file in the repository is NOT claimed.
- Private configuration/control/test-document updates remain mutable inside their new synthetic run; do not distribute raw run directories as public evidence.
- Checkpoints preserve full snapshots and therefore use more disk. Monitor available space before long tests; no automatic evidence pruning or data deletion was added.
- Numbered checkpoints are read after a completed synchronous write in normal flow. No crash-consistent artifact transaction or concurrent live-reader protocol was added; audits should use a stopped/completed run. Failed disk writes may leave a partial file and never get silently replaced.
- Existing caller-authentication, production provisioning and other product limitations were not changed. No production readiness or deployment claim.

## Final gate

| Area | Result |
|---|---|
| Maintained active writer destinations isolated | PASS |
| Two-run separation / exclusive creation / collision preservation | PASS |
| Existing maintenance evidence and images | PRESERVED — 801/801 current hashes |
| Current active audit input selection | PASS — explicit run, no historical fallback |
| Historical utility safety | RESTRICTED — unchanged, separate review required |
| Full runtime/agent/Provider regression | NOT RUN; no new claim |
| Overall bounded task | PASS |
| Production / credentials | UNTOUCHED |
| Staging / commit / push / deployment | NOT PERFORMED |
