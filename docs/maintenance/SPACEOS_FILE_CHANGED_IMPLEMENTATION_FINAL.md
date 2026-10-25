# Verified FileChanged with exact CAS — final implementation

Date: 2026-10-24. Repository: `C:\spaceos`.

**PASS for the bounded FileChanged contract and executed isolated verification. No production database access or deployment.**

Implements the owner's approved option A from `SPACEOS_FILE_CHANGED_IMPLEMENTATION.md`. The earlier document remains the historical pre-approval gate, not the current implementation status. No new schema, laboratory, general revision framework, Policy capability or permanent agent grant was introduced. Schema remains v4.

## 1. Exact changes

| File | Functions / change |
|---|---|
| `space-core/src/provider_observation.rs` | Creation wrapper retains `admit_file_created`; private `admit_filesystem` shares existing authentication/path/World checks for exactly FileCreated and FileChanged. New `verify_file_changed`, small private `verified_context` helper, and `validate_file_change`. The existing opaque non-deserializable verified context carries instance/root/binding authority, not a public trusted flag. |
| `space-core/src/storage.rs` | New `next_file_version` and `apply_verified_file_changed`. Existing creation-only persistence now explicitly rejects a non-FileCreated proof. Existing generic observation storage, schema/table/trigger and relation authorization remain otherwise unchanged. |
| `space-core/src/ipc.rs` | `report_observation` selects the specific verified FileChanged path; retains principal authentication, resolved-World observation.report gate, trace handling and post-commit publication. Other observation types still reach the creation validator and are rejected. |
| `filesystem-provider/src/observation.rs` | `apply_observation` now returns `Result<ObservationOutcome>`; preserves credential/transport/admission errors, deserializes the actual outcome, rejects malformed empty identity/sequence responses and distinguishes valid replay. |
| `filesystem-provider/src/watcher.rs` | `report` propagates helper errors. Existing FileChanged handler already reads Core before stat and preserves that read's updated_at; it was retained. No retry with relabelled stale metadata was added. |
| `filesystem-provider/src/scanner.rs` | `report` propagates errors rather than counting rejected observations as success; documentation corrected. No conversion of unsupported FileUpdated/rename/move/delete contracts into FileChanged. |
| `space-core/src/storage_file_admission_tests.rs` | Five new tests for exact CAS, duplicate/clock behavior, transaction failures/negative shapes, receipt instance takeover, timestamp precision/overflow and SQLite reopen persistence. Includes relation creation after a verified change. |
| `filesystem-provider/tests/verified_file_changed.rs` | New real notify watcher child + separate actual Core IPC child process test, synthetic credentials/roots/SQLite, restart, adversarial IPC, rollback and subscription evidence. |
| `filesystem-provider/tests/i5_reconciliation.rs` | Updated the fixture's old assumption that denied scanner writes succeed. It now asserts rejection propagation, admits a real synthetic child through authenticated observation transport, and retains scoped read/identity/denial/not-absence assertions. Explicitly tests unsupported FileUpdated rejection. |

Only these nine Rust files and task evidence/report artifacts changed in this task. Existing unrelated dirty-worktree changes were preserved. No Browser, Mission, Gateway, MCP, World UI, Policy or capability profile source was changed. No commit, push or deployment.

## 2. Admission / mutable-field contract

FileChanged requires:

- Authenticated filesystem principal, its existing observation.report authority in the Core-resolved World, and an authenticated registered instance credential distinct from principal/transport credentials.
- Exactly one existing filesystem File, no relations or deletion batch. Directory and unsupported lifecycle event shapes are rejected.
- Consistent object/event IDs, provider/actor filesystem, external identity, properties.path and payload.path; actual canonical filesystem path must still resolve to a File under one unique authorized source.
- Same canonical ID, File type, provider, external identity, created_at and World as the current row. Every property except meta must match exactly.
- A valid protected v4 FileCreated admission receipt. In the write transaction its instance ID, canonical source root, World and full binding-set digest must match the verified current authority. A different registered instance cannot inherit another File's receipt merely by presenting a matching path.

Allowed meta is the exact existing eight-field stat shape: path, file_type, size, extension, mime, created, modified, accessed. Path must match canonical external identity, file_type must be File, size must be u64, mime a nonempty string of at most 255 bytes, extension null or a string of at most 1024 bytes, timestamps null or valid RFC3339 strings of at most 64 bytes. Extra fields, negative/non-integer size, error snapshots and unapproved property edits are denied. This validates shape/identity; authenticated Provider metadata is still an observation, not an attestation of file bytes.

Core constructs the result from its stored row and copies only validated meta. Format/fingerprint/semantic detection properties remain unchanged and may still describe creation-time content. No content reclassification, identity reassignment, filesystem write, rename, move or delete operation was added.

## 3. Exact CAS and timestamp representation

The incoming Object.updated_at is the exact expected base read from Core before metadata capture, never a proposed new value. Inside the existing immediate SQLite write transaction:

1. Verify v4 table/trigger/FK enforcement, existing File, receipt, readonly properties and exact source authority.
2. Recognize a valid observation replay before CAS. Require the same event/resource/World identity; return applied=false, seq0, without writes. Authentication and valid current provenance remain required; replay cannot recreate absent trust.
3. For a new observation, compare incoming and current DateTime values at full nanosecond precision. A mismatch returns `conflict: FileChanged expected updated_at differs from current version` without changing object/receipt/event/log.
4. Compute `max(Core UTC now, previous + 1 nanosecond)` with checked Chrono arithmetic. Verify strict increase and exact RFC3339 serialize/parse round-trip.

No conversion to signed i64 epoch-nanoseconds is used (which would impose a much narrower date range). Ordinary RFC3339 years 0000–9999 are required; leap-second encodings, overflow beyond the representable approved range and invalid precision fail closed. The next value cannot repeat within a canonical admitted object's accepted update chain. SQLite stores its full string without truncation and the Object reader restores the full DateTime.

Clock rollback or a future initial Provider timestamp can produce a logical Core timestamp ahead of physical now, as approved. This is a monotonic revision value, not proof of capture time. Provider observed_at/event.timestamp/OS modified times remain audit/metadata only; none decides CAS ordering. Two reports from one base cannot both commit, regardless of their wall-clock values. This is optimistic concurrency, not a total chronological order of physical file writes; an observation losing the race needs a genuinely new read and stat, not a new base pasted onto old metadata.

Timestamp exhaustion fails closed. No timestamp normalization/backfill or new receipt is synthesized to work around an invalid existing object.

## 4. Receipt and transaction invariant

The transaction reads all eleven protected receipt TEXT columns into a private local snapshot only after validating the receipt. It updates properties/updated_at on the existing row. The unchanged generic UPDATE trigger invalidates the old receipt. The same verified transaction restores the exact original receipt columns, preserving the original FileCreated event/observation and instance/source/World admission facts. It then inserts FileChanged event and observation log and verifies receipt validity before commit.

Any failure, including restore/event/log failure or collision, rolls back the metadata update, trigger deletion and restoration together. An absent/invalidated receipt is rejected before UPDATE and never restored. Generic/legacy updates still invalidate receipts, including same-value updates. Generic apply_observation cannot mint or restore trusted admission.

IPC publishes FileChanged only after the storage call has committed with applied=true. Replay, conflict, validation denial and transaction failure do not publish a success notification. No success event/log exists for a rejected new update.

## 5. Real watcher versus synthetic IPC evidence

Latest run directory:
`C:\spaceos\test-tmp\verified-changed-7a24f097-3cdd-4a77-9c71-84bf59a93b64`.

Private Core endpoint:
`\\.\pipe\spaceos-4-3a4-changed-364fc909-8ea8-4ff4-9f54-26424cee4ad1`.

A separate child process runs the actual Core library IPC/storage; a second child process runs the actual filesystem-provider notify watcher and authenticated provider helper. Both are hosted by the compiled test executable, not deployed executables/services. Synthetic credentials and roots A/B are injected server-side; no production credential/configuration/pipe/HTTP/CDP endpoint is used. The Provider child registers its type and individual instance using real Core IPC. Its actual source-bound reads occur before metadata stat.

### Genuine watcher coverage

The parent writes a new synthetic watched.txt, then physically modifies it from 7 to 30 bytes. Real OS notify callbacks create/admit and update it. No scripted observation substitutes for those two phases. SQLite wait predicates verify actual metadata size, stable object identity, exact original receipt and advancing Core updated_at. Core subscription frames are consumed through a sequence newer than the pre-modification watermark, not merely an unrelated earlier FileChanged.

- Canonical File: `f75e2304-f8de-49bc-80f2-581289ea3103`, World A.
- Stable created_at: `2026-10-24T17:09:33.243465800Z`.
- Snapshot after creation settled: updated_at `2026-10-24T17:09:33.263375100Z`.
- Snapshot after genuine modification: updated_at `2026-10-24T17:09:33.308701700Z`.
- Original FileCreated event: `90c2e00b-64d0-4a64-9951-51277a9fda16`, seq1.
- Pre-modification watermark: seq3. Observed post-watermark FileChanged: `02c29b33-a04e-4391-970a-dfff666371a2`, seq4, actor/provider filesystem, World A. Each consumed notification was checked against a persisted SQLite event.

Notify can legitimately produce multiple Modify callbacks. The latest run retained one object, one FileCreated and seven FileChanged events across watcher, explicit IPC and restart phases. It does not claim one event per physical write. Idempotency is defined by delivery of the same observation identity, not deduplication of distinct OS notifications.

### Explicit synthetic IPC coverage (separate from notify)

The watcher is stopped cleanly with its oneshot stop before controlled races. Two metadata snapshots are captured from one Core base. The newer snapshot is sent first using the real authenticated Provider helper with provider event time deliberately set to 1990. It succeeds; the stale snapshot is denied. Actual outcome:

```json
{"applied":true,"event_id":"6857865c-e021-4be4-be11-6da2f9fc81a1","seq":6}
```

The duplicate delivery returns:

```json
{"applied":false,"event_id":"6857865c-e021-4be4-be11-6da2f9fc81a1","seq":0}
```

Both the replay and stale conflict leave event/log counts unchanged. The successful explicit event is also received on the authenticated Core subscription. Pending genuine watcher frames are drained first, then no-frame assertions verify that replay/negative requests and injected transaction failure publish no success frame.

Real helper/IPC negative cases include wrong instance credential, principal credential substituted as instance credential (locally rejected before transport), unknown instance, bad principal proof, conflicting World, other source root, changed created_at, unapproved properties, forged object ID, and a physically existing but unadmitted File. Denials preserve one canonical object, receipt, observation-log count and event count in this harness. Sanitized examples:

```text
provider Core request denied: conflict: FileChanged expected updated_at differs from current version
provider Core request denied: denied: filesystem observation World conflicts with trusted binding
provider Core request denied: denied: FileChanged identity mismatch
provider Core request denied: denied: FileChanged modifies unapproved properties
provider Core request denied: denied: FileChanged requires existing File
```

A SQLite BEFORE INSERT event trigger injects a real FileChanged failure after metadata/receipt SQL has run. Full object serialization, original receipt, event/log counts and subscription silence prove rollback, not a mocked storage return.

### Controlled restart

The watcher exits through its stop channel. Only the owned child Core is terminated/waited, then a new owned Core starts on the same synthetic DB. Original receipt is verified unchanged. A new Provider watcher process performs authenticated re-registration, physically modifies the file to 20 bytes, and the same File identity/created_at/World persists with updated_at `2026-10-24T17:09:33.818771500Z`. A stale pre-restart report is still denied. Both Provider children exit cleanly; both owned Core children are stopped. Final integrity_check is ok.

This is a coordinated restart test, not transparent reconnect/resume while a watcher remains running across a dead Core. No such recovery was implemented or claimed.

Structured evidence: `docs/maintenance/file-changed-watcher-ipc.json`; child logs in the latest run directory: `core-before.log`, `provider-before.log`, `core-after.log`, `provider-after.log`. No credentials are present in the structured evidence. This is real Core subscription delivery, **not Bridge WebSocket or World browser verification**; neither Bridge nor UI was started/tested in this task.

## 6. Executed commands and results

Windows PowerShell; existing toolchain rustc `1.99.0 (b940084d7 2026-09-28)`, cargo `1.99.0 (5f94df478 2026-08-27)`. Common environment:

```powershell
$env:CARGO_TARGET_DIR='C:\spaceos\target'
$env:CARGO_INCREMENTAL='0'
$env:SPACEOS_TEST_ISOLATION_ROOT='C:\spaceos'
$env:SPACEOS_CORE_TOKEN='synthetic-file-changed-tests-only'
```

For Core phase_b/semantic fixtures, TEMP/TMP were pointed at a fresh UUID directory under `C:\spaceos\test-tmp`, containing their fixed-name cleanup within synthetic state.

| Executed command | Final result / evidence in docs/maintenance |
|---|---|
| `cargo check --offline --locked -p space-core -p filesystem-provider -j2` | PASS; file-changed-check.log |
| `cargo test --offline --locked -p space-core --lib file_admission_tests -j2 -- --test-threads=1` | Initial 16 PASS; file-changed-unit.log (before adding the fifth new test) |
| `cargo test --offline --locked -p space-core --lib --test phase_b --test semantic_api --test durable_file_provenance -j2 -- --test-threads=1` | 45 unit + 12 phase_b + 7 semantic + 2 behavioral IPC PASS; file-changed-core-regressions.log |
| `cargo test --offline --locked -p filesystem-provider --lib --test verified_file_changed --test i5_reconciliation --test roots_gate -j2 -- --nocapture --test-threads=1` | 8 unit + 1 reconciliation + 1 roots_gate + 1 behavioral watcher test PASS; file-changed-filesystem-regressions.log |
| `cargo test --offline --locked -p space-core --lib -j2 -- --test-threads=1` | Final 45 PASS including relation-after-change assertion; file-changed-final-core-unit.log |
| `cargo test --offline --locked -p filesystem-provider --test verified_file_changed real_watcher_cas_authenticated_restart_and_atomicity -j2 -- --nocapture --test-threads=1` | Latest enhanced subscription/negative test PASS; file-changed-final-watcher-v3.log |

**77 distinct behavioral tests passed.** Raw combined-suite Cargo counts include two additional guarded child-process entry-point tests (one per executable) which do nothing in the parent; those are excluded from 77. Repeated runs are not additional coverage.

Core coverage includes existing Model A/principal/Policy/dispatch/subscription, provider instance/World isolation, 17 storage admission/relation/change tests, and ten Mission persistence/lifecycle/attempt tests. Filesystem coverage includes actual signed Provider dispatch/root denials; roots_gate retained its original assertions on per-run private endpoints. No full workspace build/test or production E2E was attempted.

New tests additionally prove: Directory/type denial; malformed/extra metadata; receipt source takeover denial; event collision rollback; generic update invalidation blocks further FileChanged; relation creation still accepts a valid File after changed metadata; leap-second and maximum-year fail-closed behavior; +1 ns exact serialization in year2300; SQLite close/reopen restores the nanosecond version and rejects an old base. The latter uses no i64 timestamp_nanos conversion and proves checked logical time survives restart.

### Failures observed and preserved

- `file-changed-watcher-ipc.log`: first new test compilation failed with E0597 due to a test helper returning a statement-borrowing temporary. Fixed by storing the collected result before statement destruction; production code compiled successfully already.
- `file-changed-watcher-ipc-run2.log`: the new harness incorrectly expected a MutationDenied audit row for invalid principal proof. Actual IPC rejected it before dispatch with zero event growth. Corrected the test to require unchanged event count; no Core behavior was weakened or changed for this assertion.
- `file-changed-reconciliation-initial.log`: old reconciliation test unwrapped a scanner success that previously hid Core's root-object admission denial. New Result propagation exposed it. The updated test explicitly asserts that denial, then establishes a valid child through real authenticated admission for its read tests. It also requires unsupported FileUpdated to remain denied. `file-changed-reconciliation-final.log` and final combined Filesystem log pass.

Earlier positive watcher reruns are retained in file-changed-watcher-ipc-run3.log, file-changed-final-watcher-ipc.log and file-changed-final-watcher-v2.log. Final structured evidence corresponds to final-watcher-v3, not those earlier UUID runs. Existing Core async-trait warnings, a watcher unused-variable warning and test-only roots_gate dead-code warnings were not refactored.

## 7. Compatibility and remaining limitations

- Generic and legacy UPDATE still invalidate File admission, even for same-value writes. FileChanged cannot recertify historical/unreceipted or invalidated rows. No historical backfill.
- Existing FileCreated wire shape/Provider behavior remains supported; creation/restart/rotation/relation tests pass. No Directory updates or new File lifecycle authority was added.
- **Scanner compatibility is deliberately visible, not silently repaired:** scanning its exact configured binding root can report the existing root-object admission denial; FileUpdated/move/delete remain unsupported by the verified Core path. Such scans now return errors instead of previously reporting false success. This task does not claim general scanner reconciliation completion. A separate owner-approved correction is needed for those lifecycle contracts.
- The watcher loop logs a rejected observation and continues processing future events. It does not automatically rebase or retry the rejected snapshot. A future notify event performs a new Core read and stat.
- CAS protects Core projection versions, not an atomic disk snapshot or malicious-Provider attestation. No guarantee that OS metadata cannot change immediately after stat. Authenticated source authority and all canonical identity boundaries remain required.
- Multiple notify events may yield multiple valid FileChanged events, even with equal metadata. Duplicate observation delivery remains a no-op. No arbitrary debounce or ordering by Provider clocks was added.
- No real Bridge WebSocket, World/Shell UI, Agent/MCP Mission or production Provider service was exercised. Only evidence actually executed is marked PASS.
- No production DB was opened/migrated. Existing v4 migration code was not changed; synthetic migration regressions were rerun only. No production process was restarted/replaced.

## 8. Final source / evidence SHA-256

| Artifact | SHA-256 |
|---|---|
| space-core/src/storage.rs | A8F1E15819EB13BE33D44B9AAC59FBD7078F7422E29D7D28F2ABFA1489E288A5 |
| space-core/src/ipc.rs | 4E565C8650AB51D32E749B89284413BE7CC41F42C8BE55FBBE76EFB29A7B6BBE |
| space-core/src/provider_observation.rs | 9137C7724E61E54A806EDB7D7B771EF290D85D182E39C718C67C94E6002F6579 |
| space-core/src/storage_file_admission_tests.rs | A1EAC596077391981417E9B1DAD552DBCCB286D7DD9E82891033FD2D3E653CBD |
| filesystem-provider/src/watcher.rs | 1A5C4A9437B5E6B93B5DEB630331C720EEC02610DBB5FC74A531A06D7F02689F |
| filesystem-provider/src/observation.rs | 5A8263A1CFB9831090C0D15D752BA083FD2771F24A4F72F1DE8C60B25700484C |
| filesystem-provider/src/scanner.rs | 00EBABCF175E62DF59F99C06376E07047982B7C114F2AF520562CF38F9B5E422 |
| filesystem-provider/tests/verified_file_changed.rs | EE33BCE8A03D5FE68DCF2AD78E2B86DF00947CD5E5E25A0FE20CF0F16093782F |
| filesystem-provider/tests/i5_reconciliation.rs | 9ED7A045CFC618284BE218CC1958412BDC1C34AA0E76CCBB7D562D5D3417D433 |
| target/debug/deps/verified_file_changed-f6dbb7d8c7f604da.exe | 12C9187CAF8FC067287F96FDE757C43C09F626489602999F24699110626F572D |
| docs/maintenance/file-changed-watcher-ipc.json | 91DA97B5BDF20DE16477194167B4B6EC482CF6496581A3F415556EC446FD3DE4 |

Free disk monitored: initial 28,877,869,056 bytes; final observed 28,738,670,592 bytes. No target tree was copied or removed. No other laboratory files were removed. roots_gate removes only its own newly generated per-run synthetic fixture.

At final process check no verified_file_changed child remained. Unrelated existing release Core PID11956, release Browser PID26108 and sibling-lab Filesystem PID14808 remained present with their existing executable paths; none was contacted or stopped by this task.

**VERIFIED FILECHANGED: PASS. EXACT CAS / RECEIPT / REAL WATCHER / CONTROLLED RESTART: PASS. BRIDGE/WORLD: NOT TESTED. PRODUCTION: UNTOUCHED. DEPLOYMENT: NOT PERFORMED.**
