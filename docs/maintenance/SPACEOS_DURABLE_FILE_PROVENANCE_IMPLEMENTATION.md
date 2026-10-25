# Durable filesystem File admission provenance — implementation

Date: 2026-10-24. Repository: `C:\spaceos`.

**Result: PASS for the bounded receipt implementation and the executed isolated tests. Deployment NOT performed.** Relation authorization and FileChanged remain deliberately unimplemented.

## Scope and safety

Implemented owner-approved design B from `SPACEOS_DURABLE_PROVIDER_PROVENANCE_DESIGN.md`. The inspected source baseline was schema version 3; the new source version is 4. No running/production database was opened, inspected, migrated or used by these tests. Database fixtures and files are synthetic, under `C:\spaceos\test-tmp`, or SQLite `:memory:`. No additional laboratory or Rust target tree was created. Builds used the existing target with incremental compilation disabled, offline/locked dependencies and two jobs.

The repository already contained substantial owner changes. This task changed only the three Core source files listed below and added two test files, this report and its maintenance evidence. Before-images of those three files are preserved in `docs/maintenance/durable-file-provenance-before/`; they are the task baseline, not clean Git HEAD. No Policy, capability profile, Browser Provider, Mission, Bridge or frontend source was changed by this task. No relation capability was granted. No production credentials were read or generated. No commit, push or deployment occurred.

## Exact implementation changes

| File | Functions / definitions changed | Purpose |
|---|---|---|
| `space-core/src/storage.rs` | `SCHEMA_VERSION`, `migrate`, `FILE_ADMISSION_TABLE`, `FILE_ADMISSION_TRIGGER`, `verify_file_admission_schema` | Add transactional v3→v4 migration and fail-closed schema validation. |
| Same | `apply_observation`, new private `apply_observation_inner`, crate-private `apply_admitted_file_created` | Preserve the generic path without authority; mint a receipt only from the opaque verified context in the same transaction as object/event/log. |
| Same | `TransactionStorage::{insert_semantic_object, update_semantic_properties, create_object, delete_object_cascade}`, `Storage::{create_object, delete_object}` | Verify receipt enforcement exists before object mutation; delegated `update_object` inherits the guard. SQL trigger handles every UPDATE, not just IPC. |
| Same | crate-private `has_file_admission` | Check protected receipt against canonical object, observation log and event; no public permission/query endpoint, not wired to relation authorization. |
| `space-core/src/provider_observation.rs` | new opaque `VerifiedFilesystemAdmission`; `ObservationBindings::verify_file_created` | Reuse existing `admit_file_created` instance/source/World checks, retain normalized observation and authenticated instance/root/binding fingerprint for storage. No Deserialize or public trusted flag. |
| `space-core/src/ipc.rs` | `handle_method`, `report_observation` branch | Obtain verified context, retain resolved-World principal capability check, use admitted storage path; publish only after successful transaction and only when `applied=true`. |
| `space-core/src/storage_file_admission_tests.rs` | 11 new tests | Storage/schema/adversarial/rollback/migration tests. |
| `space-core/tests/durable_file_provenance.rs` | `real_ipc_admission_restart_and_negative_cases`, guarded `admission_core_child` | Real private named-pipe Core process, SQLite inspection, subscription notification, restart, rotation and legacy mutation evidence. Child entry point is not counted as a separate behavioral test. |

### Schema and invariant

`filesystem_file_admissions` is private Core metadata, not an Object, provider JSON property or capability. It stores:

- `object_id`: primary key, NOT NULL, FK to objects with ON DELETE CASCADE.
- `observation_id`: NOT NULL UNIQUE FK to observation_log.
- `event_id`: NOT NULL UNIQUE FK to events.
- `admitted_type='File'`, `provider_id='filesystem'`: checked constants.
- Nonempty `world_id`; external identity and object creation timestamp snapshot.
- Nonempty authenticated `instance_id`, canonical `source_root`, 64-character `source_binding_digest`.

The digest is the existing registration fingerprint of the instance's authorized binding set, not a credential and not a per-root digest. The particular root and World are stored separately. No token, secret or token-derived authentication verifier is stored in the receipt.

The trigger is `AFTER UPDATE ON objects`, with no column/WHEN restriction, and deletes receipts for both OLD.id and NEW.id. Therefore same-value UPDATE, generic UPSERT conflict, type/identity/property changes and future id-changing SQL invalidate. It is conservative even for trusted generic writers. DELETE cascades in the same SQLite statement; INSERT OR REPLACE cannot inherit the deleted row's receipt. Ordinary relation writes do not touch receipts.

Migration checks `PRAGMA user_version`, runs table creation, trigger creation and version 4 stamping in one SQLite transaction, verifies enforcement, then commits. No historical data is backfilled. An unexpected preexisting receipt table causes migration failure and rollback rather than silent reuse. Existing v4 databases with missing/broken enforcement are not silently repaired.

Before admitted writes, receipt predicates and Core object mutation helpers, verification requires foreign keys ON, exact version 4, and the exact expected table/trigger definitions from sqlite_master. Names alone are insufficient; a replacement no-op trigger fails. An abnormal schema also rejects generic object writes, preventing a write while the trigger is absent from retaining stale trust. This is an intentional fail-closed behavior for damaged/incompatible schemas, not a new authorization grant. Reads need not certify receipt trust and may remain available. Arbitrary malicious DBA/DDL access remains outside the Core trust boundary.

### Admission and transaction ordering

1. Existing IPC authentication establishes the filesystem principal; caller JSON is not identity proof.
2. Existing admission verifies registered instance credential, provider registration, supported FileCreated shape, actual resource type/path, unique authorized source, exact/derived World and identity collision constraints.
3. New opaque context retains the normalized observation and source facts. IPC still checks `observation.report` in the resolved World.
4. Under the existing immediate write transaction: verify schema; check replay; require a new canonical object for a fresh admission; INSERT object; INSERT event; INSERT observation log; INSERT File receipt; commit.
5. IPC publishes the event after commit only. Any error rolls back projections/event/log/receipt together.

Directory remains a supported FileCreated observation but never receives a File receipt. Generic `apply_observation`, legacy create/update and semantic object creation cannot mint receipts. Public IPC payloads and provider code are unchanged; there is no externally supplied trusted flag.

Replay must agree with stored event ID, World, object identity and path; an existing receipt additionally constrains admitted type/external identity/World. Replay returns `applied=false`, writes nothing, publishes nothing and never repairs a missing receipt. Neither deletion/recreation nor a generic same-id replacement can revive an old receipt. A genuinely new verified admission after deletion requires a fresh observation/event and current authorization; this is not replay recertification.

Receipt validity is historical, not a live Provider health check. Restart and credential rotation do not erase it, and it does not reassign Worlds when source configuration changes. Observations after restart still require current authenticated registration. There is no automatic revalidation. The receipt does not certify current file bytes, inode identity, continued path existence or absence of later out-of-band disk modification. Existing path/type admission checks are retained, not strengthened into a filesystem snapshot guarantee.

## Executed tests

Toolchain: `rustc 1.99.0 (b940084d7 2026-09-28)`, `cargo 1.99.0 (5f94df478 2026-08-27)`. Windows PowerShell, working directory `C:\spaceos`.

Common test environment (synthetic only):

```powershell
$env:CARGO_TARGET_DIR='C:\spaceos\target'
$env:CARGO_INCREMENTAL='0'
$env:SPACEOS_TEST_ISOLATION_ROOT='C:\spaceos'
$env:SPACEOS_CORE_TOKEN='synthetic-file-admission-tests-only'
```

For the combined library/phase_b/semantic_api command, TEMP and TMP were set to a newly created UUID directory under `C:\spaceos\test-tmp`. This contains the older tests' fixed-name cleanup within fresh synthetic fixtures, not existing external temporary data.

| Executed command | Result | Evidence under `docs/maintenance/` |
|---|---|---|
| `cargo check --offline --locked -p space-core -j2` | PASS | `durable-file-provenance-check.log` |
| `cargo test --offline --locked -p space-core --lib file_admission_tests -j2 -- --test-threads=1` | 11/11 PASS, initial run | `durable-file-provenance-unit.log` |
| `cargo test --offline --locked -p space-core --lib -j2 -- --test-threads=1` | 39/39 PASS, initial full library run | `durable-file-provenance-core-regressions.log` |
| `cargo test --offline --locked -p space-core --lib --test phase_b --test semantic_api -j2 -- --test-threads=1` | 39 library + 12 phase_b + 7 semantic API PASS | `durable-file-provenance-final-regressions.log` |
| `cargo test --offline --locked -p filesystem-provider --test i5_reconciliation -j2 -- --nocapture --test-threads=1` | 1/1 PASS | `durable-file-provenance-provider-regressions.log` |
| `cargo test --offline --locked -p space-core --test durable_file_provenance real_ipc_admission_restart_and_negative_cases -j2 -- --nocapture --test-threads=1` | 1/1 PASS, repeated after final functional changes | `durable-file-provenance-ipc.log`, `durable-file-provenance-final-ipc.log` |

**Final distinct coverage: 60 tests PASS, 0 failures** (39+12+7+1+1), not the sum of repeated runs. The final 39 library tests include the enhanced missing-table and conflicting-type replay checks, existing instance/World security, dispatch credentials and 10 Mission persistence/lifecycle/attempt/verification tests. Existing warnings remain: two async public trait warnings in Core, one unused provider variable in Filesystem. No unrelated warning fixes were applied.

### New regression coverage

- Verified File receipt versus Directory without receipt; generic forged trusted marker and Directory→File relabelling never mint.
- Public generic create/update, transactional UPSERT, generic observation update, raw same-value UPDATE invalidate.
- Transactional UPDATE/DELETE rollback preserves the old receipt; committed delete and replacement remove it; ID reuse and old replay do not revive it.
- Event collision and reused observation identity leave no partial projection/receipt. Injected receipt INSERT abort rolls back all four tables.
- Idempotent replay, conflicting File→Directory replay rejection, invalidation followed by replay without recertification, and rejection of fresh overwrite of an existing canonical object.
- Synthetic v3 migration, no historical backfill, reopen/version4 persistence and integrity check; incompatible preexisting table fails without advancing version.
- Missing table, missing trigger, no-op replacement trigger, broken receipt column, disabled FK enforcement and incompatible version fail closed, including legacy UPDATE attempts.
- Existing provider admission security tests retain instance credential checks, unique source/World binding and adversarial rejection. Filesystem scanner repeat reconciliation preserves identity and never treats authorization denial as absence.

## Real IPC and SQLite evidence

Final run: `C:\spaceos\test-tmp\durable-file-ipc-2cc475b8-d66c-4a07-95e4-b1802545394c`.

Private endpoint: `\\.\pipe\spaceos-4-3a4-admission-2cc475b8-d66c-4a07-95e4-b1802545394c`. No production pipe or HTTP endpoint was contacted. The child hosts actual Core library IPC and Storage, not a mock handler. It runs from the compiled test executable, not the deployed release executable. The provider caller is synthetic; this is not a real watcher daemon or full product E2E test.

Actual FileCreated response:

```json
{"ok":true,"result":{"applied":true,"event_id":"37775409-d9ce-44a7-85a6-16c8d7a702a8","seq":1}}
```

File object: `1c26396d-5e92-4dce-bb96-f96523e46811`; observation: `5aebaf32-5e9d-4a50-8f3d-fe2b53497996`; World `receipt-A`; authenticated instance `receipt-instance-A`. A read-only SQLite query confirmed the matching receipt/event/observation keys before restart. The authenticated Core subscription received the same committed FileCreated ID, sequence 1, actor/provider `filesystem`, World `receipt-A` and the same object ID.

Actual denial categories included:

```json
{"ok":false,"error":"denied: Provider instance is unknown, unauthenticated or unregistered"}
{"ok":false,"error":"denied: filesystem observation World conflicts with trusted binding"}
{"ok":false,"error":"denied: filesystem observation path mismatch"}
```

Wrong instance secret, unknown instance, transport credential substituted for instance credential, foreign World and forged external path were rejected without object/event count growth or FileCreated notification. Directory succeeded without a new File receipt. Legacy fake File and Directory relabel were stored under existing rules without minting trust.

The parent terminated and waited for only its owned child process, then started another child against the same synthetic DB with a rotated instance credential. Existing receipt/event counts survived. Observation before re-registration failed; old credential failed; new registration plus valid replay returned `applied=false`, with no duplicate event. Legacy same-data UPDATE then removed the receipt; authenticated replay still did not recertify it. Final `PRAGMA integrity_check` returned `ok`. Both owned children were stopped.

Sanitized structured evidence: `docs/maintenance/durable-file-provenance-ipc.json`. Child logs: `core-before.log` and `core-after.log` in the run directory. They contain no production credentials; instance credentials are not included in the structured report.

## Hash inventory

SHA-256 at completion (source hashes are task output, not a clean repository/release inventory):

| Artifact | SHA-256 |
|---|---|
| `space-core/src/storage.rs` | `1DDC1005CC8644D19C36427966D329DECB258CD2454969B6FD619B64ED2D2D8C` |
| `space-core/src/ipc.rs` | `6D653797F3D5E49017BCC392EBEA748C1F6F91CDC02F3378F3998F225023388B` |
| `space-core/src/provider_observation.rs` | `86EC3FB1F1F1402ADDB0603F46B7C8C5C3BC59FED6E1DFE27C48E0E5D2C7A1AE` |
| `space-core/src/storage_file_admission_tests.rs` | `6B1B6281F0AC50E959A9FBF07BBC835F0F74475B91BE2F154C4ADADC1E142430` |
| `space-core/tests/durable_file_provenance.rs` | `4A189D5B1A594F9488357CCA6EBE6362AA789120B4DA9953F3F2AE52DAD37CDE` |
| `target/debug/deps/durable_file_provenance-a5612038560396b6.exe` | `E089E97FB2096AF2449A3A59265D7671A906979DDE6D9164E57E2A11AB6B2AF2` |
| `target/debug/deps/space_core-6946e66b3fd18dea.exe` | `BC083EF8AA24064EA597E37782A7691E15D47F1436C93BE494B10B8F0DC3ECF1` |
| `docs/maintenance/durable-file-provenance-ipc.json` | `34357E3AB5AF06CB62A9194EF317672546AA5CEC2D09FAA2CFCFAFBEF345AD9D` |

The exact existing-file before-images have SHA-256: storage `526AFCC76832152EF668137A7757A59D02244546592EFD1AC40972F2007FE652`; IPC `23D73FDEE1D8182051617FEE9721A517EB9979443E3DFBEE42A6630990D45B8D`; provider observation `C78F1F3EC8B4BA3C551EE980247B82AE84815B8C03817BC094FBC9245A2DFA74`.

## Limitations and operational consequences

- No relation endpoint authorization or FileChanged behavior was added. No permanent agent authority changed. This receipt is only the necessary provenance prerequisite for a separately approved future contract.
- No historical File obtains trust automatically; a missing or invalidated receipt remains absent. Same-value updates intentionally lose certification.
- Direct hostile database/DDL writers are outside the trust boundary. Exact DDL verification rejects even an externally rewritten semantically equivalent schema; manual repair/revalidation must be separately controlled, not improvised by the application.
- No full workspace, real watcher, Bridge WebSocket/browser or production integration suite was run. Core commit→IPC subscription delivery was verified; existing Bridge post-commit forwarding was not changed or claimed tested here.
- Tests demonstrate synthetic migration and restart, not production migration readiness. Do not run old binaries against the new database as an assumed compatible rollback: deployment needs coordinated binary rollback and a verified pre-migration cold backup. This task did not execute that procedure.
- Existing live processes remained present at final check: release Core PID11956, release Browser PID26108 and sibling-lab Filesystem PID14808, with their original executable paths. No process belonging to the new durable admission IPC harness remained at final check. No unrelated processes were stopped.
- Free disk space was monitored: initially 28,973,699,072 bytes; final observed 28,892,057,600 bytes. No target directory was copied or deleted, and no other laboratory's files were removed.

**Production database: UNTOUCHED. Deployment: NOT AUTHORIZED / NOT PERFORMED.**
