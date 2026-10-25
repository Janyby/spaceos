# Provider-owned File relations — final implementation report

Date: 2026-10-24. Repository: `C:\spaceos`.

**PASS for the bounded Core contract and executed synthetic Core/IPC tests. No deployment.**

References: `SPACEOS_MVP_GAP_CONTRACT_ANALYSIS.md`, the earlier blocked `SPACEOS_PROVIDER_FILE_RELATION_IMPLEMENTATION.md`, and `SPACEOS_DURABLE_FILE_PROVENANCE_IMPLEMENTATION.md`. The previous provenance blocker is addressed by the existing v4 receipt, not by trusting provider labels or reconstructing admission from an unprotected event JOIN.

## Implemented contract

`related_to` now supports either two existing semantic-owned endpoints (unchanged authorization contract) or exactly one semantic-owned endpoint and one verified filesystem File, in either orientation. Both endpoints must belong to the command's exact World. Unsupported relation types, two File endpoints, unapproved provider types, forged/legacy Files and missing/invalidated receipts are denied.

Receipt verification reuses the existing `Storage::has_file_admission` SQL predicate, extracted into one shared private connection-taking helper. The relation path passes the connection of its existing immediate write transaction to that helper. It does not open another connection or validate the receipt before the transaction. Object resolution, ownership checks, File admission checks, relation INSERT and success-event INSERT occur in that same transaction. No new trust boundary or schema was required.

For a File reference, IPC additionally requires authenticated `semantic.read` for the command's exact World using the existing `principal_authorized` path. This is separate from, and in addition to, the existing `semantic.relation.create`, delegated-agent eligibility, configured semantic World and membership checks. The storage receipt never serves as caller authentication or a capability grant. The extra read check runs while Core's storage mutex is held; storage then independently resolves and validates the endpoints inside its transaction.

Semantic-to-semantic relations retain their previous capability requirement; a registered delegated synthetic agent with relation authority but no read grant still succeeds for semantic-only endpoints. No global read-all, reader credential reuse, public trusted flag, system bypass or permanent profile expansion was introduced.

The File row is only read. Its identity, provider, external ID, ownership, properties, created_at and updated_at are not written. No disk action is issued. RelationCreated retains the actual caller as actor and the exact World; relation and success event commit atomically. Existing post-commit `publish_committed_event` remains unchanged.

## Exact changed files and functions

| File | Changes |
|---|---|
| `space-core/src/storage.rs` | `Storage::create_semantic_relation_as`: classify transactional semantic ownership versus verified filesystem File; require same World; reject more than one File endpoint. `Storage::has_file_admission` now delegates to shared private `has_file_admission_on(&Connection, id)`, which contains the unchanged v4 schema/receipt/object/log/event predicate. |
| `space-core/src/ipc.rs` | `handle_method`, `create_semantic_relation` branch: provider endpoint references require the same authenticated actor's `semantic.read` in the requested World. Existing mutation gate and post-commit publication retained. |
| `space-core/src/storage_file_admission_tests.rs` | Added `file_relation_receipt_validation_is_transactional_and_fail_closed`: event-INSERT fault rolls back relation, receipt remains, and missing invalidation trigger denies relation creation. |
| `space-core/tests/durable_file_provenance.rs` | Added `real_ipc_file_relations_authority_atomicity_and_restart` plus small request/registration/command helpers. Extended only the synthetic child Core registrar's explicit test grants/Worlds to register test agents. Existing production profiles are untouched. |

Added this report and `provider-file-relations-*.log` / `provider-file-relations-ipc.json` maintenance evidence. No Policy, capability profile, Mission, Provider, frontend, credential or schema source changes were made. `SCHEMA_VERSION` remains 4. The preexisting dirty worktree is preserved; the table describes this task's changes, not all differences against Git HEAD.

## Executed verification

Windows PowerShell, existing target only, offline locked dependencies, no incremental directory:

```powershell
$env:CARGO_TARGET_DIR='C:\spaceos\target'
$env:CARGO_INCREMENTAL='0'
$env:SPACEOS_TEST_ISOLATION_ROOT='C:\spaceos'
$env:SPACEOS_CORE_TOKEN='synthetic-file-relation-tests-only'
```

For existing fixed-name storage/semantic fixtures, TEMP and TMP were set to a newly created `C:\spaceos\test-tmp\relation-final-<UUID>` directory. No existing external fixture tree or database was reused/deleted. All new runtime files, source roots and SQLite state are synthetic UUID paths under the existing repository's `test-tmp`. No new laboratory or target tree was created.

Toolchain: rustc `1.99.0 (b940084d7 2026-09-28)`; cargo `1.99.0 (5f94df478 2026-08-27)`.

Commands actually executed:

```powershell
cargo test --offline --locked -p space-core --test durable_file_provenance real_ipc_file_relations_authority_atomicity_and_restart -j2 -- --nocapture --test-threads=1
cargo test --offline --locked -p space-core --lib --test phase_b --test semantic_api -j2 -- --test-threads=1
cargo test --offline --locked -p space-core --lib --test phase_b --test semantic_api --test durable_file_provenance -j2 -- --test-threads=1
```

The new IPC command ran twice before the final combined run. Final combined results:

| Suite | Result |
|---|---|
| Core library | 40 PASS: existing security/provider/World/dispatch/subscription/Mission tests, 11 durable provenance tests and 1 new transaction test |
| phase_b | 12 PASS |
| semantic_api | 7 PASS |
| durable_file_provenance | 2 behavioral IPC tests PASS; Cargo also counts 1 guarded child entry point which is a no-op in the parent |

**61 distinct behavioral tests passed; final Cargo total 62 includes the child-process entry point. No final failures.** Do not count repeated runs as additional tests.

### Observed regression and correction

The first combined run failed `semantic_relation_create` at `space-core/tests/semantic_api.rs:536`: the cross-World semantic endpoint still failed closed, but the reordered property World check returned a different error instead of the established `configured semantic world` ownership error. Restored ownership-before-property validation. The original assertion was not weakened or edited; the final seven semantic API tests passed.

Failure preserved in `docs/maintenance/provider-file-relations-regressions.log`. Final results are in `provider-file-relations-final-regressions.log`; earlier positive IPC logs are `provider-file-relations-ipc.log` and `provider-file-relations-final-ipc.log`. Existing two async public trait warnings remain; no unrelated warning cleanup was performed.

## Real Core process / IPC / SQLite evidence

Final new relation run:

- Directory: `C:\spaceos\test-tmp\file-relations-e0edc332-24f6-4410-83ce-45a0cc92822b`.
- Private pipe: `\\.\pipe\spaceos-4-3a4-admission-e0edc332-24f6-4410-83ce-45a0cc92822b`.
- SQLite: `synthetic.db` inside that directory, never a production path.
- Core: actual library IPC/Storage hosted by a separate owned test executable process; not a mocked handler and not the running release Core.
- Agent: newly registered `relation-agent`, synthetic credential, explicit `semantic.read`, `semantic.relation.create`, `semantic.object.patch`, World `receipt-A` only.
- Provider instance: synthetic filesystem instance A, registered/authenticated against synthetic source A. A second real synthetic admission uses instance/source B and World B for cross-World negatives.

Task `cf710529-e1c6-446a-9297-b06bae8faff1` and receipt-bearing File `f6cb86a9-95ab-4461-a915-021ceaf77967` produced:

| Direction | Relation ID | RelationCreated event ID | Sequence |
|---|---|---|---|
| Task → File | `d625b96f-387c-41a2-b23a-c7b3ecf7f0dc` | `664e3e5f-be4f-4491-91bd-3412edf69faa` | 4 |
| File → Task | `737394d9-baa6-4301-9588-be2b4c1bc39b` | `2b77a6ec-9fe2-46fa-8005-058530e30147` | 5 |

Both IPC responses returned `ok:true`, `related_to`, actor `relation-agent`, World `receipt-A`. An authenticated Core subscription received the same success-event IDs; read-only SQLite queries confirmed persisted relations when notifications were consumed. Event actor is the agent, not `space-world-user` or filesystem. Provider_id on these semantic events remains null, as in the existing semantic relation contract.

Sanitized denial responses include:

```json
{"ok":false,"error":"denied: provider reference requires semantic.read in the requested World"}
{"ok":false,"error":"denied: relation-no-create lacks semantic.relation.create or missing auth"}
{"ok":false,"error":"denied: relation-agent lacks semantic.relation.create or missing auth"}
{"ok":false,"error":"File to File semantic relations are not allowed"}
{"ok":false,"error":"target object has no verified filesystem File admission"}
{"ok":false,"error":"object is provider-owned and read-only"}
{"ok":false,"error":"correlation_id conflict: success event already exists"}
```

Negative tests cover missing read in both directions, missing relation capability, invalid agent token, foreign agent World, foreign requested World, an authentically admitted World-B File, fabricated trusted=true File, Directory before relabel, Directory→File legacy relabel, unsupported contains, missing endpoint and File→File. Authorization denials may retain existing MutationDenied audit records; they create no relation or RelationCreated success event. Transactional endpoint/event failures add neither a relation nor an event, and emit no success subscription notification.

A same-correlation event collision rolls back the newly inserted relation. A separate storage test injects a real SQLite event INSERT abort, demonstrating rollback without relying only on the collision case. Missing trigger fails closed inside that relation transaction. The original receipt remains unchanged throughout successful relation creation and failed event mutations.

Full serialized File object equality and file byte equality were asserted before versus after relation operations. An agent with an explicit semantic PATCH grant was still denied File PATCH. Thus reference permission did not become mutation permission.

The parent stopped and waited for its owned Core child, started a new child against the same synthetic database, and re-registered the agent through authenticated IPC (agent registration is intentionally ephemeral). `query_relations` returned both original IDs after restart. The File's complete stored representation remained unchanged. A subsequent authorized legacy UPDATE invalidated the File receipt; a new relation request was then denied and relation count stayed unchanged. SQLite integrity returned `ok`. Both owned Core children were stopped.

Structured responses/notifications/restart query: `docs/maintenance/provider-file-relations-ipc.json`. Owned Core logs: `relations-before.log`, `relations-after.log` in the run directory. No instance/agent tokens are included in the structured evidence.

## Hashes at final verification

SHA-256:

| File | Hash |
|---|---|
| `space-core/src/storage.rs` | `D86E6A4B92CB7911DBCD8FF60C00A96371796B2C7B2C329161514749A1E2ECEF` |
| `space-core/src/ipc.rs` | `7A1442F957107FA2475C910BF6326E116D3780285E0BDE4069C0052BFEFCE773` |
| `space-core/src/storage_file_admission_tests.rs` | `2A29FE8AD29D0C5C256CF15F9940E149E6ECC8F8113EAFFD474557118BA97233` |
| `space-core/tests/durable_file_provenance.rs` | `9E2D360C0098D7B903A98BA5AA1812125BC70AC708460731DFA4DEC4EA8CA2DA` |
| `target/debug/deps/durable_file_provenance-a5612038560396b6.exe` | `C46D1C64A6B70C3E4A4D4DFC8A092DA07D6CBAE56EBCC637146924DE814DF06F` |
| `docs/maintenance/provider-file-relations-ipc.json` | `05C4EDF64D7DF9D516771E9D33F21C9120F4222574D3085B7E0FF8E8BB8A0DF8` |

## Limitations and preserved boundaries

- This verifies the Core contract, real authenticated IPC, actual SQLite and Core subscription delivery. No Bridge WebSocket, browser UI, real Mission/OpenCode agent or real filesystem watcher daemon E2E was run. Existing publication code is preserved, not claimed newly verified end-to-end through World.
- Receipts remain historical admission facts, not proof of current file bytes/existence or live provider availability. Credential rotation/source reconfiguration does not automatically recertify or reassign Files.
- Invalidating a receipt prevents new File references; this patch does not delete preexisting relations or introduce ongoing relation reauthorization. No broader relation lifecycle contract was requested.
- Trusted in-process storage callers remain responsible for Policy checks, as before. Caller authorization is enforced by authenticated IPC; the new storage predicate is not an authorization substitute or public trusted-input switch.
- space-world-user and permanent Mission-agent grants are unchanged. A production Mission agent without explicit relation capability remains unable to create these relations. Any permanent grant still requires a separate owner decision.
- No schema migration beyond the existing v4, no FileChanged, no new laboratory, no target duplication, no production DB access, no commit/push/deployment.
- Unrelated processes remained present with their existing paths: Core PID11956 in target/release, Browser PID26108 in target/release, Filesystem PID14808 in the sibling I.5 lab. None was contacted or stopped. No durable_file_provenance child remained at final process check.
- Disk monitored: initial free 28,897,468,416 bytes; final observed 28,885,434,368 bytes. No other laboratory's files were removed.

**Production state: UNTOUCHED. Deployment: NOT PERFORMED.**
