# Post-Vertical-Slice-II consolidation review

Review date: 2026-10-25, Europe/Moscow. Repository: `C:\spaceos`.

**Conclusion: PARTIAL — the bounded integration corrections are supported by source and preserved execution evidence; this is not a production security approval.** No implementation code was changed in this review. Only this report was added. No services, production endpoints, databases or credentials were accessed; no build, model request, deployment, commit or push was performed.

## 1. Baseline, attribution and preservation

Current branch: `flaky-t1t2t3t4`. HEAD: `7990bd9f525bbe626ed5be07bf3c964270028ae4`, `fix(bridge-ui): repair world.html inline script and guard static UI assets`.

At inspection, the tracked unstaged diff contains **39 files, 7,793 insertions and 732 deletions**. The index has no staged diff. `git status --porcelain=v1` reports 96 entries, including collapsed untracked directories; this is not a count of every untracked file. There are no per-task commits separating the accumulated work. Therefore HEAD is a branch baseline, not the immediate pre-Slice-II source baseline, and line-by-line task attribution cannot be reconstructed reliably from Git alone.

Pre-existing WIP includes Core authentication/read scopes, Mission/attempt persistence, Gateway/MCP, Runtime read callers, filesystem authentication, OpenCode configuration and SpaceTrace hooks. In particular, dirty `opencode.json`, `runtime/src/*`, `space-bridge/src/mission.rs`, `agent_gateway.rs`, `mcp.rs` and `spacetrace/src/hooks.rs` must not be attributed to the Browser recovery task. The OpenCode configuration contents were not read because they are unnecessary to this review and may contain private configuration.

Important required implementation files are still **untracked**, including `space-core/src/browser_actions.rs`, `provider_observation.rs`, `service_registration.rs`, `semantic_read.rs`, `mission_storage.rs`, `space-bridge/src/event_stream.rs`, `service_credentials.rs`, and multiple security/integration tests. A tracked-only diff or archive would omit necessary code. No WIP was staged, reverted, cleaned or reclassified as disposable.

Read-only SHA-256 comparisons against preserved inventories establish a more useful stage boundary:

- `full-slice-ii-inventory.json`: 91 source entries; **75 unchanged, 16 changed**. The changed set is Core action/IPC/Policy/provider-observation/service-registration/storage; Bridge fixture pair/EventPump/handlers/service-credentials/state; Browser IPC/provider; World/Shell HTML. This matches the documented subsequent integration surface. It is not an assertion that every hunk in those files belongs to one task.
- Mission/attempt storage and orchestration, Gateway/MCP/read-client sources, all inventoried filesystem sources, principal/read/subscription contracts, dispatch proofs and SpaceTrace sources match their Slice-II hashes.
- `scoped-browser-inventory.json`: 13/20 source hashes still match; the seven differences are the four documented Bridge/UI recovery implementation files and three associated tests/driver files. Both fixture sources remain unchanged from that inventory. All **35 recorded artifact hashes** match.
- `scoped-browser-recovery-inventory.json`: **20/20 source hashes and 14/14 artifact hashes match**. All eight recorded binary observations match their current files, representing four unique executable paths. These are fixture/debug candidates, not certified production binaries.

The four matching executable hashes are:

```text
mvp-core-fixture.exe    f206d6570e21bb6fbe66c99401aa19508b9c21654886686d42ab40113e2846ad
mvp-runtime-fixture.exe 298cfa80a5220b13a34dbc3f64be8d7fa32a0e05ed1b5ebe97febfafd565ada4
filesystem-provider.exe 72e28751327be3322e920a6658f8241a573c791723c1857c5f55c3345f8f1798
browser-provider.exe    a29ee38638a634679274a0c40a13e31b64ddf9f98336b3a9cbc070f39a0668dd
```

Paths are under `C:\spaceos\target\debug`. Historical PIDs in the reports are evidence, never current process-control instructions. Free disk observed during this review: 26,688,692,224 bytes. No target/laboratory duplication or deletion occurred.

## 2. Current architecture and verified boundaries

### Files and relations

`filesystem-provider/src/watcher.rs::handle_fs_event` obtains the scoped Core base before metadata capture. `filesystem-provider/src/observation.rs::apply_observation` propagates transport/admission/CAS errors instead of treating rejection as success. A conflict is not rebased onto fresh Core state using stale metadata.

`space-core/src/provider_observation.rs::{admit_filesystem,verify_file_changed,validate_file_change}` authenticates a registered instance, resolves its exact root/World and validates the narrow shape and canonical identity. `space-core/src/storage.rs::apply_verified_file_changed` uses an immediate transaction, valid v4 receipt, recognized-replay handling before CAS, exact `updated_at` equality and checked Core-owned next version. Only metadata and authoritative update time change. The generic UPDATE trigger removes the receipt; only the verified transaction restores the protected prior admission facts. File/event/log/receipt commit together; publication occurs after commit.

`Storage::create_semantic_relation_as` resolves both endpoints and calls the shared receipt predicate on the **same transaction connection**. It allows semantic-to-semantic or exactly one admitted File, either direction, only `related_to`, exact World. Core IPC's `create_semantic_relation` branch separately requires the caller's relation capability and, for a provider reference, scoped `semantic.read`. A receipt is not authorization. File PATCH remains denied; no File identity/properties/timestamps/disk mutation is introduced. Existing Mission lifecycle guards remain present. No permanent Mission-agent or `space-world-user` capability expansion was found.

### Browser creation and operational authority

`space-core/src/provider_observation.rs::{configure_browser,browser_authority,verify_browser_tab_created,browser_tab_identity}` separates authenticated instance, owner-configured source, World, epoch and raw target. Canonical hashes are identifiers, not authorization. Existing durable authority pins and transactional storage prevent silent source/World reassignment; creation replay does not update the snapshot. Filesystem binding/receipt machinery is separate.

`space-core/src/service_registration.rs` defines the new reserved `browser_world` / `space-world-browser` profile with exactly `semantic.read`, `action.get_tabs`, `action.screenshot`. Core IPC registration requires exact configured World, an available unique registered source, durable matching authority pins, the separate registrar and no instance/dispatch credential substitution. Legacy profiles remain unchanged.

`space-core/src/browser_actions.rs::{authorized,canonical,execute}` checks Core authentication/capabilities/World, reconstructs the canonical identity, validates pins and compares current canonical snapshots. It obtains a signed live enumeration, filters to canonical matches, rechecks authorization and requires an exact live screenshot target. Storage locks are released before Provider/CDP awaits. `forward_action_to_provider` preserves signed dispatch and checks the returned action ID/performer; see finding 1 for the inherited credential-forwarding risk.

`browser-provider/src/provider.rs::execute_world_action` and its IPC verifier check signed protocol, configured observation instance/runtime/source/World, debugger-pinned live targets and cached Core-adopted canonical objects. Screenshot attaches to the exact target and re-pins after attachment; it does not use the legacy current/first-tab/reconnect fallback. `get_tabs` may omit unavailable/nonmatching rows; a successful empty list is not proof of full source health. Core's `canonical(...).is_ok()` filtering intentionally fails closed, but currently discards the rejection reason.

ActionRequested persists before dispatch; scoped terminal events persist before publication/success. External screenshot execution cannot be rolled back atomically with SQLite. A terminal write failure must not be interpreted as evidence that CDP never ran.

### Bridge, World and Shell

`space-bridge/src/event_stream.rs::EventPump::poll` now selects FileCreated, FileChanged, BrowserTabCreated and existing semantic-operation events. It validates increasing sequences; query/send failure preserves the relevant cursor; unrelated event types advance it without notification. Scoped read authorization remains in `ipc_client::query_events` and Core's read scope. No global reads were introduced. The pump is a live stream initialized at the latest visible cursor, not a client-acknowledged durable message queue.

`world.html::{tabObjs,browserLiveTarget,objName,renderTreeMode,renderGraphMode,renderSidebar}` uses Core objects for semantic representation. Live CDP handles are optional overlays joined by canonical ID/raw target plus source/instance/epoch/World in scoped mode; hashed external identity is not a CDP target. Live rows do not create duplicate semantic nodes or rewrite creation snapshots. Graph's bounded fallback remains intentional; an unlinked BrowserTab is not promised a Graph overview node. `handleDelegatedClick` no longer shadows `hideDrop()` with the former local boolean.

`world.html::{canBrowser,applyBrowserMode,loadBrowserAuthority}` and Shell's `{shellCanBrowser,applyShellBrowserAuthority,loadBrowserAuthority}` start disabled and require a recognized mode plus bounded capabilities. Unknown/loading/error state is unavailable, not implicit legacy. Capability metadata is UX gating, not caller authentication or proof of Provider availability. Unsupported scoped operations remain disabled.

`space-bridge/src/state.rs::{browser_world_action,ensure_browser_world_agent}` recovers only from the exact pre-dispatch authorization error. It conditionally clears the failed cached token and performs authenticated re-registration for **subsequent** requests, never replaying the rejected screenshot. No ambiguous transport failure gets an automatic retry/fallback. `handlers.rs::core_err` maps typed scoped failures conservatively; unknown errors remain 500. String-based classification remains compatibility debt, not a reason to broaden heuristics.

## 3. Five prioritized remaining issues

### 1 — P1: reusable caller credential crosses the Provider boundary

**Confirmed source fact; compromise/exfiltration is not demonstrated.** `space-core/src/action.rs:58–74` serializes `ActionRequest.actor_token`. `dispatch.rs::{DispatchPayload,issue_at}` embeds/clones the complete request. `ipc.rs::forward_action_to_provider` (line 2343 onward) both serializes the complete request into Provider parameters and includes its complete signed proof. `browser_actions.rs::execute` clones the authenticated request for live enumeration and screenshot without removing that token.

Therefore the Provider receives a reusable actor bearer credential, not only a short-lived operation-bound authorization. The new scoped actor also has semantic read authority in its World. A compromised Provider with Core transport reachability would potentially inherit that caller authority. Signing the envelope does not confine reuse of the bearer field. The dispatch mechanism is unchanged from Slice II; this is inherited trust coupling exposed by the new scoped path, not a newly proven Policy bypass.

**Minimum follow-up:** explicitly decide the Provider trust boundary, then authorize a bounded dispatch wire correction that authenticates at Core but does not delegate the reusable actor credential. Keep attribution and operation-bound proof. Required test: intercept only synthetic signed Provider input, assert no reusable caller credential occurs in either outer request or proof, preserve signature/replay/performer checks, and prove the Provider cannot use a captured operation proof as an agent credential. No real credential was captured/read in this review.

### 2 — P1 release gate: source/artifact separation and production trust are not consolidated

**Confirmed packaging risk and known production limitation, not observed production exposure.** Required security source remains untracked; `test-tmp/` is also untracked and **not ignored**. `git check-ignore` does not exclude the known `test-tmp/vertical-corrections-8HV07i/fixture.secret.json` path. A broad add/archive could include synthetic credentials, DBs and Chrome profiles; a tracked-only archive could omit security code. A successful known-secret scan of public reports is not a universal secret scan of the repository.

Fixture binaries are correctly gated by `mvp-lab-runtime`; fixture launchers require an explicit isolation marker, canonical test-state containment, private endpoints and fixed lab origin. Lab login is explicit, not a normal production default. No evidence was found of automatic production activation. Nevertheless copying a fixture configuration is not a production setup procedure. Unset Browser mode still preserves legacy compatibility; it does not select scoped mode automatically.

Production localhost caller authentication, legacy broad/unscoped dashboard authority, Windows SID/pipe ACLs, separate credential provisioning/rotation and the intended scoped source/World/epoch remain owner decisions. Origin/CORS is not native-client authentication. These risks are not silently accepted by this review.

**Minimum follow-up:** an explicit reviewed release-source allowlist and artifact exclusion plan, then the existing owner security/readiness gate. Do not clean/delete evidence or rewrite credentials as part of this review. Test the resulting package contains all required source and no private fixture/profile/database material; production readiness must use a separately approved non-fixture topology.

### 3 — P2: the new Browser registrar is missing from startup credential validation

**Confirmed configuration-validation defect, with late fail-closed checks retained.** `security_config.rs::ROLE_KEYS` (line 7) omits `SPACEOS_SERVICE_REGISTRAR_TOKEN_BROWSER_WORLD`, although `service_registration.rs::from_env` loads it. The documented all-present-domain validation therefore does not validate this credential's encoding/length/aliases before SQLite opens. `from_env` silently skips malformed values or failed insertion.

Registry insertion still rejects principal/transport/other-registrar aliases; IPC's new registration branch rejects observation-instance/dispatch aliases. This is not evidence of a successful cross-domain registration. The consequence is late denial/silent missing registration instead of a reliable startup diagnostic.

**Minimum follow-up:** include the new role in existing startup validation, without changing grants or secret storage. Add isolated environment tests for malformed value and every existing credential-domain collision, plus a valid distinct value. The current lab isolation early return means happy-path fixture tests cannot certify production environment loading.

### 4 — P2: WebSocket backlog/backpressure is unbounded

**Confirmed source behavior; resource exhaustion has not been reproduced.** `handlers.rs::handle_websocket` (lines 879, 910) creates an unbounded outbound channel per client and polls with `after_seq` but `limit=None`. `ipc_client::query_events` preserves this. Core IPC's `query_events` branch and `Storage::query_events` (line 1659) materialize/filter the matching history without a bound before any optional tail limit. A stalled client or large backlog can accumulate memory and repeated work; the storage read also holds Core's mutex. Current correctness tests exercise small streams, not pressure.

No cursor-loss defect is established for the current unbounded query. **Do not fix this by merely setting a limit:** the current API returns the newest tail, so a limited catch-up request followed by advancing to its last sequence could skip older pending rows.

**Minimum follow-up:** separately approve a bounded oldest-first catch-up/backpressure contract preserving scoped visibility and cursor semantics. Tests must cover backlog beyond the bound, slow/disconnected clients, gaps caused by hidden Worlds, reconnect snapshots and no loss/unauthorized delivery. Existing queue-send success is not a client receipt acknowledgement.

### 5 — P2 functional gap: filesystem scan/reconciliation remains narrower than watcher support

**Documented and source-consistent compatibility limitation, not a new FileChanged regression.** `filesystem-provider/src/scanner.rs` and `observation.rs` now propagate actual admission errors. `provider_observation.rs::admit_filesystem` excludes the exact binding root and accepts only the approved FileCreated/FileChanged shapes. Scanning the exact root can therefore return root admission denial; legacy FileUpdated/move/delete reconciliation is not implemented by the verified admission contract. The old false-success behavior must not be restored.

**Minimum follow-up:** define the smallest root-scan/reconciliation behavior using existing authorized reads and admitted child Files, without inventing Directory update/delete/rename authority. Require a real scan through authenticated Core, error propagation, duplicate/restart handling and preservation of receipts/CAS. Any lifecycle expansion needs separate approval. This matters before presenting Files scan as a generally working project synchronization action.

## 4. Regression evidence and material coverage limits

Reports reviewed: `SPACEOS_DURABLE_FILE_PROVENANCE_IMPLEMENTATION.md`, `SPACEOS_PROVIDER_FILE_RELATIONS_FINAL.md`, `SPACEOS_FILE_CHANGED_IMPLEMENTATION_FINAL.md`, `SPACEOS_BROWSER_OBSERVATION_IMPLEMENTATION.md`, `SPACEOS_FULL_VERTICAL_SLICE_II.md`, its evidence/gaps, `SPACEOS_VERTICAL_SLICE_II_CORRECTIONS.md`, `SPACEOS_SCOPED_BROWSER_ACTION_IMPLEMENTATION.md`, and `SPACEOS_SCOPED_BROWSER_RECOVERY_FINAL.md`, together with actual implementation paths and preserved hash inventories. Historical reports describe historical hashes; later inventory matches establish the final reviewed source.

Preserved execution evidence, **not rerun/recertified as new execution**:

| Evidence | Executed result / boundary |
|---|---|
| `provider-file-relations-final-regressions.log`, `provider-file-relations-ipc.json` | 61 behavioral tests; Cargo total 62 includes a guarded child entry point. Actual authenticated IPC, both relation directions, scoped negative cases, atomic rollback and restart |
| FileChanged final report / `file-changed-watcher-ipc.json` | Real watcher create/change, exact CAS conflict/replay, receipt persistence, synthetic IPC negatives and controlled restart; not merely fixture calls described as watcher activity |
| Browser observation report / verified observation evidence | Real dedicated Chrome creation/replay/restart and same-port replacement denial; two-Chrome concurrency and every new-epoch scenario are not certified |
| Slice II reports / `full-slice-ii-inventory.json` | Real selected Qwen/OpenCode: six model-originated MCP calls; Task/Summary persisted; owner-confirmed exact-attempt Mission completed; controlled persistence restart. Original WS/search/canonical UI failures retained rather than rewritten |
| Corrections report / `vertical-slice-ii-corrections-*` | 201 focused regressions; actual scoped WS new event types, rejection/replay/foreign-World checks; real search/UI evidence. Original assertion/setup failures are disclosed in the report |
| Scoped action report / `scoped-browser-*` | 260 focused regressions, 60 real runtime assertions, 20 artifact checks; signed negative cases, real canonical screenshot and restart. Retained invocation/setup failures explicitly disclosed |
| Recovery report / `scoped-browser-recovery-*` | 208 focused regressions, 72 real runtime assertions, 27 audit checks. Real stale request 403 with no action-event increase; a separate subsequent screenshot succeeds; World/Shell capability gating and same canonical object after restart |

These suites overlap; totals must not be summed into a unique overall test count. Logs/inventories are local evidence, not independent cryptographic certification of test execution.

Fresh focused review executions used Node v24.20.0 and the existing test executable, **without Cargo/builds or runtime services**:

```powershell
node tests/scoped_browser_recovery_ui.cjs
node tests/vertical_slice_ii_ui.cjs
& C:\spaceos\target\debug\deps\space_bridge-9bb2d8bd5287de08.exe event_stream::tests --test-threads=1
```

Results: **20/20 + 26/26 + 5/5 = 51 PASS, zero assertion failures.** Node tests execute actual extracted HTML helpers with DOM/HTTP doubles; they are not fresh real-browser evidence. The Rust executable runs the five EventPump unit tests only. Its SHA-256 is `224a37e478e6b29036148ed2eec7811b8811677f97f5ada13ff5ce52d854d65d`; the preserved recovery build log identifies this same executable name. Read-only hash verification completed at 2026-10-25T03:45:56Z. No fresh real transport/UI pass is claimed. Expected negative Git probes returned nonzero for untracked files/not-ignored paths; these were inspection results, not test failures.

Remaining material integration tests attach to the findings above. Additionally, existing reports explicitly leave concurrent stale-token refresh races, post-CDP terminal SQLite write failure, ambiguous multiple sources in one World, scoped-action Chrome replacement races and every real 502/503/504 outage branch unverified. Earlier Browser admission port-reuse testing does not substitute for all scoped-action races. These need bounded failure injection, not another successful Qwen Mission. No unexecuted scenario is marked PASS.

## 5. Consistency, intentional limits and complexity

No new bypass of Core Policy, semantic World ownership, File receipts/CAS, reserved actor registration, provider instance authentication or Mission lifecycle was found in the reviewed corrections. Hash preservation supports the unchanged Mission/Gateway/filesystem contracts. This is a scoped review conclusion, not a proof of absence of all defects; finding 1 prevents an unconditional claim of least-privilege credential separation.

Intentional MVP limits remain: Browser creation snapshots only; no navigation/update/close/delete persistence; only scoped get_tabs/screenshot; no automatic epoch rotation; no ungranted Mission relation creation; historical unreceipted Files are not recertified; invalidated receipts block new relations but do not retroactively delete old ones. World Graph overview omissions are deliberate, not missing SQLite records.

Remaining product evidence limitations include Summary `progress` versus legacy `summary/status` ambiguity, inconsistent World scope/health labels, older event presentation, unresolved canvas-pointer behavior and unverified ETW/model-process attribution. An expired lab login can still be presented as “Bridge unavailable.” None authorizes exposing hidden events, fabricating health, silently changing Summary ontology or rewriting owner verification history.

There is no new service or schema from scoped Browser actions/recovery. Shared canonical identity constructors are reused rather than a second UI database. World and Shell necessarily duplicate some capability validation/presentation helpers; focused tests currently constrain both. Legacy/scoped routing and string-based error mapping add maintenance cost, but removing legacy authority, adding structured errors or broadly unifying frontend code would be an architectural/compatibility decision outside this review. The large multi-mode fixture driver also increases audit complexity; retain its explicit executed-path evidence instead of treating dormant modes as executed coverage.

## 6. Recommended next milestone

**Bounded “consolidated candidate and failure-recovery gate,” then a project synchronization acceptance slice.** First resolve the caller-token boundary and release artifact/security decisions, fix the narrow startup validation omission, and authorize targeted pressure/failure tests. No further successful model Mission is needed for those deterministic questions. Next address the root-scan contract and clarify the exact Summary output property/read-back in the existing Mission acceptance criteria, without creating a new orchestration or ontology layer.

The completed isolated Browser/UI/WS corrections may be retained as the functional baseline. Production readiness remains **NOT APPROVED**; legacy authority and caller authentication are not accepted risks by implication. No production access, cleanup, commit, push or deployment was performed. Any implementation arising from this review requires a separate owner scope.
