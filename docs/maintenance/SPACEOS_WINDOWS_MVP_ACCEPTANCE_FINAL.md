# SpaceOS Windows MVP acceptance — final

Date: 2026-10-25. Repository: `C:\spaceos`. Role: MVP release verification engineer.

This is the final bounded acceptance of the existing Windows MVP candidate before an owner-approved source freeze and CachyOS migration, executed per `SPACEOS_MVP_FREEZE_CACHYOS_HANDOFF.md` §5C as the single approved path. No functionality was added, no architecture refactored, no implementation contract changed, no source edited, no production endpoint or database opened, no cleanup/staging/commit/push/deployment performed. Qwen and OpenCode were not launched.

**Overall acceptance result: PASS.** The bounded model-free v5 candidate passed integrity, one fresh isolated runtime acceptance and the accompanying audit with all owned cleanup complete. Source freeze readiness: candidate is verification-ready and preservation-complete on disk; freezing still requires exactly the owner decisions in section 8.

## 1. Current candidate integrity (executed, all PASS)

| Check | Actual result |
|---|---|
| Git branch / HEAD | `flaky-t1t2t3t4`; `7990bd9f525bbe626ed5be07bf3c964270028ae4` (`fix(bridge-ui): repair world.html inline script and guard static UI assets`) |
| Git tree | 237 tracked paths; 40 tracked unstaged modifications (exactly the accumulated WIP list documented by the source-isolation report); 900 visible untracked paths; 0 staged changes |
| Git index SHA-256 | `acc7b09c01c5d22f21ed88ece3df47023dd3515d581a7baf40473f8c5cea5e3c` — matches both isolation reports; unchanged through this acceptance in live checks and at report creation |
| Cargo metadata (`--offline --locked --no-deps`) | 7 packages / 120 targets / 0 missing target roots; all `0.1.0`; edition 2021 except SpaceTrace edition 2024 |
| 250 source/support candidates | Reconstruction of the exact documented set (space-bridge, runtime, filesystem/browser providers, core, spacetrace, root/agent-demo/mock/scripts/tests groups): 250/250 exist on disk, 0 duplicates, 0 missing; 178 tracked + exactly the 72 documented untracked candidates (set matches the handoff list line-for-line); none ignored |
| 250-source fingerprint | `39255f407b5f5e26038632369192cac12d2011a9aa38c99c2815a1a75e7c3dc2` — EXACT match (SHA-256 of JSON-encoded sorted `[relative path, SHA256]` records; 11 per-group counts match the handoff table) |
| Selection hashes | 5/5 match: `Cargo.lock` `e61b82c9…`, `space-bridge/static/index.html` `90ce43bb…`, `space-bridge/static/world.html` `3ae990ad…`, `space-core/src/action_storage.rs` `66d27084…`, `space-core/src/storage.rs` `a84fcb7e…` |
| v5 binaries vs `action-v5-inventory.json` | 4/4 match: `target\debug\mvp-core-fixture.exe` `9a6d29e0…`, `mvp-runtime-fixture.exe` `472e3aaf…`, `filesystem-provider.exe` `147edd63…`, `browser-provider.exe` `3003a77f…` → **no rebuild required; no build performed** |
| v5 source continuity (19 hashes) | 16/19 unchanged; the only deltas are the later authorized evidence-writer changes to `tests/action_v5_audit.cjs`, `tests/scoped_browser_actions.cjs`, `tests/vertical_slice_ii_corrections_runtime.cjs` (exactly as documented) |
| Writer-stage hashes (18 modified + 2 new) | 20/20 match the writer-isolation report's recorded hashes, including both new helpers `tests/evidence_paths.cjs` `6d04285c…` and `tests/evidence_writer_isolation.cjs` `c62bab22…` |
| Evidence integrity exception | The three overwritten fixed-name maintenance JSONs retain their disclosed current bytes: `provider-file-relations-ipc.json` `f005c952…`, `durable-file-provenance-ipc.json` `3fbdd7da…`, `file-changed-watcher-ipc.json` `66d23973…` (not originals; disclosed in the freeze record) |
| Historical maintenance evidence | Both documented aggregates reproduced exactly with SHA-256 of JSON-compact sorted `[repo-relative path, hash]` records: 801-file writer baseline `9b6e319c…` and 802-file handoff baseline `00642ec6…`; re-reproduced again after the acceptance run. No maintenance file changed; my own per-file before-snapshot is kept at `C:\spaceos\test-tmp\integrity-scan-SPWF1\maintenance-snapshot-before.json` (803 entries) |

Integrity recheck tooling (read-only Node scripts and per-file hashing) is confined to `C:\spaceos\test-tmp\integrity-scan-SPWF1\` (ignored test state, not evidence and not source).

## 2. One isolated Windows runtime acceptance (executed once, PASS)

Writer freshness first: `node tests/evidence_writer_isolation.cjs` → **25/25 PASS** (`{"checks":25,"result":"PASS"}`, runs `C:\spaceos\test-tmp\evidence-writer-A-zAK2v0` / `evidence-writer-B-fqBIcr`).

Precondition: fixture-owned endpoints verified free — TCP 21973 (Bridge origin), 21974 (owned synthetic page), 21975 (owned Chrome CDP), 21976 (foreign-origin guard value) all had no listeners. The unrelated live stack was not stopped or affected: `C:\spaceos\target\release\space-core.exe` PID 11956, `browser-provider.exe` PID 26108, two+ `spaceos-mcp.exe` instances and `C:\spaceos-i5-runtime-lab\target\debug\filesystem-provider.exe` PID 14808 remained running through the whole run (PIDs unchanged after it).

```powershell
Set-Location C:\spaceos
$env:SPACEOS_SCOPED_BROWSER_TEST = '1'
$env:SPACEOS_SCOPED_BROWSER_RECOVERY_TEST = '0'
$env:SPACEOS_ACTION_V5_TEST = '1'
node tests\vertical_slice_ii_corrections_runtime.cjs
node tests\action_v5_audit.cjs --run "<exact-new-run>"
```

Actual results:

- **Runtime driver:** exit code 0; **86/86 runtime assertions PASS** (same count as the designated final evidence). Fresh run dir printed by the execution and used verbatim for the audit — never discovered as "latest":
  `C:\spaceos\test-tmp\vertical-corrections-eBPBKP`
  World `db45a242-d2d5-4572-b5fa-4e355a219eb8`; platform v5 action `495d7565-3818-46f7-8c5d-6916559be9ef` outcome `completed`.
- **Coverage actually exercised this run** (selection of PASS names from stdout): real isolated Core + exact-World minimal agent grants; canonical Bridge at the dedicated origin; filesystem File object with exact World/provider; FileChanged stable identity and Core revision; durable File admission receipt retained; canonical BrowserTab ID with exact World; synthetic scoped actor establishing Task→verified-File; Bridge WS delivering both `FileChanged` and `BrowserTabCreated`; rejected/replayed observations producing no writes; foreign-World event absent from authorized WS and queries; registration denying reader/provider/transport credentials, foreign World, missing grants, substitution and delegation; Core denying bad tokens, foreign Worlds/objects, raw-only targets, unsupported/substituted/forged actions; signed Provider denying stale epoch, foreign source/World, substituted instances/targets and forged snapshots; real scoped get_tabs returning only the admitted live World; HTTP dashboard authentication and foreign-Origin denial; real Bridge scoped get_tabs and real HTTP Core signed screenshot; scoped-mode fallback denials for all legacy browser controls; generic raw screenshot rejection; operational reads creating no BrowserTabCreated; canonical Browser snapshot unchanged; committed screenshot with exact actor/World/performer; synthetic SQLite schema v5 with integrity and foreign-key consistency; reservation-failure aborting external dispatch; real scan Started; callback withheld leaving durable authority Started (not Completed); Core restart preserving exact pending authority; lost completion response committing exactly one attributed terminal outcome; duplicate/conflicting/substituted completions rejected or recognized; Started-write failures leaving reserved authority; retryable pre-Started completion; terminal-event failure rolling back action and event together; v2 wire use on both providers with bearer fields omitted; and the full restart block: restart without Provider registration fails closed, exact canonical live identity reconstructed, signed performer identity changes, Browser creation snapshot persists byte-exact, File admission receipt survives, old callback cannot be reassigned to a restarted performer, missing background job remains explicitly unresolved.
- **Writer isolation during the run:** all runtime evidence stayed inside the run dir as immutable numbered checkpoints `action-v5-runtime.000001.json` … `action-v5-runtime.000130.json`, plus `STOP` control file, `core.db`, `ACTION_V5_PROVIDER_SCREENSHOT.jpg` (real scoped screenshot), per-process `.out`/`.err` logs, `seeds.json` and the private `fixture.secret.json` (run-local, never published). No maintenance destination was touched.
- **Cleanup:** final line `{"cleanup":{"coreStopped":true,"bridgeStopped":true,"filesystemStopped":true,"browserStopped":true,"chromeStopped":true}}`; no leftover `target\debug` fixture processes; ports 21973–21976 all free again; STOP was written by v5 mode automatically, so the run was fully non-interactive.
- **Audit:** `node tests/action_v5_audit.cjs --run C:\spaceos\test-tmp\vertical-corrections-eBPBKP` → **17/17 checks PASS**, exit 0, final JSON `{"checks":17,"runtimeAssertions":86,"run":"C:\\spaceos\\test-tmp\\vertical-corrections-eBPBKP","world":"db45a242-d2d5-4572-b5fa-4e355a219eb8","action_id":"495d7565-3818-46f7-8c5d-6916559be9ef","outcome":"completed"}`. The audit inventory `action-v5-inventory.json` (21,780 bytes) was written inside that same run dir; retained historical maintenance evidence was not overwritten or replaced.
- Historical baselines were treated as read-only references only; the older schema-4 Browser real-observation test and the older pre-v5 recovery audit were NOT rerun, per the handoff.

## 3. Manual UI acceptance preparation (existing mechanism; no code changed)

No persistent manual-session mode exists and none was invented. The existing, already used mechanism is the corrections runtime driver's controlled live window: in the plain scoped mode the driver completes its verification flow, announces status `READY_FOR_REAL_UI`, and then keeps every owned component live, taking an evidence snapshot roughly every 1.2 s for up to **20 minutes**, until the owner creates the run-local `STOP` file — then it exits cleanly through its `finally` block and stops everything it owns. In v5 mode the driver auto-writes `STOP` at the end by design (deterministic); it has no manual window.

### 3.1 Safe startup (synthetic state only)

```powershell
Set-Location C:\spaceos
$env:SPACEOS_SCOPED_BROWSER_TEST = '1'
$env:SPACEOS_SCOPED_BROWSER_RECOVERY_TEST = '0'
# SPACEOS_ACTION_V5_TEST intentionally NOT set for the manual window
node tests\vertical_slice_ii_corrections_runtime.cjs
```

Wait for the console JSON line `{"status":"READY_FOR_RECOVERY_UI"|"READY_FOR_ONE_WORLD_MISSION"|"READY_FOR_REAL_UI",...}`; the manual inspection period begins at `READY_FOR_REAL_UI` (the driver remains running and must be left running). Every owned component is fresh and run-unique; no production pipe, port, token or database is used. The headless Chrome and the owned providers touch nothing outside the run dir.

Owner-side required components and addresses (assigned by the driver, printed in the JSON lines and stored in the run evidence):

| Component | Role | Local address / identity |
|---|---|---|
| `target\debug\mvp-core-fixture.exe` | isolated Core (own pipe + own `core.db` in the run) | `\\.\pipe\spaceos-4-3a4-process-mvp2b1-<uid>` |
| `target\debug\mvp-runtime-fixture.exe bridge` | Bridge HTTP/WS, enforces port 21973 | `http://127.0.0.1:21973/` = World UI (`world.html`); `http://127.0.0.1:21973/index.html` = full Shell |
| `target\debug\filesystem-provider.exe` | authorized Filesystem observation/actions | rooted strictly at `<run>\project-files` |
| `target\debug\browser-provider.exe` | authorized scoped Browser reads | owns CDP `http://127.0.0.1:21975` |
| owned headless Chrome + page server | synthetic page and actual CDP backend | page server `http://127.0.0.1:21974/…` |

### 3.2 Test login (credential stays private)

The synthetic login proof is a per-configuration secret stored only inside the run: locally open `C:\spaceos\test-tmp\<run>\fixture.secret.json` and read the `login` field, plus the `world_id` (also printed on the driver console). Log in through the Shell/World login panel at `http://127.0.0.1:21973/`, or equivalently:

```powershell
$secret = (Get-Content 'C:\spaceos\test-tmp\<run>\fixture.secret.json' -Raw | ConvertFrom-Json)
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:21973/api/lab-auth/login -Headers @{Origin='http://127.0.0.1:21973'} -ContentType 'application/json' -Body (@{proof=$secret.login; world_id='<world uuid printed by the driver>'} | ConvertTo-Json -Compress) -SessionVariable ui
```

Do not copy the proof value, cookie or `fixture.secret.json` into any report, chat or repository path; it is run-local private state by contract. (The exact raw command with the credential substituted is run-local; this document intentionally contains no credential byte.)

### 3.3 What to inspect (World and Shell against synthetic state)

- World UI at `http://127.0.0.1:21973/`: Space/Tree/Graph/Focus inspection of the run's canonical World only (the UUID printed by the driver): Task/Summary objects, the admitted File object with its exact World/provider labels, evidence object, the Task→File relation established by the explicitly authorized synthetic actor, the relation to Project, and the canonical BrowserTab created from the real owned Chrome (ID printed as `browser-tab-v1-…`; exact object, no extra snapshots). In `index.html` check the Shell presence (Browser tab list, provider panels).
- File test resources: `<run>\project-files\` — the driver's created/changed document and scan results are the admitted test data. Optionally, during one window, create a plain text file inside `<run>\project-files\` and watch a FileCreated→FileChanged cycle reach the World UI through the authenticated WS; do not modify anything outside that folder.
- Browser test resources: open `http://127.0.0.1:21974/evidence.html` from any normal browser tab — that is the synthetic page actually rendered by the owned headless Chrome; the World's BrowserTab representation covers tab identity, and scoped screenshot intake is exercised by the driver. Legacy browser controls (`/api/browser/activate|close|new-tab|navigate|reload|content`) are expected to be denied in this scope (fail-closed) — that is correct behavior, not a defect.
- WS: the UI receives `FileChanged`/`BrowserTabCreated` and semantic events over `wss`/`ws /ws` only with the logged-in session (foreign-World and unauthenticated delivery were asserted by this run).

### 3.4 Safe shutdown (owner action ends the window)

```powershell
New-Item 'C:\spaceos\test-tmp\<run>\STOP' -ItemType File -Force
```

The driver stops core/bridge/both providers/headless Chrome and closes the page server itself, then prints the `cleanup` JSON (all five `*Stopped:true`). Expected afterwards: no `C:\spaceos\target\debug\{mvp-core-fixture,mvp-runtime-fixture,filesystem-provider,browser-provider}.exe` processes remain and ports 21973–21976 are free. Never use `stop_spaceos.ps1` or taskkill on the fixture set; and never stop the unrelated production processes (`C:\spaceos\target\release\space-core.exe` PID 11956, `browser-provider.exe` PID 26108, `spaceos-mcp.exe` instances, and `C:\spaceos-i5-runtime-lab\target\debug\filesystem-provider.exe` PID 14808).

### 3.5 Optional recovery-UI window (existing)

With `SPACEOS_SCOPED_BROWSER_RECOVERY_TEST = '1'` (instead of the scoped-only flags), the driver pauses at `READY_FOR_RECOVERY_UI` awaiting `<run>\RESTART` (typed as a file) to perform the controlled restart segment, then completes; `STOP` at any time ends it. Use only if the owner also wants the recovery UI path inspected.

### 3.6 Known launcher limitations (reported, not fixed here)

1. **v5 mode cannot be used for manual UI inspection** — it writes `STOP` automatically on completion (single deterministic bump; by design). For manual inspection only the plain scoped (or recovery) mode provides the live window.
2. **The plain scoped window is hard-bounded at 20 minutes.** If the owner needs a longer or indefinite synthetic manual session, that is not implemented. Smallest separate follow-up (owner approval required, no edit in this acceptance): a tiny driver-flag extension that extends or removes the READY_FOR_REAL_UI wait bound (i.e., optionally trust only the `STOP` control file), plus documentation — everything else (login, addresses, cleanup) already exists and would be unchanged.
3. `launch.ps1` exists for the production wiring (Chromium-155 at CDP 9222, pipe `spaceos-core`, Bridge `127.0.0.1:8765`, watch path `C:\spaceos\data`, shared `chrome-profile`) and does not auto-stop — but it is not the synthetic-state inspection path and was deliberately not run in this acceptance (production access).

## 4. Freeze decisions (boundaries reconfirmed)

- **Manifest completeness:** the 250-path inclusion surface is complete and current on disk (250/250 present, none ignored, 72 untracked pinned). A Git-only freeze attempt would omit the 72 required untracked files (including `space-core\src\action_storage.rs`, which `storage.rs` explicitly includes) — owner selection of the complete manifest remains mandatory.
- **Tracked `opencode.json`:** owner decision exactly as the handoff states — choose exclusion from any distributable/committed source package versus producing a separately reviewed sanitized template; do not publish without explicit approval. Its contents were not inspected, exported or reproduced anywhere in this acceptance; leave the current unstaged edits alone and do not `git add -f` or reset that file.
- **Freeze blockers:** no additional confirmed Windows functional blocker was found. Nothing in integrity, the fresh v5 runtime acceptance or the audit blocked preservation or reproduction. The only freeze prerequisites are owner decisions, not code defects.
- **Deferred (non-blockers, unchanged):** WebSocket scaling/backpressure, Filesystem lifecycle expansion (rename/move/delete/general reconciliation), Windows production authentication/provisioning and ACL/caller-brand risks, optional native Wry UI, Linux/OS tracing (ETW attribution unverified; no eBPF collector), as well as clean-checkout build certification (needs owner-approved complete revision first; `Cargo.lock` is not vendoring).
- **Handoff evidence exceptions to keep disclosed:** the three overwritten historical fixed-name JSONs (current-bytes hashes above) are later v5 results, not lost originals; do not claim reconstruction.

## 5. Tool versions used

Read-only toolchain observed in the handoff and used unchanged here: cargo/rustc 1.99.0, Node v24.20.0. OpenCode 2.1.3 is the historical execution-CLI version installed with the owner Desktop setup; the acceptance rode the driver's existing client utility paths only where the approved driver already did; no OpenCode server, session or model process was launched, and no Qwen call was made.

## 6. Report-of-record artifacts produced by this acceptance

New files (all outside history-sensitive maintenance evidence; none overwritten existing evidence):

| Path | Content |
|---|---|
| `C:\spaceos\docs\maintenance\SPACEOS_WINDOWS_MVP_ACCEPTANCE_FINAL.md` | this report (the only new maintenance file) |
| `C:\spaceos\test-tmp\vertical-corrections-eBPBKP\` | the fresh runtime run: 130 immutable runtime checkpoints, `action-v5-inventory.json` (audit inventory, 17 checks recorded), `core.db`, `ACTION_V5_PROVIDER_SCREENSHOT.jpg`, per-process logs, `STOP` |
| `C:\spaceos\test-tmp\evidence-writer-A-zAK2v0\`, `...\evidence-writer-B-fqBIcr\` | fresh 25/25 writer-isolation result pair |
| `C:\spaceos\test-tmp\integrity-scan-SPWF1\` | read-only integrity tooling: `candidate-250.txt`, `integrity-expectations.txt`, `scan.cjs`, `agg-brute*.cjs`, `maintenance-snapshot-before.json`, `acceptance-v5-stdout.log` (full runtime stdout incl. 86 PASS lines) |

No historical report, image, log, JSON or maintenance utility was changed, moved, overwritten or repackaged. Git index hash `acc7b09c01c5d22f21ed88ece3df47023dd3515d581a7baf40473f8c5cea5e3c` still current and `git diff --cached --name-only` empty.

## 7. Exact source freeze readiness

`mvp-windows-v5-freeze` candidate state as proposed label (owner to confirm): Windows MVP / SQLite v5 / dispatch v2 / post-evidence-isolation — **readiness: verification-complete and preservation-complete on disk; freeze is PENDING OWNER APPROVAL of the decisions in section 8.** After owner approval, the freeze commit/tag must include the complete 250-path selection (or its approved successor), not only modified tracked files, and must never include `test-tmp\`, `target\`, runtime databases/tokens/private JSON, Chrome profiles or the unreviewed tracked `opencode.json`.

## 8. Explicit remaining owner decisions

1. **Approve the source freeze scope, label and timing** — approve the current complete post-writer v5 source/test/docs candidate and its disclosed limits as the freeze content (`mvp-windows-v5-freeze` label proposal), with the freeze commit as a separate owner-approved step. This acceptance gives the fresh model-free runtime evidence in favor.
2. **Tracked `opencode.json` disposition** — exclude from the distributable/committed package, or approve producing a separately reviewed sanitized template; no publishing without explicit approval; do not inspect it with automated tooling either way inside this pipeline.
3. **Evidence acceptance choice** — record this fresh model-free acceptance (`vertical-corrections-eBPBKP`: 86 + 17 checks) as the current pre-freeze runtime evidence (per §2), alongside the reused real Mission/UI evidence (unchanged source hashes) and the disclosed 3-JSON integrity exception in the freeze record.
4. **Manual UI acceptance choice** — accept the existing 20-minute bounded window (section 3), and optionally schedule the separately approved hold-bound follow-up from §3.6.
5. **CachyOS first-stage scope and Linux transport plan** — per handoff §8: browser-served Core/World/Shell + scoped filesystem/browser + existing Mission first, then owner-approved transport/path/identity provisioning (local sockets or explicitly configured loopback TCP with equivalent isolation; neither fixed-port fallbacks), native UI/OS collector deferred.

STOP: acceptance gate finished; all authority now returns to the owner. No further action was taken after this report.
