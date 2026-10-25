# SpaceOS source and test artifact isolation gate

Date: 2026-10-25. Repository: `C:\spaceos`.
Role: independent build/repository integrity review.

**Overall gate: PARTIAL. Source inclusion and normal Git artifact exclusion verified; repository-wide evidence-writer isolation is NOT complete.** No application source, test, existing report/evidence, database or production configuration was modified. The only changes in this task are `.gitignore` and this report. No staging, build, test execution, cleanup, deletion, migration, service connection, commit, push or deployment occurred.

## 1. Actual Git state and scope

Branch `flaky-t1t2t3t4`; HEAD `7990bd9f525bbe626ed5be07bf3c964270028ae4`, `fix(bridge-ui): repair world.html inline script and guard static UI assets`.

Before this task:

- 237 tracked paths; 39 tracked unstaged modifications; zero staged changes.
- 96 short-status entries (collapsed directories), not 96 individual changed files.
- 3,632 visible untracked paths, of which 2,737 were under `test-tmp/`. The latter had no tracked entries.
- 8,656 actual regular files were inventoried under `test-tmp/`, including files already hidden by other ignore rules. They were not removed, read as credentials, or repackaged.
- 895 other visible untracked paths include required source, reports, screenshots, historical source copies and auxiliary WIP. Untracked does not mean disposable.

After ignore changes and before creating this report, visible untracked count is 895 and visible `test-tmp/` count is zero. This report adds one visible untracked path: expected final count 896. Tracked modifications become 40 solely because `.gitignore` changed. Index remains unstaged.

All 39 pre-existing tracked modifications are listed below; they are accumulated WIP, not attributed to this review:

```text
Cargo.lock
browser-provider/Cargo.toml
browser-provider/src/cdp.rs
browser-provider/src/ipc.rs
browser-provider/src/main.rs
browser-provider/src/objects.rs
browser-provider/src/provider.rs
filesystem-provider/Cargo.toml
filesystem-provider/src/actions.rs
filesystem-provider/src/browser.rs
filesystem-provider/src/ipc.rs
filesystem-provider/src/lib.rs
filesystem-provider/src/observation.rs
filesystem-provider/src/scanner.rs
filesystem-provider/src/watcher.rs
filesystem-provider/tests/roots_gate.rs
opencode.json
runtime/src/main.rs
runtime/src/ui.rs
space-bridge/Cargo.toml
space-bridge/src/bin/spaceos-mcp.rs
space-bridge/src/handlers.rs
space-bridge/src/ipc_client.rs
space-bridge/src/lib.rs
space-bridge/src/mcp.rs
space-bridge/src/mission.rs
space-bridge/src/state.rs
space-bridge/static/index.html
space-bridge/static/world.html
space-bridge/tests/mcp_stdio.rs
space-bridge/tests/static_ui.rs
space-core/Cargo.toml
space-core/src/action.rs
space-core/src/ipc.rs
space-core/src/lib.rs
space-core/src/main.rs
space-core/src/policy.rs
space-core/src/storage.rs
spacetrace/src/hooks.rs
```

Read the actual post-Slice-II review and SQLite-v5 final report, root and all seven member Cargo manifests, module declarations and relevant evidence writers. Archived Cargo manifests beneath `docs/maintenance/consolidation-source-preservation/before/` are historical snapshots, not additional workspace members.

## 2. Exact ignore changes

Added, without changing/removing existing rules:

```gitignore
runtime/*.db-wal
runtime/*.db-shm
runtime/*.db-journal

# Synthetic test state: databases, credentials, browser profiles and run evidence
/test-tmp/

/core.db-journal
```

The first three cover SQLite companions alongside the already excluded runtime databases. The root journal rule completes the existing root `core.db` dump rules. The anchored test-state directory rule covers synthetic SQLite/WAL/journals, per-attempt credentials, login files, Chrome user-data profiles, MCP configuration, logs and runtime evidence regardless of their basename. No global `*.json`, `*.rs`, `*.toml`, `*.md`, `tests/` or `docs/maintenance/` rule was added.

Existing `target/`, `target-agent-staging/`, `runtime/*.token`, runtime DB/log/error/JSON rules and the existing local data/profile/log directory exclusions remain unchanged. No new target tree was created. Available disk at inspection: 26,338,078,720 bytes; no compilation needed.

**Boundary:** ignore rules protect untracked artifacts against ordinary broad Git staging, not `git add -f`, explicit archive/copy tools, already tracked secrets, filesystem access, or overwriting files. This is not a secret scanner, Windows ACL or packaging implementation. Existing dirty tracked `opencode.json` is preserved: its contents were not inspected, exported or approved for distribution. It must receive owner-led private-configuration review before any source package. Ignore rules cannot untrack or sanitize it.

## 3. Preservation evidence for THIS task

Before editing, a read-only Node/crypto audit hashed the regular files in the union of `git ls-files -z` and `git ls-files --others --exclude-standard -z`, excluding `test-tmp/`, `.gitignore` and this report. It hashes bytes without printing contents, including opaque tracked configuration. Directories/subrepositories are not treated as regular source files.

- 1,130 regular existing files hashed.
- Aggregate SHA-256 of JSON-encoded sorted `[relativePath, fileSha256]` pairs: `731e49c2d0d33ccca2202510c12972809bf5bb382c147a5241c2173cfb1bd43b`.
- The same aggregate and count matched after the ignore edit; rechecked after report creation.
- Git index SHA-256 before/after: `acc7b09c01c5d22f21ed88ece3df47023dd3515d581a7baf40473f8c5cea5e3c`.
- `test-tmp/` metadata digest of sorted `[path, size, mtimeMs]` records: `004bfb769f3076900076574eb0a2b287aa53d075551c68965105e83616bda392`, 8,656 files, unchanged. This is metadata preservation evidence, NOT a claim that every private/profile/database byte was opened and hashed.
- No ignored runtime/production database was opened. Existing build artifacts were not rebuilt, copied or comprehensively rehashed.

The aggregate deliberately includes existing documentation/evidence and archived source snapshots outside the ignored state, so a change to any inventoried evidence file would change it. The index match and empty cached diff establish no staging. These checks do not claim a historical baseline from before the v5 stage.

### Historical evidence exception (predates this task)

The v5 final report explicitly records that older test writers overwrote:

| Existing path under docs/maintenance | Current SHA-256, preserved in this task |
|---|---|
| `durable-file-provenance-ipc.json` | `3fbdd7dace3a3b907bafca57975258941beba6f7e9d144b16001b54a2f1c5813` |
| `file-changed-watcher-ipc.json` | `66d2397361af02a94641a25b94aaa177a51c4c6e8a11397f05c34cf4cc45b1c6` |
| `provider-file-relations-ipc.json` | `f005c952478f275ea6b5c55e324601b94e57ac87d7a58b0a5f14be2b4f5aba2f` |

These current files contain genuine later v5 test results, **not the original JSON bytes referenced by older reports**. Their original hashes must NOT be claimed to match. No original contents were invented, reconstructed or restored. The explicitly v5-named copies (`action-v5-provider-file-relations-ipc.json`, `action-v5-durable-file-provenance-ipc.json`, `action-v5-file-changed-watcher-ipc.json`) and all other evidence were left intact.

The three corrected Rust destinations are source-confirmed:

- `space-core/tests/durable_file_provenance.rs:76–77,159`: UUID-specific `test-tmp/file-relations-<UUID>/provider-file-relations-ipc.json`.
- Same file `:167–168,210–211`: UUID-specific `test-tmp/durable-file-ipc-<UUID>/durable-file-provenance-ipc.json`.
- `filesystem-provider/tests/verified_file_changed.rs:89–91,174`: UUID-specific `test-tmp/verified-changed-<UUID>/file-changed-watcher-ipc.json`.

The output filenames inside unique run directories are legitimate; the fixed historical report paths are no longer their write destinations. The v5 report records successful reruns after those writer corrections. This task did not rerun them or claim fresh runtime results.

## 4. Remaining fixed historical writers — explicit failed gate

**“No executable/test fixture silently writes into historical report destinations” cannot currently be confirmed.** Source inspection finds further direct fixed-destination writes, independent of the three corrected tests. They were NOT executed and were NOT changed because this task authorizes only ignore/report edits.

| Writer / responsible location | Fixed destination / effect |
|---|---|
| `browser-provider/tests/verified_observation.rs:79` | `docs/maintenance/browser-observation-runtime-evidence.json` |
| `scripts/browser-observation-probe.mjs:52` | `browser-runtime-identity-probe.json` |
| `scripts/browser-observation-real-test.mjs:29` | Opens `browser-observation-real-runtime.log` with truncating `w` mode |
| `tests/authorization-positive.cjs:8` | `consolidation-authorization-pointer.json` |
| `tests/consolidated_mvp_smoke.cjs:8,10` | `consolidated-mvp-smoke-evidence.json` |
| `tests/full_vertical_slice.cjs:8,10`, `full_vertical_slice_ii.cjs:9,11` | Fixed `full-slice[-ii]-smoke-evidence.json` |
| `tests/vertical_slice_ii_corrections_runtime.cjs:12` and save helper | Fixed corrections/scoped/recovery/v5 runtime JSON selected by flags |
| `tests/scoped_browser_actions.cjs:63` | Fixed scoped/recovery/v5 Provider screenshot JPG selected by flags |
| `tests/check_consolidated_mvp.cjs:24` | Fixed before-verification/final-check JSON |
| `tests/check_full_vertical_slice.cjs:32`, `check_full_vertical_slice_ii.cjs:44`, `check_full_vertical_slice_ii_http.cjs:15` | Fixed audit/HTTP JSON |
| `tests/vertical_slice_ii_corrections_audit.cjs:29`, `scoped_browser_audit.cjs:30`, `scoped_browser_recovery_audit.cjs:40`, `action_v5_audit.cjs:45` | Fixed audit/inventory JSON |
| Historical `docs/maintenance/*.cjs` utilities, including `analyze.cjs`, `finalize-*.cjs`, `run-*.cjs`, `test-completion-commands.cjs` | Write fixed summaries/manifests/patches; some build/run utilities retain sibling-laboratory paths |

Examples are sufficient to disprove global writer isolation; this is not a complete static-effect proof for arbitrary JavaScript/PowerShell. Some fixed snapshots are intentional report-generation outputs, but repeated execution can still overwrite preserved evidence. Gitignore does not prevent this.

**Required separately approved follow-up:** move remaining execution outputs to unique run directories in existing `test-tmp`, require explicit non-overwriting publication/promotion of sanitized evidence, and preserve old report references. Do not run fixed-destination drivers over historical evidence until that is addressed. Do not merely ignore `docs/maintenance` or delete old reports.

## 5. Build-source completeness verification

Executed `cargo metadata --offline --locked --no-deps --format-version 1`, without building or changing the lockfile. It resolves **7 packages, 120 targets, zero missing target roots, zero custom-build targets**, using existing `C:\spaceos\target`. All local path dependencies resolve to the seven workspace members.

Inspected out-of-line Rust `mod` declarations, explicit `#[path]` declarations and include macros. A filesystem resolution check found **96 declarations with existing module files, zero unresolved candidates**. This is source-closure inspection, not a new compiler pass or verification of every conditional compilation branch.

Important chains:

- `space-core/src/lib.rs` declares dispatch, principal, provider observation, service registration, semantic/read/security and Mission modules.
- `storage.rs:16–26` includes `mission_storage.rs`, new private v5 `action_storage.rs`, and both admission test modules. “Private” here means Rust/Core-internal implementation, not a credential file; **action_storage.rs MUST be included**.
- `ipc.rs:2184–2185` includes `browser_actions.rs`.
- Filesystem `lib.rs` includes `reconciliation_read.rs`; Runtime `main.rs` includes `read_client.rs`.
- Bridge `lib.rs` includes Gateway, event stream, Mission commands and service credentials loading. The code named `service_credentials.rs` is required source, not generated secret material.
- The four new Bridge fixture bins are explicitly feature-gated by `mvp-lab-runtime`; preserve their sources for test reproducibility, do not ship their executables/configurations as production defaults. The pre-existing `mcp_fixture` target is separate and is not claimed to have that feature gate.
- Bridge `handlers.rs:83–84` serves the real `space-bridge/static/world.html` and directory; Runtime `ui.rs:92–94` loads its own static index/app.js. Both asset sets are in the manifest.
- `tests/contract_tests.js` uses `tests/services.js`, which spawns `mock/contract_server.js` and `mock/proxy.js`; both untracked mock source files are required for that optional mock-contract regression harness. They are not real integration evidence.

The exact build/test/static manifest below has **201 files: 39 untracked and 162 tracked**. Of its untracked files, 23 are under src and 16 under tests. None is ignored, verified using `git check-ignore --no-index --stdin`. A tracked-only archive omits those required untracked files and is incomplete.

## 6. Proposed future inclusion manifest — owner review ONLY

No staging was performed. This is an explicit proposal, not authorization to commit the accumulated WIP or publish credentials. For a future commit, select changed/untracked entries individually after review; unchanged tracked dependencies need no new staging. Paths below are repository-relative to `C:\spaceos`.

### A. Complete active build, Rust tests and frontend inputs

Legend: `T` already tracked (possibly dirty), `?` currently untracked. Each line is `status SHA256 path`. Includes root/member manifests, Cargo.lock, all active member src/tests/support files and both static asset sets, not archived lookalikes.

Aggregate of sorted `path + NUL + SHA256 + LF` records: `8683a167bea8ac54a0774a4de040844ff56126c003816e36d0aebe94d1ec5d2d`.

```text
T e61b82c9e65e663391bf2dea50ed9e92a5edcd43d277c30fef1de867da065ad9 Cargo.lock
T ee227a6a739352175ffa0cf7227d7f63e399f8e7ae2849b8d109fc13f59fd40a Cargo.toml
T 6b211733285e1158d3089fd7d0101e5b46fe2508c30893de4c66c565d9d6b4fb agent-demo/Cargo.toml
T 73ec7221ddee21d5c76f49368f4a97424b291e76aa4569f233b4664edc2f06f1 agent-demo/src/main.rs
T 8cbfedd0b254780b8ee00ba9c24201f1337b6055ab9cfab102598a06e3a9b343 browser-provider/Cargo.toml
T 61b7a04d8949581acbbd0645b0dea7f377af16c53b60c2f875bd3c72c657221b browser-provider/src/cdp.rs
T 67dbb039c60af56a310a55a0c05abce9b09186f92c2c794a6c935bb25e6f2ecf browser-provider/src/ipc.rs
T 2ae0fcc975b0e5174249605341e9e24a4176eec853129373b798b520092cfdd9 browser-provider/src/lib.rs
T 7c38e227f9c3fe22aa95d30160af50f3954ae6c2297122faaa258d2627742049 browser-provider/src/main.rs
T 43c00076f73166ddbc5c8e2e285ad9fdbe41effb8913fae6d3421f5fe6184ff9 browser-provider/src/objects.rs
T f55db6ea085e6928f39682be27967723d69dcfb7ff7c7de637c4737fab9ecac0 browser-provider/src/provider.rs
? e2ee4b5d7babdb9aa909d31a9088879925433c053fc8fb5b0f552aac8bac26bf browser-provider/tests/verified_observation.rs
T ab7dedfee9f8ea108ab5d43a2ecd9f19d200bcae23c606ec435cb91fa5976e12 filesystem-provider/Cargo.toml
T f8030854a6fc0158bf354f872c3956d1792b5758a7860d1645807cfe9d246896 filesystem-provider/src/actions.rs
T 2ebd244b837c8ec061379112386022a670e500eb9b6e8b2779bc3e524b6351f9 filesystem-provider/src/browser.rs
T cf7b97fd12c18304fa0f3eb487162ffaa77dad17360ce11227e50488f2f61616 filesystem-provider/src/format.rs
T 9391984444b70ab4f45ed5f1d5125946345d0730fd41bfea05e4834d6d44b093 filesystem-provider/src/identity.rs
T 633237b60aab43b3f6133bba4e4cd2191c06b6fb66a349b43ef37af749b3479d filesystem-provider/src/ipc.rs
T f463733777655eb4d2ff28f814c83178150e453ce023b6a0b0e83a21e7b4c7f4 filesystem-provider/src/lib.rs
T 2d8f262ea9655df4cc7c9f8bab2a350f72a6810ca09a0eb46ca3724d46c463e9 filesystem-provider/src/main.rs
T 4bd26f92e40476082eb62917d2544afe55eba7373ad6b05e41ef29f1d5128f4c filesystem-provider/src/metadata.rs
T 5a8263a1cfb9831090c0d15d752ba083fd2771f24a4f72f1de8c60b25700484c filesystem-provider/src/observation.rs
T 5c3322693fb2d9c89208d8a338c16bb29b5a9c50cd4aa9ae42ac9b96dbb316d5 filesystem-provider/src/performer.rs
? 1436bd08cba2f83707f11c04e214b62ceb65a02760f1112bd99b25a0352639ae filesystem-provider/src/reconciliation_read.rs
T e4d4ba51cabf96407a472ad7a48602277f9bbff552957e6df1bbee5ed898fd4a filesystem-provider/src/roots.rs
T 00ebabcf175e62df59f99c06376e07047982b7c114f2af520562cf38f9b5e422 filesystem-provider/src/scanner.rs
T 1a5c4a9437b5e6b93b5deb630331c720eec02610dbb5fc74a531a06d7f02689f filesystem-provider/src/watcher.rs
T c5845905f758a5e9686a70e7fff4cfb40ea475586e16cf318b221142b0c57324 filesystem-provider/tests/break_b11_b14.rs
T ff34effc4e967b77011884cd7f33663988986bd9b96fff9e6f476b31d5ebe4d0 filesystem-provider/tests/break_b15_mixed.rs
T 9c6152caf6ee0f064ca64d6f0f13bc110d3db7ea0fab03ff7d4171bf067bc5b4 filesystem-provider/tests/break_b1_storm.rs
T b8fc51e7dc29ebb4165ec73f83d15ffe70e3a7eda07560c89ce0456bfd4f343f filesystem-provider/tests/break_b4_identity.rs
T 014d8a768c17f0cc9799ae4ae6cc6672415603e0cedb82090dae3ce0a58d95f6 filesystem-provider/tests/crash_t7.rs
T 3373a6d91785b3da337e529b1a51290dfc48e00441adb2197986acb7492c0fa8 filesystem-provider/tests/e2e_identity.rs
T 761acefacd36f2d145c9baf69c28e89296e8bdcdfc72edd1e9ec6d784836f03f filesystem-provider/tests/extended_x1.rs
T 6db94e775caa71dd2a04ee8f56dfde2ebc50547a5c337f7bf226b90048fe1a96 filesystem-provider/tests/extended_x10.rs
T 7fe8011031bdc8a3c5b230b92d6a69834a2bbb7ac6bebb9efa60a688d428e55b filesystem-provider/tests/extended_x13.rs
T 37a5933e301cc775ed54d5c81a19c868745b70d6f7230b4de55509e27f7b11b5 filesystem-provider/tests/foundation_level_d.rs
T 467c5f4f12118d594c6621a94b4c4c8d0e79f7bbcdcbff85ab03e8b47ec4fb8c filesystem-provider/tests/foundation_m5.rs
? 9ed7a045cfc618284be218cc1958412bdc1c34aa0e76ccbb7d562d5d3417d433 filesystem-provider/tests/i5_reconciliation.rs
T d11e691cc0dab5079eabcaaeff05c44aac0b0683448bad22f67d51848b068e4b filesystem-provider/tests/performer_identity.rs
T 61b7760326181954e13111ffefed803e8e5a4592c9d8e1cfa17d64496a28a3ee filesystem-provider/tests/process_observer.rs
T a49329e55900012cb32af6320965d0b629e0a11f3a87187fa65c9ccb48a90eca filesystem-provider/tests/product_slice.rs
T 5948c261a1d6a6e5b7cb698087178deb451406c14330b300d471602e098760eb filesystem-provider/tests/qual_s15.rs
T 992eca6e918b69e17409e633b4f13c46b7384d731b7df2b697e8f2046019e3a0 filesystem-provider/tests/roots_gate.rs
T 84d652bb3df587bdbe459297d2d98c195c9c9bcc53f2f29cc33e14068b573353 filesystem-provider/tests/scan_search.rs
T 6285ba65d189324607ede3a57e5ee197bd303422f951e00423614f1c0a9723d4 filesystem-provider/tests/spacetrace_slice2.rs
T 7c243f280a5475a31b66c6235f2a035cd74bee2cc6263ca62fa157d8179ea03f filesystem-provider/tests/support/global_lock.rs
T fa173768a847cf66750bfeb4f61cb6714ca58133f0225b3dc1ca3c38bb99486b filesystem-provider/tests/v14_event_fields.rs
T 95f76cdb07122fe48bedcee95fcff60819300bc517ae3f0a8a834803df1eae6b filesystem-provider/tests/v14_reactive_latency.rs
T fc08bdd36756cf25d9a9ebefdf6715a846f89aaf4963f76ff6e1370a11306993 filesystem-provider/tests/v15_provider_recovery.rs
T 3b111fcf572e668692b3a011347e6caf495aa44332dcae3a45dbc31cc6402070 filesystem-provider/tests/v16_deletion_provider.rs
T 9301c67f8df9f36be49a99e76f9a399104a47006b92c6e52a647fe94327f52a6 filesystem-provider/tests/v17_action_semantics.rs
T 0bfb53d96dc9410da919986dae9644cf950f0cc9f7dd42a7a53b0f9de8e7185a filesystem-provider/tests/v17_latency500.rs
T 136db810baa423b7f0f4fefbb213982c191a2b46fb5adec8a197445d0528fa84 filesystem-provider/tests/v17_uncertain.rs
? 7ab5713ae1b3bf1160c4174cb78bf304679c6f306d819524e111969d425601d5 filesystem-provider/tests/verified_file_changed.rs
T 47b12c2a33639b781e84c4f8c3e011d7438ed5daf699f9993efed3e0013f4f04 runtime/Cargo.toml
T 0ae0d43d7698bee0bae67664e2c60754332128ed387b9d5483dcff692e47e608 runtime/src/main.rs
? 4a8035b0717085dc5f6bd2d3bc644293cc1bcc67ddaf987594975de7d60455f7 runtime/src/read_client.rs
T d3e38c7964ef3bc60e8650452bb4a4c7955960a3972aae79e18acca1ed349f06 runtime/src/ui.rs
T 6421eb1931c2933876a399e478c02d7389ef4e5c55e7ccfca29aba286abd4543 runtime/src/wry_wrapper.rs
T e346462528cef6d8d3fb551db36913a08150837e2743957395803f1213a22b80 runtime/static/app.js
T 95d750a4b17115e22cd37bb82712f8b3e43131f5ae12221a14c384cc7856994b runtime/static/index.html
T 81e7acc33d33b05758990c0f4cf28f6eca8fb05894029e17f639d6417a2f9c19 space-bridge/Cargo.toml
? 984cc8e361c18d67009bb6ecdb2edcae638b81a16f6ce4d55f327f46f539564b space-bridge/src/agent_gateway.rs
T 77c4d39b48d87725a6ecbaf9a399d1f9f1a4756b2beb494183b16fd3e89f793c space-bridge/src/app.rs
? 905c6c6bf521311d836aa0a3b0cabbd19fc238dbe1b9e8760e9e20f0c281428c space-bridge/src/bin/isolated-bridge-fixture.rs
? aa58852443dccd0e7899d9ed3d0c65537fd97005f83d2e37814a5e9db42c736a space-bridge/src/bin/isolated-core-fixture.rs
T 7ebf2fe2e823d4931c0df1bcd92144e1e0c9145a5015634462b2c8561c5bbe51 space-bridge/src/bin/mcp_fixture.rs
? 78dceee9e29c752e9a658cb3d478400ec9a15357436f53815c911259855383f2 space-bridge/src/bin/mvp-core-fixture.rs
? ec702d821b1a193f00d547c216e70a55ab7724c9dc2704776c4c5e3644f7fc3e space-bridge/src/bin/mvp-runtime-fixture.rs
T 50e0def0a3e68cb18658a8add268189c0cc6fe94458fed0e2ac773444e7aae90 space-bridge/src/bin/spaceos-mcp.rs
? d97b679019754396599b2a13213d182013842c6f27c376066f4a95785ccb2827 space-bridge/src/event_stream.rs
T b1bde64101f2415047fc17b9f0913d350c78327263e04f1acee93d18bb96141a space-bridge/src/handlers.rs
T 3b7837f9e74ed89e32d1001d547059f8b307c2ce0a446368ffbd49f39644b760 space-bridge/src/ipc_client.rs
T 79bc9fc166cde93dfbb95599caa42db33258142ac2a2d46be84419412d526693 space-bridge/src/lib.rs
T 493b6bdd496b215c2446f0bcbf6b4be7a93a57bb42d3a0b2ed144ec5259b80da space-bridge/src/main.rs
T c78d7f83f00e40b254df0cd13090683a95e09e5302e587624dd1202d21258d51 space-bridge/src/mcp.rs
T 9c0f2164f3a7593ac91cf3d53bf6b42ebe972ba518e99285b1aeeafa3cfa0c98 space-bridge/src/mcp_client.rs
T b6de58fdcdaa08084e9046e18d944c5229ba91f8ba9f686c5cdf8c434cbe7193 space-bridge/src/mission.rs
? 813455b42480d8d8cd59fb04eea5a73ca2f17c298b02baa413acb908cdaeb40e space-bridge/src/mission_commands.rs
T 27be4953cca8d016f07e3591ee8787664c8edac245fcffcdba005697529c3962 space-bridge/src/server.rs
? 7ce631b3f7fda42a393c9c2d570d1e0ec44fb955e8375c33d7fb67026331066b space-bridge/src/service_credentials.rs
T fa904335c1f015b00be9aea523e9e19ceaaf949b4587b5e1bbb57f2a7588df68 space-bridge/src/state.rs
T 90ce43bb78951c065d896c671de47300e5abbf765a951b4f888522345dfa735c space-bridge/static/index.html
T 3ae990adcbc35d84b5222e93f225fcf30e46a8744986cd71553a6c4271569cf8 space-bridge/static/world.html
? 7a9d27328a193f8df122899da07157b1066e32d27a8c76fd1023bb13a0f301e8 space-bridge/tests/full_runtime_security_e2e.rs
? 54bc5a2412e103884a031c39cc117cc95531e7c3e5f7bd0214f2de8a81eb20ea space-bridge/tests/i5_runtime.rs
T 59eabff4aa24c9e8773da2d478c3649ff0ac79791b2b43508101e29bac41f449 space-bridge/tests/load_path.rs
T b19956cae180619e087412f2629095b3688d1f7a87e3e4e689c0a54080c45319 space-bridge/tests/mcp_client.rs
T 7c60053e8f59f7e800155a0a69f38f072ced0959b6fc661b016badbe2d4f5716 space-bridge/tests/mcp_e2e.rs
T 4c40286c21424ff502927e399bf206bb3b771f1f656b7bfc26917cd55e970dcf space-bridge/tests/mcp_stdio.rs
T 1c785ecd2bb4ee36bf68e04e3d7446249b92a92b77a3abe4839d291a76c1b2fe space-bridge/tests/mission_workflow.rs
? a25e6b7eef548786032ed8e6be4dec5f04b73125e865c0a824a2e96a67016e0e space-bridge/tests/process_registration_e2e.rs
? 1f2f2163a69e0ade0c0e3a7ee0ea33b5495b772661298146758f1c192505bc57 space-bridge/tests/semantic_http_e2e.rs
T 2d365f2b460bec06a7f9bb0d46c69e55fe1c6868bb34542fa0537e58b700caa2 space-bridge/tests/static_ui.rs
T 7c243f280a5475a31b66c6235f2a035cd74bee2cc6263ca62fa157d8179ea03f space-bridge/tests/support/global_lock.rs
T 9366475092633b7b8291edd90f421328a8c442eac37cb5434bbd3d5fe8c89c0f space-bridge/tests/vertical_slice.rs
T a81fca4268699c1b7aad5d20c36291de2e24eda87c8f7a001da05311f29a4c05 space-core/Cargo.toml
T a0cfb3539aef7d08b02bd72399ca6b86627633f799912d40f8aed4ffde9a3c23 space-core/src/action.rs
? 66d27084888a76cb8b2d311c9f41573d5d380d1c50baa60eb62a78aa6360499b space-core/src/action_storage.rs
? 015ee1ca2f93bf10021683cf7b6c1c66907425158e0e46ed8df26167b706bfa4 space-core/src/browser_actions.rs
? 25cccee5fc91b12fc6734c1890bb874bceeba7177f2bb91cd921809d95f8f6ba space-core/src/dispatch.rs
T 719cdc9fa1c5a570489f02f6d7e20c8cb35a3bfd58d7a019c3ce3d33960a5f39 space-core/src/event.rs
T 574bf9d79014050deb42124257d5a8e6f573a455db7c05f16f0d4296d0a3f1fe space-core/src/ipc.rs
T 122272e557a0387363e253f9de7978c5f562cbf4ee76862416f8f54268f49c7c space-core/src/lib.rs
T 078422f73d2a9b11d268b15b72d32ea8cb36315e9c5bcfced2e7ff8a5f9ea1de space-core/src/main.rs
? e7fd74c9335bdaa9b5e9a46ae57db75ddfc3506cc6ff9b98742774b543276eb2 space-core/src/mission.rs
? cf7ecd7fc4fda53cbaf53a11dc8176b5b98f4db0648ae1acc1dcc53b3526a81f space-core/src/mission_storage.rs
T 4d3cf714ad7ca4532084a45697e0882c4a6992b44994f8a6ecd2159334512690 space-core/src/object.rs
T 5e438c124df2f3e20f49074df4040125a36dfcf82d804fcf4eb25ffe176f4b37 space-core/src/observation.rs
T bf85fa38d5659a7af205c61681d7edfae7386eef7f17d36bc4e1c0babf32ec34 space-core/src/policy.rs
? 70d295775c6655c5d025de5c0ad82aea04a389c6b6f51139f54ca0a35ee19ad0 space-core/src/principal.rs
T bde1571c35f97faab13757a7a6fb27905e48a404e7e3b08cd484a4a34b66efda space-core/src/provider.rs
? 024d7dcee4a2e00a540e8a6720204d5df0934573935be64ad1e898838fd5b55e space-core/src/provider_observation.rs
T 77617b3fc23c0cfe9211b684175629d198da600183462d121fb98b5c8bbe59e0 space-core/src/query.rs
T 2dc1c8ccf18aeddbe4916c38ca9ae4911e47a51aa8316c5eca27987c540d67f7 space-core/src/relation.rs
? dd902a2e3e3ff689629f8401199531606ec3eaa1a32d47aea899fea2cdd34d9a space-core/src/security_config.rs
? 824095efed6ed8f51dd4cbffc49303c4af5b4552283f5f9fdbc9e561827e1dc8 space-core/src/semantic.rs
? 79b3736c719ce1141fafb46c12ee88fd26b1b0cf8ae91ef2f2eca87ec85b2e5e space-core/src/semantic_read.rs
? 568b0b4080e01f2323166656cfefd3e9fbba1c54b154900a699d57da1057b8b0 space-core/src/service_registration.rs
T eb6eea5279c533e11c5fdabeee68b6ece03903b2c9aed0243901c273c22b61ff space-core/src/space.rs
T a84fcb7e41ef4bc4a8a12a779fa613a3abda5a182296f6cae9f045021c0ae83c space-core/src/storage.rs
? a62f3e78a6f022c0234e96b7ff3777ace0513aca2b78fe9950ae87aee3130f43 space-core/src/storage_browser_observation_tests.rs
? 10adac695a5d61718e6b44b145a183f0923dcf6425243615d8e216c74ac65f32 space-core/src/storage_file_admission_tests.rs
T 672fb302b91f8b8fe8a396aed6d949553b72ca85060f7136a9fe9e935365c28d space-core/src/subscription.rs
T ee8efa0c5af1cf0643e5181b620fc769bd4b7bb6acda8e4fc0b1cc4e8ba8e8e1 space-core/tests/batch_experiment.rs
T cc17ea500b0f168ce27c29a2f5e0d61b887e929a5cdbef0083a87ceb73940d27 space-core/tests/break_b2_growth.rs
T 248f07b2c34f4a70456f27d08e7e808744a6eb2c0fafaf40cf4685377463178c space-core/tests/break_b3_concurrency.rs
T d8caf2d06618cd431530a38587ca8de6da69092232298945ffe5421776999324 space-core/tests/break_b5_cascade.rs
T c33abbfef88e22e1e67f9717e4f7be54c17ff85395442955d6694fd3a978f0d3 space-core/tests/break_b6_worlds.rs
T 9782a1d711d11de49000d658c5e3f936d874015934005de7bc36a15a646374eb space-core/tests/break_b7_actions.rs
T 4272015e45db4ace42ec5a7c704b28683118c9507a62d4d518acd6102f8b0b87 space-core/tests/break_b8_b10.rs
T babb6d0eaee410d8b943a3cf3c3c2000c0fedfe72f5e8ed66a37e9dd2c6708d3 space-core/tests/break_b9_b13.rs
T bf854bc8d9986d11e771fbaa9d8546667cab4d84bed17e03454be55b7542a3fa space-core/tests/crash_t123.rs
T 16b327e2df3a3443af214794316566c88df1fe7e67f6d3cc5e537516c85efdab space-core/tests/crash_t45.rs
T a0bc73d9c006d79b3a31454abb816dbe453f5a460559098a118aa84e564d0419 space-core/tests/crash_t6.rs
T 1b7627fe35777dc49f99278dac25ed0d6498506b892113d9bc41ac8dcf14abbe space-core/tests/crash_t9.rs
? 937bc368be216e8ae2ac18c81bbc2e3f18c75c046645c3912f249ac8ee7e7106 space-core/tests/durable_file_provenance.rs
T 30204c5e888f36838fe87ba4c420abcd746e4f80dd2c18d596f50fdda67e7ee9 space-core/tests/extended_x2.rs
T 69976e40b75f969d62a4685ee42d2f71d41884bd90d93c16db6eeca6a2e33a05 space-core/tests/extended_x3.rs
T 7c07a68973cea40bd381f393d7843267e9fdfeb30345b0cfc571f6f67bc136b4 space-core/tests/extended_x4.rs
T 8d76fc6b0a4462030b40580ad2a15ca20d974a9de26cda92a13e0963f95312a1 space-core/tests/extended_x5.rs
T d7e1c51a84ddec22e650540bb6240623795ac72ccf77b9ccaf7cc8f78f0a52ae space-core/tests/extended_x6.rs
T dd5a1055b059d96d5ae554078cdf7ff74589517eb8d6cb7210a21c9301296061 space-core/tests/extended_x7.rs
T 64ac89317183e8a622453d3ba69998db5136aef686ebc5c9199f9fb008cfd6c1 space-core/tests/extended_x8.rs
T 0f989d36b672011a8fd6600a8e5d8100aa0a74d90b6b856b82904b12433cd50b space-core/tests/extended_x9.rs
T 0dbf913ba901ce867b97421ad98647943b4548b108af739b0de1776b59150135 space-core/tests/foundation_m1.rs
T 77712eff89d3aedee2d160f64ca3973b0adc5a130239e5305295e346dc59bcc2 space-core/tests/foundation_m2.rs
T 98e5e5bc660aa096c189f3bee769cf66b1dd6b7f28d7626dcd1a41dc8ac92e9d space-core/tests/foundation_m3.rs
T 2f6ab1e41e4bb132f2134c71244c16b7a8dad924f26db9c707dd5960bcb51c9b space-core/tests/foundation_m4.rs
T 257bc2cb1bbf0b574dc431044008a308729f95286a7c8cb347d6d5d37d6ecc22 space-core/tests/foundation_m6.rs
T 8693e0ba336e69cbe5f52b65001b8bf40df6ac87322e8529633e2376bdec5191 space-core/tests/foundation_m7.rs
T 16346625efbcc40892544503dd3335e42aaafb2745d8cf137a99cc9df0abfba4 space-core/tests/g7_core_restart.rs
T 61a2c02b0b7c36012790ac3b7fb6d7e18cdcb0b1b52a08e624c8d2218cca7334 space-core/tests/phase_b.rs
T de36444352d1c42d3f264866e044aa420822c676f56c7e92f34992220c019ac3 space-core/tests/qual_gate2.rs
T d4a6c752e0324048d216e08bc48efdb9aa9b9c1951ad328f6809a159cfd9eafd space-core/tests/qual_gate3.rs
T 65e913103e6ed2a756cb008151bd236bc7a83a88386ae9469e80ef1be6045c2b space-core/tests/qual_matrix.rs
T b1ac47db6e665c6811ddb0f40bc51566b39639a95cbda5518d4825fb93084bcc space-core/tests/qual_s1.rs
? 1d75044612052e649a201ea4cffd01a0d77b0f508b7f626cfa0a9a843101e8b7 space-core/tests/security_verification.rs
? f44276970c9d814634ad4f9ff562303770b84ae76cae19386f628c3cade390f4 space-core/tests/semantic_api.rs
? b5c46661b98a6cef00ac588ad72745572fe131f5d7c55df9a14cf11190a51ee3 space-core/tests/semantic_read.rs
? 9b9d44be974f62beca737f46f19e240a8968a22286d57f9ea74efd328163bd66 space-core/tests/semantic_read_hardening.rs
? 6b30afa48f002e6ad469ca3d898bd8cd0d12369cc85786fb856ffb1a8b984e69 space-core/tests/semantic_read_performance.rs
T e2eff09880a631720e3abe5b2a49cfb74e3b0bb1f82cbc0d6d97fa8454fdd7ec space-core/tests/spacetrace_ipc.rs
T 89188ef0d3af55caf373b5ff6fca2251d413d2afd0f90382082ddf1ad3acdd31 space-core/tests/spacetrace_overhead.rs
T 416d9679579b45fe8d8fa5a7ce9cb1229b49c52135a482f53f0ab18c47dee4b7 space-core/tests/spacetrace_slice3.rs
? 1994f6d1116944171882f7dab21046c8ef33f0f860e76a827687026300958712 space-core/tests/stage4_2_identity.rs
? 48ca9522d3919d1713f2ab474b4ffe4d9a3d66e06259cb055d26f40ffd46d3e7 space-core/tests/stage4_3a2_identity.rs
? d25b49aeb6593800636e8f6371a4ad29a0df43ebb555b9c3c2037f3ae4543b85 space-core/tests/stage4_3a3_registration.rs
T b60ec4852c4de240654d61ff1d8822c410981e18c77e95514c9f095acaaf8dd8 space-core/tests/support/crash_util.rs
T 7c243f280a5475a31b66c6235f2a035cd74bee2cc6263ca62fa157d8179ea03f space-core/tests/support/global_lock.rs
T c6b1ef624a69cb75a6d2d28a3f0dc237cb540f8dc40c62ee8b2b1369b30f0ba0 space-core/tests/t2t3_attribution.rs
T 2a15b92d0c15822f35ced130705b7028f4f7758eb5cb45bcb6bad4b5b228d6d1 space-core/tests/v11_isolation.rs
T 04946772f4271db58533a24ac086e609872a7c2e860f37db0331ad01fefbde48 space-core/tests/v12_policy.rs
T fa59891b921ef47341d656287000ed5e2e9b4d5c12ace78b34b2719940a134ac space-core/tests/v13_trust.rs
T 32b5ea9b6c9c5aa289900d524085df6926664a4482c4bf55cb604c0dc1f304eb space-core/tests/v14_measure.rs
T 9f01b1971a0b3a2eaf318bc395912f4cbdf75cabcfe465b7c50f5065d577769a space-core/tests/v14_reactivity.rs
T f16420192844290788d38bc9866d2507aab70d913344ad89871ae0493f6cebf0 space-core/tests/v14_subscriptions.rs
T 89db41167ba2ce58c2ff025e050e29c583779903c0d24655c40b544a40e21478 space-core/tests/v15_crash_core.rs
T e15a37169665d1137970c98143879ccdd6c5491d40a92057871fe2a6e65291ea space-core/tests/v16_deletion_core.rs
T 8d81e2cb6aa0ae93150e0b01f8f59a2985fc039670391051e0bd55009d25bef7 space-core/tests/v17_actions.rs
T b65931f7dab5af3c83179ba6a59ecbc6d7915ee6b815d6886ea082e06a097a00 space-core/tests/v17_agent.rs
T 7bee5f6f07e5d7a758d8ac1f23b21b4593fb2e127105741639a17586b721343d space-core/tests/v17_checker.rs
T f7509b3fdd857332667203743ab9056038fb8745fb47018e3e7fc4d613d57127 space-core/tests/v17_matrix.rs
T e2906cb49b20b4eeaf766b98096c2e4ed01254b530f24809c02eea1a36e06caa space-core/tests/v17_perf.rs
T fbad15a69905acbfe62610f91205802c4ebd5dbcae18f7ea84750d2803af3026 space-core/tests/v1_world.rs
T bc9bfce33a4830551ea6f2f125ad47307db4e05f9204382cc94786ad57ae84e1 space-core/tests/wal_experiment.rs
T 1c45542a02abeb48d8570c4c6c55830f418fbfd912e48cc518b639a5033298cf spacetrace/Cargo.toml
T 235a9e3f0ad0bcdd6b49d394e52621c4a24c204810d15779b62eb47d591aac28 spacetrace/src/buffer.rs
T 96218d4153d6cd3cbe4fcdbc9c82cb3e5f1a19985120db51355004efc05290fc spacetrace/src/correlation.rs
T ef463d3b1dd397858ab7d417545de3c8b52a17751db7641ca45ce9164d6e6655 spacetrace/src/etw.rs
T b0f4a9d3a47006418c5a29b3103202485a60ae6a59588ef0ae1d39316b004eff spacetrace/src/hooks.rs
T a695e96416be2167fb17bc860256549f19612d26aa92de5933b6a19f690521d0 spacetrace/src/lib.rs
T 314d60e9845753451ab58f3b4368f200ac1b69a0d2569a57ad07ebc699c0a158 spacetrace/src/provider.rs
T 071f944352f0f6740c227a0dbfc4d7d93ebb8384a3430661822a8baaf17b7925 spacetrace/src/trace_event.rs
T cd31b742e11282e6b56445a2bab1102dbe5fd8283fd7baed7397ea84edd2ab25 spacetrace/src/verdict.rs
T 2328fd8d2bd6b8dd6774ba875de7ba1d115d86a04e6c1b75a97dc078fd2fd1b5 spacetrace/tests/breakage.rs
T 1094ae5f7b766b5401efe06706037ae176ed57da63ad627de5a923d11e06b68b spacetrace/tests/etw.rs
T 2050aea03c51e6af2db5987b22128acdcea7263bd349a576ab41ccf1126645b6 spacetrace/tests/support/global_lock.rs
T 33f8342a42e8493a01f20489a21e26f31d8bd0ce7ef05d5927f5ac79fc00f8ef spacetrace/tests/verdict.rs
T d97f297203c30469cc7d18a656b54ae5039f9be7ef0c0d798e25d1a394da1a36 spacetrace/tests/vertical_slice.rs
```

### B. Exact operational/test-harness support sources

Keep visible, review before staging or execution. These include deployment tooling as source documentation, NOT permission to run it. Current mock-contract dependencies are included explicitly; no generated profile/config is included.

```text
build_all.bat
build_all.ps1
check.bat
check_mcp_tools.ps1
diagnose.bat
diagnose.ps1
launch.ps1
mock/contract_server.js
mock/proxy.js
run_cargo_check4.bat
run_cargo_check_wry.bat
run_diagnose.bat
run_tests.ps1
scripts/Deploy-Stage2B3.ps1
scripts/Inspect-BackupSqlite.cjs
scripts/Prepare-WorldRollback.ps1
scripts/Read-SpaceOSEnvironment.ps1
scripts/Verify-WorldRollback.cjs
scripts/browser-observation-probe.mjs
scripts/browser-observation-real-test.mjs
stage.ps1
stop_spaceos.ps1
test_browser_provider.ps1
test_filesystem_provider.ps1
tests/action_v5_audit.cjs
tests/action_v5_runtime.cjs
tests/attempt_binding.cjs
tests/authorization-positive.cjs
tests/check_consolidated_mvp.cjs
tests/check_full_vertical_slice.cjs
tests/check_full_vertical_slice_ii.cjs
tests/check_full_vertical_slice_ii_http.cjs
tests/consolidated_mvp_smoke.cjs
tests/contract_tests.js
tests/full_vertical_slice.cjs
tests/full_vertical_slice_ii.cjs
tests/mission_verification_ui.cjs
tests/scoped_browser_actions.cjs
tests/scoped_browser_audit.cjs
tests/scoped_browser_recovery_audit.cjs
tests/scoped_browser_recovery_checks.cjs
tests/scoped_browser_recovery_ui.cjs
tests/services.js
tests/vertical_slice_ii_corrections_audit.cjs
tests/vertical_slice_ii_corrections_runtime.cjs
tests/vertical_slice_ii_ui.cjs
verify_spaceos.ps1
```

The untracked support files above are legitimate source candidates, not generated artifacts. Root `fix_test.py` and `fix_test2.py` are one-off source-rewriting utilities, not required build/runtime/test dependencies found here: preserve, but do not automatically include/run them. `UI_custom/`, `reference_ui/`, `spaceosprompt/` and `ui-night-vision.md` remain independent owner WIP, not silently classified as release dependencies.

### C. Documentation candidate manifest

Preserve existing tracked baseline documentation. The following exact non-archival docs Markdown paths remain visible for individual owner review, plus this new report and `.gitignore`. Maintenance reports are not broadly ignored. This is not secret-clearance of every report attachment.

```text
.gitignore
docs/ACCEPTANCE.md
docs/ACCEPTANCE_LOG.md
docs/AGENT_GATEWAY.md
docs/ARCHITECTURE.md
docs/ARCHITECTURE_DECISIONS.md
docs/BREAKAGE_CAMPAIGN.md
docs/CURRENT_REALITY_REPORT.md
docs/CURRENT_SYSTEM_MAP.md
docs/DOCS_STATE.md
docs/FOUNDATION_CONTRACTS.md
docs/FOUNDATION_GATE.md
docs/MCP_SETUP.md
docs/MIGRATION_AUDIT_PLAN.md
docs/NIGHT_SHIFT_REPORT.md
docs/PRODUCT_VERTICAL_SLICE.md
docs/PROJECT_STAGE_ASSESSMENT.md
docs/PROJECT_STATE_AND_NEXT_STAGE.md
docs/PROJECT_STATUS.md
docs/README.md
docs/RESULTS.md
docs/ROADMAP.md
docs/SPACEOS_FULL_REVIEW.md
docs/SPACEOS_READINESS_AUDIT.md
docs/SPACEOS_night.md
docs/SPACETRACE.md
docs/SPACETRACE_ARCHITECTURE.md
docs/SPACETRACE_AUDIT.md
docs/SPACETRACE_BREAKAGE.md
docs/SPACETRACE_ETW_ACCEPTANCE.md
docs/SPACETRACE_ETW_CORRELATION.md
docs/SPACETRACE_INTEGRATION_GATE.md
docs/SPACETRACE_RESULTS.md
docs/STAGE_2B_3_DEPLOYMENT.md
docs/STAGE_2B_3_DEPLOYMENT_ATTEMPT.md
docs/STAGE_2B_3_PREFLIGHT.md
docs/STAGE_2B_3_READINESS.md
docs/STAGE_2B_3_VERIFICATION.md
docs/T2T3_ATTRIBUTION.md
docs/T2T3_FULL_RATE_SENSITIVITY.md
docs/T2T3_RATE_MATCHED_SYNC_ISOLATION.md
docs/T2T3_SYNC_ISOLATION.md
docs/T2T3_WARMED_SYNC_ISOLATION.md
docs/TESTS.md
docs/V0_RESULTS.md
docs/V1_1_RESULTS.md
docs/V1_2_RESULTS.md
docs/V1_3_RESULTS.md
docs/V1_4_RESULTS.md
docs/V1_5_RESULTS.md
docs/V1_6_RESULTS.md
docs/V1_7_FOUNDATION_HARDENING.md
docs/V1_8_WAL_RESULTS.md
docs/V1_9_BATCH_RESULTS.md
docs/V1_RESULTS.md
docs/V1_SPEC.md
docs/VERDICT_RULES.md
docs/VERIFICATION_SLICES.md
docs/WORLD_BOUNDARY_AUDIT.md
docs/maintenance/SPACEOS_ASYNC_COMPLETION_AND_DISPATCH_FINAL.md
docs/maintenance/SPACEOS_BROWSER_OBSERVATION_FINAL_DESIGN.md
docs/maintenance/SPACEOS_BROWSER_OBSERVATION_IMPLEMENTATION.md
docs/maintenance/SPACEOS_COMPLETE_FIRST_REAL_MISSION.md
docs/maintenance/SPACEOS_CONSOLIDATED_MVP_SMOKE.md
docs/maintenance/SPACEOS_CONSOLIDATED_SOURCE_MANIFEST.md
docs/maintenance/SPACEOS_CONSOLIDATION_EXECUTION.md
docs/maintenance/SPACEOS_CONSOLIDATION_FINAL_REPORT.md
docs/maintenance/SPACEOS_CONSOLIDATION_PLAN.md
docs/maintenance/SPACEOS_CONSOLIDATION_TEST_RESULTS.md
docs/maintenance/SPACEOS_DISK_INVENTORY.md
docs/maintenance/SPACEOS_DISPATCH_CREDENTIAL_BOUNDARY_FINAL.md
docs/maintenance/SPACEOS_DURABLE_FILE_PROVENANCE_IMPLEMENTATION.md
docs/maintenance/SPACEOS_DURABLE_PROVIDER_PROVENANCE_DESIGN.md
docs/maintenance/SPACEOS_FILE_CHANGED_IMPLEMENTATION.md
docs/maintenance/SPACEOS_FILE_CHANGED_IMPLEMENTATION_FINAL.md
docs/maintenance/SPACEOS_FINAL_CLEANUP_APPROVAL.md
docs/maintenance/SPACEOS_FIRST_REAL_LOCAL_MODEL_MISSION.md
docs/maintenance/SPACEOS_FULL_VERTICAL_SLICE.md
docs/maintenance/SPACEOS_FULL_VERTICAL_SLICE_EVIDENCE.md
docs/maintenance/SPACEOS_FULL_VERTICAL_SLICE_GAPS.md
docs/maintenance/SPACEOS_FULL_VERTICAL_SLICE_II.md
docs/maintenance/SPACEOS_FULL_VERTICAL_SLICE_II_EVIDENCE.md
docs/maintenance/SPACEOS_FULL_VERTICAL_SLICE_II_GAPS.md
docs/maintenance/SPACEOS_LAB_CLEANUP_PLAN.md
docs/maintenance/SPACEOS_LAB_DELETION_CANDIDATES.md
docs/maintenance/SPACEOS_LOCAL_MODEL_READINESS.md
docs/maintenance/SPACEOS_LOCAL_MODEL_VALIDATION.md
docs/maintenance/SPACEOS_MISSION_VERIFICATION_UI_CORRECTION.md
docs/maintenance/SPACEOS_MVP_GAP_CONTRACT_ANALYSIS.md
docs/maintenance/SPACEOS_POST_SLICE_II_REVIEW.md
docs/maintenance/SPACEOS_PROVIDER_FILE_RELATIONS_FINAL.md
docs/maintenance/SPACEOS_PROVIDER_FILE_RELATION_IMPLEMENTATION.md
docs/maintenance/SPACEOS_REMAINING_GAPS.md
docs/maintenance/SPACEOS_ROOTS_GATE_DIAGNOSIS.md
docs/maintenance/SPACEOS_ROOTS_GATE_FIX.md
docs/maintenance/SPACEOS_SCOPED_BROWSER_ACTION_DESIGN.md
docs/maintenance/SPACEOS_SCOPED_BROWSER_ACTION_IMPLEMENTATION.md
docs/maintenance/SPACEOS_SCOPED_BROWSER_RECOVERY_FINAL.md
docs/maintenance/SPACEOS_SOURCE_ARTIFACT_ISOLATION_FINAL.md
docs/maintenance/SPACEOS_SOURCE_DIFF_MANIFEST.md
docs/maintenance/SPACEOS_SQLITE_V5_ACTION_COMPLETION_FINAL.md
docs/maintenance/SPACEOS_VERTICAL_SLICE_II_CORRECTIONS.md
docs/maintenance/SPACEOS_WORLD_BROWSER_ACTION_E2E.md
docs/opencode-spaceos.md
```

Root `VERTICAL_SLICE_REPORT.md` and other root prose/design files are preserved auxiliary owner-review candidates, not required compiler inputs. Historical `docs/maintenance/consolidation-source-preservation/`, diffs, JSON inventories, logs and screenshots are an **evidence archive**, not a second active source tree. They remain untouched and visible but are not recursively included by this proposal.

### D. Historical executable utilities — explicit quarantine-by-procedure

These preserved files are source-form historical tools, not active Cargo dependencies. Their sibling paths and fixed destinations require separate review before inclusion/execution; no new ignore rule hides them:

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

### E. Must NOT enter the source package as generated/private material

- All `test-tmp/` content: fixture.secret.json, child/provider/attempt configuration, login proofs, synthetic credentials/tokens, Chrome profiles/cookies, DB/WAL/SHM/journals, gateway results/prompts and raw logs. Synthetic does not mean safe as an automatic production default.
- Runtime databases/auxiliaries, tokens and private JSON/configuration; personal/local `opencode.json` pending sanitization review. No credentials were inspected to approve it.
- `target/`, staging targets, compiled executables/rlibs/pdbs, incremental caches, local crash dumps, data/profile/log directories.
- Raw evidence attachments or configuration copies containing session cookies, bearer credentials, private prompts or personal source paths. Public evidence requires explicit content review; ignoring test state is not universal secret detection.
- Archived source before-images must not replace active source inputs. Reviewed sanitized historical logs/images may be included separately as evidence, never by an indiscriminate repository archive.

## 7. Executed verification commands and results

All commands were diagnostic/read-only except the two authorized file edits through patching. No `git add` (including dry-run), reset, checkout, clean, archive/package, runtime launcher, test suite or rebuild was invoked.

```powershell
git status --porcelain=v1
git status --porcelain=v1 --untracked-files=no
git ls-files -z
git ls-files --others --exclude-standard -z
git ls-files runtime test-tmp data chrome chrome-profile logs
git ls-files --others --ignored --exclude-standard runtime
git diff --cached --stat
git diff --cached --name-only
git log -1 --format="%H %s"
git branch --show-current
cargo metadata --offline --locked --no-deps --format-version 1
# Node read-only aggregation of Git path lists, SHA-256, target/module existence.
# Node feeds manifest paths to:
git check-ignore --no-index --stdin
# Node feeds representative artifact paths to:
git check-ignore --no-index -v --stdin
# Source-only rg inspection of module declarations and filesystem writes.
git diff --check -- .gitignore
```

Representative actual and hypothetical future artifact checks (hypothetical paths were NOT created):

```text
.gitignore:12:/test-tmp/	test-tmp/vertical-corrections-vcy8HG/fixture.secret.json
.gitignore:12:/test-tmp/	test-tmp/vertical-corrections-vcy8HG/core.db
.gitignore:12:/test-tmp/	test-tmp/future-run/core.db-wal
.gitignore:12:/test-tmp/	test-tmp/future-run/chrome-profile/Local State
.gitignore:12:/test-tmp/	test-tmp/future-run/trace.log
.gitignore:23:core.db	runtime/core.db
.gitignore:25:core.db-wal	runtime/core.db-wal
.gitignore:24:core.db-shm	runtime/core.db-shm
.gitignore:9:runtime/*.db-journal	runtime/core.db-journal
.gitignore:6:runtime/*.token	runtime/space-core.token
.gitignore:26:/core.db-journal	core.db-journal
.gitignore:1:target/	target/debug/space-core.exe
.gitignore:20:target-agent-staging/	target-agent-staging/debug/space-core.exe
```

Source/manifest check: zero ignored paths. The final batched check covered 335 build/source/harness/documentation paths and returned exit 1 with empty output (Git's expected result when none is ignored). All 8,656 actual test-state files are returned by `git ls-files --others --ignored --exclude-standard -z -- test-tmp`; zero test-state paths remain visible as ordinary untracked files. Final Git state is 97 collapsed status entries, 896 visible untracked paths, 40 tracked modifications, zero staged changes. `git diff --check -- .gitignore` passed.

After report creation, the 1,130-file byte-hash aggregate, test-state metadata digest and Git index hash still exactly match the before-edit snapshot. All fixed historical JSON current hashes above are unchanged. Runtime Rust sources/manifests, scripts, tests and maintenance Markdown remain visible. No production endpoint/process/database or sibling-laboratory contents were accessed.

Tests were unnecessary for verifying the three deterministic corrected writer paths and would risk unrelated fixed report destinations; previous report results are referenced, not relabelled as executed here. Cargo metadata validates target discovery, not runtime behavior.

## 8. Remaining risks and outcome

1. **BLOCKING for a claim of universal evidence-writer isolation:** remaining fixed report/screenshot writers need separately approved corrections; do not rerun them blindly.
2. **BLOCKING for tracked-only source packaging:** required untracked security/Mission/action source and tests must be explicitly reviewed/included. This report is the proposed exact inclusion list, not a completed commit/package.
3. **Owner security gate:** tracked local OpenCode configuration, historical attachments and arbitrary archive tools are not covered by Gitignore. No comprehensive secret-clearance or production package is claimed.
4. Historical exact bytes for the three overwritten JSON summaries remain unavailable here. Current integrity does not repair past history. Record the exception in any later release evidence.
5. Ignore rules are preventive defaults, not enforcement against forced staging or execution. Historical utilities can write outside this repository if deliberately run; none was run.

| Gate | Result |
|---|---|
| Required source/module/target inventory | VERIFIED — 201 exact build/test/static inputs; required untracked files visible |
| Normal Git exclusion of test state / SQLite companions | PASS |
| Current WIP, existing evidence and Git index preservation | PASS within stated byte-hash/metadata scope |
| Three corrected Rust evidence writers | VERIFIED BY SOURCE; not rerun |
| All executable writers isolated from historical destinations | NOT MET — confirmed remaining writers |
| Overall isolation gate | PARTIAL — no blanket source-package/staging approval |
| Production / sibling laboratories | UNTOUCHED |
| Staging / commit / push / deployment | NOT PERFORMED |

Next owner decision: authorize a separate bounded evidence-writer isolation pass, then review the explicit staging manifest and configuration sanitization. No source-code correction or additional authority change was assumed.
