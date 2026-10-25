# Consolidated real MVP smoke

Date: 2026-10-24. Result: PASS for one synthetic-world, actual local-model Mission in the canonical candidate. Not production E2E.

## Actual runtime and evidence

Main Cargo output: C:\spaceos\target\debug. Core/Bridge test adapters are feature-gated mvp-core-fixture.exe and mvp-runtime-fixture.exe. MCP is the bounded Cargo spaceos-mcp.exe. Fresh state: C:\spaceos\test-tmp\consolidated-mvp-9u4ZUx\core.db. HTTP: 127.0.0.1:21973; unique configured Core/provider pipe names. No production HTTP/CDP endpoint was used by this smoke.

Existing installed OpenCode 2.1.3 PID 25196 was used with its supported API and normal configured local provider. No new profile, credential copy, auth-store export, model download, provider change or model fallback. Unauthenticated GET /v1/models returned 401; evidence: consolidation-smoke-direct-catalog-401.json. That does not imply the configured OpenCode provider cannot authenticate. The actual supported catalog and successful execution establish the selected model.

| Correlation | Actual value |
|---|---|
| Model | unsloth/qwen-q8 |
| World | e41131dc-318a-4c04-8c56-3de5f322b873 |
| Mission | 4ce18f8d-5663-455d-bca0-22a59d956953 |
| Attempt | 5acccad0-a15d-4b1a-ab2c-f0e7f4d603bc |
| OpenCode session | ses_e9291ece2ffey5Qq2rObihp6lQ |
| Agent | canonical-mvp-agent |
| Existing Task | 956e297d-16b5-43cb-a426-7f50305a2515 |
| Summary object | 736f1720-2803-48dd-ab2e-a7120803c44a |
| Existing Evidence | 3d6eb259-eb2b-487d-ba15-6addd8082637 |
| Existing related_to | ee507066-401c-4d06-98d5-4d08cb2c6a92 |

Synthetic setup seeded a Task, Evidence, Summary, relationship and foreign guard through the fixture/Core contract. The evidence object's PASS is synthetic input, not a fabricated external test result. Setup is distinct from model-originated execution.

## Real UI and execution

Opened World from the actual canonical Bridge. World +Mission opened its existing Shell Mission UI. Entered the goal, selected unsloth/qwen-q8 explicitly and clicked Execute once. No scripted replacement for the agent.

Actual model-originated MCP sequence, all accepted by Core:

1. spaceos_query_objects
2. spaceos_query_relations
3. spaceos_patch_object — existing Task unfinished -> completed; records reviewed_evidence_id from the returned relationship.
4. spaceos_patch_object — existing Summary updated with meaningful evidence-bound progress.

Persisted summary: “Task completed. Reviewed against Evidence ID: 3d6eb259-eb2b-487d-ba15-6addd8082637.”

The agent had only semantic.read and semantic.object.patch in the exact synthetic World. Four admitted MCP calls out of 30; 600-second maximum retained. Attempt/session/World/model binding persisted; the foreign guard remained unchanged. No automatic retry or model substitution.

Executor succeeded. Core lifecycle entered verifying at 2026-10-24T10:21:51.226903200Z, event sequence 18. The real verification dialog displayed the exact Mission and attempt; nonempty evidence was entered and Confirm clicked once. Core then completed the Mission through its existing commands. The successful executor alone did not bypass verification. The final assistant result was present in this run.

## Persistence and representations

Independent readonly check: 15 checks before explicit verification; 18 after completion and real Core/Bridge restart. Actual restart recorded 2026-10-24T10:23:46.830Z. Objects, relations and events matched across restart; SQLite integrity_check = ok. World and Shell both displayed the same authoritative completed state after reopening/authentication.

Final check evidence timestamp is 2026-10-24T14:08:42.546Z (a later reread after the live runtime had stopped). Do not infer model runtime duration from this later audit timestamp. Owned runtime cleanup was recorded at 2026-10-24T10:49:26.108Z; coreStopped=true and bridgeStopped=true. The harness evidence's startup label READY_FOR_ONE_WORLD_MISSION is stale; authoritative outcome is the verified SQLite/Core state and final-check verdict, not that label.

Space, Tree, Graph and Focus were visually inspected. Graph displayed the actual related_to edge. Task properties showed completed plus the evidence ID. Create/Edit/Relation dialogs were opened and cancelled; no extra UI mutation is claimed from those dialog inspections. Their HTTP/Core mutations are independently covered by semantic_http_e2e.

## Evidence files in this directory

- consolidated-mvp-smoke-evidence.json: setup, actual Gateway/MCP responses, Core/SQLite snapshots, processes and binary hashes, restart and cleanup.
- consolidated-mvp-before-verification.json / consolidated-mvp-final-check.json: exact checks and authoritative correlation.
- consolidated-mvp-ui-evidence.json: actual UI states and console entries.
- CONSOLIDATED_MVP_MODEL_SELECTED.png / VERIFICATION.png / COMPLETED_SHELL.png / COMPLETED_WORLD.png / RESTART_SHELL.png / RESTART_WORLD.png (each has CONSOLIDATED_MVP_ prefix).
- CONSOLIDATED_SPACE.png, TREE.png, GRAPH.png, FOCUS.png, TASK_EDITOR.png, RELATION_EDITOR.png, OBJECT_CREATE.png (each has CONSOLIDATED_ prefix).

## Limits

Providers were intentionally absent from this model smoke; provider PASS belongs only to the separate real four-process regression. World console entries were empty. Shell popup captured two Electron sandbox/preload errors; recorded, not silently classified as an error-free browser. Application execution and final states still succeeded.

The popup initially loaded a cached older bare index.html. A versioned laboratory URL loaded the current asset. Cache-busting/release asset versioning remains unresolved. World metadata sometimes renders World as “—” despite correct Core scope. Object naming falls back to type/ID for properties.title. Optional-provider footer status can change between Filesystem unavailable and Roots —; this does not mean Core is down. These limitations are not redesigned here.

No second Mission confirmation occurred in this fresh-world run. The earlier historical extra confirmation's exact cause remains unknown; the new run is not retroactive proof of that cause. No seamless recovery of an in-flight agent is claimed; this restart was after terminal completion.
