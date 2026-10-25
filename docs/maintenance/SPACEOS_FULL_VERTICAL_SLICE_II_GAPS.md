# Full vertical slice II — remaining MVP integration gaps

Date:2026-10-24. Overall **PARTIAL**. This stage verifies rather than changes production contracts. The three earlier Core provider admission/reference gaps no longer reproduce: FileChanged, BrowserTabCreated and Task → admitted File now work. The following actual integration issues remain.

## 1. Confirmed Bridge WS filter omits the new provider events

File: space-bridge/src/event_stream.rs. Function: EventPump::poll. Exact condition:

```rust
if (event.payload.get("semantic_operation").is_some() || event.event_type == "FileCreated") && !send(event) {
    return Err("delivery_failure");
}
self.cursor = seq;
```

Verified provider FileChanged and BrowserTabCreated deliberately do not invent semantic_operation payload fields. The Core query succeeds with valid committed events; the pump does not receive Err or empty data for these rows. It discards them at this predicate and advances the cursor. The same events are visible through real Bridge GET /api/events and SQLite and have actual Core EventPublished traces. FileCreated, relations and model ObjectUpdated notifications reached the same authenticated collector. Therefore this is a confirmed Bridge event-selection defect, not a Provider admission, model or Core authorization failure.

Evidence: full-slice-ii-ws-failure.log; full-slice-ii-audit.json gaps; full-slice-ii-before-restart.json actual WS frames; full-slice-ii-http.json actual events; SQLite seq10(BrowserTabCreated),seq12/17(FileChanged). It prevents a full live-notification PASS even though polling eventually reads state.

Minimum proposed correction: extend the existing explicit event predicate narrowly to FileChanged and BrowserTabCreated (or the existing intended domain-event allowlist), preserving scoped query authorization, cursor discipline and post-commit sourcing. Do not modify provider payloads to masquerade as semantic commands, add global event reads, or weaken the session/Origin gate. Add EventPump unit assertions and a real HTTP+WS test for each admitted type, including no notification on replay/rejection and foreign-World isolation. **Not implemented in this verification task.**

## 2. World does not consistently represent canonical Browser objects

File: space-bridge/static/world.html.

- renderTreeMode, Web branch, calls tabObjs() only, which is derived from legacy W.tabs. It ignores memberObjs('Web') from the canonical Core query. The actual Tree says No CDP tabs although the canonical BrowserTab is readable and Shell lists it.
- renderGraphMode builds nodes from relation endpoints, then fallback clusters Mission/Files/Projects only. An unlinked BrowserTab cannot appear through this fallback. This is existing bounded overview behavior, not proof that Core lost the object. This task does not claim Browser relations are permitted.
- objName attempts to match Browser external_id against W.tabs.target_id. The new external_id is a versioned hash, not the raw target; the canonical title/url are properties. World therefore shows the hash instead of the available title. Space counts canonical tabs but quick-access/live counters show0, while signed get_tabs through Core actually returns one admitted tab.
- renderSidebar's Browser actions derive a target from external_id, which is no longer a raw CDP target ID. Actions were not invoked here; navigation/activation/frame compatibility is unverified and must not be reported PASS. Creation/replay does not authorize those actions.

Minimum proposed correction: map the existing canonical BrowserTab properties and IDs into existing World representations; treat any live CDP handles as a separate optional overlay joined through the explicit raw target property, never a replacement identity or second persistent model. Preserve bounded overview semantics and decide explicitly whether Web belongs in its fallback. Add real canonical-tab fixtures to representation tests and verify no duplicate nodes. Do not add Browser lifecycle operations or new grants as part of this presentation fix. **No frontend implementation changed here.**

## 3. Confirmed World search selection exception

File: space-bridge/static/world.html. Function: handleDelegatedClick, lines1959–1964 in the verified asset.

```javascript
const hideDrop = btn.getAttribute('data-hide-drop') === 'true';
// ...
selectObj(target, open === 'true');
if (hideDrop) hideDrop();
```

The local boolean shadows the global hideDrop function. Actual search-result selection changes Focus and then throws `TypeError: hideDrop is not a function`, leaving the dropdown open. Four instances captured after reload in full-slice-ii-ui.json. This is confirmed source/runtime evidence, not a hypothesis. Minimal fix is to rename the local flag (e.g. shouldHideDrop) while keeping the function call. Add an executable handler test, not only script syntax/static parity. **Not changed during verification.**

Canvas Graph-node clicks also did not select during this run; search outside the viewport successfully selected the same object. Viewport pointer capture is a plausible contributor, but no controlled root-cause proof was performed. Do not conflate this with the confirmed hideDrop shadowing or claim a fixed canvas interaction.

## 4. Summary property ambiguity

Real model patched the correct existing Summary with substantive progress, relationship_used and exact Task/File/BrowserTab IDs. But the pre-existing `summary: "Not reviewed yet"` and `status: "unfinished"` remain; progress contains the new narrative. This is not a Core persistence failure or unauthorized mutation: properties are flexible and the accepted patch was persisted exactly. It is a product-level ambiguity for consumers expecting one canonical summary field.

Minimum next step: explicitly name the existing desired output property in the Mission prompt/acceptance criteria and read it back before accepting the business result. If a stricter reusable Summary ontology/validation is desired, seek the corresponding owner contract decision first. Do not fabricate a successful summary overwrite, manually rewrite the model result, silently retry models, or add a new Summary schema in this verification stage.

## 5. Existing presentation and lifecycle diagnostics

- World metadata World field shows —; Shell recent objects show world:(root). Authoritative object scope is properties.world_id; relation/event scope is top-level world_id. Presentation does not reflect that mapping consistently.
- Provider/system counters remain — or0/0 and Shell waits for system status despite actual signed stat/get_tabs success. The synthetic registrar was not broadened to hide this. Product health needs to distinguish missing dashboard grants/status from provider operation failure.
- The latest-events sidebar still favors older entries. Current scoped API/UI count31 differs from40 total SQLite events; do not expose otherwise unauthorized unscoped events to make counters equal.
- Mission command-event payloads can say execution_status=not_launched while the actual persisted MissionAttempt status is succeeded. Current authoritative Mission properties, attempt and MissionExecutionChanged prove execution; this legacy diagnostic field must not be treated as the executor state.
- World/Shell require explicit synthetic login after Bridge restart; seamless session continuity and in-flight execution recovery are not tested. The restart occurred after terminal execution and owner verification.
- The owner supplied verification note "q". It is nonempty and valid under the current contract, with exact attempt binding. Its adequacy as human evidence is not enforced by a minimum-length rule. Preserve the historical owner action; do not overwrite it or claim a more detailed note was recorded.
- The first unexecuted draft's path was malformed by test-input escaping; it was cancelled and retained. The corrected executed draft uses C:/spaceos/test-tmp/full-slice-ii-NKAHWN/project-files. No model retried on the cancelled draft.
- Narrow test browser screenshots do not establish all-viewport readability; some offscreen fields are confirmed by actual DOM/accessibility instead. No visual-layout remediation was authorized or attempted.

## 6. SpaceTrace and security limits

Core-event correlation is confirmed (29 IDs,10 attempt correlations). ETW OS traces, actual inference process ownership and model-process-to-Mission attribution are NOT TESTED. No ETW collector/account/ACL change was performed. Trace stats showing a runtime instance are not such proof.

Synthetic identities/roots/World/epoch and exact-Origin lab authentication are not production credential provisioning or a Windows SID/ACL certification. Existing localhost native-client/Origin and legacy authorization limitations are not declared fixed. No permanent Mission-agent relation grant, global read, actor=system bypass, new Browser mutation authority, table or migration was introduced.

## Minimum next owner scope

1. Approve the narrow Bridge event-predicate correction plus real scoped WS regressions.
2. Approve the one-variable World search fix and canonical BrowserTab representation mapping; preserve existing zoom/navigation and creation-only Browser contract.
3. Clarify the desired Summary output property and acceptance read-back; no new orchestration layer is necessary.

No further model execution is needed merely to reproduce the first two deterministic defects. Retain the successful real Mission/provider/persistence evidence as the regression baseline. No production deployment, commit or push was performed.
