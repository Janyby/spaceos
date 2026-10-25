# Windows v5 source snapshot publication

Date: 2026-10-25. Owner-approved source-only root snapshot for Janyby/spaceos.

## Scope and history

The snapshot branch is windows-v5-snapshot. It is intentionally created as a root commit without parents: inherited local history contains core.db-wal/core.db-shm. The owner approved publishing a clean-history snapshot while preserving every existing local branch and the original working-tree/index state. No history is rewritten, no force push, no merge into main and no production deployment is authorized.

An alternate index under the ignored C:\spaceos\test-tmp\windows-v5-snapshot-v98oNA is used; the original branch remains checked out with its WIP preserved. This is not a clean-up/reset of that WIP.

## Inclusion and security

Includes all 250 reviewed source/support candidates, .gitignore, README and 23 reviewed documentation files, including this publication note: 275 paths total. The 72 formerly untracked source/support candidates are explicitly included. Cargo manifests/lockfile, all active Rust modules, HTML assets, maintained tests and evidence helpers are included.

Excludes opencode.json entirely, both its working version and inherited baseline; no sanitized template was invented. Its structured working/HEAD review found no populated credential fields; exclusion additionally avoids publishing unneeded local configuration. No existing local copy is modified.

Also excludes historical database files/history, test-tmp, target, executables, browser profiles, personal auth/session stores, runtime configuration/tokens and raw historical JSON/log/image attachments. Published reports retain historical references; raw referenced evidence remains private/local and is not claimed to accompany this branch.

Credential-pattern/assignment review of every selected text file found no real provider/service credentials. Documentation example placeholders and negative unit-test values are not exported private runtime credentials. Generated synthetic keys/fixtures remain ignored. This is a bounded source review, not a guarantee about unrelated unpublished local history.

## Owner-approved formatting delta

Only extra terminal blank lines were removed in 44 specifically listed files: 54 bytes total; preceding bytes unchanged. The exact before/after hashes are retained in the private snapshot run's eof-normalization.json. This includes terminal-newline normalization in three earlier maintenance reports; their old recorded byte hashes are historical, not current hashes. Raw runtime evidence/images/databases were not edited.

Source/support fingerprint before this formatting: 39255f407b5f5e26038632369192cac12d2011a9aa38c99c2815a1a75e7c3dc2.
After formatting, same 250 sorted JSON [path,SHA256] records: 20642e7c626ea3d9c85b23169d01dce3e0aaa42fbfd9bf96d5f473307a184d2f.

The regular git diff --cached --check against the existing source baseline passes after these corrections. An additional empty-tree/full-root lint reports five inherited whitespace findings: four trailing-space lines in diagnose.ps1 (68,109,127,138) and a terminal blank line in filesystem-provider/src/scanner.rs (359). Those unchanged legacy files were not normalized outside the owner's 44-file approval. This extra full-root style diagnostic is disclosed, not represented as a passing check or security defect.

## Verification after formatting

- cargo metadata --offline --locked --no-deps: seven packages / 120 targets.
- cargo check --offline --locked -p space-core -p space-bridge -p filesystem-provider -p browser-provider -p runtime -j2: PASS.
- cargo test --offline --locked -p space-core -p space-bridge --lib -j2 -- --test-threads=1: Bridge 44/44, Core 59/59 PASS.
- node tests/evidence_writer_isolation.cjs: 25/25 PASS.
- node tests/scoped_browser_recovery_ui.cjs: 20/20 PASS (extracted production helpers / DOM doubles).
- node tests/mission_verification_ui.cjs: 11/11 PASS (DOM/HTTP doubles).
- No model or additional full runtime was launched for this formatting/publication step.

Logs remain in the ignored snapshot run. The final acceptance report documents the prior real isolated v5 acceptance (86 runtime + 17 audit). These are historical results; they do not assert a redesigned World UI passed acceptance or a new executable was production-deployed.

Final commit SHA and remote verification are reported to the owner after publication; this file does not embed its own commit hash. Existing main and original branch remain unchanged.
