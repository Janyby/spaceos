# SpaceOS — PC V0

> Historical overview (prototype era). Current architecture, status and
> acceptance evidence live in `docs/ARCHITECTURE.md`, `docs/PROJECT_STATUS.md`
> and `docs/ACCEPTANCE_LOG.md`.

## What this is

First experiment: `SPACE CORE | IPC | FILESYSTEM PROVIDER`.
Two approaches are built over the same Core and can be compared:

- **A — Format Knowledge**: layered detection (extension -> MIME -> magic -> semantic type), facts stored separately from interpretation.
- **B — Observation**: filesystem watcher + process observation, recorded as real observations (never invented relations).

## Build

```powershell
cargo build --workspace        # binaries: spaceos (runtime), space-core, filesystem-provider
```

## Run

```powershell
target\debug\spaceos.exe run                     # Core + Provider in one runtime, root C:\spaceos-test
target\debug\spaceos.exe run --root D:\somedir
target\debug\spaceos.exe objects                 # query Core
target\debug\spaceos.exe events [EventType]      # query events
target\debug\spaceos.exe providers
target\debug\spaceos.exe stat C:\spaceos-test\x.txt
target\debug\spaceos.exe rename A B              # command loop: Core -> Policy -> Provider -> Windows -> Observation
target\debug\spaceos.exe subscribe               # print live events
```

IPC: Windows Named Pipe `\\.\pipe\spaceos-core` and `\\.\pipe\spaceos-fs-provider`,
newline-delimited JSON: `{"method": "...", "params": {...}}`.

## Storage

SQLite at `C:\spaceos\runtime\core.db` (env `SPACEOS_DB`).
Reconstruction test: delete the DB, restart, rescan — World is rebuilt from Reality.

## Identity

Path is not the permanent identity. Scanner uses a fingerprint
(content hash of first 4 KiB + size) to detect renames: same fingerprint +
missing old path ⇒ same Object, `FileMoved` event. Uncertainty is recorded
(`identity_source`), never invented.

## Format detection (A)

`format.rs`: extension → MIME → magic bytes → fallback. Examples:
text/markdown, JSON, CSV, Rust/Python source, PNG, JPEG, PDF, ZIP.
Stored: `format`, `semantic_type`, `detected_by`, `confidence`.

## Observation (B)

`watcher.rs` maps notify events to `FileCreated/FileChanged/FileDeleted`.
`observation.rs` polls the process list (`ProcessStarted/ProcessExited`).
File↔process relations (e.g. rustc reads main.rs) require ETW on Windows —
planned next; V0 records only events that actually happened.

## Notes / limitations

- External (out-of-band) renames surface as FileChanged + delete/create pair;
  `FileMoved` is emitted by the scanner/provider reconciliation path.
- Core is OS-independent; only IPC uses Windows named pipes
  (Linux: Unix socket, same protocol).
- No LLM anywhere in detection.
