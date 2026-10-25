# SpaceOS — PC V0

First SpaceOS hypothesis on Windows PC:

```
       SPACE CORE
            |
     Provider Protocol
        /          \
Format Knowledge  Observation
        \          /
            WORLD
```

Core is the small, stable semantic layer. The physical filesystem is
Reality / ground truth. Core state (SQLite) is reconstructable from
Reality through the Filesystem Provider.

## Status

- **Current SpaceOS V0 implementation is Rust** (`space-core`,
  `filesystem-provider`, `runtime` cargo workspace).
- **Python V0 is a legacy experimental prototype**, preserved under
  `_legacy-python-v0/` for reference. It is not built, shipped, or
  executed by the current system.
- **Python is NOT a runtime dependency of SpaceOS.** No Rust binary
  invokes Python.

## Layout

```
C:\spaceos\
  space-core\src\        # core: domain, storage(sqlite), query, subscriptions, provider registry, pipe server
  filesystem-provider\src\   # scanner, watcher, identity, metadata, format (A), observation, processes (B), actions
  runtime\src\           # `spaceos` CLI (run, objects, events, actions, subscribe)
  docs\
  _legacy-python-v0\     # legacy Python V0 prototype (reference only)
```

## IPC

Windows Named Pipe `\\.\pipe\spaceos-core`, newline-delimited JSON messages
(see `space-core/src/ipc.rs`). Core owns the pipe; providers and human/agent
clients connect. Future Linux: Unix domain socket without changing Core
semantics.

## Run

```bat
cargo build --workspace
target\debug\spaceos.exe run --root C:\spaceos-test
target\debug\spaceos.exe objects
target\debug\spaceos.exe events
target\debug\spaceos.exe rename "C:\spaceos-test\hello.txt" "C:\spaceos-test\hello2.txt"
target\debug\spaceos.exe subscribe
```

## Tests

Results of the V0 agent-spec test battery are documented in
`docs\V0_RESULTS.md`.
