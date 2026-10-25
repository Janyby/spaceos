# SpaceOS Agent Gateway (stage 1)

The Bridge connects to an **already running OpenCode v2 Server**. It does not
start OpenCode and it never uses the local model endpoint at
`127.0.0.1:8888/v1` as a substitute.

## Server configuration

For the OpenCode v2 managed service used by Desktop, no manual secret copy is
needed: Bridge reads the current loopback URL and generated password from the
local `opencode/service.json` registration. The secret stays in Bridge memory.

For an explicitly managed server, set these variables in the environment that
starts `space-bridge` (they take priority over auto-discovery):

```powershell
$env:OPENCODE_SERVER_URL = "http://127.0.0.1:4096/api"
$env:OPENCODE_SERVER_USERNAME = "opencode"
$env:OPENCODE_SERVER_PASSWORD = "<the password configured on OpenCode Server>"
```

`OPENCODE_SERVER_URL` must use plain HTTP on `localhost`,
`127.0.0.1`, or another loopback IP. Do not put credentials in the URL. The
password stays in the Bridge process and is never returned to World.

OpenCode Desktop and TUI can choose a random port; managed-service discovery
follows that port without starting or restarting OpenCode. OpenCode v2 protects
the API with HTTP Basic Auth. A `401 Authentication required` response means
the URL is reachable but the discovered or explicit credentials do not match.

## Trust boundary

The route is `World -> Bridge Agent Gateway -> OpenCode Server`. When OpenCode
calls the configured `spaceos` MCP server, SpaceOS resource operations continue
through `MCP -> Core -> Policy -> Provider -> Observation`. OpenCode's other
tools are governed by OpenCode's own permission system, not by SpaceOS Policy.

The existing MCP server uses the fixed identity `MCP_ACTOR =
space-bridge-mcp`. Therefore two OpenCode agents using this MCP configuration
are not distinguishable in Core audit records. Stage 1 records this limitation
and does not change the identity model. A future identity design must assign a
server-verified per-agent principal without accepting an actor name from the
browser or tool caller.

## Bridge endpoints

- `GET /api/agent/sessions` — sanitized session list and running/idle state.
- `POST /api/agent/send` — admit `{session_id, message}` to the selected session.
- `GET /api/agent/status/{request_id}` — transient execution/result state.
- `/ws` — emits `agent_update` messages for the same state transitions.

These endpoints are exposed only by the existing Bridge listener, whose
default bind address is `127.0.0.1:8765`.
