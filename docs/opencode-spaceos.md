# SpaceOS tool routing

When the user asks for SpaceOS state, health, objects, relations, events,
missions, actions, providers, filesystem state, or browser state, use the
corresponding tool from the `spaceos` MCP server before answering.

For the request `Покажи состояние SpaceOS` (and equivalent status requests),
call the `spaceos_status` tool from the `spaceos` MCP server. Base the answer on
that tool result. Do not substitute shell commands, direct HTTP calls, or the
local LLM endpoint. If the MCP tool is unavailable, report that failure instead
of silently taking a different route.

SpaceOS resource actions must use the `spaceos` MCP tools so they pass through
Core, Policy, Providers, and Observation. Other OpenCode tools remain governed
by OpenCode permissions and are not covered by SpaceOS Policy.
