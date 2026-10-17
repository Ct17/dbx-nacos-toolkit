English | [简体中文](README.md)

# Nacos Toolkit — DBX Plugin

Connect to Nacos from [DBX](https://github.com/t8y2/dbx) and expose **read-only MCP tools** to AI agents.

DBX ships a built-in Nacos config browser, but the MCP surface of that built-in connection is disabled (`mcpMode: unsupported` in the official connection type definition — see [issue #11893](https://github.com/t8y2/dbx/issues/11893)). This plugin solves that through DBX's official plugin system: it provides its own Nacos connection type and bridges the tools into the DBX MCP server, so AI clients such as `Claude Code`, `Cursor` and `Windsurf` can read Nacos configs directly.

## Features

**Connection form** (connection tree → New connection → Nacos)

| Field | Description |
| --- | --- |
| Server address / port | Accepts `127.0.0.1`, a domain name, or `https://…`; you can also paste `https://host:8848/nacos`. The port is not appended when already present; defaults to 8848 |
| Context path | `/nacos` by default; change it for custom deployments. When the address already contains a path, this field wins |
| Console port | Nacos 3.x console port (8080 by default, auto-detected when empty); leave empty for 1.x / 2.x |
| API version | `auto` (default, recommended) / `v1` / `v3`; set manually only when auto-detection picks the wrong protocol stack |
| Namespace ID | Leave empty for public |
| Username / password | Optional when auth is disabled on Nacos 1.x / 2.x; **mandatory on Nacos 3.x** (the console API always requires an account — see below) |

**MCP tools** (all read-only, declared with `readOnlyHint: true`, no manual confirmation required for AI calls)

| Tool | Description |
| --- | --- |
| `get_config` | Read the full config by dataId (+ group / namespace) |
| `search_configs` | Search configs by keyword; returns paginated metadata (dataId / group / type / md5, no content) |
| `diff_config` | **Cross-environment diff**: compare one config across 2–8 namespaces line by line (the first is the baseline), with context-compressed output |
| `get_config_history` | **Config history**: list revisions (timestamp / operator / opType / md5); pass `historyId` to read the full content of that revision |
| `list_namespaces` | List all namespaces with their config counts |
| `list_services` | List registered service names in a namespace |
| `list_instances` | List the instances of a service (IP / port / health / weight / cluster / metadata) |

In AI clients the tools appear as `dbx_nacos_toolkit__get_config`, etc. (`dbx_<plugin prefix>__<tool name>`, where the prefix is the last segment of the plugin id); in the built-in AI panel they appear as `<prefix>__<tool name>`.

**Workbench**: shows the connection probe result and the list of tools currently exposed to AI.

## Architecture

```text
AI client (Claude Code / Cursor / DBX AI panel)
        │  MCP (tools/list, tools/call)
        ▼
DBX MCP server / host        credentials injected by the host, never through the model
        │  plugin bridge protocol (mcp/tools, mcp/call, lifecycle.connection)
        ▼
This plugin's Go sidecar    standard library only, auto-detects the protocol stack (v1 / v3)
        │  HTTP (v1: 1.x/2.x Open API; v3: 3.x native client + console API)
        ▼
Nacos server
```

- Protocol implementation: [`dbx-plugin-sdk`](https://github.com/t8y2/dbx/tree/main/plugins/sdk/go) (Sidecar Protocol v1, JSON-RPC 2.0 / JSONL over stdio)
- Nacos calls: the protocol stack is auto-detected on connect — v3 (Nacos 3.x native, `/v3/client/**` + `/v3/console/**` on two ports) takes precedence, falling back to v1 (`/v1/cs/configs`, `/v1/console/namespaces`, `/v1/ns/service/list`, `/v1/ns/instance/list`, …). Login and token renewal are automatic (v1 uses `accessToken`, v3 uses a `Bearer` header)
- No third-party dependencies: Go standard library + the DBX SDK (the diff is a hand-written LCS implementation)

## Repository layout

```text
├── manifest.json            # plugin contract: connection form, workbench, mcp contributions
├── dbx-plugin.toml          # packaging config (Go backend)
├── backend/
│   ├── main.go              # handlers: connection lifecycle + protocol dispatch
│   ├── nacos.go             # client infrastructure: URL parsing, protocol probing, auth, transport
│   ├── v1.go                # Nacos 1.x/2.x Open API protocol stack
│   ├── v3.go                # Nacos 3.x native API stack (client + console)
│   ├── mcp.go               # MCP tool definitions and dispatch
│   ├── diff.go              # line-level diff (LCS, zero dependencies)
│   ├── main_test.go         # integration tests (httptest-mocked Nacos, v1 + v3)
│   └── diff_test.go         # diff algorithm unit tests
├── scripts/smoke.py         # protocol-level smoke test: drives the sidecar like a host (mock Nacos)
├── scripts/e2e_docker.py    # real Nacos in Docker: assertions for all 7 tools + error paths + auth (v1/v3)
├── ui/index.html            # workbench UI (sandboxed static page)
├── third_party/dbx-plugin-sdk/  # local copy of the Go SDK (go.mod replace)
└── docs/reference/          # archived development references (official docs, schema)
```

## Development

Requirements: Go 1.22+, Node.js 22+ (CLI only), `@dbx-app/plugin-cli`.

```bash
# Unit tests (httptest-mocked Nacos, runs offline)
cd backend && go test ./... -v

# Protocol-level smoke test (builds the sidecar, starts a mock Nacos, simulates a host session)
python3 scripts/smoke.py

# End-to-end against a real Nacos (requires local Docker; the script seeds test data and is repeatable)
# v1 stack (Nacos 2.4.3 etc.)
docker run -d --name nacos-e2e -p 18848:8848 -e MODE=standalone nacos/nacos-server:v2.4.3
python3 scripts/e2e_docker.py --port 18848

# v3 native stack (Nacos 3.2+, no adapter; the console port 8080 must be mapped)
docker run -d --name nacos3-e2e -p 18868:8848 -p 18088:8080 \
  -e MODE=standalone -e NACOS_AUTH_ENABLE=false \
  -e NACOS_AUTH_TOKEN=<Base64-encoded string of ≥32 bytes, required by 3.x images> \
  nacos/nacos-server:v3.2.4
python3 scripts/e2e_docker.py --port 18868 --console-port 18088

# Standalone dev host (no DBX needed; debug the real backend/frontend and the connection form in a browser)
npx @dbx-app/plugin-cli dev --path . --port 5190

# Package an unsigned development build
npx @dbx-app/plugin-cli package .
```

> `backend/go.mod` uses a `replace` directive pointing at `third_party/dbx-plugin-sdk` (the SDK has no Go module tag yet). If you work from a DBX source checkout, use `--sdk-root` or `DBX_PLUGIN_SDK_ROOT` instead.

## Install and use in DBX

1. Package: `dbx-plugin package .` → `dist/io.github.ct17.nacos_toolkit-0.2.0-darwin-arm64.dbxp`
2. DBX → Plugin Center → Settings → Third-party & developer options → enable "Allow unsigned development builds"
3. Install the `.dbxp` → confirm "Nacos Toolkit" appears under Installed
4. New connection → choose **Nacos** (provided by the plugin) → fill in the address and credentials → test the connection
5. Open the connection → enable this plugin under "Built-in AI tools" → AI clients can now call the tools

**MCP client config** (`.mcp.json`, same as a regular DBX setup):

```json
{
  "mcpServers": {
    "dbx": { "command": "npx", "args": ["-y", "@dbx-app/mcp-server"] }
  }
}
```

Then add the connection and the `dbx_nacos_toolkit__*` tools to the allowlist in **DBX Settings → MCP**.

## Nacos 3.x support (native v3, no adapter)

The plugin **auto-detects the protocol stack** on connect: it probes the v3 console health endpoint first (Nacos 3.x) and falls back to the v1 health endpoint (1.x / 2.x). Nacos 3.x talks to the native v3 API directly — **no legacy adapter is required**:

| Environment | Protocol stack | Connection settings |
| --- | --- | --- |
| Nacos 1.x / 2.x | v1 Open API | none (username/password only when auth is enabled) |
| Nacos 3.x (native) | v3 API | set **Console port** (default 8080) + **username/password** |
| Nacos 3.x + legacy adapter | v3 (probing wins) | same as above; the adapter is no longer required |

Two notes for Nacos 3.x:

1. **Console port**: the v3 console API (search / history / namespaces / service list) runs on a separate port (8080 by default, independent of the 8848 service port). If Docker remapping changes the port (e.g. `-p 18088:8080`), set the actual port in the connection; on a default install you can leave it empty (auto-detected).
2. **An account is required**: the v3 console API enforces login even when authentication is disabled on the server. On a fresh instance, initialize the admin user first (`POST /v3/auth/user/admin`), then use that account in the connection. Anonymous connections work only for `get_config` / `diff_config` / `list_instances` (client API).

Verified against plain Nacos 3.2.4 with no adapter: **43 end-to-end checks passed × 3 runs** (two instances: with and without auth), and 43/43 on the 2.4.3 regression. Details in `docs/e2e-nacos3-native-2026-10-17.md`.

## Known limitations

- **Read-only**: this version provides no write operations (publishing configs, registering services) — planned for a later release
- **Desktop only**: DBX Web/Docker backends do not expose plugin tools (an upstream limitation)
- **Credentials**: injected into the sidecar by the host and never written to disk; the sidecar keeps the connection snapshot and active session in memory only (Go strings cannot be zeroed in place, so they are reclaimed by GC after disconnect). Health probing (`nacos/ping`) carries no credentials
- **2 MiB response cap**: responses above the cap fail explicitly instead of being silently truncated and handed to the AI
- **What the AI sees**: tool results contain only the requested result and the HTTP status code; upstream response bodies go to the plugin log and never enter the model context
- **Nacos protocol**: 1.x / 2.x use the v1 Open API; 3.x switches to the native v3 API automatically (3.2+ needs no legacy adapter). The v3 instance list omits instances that are down (`enabled=false`) — official Nacos semantics; v3 console endpoints require an account (see "Nacos 3.x support")

## Performance

- Repeated tool calls on the same connection reuse one Nacos client: the token and HTTP connection pool are not re-established (login happens once, on the first call); the cache is dropped when the connection closes
- Upstream calls time out at 15 s; a single tool call has a 30 s total budget

## Roadmap

- [x] `diff_config`: compare one config across namespaces/environments (the core ask in issue #11893) — v0.1.1
- [x] `list_instances`: service instance listing — v0.1.1
- [x] `get_config_history`: config revisions (list + single revision content) — v0.1.1
- [x] Nacos 3.x native v3 protocol stack: auto-detection + console port / Bearer auth, no legacy adapter — v0.2.0
- [x] `list_instances` cluster filter; `diff_config` parallel reads (2–8 namespaces in a single round trip) — v0.2.0
- [ ] Workbench upgraded to a config browser (list / search / view)
- [ ] Write tools (publish config with a dryRun preview + two-phase confirmation, requiring host-side manual confirmation)

## References

- [DBX plugin development docs](https://dbxio.com/cn/docs/plugin-development) (archived in `docs/reference/`)
- [DBX plugin quick start](https://github.com/t8y2/dbx/blob/main/plugins/GETTING_STARTED.zh-CN.md)
- [DBX MCP integration docs](https://dbxio.com/cn/docs/mcp)

## License

Apache-2.0
