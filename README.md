# Neo

**Turn any web app into an API.** No official API needed. No browser automation.

Neo is a Chrome extension that passively captures every API call your browser makes, learns the patterns, and lets AI (or you) replay them directly.

## The Problem

AI agents operating web apps today have two options, both bad:

| Approach               | Pain                                                               |
| ---------------------- | ------------------------------------------------------------------ |
| **Official APIs**      | Most SaaS doesn't have one, or only exposes 10% of features        |
| **Browser automation** | Screenshot → OCR → click. Slow, fragile, breaks on every UI change |

**Neo is the third way.** Every web app already has a complete internal API — the frontend calls it every time you click something. Neo captures those calls and makes them replayable.

**v2: Now with UI automation.** Neo v2 adds an accessibility-tree-based UI layer — `snapshot`, `click`, `fill`, `type`, `press`, `hover`, `scroll`, `select`, `screenshot`, `get`, `wait`. When an API exists, use it directly. When it doesn't, Neo can drive the UI through the same CLI. One tool, both layers.

## How It Works

```
Browse normally → Neo records all API traffic → Schema auto-generated → AI replays APIs directly
                                                                      → Or drives UI via a11y tree
```

### 1. Capture (always-on)

The Chrome extension intercepts every `fetch()` and `XMLHttpRequest` — URLs, headers, request/response bodies, timing, even which DOM element triggered the call.

### 2. Learn

Run `neo-schema` on a domain to auto-generate its API map: endpoints, required auth headers, query parameters, response structure, error codes.

### 3. Execute

Run API calls inside the browser tab's context via Chrome DevTools Protocol. Cookies, CSRF tokens, session auth — all inherited automatically. No token management needed.

## Quick Start

```bash
git clone https://github.com/4ier/neo.git
cd neo && npm install && npm run build
npm link  # makes `neo` available globally
```

Load the extension:
1. Open `chrome://extensions`
2. Enable "Developer mode"
3. Click "Load unpacked" → select `extension/dist/`
4. **Important:** Copy the generated Extension ID (e.g., `dfgodilhpdcalfijfebibl...`) and export it as an environment variable so the CLI can find it:
   ```bash
   export NEO_EXTENSION_ID="your_extension_id_here"
   ```
5. Browse any website — Neo starts capturing immediately

## CLI Tools

All commands go through a single CLI: `neo <command>`.

Requires a browser with CDP (Chrome DevTools Protocol) enabled.

```bash
# --- Connection & Sessions ---
neo connect [port]                          # Connect to CDP, save session
neo connect --electron <app-name>           # Auto-discover Electron app's CDP port
neo launch <app> [--port N]                 # Launch Electron app with CDP enabled
neo discover                                # Find reachable CDP endpoints on localhost
neo sessions                                # List saved sessions
neo tab                                     # List CDP targets in active session
neo tab <index> | neo tab --url <pattern>   # Switch active tab target
neo inject [--persist] [--tab pattern]      # Inject Neo capture script into target

# --- Capture & Traffic ---
neo status                                  # Overview of captured data
neo capture summary                         # Quick overview
neo capture list github.com --limit 10      # Shows IDs for replay/detail
neo capture list --since 1h                 # Time-filtered
neo capture domains
neo capture search "CreateTweet" --method POST
neo capture watch x.com                     # Live tail (like tail -f)
neo capture stats x.com                     # Method/status/timing breakdown
neo capture export x.com --since 2h > x.json
neo capture export x.com --format har > x.har  # HAR 1.2 for Postman/devtools
neo capture import x-captures.json
neo capture prune --older-than 7d
neo capture gc x.com [--dry-run]            # Smart dedup

# --- API Replay & Execution ---
neo replay <capture-id> --tab x.com         # Replay a captured call
neo exec <url> --method POST --body '{...}' --tab example.com --auto-headers
neo api x.com HomeTimeline                  # Smart call (schema lookup + auto-auth)

# --- Schema & Analysis ---
neo schema generate x.com                   # Generate from captures
neo schema generate --all                   # Batch all domains
neo schema show x.com [--json]
neo schema openapi x.com                    # Export OpenAPI 3.0 spec
neo schema diff x.com                       # Changes from previous version
neo schema coverage                         # Domains with/without schemas
neo label x.com [--dry-run]                 # Semantic endpoint labels
neo flows x.com [--window 5000]             # API call sequence patterns
neo deps x.com [--min-confidence 1]         # Response→request data dependencies
neo workflow discover|show|run <name>       # Multi-step workflow discovery & replay
neo suggest x.com                           # AI capability analysis
neo export-skill x.com                      # Generate agent-ready SKILL.md

# --- UI Automation (v2) ---
neo snapshot [-i] [-C] [--json]             # A11y tree with @ref mapping
neo click @ref [--new-tab]                  # Click element by @ref
neo fill @ref "text"                        # Clear + fill input
neo type @ref "text"                        # Append text to input
neo press <key>                             # Keyboard key (supports Ctrl+a, Enter, etc.)
neo hover @ref                              # Hover over element
neo scroll <dir> [px] [--selector css]      # Scroll by direction
neo select @ref "value"                     # Set dropdown value
neo screenshot [path] [--full] [--annotate] # Capture screenshot
neo get text @ref | neo get url | neo get title  # Extract info
neo wait @ref | neo wait --load networkidle | neo wait <ms>  # Wait for element/load/time

# --- Page Interaction ---
neo read github.com                         # Extract readable text
neo eval "document.title" --tab github.com  # Run JS in page
neo open https://example.com                # Open URL

# --- Mock & Bridge ---
neo mock x.com [--port 8080 --latency 200]  # Mock server from schema
neo bridge [--json] [--interactive]          # Real-time WebSocket capture stream

# --- Diagnostics ---
neo doctor                                  # Check Chrome, extension, schemas
neo reload                                  # Reload extension from CLI
neo tabs [filter]                           # List open Chrome tabs
```

### Sessions & Multi-App Support

Neo isn't just for Chrome. Any app with CDP support works — including Electron apps:

```bash
# Launch VS Code with CDP and connect
neo launch code --port 9230
neo snapshot                # See VS Code's accessibility tree
neo click @14               # Click a menu item

# Or connect to an already-running Electron app
neo connect --electron slack

# Inject Neo's capture script into any CDP target
neo inject --persist        # Survives page navigation
neo inject --tab slack      # Target specific tab
```

Sessions are saved automatically. Switch between them with `--session`:

```bash
neo --session vscode snapshot
neo --session chrome api x.com HomeTimeline
```

### UI Automation (v2)

Neo v2 adds a full UI interaction layer built on the accessibility tree — no screenshots, no coordinates, no pixel-matching:

```bash
# 1. Take a snapshot — each interactive element gets a @ref
neo snapshot
#  @1  button "Sign in"
#  @2  textbox "Search"
#  @3  link "Pricing"

# 2. Interact by @ref
neo click @1
neo fill @2 "AI agents"
neo press Enter
neo screenshot results.png --full
```

This gives AI agents a fast, semantic way to interact with any UI. Combine with API capture for a dual-channel approach: use APIs when they exist, fall back to UI when they don't.

The bridge creates a persistent WebSocket channel between the extension and CLI. The extension auto-connects to `ws://127.0.0.1:9234` and streams every capture in real-time. In interactive mode, you can query the extension directly: `ping`, `status`, `capture.count`, `capture.list`, `capture.domains`, `capture.search`, `capture.clear`.

## Architecture

```
┌─────────────────────────────────────┐
│  Chrome / Electron App (CDP)         │
│                                      │
│  inject/interceptor.ts               │
│    ├─ Monkey-patches fetch & XHR     │
│    ├─ Intercepts WebSocket/SSE       │
│    ├─ Tracks DOM triggers (click →   │
│    │   API correlation)              │
│    └─ Records full request/response  │
│                                      │
│  content/index.ts                    │
│    └─ Bridges page ↔ extension       │
│                                      │
│  background/index.ts                 │
│    ├─ Persists to IndexedDB (Dexie)  │
│    ├─ Per-domain cap (500 entries)   │
│    └─ WebSocket Bridge client        │
│                                      │
└──────────────┬──────────────────────┘
               │ Chrome DevTools Protocol
┌──────────────┴──────────────────────┐
│  CLI: neo (Node.js)                  │
│                                      │
│  Layer 1: API Capture & Replay       │
│  ├─ neo capture → traffic management │
│  ├─ neo schema  → API discovery      │
│  ├─ neo exec    → execute in browser │
│  ├─ neo api     → smart schema call  │
│  ├─ neo replay  → re-run captured    │
│  └─ neo flows/deps → pattern analysis│
│                                      │
│  Layer 2: UI Automation (v2)         │
│  ├─ neo snapshot → a11y tree + @refs │
│  ├─ neo click/fill/type/press/hover  │
│  ├─ neo scroll/select/screenshot     │
│  └─ neo get/wait                     │
│                                      │
│  Session Management                  │
│  ├─ neo connect/launch/discover      │
│  ├─ neo tab → target switching       │
│  └─ neo inject → script injection    │
│                                      │
└──────────────┬──────────────────────┘
               │
┌──────────────┴──────────────────────┐
│  AI Agent (OpenClaw / any LLM)       │
│  ├─ API-first: schema → exec/api     │
│  ├─ UI fallback: snapshot → click    │
│  └─ Dual-channel automation          │
└──────────────────────────────────────┘
```

## Real-World Demo

### Post a tweet via captured API

```bash
# 1. Browse X normally — Neo captures GraphQL mutations
neo schema show api.x.com

# 2. Find the CreateTweet endpoint
neo capture search "CreateTweet" --method POST

# 3. Replay with the original auth (cookies inherited automatically)
neo replay abc123 --tab x.com

# 4. Or craft a new call with auto-detected auth headers
neo exec "https://x.com/i/api/graphql/.../CreateTweet" \
  --method POST --auto-headers \
  --body '{"variables":{"tweet_text":"Hello from Neo!"},...}'
```

### Understand what a button does

Neo's trigger tracking maps UI interactions to API calls:

```bash
neo schema show github.com
# Output includes:
#   POST /repos/:owner/:repo/star  (3x, 280ms)
#     ← click button.js-social-form "Star" (3x)
```

### Live-monitor API traffic

```bash
neo capture watch api.openai.com
# 14:23:01  POST 200 /v1/chat/completions (1230ms)
# 14:23:05  SSE_MSG 200 /v1/chat/completions (0ms) [sse]
```

## Storage

- **Per-domain cap**: 500 captures max per domain, oldest auto-cleaned
- **Body truncation**: Response bodies capped at 100KB
- **Location**: Chrome's IndexedDB (`neo-capture-v01` database)
- **Schemas**: Exportable as JSON files for persistence across devices

## Smart Filtering

The interceptor ignores noise automatically:
- Static assets (images, fonts, CSS, JS bundles)
- Analytics/tracking (Google Analytics, Meta Pixel, Sentry, etc.)
- Browser internals (chrome-extension://, data:, blob:)
- **Rate limiting**: Max 3 captures per URL pattern per minute (prevents polling endpoint bloat)

## Security & Privacy

Neo runs entirely locally — no external servers, no telemetry, no data leaves your machine.

**What Neo captures:** Every fetch/XHR/WebSocket call your browser makes on every website. This is powerful but invasive by design.

**Auth header redaction (v1.1.0+):** Auth header values (Bearer tokens, CSRF tokens, cookies, session IDs) are redacted at capture time before storage. IndexedDB only stores header *names*, not values. When `--auto-headers` executes an API call, it fetches live auth headers from the browser in real-time via CDP — never replays stored credentials.

**What you should know:**

| Aspect              | Detail                                                                                             |
| ------------------- | -------------------------------------------------------------------------------------------------- |
| **Capture scope**   | `<all_urls>` — Neo sees traffic on *every* website, including banking, email, medical portals      |
| **Content script**  | Runs in `MAIN` world (shares JS context with pages) to intercept fetch/XHR                         |
| **CDP port**        | CLI requires Chrome on port 9222 — any local process can connect to this port                      |
| **Response bodies** | Stored in IndexedDB (truncated to 100KB) — may contain personal data from API responses            |
| **Schema files**    | Store only endpoint structure (paths, header names, response shapes) — no credentials or user data |
| **Export**          | `neo capture export` redacts auth by default; `--include-auth` requires explicit opt-in            |

**Recommendations:**

- Review captured domains periodically: `neo capture domains`
- Prune sensitive captures: `neo capture clear banksite.com`
- Don't install Neo on shared machines where others have local access
- The CDP port (9222) should not be exposed beyond localhost

Neo is a developer tool that trades privacy surface for capability. Use it knowingly.

## Tech Stack

- TypeScript, Vite (multi-entry build)
- Chrome Manifest V3
- Dexie.js (IndexedDB wrapper)
- Chrome DevTools Protocol for CLI ↔ browser communication
- No backend server, no external dependencies at runtime

## Roadmap

- [x] Extension: capture + store + command-driven workflow
- [x] CLI tools: unified `neo` CLI with subcommands
- [x] Storage management: per-domain caps, auto-cleanup, rate limiting
- [x] Schema: browser-side analysis, URL normalization, body structure extraction
- [x] Smart filtering: static assets, analytics, duplicate suppression
- [x] WebSocket capture (open/close/send/recv with throttling)
- [x] Capture replay: `neo replay <id>` re-executes captured calls
- [x] Import/export: cross-device capture migration
- [x] Smart API call: `neo api` with schema lookup + auto-auth
- [x] Flow analysis: `neo flows` discovers API call sequences
- [x] Dependency chains: `neo deps` finds response→request data flow
- [x] Schema versioning with diff detection and history
- [x] Diagnostics: `neo doctor` for setup verification
- [x] Pure function extraction + 73 unit tests + CI
- [x] Agent skill export: `neo export-skill` generates SKILL.md
- [x] Mock server: `neo mock` generates local HTTP server from schema
- [x] HAR 1.2 export format for Postman/Charles/devtools interop
- [x] OpenAPI 3.0 spec generation from captured schemas
- [x] Batch schema generation (`--all`)
- [x] Semantic endpoint labeling (`neo label`)
- [x] Multi-step workflow discovery and execution (`neo workflow`)
- [x] Session management: `neo connect`, `neo sessions`, `--session` flag
- [x] Electron support: `neo launch`, `neo connect --electron`
- [x] Tab management: `neo tab` list/switch targets
- [x] Script injection: `neo inject` with `--persist` and `--tab`
- [x] **v2 UI layer**: `snapshot`, `click`, `fill`, `type`, `press`, `hover`, `scroll`, `select`, `screenshot`, `get`, `wait`
- [ ] Dual-channel: Neo API-first → UI fallback (automatic)

## License

MIT
