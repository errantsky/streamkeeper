# Claude Code Context for Streamkeeper

## Project Overview

**Streamkeeper** is an Elixir/OTP implementation of the [Durable Streams protocol](https://github.com/durable-streams/durable-streams) - append-only, URL-addressable byte logs with long-polling and SSE support.

- **Hex package name:** `streamkeeper`
- **Module namespace:** `DurableStreams.*` (kept for clarity, doesn't match package name)
- **Primary use case:** Resumable streaming for AI/LLM token streaming, real-time events

## Key Commands

```bash
# Run tests (ALWAYS run after changes)
mix test

# Run conformance tests (requires Node.js)
mix durable_streams.conformance

# Run benchmarks
mix bench              # Run all benchmarks
mix bench storage      # Run specific benchmark

# Build hex package
mix hex.build

# Publish to hex.pm
mix hex.publish

# Generate docs
mix docs
```

## Testing Requirements

**CRITICAL:** Always run `mix test` after making any code changes. The test suite includes:
- Unit/integration tests (including retention tests)
- Property-based tests
- Protocol conformance verification

All tests must pass before committing. Current status: **175 tests, 15 properties, 0 failures**

For full protocol conformance testing:
```bash
mix durable_streams.conformance
```
This runs the official Durable Streams conformance suite (131/131 tests must pass).

## Post-Change Checklist

After making significant changes, verify:

1. **Unit tests pass:** `mix test`
2. **Conformance tests pass:** `mix durable_streams.conformance`
3. **Code compiles without warnings:** `mix compile --warnings-as-errors`
4. **Examples still work:**
   - `elixir examples/simple_demo.exs`
   - `iex examples/llm_streaming.exs` (check http://localhost:4000)
5. **Documentation is updated:**
   - `README.md` for user-facing changes
   - `CHANGELOG.md` for all changes
   - `CLAUDE.md` if project structure or key details change
6. **No stale references:** Search for old module/function names if renaming

## Changelog Management

The project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format.

**When making changes:**
1. Update `CHANGELOG.md` with your changes under the appropriate section
2. Use sections: `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`
3. Keep entries concise but descriptive

**For releases:**
1. Change `[Unreleased]` to `[X.Y.Z] - YYYY-MM-DD`
2. Add a new `[Unreleased]` section at the top
3. Follow [Semantic Versioning](https://semver.org/):
   - MAJOR: Breaking API changes
   - MINOR: New features, backward compatible
   - PATCH: Bug fixes, backward compatible

## Release Process

1. **Update version** in `mix.exs` (`@version`)
2. **Update CHANGELOG.md** with release date
3. **Run tests:** `mix test`
4. **Commit:** `git commit -am "Release vX.Y.Z"`
5. **Tag:** `git tag -a vX.Y.Z -m "vX.Y.Z - Description"`
6. **Push:** `git push origin main && git push origin vX.Y.Z`
7. **Publish:** `mix hex.publish`

## Dependencies

Keep dependencies up to date. Check for updates periodically:
```bash
mix hex.outdated
```

**Required dependencies:**
- `plug` ~> 1.15
- `phoenix_pubsub` ~> 2.1

**Optional dependencies:**
- `plug_cowboy` ~> 2.7 (HTTP server)
- `bandit` ~> 1.6 (HTTP server alternative)
- `phoenix_live_view` ~> 1.0 (for `DurableStreams.LiveView` helper)

**Dev/test dependencies:**
- `jason` ~> 1.4 (JSON encoding for tests)
- `ex_doc` ~> 0.34 (documentation generation)
- `credo` ~> 1.7 (code analysis)
- `sobelow` ~> 0.13 (security analysis)
- `stream_data` ~> 1.1 (property-based testing)
- `benchee` ~> 1.3 (benchmarking)
- `benchee_html` ~> 1.0 (HTML benchmark reports)
- `benchee_markdown` ~> 0.3 (Markdown benchmark reports)

When updating dependencies:
1. Update version in `mix.exs`
2. Run `mix deps.get`
3. Run `mix test` to verify compatibility
4. Update CHANGELOG if significant

## Project Structure

```
lib/
├── durable_streams.ex           # Main API module
├── durable_streams/
│   ├── application.ex           # OTP application
│   ├── stream_manager.ex        # Core stream operations
│   ├── stream.ex                # Stream struct
│   ├── offset.ex                # Offset generation/parsing
│   ├── json.ex                  # JSON encoding/decoding
│   ├── live_view.ex             # Phoenix LiveView helper (optional)
│   ├── server/
│   │   └── stream_server.ex     # Per-stream GenServer
│   ├── storage/
│   │   ├── behaviour.ex         # Storage behaviour
│   │   └── ets.ex               # ETS storage backend
│   ├── retention/
│   │   ├── supervisor.ex        # Retention subsystem supervisor
│   │   ├── scheduler.ex         # Periodic compaction scheduler
│   │   └── worker.ex            # Compaction worker (stateless)
│   └── protocol/
│       ├── plug.ex              # Main Plug router
│       └── handlers/            # HTTP handlers (create, append, read, etc.)
examples/
├── simple_demo.exs              # CLI API demo
└── llm_streaming.exs            # LiveView + LLM streaming demo
bench/
├── config.exs                   # Benchmark configuration
├── benchmarks/
│   ├── concurrent_bench.exs     # Concurrent access benchmarks
│   ├── offset_bench.exs         # Offset generation benchmarks
│   ├── retention_bench.exs      # Retention policy benchmarks
│   └── storage_bench.exs        # Storage operations benchmarks
└── results/                     # Generated HTML/Markdown reports
```

## Key Implementation Details

### Offset Format
```
{monotonic_integer_hex}
Example: 0000000000a1b2c3 (16 hex chars = 64 bits)
```
- Generated from `erlang:unique_integer([:monotonic, :positive])`
- Zero-padded hex for lexicographic sortability
- Clock-independent, guaranteed monotonic and unique
- Sentinel value `-1` means "start of stream"
- Use `Offset.to_integer/1` to convert offset to integer for ETS operations

### JSON Mode
When `Content-Type: application/json`:
- Arrays are flattened ONE level: `[{a:1}, {b:2}]` stores 2 messages
- Each message stored with its own offset
- Empty arrays `[]` on POST return 400

### LiveView Module
`DurableStreams.LiveView` is conditionally compiled - only available when `phoenix_live_view` is installed. Uses `ds_` prefixed assigns to avoid conflicts.

### Long-Polling
Default timeout: 30 seconds. Waiters are notified via Phoenix.PubSub when new data arrives.

### Retention Policies
Streams can have automatic retention with `max_age`, `max_messages`, or `max_bytes`. The `Retention.Scheduler` runs periodically (default: 30s) and spawns `Retention.Worker` tasks to compact streams. Compacted offsets return `410 Gone` with `Stream-Earliest-Offset` header.

### HTTP Integration
`DurableStreams.Protocol.Plug` is a composable Plug router. Users must forward to it from their own router:

```elixir
# Phoenix router
forward "/v1/stream", DurableStreams.Protocol.Plug

# Standalone
defmodule MyRouter do
  use Plug.Router
  plug :match
  plug :dispatch
  forward "/v1/stream", to: DurableStreams.Protocol.Plug
end
```

Note: There is no `V1Plug` - the path prefix is chosen by the user.

## Examples

The `examples/` directory uses Phoenix Playground for single-file demos:
- **simple_demo.exs** - Run with `elixir examples/simple_demo.exs`
- **llm_streaming.exs** - Run with `iex examples/llm_streaming.exs`, open http://localhost:4000

Examples use `{:streamkeeper, path: "."}` for local development.

## Common Tasks

### Adding a new feature
1. Implement the feature
2. Add tests
3. Run `mix test`
4. Update CHANGELOG.md
5. Update README.md if user-facing
6. Commit with descriptive message

### Fixing a bug
1. Write a failing test that reproduces the bug
2. Fix the bug
3. Verify test passes
4. Run full test suite
5. Update CHANGELOG.md under `Fixed`
6. Commit

### Updating documentation
1. Update relevant .md files
2. Update `@moduledoc` / `@doc` in code
3. Run `mix docs` to verify
4. Commit

## GitHub Repository

- URL: https://github.com/errantsky/streamkeeper
- Main branch: `main`
- Releases tagged as `vX.Y.Z`

## Hex.pm

- Package: https://hex.pm/packages/streamkeeper
- Docs: https://hexdocs.pm/streamkeeper
