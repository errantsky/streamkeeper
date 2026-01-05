# DurableStreams Examples

This directory contains example applications demonstrating DurableStreams functionality.

## Requirements

- Elixir 1.15+
- Erlang/OTP 27+ (for stdlib `:json` module)

## Examples

### 1. Simple Demo (`simple_demo.exs`)

A command-line demonstration of the core DurableStreams API showing:
- Stream creation
- Appending data
- Reading data
- Long-polling for updates
- JSON mode
- Stream cleanup

**Run:**
```bash
cd /path/to/durable_streams
elixir examples/simple_demo.exs
```

**Expected output:**
```
============================================================
DurableStreams Simple Demo
============================================================

--- 1. Creating a Stream ---
   Created stream: demo-stream
   Content-Type: text/plain
   ...

--- 2. Appending Data ---
   Appended: "Hello, World!"
   ...
```

### 2. Live Activity Stream (`live_activity_stream.exs`)

An interactive Phoenix LiveView application demonstrating:
- Real-time event streaming
- Multi-client synchronization
- JSON mode with structured events
- Stream creation and switching

**Run:**
```bash
cd /path/to/durable_streams
iex examples/live_activity_stream.exs
```

Then open http://localhost:4000 in your browser.

**Try this:**
1. Open the URL in multiple browser tabs
2. Post an event in one tab
3. Watch it appear instantly in all tabs!

## Dependencies

These examples use [Phoenix Playground](https://github.com/phoenix-playground/phoenix_playground) to create single-file Phoenix applications without needing a full project setup.

The examples automatically install their dependencies via `Mix.install/1` - no manual setup required.
