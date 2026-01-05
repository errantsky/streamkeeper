# Streamkeeper Examples

This directory contains example applications demonstrating Streamkeeper (DurableStreams) functionality.

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
cd /path/to/streamkeeper
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

### 2. LLM Token Streaming (`llm_streaming.exs`)

The flagship example demonstrating the primary use case from the [Durable Streams announcement](https://electric-sql.com/blog/2025/12/09/announcing-durable-streams): **resumable AI token streaming**.

Features demonstrated:
- **Resumable streaming** - Disconnect mid-AI-response and resume without losing tokens
- **Multi-client broadcast** - Multiple tabs watch the same AI response in real-time
- **Replay capability** - Re-watch the entire response from the beginning
- **DurableStreams.LiveView helper** - Shows how to use the DSLive module for easy integration
- **Multi-provider support** - Works with Claude (Anthropic), GPT (OpenAI), or Demo mode

**Run:**
```bash
cd /path/to/streamkeeper
iex examples/llm_streaming.exs
```

Then open http://localhost:4000 in your browser.

**API Keys (Optional):**

The example works without any API keys using **Demo mode**, which simulates streaming with pre-defined text. For real AI responses, set one or both:

```bash
export ANTHROPIC_API_KEY=your-anthropic-key-here
export OPENAI_API_KEY=your-openai-key-here
```

If both are set, one is randomly chosen for each generation.

**Try this:**
1. Enter a prompt and click "Generate Response"
2. Mid-stream, click "Disconnect", then "Resume" → no tokens lost!
3. Open another browser tab and click "Replay from Start" to watch the full response
4. Open multiple tabs before generating → all tabs see tokens stream in real-time

## Dependencies

These examples use [Phoenix Playground](https://github.com/phoenix-playground/phoenix_playground) to create single-file Phoenix applications without needing a full project setup.

The examples automatically install their dependencies via `Mix.install/1` - no manual setup required.
