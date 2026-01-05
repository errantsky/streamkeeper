# LLM Token Streaming - Resumable AI Responses with DurableStreams
#
# This example demonstrates the primary use case from the Durable Streams
# announcement: "token streaming is the UI for chat and copilots"
#
# Key features showcased:
#
# 1. RESUMABLE STREAMING - Disconnect mid-response and resume without losing tokens
#    (connection breaks in AI apps are common: tab switches, network flaps, refreshes)
#
# 2. MULTI-CLIENT BROADCAST - Multiple tabs watch the same AI response in real-time
#    (no per-client session state on server)
#
# 3. REPLAY CAPABILITY - Re-watch the entire response from the beginning
#    (useful for debugging, logging, or showing responses to late joiners)
#
# Requirements:
#   - Set ANTHROPIC_API_KEY environment variable
#
# Run with: iex examples/llm_streaming.exs
# Then open: http://localhost:4000
#
# Try:
#   1. Submit a prompt and watch tokens stream in
#   2. Click "Disconnect" mid-stream, then "Resume" - no tokens lost!
#   3. Open another tab and click "Replay" to watch from the beginning
#   4. Open multiple tabs and submit a prompt - all tabs see the same stream

Mix.install([
  {:phoenix_playground, "~> 0.1.8"},
  {:phoenix_live_view, "~> 1.0"},
  {:durable_streams, path: "."},
  {:req, "~> 0.5"}
])

defmodule LLMStreamingLive do
  use Phoenix.LiveView

  @default_prompt "Explain how durable streams make AI applications more reliable, in about 3 paragraphs."

  def mount(_params, _session, socket) do
    # Initialize socket state - handle_params will handle URL params after mount
    socket =
      socket
      |> assign(:stream_id, nil)
      |> assign(:tokens, [])
      |> assign(:status, :idle)
      |> assign(:listener_ref, nil)
      |> assign(:listener_monitor, nil)
      |> assign(:offset, "-1")
      |> assign(:token_count, 0)
      |> assign(:resumed_from, nil)
      |> assign(:prompt, @default_prompt)
      |> assign(:error, nil)
      |> assign(:generation_status, nil)

    {:ok, socket}
  end

  # Handle URL parameter changes (for push_patch and page refresh)
  def handle_params(params, _uri, socket) do
    # Only join a stream when connected and we don't already have one
    socket =
      if connected?(socket) do
        case params do
          %{"stream" => stream_id} when stream_id != "" ->
            if socket.assigns.stream_id != stream_id do
              join_existing_stream(socket, stream_id)
            else
              socket
            end
          _ ->
            socket
        end
      else
        socket
      end

    {:noreply, socket}
  end

  # Join an existing stream (for page refresh or sharing)
  defp join_existing_stream(socket, stream_id) do
    case DurableStreams.get_metadata(stream_id) do
      {:ok, meta} ->
        socket
        |> assign(:stream_id, stream_id)
        |> assign(:generation_status, if(meta.closed, do: "Generation complete", else: "Joined existing stream"))
        |> start_listener()

      {:error, :not_found} ->
        assign(socket, :error, "Stream not found: #{stream_id}")
    end
  end

  def terminate(_reason, socket) do
    stop_listener(socket)
    :ok
  end

  def render(assigns) do
    ~H"""
    <style>
      * { box-sizing: border-box; }
      body { margin: 0; background: #0a0a0f; }
      .container { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 1000px; margin: 0 auto; padding: 24px; color: #e2e8f0; }
      h1 { color: #f8fafc; margin-bottom: 8px; font-size: 28px; }
      .subtitle { color: #94a3b8; margin-bottom: 32px; font-size: 15px; line-height: 1.5; }
      .card { background: #151520; border-radius: 12px; padding: 24px; margin-bottom: 20px; border: 1px solid #2a2a3a; }
      .card h3 { margin: 0 0 16px 0; color: #f1f5f9; font-size: 16px; font-weight: 600; }
      .status-bar { display: flex; gap: 16px; align-items: center; flex-wrap: wrap; margin-bottom: 16px; }
      .status { display: flex; align-items: center; gap: 8px; font-size: 14px; }
      .status-dot { width: 10px; height: 10px; border-radius: 50%; }
      .status-dot.streaming { background: #22c55e; box-shadow: 0 0 10px #22c55e; animation: pulse 1s infinite; }
      .status-dot.connected { background: #22c55e; }
      .status-dot.disconnected { background: #64748b; }
      .status-dot.idle { background: #64748b; }
      @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.6; } }
      .stat { background: #0a0a0f; padding: 8px 14px; border-radius: 8px; font-size: 13px; }
      .stat-label { color: #64748b; }
      .stat-value { color: #a78bfa; font-weight: 600; margin-left: 6px; font-family: monospace; }
      .btn { padding: 10px 20px; border: none; border-radius: 8px; cursor: pointer; font-weight: 500; font-size: 14px; transition: all 0.2s; }
      .btn:disabled { opacity: 0.5; cursor: not-allowed; }
      .btn-primary { background: linear-gradient(135deg, #8b5cf6, #6366f1); color: white; }
      .btn-primary:hover:not(:disabled) { background: linear-gradient(135deg, #7c3aed, #4f46e5); }
      .btn-danger { background: #dc2626; color: white; }
      .btn-danger:hover:not(:disabled) { background: #b91c1c; }
      .btn-success { background: #16a34a; color: white; }
      .btn-success:hover:not(:disabled) { background: #15803d; }
      .btn-secondary { background: #374151; color: white; }
      .btn-secondary:hover:not(:disabled) { background: #4b5563; }
      textarea { background: #0a0a0f; border: 1px solid #2a2a3a; color: #e2e8f0; padding: 16px; border-radius: 8px; font-size: 14px; width: 100%; resize: vertical; min-height: 80px; font-family: inherit; line-height: 1.5; }
      textarea:focus { outline: none; border-color: #6366f1; }
      textarea::placeholder { color: #64748b; }
      .response-area { background: #0a0a0f; border-radius: 8px; padding: 20px; min-height: 200px; max-height: 400px; overflow-y: auto; font-size: 15px; line-height: 1.7; white-space: pre-wrap; word-wrap: break-word; }
      .cursor { display: inline-block; width: 2px; height: 1em; background: #a78bfa; animation: blink 1s step-end infinite; margin-left: 2px; vertical-align: text-bottom; }
      @keyframes blink { 0%, 100% { opacity: 1; } 50% { opacity: 0; } }
      .empty-state { color: #64748b; font-style: italic; }
      .error { background: #7f1d1d; border: 1px solid #dc2626; color: #fecaca; padding: 12px 16px; border-radius: 8px; margin-bottom: 16px; font-size: 14px; }
      .feature-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; margin-top: 16px; }
      .feature-box { background: #0a0a0f; border-radius: 8px; padding: 16px; border-left: 3px solid #6366f1; }
      .feature-title { color: #a78bfa; font-weight: 600; margin-bottom: 8px; font-size: 14px; }
      .feature-desc { color: #94a3b8; font-size: 13px; line-height: 1.5; }
      .resumed-notice { background: linear-gradient(135deg, #1e1b4b, #1e3a5f); border: 1px solid #4f46e5; border-radius: 8px; padding: 12px 16px; margin-bottom: 16px; font-size: 14px; }
      .resumed-notice strong { color: #a78bfa; }
      .btn-group { display: flex; gap: 8px; flex-wrap: wrap; }
      .stream-id { font-family: monospace; font-size: 12px; color: #64748b; background: #0a0a0f; padding: 4px 8px; border-radius: 4px; }
      .join-form { display: flex; gap: 8px; margin-top: 16px; padding-top: 16px; border-top: 1px solid #2a2a3a; }
      .join-form input { flex: 1; background: #0a0a0f; border: 1px solid #2a2a3a; color: #e2e8f0; padding: 8px 12px; border-radius: 6px; font-size: 13px; font-family: monospace; }
      .join-form input:focus { outline: none; border-color: #6366f1; }
      .join-form input::placeholder { color: #64748b; }
      .share-url { background: #0a0a0f; border: 1px solid #2a2a3a; border-radius: 6px; padding: 8px 12px; font-family: monospace; font-size: 12px; color: #a78bfa; margin-top: 12px; word-break: break-all; }
    </style>

    <div class="container">
      <h1>LLM Token Streaming</h1>
      <p class="subtitle">
        Resumable AI responses with DurableStreams — disconnect mid-stream and resume without losing tokens.
        <br/>Open multiple tabs to see the same stream broadcast to all clients.
      </p>

      <%= if @error do %>
        <div class="error"><%= @error %></div>
      <% end %>

      <!-- Prompt Input Card -->
      <div class="card">
        <h3>Prompt</h3>
        <form phx-submit="submit_prompt">
          <textarea name="prompt" placeholder="Enter your prompt..." phx-update="ignore" id="prompt-input"><%= @prompt %></textarea>
          <div style="margin-top: 12px; display: flex; justify-content: space-between; align-items: center;">
            <button type="submit" class="btn btn-primary" disabled={@status in [:streaming, :connecting]}>
              <%= if @status == :streaming, do: "Generating...", else: "Generate Response" %>
            </button>
            <%= if @stream_id do %>
              <span class="stream-id">Stream: <%= truncate_id(@stream_id) %></span>
            <% end %>
          </div>
        </form>

        <%= if @stream_id do %>
          <div class="share-url">
            Share URL: <%= "http://localhost:4000/?stream=#{@stream_id}" %>
          </div>
        <% else %>
          <form phx-submit="join_stream" class="join-form">
            <input type="text" name="stream_id" placeholder="Enter stream ID to join existing stream..." />
            <button type="submit" class="btn btn-secondary">Join Stream</button>
          </form>
        <% end %>
      </div>

      <!-- Response Card -->
      <div class="card">
        <h3>Response</h3>

        <div class="status-bar">
          <div class="status">
            <div class={"status-dot #{status_class(@status)}"}></div>
            <span><%= status_text(@status) %></span>
          </div>

          <div class="stat">
            <span class="stat-label">Tokens:</span>
            <span class="stat-value"><%= @token_count %></span>
          </div>

          <div class="stat">
            <span class="stat-label">Offset:</span>
            <span class="stat-value"><%= truncate_offset(@offset) %></span>
          </div>

          <div class="btn-group">
            <%= if @status == :streaming or @status == :connected do %>
              <button class="btn btn-danger" phx-click="disconnect">Disconnect</button>
            <% end %>
            <%= if @status == :disconnected and @stream_id do %>
              <button class="btn btn-success" phx-click="resume">Resume</button>
              <button class="btn btn-secondary" phx-click="replay">Replay from Start</button>
            <% end %>
          </div>
        </div>

        <%= if @resumed_from do %>
          <div class="resumed-notice">
            <strong>Resumed from offset <%= truncate_offset(@resumed_from) %></strong> —
            No tokens were lost! The stream remembers your position.
          </div>
        <% end %>

        <div class="response-area" id="response-area">
          <%= if @tokens == [] do %>
            <span class="empty-state">Response will appear here as tokens stream in...</span>
          <% else %>
            <%= for token <- Enum.reverse(@tokens) do %><%= token %><% end %><%= if @status == :streaming do %><span class="cursor"></span><% end %>
          <% end %>
        </div>

        <%= if @generation_status do %>
          <div style="margin-top: 12px; font-size: 13px; color: #64748b;">
            <%= @generation_status %>
          </div>
        <% end %>
      </div>

      <!-- Features Explanation -->
      <div class="card" style="background: #12121a;">
        <h3>What makes this special?</h3>
        <div class="feature-grid">
          <div class="feature-box">
            <div class="feature-title">Resumable Streaming</div>
            <div class="feature-desc">
              Click "Disconnect" mid-response, then "Resume" — you continue exactly where you left off.
              No tokens are lost because each has a durable offset.
            </div>
          </div>
          <div class="feature-box">
            <div class="feature-title">Multi-Client Broadcast</div>
            <div class="feature-desc">
              Open this page in multiple tabs. All tabs see the same token stream in real-time.
              The server doesn't track per-client state — the stream itself is durable.
            </div>
          </div>
          <div class="feature-box">
            <div class="feature-title">Replay Capability</div>
            <div class="feature-desc">
              Click "Replay from Start" to re-watch the entire response. Late joiners can see
              the full AI response, not just what arrived after they connected.
            </div>
          </div>
          <div class="feature-box">
            <div class="feature-title">CDN-Friendly</div>
            <div class="feature-desc">
              Offset-based URLs enable edge caching. Scale to thousands of clients watching
              the same AI response without overwhelming your origin server.
            </div>
          </div>
        </div>
      </div>
    </div>
    """
  end

  # Submit a new prompt
  def handle_event("submit_prompt", %{"prompt" => prompt}, socket) do
    if prompt == "" do
      {:noreply, assign(socket, :error, "Please enter a prompt")}
    else
      api_key = System.get_env("ANTHROPIC_API_KEY")

      if is_nil(api_key) or api_key == "" do
        {:noreply, assign(socket, :error, "ANTHROPIC_API_KEY environment variable not set")}
      else
        # Create a new stream for this response
        stream_id = "llm-response-#{:erlang.system_time(:millisecond)}"

        case DurableStreams.create(stream_id, content_type: "application/json") do
          {:ok, _} ->
            # Start the LLM streaming in a background process
            parent = self()
            spawn(fn -> stream_claude_response(parent, stream_id, prompt, api_key) end)

            socket =
              socket
              |> assign(:stream_id, stream_id)
              |> assign(:tokens, [])
              |> assign(:token_count, 0)
              |> assign(:offset, "-1")
              |> assign(:error, nil)
              |> assign(:resumed_from, nil)
              |> assign(:prompt, prompt)
              |> assign(:generation_status, "Starting generation...")
              |> start_listener()
              |> push_patch(to: "/?stream=#{stream_id}")

            {:noreply, socket}

          {:error, reason} ->
            {:noreply, assign(socket, :error, "Failed to create stream: #{inspect(reason)}")}
        end
      end
    end
  end

  def handle_event("disconnect", _, socket) do
    socket =
      socket
      |> stop_listener()
      |> assign(:status, :disconnected)

    {:noreply, socket}
  end

  def handle_event("resume", _, socket) do
    resumed_from = socket.assigns.offset

    socket =
      socket
      |> assign(:resumed_from, resumed_from)
      |> start_listener()

    {:noreply, socket}
  end

  def handle_event("replay", _, socket) do
    socket =
      socket
      |> assign(:tokens, [])
      |> assign(:token_count, 0)
      |> assign(:offset, "-1")
      |> assign(:resumed_from, nil)
      |> start_listener()

    {:noreply, socket}
  end

  def handle_event("join_stream", %{"stream_id" => stream_id}, socket) do
    stream_id = String.trim(stream_id)

    if stream_id == "" do
      {:noreply, assign(socket, :error, "Please enter a stream ID")}
    else
      socket =
        socket
        |> assign(:error, nil)
        |> push_patch(to: "/?stream=#{stream_id}")

      {:noreply, socket}
    end
  end

  # Handle messages from the listener process
  def handle_info({:listener_status, status}, socket) do
    {:noreply, assign(socket, :status, status)}
  end

  def handle_info({:new_tokens, messages, new_offset}, socket) do
    new_tokens =
      messages
      |> Enum.map(fn msg ->
        case DurableStreams.JSON.decode(msg.data) do
          {:ok, %{"token" => token}} -> token
          {:ok, %{"status" => "complete"}} -> ""
          _ -> ""
        end
      end)
      |> Enum.filter(&(&1 != ""))

    socket =
      socket
      |> assign(:tokens, new_tokens ++ socket.assigns.tokens)
      |> assign(:token_count, socket.assigns.token_count + length(new_tokens))
      |> assign(:offset, new_offset)

    {:noreply, socket}
  end

  def handle_info({:generation_status, status}, socket) do
    {:noreply, assign(socket, :generation_status, status)}
  end

  def handle_info({:stream_complete}, socket) do
    socket =
      socket
      |> assign(:status, :disconnected)
      |> assign(:generation_status, "Generation complete")

    {:noreply, stop_listener(socket)}
  end

  def handle_info({:stream_error, error}, socket) do
    {:noreply, assign(socket, :error, error)}
  end

  # Handle listener process dying unexpectedly
  def handle_info({:DOWN, _ref, :process, _pid, reason}, socket) do
    if reason != :normal and reason != :killed do
      {:noreply, assign(socket, :status, :disconnected)}
    else
      {:noreply, socket}
    end
  end

  # Ignore any stray EXIT messages (from old linked processes)
  def handle_info({:EXIT, _pid, _reason}, socket) do
    {:noreply, socket}
  end

  # Start the long-poll listener (uses spawn + monitor instead of spawn_link)
  defp start_listener(socket) do
    # First stop any existing listener
    socket = stop_listener(socket)

    stream_id = socket.assigns.stream_id
    offset = socket.assigns.offset

    if stream_id do
      parent = self()
      pid = spawn(fn -> long_poll_loop(parent, stream_id, offset) end)
      monitor_ref = Process.monitor(pid)

      socket
      |> assign(:listener_ref, pid)
      |> assign(:listener_monitor, monitor_ref)
      |> assign(:status, :connecting)
    else
      socket
    end
  end

  defp stop_listener(socket) do
    # Demonitor first to avoid receiving DOWN message
    if socket.assigns[:listener_monitor] do
      Process.demonitor(socket.assigns.listener_monitor, [:flush])
    end

    # Then kill the process
    if socket.assigns[:listener_ref] do
      Process.exit(socket.assigns.listener_ref, :kill)
    end

    socket
    |> assign(:listener_ref, nil)
    |> assign(:listener_monitor, nil)
  end

  # Long-poll loop
  defp long_poll_loop(parent, stream_id, offset) do
    send(parent, {:listener_status, :streaming})
    do_long_poll(parent, stream_id, offset)
  end

  defp do_long_poll(parent, stream_id, offset) do
    case DurableStreams.StreamManager.read_messages(stream_id, offset, live: true, timeout: 30_000) do
      {:ok, %{messages: [], closed: true}} ->
        send(parent, {:stream_complete})

      {:ok, %{messages: [], offset: new_offset}} ->
        do_long_poll(parent, stream_id, new_offset)

      {:ok, %{messages: messages, offset: new_offset, closed: closed}} ->
        send(parent, {:new_tokens, messages, new_offset})

        if closed do
          send(parent, {:stream_complete})
        else
          do_long_poll(parent, stream_id, new_offset)
        end

      {:error, :not_found} ->
        send(parent, {:listener_status, :disconnected})

      {:error, _reason} ->
        send(parent, {:listener_status, :disconnected})
    end
  end

  # Stream Claude response into the durable stream
  defp stream_claude_response(parent, stream_id, prompt, api_key) do
    send(parent, {:generation_status, "Connecting to Claude API..."})

    url = "https://api.anthropic.com/v1/messages"

    headers = [
      {"x-api-key", api_key},
      {"anthropic-version", "2023-06-01"},
      {"content-type", "application/json"}
    ]

    body =
      DurableStreams.JSON.encode!(%{
        "model" => "claude-sonnet-4-20250514",
        "max_tokens" => 1024,
        "stream" => true,
        "messages" => [
          %{"role" => "user", "content" => prompt}
        ]
      })

    # Use Req for streaming
    case Req.post(url,
           headers: headers,
           body: body,
           into: fn {:data, chunk}, acc -> handle_sse_chunk(parent, stream_id, chunk, acc) end,
           receive_timeout: 60_000
         ) do
      {:ok, _response} ->
        # Mark stream as complete
        DurableStreams.append(stream_id, DurableStreams.JSON.encode!(%{"status" => "complete"}))
        DurableStreams.close(stream_id)
        send(parent, {:generation_status, "Generation complete"})

      {:error, %Req.TransportError{reason: reason}} ->
        send(parent, {:stream_error, "Connection error: #{inspect(reason)}"})

      {:error, reason} ->
        send(parent, {:stream_error, "API error: #{inspect(reason)}"})
    end
  end

  # Handle SSE chunks from Claude API
  defp handle_sse_chunk(parent, stream_id, chunk, acc) do
    # SSE format: "event: ...\ndata: {...}\n\n"
    lines = String.split(chunk, "\n")

    Enum.each(lines, fn line ->
      if String.starts_with?(line, "data: ") do
        json_str = String.trim_leading(line, "data: ")

        if json_str != "[DONE]" do
          case DurableStreams.JSON.decode(json_str) do
            {:ok, %{"type" => "content_block_delta", "delta" => %{"text" => text}}} ->
              # Store each token in the durable stream
              token_msg = DurableStreams.JSON.encode!(%{"token" => text})
              DurableStreams.append(stream_id, token_msg)
              send(parent, {:generation_status, "Streaming tokens..."})

            {:ok, %{"type" => "message_start"}} ->
              send(parent, {:generation_status, "Response started..."})

            {:ok, %{"type" => "message_stop"}} ->
              send(parent, {:generation_status, "Response complete"})

            _ ->
              :ok
          end
        end
      end
    end)

    {:cont, acc}
  end

  # Helpers
  defp status_class(:streaming), do: "streaming"
  defp status_class(:connected), do: "connected"
  defp status_class(:connecting), do: "streaming"
  defp status_class(_), do: "idle"

  defp status_text(:streaming), do: "Streaming (long-polling)"
  defp status_text(:connected), do: "Connected"
  defp status_text(:connecting), do: "Connecting..."
  defp status_text(:disconnected), do: "Disconnected"
  defp status_text(:idle), do: "Idle"

  defp truncate_offset("-1"), do: "(start)"

  defp truncate_offset(offset) when is_binary(offset) and byte_size(offset) > 16 do
    String.slice(offset, 0, 6) <> "..." <> String.slice(offset, -6, 6)
  end

  defp truncate_offset(offset), do: offset

  defp truncate_id(id) when byte_size(id) > 24 do
    String.slice(id, 0, 24) <> "..."
  end

  defp truncate_id(id), do: id
end

# Startup message
IO.puts("""

\e[1;35m══════════════════════════════════════════════════════════════\e[0m
\e[1;37m  LLM Token Streaming Demo\e[0m
\e[1;35m══════════════════════════════════════════════════════════════\e[0m

\e[1;32m  ➜  Open:\e[0m  \e[4mhttp://localhost:4000\e[0m

\e[33m  Features demonstrated:\e[0m
    • Resumable token streaming (disconnect mid-response, resume)
    • Multi-client broadcast (open multiple tabs)
    • Replay capability (re-watch from the beginning)

\e[33m  Requirements:\e[0m
    • ANTHROPIC_API_KEY environment variable must be set

\e[33m  Try this:\e[0m
    1. Enter a prompt and click Generate
    2. Mid-stream, click Disconnect, then Resume → no tokens lost!
    3. Open another tab and click Replay to watch from start

\e[1;35m══════════════════════════════════════════════════════════════\e[0m
  Press Ctrl+C twice to stop.
\e[1;35m══════════════════════════════════════════════════════════════\e[0m
""")

# Check for API key
if System.get_env("ANTHROPIC_API_KEY") do
  IO.puts("\e[32m  ✓ ANTHROPIC_API_KEY is set\e[0m\n")
else
  IO.puts("\e[31m  ✗ ANTHROPIC_API_KEY not set - please set it before using\e[0m")
  IO.puts("\e[33m    export ANTHROPIC_API_KEY=your-key-here\e[0m\n")
end

PhoenixPlayground.start(live: LLMStreamingLive)
