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
#   - Set ANTHROPIC_API_KEY and/or OPENAI_API_KEY environment variable
#   - If both are set, one is randomly chosen for each generation
#
# Run with: iex examples/llm_streaming.exs
# Then open: http://localhost:4000
#
# Try:
#   1. Submit a prompt and watch tokens stream in
#   2. Click "Disconnect" mid-stream, then "Resume" - no tokens lost!
#   3. Open another tab and click "Replay" to watch from the beginning
#   4. Open multiple tabs and submit a prompt - all tabs see the same stream
#
# Note: This example uses Phoenix Playground with hot-reload guards to support
# multiple browser tabs and page refreshes during streaming.

Mix.install([
  {:phoenix_playground, "~> 0.1.8"},
  {:phoenix_live_view, "~> 1.0"},
  {:streamkeeper, path: "."},
  {:req, "~> 0.5"}
])

require Logger

# Only define the module if it doesn't already exist (prevents redefinition on hot reload)
unless Code.ensure_loaded?(LLMStreamingLive) do
defmodule LLMStreamingLive do
  use Phoenix.LiveView
  require Logger

  alias DurableStreams.LiveView, as: DSLive

  @default_prompt "Explain how durable streams make AI applications more reliable, in about 3 paragraphs."

  @mock_response_text """
  Durable streams fundamentally transform how AI applications handle the inherent unreliability of network connections. When a user is watching an AI response stream in, any interruption—whether from a network hiccup, a tab switch on mobile, or simply closing their laptop—traditionally means losing that response entirely. The user must start over, wasting both time and compute resources. Durable streams solve this by giving every token a unique, persistent offset, turning an ephemeral stream into a replayable log.

  The architecture is elegantly simple: instead of streaming tokens directly to clients, the AI backend appends each token to a durable stream. Clients then read from this stream using their last known offset. If they disconnect and reconnect, they simply resume from where they left off—no tokens are lost, no regeneration is needed. This also enables powerful new patterns: multiple clients can watch the same response simultaneously, late joiners can replay from the beginning, and responses can be cached at the edge using standard HTTP semantics.

  For production AI applications, this reliability is transformative. Consider a coding assistant generating a complex solution, or a customer service bot providing detailed instructions. Losing these responses mid-stream creates frustration and costs money. With durable streams, the infrastructure handles the complexity of connection management, letting developers focus on building great AI experiences. The protocol is simple HTTP—no WebSocket complexity, no special client libraries—just standard web infrastructure that scales naturally.

  Beyond reliability, durable streams unlock observability and debugging capabilities that are impossible with ephemeral streams. Every token is logged with its offset, making it trivial to replay exactly what a user saw, investigate issues, or analyze response patterns. This durability-first approach represents a fundamental shift in how we think about real-time AI: not as fragile streams of data, but as persistent, addressable logs that clients can navigate at will.
  """

  def mount(_params, _session, socket) do
    # Initialize stream listener state via DSLive, plus app-specific state
    socket =
      socket
      |> DSLive.init()
      |> assign(:tokens, [])
      |> assign(:token_count, 0)
      |> assign(:resumed_from, nil)
      |> assign(:prompt, @default_prompt)
      |> assign(:error, nil)
      |> assign(:generation_status, nil)
      |> assign(:demo_mode, false)

    {:ok, socket}
  end

  # Handle URL parameter changes (for push_patch and page refresh)
  def handle_params(params, _uri, socket) do
    # Only handle stream changes when connected
    socket =
      if connected?(socket) do
        case params do
          %{"stream" => stream_id} when stream_id != "" ->
            if DSLive.stream_id(socket) != stream_id do
              join_existing_stream(socket, stream_id)
            else
              socket
            end
          _ ->
            # No stream param - reset state if we had a stream before
            if DSLive.stream_id(socket) do
              socket
              |> DSLive.reset()
              |> assign(:tokens, [])
              |> assign(:token_count, 0)
              |> assign(:generation_status, nil)
              |> assign(:resumed_from, nil)
              |> assign(:error, nil)
              |> assign(:demo_mode, false)
            else
              socket
            end
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
        status_msg = if meta.closed do
          "Stream complete (closed)"
        else
          "Joined stream - waiting for new tokens..."
        end

        socket
        |> assign(:generation_status, status_msg)
        |> DSLive.listen(stream_id)

      {:error, :not_found} ->
        assign(socket, :error, "Stream not found: #{stream_id}")
    end
  end

  def terminate(_reason, socket) do
    DSLive.stop(socket)
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
      .demo-banner { background: linear-gradient(135deg, #78350f, #451a03); border: 1px solid #d97706; color: #fef3c7; padding: 12px 16px; border-radius: 8px; margin-bottom: 16px; font-size: 14px; }
      .demo-banner strong { color: #fbbf24; }
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

      <%= if @demo_mode do %>
        <div class="demo-banner">
          <strong>Demo Mode</strong> — Using simulated responses. Set ANTHROPIC_API_KEY or OPENAI_API_KEY for real AI responses.
        </div>
      <% end %>

      <!-- Prompt Input Card -->
      <div class="card">
        <h3>Prompt</h3>
        <form phx-submit="submit_prompt">
          <textarea name="prompt" placeholder="Enter your prompt..." phx-update="ignore" id="prompt-input"><%= @prompt %></textarea>
          <div style="margin-top: 12px; display: flex; justify-content: space-between; align-items: center;">
            <button type="submit" class="btn btn-primary" disabled={@ds_status in [:streaming, :connecting]}>
              <%= if @ds_status == :streaming, do: "Generating...", else: "Generate Response" %>
            </button>
            <%= if @ds_stream_id do %>
              <span class="stream-id">Stream: <%= truncate_id(@ds_stream_id) %></span>
            <% end %>
          </div>
        </form>

        <%= if @ds_stream_id do %>
          <div class="share-url">
            Share URL: <%= "http://localhost:4000/?stream=#{@ds_stream_id}" %>
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
            <div class={"status-dot #{status_class(@ds_status)}"}></div>
            <span><%= status_text(@ds_status) %></span>
          </div>

          <div class="stat">
            <span class="stat-label">Tokens:</span>
            <span class="stat-value"><%= @token_count %></span>
          </div>

          <div class="stat">
            <span class="stat-label">Offset:</span>
            <span class="stat-value"><%= truncate_offset(@ds_offset) %></span>
          </div>

          <div class="btn-group">
            <%= if @ds_status == :streaming or @ds_status == :connecting do %>
              <button class="btn btn-danger" phx-click="disconnect">Disconnect</button>
            <% end %>
            <%= if @ds_status == :disconnected and @ds_stream_id do %>
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
            <%= for token <- Enum.reverse(@tokens) do %><%= token %><% end %><%= if @ds_status == :streaming do %><span class="cursor"></span><% end %>
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
      # Check which API keys are available
      anthropic_key = System.get_env("ANTHROPIC_API_KEY")
      openai_key = System.get_env("OPENAI_API_KEY")

      available_providers =
        []
        |> then(fn list -> if valid_key?(anthropic_key), do: [{:anthropic, anthropic_key} | list], else: list end)
        |> then(fn list -> if valid_key?(openai_key), do: [{:openai, openai_key} | list], else: list end)

      # Fall back to mock provider if no API keys are set
      {provider, api_key} =
        if available_providers == [] do
          {:mock, nil}
        else
          Enum.random(available_providers)
        end

      # Create a new stream for this response
      stream_id = "llm-response-#{:erlang.system_time(:millisecond)}"

      case DurableStreams.create(stream_id, content_type: "application/json") do
        {:ok, _} ->
          # Start the LLM streaming in a background process
          parent = self()
          spawn(fn -> stream_llm_response(parent, stream_id, prompt, provider, api_key) end)

          provider_label = provider_display_name(provider)

          socket =
            socket
            |> assign(:tokens, [])
            |> assign(:token_count, 0)
            |> assign(:error, nil)
            |> assign(:resumed_from, nil)
            |> assign(:prompt, prompt)
            |> assign(:generation_status, "Starting generation (#{provider_label})...")
            |> assign(:demo_mode, provider == :mock)
            |> DSLive.listen(stream_id)
            |> push_patch(to: "/?stream=#{stream_id}")

          {:noreply, socket}

        {:error, reason} ->
          {:noreply, assign(socket, :error, "Failed to create stream: #{inspect(reason)}")}
      end
    end
  end

  defp provider_display_name(:anthropic), do: "Claude"
  defp provider_display_name(:openai), do: "OpenAI"
  defp provider_display_name(:mock), do: "Demo"

  defp valid_key?(nil), do: false
  defp valid_key?(""), do: false
  defp valid_key?(_), do: true

  def handle_event("disconnect", _, socket) do
    {:noreply, DSLive.stop(socket)}
  end

  def handle_event("resume", _, socket) do
    stream_id = DSLive.stream_id(socket)
    resumed_from = DSLive.offset(socket)

    socket =
      socket
      |> assign(:resumed_from, resumed_from)
      |> DSLive.listen(stream_id)

    {:noreply, socket}
  end

  def handle_event("replay", _, socket) do
    stream_id = DSLive.stream_id(socket)

    socket =
      socket
      |> assign(:tokens, [])
      |> assign(:token_count, 0)
      |> assign(:resumed_from, nil)
      |> DSLive.listen(stream_id, offset: "-1")

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

  # Handle messages from the stream listener (via DSLive)
  def handle_info(msg, socket) do
    if DSLive.stream_message?(msg) do
      case DSLive.handle_message(socket, msg) do
        {:data, messages, socket} ->
          {:noreply, process_stream_messages(socket, messages)}

        {:status, _status, socket} ->
          {:noreply, socket}

        {:complete, socket} ->
          {:noreply, assign(socket, :generation_status, "Generation complete")}

        {:error, reason, socket} ->
          {:noreply, assign(socket, :error, "Stream error: #{inspect(reason)}")}
      end
    else
      handle_other_info(msg, socket)
    end
  end

  defp handle_other_info({:generation_status, status}, socket) do
    {:noreply, assign(socket, :generation_status, status)}
  end

  defp handle_other_info({:stream_error, error}, socket) do
    {:noreply, assign(socket, :error, error)}
  end

  defp handle_other_info({:EXIT, _pid, _reason}, socket) do
    {:noreply, socket}
  end

  defp handle_other_info(_msg, socket) do
    {:noreply, socket}
  end

  # Process incoming stream messages and extract tokens
  defp process_stream_messages(socket, messages) do
    {new_tokens, error} =
      Enum.reduce(messages, {[], nil}, fn msg, {tokens, err} ->
        case DurableStreams.JSON.decode(msg.data) do
          {:ok, %{"token" => token}} -> {[token | tokens], err}
          {:ok, %{"status" => "complete"}} -> {tokens, err}
          {:ok, %{"status" => "error", "error" => error_msg}} -> {tokens, error_msg}
          _ -> {tokens, err}
        end
      end)

    # Tokens were collected in reverse order, so reverse back
    new_tokens = Enum.reverse(new_tokens)

    # Tokens arrive in chronological order [oldest, ..., newest]
    # We store as [newest, ..., oldest] so prepend reversed new_tokens
    socket =
      socket
      |> assign(:tokens, Enum.reverse(new_tokens) ++ socket.assigns.tokens)
      |> assign(:token_count, socket.assigns.token_count + length(new_tokens))

    if error, do: assign(socket, :error, error), else: socket
  end

  # Dispatch to appropriate LLM provider
  defp stream_llm_response(parent, stream_id, _prompt, :mock, _api_key) do
    stream_mock_response(parent, stream_id)
  end

  defp stream_llm_response(parent, stream_id, prompt, :anthropic, api_key) do
    stream_claude_response(parent, stream_id, prompt, api_key)
  end

  defp stream_llm_response(parent, stream_id, prompt, :openai, api_key) do
    stream_openai_response(parent, stream_id, prompt, api_key)
  end

  # Stream mock response with simulated delays
  defp stream_mock_response(parent, stream_id) do
    Logger.info("[LLM] Starting mock streaming for stream: #{stream_id}")
    send(parent, {:generation_status, "Connecting to Demo API..."})

    # Small initial delay to simulate connection
    Process.sleep(200)
    send(parent, {:generation_status, "Response started..."})

    # Split text into tokens (words and punctuation)
    tokens = tokenize_for_streaming(@mock_response_text)

    # Stream each token with realistic delays
    Enum.each(tokens, fn token ->
      token_msg = DurableStreams.JSON.encode!(%{"token" => token})
      DurableStreams.append(stream_id, token_msg)
      send(parent, {:generation_status, "Streaming tokens (Demo)..."})

      # Variable delay: 20-60ms per token, slower for punctuation
      delay = if String.match?(token, ~r/[.!?,;:]$/), do: :rand.uniform(80) + 40, else: :rand.uniform(40) + 20
      Process.sleep(delay)
    end)

    # Mark stream as complete
    Logger.info("[LLM] Mock streaming completed successfully")
    DurableStreams.append(stream_id, DurableStreams.JSON.encode!(%{"status" => "complete"}))
    DurableStreams.close(stream_id)
    send(parent, {:generation_status, "Generation complete"})
  end

  # Tokenize text for realistic streaming (preserves spaces with words)
  defp tokenize_for_streaming(text) do
    # Split into words but keep leading spaces attached
    text
    |> String.split(~r/(?=\s)/)
    |> Enum.flat_map(fn part ->
      case part do
        "" -> []
        _ -> [part]
      end
    end)
  end

  # Stream Claude response into the durable stream
  defp stream_claude_response(parent, stream_id, prompt, api_key) do
    Logger.info("[LLM] Starting Claude API request for stream: #{stream_id}")
    send(parent, {:generation_status, "Connecting to Claude API..."})

    url = "https://api.anthropic.com/v1/messages"

    headers = [
      {"x-api-key", api_key},
      {"anthropic-version", "2023-06-01"},
      {"content-type", "application/json"}
    ]

    body =
      DurableStreams.JSON.encode!(%{
        "model" => "claude-haiku-4-5",
        "max_tokens" => 1024,
        "stream" => true,
        "messages" => [
          %{"role" => "user", "content" => prompt}
        ]
      })

    stream_with_sse(parent, stream_id, url, headers, body, :anthropic)
  end

  # Stream OpenAI response into the durable stream
  defp stream_openai_response(parent, stream_id, prompt, api_key) do
    Logger.info("[LLM] Starting OpenAI API request for stream: #{stream_id}")
    send(parent, {:generation_status, "Connecting to OpenAI API..."})

    url = "https://api.openai.com/v1/chat/completions"

    headers = [
      {"authorization", "Bearer #{api_key}"},
      {"content-type", "application/json"}
    ]

    body =
      DurableStreams.JSON.encode!(%{
        "model" => "gpt-4o-mini",
        "max_tokens" => 1024,
        "stream" => true,
        "messages" => [
          %{"role" => "user", "content" => prompt}
        ]
      })

    stream_with_sse(parent, stream_id, url, headers, body, :openai)
  end

  # Common SSE streaming logic
  defp stream_with_sse(parent, stream_id, url, headers, body, provider) do
    try do
      case Req.post(url,
             headers: headers,
             body: body,
             into: fn {:data, chunk}, acc -> handle_sse_chunk(parent, stream_id, chunk, acc, provider) end,
             receive_timeout: 60_000
           ) do
        {:ok, _response} ->
          Logger.info("[LLM] #{provider} API streaming completed successfully")
          DurableStreams.append(stream_id, DurableStreams.JSON.encode!(%{"status" => "complete"}))
          DurableStreams.close(stream_id)
          send(parent, {:generation_status, "Generation complete"})

        {:error, %Req.TransportError{reason: reason}} ->
          Logger.error("[LLM] #{provider} API transport error: #{inspect(reason)}")
          DurableStreams.append(stream_id, DurableStreams.JSON.encode!(%{"status" => "error", "error" => "Connection error: #{inspect(reason)}"}))
          DurableStreams.close(stream_id)
          send(parent, {:stream_error, "Connection error: #{inspect(reason)}"})

        {:error, reason} ->
          Logger.error("[LLM] #{provider} API error: #{inspect(reason)}")
          DurableStreams.append(stream_id, DurableStreams.JSON.encode!(%{"status" => "error", "error" => "API error: #{inspect(reason)}"}))
          DurableStreams.close(stream_id)
          send(parent, {:stream_error, "API error: #{inspect(reason)}"})
      end
    rescue
      e ->
        Logger.error("[LLM] Exception during #{provider} API call: #{inspect(e)}")
        DurableStreams.append(stream_id, DurableStreams.JSON.encode!(%{"status" => "error", "error" => "Exception: #{inspect(e)}"}))
        DurableStreams.close(stream_id)
    end
  end

  # Handle SSE chunks from LLM APIs
  defp handle_sse_chunk(parent, stream_id, chunk, acc, provider) do
    Logger.debug("[LLM] SSE chunk: #{byte_size(chunk)} bytes\n#{chunk}")

    # Check if this is a direct JSON error (not SSE format)
    # This happens when the API rejects the request before streaming starts
    trimmed = String.trim(chunk)
    if String.starts_with?(trimmed, "{") and String.contains?(trimmed, "error") do
      case DurableStreams.JSON.decode(trimmed) do
        {:ok, %{"type" => "error", "error" => %{"message" => message}}} ->
          Logger.error("[LLM] API error: #{message}")
          send(parent, {:stream_error, message})
          {:cont, acc}

        {:ok, %{"error" => %{"message" => message}}} ->
          Logger.error("[LLM] API error: #{message}")
          send(parent, {:stream_error, message})
          {:cont, acc}

        _ ->
          parse_sse_lines(parent, stream_id, chunk, provider)
          {:cont, acc}
      end
    else
      parse_sse_lines(parent, stream_id, chunk, provider)
      {:cont, acc}
    end
  end

  # Parse SSE format: "event: ...\ndata: {...}\n\n"
  defp parse_sse_lines(parent, stream_id, chunk, provider) do
    lines = String.split(chunk, "\n")

    Enum.each(lines, fn line ->
      if String.starts_with?(line, "data: ") do
        json_str = String.trim_leading(line, "data: ")

        if json_str != "[DONE]" do
          case DurableStreams.JSON.decode(json_str) do
            {:ok, data} ->
              handle_sse_data(parent, stream_id, data, provider)

            {:error, _} ->
              Logger.warning("[LLM] Failed to parse SSE data: #{json_str}")
          end
        end
      end
    end)
  end

  # Handle Claude SSE data
  defp handle_sse_data(parent, stream_id, %{"type" => "content_block_delta", "delta" => %{"text" => text}}, :anthropic) do
    token_msg = DurableStreams.JSON.encode!(%{"token" => text})
    DurableStreams.append(stream_id, token_msg)
    send(parent, {:generation_status, "Streaming tokens (Claude)..."})
  end

  defp handle_sse_data(parent, _stream_id, %{"type" => "message_start"}, :anthropic) do
    Logger.debug("[LLM] Claude message started")
    send(parent, {:generation_status, "Response started..."})
  end

  defp handle_sse_data(parent, _stream_id, %{"type" => "message_stop"}, :anthropic) do
    Logger.debug("[LLM] Claude message stopped")
    send(parent, {:generation_status, "Response complete"})
  end

  defp handle_sse_data(parent, _stream_id, %{"type" => "error", "error" => %{"message" => message}}, :anthropic) do
    Logger.error("[LLM] Claude API error in stream: #{message}")
    send(parent, {:stream_error, message})
  end

  # Handle OpenAI SSE data
  defp handle_sse_data(parent, stream_id, %{"choices" => [%{"delta" => %{"content" => text}} | _]}, :openai) when is_binary(text) do
    token_msg = DurableStreams.JSON.encode!(%{"token" => text})
    DurableStreams.append(stream_id, token_msg)
    send(parent, {:generation_status, "Streaming tokens (OpenAI)..."})
  end

  defp handle_sse_data(parent, _stream_id, %{"choices" => [%{"delta" => %{"role" => "assistant"}} | _]}, :openai) do
    Logger.debug("[LLM] OpenAI message started")
    send(parent, {:generation_status, "Response started..."})
  end

  defp handle_sse_data(parent, _stream_id, %{"choices" => [%{"finish_reason" => reason} | _]}, :openai) when not is_nil(reason) do
    Logger.debug("[LLM] OpenAI message finished: #{reason}")
    send(parent, {:generation_status, "Response complete"})
  end

  defp handle_sse_data(parent, _stream_id, %{"error" => %{"message" => message}}, :openai) do
    Logger.error("[LLM] OpenAI API error in stream: #{message}")
    send(parent, {:stream_error, message})
  end

  # Catch-all for unhandled SSE events
  defp handle_sse_data(_parent, _stream_id, data, provider) do
    Logger.debug("[LLM] #{provider} SSE event: #{inspect(Map.get(data, "type", "unknown"))}")
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
end  # end unless

# Only print startup message and start server once
unless Process.whereis(PhoenixPlayground.Endpoint) do
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
      • Multi-provider support (Claude and OpenAI)

  \e[33m  Requirements:\e[0m
      • Set ANTHROPIC_API_KEY and/or OPENAI_API_KEY
      • If both are set, one is randomly chosen per request

  \e[33m  Try this:\e[0m
      1. Enter a prompt and click Generate
      2. Mid-stream, click Disconnect, then Resume → no tokens lost!
      3. Open another tab and click Replay to watch from start

  \e[1;35m══════════════════════════════════════════════════════════════\e[0m
    Press Ctrl+C twice to stop.
  \e[1;35m══════════════════════════════════════════════════════════════\e[0m
  """)

  # Check for API keys
  anthropic_set = System.get_env("ANTHROPIC_API_KEY") != nil
  openai_set = System.get_env("OPENAI_API_KEY") != nil

  if anthropic_set do
    IO.puts("\e[32m  ✓ ANTHROPIC_API_KEY is set (Claude)\e[0m")
  else
    IO.puts("\e[33m  ○ ANTHROPIC_API_KEY not set\e[0m")
  end

  if openai_set do
    IO.puts("\e[32m  ✓ OPENAI_API_KEY is set (GPT)\e[0m")
  else
    IO.puts("\e[33m  ○ OPENAI_API_KEY not set\e[0m")
  end

  if not anthropic_set and not openai_set do
    IO.puts("\e[36m  ✓ Demo mode available (no API keys needed)\e[0m")
    IO.puts("\e[90m    Set API keys for real AI responses:\e[0m")
    IO.puts("\e[90m    export ANTHROPIC_API_KEY=your-key-here\e[0m")
    IO.puts("\e[90m    export OPENAI_API_KEY=your-key-here\e[0m")
  end

  IO.puts("")

  # Disable live reload to prevent script re-evaluation
  Application.put_env(:phoenix_playground, PhoenixPlayground.Endpoint, [
    live_reload: [patterns: []],
    code_reloader: false
  ])

  PhoenixPlayground.start(
    live: LLMStreamingLive,
    open_browser: false
  )
end
