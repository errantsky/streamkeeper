# Live Activity Stream - Real-time DurableStreams Demo
#
# This example showcases DurableStreams' key features:
#
# 1. LONG-POLLING - Server holds connection until new data arrives
#    (no wasteful client polling - true server push)
#
# 2. RESUME FROM OFFSET - Disconnect and reconnect without missing events
#    (offsets are durable cursors into the stream)
#
# 3. JSON MODE - Structured events stored as discrete messages
#
# Run with: iex examples/live_activity_stream.exs
# Then open: http://localhost:4000
#
# Try: Open multiple tabs, post events, disconnect/reconnect!

Mix.install([
  {:phoenix_playground, "~> 0.1.8"},
  {:phoenix_live_view, "~> 1.0"},
  {:durable_streams, path: "."}
])

defmodule ActivityStreamLive do
  use Phoenix.LiveView

  @stream_id "activity-stream"

  def mount(_params, _session, socket) do
    # Ensure stream exists
    case DurableStreams.create(@stream_id, content_type: "application/json") do
      {:ok, _} -> :ok
      {:error, :already_exists} -> :ok
    end

    socket =
      socket
      |> assign(:events, [])
      |> assign(:offset, "-1")
      |> assign(:status, :disconnected)
      |> assign(:listener_ref, nil)
      |> assign(:events_received, 0)
      |> assign(:last_reconnect, nil)

    # Auto-connect when LiveView mounts
    socket = if connected?(socket), do: start_listener(socket), else: socket

    {:ok, socket}
  end

  def terminate(_reason, socket) do
    stop_listener(socket)
    :ok
  end

  def render(assigns) do
    ~H"""
    <style>
      * { box-sizing: border-box; }
      body { margin: 0; background: #0f172a; }
      .container { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 900px; margin: 0 auto; padding: 20px; color: #e2e8f0; }
      h1 { color: #f8fafc; margin-bottom: 5px; font-size: 28px; }
      .subtitle { color: #94a3b8; margin-bottom: 30px; font-size: 16px; }
      .card { background: #1e293b; border-radius: 12px; padding: 24px; margin-bottom: 20px; border: 1px solid #334155; }
      .card h3 { margin: 0 0 16px 0; color: #f1f5f9; font-size: 16px; font-weight: 600; }
      .status-bar { display: flex; gap: 20px; align-items: center; flex-wrap: wrap; }
      .status { display: flex; align-items: center; gap: 8px; font-size: 14px; }
      .status-dot { width: 10px; height: 10px; border-radius: 50%; }
      .status-dot.connected { background: #22c55e; box-shadow: 0 0 8px #22c55e; }
      .status-dot.disconnected { background: #ef4444; }
      .status-dot.connecting { background: #eab308; animation: pulse 1s infinite; }
      @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
      .stat { background: #0f172a; padding: 8px 16px; border-radius: 8px; font-size: 13px; }
      .stat-label { color: #64748b; }
      .stat-value { color: #38bdf8; font-weight: 600; margin-left: 6px; font-family: monospace; }
      .btn { padding: 10px 20px; border: none; border-radius: 8px; cursor: pointer; font-weight: 500; font-size: 14px; transition: all 0.2s; }
      .btn-primary { background: #3b82f6; color: white; }
      .btn-primary:hover { background: #2563eb; }
      .btn-danger { background: #dc2626; color: white; }
      .btn-danger:hover { background: #b91c1c; }
      .btn-success { background: #16a34a; color: white; }
      .btn-success:hover { background: #15803d; }
      input[type="text"] { background: #0f172a; border: 1px solid #334155; color: #e2e8f0; padding: 12px 16px; border-radius: 8px; font-size: 14px; width: 100%; }
      input[type="text"]:focus { outline: none; border-color: #3b82f6; }
      input[type="text"]::placeholder { color: #64748b; }
      .form-row { display: flex; gap: 12px; margin-bottom: 12px; }
      .form-row input { flex: 1; }
      .events-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; }
      .events-list { max-height: 400px; overflow-y: auto; }
      .event-item { padding: 16px; border-bottom: 1px solid #334155; display: flex; justify-content: space-between; align-items: flex-start; }
      .event-item:last-child { border-bottom: none; }
      .event-type { background: #1d4ed8; color: #bfdbfe; padding: 4px 10px; border-radius: 6px; font-size: 12px; font-weight: 500; }
      .event-message { color: #e2e8f0; margin-left: 12px; }
      .event-meta { color: #64748b; font-size: 12px; text-align: right; }
      .event-offset { font-family: monospace; font-size: 11px; color: #475569; }
      .empty-state { text-align: center; padding: 60px 20px; color: #64748b; }
      .feature-box { background: #0f172a; border-radius: 8px; padding: 16px; margin-top: 16px; }
      .feature-title { color: #22c55e; font-weight: 600; margin-bottom: 8px; font-size: 14px; }
      .feature-desc { color: #94a3b8; font-size: 13px; line-height: 1.5; }
      .offset-display { font-family: monospace; background: #0f172a; padding: 4px 8px; border-radius: 4px; font-size: 12px; color: #38bdf8; }
    </style>

    <div class="container">
      <h1>Live Activity Stream</h1>
      <p class="subtitle">Real-time event streaming with DurableStreams long-polling</p>

      <!-- Connection Status Card -->
      <div class="card">
        <h3>Connection Status</h3>
        <div class="status-bar">
          <div class="status">
            <div class={"status-dot #{@status}"}></div>
            <span><%= status_text(@status) %></span>
          </div>

          <div class="stat">
            <span class="stat-label">Events received:</span>
            <span class="stat-value"><%= @events_received %></span>
          </div>

          <div class="stat">
            <span class="stat-label">Current offset:</span>
            <span class="stat-value"><%= truncate_offset(@offset) %></span>
          </div>

          <%= if @status == :connected do %>
            <button class="btn btn-danger" phx-click="disconnect">Disconnect</button>
          <% else %>
            <button class="btn btn-success" phx-click="reconnect">
              <%= if @offset != "-1", do: "Resume", else: "Connect" %>
            </button>
          <% end %>
        </div>

        <%= if @last_reconnect do %>
          <div class="feature-box">
            <div class="feature-title">Resumed from offset</div>
            <div class="feature-desc">
              Reconnected at <span class="offset-display"><%= truncate_offset(@last_reconnect) %></span> —
              no events were missed! The server remembers your position in the stream.
            </div>
          </div>
        <% end %>
      </div>

      <!-- Post Event Card -->
      <div class="card">
        <h3>Post Event</h3>
        <form phx-submit="post_event">
          <div class="form-row">
            <input type="text" name="type" placeholder="Event type (e.g., user_signup, purchase, alert)" />
          </div>
          <div class="form-row">
            <input type="text" name="message" placeholder="Event message..." />
            <button type="submit" class="btn btn-primary">Post Event</button>
          </div>
        </form>
      </div>

      <!-- Events List Card -->
      <div class="card">
        <div class="events-header">
          <h3 style="margin: 0;">Live Events (<%= length(@events) %>)</h3>
          <%= if @events != [] do %>
            <button class="btn btn-danger" style="padding: 6px 12px; font-size: 12px;" phx-click="clear_events">Clear Display</button>
          <% end %>
        </div>

        <div class="events-list">
          <%= if @events == [] do %>
            <div class="empty-state">
              <p>No events yet.</p>
              <p style="font-size: 13px;">Post an event above, or open another tab and post from there!</p>
            </div>
          <% else %>
            <%= for event <- @events do %>
              <div class="event-item">
                <div>
                  <span class="event-type"><%= event.type %></span>
                  <span class="event-message"><%= event.message %></span>
                </div>
                <div class="event-meta">
                  <div><%= event.time %></div>
                  <div class="event-offset"><%= truncate_offset(event.offset) %></div>
                </div>
              </div>
            <% end %>
          <% end %>
        </div>
      </div>

      <!-- Features Explanation -->
      <div class="card" style="background: #1a2744;">
        <h3>What's happening under the hood?</h3>
        <div style="display: grid; gap: 16px;">
          <div class="feature-box">
            <div class="feature-title">Long-Polling (not client polling!)</div>
            <div class="feature-desc">
              The server holds your connection open until new data arrives.
              No wasteful repeated requests — when you post an event, all connected
              clients receive it instantly because their long-poll requests complete.
            </div>
          </div>
          <div class="feature-box">
            <div class="feature-title">Resume from Offset</div>
            <div class="feature-desc">
              Click "Disconnect" then "Resume" — you won't miss any events posted while disconnected!
              Each event has a unique offset. When you reconnect, you continue from your last position.
            </div>
          </div>
          <div class="feature-box">
            <div class="feature-title">Try This</div>
            <div class="feature-desc">
              Open this page in multiple browser tabs. Post events from one tab and watch them
              appear instantly in all tabs. Then disconnect one tab, post events from another,
              and reconnect — the disconnected tab catches up immediately!
            </div>
          </div>
        </div>
      </div>
    </div>
    """
  end

  # Start long-poll listener
  defp start_listener(socket) do
    parent = self()
    offset = socket.assigns.offset

    ref = spawn_link(fn -> long_poll_loop(parent, @stream_id, offset) end)

    socket
    |> assign(:listener_ref, ref)
    |> assign(:status, :connecting)
  end

  defp stop_listener(socket) do
    if socket.assigns.listener_ref do
      Process.exit(socket.assigns.listener_ref, :shutdown)
    end
    socket
  end

  # Long-poll loop - runs in separate process
  defp long_poll_loop(parent, stream_id, offset) do
    # Notify parent we're connected
    send(parent, {:listener_status, :connected})

    do_long_poll(parent, stream_id, offset)
  end

  defp do_long_poll(parent, stream_id, offset) do
    # This blocks until data arrives or timeout (30s)
    case DurableStreams.StreamManager.read_messages(stream_id, offset, live: true, timeout: 30_000) do
      {:ok, %{messages: [], offset: new_offset}} ->
        # Timeout with no data - keep polling
        do_long_poll(parent, stream_id, new_offset)

      {:ok, %{messages: messages, offset: new_offset}} ->
        # Got data! Send to parent
        send(parent, {:new_events, messages, new_offset})
        do_long_poll(parent, stream_id, new_offset)

      {:error, _reason} ->
        send(parent, {:listener_status, :disconnected})
    end
  end

  # Handle listener status updates
  def handle_info({:listener_status, status}, socket) do
    {:noreply, assign(socket, :status, status)}
  end

  # Handle new events from long-poll
  def handle_info({:new_events, messages, new_offset}, socket) do
    new_events =
      messages
      |> Enum.map(fn msg ->
        case DurableStreams.JSON.decode(msg.data) do
          {:ok, data} ->
            %{
              type: data["type"] || "event",
              message: data["message"] || inspect(data),
              time: data["time"] || "—",
              offset: msg.offset
            }
          _ ->
            %{type: "raw", message: msg.data, time: "—", offset: msg.offset}
        end
      end)

    all_events = new_events ++ socket.assigns.events

    socket =
      socket
      |> assign(:events, Enum.take(all_events, 100))
      |> assign(:offset, new_offset)
      |> assign(:events_received, socket.assigns.events_received + length(messages))

    {:noreply, socket}
  end

  # Post a new event
  def handle_event("post_event", %{"type" => type, "message" => message}, socket) do
    event = %{
      "type" => if(type == "", do: "event", else: type),
      "message" => message,
      "time" => Calendar.strftime(DateTime.utc_now(), "%H:%M:%S")
    }

    DurableStreams.append(@stream_id, DurableStreams.JSON.encode!(event))
    {:noreply, socket}
  end

  # Disconnect from stream
  def handle_event("disconnect", _, socket) do
    socket = stop_listener(socket)
    {:noreply, assign(socket, :status, :disconnected)}
  end

  # Reconnect to stream (resume from last offset)
  def handle_event("reconnect", _, socket) do
    last_reconnect = if socket.assigns.offset != "-1", do: socket.assigns.offset, else: nil
    socket = start_listener(socket)
    {:noreply, assign(socket, :last_reconnect, last_reconnect)}
  end

  # Clear displayed events (but keep offset for resume demo)
  def handle_event("clear_events", _, socket) do
    {:noreply, assign(socket, :events, [])}
  end

  # Helpers
  defp status_text(:connected), do: "Connected (long-polling)"
  defp status_text(:connecting), do: "Connecting..."
  defp status_text(:disconnected), do: "Disconnected"

  defp truncate_offset("-1"), do: "(start)"
  defp truncate_offset(offset) when byte_size(offset) > 20 do
    String.slice(offset, 0, 8) <> "..." <> String.slice(offset, -8, 8)
  end
  defp truncate_offset(offset), do: offset
end

# Startup message
IO.puts("""

\e[1;36m══════════════════════════════════════════════════════════════\e[0m
\e[1;37m  Live Activity Stream Demo\e[0m
\e[1;36m══════════════════════════════════════════════════════════════\e[0m

\e[1;32m  ➜  Open:\e[0m  \e[4mhttp://localhost:4000\e[0m

\e[33m  Features demonstrated:\e[0m
    • Long-polling (server holds connection until data arrives)
    • Resume from offset (reconnect without missing events)
    • JSON mode (structured event storage)

\e[33m  Try this:\e[0m
    1. Open in multiple browser tabs
    2. Post events from one tab → watch them appear in all tabs
    3. Disconnect a tab, post events, then Resume → no events lost!

\e[1;36m══════════════════════════════════════════════════════════════\e[0m
  Press Ctrl+C twice to stop.
\e[1;36m══════════════════════════════════════════════════════════════\e[0m
""")

PhoenixPlayground.start(live: ActivityStreamLive)
