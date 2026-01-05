# Live Activity Stream - Interactive DurableStreams Demo
#
# This example demonstrates DurableStreams with a real-time UI:
# - Create and manage streams from the browser
# - Post events via a form
# - Watch events appear in real-time via long-polling
# - Open multiple tabs to see multi-client sync
#
# Run with: iex examples/live_activity_stream.exs
# Then open: http://localhost:4000
#
# Try opening multiple browser tabs and posting events!

Mix.install([
  {:phoenix_playground, "~> 0.1.8"},
  {:phoenix_live_view, "~> 1.0"},
  {:durable_streams, path: "."}
])

defmodule ActivityStreamLive do
  use Phoenix.LiveView

  @default_stream "activity-stream"
  @poll_interval 1000

  def mount(_params, _session, socket) do
    # Ensure default stream exists
    case DurableStreams.create(@default_stream, content_type: "application/json") do
      {:ok, _} -> :ok
      {:error, :already_exists} -> :ok
    end

    # Start polling for new events
    if connected?(socket) do
      send(self(), :poll)
    end

    socket =
      socket
      |> assign(:stream_id, @default_stream)
      |> assign(:events, [])
      |> assign(:offset, "-1")
      |> assign(:event_type, "")
      |> assign(:event_message, "")
      |> assign(:new_stream_id, "")
      |> assign(:error, nil)

    {:ok, socket}
  end

  def render(assigns) do
    ~H"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px;">
      <h1 style="color: #1a1a2e; margin-bottom: 10px;">Live Activity Stream</h1>
      <p style="color: #666; margin-bottom: 30px;">Real-time event streaming with DurableStreams</p>

      <!-- Stream Selector -->
      <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 20px;">
        <h3 style="margin-top: 0; color: #333;">Current Stream</h3>
        <div style="display: flex; gap: 10px; align-items: center; flex-wrap: wrap;">
          <code style="background: #e9ecef; padding: 8px 16px; border-radius: 4px; font-size: 14px;">
            <%= @stream_id %>
          </code>
          <span style="color: #28a745; font-weight: 500;">
            ● Live
          </span>
        </div>

        <!-- Create New Stream -->
        <form phx-submit="create_stream" style="margin-top: 15px; display: flex; gap: 10px;">
          <input
            type="text"
            name="stream_id"
            value={@new_stream_id}
            placeholder="New stream name..."
            style="flex: 1; padding: 8px 12px; border: 1px solid #ddd; border-radius: 4px;"
          />
          <button type="submit" style="padding: 8px 16px; background: #6c757d; color: white; border: none; border-radius: 4px; cursor: pointer;">
            Create & Switch
          </button>
        </form>
      </div>

      <!-- Error Display -->
      <%= if @error do %>
        <div style="background: #f8d7da; color: #721c24; padding: 12px; border-radius: 4px; margin-bottom: 20px;">
          <%= @error %>
        </div>
      <% end %>

      <!-- Event Form -->
      <div style="background: #fff; border: 1px solid #ddd; padding: 20px; border-radius: 8px; margin-bottom: 20px;">
        <h3 style="margin-top: 0; color: #333;">Post Event</h3>
        <form phx-submit="post_event">
          <div style="display: flex; gap: 10px; margin-bottom: 10px;">
            <input
              type="text"
              name="type"
              value={@event_type}
              placeholder="Event type (e.g., user_action)"
              style="flex: 1; padding: 10px; border: 1px solid #ddd; border-radius: 4px;"
            />
          </div>
          <div style="display: flex; gap: 10px;">
            <input
              type="text"
              name="message"
              value={@event_message}
              placeholder="Event message..."
              style="flex: 1; padding: 10px; border: 1px solid #ddd; border-radius: 4px;"
            />
            <button type="submit" style="padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: 500;">
              Post
            </button>
          </div>
        </form>
      </div>

      <!-- Event List -->
      <div style="background: #fff; border: 1px solid #ddd; border-radius: 8px; overflow: hidden;">
        <div style="background: #f8f9fa; padding: 15px 20px; border-bottom: 1px solid #ddd;">
          <h3 style="margin: 0; color: #333;">Events (<%= length(@events) %>)</h3>
        </div>
        <div style="max-height: 400px; overflow-y: auto;">
          <%= if @events == [] do %>
            <div style="padding: 40px; text-align: center; color: #999;">
              No events yet. Post one above!
            </div>
          <% else %>
            <%= for {event, index} <- Enum.with_index(@events) do %>
              <div style={"padding: 15px 20px; border-bottom: 1px solid #eee; background: #{if rem(index, 2) == 0, do: "#fff", else: "#fafafa"};"}>
                <div style="display: flex; justify-content: space-between; align-items: start;">
                  <div>
                    <span style="background: #e3f2fd; color: #1565c0; padding: 2px 8px; border-radius: 4px; font-size: 12px; font-weight: 500;">
                      <%= event["type"] || "event" %>
                    </span>
                    <span style="color: #333; margin-left: 10px;"><%= event["message"] || inspect(event) %></span>
                  </div>
                  <span style="color: #999; font-size: 12px;"><%= event["time"] || "" %></span>
                </div>
              </div>
            <% end %>
          <% end %>
        </div>
      </div>

      <!-- Instructions -->
      <div style="margin-top: 30px; padding: 20px; background: #f8f9fa; border-radius: 8px; color: #666; font-size: 14px;">
        <strong>Try this:</strong> Open this page in multiple browser tabs. Post an event in one tab
        and watch it appear instantly in all tabs!
      </div>
    </div>
    """
  end

  # Post a new event
  def handle_event("post_event", %{"type" => type, "message" => message}, socket) do
    event = %{
      "type" => if(type == "", do: "event", else: type),
      "message" => message,
      "time" => Calendar.strftime(DateTime.utc_now(), "%H:%M:%S")
    }

    case DurableStreams.append(socket.assigns.stream_id, DurableStreams.JSON.encode!(event)) do
      {:ok, _offset} ->
        {:noreply, socket |> assign(:event_type, "") |> assign(:event_message, "") |> assign(:error, nil)}

      {:error, reason} ->
        {:noreply, assign(socket, :error, "Failed to post: #{inspect(reason)}")}
    end
  end

  # Create a new stream and switch to it
  def handle_event("create_stream", %{"stream_id" => stream_id}, socket) when stream_id != "" do
    case DurableStreams.create(stream_id, content_type: "application/json") do
      {:ok, _} ->
        {:noreply, socket |> assign(:stream_id, stream_id) |> assign(:events, []) |> assign(:offset, "-1") |> assign(:new_stream_id, "") |> assign(:error, nil)}

      {:error, :already_exists} ->
        # Stream exists, just switch to it
        {:noreply, socket |> assign(:stream_id, stream_id) |> assign(:events, []) |> assign(:offset, "-1") |> assign(:new_stream_id, "") |> assign(:error, nil)}
    end
  end

  def handle_event("create_stream", _, socket), do: {:noreply, socket}

  # Poll for new events
  def handle_info(:poll, socket) do
    Process.send_after(self(), :poll, @poll_interval)

    case DurableStreams.StreamManager.read_messages(socket.assigns.stream_id, socket.assigns.offset) do
      {:ok, %{messages: []}} ->
        {:noreply, socket}

      {:ok, result} ->
        new_events =
          result.messages
          |> Enum.map(fn msg ->
            case DurableStreams.JSON.decode(msg.data) do
              {:ok, event} -> event
              _ -> %{"message" => msg.data}
            end
          end)

        all_events = new_events ++ socket.assigns.events
        {:noreply, socket |> assign(:events, Enum.take(all_events, 100)) |> assign(:offset, result.offset)}

      {:error, _} ->
        {:noreply, socket}
    end
  end
end

# Start the application
IO.puts("""

===============================================
  Live Activity Stream Demo
===============================================

Open your browser to: http://localhost:4000

Try opening multiple tabs and posting events!
Events sync in real-time across all tabs.

Press Ctrl+C twice to stop.
===============================================
""")

PhoenixPlayground.start(live: ActivityStreamLive)
