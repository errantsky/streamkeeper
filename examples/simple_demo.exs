# Simple DurableStreams Demo
#
# This example demonstrates the core DurableStreams API:
# - Creating streams
# - Appending data
# - Reading data
# - Long-polling for updates
# - Stream cleanup
#
# Run with: elixir examples/simple_demo.exs
# (from the durable_streams project root)

Mix.install([
  {:durable_streams, path: "."}
])

defmodule SimpleDemo do
  @moduledoc """
  A simple demonstration of DurableStreams functionality.
  """

  def run do
    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("DurableStreams Simple Demo")
    IO.puts(String.duplicate("=", 60) <> "\n")

    # 1. Create a text stream
    demo_create_stream()

    # 2. Append data
    demo_append_data()

    # 3. Read data
    demo_read_data()

    # 4. Long-polling
    demo_long_polling()

    # 5. JSON mode
    demo_json_mode()

    # 6. Cleanup
    demo_cleanup()

    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("Demo complete!")
    IO.puts(String.duplicate("=", 60) <> "\n")
  end

  defp demo_create_stream do
    section("1. Creating a Stream")

    case DurableStreams.create("demo-stream", content_type: "text/plain") do
      {:ok, stream_id} ->
        IO.puts("   Created stream: #{stream_id}")

      {:error, :already_exists} ->
        IO.puts("   Stream already exists (cleaning up first...)")
        DurableStreams.delete("demo-stream")
        {:ok, stream_id} = DurableStreams.create("demo-stream", content_type: "text/plain")
        IO.puts("   Created stream: #{stream_id}")
    end

    # Show metadata
    {:ok, meta} = DurableStreams.get_metadata("demo-stream")
    IO.puts("   Content-Type: #{meta.content_type}")
    IO.puts("   Created at: #{meta.created_at}")
  end

  defp demo_append_data do
    section("2. Appending Data")

    messages = ["Hello, World!", "This is message 2", "And message 3"]

    for msg <- messages do
      {:ok, offset} = DurableStreams.append("demo-stream", msg)
      IO.puts("   Appended: #{inspect(msg)}")
      IO.puts("   Offset: #{offset}")
    end
  end

  defp demo_read_data do
    section("3. Reading Data")

    # Read from beginning
    {:ok, result} = DurableStreams.read("demo-stream", "-1")
    IO.puts("   Reading from offset -1 (beginning):")
    IO.puts("   Data: #{inspect(result.data)}")
    IO.puts("   Next offset: #{result.offset}")
    IO.puts("   Has more: #{result.has_more}")

    # Read from last offset (should be empty)
    {:ok, result2} = DurableStreams.read("demo-stream", result.offset)
    IO.puts("\n   Reading from last offset (should be empty):")
    IO.puts("   Data: #{inspect(result2.data)}")
  end

  defp demo_long_polling do
    section("4. Long-Polling")

    IO.puts("   Starting a long-poll reader in background...")

    # Get current offset
    {:ok, result} = DurableStreams.read("demo-stream", "-1")
    current_offset = result.offset

    # Start a reader that waits for new data
    reader = Task.async(fn ->
      IO.puts("   [Reader] Waiting for new data...")
      start = System.monotonic_time(:millisecond)
      {:ok, result} = DurableStreams.read("demo-stream", current_offset, live: true, timeout: 5000)
      elapsed = System.monotonic_time(:millisecond) - start
      {result, elapsed}
    end)

    # Wait a bit, then append new data
    :timer.sleep(500)
    IO.puts("   [Writer] Appending new data...")
    {:ok, _} = DurableStreams.append("demo-stream", "New data from long-poll demo!")

    # Wait for reader to receive it
    {result, elapsed} = Task.await(reader, 6000)
    IO.puts("   [Reader] Received after #{elapsed}ms: #{inspect(result.data)}")
  end

  defp demo_json_mode do
    section("5. JSON Mode")

    # Clean up first if exists
    DurableStreams.delete("json-demo")

    # Create JSON stream
    {:ok, _} = DurableStreams.create("json-demo", content_type: "application/json")
    IO.puts("   Created JSON stream")

    # In JSON mode, each append is stored as a distinct message
    # (Array flattening happens at the HTTP layer, not the internal API)
    events = [
      %{type: "user_login", user: "alice"},
      %{type: "purchase", amount: 99.99},
      %{type: "user_logout", user: "alice"}
    ]

    IO.puts("   Appending #{length(events)} events individually:")
    for event <- events do
      json_data = :json.encode(event) |> IO.iodata_to_binary()
      {:ok, _} = DurableStreams.append("json-demo", json_data)
      IO.puts("   - #{json_data}")
    end

    # Read back as messages (each stored separately)
    {:ok, result} = DurableStreams.StreamManager.read_messages("json-demo", "-1")
    IO.puts("\n   Reading back as messages:")
    for {msg, i} <- Enum.with_index(result.messages, 1) do
      IO.puts("   Message #{i}: #{msg.data}")
    end
    IO.puts("\n   Note: Via HTTP API, POST'ing an array flattens it automatically")

    # Cleanup
    DurableStreams.delete("json-demo")
  end

  defp demo_cleanup do
    section("6. Cleanup")

    IO.puts("   Closing stream (prevents further appends)...")
    :ok = DurableStreams.close("demo-stream")
    {:ok, meta} = DurableStreams.get_metadata("demo-stream")
    IO.puts("   Stream closed: #{meta.closed}")

    IO.puts("   Deleting stream...")
    :ok = DurableStreams.delete("demo-stream")
    IO.puts("   Stream deleted!")

    # Verify deletion
    case DurableStreams.get_metadata("demo-stream") do
      {:error, :not_found} -> IO.puts("   Verified: stream no longer exists")
      _ -> IO.puts("   Warning: stream still exists")
    end
  end

  defp section(title) do
    IO.puts("\n--- #{title} ---\n")
  end
end

# Run the demo
SimpleDemo.run()
