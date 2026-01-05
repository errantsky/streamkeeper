defmodule DurableStreams do
  @moduledoc """
  Elixir implementation of the Durable Streams protocol.

  Durable Streams provides append-only, URL-addressable byte logs with
  support for long-polling and Server-Sent Events (SSE).

  ## Getting Started

  The library starts automatically as part of your OTP application.
  You can use the programmatic API immediately:

      # Create a stream
      {:ok, "events"} = DurableStreams.create("events", content_type: "text/plain")

      # Append data
      {:ok, offset} = DurableStreams.append("events", "user logged in")

      # Read all data from the beginning
      {:ok, result} = DurableStreams.read("events", "-1")
      result.data  # => "user logged in"

  ## Phoenix Integration

  Forward requests from your Phoenix router to expose the HTTP API:

      # In your router.ex
      forward "/v1/stream", DurableStreams.Protocol.Plug

  ## Standalone HTTP Server

  For standalone usage with Cowboy:

      {:ok, _} = Plug.Cowboy.http(DurableStreams.Protocol.V1Plug, [], port: 4437)

  ## JSON Mode

  Streams with `content_type: "application/json"` operate in JSON mode,
  where arrays are flattened and messages are stored separately:

      {:ok, _} = DurableStreams.create("json-events", content_type: "application/json")
      {:ok, _} = DurableStreams.append("json-events", ~s([{"event": "a"}, {"event": "b"}]))

      # Use read_messages/3 via StreamManager for JSON streams
      {:ok, result} = DurableStreams.StreamManager.read_messages("json-events", "-1")
      # result.messages => [%{data: "{\"event\":\"a\"}", offset: "..."}, ...]

  ## HTTP API Reference

  | Method | Path | Purpose |
  |--------|------|---------|
  | `PUT` | `/:id` | Create stream |
  | `POST` | `/:id` | Append data |
  | `GET` | `/:id?offset=X` | Read from offset |
  | `GET` | `/:id?offset=X&live=true` | Long-poll for new data |
  | `GET` | `/:id?offset=X&live=sse` | Server-Sent Events |
  | `DELETE` | `/:id` | Delete stream |
  | `HEAD` | `/:id` | Get metadata |

  See the [Durable Streams specification](https://github.com/durable-streams/durable-streams)
  for full protocol details.
  """

  alias DurableStreams.StreamManager

  @typedoc "A stream identifier (any string)"
  @type stream_id :: String.t()

  @typedoc "An opaque, lexicographically sortable offset string"
  @type offset :: String.t()

  @typedoc "Result of a read operation"
  @type read_result :: %{
          data: binary(),
          offset: offset(),
          has_more: boolean(),
          closed: boolean()
        }

  @doc """
  Creates a new stream with the given ID and options.

  ## Options

  - `:content_type` - The content type (default: "application/octet-stream")
  - `:ttl` - Time-to-live in seconds (default: nil)

  ## Examples

      {:ok, "my-stream"} = DurableStreams.create("my-stream")
      {:ok, "json-stream"} = DurableStreams.create("json-stream", content_type: "application/json")
      {:error, :already_exists} = DurableStreams.create("my-stream")
  """
  @spec create(stream_id(), keyword()) :: {:ok, stream_id()} | {:error, :already_exists}
  defdelegate create(stream_id, opts \\ []), to: StreamManager

  @doc """
  Appends data to a stream.

  Returns `{:ok, offset}` on success, where offset is the position
  of the appended data that can be used for subsequent reads.

  ## Examples

      {:ok, offset} = DurableStreams.append("my-stream", "Hello!")
      {:error, :not_found} = DurableStreams.append("nonexistent", "data")
      {:error, :closed} = DurableStreams.append("closed-stream", "data")
  """
  @spec append(stream_id(), binary()) ::
          {:ok, offset()} | {:error, :not_found | :closed | :seq_conflict}
  defdelegate append(stream_id, data), to: StreamManager

  @doc """
  Reads data from a stream starting after the given offset.

  Use `"-1"` as the offset to read from the beginning.

  ## Options

  - `:live` - If true, long-polls for new data (default: false)
  - `:timeout` - Timeout in milliseconds for live reads (default: 30_000)

  ## Examples

      {:ok, result} = DurableStreams.read("my-stream", "-1")
      # result.data => binary data
      # result.offset => next offset to use
      # result.has_more => boolean
      # result.closed => boolean
  """
  @spec read(stream_id(), offset(), keyword()) :: {:ok, read_result()} | {:error, :not_found}
  defdelegate read(stream_id, offset, opts \\ []), to: StreamManager

  @doc """
  Closes a stream, preventing further appends.

  Reading from a closed stream will still work, but the response
  will include `closed: true` in the result.

  ## Examples

      :ok = DurableStreams.close("my-stream")
  """
  @spec close(stream_id()) :: :ok | {:error, :not_found}
  defdelegate close(stream_id), to: StreamManager

  @doc """
  Deletes a stream and all its data.

  ## Examples

      :ok = DurableStreams.delete("my-stream")
      {:error, :not_found} = DurableStreams.delete("nonexistent")
  """
  @spec delete(stream_id()) :: :ok | {:error, :not_found}
  defdelegate delete(stream_id), to: StreamManager

  @doc """
  Gets the metadata for a stream.

  Returns stream metadata including content type, creation time, and TTL settings.

  ## Examples

      {:ok, meta} = DurableStreams.get_metadata("my-stream")
      # meta.id => "my-stream"
      # meta.content_type => "text/plain"
      # meta.created_at => DateTime
      # meta.closed => false
      # meta.ttl => nil
  """
  @spec get_metadata(stream_id()) :: {:ok, DurableStreams.Stream.t()} | {:error, :not_found}
  defdelegate get_metadata(stream_id), to: StreamManager
end
