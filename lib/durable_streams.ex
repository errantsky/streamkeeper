defmodule DurableStreams do
  @moduledoc """
  Durable Streams - Elixir implementation of the Durable Streams protocol.

  Durable Streams provides an append-only, URL-addressable byte log with
  support for long-polling and Server-Sent Events.

  ## Phoenix Integration

      # In your router
      forward "/v1/stream", DurableStreams.Protocol.Plug

  ## Programmatic API

      {:ok, _} = DurableStreams.create("my-stream", content_type: "text/plain")
      {:ok, offset} = DurableStreams.append("my-stream", "Hello!")
      {:ok, result} = DurableStreams.read("my-stream", "-1")
      # result.data => "Hello!"

  ## HTTP API

  | Method | Path | Purpose |
  |--------|------|---------|
  | `PUT` | `/v1/stream/:id` | Create stream |
  | `POST` | `/v1/stream/:id` | Append data |
  | `GET` | `/v1/stream/:id?offset=X` | Read from offset |
  | `GET` | `/v1/stream/:id?offset=X&live=true` | Long-poll for new data |
  | `GET` | `/v1/stream/:id?offset=X&live=sse` | Server-Sent Events |
  | `DELETE` | `/v1/stream/:id` | Delete stream |
  | `HEAD` | `/v1/stream/:id` | Get metadata |
  """

  alias DurableStreams.StreamManager

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
  defdelegate read(stream_id, offset, opts \\ []), to: StreamManager

  @doc """
  Closes a stream, preventing further appends.

  Reading from a closed stream will still work, but the response
  will include `closed: true` in the result.

  ## Examples

      :ok = DurableStreams.close("my-stream")
  """
  defdelegate close(stream_id), to: StreamManager

  @doc """
  Deletes a stream and all its data.

  ## Examples

      :ok = DurableStreams.delete("my-stream")
      {:error, :not_found} = DurableStreams.delete("nonexistent")
  """
  defdelegate delete(stream_id), to: StreamManager

  @doc """
  Gets the metadata for a stream.

  ## Examples

      {:ok, meta} = DurableStreams.get_metadata("my-stream")
      # meta.id => "my-stream"
      # meta.content_type => "text/plain"
      # meta.created_at => DateTime
      # meta.closed => false
      # meta.ttl => nil
  """
  defdelegate get_metadata(stream_id), to: StreamManager
end
