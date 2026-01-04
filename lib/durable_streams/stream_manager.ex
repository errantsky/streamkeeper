defmodule DurableStreams.StreamManager do
  @moduledoc """
  High-level API for managing streams.

  This module provides a clean interface for stream operations,
  handling process lookup and error cases gracefully.
  """

  alias DurableStreams.Server.StreamServer

  @doc """
  Creates a new stream with the given ID and options.

  ## Options

  - `:content_type` - The content type (default: "application/octet-stream")
  - `:ttl` - Time-to-live in seconds (default: nil)
  - `:storage` - Storage backend module (default: DurableStreams.Storage.ETS)

  ## Examples

      {:ok, "my-stream"} = DurableStreams.StreamManager.create("my-stream")
      {:ok, "json-stream"} = DurableStreams.StreamManager.create("json-stream", content_type: "application/json")
  """
  def create(stream_id, opts \\ []) do
    case DynamicSupervisor.start_child(
           DurableStreams.StreamSupervisor,
           {StreamServer, {stream_id, opts}}
         ) do
      {:ok, _pid} -> {:ok, stream_id}
      {:error, {:already_started, _pid}} -> {:error, :already_exists}
      {:error, :already_exists} -> {:error, :already_exists}
      error -> error
    end
  end

  @doc """
  Appends data to a stream.

  Returns `{:ok, offset}` on success, where offset is the position
  of the appended data that can be used for subsequent reads.

  ## Options

  - `:seq` - Optional sequence string for ordering enforcement (default: nil)
  """
  def append(stream_id, data, opts \\ []) do
    StreamServer.append(stream_id, data, opts)
  catch
    :exit, {:noproc, _} -> {:error, :not_found}
  end

  @doc """
  Reads data from a stream starting after the given offset.

  ## Options

  - `:live` - If true, long-polls for new data (default: false)
  - `:timeout` - Timeout in milliseconds for live reads (default: 30_000)

  ## Examples

      {:ok, result} = DurableStreams.StreamManager.read("my-stream", "-1")
      # result.data, result.offset, result.has_more, result.closed
  """
  def read(stream_id, offset, opts \\ []) do
    StreamServer.read(stream_id, offset, opts)
  catch
    :exit, {:noproc, _} -> {:error, :not_found}
  end

  @doc """
  Closes a stream, preventing further appends.

  Reading from a closed stream will still work, but the response
  will include `closed: true` in the result.
  """
  def close(stream_id) do
    StreamServer.close(stream_id)
  catch
    :exit, {:noproc, _} -> {:error, :not_found}
  end

  @doc """
  Deletes a stream and all its data.
  """
  def delete(stream_id) do
    case Registry.lookup(DurableStreams.Registry, stream_id) do
      [{pid, _}] ->
        DynamicSupervisor.terminate_child(DurableStreams.StreamSupervisor, pid)
        DurableStreams.Storage.ETS.delete(stream_id)

      [] ->
        {:error, :not_found}
    end
  end

  @doc """
  Gets the metadata for a stream.
  """
  def get_metadata(stream_id) do
    StreamServer.get_metadata(stream_id)
  catch
    :exit, {:noproc, _} -> {:error, :not_found}
  end

  @doc """
  Reads messages from a stream as a list (for JSON mode).
  Each message is returned separately instead of concatenated.

  ## Options

  - `:live` - If true, long-polls for new messages (default: false)
  - `:timeout` - Timeout in milliseconds for live reads (default: 30_000)
  """
  def read_messages(stream_id, offset, opts \\ []) do
    StreamServer.read_messages(stream_id, offset, opts)
  catch
    :exit, {:noproc, _} -> {:error, :not_found}
  end
end
