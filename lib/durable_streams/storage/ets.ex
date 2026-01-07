defmodule DurableStreams.Storage.ETS do
  @moduledoc """
  ETS-based storage backend for single-node deployments.

  Uses three ETS tables:
  - :durable_streams_meta - Stream metadata
  - :durable_streams_data - Ordered chunks {{stream_id, offset_int}, data, timestamp_ms}
  - :durable_streams_last_seq - Last seq value per stream for ordering enforcement

  The data table key is {stream_id, offset_integer} where offset_integer is derived
  from erlang:unique_integer([:monotonic, :positive]). This enables efficient seeking
  using ets:next/2 since keys are naturally ordered.
  """

  @behaviour DurableStreams.Storage.Behaviour

  use GenServer

  alias DurableStreams.{Stream, Offset}

  @meta_table :durable_streams_meta
  @data_table :durable_streams_data
  @last_seq_table :durable_streams_last_seq

  # Retention-related functions

  @doc """
  Alias for get_metadata/1, used by retention worker.
  """
  @spec get(String.t()) :: {:ok, Stream.t()} | {:error, :not_found}
  def get(stream_id), do: get_metadata(stream_id)

  @doc """
  Returns the timestamp of the first (earliest) message in the stream.
  Uses earliest_offset from metadata for O(1) lookup.
  """
  @spec get_first_message_timestamp(String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def get_first_message_timestamp(stream_id) do
    case :ets.lookup(@meta_table, stream_id) do
      [{^stream_id, %{earliest_offset: nil}}] ->
        {:error, :no_messages}

      [{^stream_id, %{earliest_offset: offset}}] ->
        key = {stream_id, Offset.to_integer(offset)}
        case :ets.lookup(@data_table, key) do
          [{_key, _data, timestamp}] -> {:ok, timestamp}
          [] -> {:error, :no_messages}
        end

      [] ->
        {:error, :not_found}
    end
  end

  @doc """
  Finds the offset of the first message with timestamp >= cutoff.
  Returns nil if no such message exists.
  """
  @spec find_offset_after_timestamp(String.t(), non_neg_integer()) :: String.t() | nil
  def find_offset_after_timestamp(stream_id, cutoff_ms) do
    reduce_stream(stream_id, nil, fn {{_, offset_int}, _, ts}, _acc ->
      if ts >= cutoff_ms, do: {:halt, int_to_offset(offset_int)}, else: {:cont, nil}
    end)
  end

  @doc """
  Finds the offset after skipping n messages from the beginning.
  Used for max_messages retention.
  """
  @spec find_offset_after_n_messages(String.t(), non_neg_integer()) :: String.t() | nil
  def find_offset_after_n_messages(stream_id, n) do
    case reduce_stream(stream_id, 0, fn {{_, offset_int}, _, _}, idx ->
      if idx == n, do: {:halt, {:found, int_to_offset(offset_int)}}, else: {:cont, idx + 1}
    end) do
      {:found, offset} -> offset
      _ -> nil
    end
  end

  @doc """
  Finds the offset after removing at least target_bytes from the beginning.
  Used for max_bytes retention.
  """
  @spec find_offset_after_n_bytes(String.t(), non_neg_integer()) :: String.t() | nil
  def find_offset_after_n_bytes(stream_id, target_bytes) do
    case reduce_stream(stream_id, 0, fn {{_, offset_int}, data, _}, bytes ->
      new_bytes = bytes + byte_size(data)
      if new_bytes >= target_bytes, do: {:halt, {:found, int_to_offset(offset_int)}}, else: {:cont, new_bytes}
    end) do
      {:found, offset} -> offset
      _ -> nil
    end
  end

  @doc """
  Deletes all messages before the given offset.
  Returns {:ok, deleted_count, deleted_bytes} on success.
  """
  @spec delete_messages_before(String.t(), String.t()) ::
          {:ok, non_neg_integer(), non_neg_integer()} | {:error, term()}
  def delete_messages_before(stream_id, new_earliest_offset) do
    new_earliest_int = Offset.to_integer(new_earliest_offset)

    {deleted_count, deleted_bytes} =
      delete_stream_while(stream_id, fn {{_, offset_int}, _, _} ->
        offset_int < new_earliest_int
      end)

    {:ok, deleted_count, deleted_bytes}
  end

  @doc """
  Updates stream metadata after compaction.
  """
  @spec update_after_compaction(
          String.t(),
          String.t(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok | {:error, term()}
  def update_after_compaction(stream_id, new_earliest, deleted_count, deleted_bytes) do
    case :ets.lookup(@meta_table, stream_id) do
      [{^stream_id, stream}] ->
        updated_stream = %{
          stream
          | earliest_offset: new_earliest,
            message_count: max(0, stream.message_count - deleted_count),
            total_bytes: max(0, stream.total_bytes - deleted_bytes)
        }

        :ets.insert(@meta_table, {stream_id, updated_stream})
        :ok

      [] ->
        {:error, :not_found}
    end
  end

  @doc """
  Lists all streams that have a retention policy configured.
  Uses ETS match specification to filter at the ETS level.
  """
  @spec list_streams_with_retention() :: [Stream.t()]
  def list_streams_with_retention do
    # Match spec: {pattern, guards, result}
    # Pattern: {stream_id, stream_struct} where we bind stream to $1
    # Guard: map_get(:retention_policy, $1) != nil
    # Result: return the stream struct ($1)
    match_spec = [{{:_, :"$1"}, [{:"/=", {:map_get, :retention_policy, :"$1"}, nil}], [:"$1"]}]
    :ets.select(@meta_table, match_spec)
  end

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl DurableStreams.Storage.Behaviour
  def create(stream_id, %Stream{} = stream) do
    case :ets.insert_new(@meta_table, {stream_id, stream}) do
      true ->
        :ets.insert(@last_seq_table, {stream_id, nil})
        :ok

      false ->
        {:error, :already_exists}
    end
  end

  @impl DurableStreams.Storage.Behaviour
  def get_metadata(stream_id) do
    case :ets.lookup(@meta_table, stream_id) do
      [{^stream_id, stream}] -> {:ok, stream}
      [] -> {:error, :not_found}
    end
  end

  @impl DurableStreams.Storage.Behaviour
  def append(stream_id, data, seq \\ nil) when is_binary(data) do
    case :ets.lookup(@meta_table, stream_id) do
      [{^stream_id, %Stream{closed: true}}] ->
        {:error, :closed}

      [{^stream_id, stream}] ->
        # Check seq ordering if provided
        case check_seq_ordering(stream_id, seq) do
          :ok ->
            offset = Offset.generate()
            offset_int = Offset.to_integer(offset)
            timestamp = System.system_time(:millisecond)

            # Key is {stream_id, offset_int} for efficient seeking
            :ets.insert(@data_table, {{stream_id, offset_int}, data, timestamp})

            # Update last seq if provided
            if seq, do: :ets.insert(@last_seq_table, {stream_id, seq})

            # Update message_count, total_bytes, current_offset, and earliest_offset (on first append)
            updated_stream = %{
              stream
              | message_count: stream.message_count + 1,
                total_bytes: stream.total_bytes + byte_size(data),
                current_offset: offset,
                earliest_offset: stream.earliest_offset || offset
            }

            :ets.insert(@meta_table, {stream_id, updated_stream})

            # Notify subscribers
            Phoenix.PubSub.broadcast(
              DurableStreams.PubSub,
              "stream:#{stream_id}",
              {:stream_append, stream_id, offset}
            )

            {:ok, offset}

          {:error, :seq_conflict} ->
            {:error, :seq_conflict}
        end

      [] ->
        {:error, :not_found}
    end
  end

  defp check_seq_ordering(_stream_id, nil), do: :ok

  defp check_seq_ordering(stream_id, new_seq) do
    case :ets.lookup(@last_seq_table, stream_id) do
      [{^stream_id, nil}] ->
        :ok

      [{^stream_id, last_seq}] ->
        # Seq must be lexicographically greater than last_seq
        # And must not be equal to last_seq (duplicate rejection)
        if new_seq > last_seq do
          :ok
        else
          {:error, :seq_conflict}
        end

      [] ->
        :ok
    end
  end

  @impl DurableStreams.Storage.Behaviour
  def read(stream_id, from_offset) do
    read_internal(stream_id, from_offset, :binary)
  end

  @impl DurableStreams.Storage.Behaviour
  def close(stream_id) do
    case :ets.lookup(@meta_table, stream_id) do
      [{^stream_id, stream}] ->
        :ets.insert(@meta_table, {stream_id, %{stream | closed: true}})

        Phoenix.PubSub.broadcast(
          DurableStreams.PubSub,
          "stream:#{stream_id}",
          {:stream_closed, stream_id}
        )

        :ok

      [] ->
        {:error, :not_found}
    end
  end

  @impl DurableStreams.Storage.Behaviour
  def delete(stream_id) do
    case :ets.lookup(@meta_table, stream_id) do
      [{^stream_id, _}] ->
        :ets.delete(@meta_table, stream_id)
        # Delete all data entries for this stream using iteration
        delete_all_stream_data(stream_id)
        :ets.delete(@last_seq_table, stream_id)
        :ok

      [] ->
        {:error, :not_found}
    end
  end

  @impl DurableStreams.Storage.Behaviour
  def current_offset(stream_id) do
    case :ets.lookup(@meta_table, stream_id) do
      [{^stream_id, %{current_offset: nil}}] -> {:ok, Offset.start()}
      [{^stream_id, %{current_offset: offset}}] -> {:ok, offset}
      [] -> {:error, :not_found}
    end
  end

  @impl DurableStreams.Storage.Behaviour
  def subscribe(stream_id) do
    Phoenix.PubSub.subscribe(DurableStreams.PubSub, "stream:#{stream_id}")
  end

  @impl DurableStreams.Storage.Behaviour
  def read_messages(stream_id, from_offset) do
    read_internal(stream_id, from_offset, :messages)
  end

  # GenServer callbacks

  @impl GenServer
  def init(_opts) do
    :ets.new(@meta_table, [:set, :public, :named_table, read_concurrency: true, write_concurrency: true])
    :ets.new(@data_table, [:ordered_set, :public, :named_table, read_concurrency: true, write_concurrency: true])
    :ets.new(@last_seq_table, [:set, :public, :named_table, read_concurrency: true, write_concurrency: true])
    {:ok, %{}}
  end

  # Private helpers

  # Shared implementation for read/2 and read_messages/2
  # format: :binary returns concatenated data, :messages returns list of message maps
  defp read_internal(stream_id, from_offset, format) do
    case :ets.lookup(@meta_table, stream_id) do
      [{^stream_id, stream}] ->
        chunks = get_chunks_after(stream_id, from_offset)
        current_off = stream.current_offset || Offset.start()

        case chunks do
          [] ->
            empty_result(current_off, stream.closed, format)

          _ ->
            {last_key, _, _} = List.last(chunks)
            {_, last_offset_int} = last_key
            last_offset = int_to_offset(last_offset_int)
            chunks_result(chunks, last_offset, stream.closed, format)
        end

      [] ->
        {:error, :not_found}
    end
  end

  defp empty_result(offset, closed, :binary) do
    {:ok, %{data: <<>>, offset: offset, has_more: false, closed: closed}}
  end

  defp empty_result(offset, closed, :messages) do
    {:ok, %{messages: [], offset: offset, has_more: false, closed: closed}}
  end

  defp chunks_result(chunks, last_offset, closed, :binary) do
    data = chunks |> Enum.map(fn {_key, d, _ts} -> d end) |> IO.iodata_to_binary()
    {:ok, %{data: data, offset: last_offset, has_more: false, closed: closed}}
  end

  defp chunks_result(chunks, last_offset, closed, :messages) do
    messages =
      Enum.map(chunks, fn {{_sid, offset_int}, data, _ts} ->
        %{data: data, offset: int_to_offset(offset_int)}
      end)

    {:ok, %{messages: messages, offset: last_offset, has_more: false, closed: closed}}
  end

  # Convert offset integer back to string format
  defp int_to_offset(offset_int) do
    :io_lib.format("~16.16.0b", [offset_int]) |> IO.iodata_to_binary()
  end

  # Returns chunks after the given offset using reduce_stream_from
  defp get_chunks_after(stream_id, from_offset) do
    from_int = Offset.to_integer(from_offset) || -1

    reduce_stream_from(stream_id, from_int, [], fn chunk, acc ->
      {:cont, [chunk | acc]}
    end)
    |> Enum.reverse()
  end

  # Delete all data entries for a stream
  defp delete_all_stream_data(stream_id) do
    delete_stream_while(stream_id, fn _ -> true end)
  end

  # Unified stream iteration primitives
  # Works like Enum.reduce_while - callback returns {:halt, result} or {:cont, new_acc}

  # Iterate from beginning of stream
  defp reduce_stream(stream_id, acc, fun) do
    reduce_stream_from(stream_id, -1, acc, fun)
  end

  # Iterate from a specific offset (exclusive - starts AFTER the given offset)
  defp reduce_stream_from(stream_id, from_int, acc, fun) do
    do_reduce_stream({stream_id, from_int}, stream_id, acc, fun)
  end

  defp do_reduce_stream(anchor, stream_id, acc, fun) do
    case :ets.next(@data_table, anchor) do
      :"$end_of_table" ->
        acc

      {^stream_id, _} = key ->
        case :ets.lookup(@data_table, key) do
          [{^key, data, ts}] ->
            case fun.({key, data, ts}, acc) do
              {:halt, result} -> result
              {:cont, new_acc} -> do_reduce_stream(key, stream_id, new_acc, fun)
            end

          [] ->
            do_reduce_stream(key, stream_id, acc, fun)
        end

      _ ->
        acc
    end
  end

  # Delete entries while condition holds, returns {deleted_count, deleted_bytes}
  defp delete_stream_while(stream_id, fun) do
    do_delete_stream({stream_id, -1}, stream_id, fun, 0, 0)
  end

  defp do_delete_stream(anchor, stream_id, fun, count, bytes) do
    case :ets.next(@data_table, anchor) do
      :"$end_of_table" ->
        {count, bytes}

      {^stream_id, _} = key ->
        case :ets.lookup(@data_table, key) do
          [{^key, data, ts}] ->
            if fun.({key, data, ts}) do
              :ets.delete(@data_table, key)
              # Keep same anchor since we deleted the key
              do_delete_stream(anchor, stream_id, fun, count + 1, bytes + byte_size(data))
            else
              {count, bytes}
            end

          [] ->
            do_delete_stream(key, stream_id, fun, count, bytes)
        end

      _ ->
        {count, bytes}
    end
  end
end
