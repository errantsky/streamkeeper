defmodule DurableStreams.Storage.ETS do
  @moduledoc """
  ETS-based storage backend for single-node deployments.

  Uses four ETS tables:
  - :durable_streams_meta - Stream metadata
  - :durable_streams_data - Ordered chunks {{stream_id, sequence}, offset, data}
  - :durable_streams_seq  - Sequence counters for offset generation
  - :durable_streams_last_seq - Last seq value per stream for ordering enforcement
  """

  @behaviour DurableStreams.Storage.Behaviour

  use GenServer

  alias DurableStreams.{Stream, Offset}

  @meta_table :durable_streams_meta
  @data_table :durable_streams_data
  @seq_table :durable_streams_seq
  @last_seq_table :durable_streams_last_seq

  # Retention-related functions

  @doc """
  Alias for get_metadata/1, used by retention worker.
  """
  @spec get(String.t()) :: {:ok, Stream.t()} | {:error, :not_found}
  def get(stream_id), do: get_metadata(stream_id)

  @doc """
  Returns the timestamp of the first (earliest) message in the stream.
  """
  @spec get_first_message_timestamp(String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def get_first_message_timestamp(stream_id) do
    chunks =
      :ets.match_object(@data_table, {{stream_id, :_}, :_, :_})
      |> Enum.sort_by(fn {{_, seq}, _, _} -> seq end)

    case chunks do
      [] ->
        {:error, :no_messages}

      [{_, offset, _} | _] ->
        case Offset.timestamp(offset) do
          nil -> {:error, :invalid_offset}
          ts -> {:ok, ts}
        end
    end
  end

  @doc """
  Finds the offset of the first message with timestamp >= cutoff.
  Returns nil if no such message exists.
  """
  @spec find_offset_after_timestamp(String.t(), non_neg_integer()) :: String.t() | nil
  def find_offset_after_timestamp(stream_id, cutoff_ms) do
    :ets.match_object(@data_table, {{stream_id, :_}, :_, :_})
    |> Enum.sort_by(fn {{_, seq}, _, _} -> seq end)
    |> Enum.find_value(fn {{_, _}, offset, _} ->
      case Offset.timestamp(offset) do
        nil -> nil
        ts when ts >= cutoff_ms -> offset
        _ -> nil
      end
    end)
  end

  @doc """
  Finds the offset after skipping n messages from the beginning.
  Used for max_messages retention.
  """
  @spec find_offset_after_n_messages(String.t(), non_neg_integer()) :: String.t() | nil
  def find_offset_after_n_messages(stream_id, n) do
    chunks =
      :ets.match_object(@data_table, {{stream_id, :_}, :_, :_})
      |> Enum.sort_by(fn {{_, seq}, _, _} -> seq end)

    if length(chunks) > n do
      {{_, _}, offset, _} = Enum.at(chunks, n)
      offset
    else
      nil
    end
  end

  @doc """
  Finds the offset after removing at least target_bytes from the beginning.
  Used for max_bytes retention.
  """
  @spec find_offset_after_n_bytes(String.t(), non_neg_integer()) :: String.t() | nil
  def find_offset_after_n_bytes(stream_id, target_bytes) do
    chunks =
      :ets.match_object(@data_table, {{stream_id, :_}, :_, :_})
      |> Enum.sort_by(fn {{_, seq}, _, _} -> seq end)

    {_, result_offset} =
      Enum.reduce_while(chunks, {0, nil}, fn {{_, _}, offset, data}, {bytes_so_far, _} ->
        new_bytes = bytes_so_far + byte_size(data)

        if new_bytes >= target_bytes do
          {:halt, {new_bytes, offset}}
        else
          {:cont, {new_bytes, nil}}
        end
      end)

    result_offset
  end

  @doc """
  Deletes all messages before the given offset.
  Returns {:ok, deleted_count, deleted_bytes} on success.
  """
  @spec delete_messages_before(String.t(), String.t()) ::
          {:ok, non_neg_integer(), non_neg_integer()} | {:error, term()}
  def delete_messages_before(stream_id, new_earliest_offset) do
    chunks =
      :ets.match_object(@data_table, {{stream_id, :_}, :_, :_})
      |> Enum.filter(fn {{_, _}, offset, _} ->
        Offset.compare(offset, new_earliest_offset) == :lt
      end)

    deleted_count = length(chunks)

    deleted_bytes =
      Enum.reduce(chunks, 0, fn {{_, _}, _, data}, acc ->
        acc + byte_size(data)
      end)

    # Delete each chunk
    Enum.each(chunks, fn {key, _, _} ->
      :ets.delete(@data_table, key)
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
  """
  @spec list_streams_with_retention() :: [Stream.t()]
  def list_streams_with_retention do
    :ets.tab2list(@meta_table)
    |> Enum.map(fn {_, stream} -> stream end)
    |> Enum.filter(fn stream -> stream.retention_policy != nil end)
  end

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl DurableStreams.Storage.Behaviour
  def create(stream_id, %Stream{} = stream) do
    case :ets.insert_new(@meta_table, {stream_id, stream}) do
      true ->
        :ets.insert(@seq_table, {stream_id, 0})
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
            sequence = :ets.update_counter(@seq_table, stream_id, 1)
            offset = Offset.generate(sequence)
            # Key is {stream_id, sequence} to ensure uniqueness in ordered_set
            :ets.insert(@data_table, {{stream_id, sequence}, offset, data})

            # Update last seq if provided
            if seq, do: :ets.insert(@last_seq_table, {stream_id, seq})

            # Update message_count and total_bytes for retention tracking
            updated_stream = %{
              stream
              | message_count: stream.message_count + 1,
                total_bytes: stream.total_bytes + byte_size(data)
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
    case :ets.lookup(@meta_table, stream_id) do
      [{^stream_id, stream}] ->
        chunks = get_chunks_after(stream_id, from_offset)

        case chunks do
          [] ->
            current = get_current_offset_internal(stream_id)
            {:ok, %{data: <<>>, offset: current, has_more: false, closed: stream.closed}}

          _ ->
            data = chunks |> Enum.map(fn {{_, _}, _, d} -> d end) |> IO.iodata_to_binary()
            {{_, _}, last_offset, _} = List.last(chunks)
            {:ok, %{data: data, offset: last_offset, has_more: false, closed: stream.closed}}
        end

      [] ->
        {:error, :not_found}
    end
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
        :ets.match_delete(@data_table, {{stream_id, :_}, :_, :_})
        :ets.delete(@seq_table, stream_id)
        :ets.delete(@last_seq_table, stream_id)
        :ok

      [] ->
        {:error, :not_found}
    end
  end

  @impl DurableStreams.Storage.Behaviour
  def current_offset(stream_id) do
    case :ets.lookup(@meta_table, stream_id) do
      [{^stream_id, _}] -> {:ok, get_current_offset_internal(stream_id)}
      [] -> {:error, :not_found}
    end
  end

  @impl DurableStreams.Storage.Behaviour
  def subscribe(stream_id) do
    Phoenix.PubSub.subscribe(DurableStreams.PubSub, "stream:#{stream_id}")
  end

  @impl DurableStreams.Storage.Behaviour
  def read_messages(stream_id, from_offset) do
    case :ets.lookup(@meta_table, stream_id) do
      [{^stream_id, stream}] ->
        chunks = get_chunks_after(stream_id, from_offset)

        case chunks do
          [] ->
            current = get_current_offset_internal(stream_id)
            {:ok, %{messages: [], offset: current, has_more: false, closed: stream.closed}}

          _ ->
            messages =
              Enum.map(chunks, fn {{_, _}, offset, data} ->
                %{data: data, offset: offset}
              end)

            {{_, _}, last_offset, _} = List.last(chunks)
            {:ok, %{messages: messages, offset: last_offset, has_more: false, closed: stream.closed}}
        end

      [] ->
        {:error, :not_found}
    end
  end

  # GenServer callbacks

  @impl GenServer
  def init(_opts) do
    :ets.new(@meta_table, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(@data_table, [:ordered_set, :public, :named_table, read_concurrency: true])
    :ets.new(@seq_table, [:set, :public, :named_table])
    :ets.new(@last_seq_table, [:set, :public, :named_table])
    {:ok, %{}}
  end

  # Private helpers

  defp get_chunks_after(stream_id, from_offset) do
    # Match all entries for this stream_id with key pattern {{stream_id, seq}, offset, data}
    :ets.match_object(@data_table, {{stream_id, :_}, :_, :_})
    |> Enum.filter(fn {{_, _}, chunk_offset, _} ->
      Offset.after?(chunk_offset, from_offset)
    end)
    |> Enum.sort_by(fn {{_, seq}, _, _} -> seq end)
  end

  defp get_current_offset_internal(stream_id) do
    case :ets.match(@data_table, {{stream_id, :_}, :"$1", :_})
         |> List.flatten()
         |> Enum.max(fn -> nil end) do
      nil -> Offset.start()
      offset -> offset
    end
  end
end
