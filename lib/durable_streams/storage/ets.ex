defmodule DurableStreams.Storage.ETS do
  @moduledoc """
  ETS-based storage backend for single-node deployments.

  Uses three ETS tables:
  - :durable_streams_meta - Stream metadata
  - :durable_streams_data - Ordered chunks {stream_id, sequence, offset, data}
  - :durable_streams_seq  - Sequence counters
  """

  @behaviour DurableStreams.Storage.Behaviour

  use GenServer

  alias DurableStreams.{Stream, Offset}

  @meta_table :durable_streams_meta
  @data_table :durable_streams_data
  @seq_table :durable_streams_seq

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl DurableStreams.Storage.Behaviour
  def create(stream_id, %Stream{} = stream) do
    case :ets.insert_new(@meta_table, {stream_id, stream}) do
      true ->
        :ets.insert(@seq_table, {stream_id, 0})
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
  def append(stream_id, data) when is_binary(data) do
    case :ets.lookup(@meta_table, stream_id) do
      [{^stream_id, %Stream{closed: true}}] ->
        {:error, :closed}

      [{^stream_id, _stream}] ->
        sequence = :ets.update_counter(@seq_table, stream_id, 1)
        offset = Offset.generate(sequence)
        # Key is {stream_id, sequence} to ensure uniqueness in ordered_set
        :ets.insert(@data_table, {{stream_id, sequence}, offset, data})

        # Notify subscribers
        Phoenix.PubSub.broadcast(
          DurableStreams.PubSub,
          "stream:#{stream_id}",
          {:stream_append, stream_id, offset}
        )

        {:ok, offset}

      [] ->
        {:error, :not_found}
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

  # GenServer callbacks

  @impl GenServer
  def init(_opts) do
    :ets.new(@meta_table, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(@data_table, [:ordered_set, :public, :named_table, read_concurrency: true])
    :ets.new(@seq_table, [:set, :public, :named_table])
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
