defmodule DurableStreams.Server.StreamServer do
  @moduledoc """
  GenServer managing a single stream's lifecycle.

  OTP Concepts:
  - GenServer for stateful stream management
  - handle_call for synchronous operations
  - handle_info for pubsub messages and timeouts
  - Process registration via Registry
  """

  use GenServer, restart: :transient

  alias DurableStreams.Stream

  defstruct [:stream_id, :storage, :waiters]

  # Client API

  def start_link({stream_id, opts}) do
    GenServer.start_link(__MODULE__, {stream_id, opts}, name: via_tuple(stream_id))
  end

  def append(stream_id, data) do
    GenServer.call(via_tuple(stream_id), {:append, data})
  end

  def read(stream_id, offset, opts \\ []) do
    live = Keyword.get(opts, :live, false)
    timeout = Keyword.get(opts, :timeout, 30_000)

    GenServer.call(via_tuple(stream_id), {:read, offset, live, timeout}, timeout + 5_000)
  end

  def close(stream_id) do
    GenServer.call(via_tuple(stream_id), :close)
  end

  def get_metadata(stream_id) do
    GenServer.call(via_tuple(stream_id), :get_metadata)
  end

  # Server callbacks

  @impl GenServer
  def init({stream_id, opts}) do
    storage = Keyword.get(opts, :storage, DurableStreams.Storage.ETS)
    content_type = Keyword.get(opts, :content_type, "application/octet-stream")
    ttl = Keyword.get(opts, :ttl)

    stream = Stream.new(stream_id, content_type: content_type, ttl: ttl)

    case storage.create(stream_id, stream) do
      :ok ->
        storage.subscribe(stream_id)
        if ttl, do: Process.send_after(self(), :ttl_expired, ttl * 1000)
        {:ok, %__MODULE__{stream_id: stream_id, storage: storage, waiters: []}}

      {:error, :already_exists} ->
        {:stop, :already_exists}
    end
  end

  @impl GenServer
  def handle_call({:append, data}, _from, state) do
    result = state.storage.append(state.stream_id, data)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call({:read, offset, false, _timeout}, _from, state) do
    result = state.storage.read(state.stream_id, offset)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call({:read, offset, true, timeout}, from, state) do
    case state.storage.read(state.stream_id, offset) do
      {:ok, %{data: <<>>} = result} when not result.closed ->
        timer_ref = Process.send_after(self(), {:waiter_timeout, from}, timeout)
        waiter = {from, offset, timer_ref}
        {:noreply, %{state | waiters: [waiter | state.waiters]}}

      result ->
        {:reply, result, state}
    end
  end

  @impl GenServer
  def handle_call(:close, _from, state) do
    result = state.storage.close(state.stream_id)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call(:get_metadata, _from, state) do
    result = state.storage.get_metadata(state.stream_id)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_info({:stream_append, _stream_id, _offset}, state) do
    for {from, offset, timer_ref} <- state.waiters do
      Process.cancel_timer(timer_ref)
      result = state.storage.read(state.stream_id, offset)
      GenServer.reply(from, result)
    end

    {:noreply, %{state | waiters: []}}
  end

  @impl GenServer
  def handle_info({:stream_closed, _stream_id}, state) do
    for {from, offset, timer_ref} <- state.waiters do
      Process.cancel_timer(timer_ref)
      result = state.storage.read(state.stream_id, offset)
      GenServer.reply(from, result)
    end

    {:noreply, %{state | waiters: []}}
  end

  @impl GenServer
  def handle_info({:waiter_timeout, from}, state) do
    {waiter, remaining} = Enum.split_with(state.waiters, fn {f, _, _} -> f == from end)

    case waiter do
      [{^from, offset, _}] ->
        result = state.storage.read(state.stream_id, offset)
        GenServer.reply(from, result)

      _ ->
        :ok
    end

    {:noreply, %{state | waiters: remaining}}
  end

  @impl GenServer
  def handle_info(:ttl_expired, state) do
    state.storage.delete(state.stream_id)
    {:stop, :normal, state}
  end

  defp via_tuple(stream_id) do
    {:via, Registry, {DurableStreams.Registry, stream_id}}
  end
end
