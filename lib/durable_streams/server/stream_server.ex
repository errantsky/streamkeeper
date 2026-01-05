defmodule DurableStreams.Server.StreamServer do
  @moduledoc """
  Internal GenServer managing a single stream's lifecycle.

  Each stream is managed by its own GenServer process, providing:
  - Process isolation for fault tolerance
  - Stateful management of waiters for long-polling
  - Automatic TTL expiration handling

  ## OTP Concepts

  - Uses `GenServer` for stateful stream management
  - `handle_call` for synchronous operations (append, read, close)
  - `handle_info` for PubSub messages and timeout handling
  - Process registration via `Registry` for stream lookup

  This is an internal module. Use `DurableStreams.StreamManager` for the public API.
  """

  use GenServer, restart: :transient

  alias DurableStreams.Stream

  defstruct [:stream_id, :storage, :waiters]

  # Client API

  def start_link({stream_id, opts}) do
    GenServer.start_link(__MODULE__, {stream_id, opts}, name: via_tuple(stream_id))
  end

  def append(stream_id, data, opts \\ []) do
    GenServer.call(via_tuple(stream_id), {:append, data, opts})
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

  def read_messages(stream_id, offset, opts \\ []) do
    live = Keyword.get(opts, :live, false)
    timeout = Keyword.get(opts, :timeout, 30_000)

    GenServer.call(via_tuple(stream_id), {:read_messages, offset, live, timeout}, timeout + 5_000)
  end

  # Server callbacks

  @impl GenServer
  def init({stream_id, opts}) do
    storage = Keyword.get(opts, :storage, DurableStreams.Storage.ETS)
    content_type = Keyword.get(opts, :content_type, "application/octet-stream")
    ttl = Keyword.get(opts, :ttl)
    expires_at = Keyword.get(opts, :expires_at)

    stream = Stream.new(stream_id, content_type: content_type, ttl: ttl, expires_at: expires_at)

    case storage.create(stream_id, stream) do
      :ok ->
        storage.subscribe(stream_id)
        schedule_expiration(ttl, expires_at)
        {:ok, %__MODULE__{stream_id: stream_id, storage: storage, waiters: []}}

      {:error, :already_exists} ->
        {:stop, :already_exists}
    end
  end

  defp schedule_expiration(ttl, _expires_at) when is_integer(ttl) and ttl > 0 do
    Process.send_after(self(), :ttl_expired, ttl * 1000)
  end

  defp schedule_expiration(_ttl, %DateTime{} = expires_at) do
    now = DateTime.utc_now()
    diff_ms = DateTime.diff(expires_at, now, :millisecond)
    if diff_ms > 0 do
      Process.send_after(self(), :ttl_expired, diff_ms)
    else
      # Already expired
      Process.send_after(self(), :ttl_expired, 0)
    end
  end

  defp schedule_expiration(_ttl, _expires_at), do: :ok

  @impl GenServer
  def handle_call({:append, data, opts}, _from, state) do
    seq = Keyword.get(opts, :seq)
    result = state.storage.append(state.stream_id, data, seq)
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
  def handle_call({:read_messages, offset, false, _timeout}, _from, state) do
    result = state.storage.read_messages(state.stream_id, offset)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call({:read_messages, offset, true, timeout}, from, state) do
    case state.storage.read_messages(state.stream_id, offset) do
      {:ok, %{messages: []} = result} when not result.closed ->
        timer_ref = Process.send_after(self(), {:waiter_timeout_messages, from, offset}, timeout)
        waiter = {from, offset, timer_ref, :messages}
        {:noreply, %{state | waiters: [waiter | state.waiters]}}

      result ->
        {:reply, result, state}
    end
  end

  @impl GenServer
  def handle_info({:stream_append, _stream_id, _offset}, state) do
    for waiter <- state.waiters do
      reply_to_waiter(waiter, state)
    end

    {:noreply, %{state | waiters: []}}
  end

  @impl GenServer
  def handle_info({:stream_closed, _stream_id}, state) do
    for waiter <- state.waiters do
      reply_to_waiter(waiter, state)
    end

    {:noreply, %{state | waiters: []}}
  end

  @impl GenServer
  def handle_info({:waiter_timeout, from}, state) do
    {waiter, remaining} = Enum.split_with(state.waiters, fn
      {f, _, _} -> f == from
      {f, _, _, _} -> f == from
    end)

    case waiter do
      [w] -> reply_to_waiter(w, state)
      _ -> :ok
    end

    {:noreply, %{state | waiters: remaining}}
  end

  @impl GenServer
  def handle_info({:waiter_timeout_messages, from, _offset}, state) do
    {waiter, remaining} = Enum.split_with(state.waiters, fn
      {f, _, _} -> f == from
      {f, _, _, _} -> f == from
    end)

    case waiter do
      [w] -> reply_to_waiter(w, state)
      _ -> :ok
    end

    {:noreply, %{state | waiters: remaining}}
  end

  @impl GenServer
  def handle_info(:ttl_expired, state) do
    state.storage.delete(state.stream_id)
    {:stop, :normal, state}
  end

  defp reply_to_waiter({from, offset, timer_ref}, state) do
    Process.cancel_timer(timer_ref)
    result = state.storage.read(state.stream_id, offset)
    GenServer.reply(from, result)
  end

  defp reply_to_waiter({from, offset, timer_ref, :messages}, state) do
    Process.cancel_timer(timer_ref)
    result = state.storage.read_messages(state.stream_id, offset)
    GenServer.reply(from, result)
  end

  defp via_tuple(stream_id) do
    {:via, Registry, {DurableStreams.Registry, stream_id}}
  end
end
