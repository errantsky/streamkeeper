defmodule DurableStreams.Retention.Scheduler do
  @moduledoc """
  Periodic scheduler for stream retention/compaction.

  This GenServer runs at regular intervals and:
  1. Queries all streams with retention policies
  2. Identifies streams that need compaction
  3. Spawns compaction tasks (with concurrency limits)
  4. Tracks in-progress compactions to avoid duplicates

  ## Configuration

  - `:interval` - How often to check for compaction needs (default: 30 seconds)
  - `:max_concurrent` - Maximum concurrent compaction tasks (default: 5)
  """

  use GenServer

  require Logger

  alias DurableStreams.Retention.Worker
  alias DurableStreams.Storage.ETS, as: Storage

  @default_interval :timer.seconds(30)
  @default_max_concurrent 5

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Triggers an immediate compaction check.
  Useful for testing or manual intervention.
  """
  @spec check_now() :: :ok
  def check_now do
    GenServer.cast(__MODULE__, :check_now)
  end

  @doc """
  Returns the current scheduler state for debugging.
  """
  @spec status() :: map()
  def status do
    GenServer.call(__MODULE__, :status)
  end

  # GenServer callbacks

  @impl GenServer
  def init(opts) do
    interval = Keyword.get(opts, :interval, @default_interval)
    max_concurrent = Keyword.get(opts, :max_concurrent, @default_max_concurrent)

    state = %{
      interval: interval,
      max_concurrent: max_concurrent,
      in_progress: MapSet.new(),
      timer_ref: nil
    }

    # Schedule first check after a short delay to let system stabilize
    {:ok, schedule_check(state, :timer.seconds(5))}
  end

  @impl GenServer
  def handle_cast(:check_now, state) do
    {:noreply, run_compaction_check(state)}
  end

  @impl GenServer
  def handle_call(:status, _from, state) do
    status = %{
      interval: state.interval,
      max_concurrent: state.max_concurrent,
      in_progress_count: MapSet.size(state.in_progress),
      in_progress_streams: MapSet.to_list(state.in_progress)
    }

    {:reply, status, state}
  end

  @impl GenServer
  def handle_info(:check_compaction, state) do
    state = run_compaction_check(state)
    {:noreply, schedule_check(state)}
  end

  @impl GenServer
  def handle_info({:compaction_complete, stream_id, result}, state) do
    case result do
      :ok ->
        Logger.debug("[Retention.Scheduler] Compaction completed for #{stream_id}")

      {:error, reason} ->
        Logger.warning(
          "[Retention.Scheduler] Compaction failed for #{stream_id}: #{inspect(reason)}"
        )
    end

    state = %{state | in_progress: MapSet.delete(state.in_progress, stream_id)}
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
    # Task crashed - logged by Task.Supervisor
    if reason != :normal do
      Logger.warning("[Retention.Scheduler] Compaction task crashed: #{inspect(reason)}")
    end

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(msg, state) do
    Logger.debug("[Retention.Scheduler] Unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  # Private functions

  defp schedule_check(state, interval \\ nil) do
    interval = interval || state.interval

    if state.timer_ref do
      Process.cancel_timer(state.timer_ref)
    end

    timer_ref = Process.send_after(self(), :check_compaction, interval)
    %{state | timer_ref: timer_ref}
  end

  defp run_compaction_check(state) do
    available_slots = state.max_concurrent - MapSet.size(state.in_progress)

    if available_slots > 0 do
      streams_needing_compaction =
        list_streams_with_retention()
        |> Enum.filter(fn stream ->
          not MapSet.member?(state.in_progress, stream.id) and
            Worker.needs_compaction?(stream)
        end)
        |> Enum.take(available_slots)

      Enum.reduce(streams_needing_compaction, state, fn stream, acc ->
        spawn_compaction_task(acc, stream.id)
      end)
    else
      Logger.debug("[Retention.Scheduler] All compaction slots in use, skipping check")
      state
    end
  end

  defp spawn_compaction_task(state, stream_id) do
    scheduler_pid = self()

    Task.Supervisor.start_child(
      DurableStreams.Retention.TaskSupervisor,
      fn ->
        result = Worker.compact(stream_id)
        send(scheduler_pid, {:compaction_complete, stream_id, result})
      end
    )

    %{state | in_progress: MapSet.put(state.in_progress, stream_id)}
  end

  defp list_streams_with_retention do
    Storage.list_streams_with_retention()
  end
end
