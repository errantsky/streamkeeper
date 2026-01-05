defmodule DurableStreams.Retention.Supervisor do
  @moduledoc """
  Supervisor for the retention/compaction subsystem.

  Supervises:
  - `DurableStreams.Retention.TaskSupervisor` - Task.Supervisor for compaction workers
  - `DurableStreams.Retention.Scheduler` - Periodic scheduler GenServer

  The TaskSupervisor is started first so it's available when the Scheduler spawns tasks.
  """

  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Supervisor
  def init(opts) do
    scheduler_opts = Keyword.get(opts, :scheduler, [])

    children = [
      {Task.Supervisor, name: DurableStreams.Retention.TaskSupervisor},
      {DurableStreams.Retention.Scheduler, scheduler_opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
