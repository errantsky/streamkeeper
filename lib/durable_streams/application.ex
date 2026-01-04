defmodule DurableStreams.Application do
  @moduledoc """
  OTP Application for DurableStreams.

  Starts the supervision tree with:
  - Phoenix.PubSub for live update notifications
  - Registry for stream process lookup
  - ETS storage backend
  - DynamicSupervisor for stream processes
  """

  use Application

  @impl Application
  def start(_type, _args) do
    children = [
      # PubSub for broadcasting stream updates to subscribers
      {Phoenix.PubSub, name: DurableStreams.PubSub},

      # Registry for looking up stream processes by ID
      {Registry, keys: :unique, name: DurableStreams.Registry},

      # ETS storage backend (manages the ETS tables)
      DurableStreams.Storage.ETS,

      # DynamicSupervisor for spawning stream GenServers
      {DynamicSupervisor, name: DurableStreams.StreamSupervisor, strategy: :one_for_one}
    ]

    opts = [strategy: :one_for_one, name: DurableStreams.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
