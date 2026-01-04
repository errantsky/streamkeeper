defmodule Mix.Tasks.DurableStreams.Conformance do
  @moduledoc """
  Runs the Durable Streams server conformance tests.

  This task:
  1. Starts the application
  2. Starts a Cowboy HTTP server on port 4437
  3. Runs the official conformance test suite
  4. Reports results

  ## Usage

      mix durable_streams.conformance

  ## Requirements

  - Node.js and npm must be installed
  - Run `npm install` in the conformance directory first
  """

  use Mix.Task

  @shortdoc "Run Durable Streams server conformance tests"

  @impl Mix.Task
  def run(_args) do
    # Ensure dependencies are compiled
    Mix.Task.run("compile")

    # Start the application
    {:ok, _} = Application.ensure_all_started(:durable_streams)

    # Check if npm dependencies are installed
    conformance_dir = Path.join(File.cwd!(), "conformance")

    unless File.exists?(Path.join(conformance_dir, "node_modules")) do
      Mix.shell().info("Installing npm dependencies...")

      case System.cmd("npm", ["install"], cd: conformance_dir, into: IO.stream(:stdio, :line)) do
        {_, 0} -> :ok
        {_, code} -> Mix.raise("npm install failed with exit code #{code}")
      end
    end

    # Start Cowboy HTTP server
    port = 4437

    case Plug.Cowboy.http(DurableStreams.Protocol.Plug, [], port: port) do
      {:ok, _pid} ->
        Mix.shell().info("Server started on http://localhost:#{port}")

      {:error, {:already_started, _pid}} ->
        Mix.shell().info("Server already running on port #{port}")

      {:error, reason} ->
        Mix.raise("Failed to start server: #{inspect(reason)}")
    end

    # Give server time to start
    Process.sleep(500)

    # Run conformance tests
    Mix.shell().info("\nRunning conformance tests...\n")

    {_, exit_code} =
      System.cmd(
        "npx",
        ["@durable-streams/server-conformance-tests", "--run", "http://localhost:#{port}"],
        cd: conformance_dir,
        into: IO.stream(:stdio, :line),
        stderr_to_stdout: true
      )

    # Stop the server
    Plug.Cowboy.shutdown(DurableStreams.Protocol.Plug.HTTP)

    if exit_code != 0 do
      Mix.raise("Conformance tests failed with exit code #{exit_code}")
    else
      Mix.shell().info("\nAll conformance tests passed!")
    end
  end
end
