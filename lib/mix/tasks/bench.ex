defmodule Mix.Tasks.Bench do
  @shortdoc "Run performance benchmarks"
  @moduledoc """
  Runs performance benchmarks for DurableStreams.

  ## Usage

      # Run all benchmarks
      mix bench

      # Run specific benchmark
      mix bench offset
      mix bench storage
      mix bench concurrent
      mix bench retention

      # Run multiple benchmarks
      mix bench offset storage

  ## Available Benchmarks

  - `offset` - Offset generation and parsing performance
  - `storage` - ETS storage operations (append, read, metadata)
  - `concurrent` - Concurrent access patterns with varying parallelism
  - `retention` - Retention policy operations (finding offsets, compaction)

  ## Output

  Results are saved to:
  - `bench/results/html/` - Interactive HTML reports
  - `bench/results/markdown/` - Markdown summaries
  """

  use Mix.Task

  @benchmarks %{
    "offset" => "bench/benchmarks/offset_bench.exs",
    "storage" => "bench/benchmarks/storage_bench.exs",
    "concurrent" => "bench/benchmarks/concurrent_bench.exs",
    "retention" => "bench/benchmarks/retention_bench.exs"
  }

  @impl Mix.Task
  def run(args) do
    benchmarks_to_run =
      case args do
        [] -> Map.values(@benchmarks)
        names -> resolve_benchmarks(names)
      end

    case benchmarks_to_run do
      {:error, unknown} ->
        Mix.shell().error("Unknown benchmark: #{unknown}")
        Mix.shell().info("\nAvailable benchmarks: #{Map.keys(@benchmarks) |> Enum.join(", ")}")

      files ->
        Mix.shell().info("Running #{length(files)} benchmark(s)...\n")

        for file <- files do
          Mix.shell().info("=" |> String.duplicate(60))
          Mix.shell().info("Running: #{Path.basename(file)}")
          Mix.shell().info("=" |> String.duplicate(60))

          # Use Mix.Task.run to execute the script
          Mix.Task.run("run", [file])
          # Reset the task so it can be run again
          Mix.Task.reenable("run")

          Mix.shell().info("")
        end

        Mix.shell().info("All benchmarks complete!")
        Mix.shell().info("Results saved to bench/results/")
    end
  end

  defp resolve_benchmarks(names) do
    Enum.reduce_while(names, [], fn name, acc ->
      case Map.get(@benchmarks, name) do
        nil -> {:halt, {:error, name}}
        file -> {:cont, [file | acc]}
      end
    end)
    |> case do
      {:error, _} = err -> err
      files -> Enum.reverse(files)
    end
  end
end
