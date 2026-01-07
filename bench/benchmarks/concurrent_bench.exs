# Concurrent Access Benchmark
#
# Tests ETS performance under concurrent load.
# Uses Benchee's parallel option to simulate multiple processes
# accessing streams simultaneously.
#
# This tests the effectiveness of write_concurrency: true on ETS tables.
#
# Run with: mix run bench/benchmarks/concurrent_bench.exs

Application.ensure_all_started(:streamkeeper)
Code.require_file("../config.exs", __DIR__)

alias DurableStreams.Storage.ETS
alias Bench.Config

IO.puts("\n=== Concurrent Access Benchmark ===\n")

# Setup: Create a pool of streams
num_streams = 100
stream_ids = Config.setup_streams("concurrent", num_streams)

# Pre-populate with some data
payload = Config.payload(256)
IO.puts("Populating #{num_streams} streams with initial data...")

for id <- stream_ids do
  for _ <- 1..100, do: ETS.append(id, payload)
end

IO.puts("Running concurrent benchmarks...\n")

# Benchmark with different parallelism levels
for parallel <- [1, 2, 4, 8] do
  IO.puts("--- Parallelism: #{parallel} ---\n")

  Benchee.run(
    %{
      "append (random stream)" => fn ->
        id = Enum.random(stream_ids)
        ETS.append(id, payload)
      end,
      "read (random stream)" => fn ->
        id = Enum.random(stream_ids)
        ETS.read(id, "-1")
      end,
      "mixed read/write" => fn ->
        id = Enum.random(stream_ids)
        {:ok, offset} = ETS.append(id, payload)
        ETS.read(id, offset)
      end,
      "append (same stream)" => fn ->
        # Contention test - all processes hit same stream
        ETS.append(hd(stream_ids), payload)
      end
    },
    time: 3,
    warmup: 1,
    memory_time: 1,
    parallel: parallel,
    formatters: [Benchee.Formatters.Console],
    print: [configuration: false]
  )

  IO.puts("")
end

# Final comprehensive run with HTML output
IO.puts("--- Final comparison (parallel=4) ---\n")

Benchee.run(
  %{
    "append" => fn ->
      id = Enum.random(stream_ids)
      ETS.append(id, payload)
    end,
    "read" => fn ->
      id = Enum.random(stream_ids)
      ETS.read(id, "-1")
    end,
    "get_metadata" => fn ->
      id = Enum.random(stream_ids)
      ETS.get_metadata(id)
    end,
    "mixed workload" => fn ->
      id = Enum.random(stream_ids)

      case :rand.uniform(3) do
        1 -> ETS.append(id, payload)
        2 -> ETS.read(id, "-1")
        3 -> ETS.get_metadata(id)
      end
    end
  },
  Config.default_opts("concurrent", parallel: 4)
)

# Cleanup
IO.puts("\nCleaning up...")
Config.cleanup_streams(stream_ids)

IO.puts("Results saved to bench/results/")
