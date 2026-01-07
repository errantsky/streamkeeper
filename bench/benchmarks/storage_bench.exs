# Storage Operations Benchmark
#
# Measures ETS storage performance for core operations:
# - append: Writing data to a stream
# - read: Reading data from a stream
# - get_metadata: Fetching stream metadata
#
# Run with: mix run bench/benchmarks/storage_bench.exs

Application.ensure_all_started(:streamkeeper)
Code.require_file("../config.exs", __DIR__)

alias DurableStreams.Storage.ETS
alias DurableStreams.Offset
alias Bench.Config

IO.puts("\n=== Storage Operations Benchmark ===\n")

# Setup: Create test streams with varying amounts of data
stream_empty = Config.setup_streams("storage-empty", 10) |> hd()
stream_small = Config.setup_streams("storage-small", 10) |> hd()
stream_medium = Config.setup_streams("storage-medium", 10) |> hd()
stream_large = Config.setup_streams("storage-large", 10) |> hd()

# Populate streams with different amounts of data
small_payload = Config.payload(100)
medium_payload = Config.payload(1000)

IO.puts("Populating test streams...")

# Small: 100 messages
for _ <- 1..100, do: ETS.append(stream_small, small_payload)

# Medium: 1000 messages
for _ <- 1..1000, do: ETS.append(stream_medium, small_payload)

# Large: 10000 messages
for _ <- 1..10_000, do: ETS.append(stream_large, small_payload)

IO.puts("Running benchmarks...\n")

# Get starting offsets for read tests
{:ok, %{offset: small_mid}} = ETS.read(stream_small, "-1")
{:ok, %{offset: medium_mid}} = ETS.read(stream_medium, "-1")
{:ok, %{offset: large_mid}} = ETS.read(stream_large, "-1")

Benchee.run(
  %{
    # Append operations
    "append (100B payload)" => fn ->
      ETS.append(stream_empty, small_payload)
    end,
    "append (1KB payload)" => fn ->
      ETS.append(stream_empty, medium_payload)
    end,

    # Read operations - from start
    "read from start (100 msgs)" => fn ->
      ETS.read(stream_small, "-1")
    end,
    "read from start (1K msgs)" => fn ->
      ETS.read(stream_medium, "-1")
    end,
    "read from start (10K msgs)" => fn ->
      ETS.read(stream_large, "-1")
    end,

    # Read operations - from middle (tests seeking)
    "read from mid (1K msgs)" => fn ->
      ETS.read(stream_medium, medium_mid)
    end,
    "read from mid (10K msgs)" => fn ->
      ETS.read(stream_large, large_mid)
    end,

    # Metadata operations
    "get_metadata" => fn ->
      ETS.get_metadata(stream_small)
    end,
    "current_offset" => fn ->
      ETS.current_offset(stream_small)
    end
  },
  Config.default_opts("storage")
)

# Cleanup
IO.puts("\nCleaning up...")
Config.cleanup_streams([stream_empty, stream_small, stream_medium, stream_large])

IO.puts("Results saved to bench/results/")
