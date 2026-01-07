# Retention Operations Benchmark
#
# Measures performance of retention-related operations:
# - Finding offsets for compaction
# - Deleting messages before offset
# - Updating metadata after compaction
#
# Run with: mix run bench/benchmarks/retention_bench.exs

Application.ensure_all_started(:streamkeeper)
Code.require_file("../config.exs", __DIR__)

alias DurableStreams.Storage.ETS
alias DurableStreams.Stream
alias Bench.Config

IO.puts("\n=== Retention Operations Benchmark ===\n")

# Setup streams with retention policies
stream_ids = Config.stream_ids("retention", 10)
payload = Config.payload(100)

IO.puts("Setting up streams with retention policies...")

for id <- stream_ids do
  stream = Stream.new(id, retention: [max_messages: 10_000, max_bytes: 1_000_000])
  :ok = ETS.create(id, stream)
  # Populate with 5000 messages
  for _ <- 1..5000, do: ETS.append(id, payload)
end

# Get a reference stream for offset lookups
ref_stream = hd(stream_ids)
{:ok, meta} = ETS.get_metadata(ref_stream)

IO.puts("Running retention benchmarks...\n")

Benchee.run(
  %{
    "get_first_message_timestamp" => fn ->
      ETS.get_first_message_timestamp(ref_stream)
    end,
    "find_offset_after_n_messages (100)" => fn ->
      ETS.find_offset_after_n_messages(ref_stream, 100)
    end,
    "find_offset_after_n_messages (1000)" => fn ->
      ETS.find_offset_after_n_messages(ref_stream, 1000)
    end,
    "find_offset_after_n_bytes (10KB)" => fn ->
      ETS.find_offset_after_n_bytes(ref_stream, 10_000)
    end,
    "find_offset_after_n_bytes (100KB)" => fn ->
      ETS.find_offset_after_n_bytes(ref_stream, 100_000)
    end,
    "list_streams_with_retention" => fn ->
      ETS.list_streams_with_retention()
    end
  },
  Config.default_opts("retention")
)

# Benchmark compaction (destructive, so use fresh streams each iteration)
IO.puts("\n--- Compaction Benchmark (single run) ---\n")

# Create a stream specifically for compaction test
compact_id = "compact-bench-#{System.system_time(:millisecond)}"
stream = Stream.new(compact_id, retention: [max_messages: 100])
:ok = ETS.create(compact_id, stream)
for _ <- 1..1000, do: ETS.append(compact_id, payload)

# Find the offset to compact to
target_offset = ETS.find_offset_after_n_messages(compact_id, 900)

# Time the compaction
{time_us, {:ok, deleted, bytes}} =
  :timer.tc(fn ->
    ETS.delete_messages_before(compact_id, target_offset)
  end)

IO.puts("Compaction results:")
IO.puts("  Deleted: #{deleted} messages, #{bytes} bytes")
IO.puts("  Time: #{time_us / 1000}ms")

# Cleanup
IO.puts("\nCleaning up...")
Config.cleanup_streams(stream_ids ++ [compact_id])

IO.puts("Results saved to bench/results/")
