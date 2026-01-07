# Offset Generation Benchmark
#
# Measures the performance of offset generation and parsing operations.
# These are called on every append, so they need to be fast.
#
# Run with: mix run bench/benchmarks/offset_bench.exs

Application.ensure_all_started(:streamkeeper)
Code.require_file("../config.exs", __DIR__)

alias DurableStreams.Offset
alias Bench.Config

IO.puts("\n=== Offset Generation Benchmark ===\n")

# Pre-generate some offsets for parsing tests
sample_offsets = for _ <- 1..1000, do: Offset.generate()
sample_offset = hd(sample_offsets)

Benchee.run(
  %{
    "Offset.generate/0" => fn ->
      Offset.generate()
    end,
    "Offset.to_integer/1" => fn ->
      Offset.to_integer(sample_offset)
    end,
    "Offset.start/0" => fn ->
      Offset.start()
    end,
    "generate + to_integer" => fn ->
      offset = Offset.generate()
      Offset.to_integer(offset)
    end
  },
  Config.default_opts("offset")
)

IO.puts("\nResults saved to bench/results/")
