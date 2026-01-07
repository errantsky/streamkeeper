# DurableStreams Benchmarks

Performance benchmarks for DurableStreams using [Benchee](https://github.com/bencheeorg/benchee).

## Quick Start

```bash
# Run all benchmarks
mix bench

# Run specific benchmark
mix bench offset
mix bench storage
mix bench concurrent
mix bench retention
```

## Available Benchmarks

### `offset` - Offset Operations

Measures offset generation and parsing performance. These operations happen on every append.

- `Offset.generate/0` - Generate new monotonic offset
- `Offset.to_integer/1` - Parse offset to integer for ETS key
- `Offset.start/0` - Get sentinel start offset

### `storage` - ETS Storage Operations

Measures core storage performance with varying stream sizes.

- **Append**: Write operations with different payload sizes
- **Read**: Read from start and mid-stream (tests seeking performance)
- **Metadata**: Stream metadata and offset lookups

### `concurrent` - Concurrent Access

Tests ETS performance under concurrent load with varying parallelism levels (1, 2, 4, 8 processes).

- Random stream access patterns
- Mixed read/write workloads
- Contention on single stream

This validates the effectiveness of `write_concurrency: true` on ETS tables.

### `retention` - Retention Operations

Measures retention policy performance:

- Finding offsets after N messages/bytes
- Getting first message timestamp
- Listing streams with retention policies
- Compaction performance

## Output

Results are saved in multiple formats:

```
bench/results/
├── html/           # Interactive HTML reports with charts
│   ├── offset.html
│   ├── storage.html
│   ├── concurrent.html
│   └── retention.html
└── markdown/       # Markdown summaries
    ├── offset.md
    ├── storage.md
    ├── concurrent.md
    └── retention.md
```

Open the HTML files in a browser for interactive charts and detailed statistics.

## Configuration

Edit `bench/config.exs` to adjust:

- Default benchmark duration and warmup
- Output formatters
- Payload sizes

## Interpreting Results

Key metrics:

| Metric | Description |
|--------|-------------|
| **IPS** | Iterations per second (higher is better) |
| **Average** | Mean execution time (lower is better) |
| **Deviation** | Standard deviation (lower means more consistent) |
| **Median** | 50th percentile execution time |
| **99th %** | Tail latency (important for real-world performance) |
| **Memory** | Memory allocated per operation |
| **Reductions** | BEAM scheduler work units |

## Adding New Benchmarks

1. Create a new file in `bench/benchmarks/`
2. Use the shared config:
   ```elixir
   Code.require_file("../config.exs", __DIR__)
   alias Bench.Config
   ```
3. Add to `@benchmarks` map in `lib/mix/tasks/bench.ex`

## Tips

- Run benchmarks on a quiet system for consistent results
- Use `parallel: N` to test concurrent performance
- Memory measurements are per-iteration allocations
- Warmup phase ensures JIT is warmed up (OTP 24+)
