# Shared benchmark configuration
# Usage: Code.require_file("config.exs", __DIR__)

defmodule Bench.Config do
  @moduledoc """
  Shared configuration and helpers for benchmarks.
  """

  @results_dir Path.expand("results", __DIR__)

  def results_dir, do: @results_dir

  def html_file(name), do: Path.join([@results_dir, "html", "#{name}.html"])
  def markdown_file(name), do: Path.join([@results_dir, "markdown", "#{name}.md"])

  @doc """
  Standard formatters for all benchmarks.
  """
  def formatters(name) do
    [
      Benchee.Formatters.Console,
      {Benchee.Formatters.HTML, file: html_file(name)},
      {Benchee.Formatters.Markdown, file: markdown_file(name)}
    ]
  end

  @doc """
  Default benchmark options.
  """
  def default_opts(name, overrides \\ []) do
    Keyword.merge(
      [
        time: 5,
        warmup: 2,
        memory_time: 2,
        reduction_time: 2,
        formatters: formatters(name),
        print: [configuration: false, fast_warning: false]
      ],
      overrides
    )
  end

  @doc """
  Generate unique stream IDs for benchmarks.
  """
  def stream_ids(prefix, count) do
    timestamp = System.system_time(:millisecond)
    for i <- 1..count, do: "#{prefix}-#{timestamp}-#{i}"
  end

  @doc """
  Setup streams for benchmarking, returns list of stream IDs.
  """
  def setup_streams(prefix, count, opts \\ []) do
    alias DurableStreams.Storage.ETS
    alias DurableStreams.Stream

    ids = stream_ids(prefix, count)

    for id <- ids do
      stream = Stream.new(id, opts)
      :ok = ETS.create(id, stream)
    end

    ids
  end

  @doc """
  Cleanup streams after benchmarking.
  """
  def cleanup_streams(ids) do
    alias DurableStreams.Storage.ETS

    for id <- ids do
      ETS.delete(id)
    end

    :ok
  end

  @doc """
  Generate random payload of given size.
  """
  def payload(size \\ 100) do
    :crypto.strong_rand_bytes(size)
  end
end
