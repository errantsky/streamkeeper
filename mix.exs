defmodule DurableStreams.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/errantsky/durable_streams"

  def project do
    [
      app: :durable_streams,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # Hex.pm
      description: "Elixir implementation of the Durable Streams protocol",
      package: package(),

      # Docs
      name: "DurableStreams",
      source_url: @source_url,
      docs: docs(),

      # Testing
      elixirc_paths: elixirc_paths(Mix.env())
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {DurableStreams.Application, []}
    ]
  end

  defp deps do
    [
      # HTTP - Plug for framework-agnostic routing
      {:plug, "~> 1.15"},
      {:plug_cowboy, "~> 2.7", optional: true},
      {:bandit, "~> 1.0", optional: true},

      # PubSub for live updates
      {:phoenix_pubsub, "~> 2.1"},

      # JSON
      {:jason, "~> 1.4"},

      # Telemetry
      {:telemetry, "~> 1.2"},

      # Dev/Test
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      maintainers: ["errantsky"],
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"],
      source_ref: "v#{@version}"
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
