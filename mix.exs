defmodule DurableStreams.MixProject do
  use Mix.Project

  @version "0.3.0"
  @source_url "https://github.com/errantsky/streamkeeper"

  def project do
    [
      app: :streamkeeper,
      version: @version,
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # Hex.pm
      description:
        "Streamkeeper - an Elixir/OTP implementation of the Durable Streams protocol for append-only, URL-addressable byte logs with long-polling and SSE support",
      package: package(),

      # Docs
      name: "Streamkeeper",
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
      # Required dependencies (included in published package)
      {:plug, "~> 1.15"},
      {:phoenix_pubsub, "~> 2.1"},

      # Optional HTTP server adapters - users choose one
      {:plug_cowboy, "~> 2.7", optional: true},
      {:bandit, "~> 1.6", optional: true},

      # Optional Phoenix LiveView integration
      {:phoenix_live_view, "~> 1.0", optional: true},

      # Development and test dependencies (not included in published package)
      {:jason, "~> 1.4", only: [:dev, :test]},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.13", only: [:dev, :test], runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test], runtime: false},

      # Benchmarking
      {:benchee, "~> 1.3", only: :dev},
      {:benchee_html, "~> 1.0", only: :dev},
      {:benchee_markdown, "~> 0.3", only: :dev}
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
      extras: ["README.md", "CHANGELOG.md", "LICENSE"],
      source_ref: "v#{@version}",
      groups_for_modules: [
        "Core API": [
          DurableStreams,
          DurableStreams.StreamManager
        ],
        "HTTP Integration": [
          DurableStreams.Protocol.Plug
        ],
        "Phoenix LiveView": [
          DurableStreams.LiveView
        ],
        "Data Types": [
          DurableStreams.Stream,
          DurableStreams.Offset
        ],
        Storage: [
          DurableStreams.Storage.Behaviour,
          DurableStreams.Storage.ETS
        ],
        Internals: [
          DurableStreams.Server.StreamServer,
          DurableStreams.Protocol.Handlers.Create,
          DurableStreams.Protocol.Handlers.Append,
          DurableStreams.Protocol.Handlers.Read,
          DurableStreams.Protocol.Handlers.Delete,
          DurableStreams.Protocol.Handlers.Head,
          DurableStreams.Protocol.Handlers.SSE
        ]
      ],
      nest_modules_by_prefix: [
        DurableStreams.Protocol,
        DurableStreams.Storage,
        DurableStreams.Server
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
