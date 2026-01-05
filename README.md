# DurableStreams

An Elixir/OTP implementation of the [Durable Streams](https://github.com/durable-streams/durable-streams) protocol - a specification for append-only, URL-addressable byte logs.

## Features

- Full HTTP protocol compliance (PUT, POST, GET, DELETE, HEAD)
- JSON mode with array flattening
- Long-polling and Server-Sent Events (SSE) for live updates
- Stream TTL and expiration
- Sequence ordering enforcement
- ETag-based caching
- OTP supervision tree for fault tolerance

## Architecture

### Component Overview

```mermaid
graph TB
    subgraph "HTTP Layer"
        V1Plug[V1Plug Router]
        Plug[Protocol.Plug]
        Handlers[HTTP Handlers]
    end

    subgraph "Business Logic"
        SM[StreamManager]
        SS[StreamServer GenServer]
    end

    subgraph "Storage"
        ETS[ETS Storage]
        PubSub[Phoenix.PubSub]
    end

    subgraph "OTP Supervision"
        App[Application]
        Sup[StreamSupervisor]
        Reg[Registry]
    end

    V1Plug --> Plug
    Plug --> Handlers
    Handlers --> SM
    SM --> SS
    SS --> ETS
    SS --> PubSub
    App --> Sup
    App --> Reg
    App --> ETS
    Sup --> SS
```

### Request Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant P as Protocol.Plug
    participant H as Handler
    participant SM as StreamManager
    participant SS as StreamServer
    participant S as Storage.ETS

    C->>P: PUT /v1/stream/:id
    P->>H: Create Handler
    H->>SM: create(id, opts)
    SM->>SS: start_link
    SS->>S: create(id, stream)
    S-->>SS: :ok
    SS-->>SM: {:ok, pid}
    SM-->>H: {:ok, id}
    H-->>C: 201 Created

    C->>P: POST /v1/stream/:id
    P->>H: Append Handler
    H->>SM: append(id, data)
    SM->>SS: GenServer.call(:append)
    SS->>S: append(id, data)
    S-->>SS: {:ok, offset}
    SS-->>SM: {:ok, offset}
    SM-->>H: {:ok, offset}
    H-->>C: 200 OK + offset
```

### Long-Polling Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant H as Read Handler
    participant SS as StreamServer
    participant S as Storage
    participant PS as PubSub

    C->>H: GET ?offset=X&live=true
    H->>SS: read(offset, live: true)
    SS->>S: read(offset)
    S-->>SS: {:ok, %{data: <<>>}}
    Note over SS: No data, register waiter
    SS->>SS: Add to waiters list

    Note over C,PS: ... time passes ...

    C->>SS: Another client appends
    SS->>S: append(data)
    S-->>SS: {:ok, new_offset}
    SS->>PS: broadcast(:stream_append)
    PS-->>SS: Notify waiters
    SS->>S: read(offset)
    S-->>SS: {:ok, %{data: ...}}
    SS-->>H: {:ok, result}
    H-->>C: 200 OK + data
```

## Installation

Add `durable_streams` to your dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:durable_streams, "~> 0.1.0"}
  ]
end
```

## Usage

### Standalone Server

Start the HTTP server on a specific port:

```elixir
# In your application.ex or IEx
{:ok, _} = Plug.Cowboy.http(DurableStreams.Protocol.V1Plug, [], port: 4437)
```

### Phoenix Integration

Forward requests from your Phoenix router:

```elixir
# In your router.ex
forward "/v1/stream", DurableStreams.Protocol.Plug
```

### Programmatic API

Use the `DurableStreams.StreamManager` module directly:

```elixir
# Create a stream
{:ok, "my-stream"} = DurableStreams.StreamManager.create("my-stream",
  content_type: "text/plain",
  ttl: 3600  # expires in 1 hour
)

# Append data
{:ok, offset} = DurableStreams.StreamManager.append("my-stream", "Hello, World!")

# Read data
{:ok, result} = DurableStreams.StreamManager.read("my-stream", "-1")
# result.data => "Hello, World!"
# result.offset => "0006478b4bce37b5-0001-98ee"

# Long-poll for new data
{:ok, result} = DurableStreams.StreamManager.read("my-stream", offset,
  live: true,
  timeout: 30_000
)

# Delete stream
:ok = DurableStreams.StreamManager.delete("my-stream")
```

### JSON Mode

When a stream is created with `content-type: application/json`, it operates in JSON mode:

```elixir
# Create JSON stream
{:ok, _} = DurableStreams.StreamManager.create("json-stream",
  content_type: "application/json"
)

# Arrays are flattened one level
# POST [{"a": 1}, {"b": 2}] stores two messages
{:ok, _} = DurableStreams.StreamManager.append("json-stream",
  Jason.encode!([%{a: 1}, %{b: 2}])
)

# Read returns array of messages
{:ok, result} = DurableStreams.StreamManager.read_messages("json-stream", "-1")
# result.messages => [%{data: "{\"a\":1}", offset: "..."}, ...]
```

## HTTP API

| Method | Path | Description |
|--------|------|-------------|
| `PUT` | `/:stream_id` | Create a stream |
| `POST` | `/:stream_id` | Append data |
| `GET` | `/:stream_id` | Read from offset |
| `DELETE` | `/:stream_id` | Delete stream |
| `HEAD` | `/:stream_id` | Get metadata |

### Headers

#### Request Headers

| Header | Description |
|--------|-------------|
| `Content-Type` | Stream content type (required for POST, optional for PUT) |
| `Stream-TTL` | Time-to-live in seconds |
| `Stream-Expires-At` | ISO 8601 expiration timestamp |
| `Stream-Seq` | Sequence value for ordering |
| `If-None-Match` | ETag for conditional GET |

#### Response Headers

| Header | Description |
|--------|-------------|
| `Stream-Next-Offset` | Offset for resuming reads |
| `Stream-Up-To-Date` | True when no more data available |
| `Stream-Cursor` | Cursor for jitter handling |
| `ETag` | Entity tag for caching |
| `Location` | Stream URL (on 201) |

### Query Parameters

| Parameter | Description |
|-----------|-------------|
| `offset` | Start reading after this offset (-1 for beginning) |
| `live` | Enable long-polling (`true`) or SSE (`sse`) |
| `timeout` | Long-poll timeout in seconds |

## Configuration

The library uses sensible defaults but can be configured:

```elixir
# config/config.exs
config :durable_streams,
  storage: DurableStreams.Storage.ETS,
  default_timeout: 30_000
```

## OTP Supervision Tree

```mermaid
graph TB
    App[DurableStreams.Application]
    Sup[DurableStreams.StreamSupervisor<br/>DynamicSupervisor]
    Reg[DurableStreams.Registry<br/>Registry]
    ETS[DurableStreams.Storage.ETS<br/>GenServer]
    PS[Phoenix.PubSub]
    SS1[StreamServer 1]
    SS2[StreamServer 2]
    SS3[StreamServer N]

    App --> Sup
    App --> Reg
    App --> ETS
    App --> PS
    Sup --> SS1
    Sup --> SS2
    Sup --> SS3
```

Each stream is managed by its own GenServer process, providing:
- Process isolation
- Independent failure handling
- Concurrent access
- Automatic cleanup on TTL expiration

## Conformance Testing

Run the official [Durable Streams conformance tests](https://github.com/durable-streams/durable-streams) to verify protocol compliance.

### Prerequisites

- Node.js 18+ (for running the conformance test suite)

### Running Tests

```bash
# Install conformance test dependencies (first time only)
cd conformance
npm install
cd ..

# Start the server in one terminal
mix run -e 'Plug.Cowboy.http(DurableStreams.Protocol.V1Plug, [], port: 4437); Process.sleep(:infinity)'

# In another terminal, run conformance tests
cd conformance
npx @durable-streams/server-conformance-tests --run http://localhost:4437
```

Alternatively, use the mix task (starts server automatically):

```bash
mix durable_streams.conformance
```

Current conformance: **131/131 tests passing (100%)**

## License

MIT License
