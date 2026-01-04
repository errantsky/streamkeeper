defmodule DurableStreams.Protocol.Handlers.SSE do
  @moduledoc """
  Handler for GET requests with live=sse for Server-Sent Events.

  Streams data to the client as SSE events, sending heartbeats
  when no new data is available.
  """

  import Plug.Conn
  alias DurableStreams.{StreamManager, Offset}

  def call(conn) do
    stream_id = conn.path_params["stream_id"]
    offset = conn.params["offset"] || Offset.start()

    conn
    |> put_resp_header("content-type", "text/event-stream")
    |> put_resp_header("cache-control", "no-cache")
    |> send_chunked(200)
    |> stream_loop(stream_id, offset)
  end

  defp stream_loop(conn, stream_id, offset) do
    case StreamManager.read(stream_id, offset, live: true, timeout: 30_000) do
      {:ok, %{data: <<>>, closed: true}} ->
        chunk(conn, "event: close\ndata: closed\n\n")
        conn

      {:ok, %{data: <<>>}} ->
        case chunk(conn, ": heartbeat\n\n") do
          {:ok, conn} -> stream_loop(conn, stream_id, offset)
          {:error, _} -> conn
        end

      {:ok, result} ->
        event = "data: #{Base.encode64(result.data)}\nid: #{result.offset}\n\n"

        case chunk(conn, event) do
          {:ok, conn} when result.closed -> conn
          {:ok, conn} -> stream_loop(conn, stream_id, result.offset)
          {:error, _} -> conn
        end

      {:error, _} ->
        conn
    end
  end
end
