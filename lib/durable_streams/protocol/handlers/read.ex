defmodule DurableStreams.Protocol.Handlers.Read do
  @moduledoc """
  Handler for GET requests to read from a stream.

  Supports:
  - Regular reads with offset parameter
  - Long-polling with live=true parameter
  """

  import Plug.Conn
  alias DurableStreams.{StreamManager, Offset}

  def call(conn) do
    stream_id = conn.path_params["stream_id"]
    offset = conn.params["offset"] || Offset.start()
    live = conn.params["live"] in ["true", "long-poll", "1"]
    timeout = parse_timeout(conn.params["timeout"])

    case StreamManager.read(stream_id, offset, live: live, timeout: timeout) do
      {:ok, %{data: <<>>} = result} ->
        conn
        |> put_resp_header("stream-next-offset", result.offset)
        |> maybe_put_closed_header(result.closed)
        |> send_resp(204, "")

      {:ok, result} ->
        {:ok, meta} = StreamManager.get_metadata(stream_id)

        conn
        |> put_resp_header("stream-next-offset", result.offset)
        |> maybe_put_closed_header(result.closed)
        |> put_cache_headers(live)
        |> put_resp_content_type(meta.content_type)
        |> send_resp(200, result.data)

      {:error, :not_found} ->
        send_resp(conn, 404, "Stream not found")
    end
  end

  defp parse_timeout(nil), do: 30_000
  defp parse_timeout(s), do: String.to_integer(s) * 1000

  defp maybe_put_closed_header(conn, true), do: put_resp_header(conn, "x-stream-closed", "true")
  defp maybe_put_closed_header(conn, _), do: conn

  defp put_cache_headers(conn, true), do: put_resp_header(conn, "cache-control", "no-cache")
  defp put_cache_headers(conn, false), do: put_resp_header(conn, "cache-control", "public, max-age=60")
end
