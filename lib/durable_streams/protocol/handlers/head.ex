defmodule DurableStreams.Protocol.Handlers.Head do
  @moduledoc """
  Internal handler for HEAD requests to get stream metadata.

  This is an internal module used by `DurableStreams.Protocol.Plug`.
  """

  import Plug.Conn
  alias DurableStreams.{Storage, StreamManager}

  def call(conn) do
    stream_id = conn.path_params["stream_id"]

    case StreamManager.get_metadata(stream_id) do
      {:ok, meta} ->
        {:ok, current_offset} = Storage.ETS.current_offset(stream_id)

        conn
        |> put_resp_header("content-type", meta.content_type)
        |> put_resp_header("stream-id", meta.id)
        |> put_resp_header("stream-next-offset", current_offset)
        # Per protocol: HEAD responses should not be cached to avoid stale tail offsets
        |> put_resp_header("cache-control", "no-store")
        |> maybe_put_closed_header(meta.closed)
        |> maybe_put_ttl_header(meta.ttl)
        |> maybe_put_expires_at_header(meta.expires_at)
        |> send_resp(200, "")

      {:error, :not_found} ->
        send_resp(conn, 404, "")
    end
  end

  defp maybe_put_closed_header(conn, true), do: put_resp_header(conn, "stream-closed", "true")
  defp maybe_put_closed_header(conn, _), do: conn

  defp maybe_put_ttl_header(conn, nil), do: conn

  defp maybe_put_ttl_header(conn, ttl),
    do: put_resp_header(conn, "stream-ttl", Integer.to_string(ttl))

  defp maybe_put_expires_at_header(conn, nil), do: conn

  defp maybe_put_expires_at_header(conn, expires_at) do
    put_resp_header(conn, "stream-expires-at", DateTime.to_iso8601(expires_at))
  end
end
