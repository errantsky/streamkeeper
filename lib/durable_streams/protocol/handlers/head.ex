defmodule DurableStreams.Protocol.Handlers.Head do
  @moduledoc false

  import Plug.Conn
  alias DurableStreams.{StreamManager, Storage}

  def call(conn) do
    stream_id = conn.path_params["stream_id"]

    case StreamManager.get_metadata(stream_id) do
      {:ok, meta} ->
        {:ok, current_offset} = Storage.ETS.current_offset(stream_id)

        conn
        |> put_resp_header("content-type", meta.content_type)
        |> put_resp_header("stream-id", meta.id)
        |> put_resp_header("stream-next-offset", current_offset)
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
  defp maybe_put_ttl_header(conn, ttl), do: put_resp_header(conn, "stream-ttl", Integer.to_string(ttl))

  defp maybe_put_expires_at_header(conn, nil), do: conn
  defp maybe_put_expires_at_header(conn, expires_at) do
    put_resp_header(conn, "stream-expires-at", DateTime.to_iso8601(expires_at))
  end
end
