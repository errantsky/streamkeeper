defmodule DurableStreams.Protocol.Handlers.Head do
  @moduledoc """
  Handler for HEAD requests to get stream metadata.
  """

  import Plug.Conn
  alias DurableStreams.StreamManager

  def call(conn) do
    stream_id = conn.path_params["stream_id"]

    case StreamManager.get_metadata(stream_id) do
      {:ok, meta} ->
        conn
        |> put_resp_header("content-type", meta.content_type)
        |> put_resp_header("x-stream-id", meta.id)
        |> put_resp_header("x-stream-created-at", DateTime.to_iso8601(meta.created_at))
        |> maybe_put_closed_header(meta.closed)
        |> maybe_put_ttl_header(meta.ttl)
        |> send_resp(200, "")

      {:error, :not_found} ->
        send_resp(conn, 404, "")
    end
  end

  defp maybe_put_closed_header(conn, true), do: put_resp_header(conn, "x-stream-closed", "true")
  defp maybe_put_closed_header(conn, _), do: conn

  defp maybe_put_ttl_header(conn, nil), do: conn
  defp maybe_put_ttl_header(conn, ttl), do: put_resp_header(conn, "x-stream-ttl", Integer.to_string(ttl))
end
