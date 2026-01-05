defmodule DurableStreams.Protocol.Handlers.Delete do
  @moduledoc """
  Internal handler for DELETE requests to delete a stream.

  This is an internal module used by `DurableStreams.Protocol.Plug`.
  """

  import Plug.Conn
  alias DurableStreams.{JSON, StreamManager}

  def call(conn) do
    stream_id = conn.path_params["stream_id"]

    case StreamManager.delete(stream_id) do
      :ok ->
        send_resp(conn, 204, "")

      {:error, :not_found} ->
        conn
        |> put_resp_header("content-type", "application/json")
        |> send_resp(404, JSON.encode!(%{error: "Stream not found"}))
    end
  end
end
