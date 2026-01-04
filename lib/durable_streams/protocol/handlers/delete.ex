defmodule DurableStreams.Protocol.Handlers.Delete do
  @moduledoc """
  Handler for DELETE requests to delete a stream.
  """

  import Plug.Conn
  alias DurableStreams.StreamManager

  def call(conn) do
    stream_id = conn.path_params["stream_id"]

    case StreamManager.delete(stream_id) do
      :ok ->
        send_resp(conn, 204, "")

      {:error, :not_found} ->
        conn
        |> put_resp_header("content-type", "application/json")
        |> send_resp(404, Jason.encode!(%{error: "Stream not found"}))
    end
  end
end
