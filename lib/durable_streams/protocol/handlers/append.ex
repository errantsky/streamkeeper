defmodule DurableStreams.Protocol.Handlers.Append do
  @moduledoc """
  Handler for POST requests to append data to a stream.
  """

  import Plug.Conn
  alias DurableStreams.StreamManager

  def call(conn) do
    stream_id = conn.path_params["stream_id"]
    {:ok, body, conn} = read_body(conn)

    case StreamManager.append(stream_id, body) do
      {:ok, offset} ->
        conn
        |> put_resp_header("stream-next-offset", offset)
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{offset: offset}))

      {:error, :not_found} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(404, Jason.encode!(%{error: "Stream not found"}))

      {:error, :closed} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(409, Jason.encode!(%{error: "Stream is closed"}))
    end
  end
end
