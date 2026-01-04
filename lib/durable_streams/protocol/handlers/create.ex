defmodule DurableStreams.Protocol.Handlers.Create do
  @moduledoc """
  Handler for PUT requests to create a new stream.
  """

  import Plug.Conn
  alias DurableStreams.StreamManager

  def call(conn) do
    stream_id = conn.path_params["stream_id"]

    content_type =
      get_req_header(conn, "content-type") |> List.first() || "application/octet-stream"

    case StreamManager.create(stream_id, content_type: content_type) do
      {:ok, ^stream_id} ->
        conn
        |> put_resp_header("location", "/v1/stream/#{stream_id}")
        |> put_resp_content_type("application/json")
        |> send_resp(201, Jason.encode!(%{id: stream_id, content_type: content_type}))

      {:error, :already_exists} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(409, Jason.encode!(%{error: "Stream already exists"}))
    end
  end
end
