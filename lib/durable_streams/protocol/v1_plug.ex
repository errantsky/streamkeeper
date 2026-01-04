defmodule DurableStreams.Protocol.V1Plug do
  @moduledoc """
  Plug router with /v1/stream prefix for conformance testing.

  This router wraps the main Protocol.Plug and adds the /v1/stream prefix
  that the Durable Streams conformance tests expect.

  ## Standalone Usage

      # Start server for conformance tests
      {:ok, _} = Plug.Cowboy.http(DurableStreams.Protocol.V1Plug, [], port: 4437)
  """

  use Plug.Router

  plug Plug.Logger
  plug :match
  plug :dispatch

  forward "/v1/stream", to: DurableStreams.Protocol.Plug

  match _ do
    send_resp(conn, 404, "Not Found")
  end
end
