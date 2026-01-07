defmodule DurableStreams.Protocol.PlugTest do
  use ExUnit.Case
  import Plug.Test
  import Plug.Conn

  alias DurableStreams.Protocol.Plug, as: StreamPlug
  @opts StreamPlug.init([])

  setup do
    id = "test-#{System.unique_integer([:positive])}"
    on_exit(fn -> DurableStreams.delete(id) end)
    %{id: id}
  end

  describe "PUT /:stream_id" do
    test "creates a new stream", %{id: id} do
      conn =
        conn(:put, "/#{id}")
        |> put_req_header("content-type", "text/plain")
        |> StreamPlug.call(@opts)

      assert conn.status == 201
      # Location is now an absolute URL per conformance spec
      [location] = get_resp_header(conn, "location")
      assert String.ends_with?(location, "/v1/stream/#{id}")
    end

    test "returns 200 for idempotent create with same config", %{id: id} do
      # First create
      conn(:put, "/#{id}")
      |> put_req_header("content-type", "text/plain")
      |> StreamPlug.call(@opts)

      # Second create with same config returns 200 (idempotent)
      conn =
        conn(:put, "/#{id}")
        |> put_req_header("content-type", "text/plain")
        |> StreamPlug.call(@opts)

      assert conn.status == 200
    end

    test "returns 409 for create with different config", %{id: id} do
      # First create with text/plain
      conn(:put, "/#{id}")
      |> put_req_header("content-type", "text/plain")
      |> StreamPlug.call(@opts)

      # Second create with different content-type should fail
      conn =
        conn(:put, "/#{id}")
        |> put_req_header("content-type", "application/json")
        |> StreamPlug.call(@opts)

      assert conn.status == 409
    end
  end

  describe "POST /:stream_id" do
    test "appends data to stream", %{id: id} do
      # Create stream first
      conn(:put, "/#{id}")
      |> put_req_header("content-type", "text/plain")
      |> StreamPlug.call(@opts)

      # Append data (Content-Type required per conformance spec)
      conn =
        conn(:post, "/#{id}", "Hello!")
        |> put_req_header("content-type", "text/plain")
        |> StreamPlug.call(@opts)

      assert conn.status == 200
      assert get_resp_header(conn, "stream-next-offset") != []

      body = DurableStreams.JSON.decode!(conn.resp_body)
      assert is_binary(body["offset"])
    end

    test "returns 400 when Content-Type is missing" do
      conn =
        conn(:post, "/nonexistent", "data")
        |> StreamPlug.call(@opts)

      assert conn.status == 400
    end

    test "returns 404 for non-existent stream" do
      conn =
        conn(:post, "/nonexistent", "data")
        |> put_req_header("content-type", "text/plain")
        |> StreamPlug.call(@opts)

      assert conn.status == 404
    end
  end

  describe "GET /:stream_id" do
    test "reads data from stream", %{id: id} do
      # Create and append
      conn(:put, "/#{id}")
      |> put_req_header("content-type", "text/plain")
      |> StreamPlug.call(@opts)

      conn(:post, "/#{id}", "Hello")
      |> put_req_header("content-type", "text/plain")
      |> StreamPlug.call(@opts)

      # Read
      conn =
        conn(:get, "/#{id}?offset=-1")
        |> StreamPlug.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "Hello"
      assert get_resp_header(conn, "stream-next-offset") != []

      assert get_resp_header(conn, "cache-control") == [
               "public, max-age=60, stale-while-revalidate=300"
             ]
    end

    test "returns 200 with empty body when no data after offset", %{id: id} do
      # Create stream
      conn(:put, "/#{id}")
      |> put_req_header("content-type", "text/plain")
      |> StreamPlug.call(@opts)

      # Append and get offset
      append_conn =
        conn(:post, "/#{id}", "Data")
        |> put_req_header("content-type", "text/plain")
        |> StreamPlug.call(@opts)

      [offset] = get_resp_header(append_conn, "stream-next-offset")

      # Read after offset - conformance spec requires 200 with empty body
      conn =
        conn(:get, "/#{id}?offset=#{offset}")
        |> StreamPlug.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == ""
    end

    test "returns 404 for non-existent stream" do
      conn =
        conn(:get, "/nonexistent?offset=-1")
        |> StreamPlug.call(@opts)

      assert conn.status == 404
    end

    test "returns stream-closed header when stream is closed", %{id: id} do
      # Create, append, and close
      conn(:put, "/#{id}")
      |> put_req_header("content-type", "text/plain")
      |> StreamPlug.call(@opts)

      conn(:post, "/#{id}", "Data")
      |> put_req_header("content-type", "text/plain")
      |> StreamPlug.call(@opts)

      DurableStreams.close(id)

      # Read
      conn =
        conn(:get, "/#{id}?offset=-1")
        |> StreamPlug.call(@opts)

      assert conn.status == 200
      assert get_resp_header(conn, "stream-closed") == ["true"]
    end
  end

  describe "DELETE /:stream_id" do
    test "deletes a stream", %{id: id} do
      # Create first
      conn(:put, "/#{id}")
      |> StreamPlug.call(@opts)

      # Delete
      conn =
        conn(:delete, "/#{id}")
        |> StreamPlug.call(@opts)

      assert conn.status == 204

      # Verify deleted
      conn =
        conn(:get, "/#{id}?offset=-1")
        |> StreamPlug.call(@opts)

      assert conn.status == 404
    end

    test "returns 404 for non-existent stream" do
      conn =
        conn(:delete, "/nonexistent")
        |> StreamPlug.call(@opts)

      assert conn.status == 404
    end
  end

  describe "HEAD /:stream_id" do
    test "returns stream metadata", %{id: id} do
      # Create first
      conn(:put, "/#{id}")
      |> put_req_header("content-type", "text/plain")
      |> StreamPlug.call(@opts)

      # HEAD
      conn =
        conn(:head, "/#{id}")
        |> StreamPlug.call(@opts)

      assert conn.status == 200
      # Content-type may or may not have charset depending on Plug version
      [content_type] = get_resp_header(conn, "content-type")
      assert String.starts_with?(content_type, "text/plain")
      # Conformance spec uses stream-* headers (not x-stream-*)
      assert get_resp_header(conn, "stream-id") == [id]
      assert get_resp_header(conn, "stream-next-offset") != []
    end

    test "returns 404 for non-existent stream" do
      conn =
        conn(:head, "/nonexistent")
        |> StreamPlug.call(@opts)

      assert conn.status == 404
    end
  end

  describe "create, append, read cycle" do
    test "full workflow", %{id: id} do
      # Create
      conn =
        conn(:put, "/#{id}")
        |> put_req_header("content-type", "text/plain")
        |> StreamPlug.call(@opts)

      assert conn.status == 201

      # Append multiple times (Content-Type required)
      conn(:post, "/#{id}", "Hello")
      |> put_req_header("content-type", "text/plain")
      |> StreamPlug.call(@opts)

      conn(:post, "/#{id}", " ")
      |> put_req_header("content-type", "text/plain")
      |> StreamPlug.call(@opts)

      conn(:post, "/#{id}", "World!")
      |> put_req_header("content-type", "text/plain")
      |> StreamPlug.call(@opts)

      # Read all
      conn =
        conn(:get, "/#{id}?offset=-1")
        |> StreamPlug.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "Hello World!"
    end
  end
end
