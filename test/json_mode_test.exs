defmodule DurableStreams.JsonModeTest do
  use ExUnit.Case
  import Plug.Test
  import Plug.Conn

  alias DurableStreams.Protocol.Plug, as: StreamPlug
  @opts StreamPlug.init([])

  setup do
    id = "json-test-#{System.unique_integer([:positive])}"
    on_exit(fn -> DurableStreams.delete(id) end)
    %{id: id}
  end

  describe "JSON mode - append" do
    test "single object is stored as one message", %{id: id} do
      # Create JSON stream
      conn(:put, "/#{id}")
      |> put_req_header("content-type", "application/json")
      |> StreamPlug.call(@opts)

      # Append single object (Content-Type required)
      conn =
        conn(:post, "/#{id}", ~s({"name": "test"}))
        |> put_req_header("content-type", "application/json")
        |> StreamPlug.call(@opts)

      assert conn.status == 200

      # Read back
      conn =
        conn(:get, "/#{id}?offset=-1")
        |> StreamPlug.call(@opts)

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert body == [%{"name" => "test"}]
    end

    test "array is flattened one level", %{id: id} do
      # Create JSON stream
      conn(:put, "/#{id}")
      |> put_req_header("content-type", "application/json")
      |> StreamPlug.call(@opts)

      # Append array (Content-Type required)
      conn =
        conn(:post, "/#{id}", ~s([{"a": 1}, {"b": 2}, {"c": 3}]))
        |> put_req_header("content-type", "application/json")
        |> StreamPlug.call(@opts)

      assert conn.status == 200

      # Read back - should get array of 3 items
      conn =
        conn(:get, "/#{id}?offset=-1")
        |> StreamPlug.call(@opts)

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert body == [%{"a" => 1}, %{"b" => 2}, %{"c" => 3}]
    end

    test "multiple appends create multiple messages", %{id: id} do
      # Create JSON stream
      conn(:put, "/#{id}")
      |> put_req_header("content-type", "application/json")
      |> StreamPlug.call(@opts)

      # Append first message (Content-Type required)
      conn(:post, "/#{id}", ~s({"msg": "first"}))
      |> put_req_header("content-type", "application/json")
      |> StreamPlug.call(@opts)

      # Append second message (Content-Type required)
      conn(:post, "/#{id}", ~s({"msg": "second"}))
      |> put_req_header("content-type", "application/json")
      |> StreamPlug.call(@opts)

      # Read back - should get array of 2 items
      conn =
        conn(:get, "/#{id}?offset=-1")
        |> StreamPlug.call(@opts)

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert body == [%{"msg" => "first"}, %{"msg" => "second"}]
    end

    test "invalid JSON returns 400", %{id: id} do
      # Create JSON stream
      conn(:put, "/#{id}")
      |> put_req_header("content-type", "application/json")
      |> StreamPlug.call(@opts)

      # Append invalid JSON (Content-Type required)
      conn =
        conn(:post, "/#{id}", "not valid json")
        |> put_req_header("content-type", "application/json")
        |> StreamPlug.call(@opts)

      assert conn.status == 400
    end
  end

  describe "JSON mode - read" do
    test "reading after offset returns correct messages", %{id: id} do
      # Create JSON stream
      conn(:put, "/#{id}")
      |> put_req_header("content-type", "application/json")
      |> StreamPlug.call(@opts)

      # Append first message (Content-Type required)
      conn1 =
        conn(:post, "/#{id}", ~s({"msg": "first"}))
        |> put_req_header("content-type", "application/json")
        |> StreamPlug.call(@opts)

      [offset1] = get_resp_header(conn1, "stream-next-offset")

      # Append second message (Content-Type required)
      conn(:post, "/#{id}", ~s({"msg": "second"}))
      |> put_req_header("content-type", "application/json")
      |> StreamPlug.call(@opts)

      # Read after first offset - should only get second message
      conn =
        conn(:get, "/#{id}?offset=#{offset1}")
        |> StreamPlug.call(@opts)

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert body == [%{"msg" => "second"}]
    end

    test "reading empty stream returns 200 with empty array", %{id: id} do
      # Create JSON stream
      conn(:put, "/#{id}")
      |> put_req_header("content-type", "application/json")
      |> StreamPlug.call(@opts)

      # Read empty stream - conformance spec requires 200 with empty array
      conn =
        conn(:get, "/#{id}?offset=-1")
        |> StreamPlug.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "[]"
    end
  end

  describe "binary mode unchanged" do
    test "binary streams still concatenate data", %{id: id} do
      # Create binary stream
      conn(:put, "/#{id}")
      |> put_req_header("content-type", "text/plain")
      |> StreamPlug.call(@opts)

      # Append multiple chunks (Content-Type required)
      conn(:post, "/#{id}", "Hello")
      |> put_req_header("content-type", "text/plain")
      |> StreamPlug.call(@opts)

      conn(:post, "/#{id}", " World")
      |> put_req_header("content-type", "text/plain")
      |> StreamPlug.call(@opts)

      # Read back - should be concatenated
      conn =
        conn(:get, "/#{id}?offset=-1")
        |> StreamPlug.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "Hello World"
    end
  end
end
