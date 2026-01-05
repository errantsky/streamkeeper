defmodule DurableStreams.SSETest do
  use ExUnit.Case

  import Plug.Test
  import Plug.Conn

  alias DurableStreams.Protocol.Plug, as: StreamPlug
  @opts StreamPlug.init([])

  # Note: SSE streaming behavior is more thoroughly tested by the conformance tests
  # which use actual HTTP connections. These unit tests focus on error cases and
  # header validation that work reliably with Plug.Test.

  setup do
    id = "sse-test-#{System.unique_integer([:positive])}"
    on_exit(fn -> DurableStreams.delete(id) end)
    %{id: id}
  end

  describe "SSE connection setup" do
    test "returns 400 when offset parameter is missing", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

      conn =
        conn(:get, "/#{id}?live=sse")
        |> StreamPlug.call(@opts)

      assert conn.status == 400
      assert Jason.decode!(conn.resp_body)["error"] =~ "Offset"
    end

    test "returns 404 for non-existent stream" do
      conn =
        conn(:get, "/nonexistent?live=sse&offset=-1")
        |> StreamPlug.call(@opts)

      assert conn.status == 404
    end

    test "sets correct SSE headers", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
      :ok = DurableStreams.close(id)

      conn =
        conn(:get, "/#{id}?live=sse&offset=-1")
        |> StreamPlug.call(@opts)

      assert conn.status == 200
      assert get_resp_header(conn, "content-type") == ["text/event-stream"]
      assert get_resp_header(conn, "cache-control") == ["no-cache, no-store, must-revalidate"]
    end
  end

  describe "SSE event format" do
    test "sends control event for empty stream", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
      :ok = DurableStreams.close(id)

      conn =
        conn(:get, "/#{id}?live=sse&offset=-1")
        |> StreamPlug.call(@opts)

      body = get_chunked_body(conn)

      # Should contain a control event
      assert body =~ "event: control\n"
      assert body =~ "streamCursor"
      assert body =~ "streamNextOffset"
      assert body =~ "upToDate"
    end

    test "control event includes closed flag when stream is closed", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
      :ok = DurableStreams.close(id)

      conn =
        conn(:get, "/#{id}?live=sse&offset=-1")
        |> StreamPlug.call(@opts)

      body = get_chunked_body(conn)

      # Parse the control event
      control_data = extract_control_event(body)
      assert control_data["closed"] == true
    end

    test "returns zero offset instead of -1 in control for empty stream", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
      :ok = DurableStreams.close(id)

      conn =
        conn(:get, "/#{id}?live=sse&offset=-1")
        |> StreamPlug.call(@opts)

      body = get_chunked_body(conn)
      control = extract_control_event(body)

      # Should return zero offset, not "-1"
      refute control["streamNextOffset"] == "-1"
    end
  end

  describe "SSE with binary data" do
    test "sends data event with correct format", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
      {:ok, offset} = DurableStreams.append(id, "hello world")
      :ok = DurableStreams.close(id)

      conn =
        conn(:get, "/#{id}?live=sse&offset=-1")
        |> StreamPlug.call(@opts)

      body = get_chunked_body(conn)

      # Should contain data event
      assert body =~ "event: data\n"
      assert body =~ "data: hello world\n"
      assert body =~ "id: #{offset}\n"
    end

    test "handles multiline data correctly", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
      {:ok, _} = DurableStreams.append(id, "line1\nline2\nline3")
      :ok = DurableStreams.close(id)

      conn =
        conn(:get, "/#{id}?live=sse&offset=-1")
        |> StreamPlug.call(@opts)

      body = get_chunked_body(conn)

      # Each line should be prefixed with "data: " per SSE spec
      assert body =~ "data: line1\n"
      assert body =~ "data: line2\n"
      assert body =~ "data: line3\n"
    end

    test "base64 encodes non-text data", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "application/octet-stream")
      binary_data = <<0, 1, 2, 3, 4, 5>>
      {:ok, _} = DurableStreams.append(id, binary_data)
      :ok = DurableStreams.close(id)

      conn =
        conn(:get, "/#{id}?live=sse&offset=-1")
        |> StreamPlug.call(@opts)

      body = get_chunked_body(conn)

      # Should contain base64 encoded data
      expected_base64 = Base.encode64(binary_data)
      assert body =~ expected_base64
    end
  end

  describe "SSE offset handling" do
    test "reads from specific offset", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
      {:ok, offset1} = DurableStreams.append(id, "first")
      {:ok, _} = DurableStreams.append(id, "second")
      :ok = DurableStreams.close(id)

      conn =
        conn(:get, "/#{id}?live=sse&offset=#{offset1}")
        |> StreamPlug.call(@opts)

      body = get_chunked_body(conn)

      # Should only get data after offset1
      assert body =~ "second"
      refute body =~ "data: first"
    end
  end

  describe "SSE cursor handling" do
    test "generates cursor in control event", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
      :ok = DurableStreams.close(id)

      conn =
        conn(:get, "/#{id}?live=sse&offset=-1")
        |> StreamPlug.call(@opts)

      body = get_chunked_body(conn)
      control = extract_control_event(body)

      assert control["streamCursor"] != nil
      # Cursor should be a numeric timestamp string
      assert {_, ""} = Integer.parse(control["streamCursor"])
    end

    test "accepts cursor in header for jitter handling", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
      :ok = DurableStreams.close(id)

      cursor = Integer.to_string(System.system_time(:millisecond))

      conn =
        conn(:get, "/#{id}?live=sse&offset=-1")
        |> put_req_header("stream-cursor", cursor)
        |> StreamPlug.call(@opts)

      body = get_chunked_body(conn)
      control = extract_control_event(body)

      # Server should return a cursor (may be the same or different based on jitter logic)
      assert control["streamCursor"] != nil
    end

    test "accepts cursor in query param", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
      :ok = DurableStreams.close(id)

      cursor = Integer.to_string(System.system_time(:millisecond))

      conn =
        conn(:get, "/#{id}?live=sse&offset=-1&cursor=#{cursor}")
        |> StreamPlug.call(@opts)

      body = get_chunked_body(conn)
      control = extract_control_event(body)

      assert control["streamCursor"] != nil
    end
  end

  # Helper to get the body from a chunked response
  defp get_chunked_body(%Plug.Conn{adapter: {_, %{chunks: chunks}}}) when is_binary(chunks) do
    chunks
  end

  defp get_chunked_body(%Plug.Conn{adapter: {_, %{chunks: chunks}}}) when is_list(chunks) do
    Enum.join(chunks, "")
  end

  defp get_chunked_body(_conn), do: ""

  # Helper to extract control event data from SSE body
  defp extract_control_event(body) do
    case Regex.run(~r/event: control\ndata: ({.*?})\n/, body) do
      [_, json] -> Jason.decode!(json)
      nil -> %{}
    end
  end
end
