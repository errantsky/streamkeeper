defmodule DurableStreams.TTLTest do
  use ExUnit.Case

  alias DurableStreams.Stream

  describe "Stream TTL configuration" do
    test "creates stream with TTL" do
      stream = Stream.new("test", ttl: 3600)

      assert stream.ttl == 3600
      assert stream.expires_at != nil
      assert DateTime.compare(stream.expires_at, stream.created_at) == :gt
    end

    test "creates stream with expires_at" do
      future = DateTime.add(DateTime.utc_now(), 3600, :second)
      stream = Stream.new("test", expires_at: future)

      assert stream.expires_at == future
      assert stream.ttl == nil
    end

    test "creates stream without expiration" do
      stream = Stream.new("test")

      assert stream.ttl == nil
      assert stream.expires_at == nil
    end

    test "TTL computes correct expires_at" do
      before = DateTime.utc_now()
      stream = Stream.new("test", ttl: 60)
      after_create = DateTime.utc_now()

      # expires_at should be approximately 60 seconds from now
      expected_min = DateTime.add(before, 60, :second)
      expected_max = DateTime.add(after_create, 60, :second)

      assert DateTime.compare(stream.expires_at, expected_min) in [:gt, :eq]
      assert DateTime.compare(stream.expires_at, expected_max) in [:lt, :eq]
    end
  end

  describe "Stream expiration via HTTP" do
    setup do
      id = "ttl-http-test-#{System.unique_integer([:positive])}"
      on_exit(fn -> DurableStreams.delete(id) end)
      %{id: id}
    end

    test "stream with short TTL expires", %{id: id} do
      # Create stream with 1 second TTL
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain", ttl: 1)
      {:ok, _} = DurableStreams.append(id, "data")

      # Should work immediately
      {:ok, result} = DurableStreams.read(id, "-1")
      assert result.data == "data"

      # Wait for expiration
      :timer.sleep(1500)

      # Stream should be gone (TTL expired)
      # Note: The actual expiration check happens in StreamServer
      # This depends on implementation - may need adjustment
      assert {:error, :not_found} = DurableStreams.get_metadata(id)
    end

    test "stream metadata includes TTL info", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain", ttl: 3600)
      {:ok, meta} = DurableStreams.get_metadata(id)

      assert meta.ttl == 3600
      assert meta.expires_at != nil
    end
  end

  describe "Stream expiration via Plug" do
    import Plug.Test
    import Plug.Conn

    alias DurableStreams.Protocol.Plug, as: StreamPlug
    @opts StreamPlug.init([])

    setup do
      id = "ttl-plug-test-#{System.unique_integer([:positive])}"
      on_exit(fn -> DurableStreams.delete(id) end)
      %{id: id}
    end

    test "creates stream with Stream-TTL header", %{id: id} do
      conn =
        conn(:put, "/#{id}")
        |> put_req_header("content-type", "text/plain")
        |> put_req_header("stream-ttl", "3600")
        |> StreamPlug.call(@opts)

      assert conn.status == 201

      {:ok, meta} = DurableStreams.get_metadata(id)
      assert meta.ttl == 3600
    end

    test "creates stream with Stream-Expires-At header", %{id: id} do
      future = DateTime.utc_now() |> DateTime.add(3600, :second) |> DateTime.to_iso8601()

      conn =
        conn(:put, "/#{id}")
        |> put_req_header("content-type", "text/plain")
        |> put_req_header("stream-expires-at", future)
        |> StreamPlug.call(@opts)

      assert conn.status == 201

      {:ok, meta} = DurableStreams.get_metadata(id)
      assert meta.expires_at != nil
    end

    test "rejects both TTL and Expires-At headers", %{id: id} do
      future = DateTime.utc_now() |> DateTime.add(3600, :second) |> DateTime.to_iso8601()

      conn =
        conn(:put, "/#{id}")
        |> put_req_header("content-type", "text/plain")
        |> put_req_header("stream-ttl", "3600")
        |> put_req_header("stream-expires-at", future)
        |> StreamPlug.call(@opts)

      assert conn.status == 400
    end

    test "rejects invalid TTL values", %{id: id} do
      conn =
        conn(:put, "/#{id}")
        |> put_req_header("content-type", "text/plain")
        |> put_req_header("stream-ttl", "not-a-number")
        |> StreamPlug.call(@opts)

      assert conn.status == 400
    end

    test "rejects zero TTL", %{id: id} do
      conn =
        conn(:put, "/#{id}")
        |> put_req_header("content-type", "text/plain")
        |> put_req_header("stream-ttl", "0")
        |> StreamPlug.call(@opts)

      assert conn.status == 400
    end

    test "rejects negative TTL", %{id: id} do
      conn =
        conn(:put, "/#{id}")
        |> put_req_header("content-type", "text/plain")
        |> put_req_header("stream-ttl", "-100")
        |> StreamPlug.call(@opts)

      assert conn.status == 400
    end

    test "rejects TTL with leading zeros", %{id: id} do
      conn =
        conn(:put, "/#{id}")
        |> put_req_header("content-type", "text/plain")
        |> put_req_header("stream-ttl", "0100")
        |> StreamPlug.call(@opts)

      assert conn.status == 400
    end

    test "rejects invalid Expires-At format", %{id: id} do
      conn =
        conn(:put, "/#{id}")
        |> put_req_header("content-type", "text/plain")
        |> put_req_header("stream-expires-at", "not-a-date")
        |> StreamPlug.call(@opts)

      assert conn.status == 400
    end
  end
end
