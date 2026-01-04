defmodule DurableStreams.StreamTest do
  use ExUnit.Case, async: true
  alias DurableStreams.Stream

  describe "new/2" do
    test "creates a stream with default content type" do
      stream = Stream.new("test-stream")
      assert stream.id == "test-stream"
      assert stream.content_type == "application/octet-stream"
      assert stream.closed == false
      assert stream.ttl == nil
      assert %DateTime{} = stream.created_at
    end

    test "creates a stream with custom content type" do
      stream = Stream.new("test-stream", content_type: "text/plain")
      assert stream.content_type == "text/plain"
    end

    test "creates a stream with ttl" do
      stream = Stream.new("test-stream", ttl: 3600)
      assert stream.ttl == 3600
    end
  end

  describe "json_mode?/1" do
    test "returns true for application/json content type" do
      stream = Stream.new("test-stream", content_type: "application/json")
      assert Stream.json_mode?(stream) == true
    end

    test "returns false for other content types" do
      stream1 = Stream.new("test-stream", content_type: "text/plain")
      stream2 = Stream.new("test-stream", content_type: "application/octet-stream")

      assert Stream.json_mode?(stream1) == false
      assert Stream.json_mode?(stream2) == false
    end
  end
end
