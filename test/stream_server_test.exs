defmodule DurableStreams.Server.StreamServerTest do
  use ExUnit.Case

  alias DurableStreams.Server.StreamServer

  setup do
    id = "server-test-#{System.unique_integer([:positive])}"
    on_exit(fn -> DurableStreams.delete(id) end)
    %{id: id}
  end

  describe "process registration" do
    test "registers process in registry on creation", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

      # Should be able to look up the process
      [{pid, _}] = Registry.lookup(DurableStreams.Registry, id)
      assert is_pid(pid)
      assert Process.alive?(pid)
    end

    test "process is removed from registry on delete", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

      # Verify registration
      [{pid, _}] = Registry.lookup(DurableStreams.Registry, id)
      assert Process.alive?(pid)

      # Delete the stream
      :ok = DurableStreams.delete(id)

      # Process should be gone
      :timer.sleep(50)
      assert Registry.lookup(DurableStreams.Registry, id) == []
    end

    test "different streams have different processes" do
      id1 = "server-test-multi-1-#{System.unique_integer([:positive])}"
      id2 = "server-test-multi-2-#{System.unique_integer([:positive])}"

      {:ok, _} = DurableStreams.create(id1, content_type: "text/plain")
      {:ok, _} = DurableStreams.create(id2, content_type: "text/plain")

      [{pid1, _}] = Registry.lookup(DurableStreams.Registry, id1)
      [{pid2, _}] = Registry.lookup(DurableStreams.Registry, id2)

      assert pid1 != pid2

      DurableStreams.delete(id1)
      DurableStreams.delete(id2)
    end
  end

  describe "GenServer behavior" do
    test "handle_call for append works correctly", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

      {:ok, offset} = StreamServer.append(id, "data")
      assert is_binary(offset)
    end

    test "handle_call for read works correctly", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
      {:ok, _} = StreamServer.append(id, "test data")

      {:ok, result} = StreamServer.read(id, "-1")
      assert result.data == "test data"
    end

    test "handle_call for get_metadata works correctly", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

      {:ok, meta} = StreamServer.get_metadata(id)
      assert meta.id == id
      assert meta.content_type == "text/plain"
    end

    test "handle_call for close works correctly", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

      :ok = StreamServer.close(id)

      {:ok, meta} = StreamServer.get_metadata(id)
      assert meta.closed == true
    end
  end

  describe "waiter management" do
    test "waiters are notified on append", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

      # Start a waiter
      reader = Task.async(fn ->
        StreamServer.read(id, "-1", live: true, timeout: 5000)
      end)

      # Small delay then append
      :timer.sleep(50)
      {:ok, _} = StreamServer.append(id, "new data")

      {:ok, result} = Task.await(reader, 6000)
      assert result.data == "new data"
    end

    test "waiters are notified on close", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
      {:ok, offset} = StreamServer.append(id, "initial")

      # Start a waiter
      reader = Task.async(fn ->
        StreamServer.read(id, offset, live: true, timeout: 5000)
      end)

      # Close the stream
      :timer.sleep(50)
      :ok = StreamServer.close(id)

      {:ok, result} = Task.await(reader, 1000)
      assert result.closed == true
    end

    test "waiter timeout returns empty result", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
      {:ok, offset} = StreamServer.append(id, "initial")

      start_time = System.monotonic_time(:millisecond)
      {:ok, result} = StreamServer.read(id, offset, live: true, timeout: 200)
      elapsed = System.monotonic_time(:millisecond) - start_time

      assert result.data == <<>>
      assert elapsed >= 180
      assert elapsed < 500
    end

    test "multiple waiters are all notified", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
      {:ok, offset} = StreamServer.append(id, "initial")

      # Start multiple waiters
      readers =
        for _ <- 1..5 do
          Task.async(fn ->
            StreamServer.read(id, offset, live: true, timeout: 5000)
          end)
        end

      # Append new data
      :timer.sleep(50)
      {:ok, _} = StreamServer.append(id, "broadcast")

      # All readers should receive the data
      results = Task.await_many(readers, 6000)

      assert Enum.all?(results, fn
               {:ok, %{data: "broadcast"}} -> true
               _ -> false
             end)
    end
  end

  describe "read_messages with waiters" do
    test "waiters for messages are notified on append", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "application/json")
      {:ok, offset} = StreamServer.append(id, ~s({"init": true}))

      # Start a waiter for messages
      reader = Task.async(fn ->
        StreamServer.read_messages(id, offset, live: true, timeout: 5000)
      end)

      # Append new data
      :timer.sleep(50)
      {:ok, _} = StreamServer.append(id, ~s({"new": true}))

      {:ok, result} = Task.await(reader, 6000)
      assert length(result.messages) == 1
      assert hd(result.messages).data == ~s({"new": true})
    end
  end

  describe "TTL expiration" do
    test "stream process terminates on TTL expiration" do
      id = "ttl-expire-#{System.unique_integer([:positive])}"
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain", ttl: 1)

      # Get the process
      [{pid, _}] = Registry.lookup(DurableStreams.Registry, id)
      assert Process.alive?(pid)

      # Wait for expiration
      :timer.sleep(1500)

      # Process should be dead
      refute Process.alive?(pid)
      assert Registry.lookup(DurableStreams.Registry, id) == []

      # Stream should be deleted from storage
      assert {:error, :not_found} = DurableStreams.get_metadata(id)
    end

    test "stream process terminates at expires_at time" do
      id = "expires-at-#{System.unique_integer([:positive])}"
      expires_at = DateTime.utc_now() |> DateTime.add(1, :second)
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain", expires_at: expires_at)

      # Get the process
      [{pid, _}] = Registry.lookup(DurableStreams.Registry, id)
      assert Process.alive?(pid)

      # Wait for expiration
      :timer.sleep(1500)

      # Process should be dead
      refute Process.alive?(pid)
    end
  end

  describe "process isolation" do
    test "error in one stream doesn't affect another" do
      id1 = "isolation-1-#{System.unique_integer([:positive])}"
      id2 = "isolation-2-#{System.unique_integer([:positive])}"

      {:ok, _} = DurableStreams.create(id1, content_type: "text/plain")
      {:ok, _} = DurableStreams.create(id2, content_type: "text/plain")

      # Both streams should work
      {:ok, _} = DurableStreams.append(id1, "data1")
      {:ok, _} = DurableStreams.append(id2, "data2")

      # Delete one stream
      DurableStreams.delete(id1)

      # Other stream should still work
      {:ok, _} = DurableStreams.append(id2, "more data")
      {:ok, result} = DurableStreams.read(id2, "-1")
      assert result.data == "data2more data"

      DurableStreams.delete(id2)
    end
  end

  describe "init behavior" do
    test "returns error for duplicate stream creation", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

      # Second creation should fail
      result = DurableStreams.create(id, content_type: "text/plain")
      assert result == {:error, :already_exists}
    end

    test "subscribes to storage notifications on init", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

      # The server should be subscribed - verify by checking that appends notify waiters
      reader = Task.async(fn ->
        StreamServer.read(id, "-1", live: true, timeout: 5000)
      end)

      :timer.sleep(50)
      {:ok, _} = StreamServer.append(id, "data")

      {:ok, result} = Task.await(reader, 6000)
      assert result.data == "data"
    end
  end

  describe "via_tuple registration" do
    test "can call server functions using stream_id", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

      # All these should work via the via_tuple registration
      {:ok, offset} = StreamServer.append(id, "test")
      {:ok, meta} = StreamServer.get_metadata(id)
      {:ok, result} = StreamServer.read(id, "-1")
      :ok = StreamServer.close(id)

      assert is_binary(offset)
      assert meta.id == id
      assert result.data == "test"
    end
  end
end
