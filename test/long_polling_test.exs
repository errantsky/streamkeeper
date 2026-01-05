defmodule DurableStreams.LongPollingTest do
  use ExUnit.Case

  setup do
    id = "longpoll-test-#{System.unique_integer([:positive])}"
    {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
    on_exit(fn -> DurableStreams.delete(id) end)
    %{id: id}
  end

  describe "long-polling read" do
    test "returns immediately when data is available", %{id: id} do
      {:ok, _} = DurableStreams.append(id, "existing data")

      start_time = System.monotonic_time(:millisecond)
      {:ok, result} = DurableStreams.read(id, "-1", live: true, timeout: 5000)
      elapsed = System.monotonic_time(:millisecond) - start_time

      assert result.data == "existing data"
      # Should return almost immediately, not wait for timeout
      assert elapsed < 100
    end

    test "waits for new data when none available", %{id: id} do
      {:ok, offset} = DurableStreams.append(id, "initial")

      # Start a long-poll reader
      reader = Task.async(fn ->
        DurableStreams.read(id, offset, live: true, timeout: 5000)
      end)

      # Wait a bit, then append new data
      :timer.sleep(100)
      {:ok, _} = DurableStreams.append(id, "new data")

      # Reader should get the new data
      {:ok, result} = Task.await(reader, 6000)
      assert result.data == "new data"
    end

    test "times out when no data arrives", %{id: id} do
      {:ok, offset} = DurableStreams.append(id, "initial")

      start_time = System.monotonic_time(:millisecond)
      {:ok, result} = DurableStreams.read(id, offset, live: true, timeout: 500)
      elapsed = System.monotonic_time(:millisecond) - start_time

      # Should return empty after timeout
      assert result.data == <<>>
      # Should have waited approximately the timeout duration
      assert elapsed >= 450
      assert elapsed < 1000
    end

    test "multiple waiters all receive data", %{id: id} do
      {:ok, offset} = DurableStreams.append(id, "initial")

      # Start multiple readers
      readers =
        for _ <- 1..5 do
          Task.async(fn ->
            DurableStreams.read(id, offset, live: true, timeout: 5000)
          end)
        end

      # Wait a bit, then append
      :timer.sleep(50)
      {:ok, _} = DurableStreams.append(id, "broadcast")

      # All readers should get the data
      results = Task.await_many(readers, 6000)

      assert Enum.all?(results, fn
               {:ok, %{data: "broadcast"}} -> true
               _ -> false
             end)
    end

    test "returns immediately when stream is closed", %{id: id} do
      {:ok, offset} = DurableStreams.append(id, "initial")

      # Start a long-poll reader
      reader = Task.async(fn ->
        DurableStreams.read(id, offset, live: true, timeout: 10_000)
      end)

      # Close the stream
      :timer.sleep(50)
      :ok = DurableStreams.close(id)

      # Reader should return with closed status
      {:ok, result} = Task.await(reader, 1000)
      assert result.closed == true
    end

    test "waiter receives data appended during wait", %{id: id} do
      # Start waiting before any data exists
      reader = Task.async(fn ->
        DurableStreams.read(id, "-1", live: true, timeout: 5000)
      end)

      # Small delay then append
      :timer.sleep(100)
      {:ok, _} = DurableStreams.append(id, "first message")

      {:ok, result} = Task.await(reader, 6000)
      assert result.data == "first message"
    end

    test "sequential long-polls work correctly", %{id: id} do
      # First append
      {:ok, offset1} = DurableStreams.append(id, "first")

      # First long-poll (should return immediately)
      {:ok, result1} = DurableStreams.read(id, "-1", live: true, timeout: 1000)
      assert result1.data == "first"

      # Second long-poll with returned offset, start waiting
      reader = Task.async(fn ->
        DurableStreams.read(id, result1.offset, live: true, timeout: 5000)
      end)

      # Append new data
      :timer.sleep(50)
      {:ok, _} = DurableStreams.append(id, "second")

      # Should receive only the new data
      {:ok, result2} = Task.await(reader, 6000)
      assert result2.data == "second"
    end
  end

  describe "long-polling with JSON mode" do
    test "waits for new messages in JSON mode" do
      id = "json-longpoll-#{System.unique_integer([:positive])}"
      {:ok, _} = DurableStreams.create(id, content_type: "application/json")

      {:ok, offset} = DurableStreams.StreamManager.append(id, ~s({"msg": "initial"}))

      reader = Task.async(fn ->
        DurableStreams.StreamManager.read_messages(id, offset, live: true, timeout: 5000)
      end)

      :timer.sleep(100)
      {:ok, _} = DurableStreams.StreamManager.append(id, ~s({"msg": "new"}))

      {:ok, result} = Task.await(reader, 6000)
      assert length(result.messages) == 1
      assert Enum.at(result.messages, 0).data == ~s({"msg": "new"})

      DurableStreams.delete(id)
    end
  end

  describe "edge cases" do
    test "long-poll on deleted stream returns not_found", %{id: id} do
      {:ok, offset} = DurableStreams.append(id, "data")

      reader = Task.async(fn ->
        :timer.sleep(50)
        DurableStreams.read(id, offset, live: true, timeout: 5000)
      end)

      DurableStreams.delete(id)

      result = Task.await(reader, 6000)
      assert result == {:error, :not_found}
    end

    test "handles rapid append during long-poll wait", %{id: id} do
      reader = Task.async(fn ->
        DurableStreams.read(id, "-1", live: true, timeout: 5000)
      end)

      # Rapid appends
      :timer.sleep(50)
      for i <- 1..10 do
        DurableStreams.append(id, "msg#{i}")
      end

      {:ok, result} = Task.await(reader, 6000)
      # Should get at least the first message (may get more depending on timing)
      assert byte_size(result.data) > 0
    end
  end
end
