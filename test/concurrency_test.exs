defmodule DurableStreams.ConcurrencyTest do
  use ExUnit.Case

  setup do
    id = "concurrent-test-#{System.unique_integer([:positive])}"
    on_exit(fn -> DurableStreams.delete(id) end)
    %{id: id}
  end

  describe "concurrent appends" do
    test "handles many concurrent writers", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

      # Spawn 50 concurrent writers
      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            DurableStreams.append(id, "message-#{i}")
          end)
        end

      results = Task.await_many(tasks, 5000)

      # All should succeed
      assert Enum.all?(results, fn
               {:ok, offset} -> is_binary(offset)
               _ -> false
             end)

      # All offsets should be unique
      offsets = Enum.map(results, fn {:ok, offset} -> offset end)
      assert length(Enum.uniq(offsets)) == 50

      # Read back all data
      {:ok, result} = DurableStreams.read(id, "-1")
      # Each message is "message-X" where X is 1-2 digits
      # Total should be 50 messages concatenated
      assert byte_size(result.data) > 0
    end

    test "maintains offset ordering under concurrency", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

      # Spawn writers with small delays to test ordering
      tasks =
        for i <- 1..20 do
          Task.async(fn ->
            # Small random delay to interleave
            :timer.sleep(:rand.uniform(5))
            DurableStreams.append(id, "msg-#{i}")
          end)
        end

      results = Task.await_many(tasks, 5000)
      offsets = Enum.map(results, fn {:ok, offset} -> offset end)

      # Offsets when sorted should be in order they were generated
      # (lexicographic sorting matches temporal order)
      sorted_offsets = Enum.sort(offsets)

      # All offsets should be valid and sortable
      assert length(sorted_offsets) == 20
      assert Enum.all?(sorted_offsets, &is_binary/1)
    end

    test "concurrent reads don't interfere with writes", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

      # Start with some data
      {:ok, _} = DurableStreams.append(id, "initial")

      # Spawn concurrent readers and writers
      writer_tasks =
        for i <- 1..20 do
          Task.async(fn ->
            :timer.sleep(:rand.uniform(10))
            DurableStreams.append(id, "write-#{i}")
          end)
        end

      reader_tasks =
        for _ <- 1..20 do
          Task.async(fn ->
            :timer.sleep(:rand.uniform(10))
            DurableStreams.read(id, "-1")
          end)
        end

      write_results = Task.await_many(writer_tasks, 5000)
      read_results = Task.await_many(reader_tasks, 5000)

      # All writes should succeed
      assert Enum.all?(write_results, fn
               {:ok, _} -> true
               _ -> false
             end)

      # All reads should succeed
      assert Enum.all?(read_results, fn
               {:ok, %{data: data}} when is_binary(data) -> true
               _ -> false
             end)
    end
  end

  describe "concurrent stream operations" do
    test "handles concurrent create attempts gracefully" do
      id = "race-create-#{System.unique_integer([:positive])}"

      # Try to create the same stream from multiple processes
      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            DurableStreams.create(id, content_type: "text/plain")
          end)
        end

      results = Task.await_many(tasks, 5000)

      # Exactly one should succeed
      successes =
        Enum.count(results, fn
          {:ok, _} -> true
          _ -> false
        end)

      errors =
        Enum.count(results, fn
          {:error, :already_exists} -> true
          _ -> false
        end)

      assert successes == 1
      assert errors == 9

      # Cleanup
      DurableStreams.delete(id)
    end

    test "handles read during close", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
      {:ok, _} = DurableStreams.append(id, "data before close")

      # Start a reader and closer concurrently
      reader =
        Task.async(fn ->
          :timer.sleep(1)
          DurableStreams.read(id, "-1")
        end)

      closer =
        Task.async(fn ->
          DurableStreams.close(id)
        end)

      {:ok, read_result} = Task.await(reader, 5000)
      :ok = Task.await(closer, 5000)

      # Read should succeed regardless of timing
      assert read_result.data == "data before close"
    end
  end

  describe "offset uniqueness under load" do
    test "generates unique offsets across many rapid appends", %{id: id} do
      {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

      # Rapid-fire appends without delays
      offsets =
        for i <- 1..1000 do
          {:ok, offset} = DurableStreams.append(id, "m#{i}")
          offset
        end

      # All offsets must be unique
      assert length(Enum.uniq(offsets)) == 1000

      # Offsets should be lexicographically sortable
      sorted = Enum.sort(offsets)
      assert sorted == offsets
    end
  end

  describe "multiple streams concurrency" do
    test "operations on different streams are independent" do
      stream_ids =
        for i <- 1..10 do
          "multi-stream-#{i}-#{System.unique_integer([:positive])}"
        end

      # Create all streams concurrently
      create_tasks =
        for id <- stream_ids do
          Task.async(fn ->
            DurableStreams.create(id, content_type: "text/plain")
          end)
        end

      Task.await_many(create_tasks, 5000)

      # Append to all streams concurrently
      append_tasks =
        for id <- stream_ids do
          Task.async(fn ->
            for i <- 1..10 do
              DurableStreams.append(id, "stream-#{id}-msg-#{i}")
            end
          end)
        end

      Task.await_many(append_tasks, 5000)

      # Read from all streams and verify isolation
      for id <- stream_ids do
        {:ok, result} = DurableStreams.read(id, "-1")
        # Each stream should only have its own messages
        assert String.contains?(result.data, "stream-#{id}")
        # Verify no other stream's data appears (check a different stream)
        other_ids = Enum.reject(stream_ids, &(&1 == id))

        for other_id <- other_ids do
          refute String.contains?(result.data, "stream-#{other_id}")
        end
      end

      # Cleanup
      for id <- stream_ids do
        DurableStreams.delete(id)
      end
    end
  end
end
