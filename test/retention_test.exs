defmodule DurableStreams.RetentionTest do
  use ExUnit.Case, async: true

  alias DurableStreams.{Stream, Offset}
  alias DurableStreams.Storage.ETS, as: Storage
  alias DurableStreams.Retention.Worker

  describe "Stream retention policy" do
    test "new stream with retention policy" do
      stream =
        Stream.new("test-retention", retention: [max_messages: 100, max_bytes: 1024])

      assert stream.retention_policy == %{max_messages: 100, max_bytes: 1024}
      assert stream.message_count == 0
      assert stream.total_bytes == 0
      assert stream.earliest_offset == nil
    end

    test "new stream without retention policy" do
      stream = Stream.new("test-no-retention")
      assert stream.retention_policy == nil
    end

    test "retention policy with max_age" do
      stream =
        Stream.new("test-age", retention: [max_age: :timer.hours(24)])

      assert stream.retention_policy == %{max_age: :timer.hours(24)}
    end
  end

  describe "Worker.needs_compaction?/1" do
    test "returns false for stream without retention policy" do
      stream = Stream.new("test")
      refute Worker.needs_compaction?(stream)
    end

    test "returns false for stream with nil retention_policy field" do
      stream = %{retention_policy: nil}
      refute Worker.needs_compaction?(stream)
    end

    test "returns true when max_messages exceeded" do
      stream = %{
        retention_policy: %{max_messages: 10},
        message_count: 15,
        total_bytes: 100
      }

      assert Worker.needs_compaction?(stream)
    end

    test "returns false when under max_messages limit" do
      stream = %{
        retention_policy: %{max_messages: 10},
        message_count: 5,
        total_bytes: 100
      }

      refute Worker.needs_compaction?(stream)
    end

    test "returns true when max_bytes exceeded" do
      stream = %{
        retention_policy: %{max_bytes: 1000},
        message_count: 5,
        total_bytes: 1500
      }

      assert Worker.needs_compaction?(stream)
    end

    test "returns false when under max_bytes limit" do
      stream = %{
        retention_policy: %{max_bytes: 1000},
        message_count: 5,
        total_bytes: 500
      }

      refute Worker.needs_compaction?(stream)
    end
  end

  describe "Storage message_count and total_bytes tracking" do
    setup do
      id = "retention-test-#{:rand.uniform(10000)}"
      stream = Stream.new(id)
      :ok = Storage.create(id, stream)

      on_exit(fn ->
        Storage.delete(id)
      end)

      {:ok, stream_id: id}
    end

    test "append updates message_count and total_bytes", %{stream_id: id} do
      {:ok, _} = Storage.append(id, "hello")
      {:ok, stream} = Storage.get_metadata(id)

      assert stream.message_count == 1
      assert stream.total_bytes == 5

      {:ok, _} = Storage.append(id, "world!")
      {:ok, stream} = Storage.get_metadata(id)

      assert stream.message_count == 2
      assert stream.total_bytes == 11
    end
  end

  describe "Offset.timestamp/1" do
    test "extracts timestamp from valid offset" do
      offset = Offset.generate(1)
      timestamp = Offset.timestamp(offset)

      assert is_integer(timestamp)
      # Timestamp should be recent (within last minute)
      now = System.system_time(:millisecond)
      assert timestamp > now - 60_000
      assert timestamp <= now
    end

    test "returns nil for start offset" do
      assert Offset.timestamp("-1") == nil
    end

    test "returns nil for invalid offset" do
      assert Offset.timestamp("invalid") == nil
      assert Offset.timestamp(nil) == nil
    end
  end

  describe "Storage retention query functions" do
    setup do
      id = "retention-query-#{:rand.uniform(10000)}"
      stream = Stream.new(id, retention: [max_messages: 5])
      :ok = Storage.create(id, stream)

      on_exit(fn ->
        Storage.delete(id)
      end)

      {:ok, stream_id: id}
    end

    test "get_first_message_timestamp returns timestamp of first message", %{stream_id: id} do
      # Initially no messages
      assert {:error, :no_messages} = Storage.get_first_message_timestamp(id)

      # Add a message
      {:ok, _offset} = Storage.append(id, "first")
      {:ok, timestamp} = Storage.get_first_message_timestamp(id)

      now = System.system_time(:millisecond)
      assert timestamp > now - 60_000
      assert timestamp <= now
    end

    test "find_offset_after_n_messages returns correct offset", %{stream_id: id} do
      # Add 5 messages
      offsets =
        for i <- 1..5 do
          {:ok, offset} = Storage.append(id, "msg#{i}")
          offset
        end

      # Skip 2 messages should give us the 3rd offset
      result = Storage.find_offset_after_n_messages(id, 2)
      assert result == Enum.at(offsets, 2)

      # Skip more than we have returns nil
      assert Storage.find_offset_after_n_messages(id, 10) == nil
    end

    test "find_offset_after_n_bytes returns offset after removing target bytes", %{stream_id: id} do
      # Add messages of known sizes
      {:ok, offset1} = Storage.append(id, "12345")
      {:ok, offset2} = Storage.append(id, "67890")
      {:ok, _offset3} = Storage.append(id, "abcde")

      # Need to remove 5 bytes, first message is exactly 5 bytes
      result = Storage.find_offset_after_n_bytes(id, 5)
      assert result == offset1

      # Need to remove 8 bytes, need to remove first message (5 bytes) + more
      result = Storage.find_offset_after_n_bytes(id, 8)
      assert result == offset2
    end

    test "delete_messages_before removes messages and returns stats", %{stream_id: id} do
      # Add messages
      {:ok, _offset1} = Storage.append(id, "first")
      {:ok, offset2} = Storage.append(id, "second")
      {:ok, _offset3} = Storage.append(id, "third")

      # Delete messages before offset2 (should delete offset1)
      {:ok, deleted_count, deleted_bytes} = Storage.delete_messages_before(id, offset2)

      assert deleted_count == 1
      assert deleted_bytes == 5

      # Verify remaining messages
      {:ok, result} = Storage.read(id, Offset.start())
      assert result.data == "secondthird"
    end

    test "update_after_compaction updates stream metadata", %{stream_id: id} do
      # Add messages
      {:ok, _} = Storage.append(id, "first")
      {:ok, offset2} = Storage.append(id, "second")
      {:ok, _} = Storage.append(id, "third")

      # Simulate compaction update
      :ok = Storage.update_after_compaction(id, offset2, 1, 5)

      {:ok, stream} = Storage.get_metadata(id)
      assert stream.earliest_offset == offset2
      assert stream.message_count == 2
      assert stream.total_bytes == 11
    end

    test "list_streams_with_retention returns only streams with policies" do
      # Our test stream has a retention policy
      streams = Storage.list_streams_with_retention()
      assert Enum.any?(streams, fn s -> String.starts_with?(s.id, "retention-query-") end)

      # Create a stream without retention
      no_retention_id = "no-retention-#{:rand.uniform(10000)}"
      :ok = Storage.create(no_retention_id, Stream.new(no_retention_id))

      on_exit(fn ->
        Storage.delete(no_retention_id)
      end)

      # It should not appear in the list
      streams = Storage.list_streams_with_retention()
      refute Enum.any?(streams, fn s -> s.id == no_retention_id end)
    end
  end

  describe "Worker.compact/1" do
    setup do
      id = "compact-test-#{:rand.uniform(10000)}"
      stream = Stream.new(id, retention: [max_messages: 3])
      :ok = Storage.create(id, stream)

      on_exit(fn ->
        Storage.delete(id)
      end)

      {:ok, stream_id: id}
    end

    test "compacts stream when max_messages exceeded", %{stream_id: id} do
      # Add 5 messages (exceeds max_messages: 3)
      for i <- 1..5 do
        {:ok, _} = Storage.append(id, "message-#{i}")
      end

      # Verify over limit
      {:ok, stream_before} = Storage.get_metadata(id)
      assert stream_before.message_count == 5

      # Run compaction
      assert :ok = Worker.compact(id)

      # Verify compacted
      {:ok, stream_after} = Storage.get_metadata(id)
      assert stream_after.message_count == 3
      assert stream_after.earliest_offset != nil
    end

    test "no compaction needed when under limits", %{stream_id: id} do
      # Add 2 messages (under max_messages: 3)
      for i <- 1..2 do
        {:ok, _} = Storage.append(id, "message-#{i}")
      end

      # Run compaction - should return :ok but not change anything
      assert :ok = Worker.compact(id)

      {:ok, stream} = Storage.get_metadata(id)
      assert stream.message_count == 2
      assert stream.earliest_offset == nil
    end

    test "returns error for non-existent stream" do
      assert {:error, :not_found} = Worker.compact("nonexistent-stream")
    end
  end

  describe "410 Gone response for compacted offsets" do
    setup do
      id = "gone-test-#{:rand.uniform(10000)}"
      stream = Stream.new(id, retention: [max_messages: 2])
      :ok = Storage.create(id, stream)

      on_exit(fn ->
        Storage.delete(id)
      end)

      {:ok, stream_id: id}
    end

    test "read returns data for valid offsets", %{stream_id: id} do
      {:ok, _} = Storage.append(id, "msg1")
      {:ok, _offset2} = Storage.append(id, "msg2")

      {:ok, result} = Storage.read(id, Offset.start())
      assert result.data == "msg1msg2"
    end

    test "compaction sets earliest_offset correctly", %{stream_id: id} do
      # Add 4 messages (exceeds max_messages: 2)
      offsets =
        for i <- 1..4 do
          {:ok, offset} = Storage.append(id, "m#{i}")
          offset
        end

      # Run compaction
      :ok = Worker.compact(id)

      {:ok, stream} = Storage.get_metadata(id)
      # Should keep last 2 messages
      assert stream.earliest_offset == Enum.at(offsets, 2)
    end
  end
end
