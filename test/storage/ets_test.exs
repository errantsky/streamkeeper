defmodule DurableStreams.Storage.ETSTest do
  use ExUnit.Case

  alias DurableStreams.Storage.ETS, as: Storage
  alias DurableStreams.{Stream, Offset}

  setup do
    id = "storage-test-#{System.unique_integer([:positive])}"
    stream = Stream.new(id, content_type: "text/plain")
    on_exit(fn -> Storage.delete(id) end)
    %{id: id, stream: stream}
  end

  describe "create/2" do
    test "creates a new stream in storage", %{id: id, stream: stream} do
      assert :ok = Storage.create(id, stream)
      assert {:ok, stored} = Storage.get_metadata(id)
      assert stored.id == id
      assert stored.content_type == "text/plain"
    end

    test "returns error for duplicate stream", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      assert {:error, :already_exists} = Storage.create(id, stream)
    end

    test "initializes sequence counter to zero", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      assert {:ok, "-1"} = Storage.current_offset(id)
    end
  end

  describe "get_metadata/1" do
    test "returns stream metadata", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      {:ok, meta} = Storage.get_metadata(id)

      assert meta.id == id
      assert meta.content_type == stream.content_type
      assert meta.closed == false
    end

    test "returns not_found for non-existent stream" do
      assert {:error, :not_found} = Storage.get_metadata("nonexistent")
    end
  end

  describe "append/3" do
    test "appends data and returns offset", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      {:ok, offset} = Storage.append(id, "Hello")

      assert is_binary(offset)
      assert offset != "-1"
    end

    test "generates unique offsets for each append", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      {:ok, offset1} = Storage.append(id, "First")
      {:ok, offset2} = Storage.append(id, "Second")

      assert offset1 != offset2
      assert Offset.after?(offset2, offset1)
    end

    test "returns error for non-existent stream" do
      assert {:error, :not_found} = Storage.append("nonexistent", "data")
    end

    test "returns error for closed stream", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      :ok = Storage.close(id)

      assert {:error, :closed} = Storage.append(id, "data")
    end

    test "enforces sequence ordering when seq provided", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)

      assert {:ok, _} = Storage.append(id, "first", "a")
      assert {:ok, _} = Storage.append(id, "second", "b")
      assert {:error, :seq_conflict} = Storage.append(id, "third", "a")
    end

    test "rejects duplicate seq values", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)

      assert {:ok, _} = Storage.append(id, "first", "seq1")
      assert {:error, :seq_conflict} = Storage.append(id, "duplicate", "seq1")
    end

    test "allows append without seq after seq was used", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)

      assert {:ok, _} = Storage.append(id, "with-seq", "a")
      assert {:ok, _} = Storage.append(id, "without-seq", nil)
    end
  end

  describe "read/2" do
    test "reads all data from start offset", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      {:ok, _} = Storage.append(id, "Hello")
      {:ok, _} = Storage.append(id, " World")

      {:ok, result} = Storage.read(id, "-1")

      assert result.data == "Hello World"
      assert result.has_more == false
      assert result.closed == false
    end

    test "reads data after specific offset", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      {:ok, offset1} = Storage.append(id, "First")
      {:ok, _offset2} = Storage.append(id, "Second")

      {:ok, result} = Storage.read(id, offset1)

      assert result.data == "Second"
    end

    test "returns empty data when no new data", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      {:ok, offset} = Storage.append(id, "Data")

      {:ok, result} = Storage.read(id, offset)

      assert result.data == <<>>
      assert result.offset == offset
    end

    test "returns error for non-existent stream" do
      assert {:error, :not_found} = Storage.read("nonexistent", "-1")
    end

    test "indicates closed status", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      {:ok, _} = Storage.append(id, "Data")
      :ok = Storage.close(id)

      {:ok, result} = Storage.read(id, "-1")

      assert result.closed == true
    end
  end

  describe "read_messages/2" do
    test "returns messages as list", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      {:ok, offset1} = Storage.append(id, "First")
      {:ok, offset2} = Storage.append(id, "Second")

      {:ok, result} = Storage.read_messages(id, "-1")

      assert length(result.messages) == 2
      assert Enum.at(result.messages, 0) == %{data: "First", offset: offset1}
      assert Enum.at(result.messages, 1) == %{data: "Second", offset: offset2}
    end

    test "returns empty list for empty stream", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)

      {:ok, result} = Storage.read_messages(id, "-1")

      assert result.messages == []
    end

    test "filters messages after offset", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      {:ok, offset1} = Storage.append(id, "First")
      {:ok, offset2} = Storage.append(id, "Second")

      {:ok, result} = Storage.read_messages(id, offset1)

      assert length(result.messages) == 1
      assert Enum.at(result.messages, 0) == %{data: "Second", offset: offset2}
    end
  end

  describe "close/1" do
    test "closes a stream", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      assert :ok = Storage.close(id)

      {:ok, meta} = Storage.get_metadata(id)
      assert meta.closed == true
    end

    test "returns error for non-existent stream" do
      assert {:error, :not_found} = Storage.close("nonexistent")
    end
  end

  describe "delete/1" do
    test "deletes stream and all data", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      {:ok, _} = Storage.append(id, "Data")

      assert :ok = Storage.delete(id)
      assert {:error, :not_found} = Storage.get_metadata(id)
      assert {:error, :not_found} = Storage.read(id, "-1")
    end

    test "returns error for non-existent stream" do
      assert {:error, :not_found} = Storage.delete("nonexistent")
    end
  end

  describe "current_offset/1" do
    test "returns start offset for empty stream", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      assert {:ok, "-1"} = Storage.current_offset(id)
    end

    test "returns last offset after appends", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      {:ok, _offset1} = Storage.append(id, "First")
      {:ok, offset2} = Storage.append(id, "Second")

      assert {:ok, ^offset2} = Storage.current_offset(id)
    end

    test "returns error for non-existent stream" do
      assert {:error, :not_found} = Storage.current_offset("nonexistent")
    end
  end

  describe "subscribe/1" do
    test "subscribes to stream notifications", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      :ok = Storage.subscribe(id)

      {:ok, offset} = Storage.append(id, "Data")

      assert_receive {:stream_append, ^id, ^offset}
    end

    test "receives close notification", %{id: id, stream: stream} do
      :ok = Storage.create(id, stream)
      :ok = Storage.subscribe(id)

      :ok = Storage.close(id)

      assert_receive {:stream_closed, ^id}
    end
  end
end
