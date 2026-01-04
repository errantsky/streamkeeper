defmodule DurableStreamsTest do
  use ExUnit.Case

  setup do
    id = "test-#{System.unique_integer([:positive])}"
    on_exit(fn -> DurableStreams.delete(id) end)
    %{id: id}
  end

  describe "create/2" do
    test "creates a new stream", %{id: id} do
      assert {:ok, ^id} = DurableStreams.create(id)
    end

    test "returns error if stream already exists", %{id: id} do
      {:ok, ^id} = DurableStreams.create(id)
      assert {:error, :already_exists} = DurableStreams.create(id)
    end

    test "creates stream with custom content type", %{id: id} do
      {:ok, ^id} = DurableStreams.create(id, content_type: "application/json")
      {:ok, meta} = DurableStreams.get_metadata(id)
      assert meta.content_type == "application/json"
    end
  end

  describe "append/2" do
    test "appends data and returns offset", %{id: id} do
      {:ok, ^id} = DurableStreams.create(id)
      {:ok, offset} = DurableStreams.append(id, "Hello!")
      assert is_binary(offset)
      assert offset != "-1"
    end

    test "returns error if stream not found" do
      assert {:error, :not_found} = DurableStreams.append("nonexistent", "data")
    end
  end

  describe "read/3" do
    test "reads all data from beginning", %{id: id} do
      {:ok, ^id} = DurableStreams.create(id, content_type: "text/plain")
      {:ok, _} = DurableStreams.append(id, "Hello")
      {:ok, _} = DurableStreams.append(id, " World")

      {:ok, result} = DurableStreams.read(id, "-1")
      assert result.data == "Hello World"
      assert result.closed == false
    end

    test "reads data after offset", %{id: id} do
      {:ok, ^id} = DurableStreams.create(id)
      {:ok, offset1} = DurableStreams.append(id, "First")
      {:ok, _offset2} = DurableStreams.append(id, "Second")

      {:ok, result} = DurableStreams.read(id, offset1)
      assert result.data == "Second"
    end

    test "returns empty data when no new data", %{id: id} do
      {:ok, ^id} = DurableStreams.create(id)
      {:ok, offset} = DurableStreams.append(id, "Data")

      {:ok, result} = DurableStreams.read(id, offset)
      assert result.data == <<>>
    end

    test "returns error if stream not found" do
      assert {:error, :not_found} = DurableStreams.read("nonexistent", "-1")
    end
  end

  describe "close/1" do
    test "closes a stream", %{id: id} do
      {:ok, ^id} = DurableStreams.create(id)
      assert :ok = DurableStreams.close(id)

      {:ok, meta} = DurableStreams.get_metadata(id)
      assert meta.closed == true
    end

    test "prevents further appends after closing", %{id: id} do
      {:ok, ^id} = DurableStreams.create(id)
      :ok = DurableStreams.close(id)

      assert {:error, :closed} = DurableStreams.append(id, "data")
    end

    test "reading still works after closing", %{id: id} do
      {:ok, ^id} = DurableStreams.create(id)
      {:ok, _} = DurableStreams.append(id, "Data")
      :ok = DurableStreams.close(id)

      {:ok, result} = DurableStreams.read(id, "-1")
      assert result.data == "Data"
      assert result.closed == true
    end
  end

  describe "delete/1" do
    test "deletes a stream", %{id: id} do
      {:ok, ^id} = DurableStreams.create(id)
      assert :ok = DurableStreams.delete(id)
      assert {:error, :not_found} = DurableStreams.get_metadata(id)
    end

    test "returns error if stream not found" do
      assert {:error, :not_found} = DurableStreams.delete("nonexistent")
    end
  end

  describe "get_metadata/1" do
    test "returns stream metadata", %{id: id} do
      {:ok, ^id} = DurableStreams.create(id, content_type: "text/plain")
      {:ok, meta} = DurableStreams.get_metadata(id)

      assert meta.id == id
      assert meta.content_type == "text/plain"
      assert meta.closed == false
      assert %DateTime{} = meta.created_at
    end

    test "returns error if stream not found" do
      assert {:error, :not_found} = DurableStreams.get_metadata("nonexistent")
    end
  end
end
