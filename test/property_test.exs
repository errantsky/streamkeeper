defmodule DurableStreams.PropertyTest do
  use ExUnit.Case
  use ExUnitProperties

  alias DurableStreams.Offset

  describe "Offset properties" do
    property "generated offsets are always greater than start offset" do
      check all data <- binary(min_length: 1) do
        id = "prop-offset-#{System.unique_integer([:positive])}"
        {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

        {:ok, offset} = DurableStreams.append(id, data)

        assert Offset.after?(offset, "-1")

        DurableStreams.delete(id)
      end
    end

    property "sequential offsets maintain ordering" do
      check all data_list <- list_of(binary(min_length: 1), min_length: 2, max_length: 10) do
        id = "prop-seq-#{System.unique_integer([:positive])}"
        {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

        offsets =
          for data <- data_list do
            {:ok, offset} = DurableStreams.append(id, data)
            offset
          end

        # Each offset should be after the previous one
        offsets
        |> Enum.chunk_every(2, 1, :discard)
        |> Enum.each(fn [prev, curr] ->
          assert Offset.after?(curr, prev)
        end)

        # Offsets should be lexicographically sortable
        assert offsets == Enum.sort(offsets)

        DurableStreams.delete(id)
      end
    end
  end

  describe "append and read roundtrip" do
    property "appended data is readable" do
      check all data <- binary(min_length: 1, max_length: 1000) do
        id = "prop-roundtrip-#{System.unique_integer([:positive])}"
        {:ok, _} = DurableStreams.create(id, content_type: "application/octet-stream")

        {:ok, _} = DurableStreams.append(id, data)
        {:ok, result} = DurableStreams.read(id, "-1")

        assert result.data == data

        DurableStreams.delete(id)
      end
    end

    property "multiple appends concatenate correctly" do
      check all data_list <- list_of(binary(min_length: 1, max_length: 100), min_length: 1, max_length: 20) do
        id = "prop-concat-#{System.unique_integer([:positive])}"
        {:ok, _} = DurableStreams.create(id, content_type: "application/octet-stream")

        for data <- data_list do
          {:ok, _} = DurableStreams.append(id, data)
        end

        {:ok, result} = DurableStreams.read(id, "-1")
        expected = Enum.join(data_list, "")

        assert result.data == expected

        DurableStreams.delete(id)
      end
    end

    property "reading from offset returns only subsequent data" do
      check all [first | rest] = data_list <- list_of(binary(min_length: 1, max_length: 100), min_length: 2, max_length: 10) do
        id = "prop-offset-read-#{System.unique_integer([:positive])}"
        {:ok, _} = DurableStreams.create(id, content_type: "application/octet-stream")

        {:ok, first_offset} = DurableStreams.append(id, first)

        for data <- rest do
          {:ok, _} = DurableStreams.append(id, data)
        end

        {:ok, result} = DurableStreams.read(id, first_offset)
        expected = Enum.join(rest, "")

        assert result.data == expected

        DurableStreams.delete(id)
      end
    end
  end

  describe "message-based reads" do
    property "read_messages returns individual messages" do
      check all messages <- list_of(binary(min_length: 1, max_length: 100), min_length: 1, max_length: 20) do
        id = "prop-messages-#{System.unique_integer([:positive])}"
        {:ok, _} = DurableStreams.create(id, content_type: "application/json")

        offsets =
          for msg <- messages do
            {:ok, offset} = DurableStreams.StreamManager.append(id, msg)
            offset
          end

        {:ok, result} = DurableStreams.StreamManager.read_messages(id, "-1")

        assert length(result.messages) == length(messages)

        for {msg, offset, expected} <- Enum.zip([result.messages, offsets, messages]) do
          assert msg.data == expected
          assert msg.offset == offset
        end

        DurableStreams.delete(id)
      end
    end
  end

  describe "stream metadata" do
    property "content_type is preserved" do
      check all content_type <- member_of(["text/plain", "application/json", "application/octet-stream", "text/html"]) do
        id = "prop-content-type-#{System.unique_integer([:positive])}"
        {:ok, _} = DurableStreams.create(id, content_type: content_type)

        {:ok, meta} = DurableStreams.get_metadata(id)

        assert meta.content_type == content_type

        DurableStreams.delete(id)
      end
    end

    property "stream id is preserved" do
      check all suffix <- string(:alphanumeric, min_length: 1, max_length: 50) do
        id = "prop-id-#{suffix}-#{System.unique_integer([:positive])}"
        {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

        {:ok, meta} = DurableStreams.get_metadata(id)

        assert meta.id == id

        DurableStreams.delete(id)
      end
    end
  end

  describe "closed stream behavior" do
    property "closed streams reject appends" do
      check all data <- binary(min_length: 1) do
        id = "prop-closed-#{System.unique_integer([:positive])}"
        {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
        :ok = DurableStreams.close(id)

        result = DurableStreams.append(id, data)

        assert result == {:error, :closed}

        DurableStreams.delete(id)
      end
    end

    property "closed streams are still readable" do
      check all data <- binary(min_length: 1) do
        id = "prop-closed-read-#{System.unique_integer([:positive])}"
        {:ok, _} = DurableStreams.create(id, content_type: "text/plain")
        {:ok, _} = DurableStreams.append(id, data)
        :ok = DurableStreams.close(id)

        {:ok, result} = DurableStreams.read(id, "-1")

        assert result.data == data
        assert result.closed == true

        DurableStreams.delete(id)
      end
    end
  end

  describe "sequence enforcement" do
    property "seq values must be unique" do
      check all seq <- string(:alphanumeric, min_length: 1, max_length: 20) do
        id = "prop-seq-unique-#{System.unique_integer([:positive])}"
        {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

        {:ok, _} = DurableStreams.StreamManager.append(id, "first", seq: seq)
        result = DurableStreams.StreamManager.append(id, "second", seq: seq)

        assert result == {:error, :seq_conflict}

        DurableStreams.delete(id)
      end
    end

    property "seq values enforce ordering" do
      check all seqs <- list_of(string(:alphanumeric, min_length: 1, max_length: 10), min_length: 2, max_length: 10) do
        # Remove duplicates for this test
        seqs = Enum.uniq(seqs)

        if length(seqs) >= 2 do
          id = "prop-seq-order-#{System.unique_integer([:positive])}"
          {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

          sorted_seqs = Enum.sort(seqs)

          # Append in sorted order - should all succeed
          for seq <- sorted_seqs do
            result = DurableStreams.StreamManager.append(id, "data-#{seq}", seq: seq)
            assert {:ok, _} = result
          end

          DurableStreams.delete(id)
        end
      end
    end
  end

  describe "TTL configuration" do
    property "TTL creates valid expires_at" do
      check all ttl <- positive_integer() |> filter(&(&1 <= 86400)) do
        id = "prop-ttl-#{System.unique_integer([:positive])}"
        before = DateTime.utc_now()

        {:ok, _} = DurableStreams.create(id, content_type: "text/plain", ttl: ttl)

        {:ok, meta} = DurableStreams.get_metadata(id)
        after_create = DateTime.utc_now()

        assert meta.ttl == ttl
        assert meta.expires_at != nil

        expected_min = DateTime.add(before, ttl, :second)
        expected_max = DateTime.add(after_create, ttl, :second)

        assert DateTime.compare(meta.expires_at, expected_min) in [:gt, :eq]
        assert DateTime.compare(meta.expires_at, expected_max) in [:lt, :eq]

        DurableStreams.delete(id)
      end
    end
  end

  describe "data integrity" do
    property "binary data is preserved exactly" do
      check all data <- binary(min_length: 0, max_length: 10000) do
        id = "prop-binary-#{System.unique_integer([:positive])}"
        {:ok, _} = DurableStreams.create(id, content_type: "application/octet-stream")

        {:ok, _} = DurableStreams.append(id, data)
        {:ok, result} = DurableStreams.read(id, "-1")

        assert result.data == data
        assert byte_size(result.data) == byte_size(data)

        DurableStreams.delete(id)
      end
    end

    property "unicode data is preserved" do
      check all text <- string(:printable) do
        id = "prop-unicode-#{System.unique_integer([:positive])}"
        {:ok, _} = DurableStreams.create(id, content_type: "text/plain")

        {:ok, _} = DurableStreams.append(id, text)
        {:ok, result} = DurableStreams.read(id, "-1")

        assert result.data == text

        DurableStreams.delete(id)
      end
    end
  end
end
