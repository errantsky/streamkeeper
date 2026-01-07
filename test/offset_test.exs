defmodule DurableStreams.OffsetTest do
  use ExUnit.Case, async: true
  alias DurableStreams.Offset

  describe "generate/0" do
    test "generates unique offsets" do
      offsets = for _ <- 1..1000, do: Offset.generate()
      assert length(Enum.uniq(offsets)) == 1000
    end

    test "offsets are lexicographically sortable" do
      offsets = for _ <- 1..100, do: Offset.generate()
      assert offsets == Enum.sort(offsets)
    end

    test "generates offsets with correct format" do
      offset = Offset.generate()
      # Format: 16 hex chars
      assert Regex.match?(~r/^[0-9a-f]{16}$/, offset)
    end
  end

  describe "to_integer/1" do
    test "converts valid offset to integer" do
      offset = Offset.generate()
      int = Offset.to_integer(offset)
      assert is_integer(int)
      assert int > 0
    end

    test "returns nil for start offset" do
      assert Offset.to_integer("-1") == nil
    end

    test "returns nil for invalid offset" do
      assert Offset.to_integer("invalid") == nil
      assert Offset.to_integer(nil) == nil
    end

    test "round-trips correctly" do
      offset = Offset.generate()
      int = Offset.to_integer(offset)
      # Convert back to string and compare
      reconstructed = :io_lib.format("~16.16.0b", [int]) |> IO.iodata_to_binary()
      assert offset == reconstructed
    end
  end

  describe "start/0" do
    test "returns the start offset" do
      assert Offset.start() == "-1"
    end
  end

  describe "start?/1" do
    test "returns true for start offset" do
      assert Offset.start?("-1") == true
    end

    test "returns false for generated offsets" do
      assert Offset.start?(Offset.generate()) == false
    end
  end

  describe "compare/2" do
    test "start offset compares less than generated" do
      assert Offset.compare("-1", Offset.generate()) == :lt
    end

    test "generated offset compares greater than start" do
      assert Offset.compare(Offset.generate(), "-1") == :gt
    end

    test "two start offsets are equal" do
      assert Offset.compare("-1", "-1") == :eq
    end

    test "same offsets are equal" do
      offset = Offset.generate()
      assert Offset.compare(offset, offset) == :eq
    end

    test "earlier offsets compare less than later offsets" do
      offset1 = Offset.generate()
      offset2 = Offset.generate()
      assert Offset.compare(offset1, offset2) == :lt
    end
  end

  describe "after?/2" do
    test "generated offset is after start" do
      assert Offset.after?(Offset.generate(), "-1") == true
    end

    test "start is not after generated" do
      assert Offset.after?("-1", Offset.generate()) == false
    end

    test "later offset is after earlier offset" do
      offset1 = Offset.generate()
      offset2 = Offset.generate()
      assert Offset.after?(offset2, offset1) == true
      assert Offset.after?(offset1, offset2) == false
    end
  end

  describe "zero/0" do
    test "returns zero offset" do
      assert Offset.zero() == "0000000000000000"
    end

    test "zero is greater than start" do
      assert Offset.compare(Offset.zero(), Offset.start()) == :gt
    end
  end
end
