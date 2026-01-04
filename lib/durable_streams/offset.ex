defmodule DurableStreams.Offset do
  @moduledoc """
  Generates and compares opaque, lexicographically sortable offsets.

  Format: {timestamp_us}-{sequence}-{random}
  Example: "0001929a3b4c5d6e-0001-a1b2"

  Offsets are designed to be:
  - Opaque: Clients should never parse them
  - Lexicographically sortable: String comparison equals temporal order
  - Unique: Combines timestamp, sequence, and randomness
  """

  import Bitwise

  @type t :: String.t()

  @start "-1"

  @doc """
  Returns the start offset, which represents the beginning of a stream.
  """
  @spec start() :: t()
  def start, do: @start

  @doc """
  Returns true if the given offset is the start offset.
  """
  @spec start?(t()) :: boolean()
  def start?(offset), do: offset == @start

  @doc """
  Generates a new unique, lexicographically sortable offset.

  The offset format is: `{timestamp_us}-{sequence}-{random}`
  where each component is zero-padded hex.
  """
  @spec generate(non_neg_integer()) :: t()
  def generate(sequence \\ 0) do
    timestamp = System.system_time(:microsecond)
    random = :rand.uniform(0xFFFF)

    :io_lib.format("~16.16.0b-~4.16.0b-~4.16.0b", [timestamp, sequence &&& 0xFFFF, random])
    |> IO.iodata_to_binary()
  end

  @doc """
  Compares two offsets and returns :lt, :eq, or :gt.

  The start offset (-1) is always less than any generated offset.
  """
  @spec compare(t(), t()) :: :lt | :eq | :gt
  def compare(@start, @start), do: :eq
  def compare(@start, _), do: :lt
  def compare(_, @start), do: :gt
  def compare(a, b) when a < b, do: :lt
  def compare(a, b) when a > b, do: :gt
  def compare(_, _), do: :eq

  @doc """
  Returns true if `offset` is after `reference`.
  """
  @spec after?(t(), t()) :: boolean()
  def after?(offset, reference), do: compare(offset, reference) == :gt
end
