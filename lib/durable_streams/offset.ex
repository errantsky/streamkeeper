defmodule DurableStreams.Offset do
  @moduledoc """
  Generates and compares opaque, lexicographically sortable offsets.

  Format: 16 hex digits from erlang:unique_integer([:monotonic, :positive])
  Example: "0000000000a1b2c3"

  Offsets are designed to be:
  - Opaque: Clients should never parse them
  - Lexicographically sortable: String comparison equals temporal order
  - Unique: Guaranteed by erlang:unique_integer
  - Clock-independent: Uses Erlang's monotonic integer, not wall clock
  """

  @type t :: String.t()

  @start "-1"
  @zero "0000000000000000"

  @doc """
  Returns the start offset, which represents the beginning of a stream.
  """
  @spec start() :: t()
  def start, do: @start

  @doc """
  Returns the zero offset - the earliest possible real offset.
  Used for SSE when the stream is empty and we need a non-sentinel value.
  """
  @spec zero() :: t()
  def zero, do: @zero

  @doc """
  Returns true if the given offset is the start offset.
  """
  @spec start?(t()) :: boolean()
  def start?(offset), do: offset == @start

  @doc """
  Generates a new unique, lexicographically sortable offset.

  Uses erlang:unique_integer([:monotonic, :positive]) for clock-independent
  monotonic ordering. The integer is formatted as a 16-character zero-padded
  hexadecimal string for lexicographic sortability.
  """
  @spec generate() :: t()
  def generate do
    int = :erlang.unique_integer([:monotonic, :positive])
    :io_lib.format("~16.16.0b", [int]) |> IO.iodata_to_binary()
  end

  @doc """
  Converts an offset string to its integer representation for use as ETS key.

  Returns nil for the start offset (-1) or invalid offsets.
  """
  @spec to_integer(t()) :: non_neg_integer() | nil
  def to_integer(@start), do: nil

  def to_integer(offset) when is_binary(offset) do
    case Integer.parse(offset, 16) do
      {int, ""} -> int
      _ -> nil
    end
  end

  def to_integer(_), do: nil

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
