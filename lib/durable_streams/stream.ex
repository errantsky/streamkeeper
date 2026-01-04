defmodule DurableStreams.Stream do
  @moduledoc """
  Represents a durable stream's metadata and state.

  A stream is a URL-addressable, append-only byte log with:
  - A unique ID
  - A content type (defaults to application/octet-stream)
  - Creation timestamp
  - Optional TTL (time-to-live in seconds)
  - Closed state (once closed, no more appends allowed)
  """

  @type t :: %__MODULE__{
          id: String.t(),
          content_type: String.t(),
          created_at: DateTime.t(),
          closed: boolean(),
          ttl: non_neg_integer() | nil
        }

  @enforce_keys [:id, :content_type, :created_at]
  defstruct [
    :id,
    :content_type,
    :created_at,
    :ttl,
    closed: false
  ]

  @doc """
  Creates a new stream with the given ID and options.

  ## Options

  - `:content_type` - The content type of the stream (default: "application/octet-stream")
  - `:ttl` - Time-to-live in seconds (default: nil, meaning no expiration)
  """
  @spec new(String.t(), keyword()) :: t()
  def new(id, opts \\ []) do
    %__MODULE__{
      id: id,
      content_type: Keyword.get(opts, :content_type, "application/octet-stream"),
      created_at: DateTime.utc_now(),
      ttl: Keyword.get(opts, :ttl),
      closed: false
    }
  end

  @doc """
  Returns true if the stream is in JSON mode (content-type is application/json).

  In JSON mode:
  - Each POST stores messages as distinct units
  - GET returns JSON array of all messages in range
  - Array POSTs are flattened one level
  """
  @spec json_mode?(t()) :: boolean()
  def json_mode?(%__MODULE__{content_type: "application/json"}), do: true
  def json_mode?(_), do: false
end
