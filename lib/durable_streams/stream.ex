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
          ttl: non_neg_integer() | nil,
          expires_at: DateTime.t() | nil
        }

  @enforce_keys [:id, :content_type, :created_at]
  defstruct [
    :id,
    :content_type,
    :created_at,
    :ttl,
    :expires_at,
    closed: false
  ]

  @doc """
  Creates a new stream with the given ID and options.

  ## Options

  - `:content_type` - The content type of the stream (default: "application/octet-stream")
  - `:ttl` - Time-to-live in seconds (default: nil, meaning no expiration)
  - `:expires_at` - Absolute expiration DateTime (default: nil)
  """
  @spec new(String.t(), keyword()) :: t()
  def new(id, opts \\ []) do
    now = DateTime.utc_now()
    ttl = Keyword.get(opts, :ttl)
    expires_at = Keyword.get(opts, :expires_at) || compute_expires_at(ttl, now)

    %__MODULE__{
      id: id,
      content_type: Keyword.get(opts, :content_type, "application/octet-stream"),
      created_at: now,
      ttl: ttl,
      expires_at: expires_at,
      closed: false
    }
  end

  defp compute_expires_at(nil, _now), do: nil
  defp compute_expires_at(ttl, now) when is_integer(ttl) do
    DateTime.add(now, ttl, :second)
  end

  @doc """
  Returns true if the stream is in JSON mode (content-type is application/json).

  In JSON mode:
  - Each POST stores messages as distinct units
  - GET returns JSON array of all messages in range
  - Array POSTs are flattened one level
  """
  @spec json_mode?(t()) :: boolean()
  def json_mode?(%__MODULE__{content_type: content_type}) do
    # Handle charset parameter (e.g., "application/json; charset=utf-8")
    base_type = content_type |> String.split(";") |> List.first() |> String.trim() |> String.downcase()
    base_type == "application/json"
  end
  def json_mode?(_), do: false

  @doc """
  Normalizes a content-type for comparison (strips charset, lowercases).
  """
  @spec normalize_content_type(String.t()) :: String.t()
  def normalize_content_type(content_type) do
    content_type
    |> String.split(";")
    |> List.first()
    |> String.trim()
    |> String.downcase()
  end

  @doc """
  Returns true if two content-types are equivalent (same base type, ignoring charset and case).
  """
  @spec content_type_matches?(String.t(), String.t()) :: boolean()
  def content_type_matches?(type1, type2) do
    normalize_content_type(type1) == normalize_content_type(type2)
  end
end
