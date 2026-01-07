defmodule DurableStreams.Stream do
  @moduledoc """
  Represents a durable stream's metadata and state.

  A stream is a URL-addressable, append-only byte log with:
  - A unique ID
  - A content type (defaults to application/octet-stream)
  - Creation timestamp
  - Optional TTL (time-to-live in seconds)
  - Closed state (once closed, no more appends allowed)
  - Optional retention policy for automatic compaction

  ## Retention Policy

  Streams can have a retention policy that automatically removes old messages:

      DurableStreams.create("my-stream",
        retention: [
          max_age: :timer.hours(24),     # Remove messages older than 24h
          max_messages: 100_000,          # Keep at most 100k messages
          max_bytes: 50 * 1024 * 1024    # Keep at most 50MB
        ]
      )

  Whichever limit is exceeded first triggers compaction. When messages are
  removed, reads for those offsets return `410 Gone`.
  """

  @type retention_policy :: %{
          optional(:max_age) => non_neg_integer(),
          optional(:max_messages) => non_neg_integer(),
          optional(:max_bytes) => non_neg_integer()
        }

  @type t :: %__MODULE__{
          id: String.t(),
          content_type: String.t(),
          created_at: DateTime.t(),
          closed: boolean(),
          ttl: non_neg_integer() | nil,
          expires_at: DateTime.t() | nil,
          earliest_offset: String.t() | nil,
          current_offset: String.t() | nil,
          message_count: non_neg_integer(),
          total_bytes: non_neg_integer(),
          retention_policy: retention_policy() | nil
        }

  @enforce_keys [:id, :content_type, :created_at]
  defstruct [
    :id,
    :content_type,
    :created_at,
    :ttl,
    :expires_at,
    :earliest_offset,
    :current_offset,
    :retention_policy,
    closed: false,
    message_count: 0,
    total_bytes: 0
  ]

  @doc """
  Creates a new stream with the given ID and options.

  ## Options

  - `:content_type` - The content type of the stream (default: "application/octet-stream")
  - `:ttl` - Time-to-live in seconds (default: nil, meaning no expiration)
  - `:expires_at` - Absolute expiration DateTime (default: nil)
  - `:retention` - Retention policy keyword list with `:max_age`, `:max_messages`, `:max_bytes`
  """
  @spec new(String.t(), keyword()) :: t()
  def new(id, opts \\ []) do
    now = DateTime.utc_now()
    ttl = Keyword.get(opts, :ttl)
    expires_at = Keyword.get(opts, :expires_at) || compute_expires_at(ttl, now)
    retention = parse_retention_policy(Keyword.get(opts, :retention))

    %__MODULE__{
      id: id,
      content_type: Keyword.get(opts, :content_type, "application/octet-stream"),
      created_at: now,
      ttl: ttl,
      expires_at: expires_at,
      retention_policy: retention,
      earliest_offset: nil,
      current_offset: nil,
      message_count: 0,
      total_bytes: 0,
      closed: false
    }
  end

  defp parse_retention_policy(nil), do: nil
  defp parse_retention_policy([]), do: nil

  defp parse_retention_policy(opts) when is_list(opts) do
    policy = %{}
    policy = if opts[:max_age], do: Map.put(policy, :max_age, opts[:max_age]), else: policy

    policy =
      if opts[:max_messages],
        do: Map.put(policy, :max_messages, opts[:max_messages]),
        else: policy

    policy = if opts[:max_bytes], do: Map.put(policy, :max_bytes, opts[:max_bytes]), else: policy
    if map_size(policy) == 0, do: nil, else: policy
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
    base_type =
      content_type |> String.split(";") |> List.first() |> String.trim() |> String.downcase()

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
