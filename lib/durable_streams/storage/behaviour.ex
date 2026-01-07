defmodule DurableStreams.Storage.Behaviour do
  @moduledoc """
  Behaviour defining the storage interface for durable streams.

  This behaviour allows for different storage backends:
  - ETS-based (single node)
  - Distributed (multi-node with :pg)
  - Custom implementations
  """

  alias DurableStreams.{Offset, Stream}

  @type stream_id :: String.t()
  @type offset :: Offset.t()

  @type read_result :: %{
          data: binary(),
          offset: offset(),
          has_more: boolean(),
          closed: boolean()
        }

  @doc """
  Creates a new stream with the given metadata.
  Returns :ok on success, or {:error, :already_exists} if the stream already exists.
  """
  @callback create(stream_id, Stream.t()) :: :ok | {:error, :already_exists}

  @doc """
  Retrieves the metadata for a stream.
  Returns {:ok, stream} on success, or {:error, :not_found} if the stream doesn't exist.
  """
  @callback get_metadata(stream_id) :: {:ok, Stream.t()} | {:error, :not_found}

  @doc """
  Appends data to a stream with optional sequence enforcement.
  The seq parameter, if provided, must be lexicographically greater than the previous seq.
  Returns {:ok, offset} on success, or an error if the stream doesn't exist, is closed,
  or the sequence is out of order.
  """
  @callback append(stream_id, binary(), String.t() | nil) ::
              {:ok, offset} | {:error, :not_found | :closed | :seq_conflict}

  @doc """
  Reads data from a stream starting after the given offset.
  Returns {:ok, read_result} on success, or {:error, :not_found} if the stream doesn't exist.
  """
  @callback read(stream_id, offset) :: {:ok, read_result} | {:error, :not_found}

  @doc """
  Closes a stream, preventing further appends.
  Returns :ok on success, or {:error, :not_found} if the stream doesn't exist.
  """
  @callback close(stream_id) :: :ok | {:error, :not_found}

  @doc """
  Deletes a stream and all its data.
  Returns :ok on success, or {:error, :not_found} if the stream doesn't exist.
  """
  @callback delete(stream_id) :: :ok | {:error, :not_found}

  @doc """
  Gets the current (latest) offset for a stream.
  Returns {:ok, offset} on success, or {:error, :not_found} if the stream doesn't exist.
  """
  @callback current_offset(stream_id) :: {:ok, offset} | {:error, :not_found}

  @doc """
  Subscribes the calling process to notifications for a stream.
  Returns :ok on success.
  """
  @callback subscribe(stream_id) :: :ok

  @type message :: %{data: binary(), offset: offset()}
  @type read_messages_result :: %{
          messages: [message()],
          offset: offset(),
          has_more: boolean(),
          closed: boolean()
        }

  @doc """
  Reads messages from a stream as a list (for JSON mode).
  Each message is returned separately instead of concatenated.
  """
  @callback read_messages(stream_id, offset) :: {:ok, read_messages_result} | {:error, :not_found}
end
