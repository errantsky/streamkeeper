defmodule DurableStreams.Retention.Worker do
  @moduledoc """
  Performs stream compaction based on retention policies.

  This module is stateless and runs as Tasks under the TaskSupervisor.
  It calculates which messages need to be removed based on the stream's
  retention policy (max_age, max_messages, max_bytes) and performs
  the actual deletion.

  ## How Compaction Works

  1. Fetch stream metadata and retention policy
  2. Calculate new earliest offset based on policy limits
  3. Delete all messages before the new earliest offset
  4. Update stream metadata with new earliest_offset and counters

  Messages are removed when any of these conditions are met:
  - `max_age`: Message timestamp is older than (now - max_age)
  - `max_messages`: More than max_messages exist in the stream
  - `max_bytes`: Total stream size exceeds max_bytes

  The most aggressive limit (resulting in highest earliest_offset) wins.
  """

  require Logger

  alias DurableStreams.Storage.ETS, as: Storage

  @doc """
  Compacts a stream by removing messages that exceed the retention policy.

  Returns:
  - `:ok` - Compaction successful (or no compaction needed)
  - `{:error, reason}` - Compaction failed
  """
  @spec compact(String.t()) :: :ok | {:error, term()}
  def compact(stream_id) do
    Logger.debug("[Retention] Starting compaction for stream: #{stream_id}")

    with {:ok, stream} <- Storage.get(stream_id),
         {:ok, new_earliest} <- calculate_new_earliest(stream),
         :ok <- perform_compaction(stream_id, stream.earliest_offset, new_earliest) do
      Logger.info("[Retention] Compacted #{stream_id}, new earliest: #{new_earliest}")
      :ok
    else
      {:error, :no_compaction_needed} ->
        Logger.debug("[Retention] No compaction needed for #{stream_id}")
        :ok

      {:error, :not_found} ->
        Logger.warning("[Retention] Stream not found for compaction: #{stream_id}")
        {:error, :not_found}

      {:error, reason} ->
        Logger.error("[Retention] Failed to compact #{stream_id}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Checks if a stream needs compaction based on its retention policy.
  """
  @spec needs_compaction?(DurableStreams.Stream.t()) :: boolean()
  def needs_compaction?(%{retention_policy: nil}), do: false

  def needs_compaction?(stream) do
    policy = stream.retention_policy

    cond do
      policy[:max_messages] && stream.message_count > policy[:max_messages] -> true
      policy[:max_bytes] && stream.total_bytes > policy[:max_bytes] -> true
      policy[:max_age] -> has_expired_messages?(stream, policy[:max_age])
      true -> false
    end
  end

  # Check if there are messages older than max_age
  defp has_expired_messages?(stream, max_age) do
    cutoff = System.system_time(:millisecond) - max_age

    case stream.earliest_offset do
      nil ->
        # No compaction yet, check if stream has old messages
        # This requires checking the first message timestamp
        case Storage.get_first_message_timestamp(stream.id) do
          {:ok, timestamp} -> timestamp < cutoff
          _ -> false
        end

      _offset ->
        # Already compacted, check first available message
        case Storage.get_first_message_timestamp(stream.id) do
          {:ok, timestamp} -> timestamp < cutoff
          _ -> false
        end
    end
  end

  # Calculate the new earliest offset based on retention policy
  defp calculate_new_earliest(stream) do
    case stream.retention_policy do
      nil ->
        {:error, :no_compaction_needed}

      policy ->
        now = System.system_time(:millisecond)

        candidates =
          [
            calculate_earliest_by_age(stream, policy[:max_age], now),
            calculate_earliest_by_count(stream, policy[:max_messages]),
            calculate_earliest_by_size(stream, policy[:max_bytes])
          ]
          |> Enum.reject(&is_nil/1)

        case candidates do
          [] ->
            {:error, :no_compaction_needed}

          offsets ->
            # Take the highest offset (most aggressive compaction)
            new_earliest = Enum.max(offsets)

            # Only compact if new earliest is different from current
            if new_earliest != stream.earliest_offset do
              {:ok, new_earliest}
            else
              {:error, :no_compaction_needed}
            end
        end
    end
  end

  defp calculate_earliest_by_age(_stream, nil, _now), do: nil

  defp calculate_earliest_by_age(stream, max_age, now) do
    cutoff = now - max_age
    Storage.find_offset_after_timestamp(stream.id, cutoff)
  end

  defp calculate_earliest_by_count(_stream, nil), do: nil

  defp calculate_earliest_by_count(stream, max_messages) do
    if stream.message_count > max_messages do
      to_remove = stream.message_count - max_messages
      Storage.find_offset_after_n_messages(stream.id, to_remove)
    end
  end

  defp calculate_earliest_by_size(_stream, nil), do: nil

  defp calculate_earliest_by_size(stream, max_bytes) do
    if stream.total_bytes > max_bytes do
      target_removal = stream.total_bytes - max_bytes
      Storage.find_offset_after_n_bytes(stream.id, target_removal)
    end
  end

  # Perform the actual compaction
  defp perform_compaction(stream_id, _old_earliest, new_earliest) do
    # Delete messages before new_earliest and get stats
    {:ok, deleted_count, deleted_bytes} = Storage.delete_messages_before(stream_id, new_earliest)
    # Update stream metadata
    Storage.update_after_compaction(stream_id, new_earliest, deleted_count, deleted_bytes)
  end
end
