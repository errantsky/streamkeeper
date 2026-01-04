defmodule DurableStreams.Protocol.Handlers.SSE do
  @moduledoc """
  Handler for GET requests with live=sse for Server-Sent Events.

  Streams data to the client as SSE events, with control events
  containing cursor and upToDate flags.
  """

  import Plug.Conn
  alias DurableStreams.{StreamManager, Stream}

  def call(conn) do
    stream_id = conn.path_params["stream_id"]
    offset = conn.params["offset"]

    # SSE requires offset parameter
    if is_nil(offset) or offset == "" do
      conn
      |> put_resp_header("content-type", "application/json")
      |> send_resp(400, Jason.encode!(%{error: "Offset parameter required for SSE"}))
    else
      case StreamManager.get_metadata(stream_id) do
        {:ok, meta} ->
          conn
          |> put_resp_header("content-type", "text/event-stream")
          |> put_resp_header("cache-control", "no-cache, no-store, must-revalidate")
          |> put_resp_header("connection", "keep-alive")
          |> delete_resp_header("content-length")
          |> send_chunked(200)
          |> stream_loop(stream_id, offset, meta)

        {:error, :not_found} ->
          conn
          |> put_resp_header("content-type", "application/json")
          |> send_resp(404, Jason.encode!(%{error: "Stream not found"}))
      end
    end
  end

  defp stream_loop(conn, stream_id, offset, meta) do
    if Stream.json_mode?(meta) do
      stream_json_loop(conn, stream_id, offset)
    else
      stream_binary_loop(conn, stream_id, offset, meta)
    end
  end

  defp stream_binary_loop(conn, stream_id, offset, meta) do
    case StreamManager.read(stream_id, offset, live: true, timeout: 5_000) do
      {:ok, %{data: <<>>, closed: true} = result} ->
        send_control_event(conn, result.offset, true, true, stream_id)
        conn

      {:ok, %{data: <<>>} = result} ->
        # No new data, send control event with upToDate
        # Use the result offset (which is the current tail) not the requested offset
        case send_control_event(conn, result.offset, true, false, stream_id) do
          {:ok, conn} -> stream_binary_loop(conn, stream_id, result.offset, meta)
          {:error, _} -> conn
        end

      {:ok, result} ->
        # Send data event
        data = format_data(result.data, meta)
        event = build_data_event(data, result.offset)

        case chunk(conn, event) do
          {:ok, conn} ->
            # Send control event after data
            case send_control_event(conn, result.offset, !result.has_more, result.closed, stream_id) do
              {:ok, conn} when result.closed -> conn
              {:ok, conn} -> stream_binary_loop(conn, stream_id, result.offset, meta)
              {:error, _} -> conn
            end
          {:error, _} -> conn
        end

      {:error, _} ->
        conn
    end
  end

  defp stream_json_loop(conn, stream_id, offset) do
    case StreamManager.read_messages(stream_id, offset, live: true, timeout: 5_000) do
      {:ok, %{messages: [], closed: true} = result} ->
        send_control_event(conn, result.offset, true, true, stream_id)
        conn

      {:ok, %{messages: []} = result} ->
        # No new data, send control event with upToDate
        # Use the result offset (which is the current tail) not the requested offset
        case send_control_event(conn, result.offset, true, false, stream_id) do
          {:ok, conn} -> stream_json_loop(conn, stream_id, result.offset)
          {:error, _} -> conn
        end

      {:ok, result} ->
        # Send each message as a separate event
        last_offset =
          Enum.reduce_while(result.messages, offset, fn msg, _acc ->
            # Parse JSON and wrap in array
            json_data = case Jason.decode(msg.data) do
              {:ok, parsed} -> Jason.encode!([parsed])
              {:error, _} -> Jason.encode!([msg.data])
            end

            event = build_data_event(json_data, msg.offset)
            case chunk(conn, event) do
              {:ok, _} -> {:cont, msg.offset}
              {:error, _} -> {:halt, msg.offset}
            end
          end)

        # Send control event after all data
        case send_control_event(conn, last_offset, !result.has_more, result.closed, stream_id) do
          {:ok, conn} when result.closed -> conn
          {:ok, conn} -> stream_json_loop(conn, stream_id, last_offset)
          {:error, _} -> conn
        end

      {:error, _} ->
        conn
    end
  end

  defp build_data_event(data, offset) do
    # Handle newlines in data by prefixing each line with "data: "
    lines = data
    |> String.split("\n")
    |> Enum.map(&"data: #{&1}")
    |> Enum.join("\n")

    "event: data\n#{lines}\nid: #{offset}\n\n"
  end

  defp send_control_event(conn, offset, up_to_date, closed, stream_id) do
    cursor = generate_cursor(stream_id)
    control = %{
      "streamCursor" => cursor,
      "streamNextOffset" => offset,
      "upToDate" => up_to_date
    }
    control = if closed, do: Map.put(control, "closed", true), else: control

    event = "event: control\ndata: #{Jason.encode!(control)}\n\n"
    chunk(conn, event)
  end

  defp format_data(data, meta) when is_binary(data) do
    # For text/plain, just use the data as-is
    # For other types, the client should decode appropriately
    if String.starts_with?(meta.content_type, "text/") do
      data
    else
      Base.encode64(data)
    end
  end

  defp generate_cursor(stream_id) do
    timestamp = System.system_time(:millisecond)
    random = :rand.uniform(0xFFFF)
    Base.encode64("#{stream_id}:#{timestamp}:#{random}")
  end
end
