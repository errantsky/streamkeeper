defmodule DurableStreams.Protocol.Handlers.Read do
  @moduledoc """
  Internal handler for GET requests to read from a stream.

  Supports:
  - Regular reads with offset parameter
  - Long-polling with `live=true` parameter
  - JSON mode for `application/json` streams

  This is an internal module used by `DurableStreams.Protocol.Plug`.
  """

  import Plug.Conn
  alias DurableStreams.{JSON, StreamManager, Offset, Stream}

  def call(conn) do
    stream_id = conn.path_params["stream_id"]
    offset_param = conn.params["offset"]
    live = conn.params["live"] in ["true", "long-poll", "1"]
    timeout = parse_timeout(conn.params["timeout"])

    # Validate offset parameter
    case validate_offset(offset_param, conn, live) do
      {:ok, offset} ->
        case StreamManager.get_metadata(stream_id) do
          {:ok, meta} ->
            # Check if offset has been compacted (410 Gone)
            if offset_compacted?(offset, meta.earliest_offset) do
              send_gone(conn, meta.earliest_offset)
            else
              if Stream.json_mode?(meta) do
                handle_json_read(conn, stream_id, offset, live, timeout, meta)
              else
                handle_binary_read(conn, stream_id, offset, live, timeout, meta)
              end
            end

          {:error, :not_found} ->
            send_error(conn, 404, "Stream not found")
        end

      {:error, message} ->
        send_error(conn, 400, message)
    end
  end

  # Check if the requested offset has been compacted
  defp offset_compacted?(_offset, nil), do: false
  defp offset_compacted?(offset, _earliest) when offset == "-1", do: false

  defp offset_compacted?(offset, earliest_offset) do
    # If offset is before earliest_offset, it's been compacted
    Offset.compare(offset, earliest_offset) == :lt
  end

  defp send_gone(conn, earliest_offset) do
    conn
    |> put_resp_header("content-type", "application/json")
    |> put_resp_header("stream-earliest-offset", earliest_offset || Offset.zero())
    |> send_resp(410, JSON.encode!(%{error: "Offset no longer available", earliest_offset: earliest_offset}))
  end

  defp validate_offset(nil, _conn, live) do
    # Long-poll requires offset parameter
    if live do
      {:error, "Offset parameter required for long-poll"}
    else
      {:ok, Offset.start()}
    end
  end

  defp validate_offset("", _conn, _live), do: {:error, "Offset parameter cannot be empty"}

  defp validate_offset(offset, conn, _live) do
    # Check for multiple offset parameters by counting in the raw query string
    query_string = conn.query_string || ""
    offset_count = query_string
      |> String.split("&")
      |> Enum.count(fn part -> String.starts_with?(part, "offset=") end)
    multiple_offsets = offset_count > 1

    cond do
      multiple_offsets ->
        {:error, "Multiple offset parameters not allowed"}

      String.contains?(offset, ",") ->
        {:error, "Invalid offset: contains comma"}

      String.contains?(offset, " ") ->
        {:error, "Invalid offset: contains spaces"}

      String.contains?(offset, "\n") or String.contains?(offset, "\r") ->
        {:error, "Invalid offset: contains newlines"}

      String.contains?(offset, "\t") ->
        {:error, "Invalid offset: contains tab"}

      # Reject other invalid characters - only alphanumeric, dash, underscore, dot allowed
      not Regex.match?(~r/^[\w\-\.]+$/, offset) and offset != "-1" ->
        {:error, "Invalid offset format"}

      true ->
        {:ok, offset}
    end
  end

  defp handle_binary_read(conn, stream_id, offset, live, timeout, meta) do
    case StreamManager.read(stream_id, offset, live: live, timeout: timeout) do
      {:ok, %{data: <<>>} = result} ->
        # For empty streams, use zero offset instead of -1
        actual_offset = if Offset.start?(result.offset), do: Offset.zero(), else: result.offset
        # Per protocol: long-poll timeout with no data returns 204 No Content
        status = if live, do: 204, else: 200
        conn
        |> put_resp_header("stream-next-offset", actual_offset)
        |> put_resp_header("stream-up-to-date", "true")
        |> maybe_put_cursor_header(live, stream_id)
        |> maybe_put_closed_header(result.closed)
        |> put_resp_header("content-type", meta.content_type)
        |> send_resp(status, "")

      {:ok, result} ->
        etag = generate_etag(stream_id, result.offset)
        if_none_match = get_req_header(conn, "if-none-match") |> List.first()

        if if_none_match == etag do
          conn
          |> put_resp_header("etag", etag)
          |> send_resp(304, "")
        else
          conn
          |> put_resp_header("stream-next-offset", result.offset)
          |> put_resp_header("etag", etag)
          |> maybe_put_cursor_header(live, stream_id)
          |> maybe_put_up_to_date_header(result.has_more)
          |> maybe_put_closed_header(result.closed)
          |> put_cache_headers(live)
          |> put_resp_header("content-type", meta.content_type)
          |> send_resp(200, result.data)
        end

      {:error, :not_found} ->
        send_error(conn, 404, "Stream not found")
    end
  end

  defp handle_json_read(conn, stream_id, offset, live, timeout, _meta) do
    case StreamManager.read_messages(stream_id, offset, live: live, timeout: timeout) do
      {:ok, %{messages: []} = result} ->
        # For empty streams, use zero offset instead of -1
        actual_offset = if Offset.start?(result.offset), do: Offset.zero(), else: result.offset
        # Per protocol: long-poll timeout with no data returns 204 No Content
        # For regular reads, return 200 with empty array
        {status, body} = if live, do: {204, ""}, else: {200, "[]"}
        conn
        |> put_resp_header("stream-next-offset", actual_offset)
        |> put_resp_header("stream-up-to-date", "true")
        |> maybe_put_cursor_header(live, stream_id)
        |> maybe_put_closed_header(result.closed)
        |> put_resp_header("content-type", "application/json")
        |> send_resp(status, body)

      {:ok, result} ->
        # Parse each message as JSON and return array
        json_messages =
          Enum.map(result.messages, fn msg ->
            case JSON.decode(msg.data) do
              {:ok, parsed} -> parsed
              {:error, _} -> msg.data
            end
          end)

        etag = generate_etag(stream_id, result.offset)

        conn
        |> put_resp_header("stream-next-offset", result.offset)
        |> put_resp_header("etag", etag)
        |> maybe_put_cursor_header(live, stream_id)
        |> maybe_put_closed_header(result.closed)
        |> put_cache_headers(live)
        |> put_resp_header("content-type", "application/json")
        |> send_resp(200, JSON.encode!(json_messages))

      {:error, :not_found} ->
        send_error(conn, 404, "Stream not found")
    end
  end

  defp parse_timeout(nil), do: 5_000
  defp parse_timeout(s), do: String.to_integer(s) * 1000

  defp maybe_put_closed_header(conn, true), do: put_resp_header(conn, "stream-closed", "true")
  defp maybe_put_closed_header(conn, _), do: conn

  defp maybe_put_up_to_date_header(conn, true), do: conn  # has_more = true, not up to date
  defp maybe_put_up_to_date_header(conn, false), do: put_resp_header(conn, "stream-up-to-date", "true")

  defp maybe_put_cursor_header(conn, true, stream_id) do
    # Check if client sent a cursor (for jitter handling) - can be in header OR query param
    client_cursor = get_req_header(conn, "stream-cursor") |> List.first() || conn.params["cursor"]
    cursor = generate_cursor_with_jitter(stream_id, client_cursor)
    put_resp_header(conn, "stream-cursor", cursor)
  end
  defp maybe_put_cursor_header(conn, _, _), do: conn

  defp put_cache_headers(conn, true), do: put_resp_header(conn, "cache-control", "no-cache")
  # Per protocol: catch-up reads should include stale-while-revalidate for better CDN behavior
  defp put_cache_headers(conn, false), do: put_resp_header(conn, "cache-control", "public, max-age=60, stale-while-revalidate=300")

  defp generate_etag(stream_id, offset) do
    hash = :crypto.hash(:sha256, "#{stream_id}:#{offset}") |> Base.encode16(case: :lower)
    "\"#{String.slice(hash, 0, 16)}\""
  end

  defp generate_cursor_with_jitter(_stream_id, nil) do
    generate_cursor()
  end

  defp generate_cursor_with_jitter(_stream_id, client_cursor) do
    # Parse the client cursor (now just a numeric timestamp)
    case Integer.parse(client_cursor) do
      {cursor_timestamp, ""} ->
        now = System.system_time(:millisecond)
        # If client cursor is recent (within 1 second), increment timestamp to ensure uniqueness
        if now - cursor_timestamp < 1000 do
          # Add jitter - use a timestamp slightly after the client's
          Integer.to_string(cursor_timestamp + 1)
        else
          generate_cursor()
        end

      _ ->
        generate_cursor()
    end
  end

  # Cursor is just a millisecond timestamp (numeric string)
  defp generate_cursor do
    Integer.to_string(System.system_time(:millisecond))
  end

  defp send_error(conn, status, message) do
    conn
    |> put_resp_header("content-type", "application/json")
    |> send_resp(status, JSON.encode!(%{error: message}))
  end
end
