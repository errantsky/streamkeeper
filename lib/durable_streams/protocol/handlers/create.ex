defmodule DurableStreams.Protocol.Handlers.Create do
  @moduledoc false
  # Internal handler for PUT requests to create a new stream.

  import Plug.Conn
  alias DurableStreams.{Stream, StreamManager}

  def call(conn) do
    stream_id = conn.path_params["stream_id"]

    # Parse headers
    content_type =
      get_req_header(conn, "content-type") |> List.first() || "application/octet-stream"

    ttl_header = get_req_header(conn, "stream-ttl") |> List.first()
    expires_at_header = get_req_header(conn, "stream-expires-at") |> List.first()

    # Validate TTL and Expires-At aren't both specified
    if ttl_header && expires_at_header do
      send_error(conn, 400, "Cannot specify both Stream-TTL and Stream-Expires-At")
    else
      case parse_expiry(ttl_header, expires_at_header) do
        {:error, message} ->
          send_error(conn, 400, message)

        {:ok, ttl, expires_at} ->
          create_stream(conn, stream_id, content_type, ttl, expires_at)
      end
    end
  end

  defp create_stream(conn, stream_id, content_type, ttl, expires_at) do
    opts =
      [content_type: content_type]
      |> maybe_add(:ttl, ttl)
      |> maybe_add(:expires_at, expires_at)

    case StreamManager.create(stream_id, opts) do
      {:ok, ^stream_id} ->
        # Handle PUT with body
        conn = handle_initial_body(conn, stream_id)

        # Build absolute Location URL
        scheme = if conn.scheme == :https, do: "https", else: "http"
        host = conn.host
        port_suffix = if conn.port in [80, 443], do: "", else: ":#{conn.port}"
        location_url = "#{scheme}://#{host}#{port_suffix}/v1/stream/#{stream_id}"

        conn
        |> put_resp_header("location", location_url)
        |> put_resp_header("content-type", content_type)
        |> maybe_add_ttl_headers(ttl, expires_at)
        |> send_resp(201, "")

      {:error, :already_exists} ->
        handle_idempotent_create(conn, stream_id, content_type, ttl, expires_at)
    end
  end

  defp handle_idempotent_create(conn, stream_id, content_type, ttl, expires_at) do
    case StreamManager.get_metadata(stream_id) do
      {:ok, meta} ->
        if config_matches?(meta, content_type, ttl, expires_at) do
          # Build absolute Location URL
          scheme = if conn.scheme == :https, do: "https", else: "http"
          host = conn.host
          port_suffix = if conn.port in [80, 443], do: "", else: ":#{conn.port}"
          location_url = "#{scheme}://#{host}#{port_suffix}/v1/stream/#{stream_id}"

          conn
          |> put_resp_header("location", location_url)
          |> put_resp_header("content-type", meta.content_type)
          |> maybe_add_ttl_headers(meta.ttl, meta.expires_at)
          |> send_resp(200, "")
        else
          send_error(conn, 409, "Stream exists with different configuration")
        end

      {:error, :not_found} ->
        # Stream was deleted between create and metadata check
        send_error(conn, 409, "Stream already exists")
    end
  end

  defp config_matches?(meta, content_type, ttl, expires_at) do
    # Content types must match (case insensitive, ignoring charset)
    content_type_matches = Stream.content_type_matches?(meta.content_type, content_type)

    # TTL must match (either both nil or same value)
    ttl_matches = meta.ttl == ttl

    # Expires-at is tricky - if we specified TTL, it computed expires_at
    # For idempotent check, we compare the original TTL values
    # If expires_at was directly specified, we need to compare those
    expires_at_matches =
      cond do
        is_nil(expires_at) and is_nil(meta.expires_at) -> true
        is_nil(expires_at) or is_nil(meta.expires_at) -> ttl_matches
        true -> DateTime.compare(meta.expires_at, expires_at) == :eq
      end

    content_type_matches && (ttl_matches || expires_at_matches)
  end

  defp handle_initial_body(conn, stream_id) do
    {:ok, body, conn} = Plug.Conn.read_body(conn)

    if byte_size(body) > 0 do
      # For JSON mode, need to check if it's an empty array
      content_type = get_req_header(conn, "content-type") |> List.first() || "application/octet-stream"

      if Stream.normalize_content_type(content_type) == "application/json" do
        case Jason.decode(body) do
          {:ok, []} ->
            # Empty array - don't store anything
            :ok

          {:ok, items} when is_list(items) ->
            # Non-empty array - store each item
            Enum.each(items, fn item ->
              StreamManager.append(stream_id, Jason.encode!(item))
            end)

          {:ok, _single} ->
            # Single value - store as-is
            StreamManager.append(stream_id, body)

          {:error, _} ->
            # Invalid JSON - store as-is (will fail on read)
            StreamManager.append(stream_id, body)
        end
      else
        StreamManager.append(stream_id, body)
      end
    end

    conn
  end

  defp parse_expiry(nil, nil), do: {:ok, nil, nil}

  defp parse_expiry(ttl_str, nil) do
    case parse_ttl(ttl_str) do
      {:ok, ttl} -> {:ok, ttl, nil}
      error -> error
    end
  end

  defp parse_expiry(nil, expires_at_str) do
    case parse_expires_at(expires_at_str) do
      {:ok, expires_at} -> {:ok, nil, expires_at}
      error -> error
    end
  end

  defp parse_ttl(ttl_str) do
    # TTL must be a positive integer without leading zeros, plus signs, or decimal points
    cond do
      String.starts_with?(ttl_str, "+") ->
        {:error, "Invalid TTL: must not have plus sign"}

      String.starts_with?(ttl_str, "0") && ttl_str != "0" ->
        {:error, "Invalid TTL: must not have leading zeros"}

      String.contains?(ttl_str, ".") ->
        {:error, "Invalid TTL: must be an integer"}

      String.contains?(ttl_str, "e") || String.contains?(ttl_str, "E") ->
        {:error, "Invalid TTL: must be an integer, not scientific notation"}

      true ->
        case Integer.parse(ttl_str) do
          {ttl, ""} when ttl > 0 -> {:ok, ttl}
          {ttl, ""} when ttl <= 0 -> {:error, "Invalid TTL: must be positive"}
          _ -> {:error, "Invalid TTL: must be an integer"}
        end
    end
  end

  defp parse_expires_at(expires_at_str) do
    case DateTime.from_iso8601(expires_at_str) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, _} -> {:error, "Invalid Expires-At: must be ISO 8601 format"}
    end
  end

  defp maybe_add(opts, _key, nil), do: opts
  defp maybe_add(opts, key, value), do: [{key, value} | opts]

  defp maybe_add_ttl_headers(conn, ttl, expires_at) do
    conn
    |> maybe_put_header("stream-ttl", ttl && Integer.to_string(ttl))
    |> maybe_put_header("stream-expires-at", expires_at && DateTime.to_iso8601(expires_at))
  end

  defp maybe_put_header(conn, _name, nil), do: conn
  defp maybe_put_header(conn, name, value), do: put_resp_header(conn, name, value)

  defp send_error(conn, status, message) do
    conn
    |> put_resp_header("content-type", "application/json")
    |> send_resp(status, Jason.encode!(%{error: message}))
  end
end
