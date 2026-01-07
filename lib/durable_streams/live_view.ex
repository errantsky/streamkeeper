# Only compile this module if Phoenix.LiveView is available
if Code.ensure_loaded?(Phoenix.LiveView) do
  defmodule DurableStreams.LiveView do
    @moduledoc """
    Helpers for consuming durable streams in Phoenix LiveView.

    This module provides a simple, explicit API for integrating durable streams
    into LiveView applications. It handles the long-polling loop, connection
    management, and message routing while leaving application-specific logic
    to your LiveView.

    ## Usage

    ```elixir
    defmodule MyAppWeb.EventsLive do
      use Phoenix.LiveView
      alias DurableStreams.LiveView, as: DSLive

      def mount(_params, _session, socket) do
        {:ok, DSLive.init(socket)}
      end

      def handle_event("subscribe", %{"stream_id" => stream_id}, socket) do
        {:noreply, DSLive.listen(socket, stream_id)}
      end

      def handle_event("unsubscribe", _, socket) do
        {:noreply, DSLive.stop(socket)}
      end

      # Handle stream messages
      def handle_info(msg, socket) do
        if DSLive.stream_message?(msg) do
          case DSLive.handle_message(socket, msg) do
            {:data, messages, socket} ->
              # Process messages your way
              {:noreply, process_messages(socket, messages)}

            {:status, _status, socket} ->
              {:noreply, socket}

            {:complete, socket} ->
              {:noreply, assign(socket, :finished, true)}

            {:error, reason, socket} ->
              {:noreply, assign(socket, :error, reason)}
          end
        else
          # Handle other messages
          {:noreply, socket}
        end
      end

      defp process_messages(socket, messages) do
        Enum.reduce(messages, socket, fn msg, acc ->
          update(acc, :events, &[msg.data | &1])
        end)
      end
    end
    ```

    ## Socket Assigns

    This module uses the following assigns (prefixed with `ds_` to avoid conflicts):

    - `:ds_stream_id` - The current stream ID being listened to
    - `:ds_offset` - The current offset in the stream
    - `:ds_status` - Connection status (`:idle`, `:connecting`, `:streaming`, `:disconnected`)
    - `:ds_listener_pid` - PID of the listener process
    - `:ds_listener_ref` - Monitor reference for the listener

    ## Options

    Both `init/2` and `listen/3` accept options:

    - `:timeout` - Long-poll timeout in milliseconds (default: 30_000)
    - `:offset` - Starting offset (default: "-1" for beginning)
    """

    import Phoenix.Component, only: [assign: 3]
    require Logger

  @default_timeout 30_000

  # Message types used internally
  @status_msg :ds_status
  @data_msg :ds_data
  @complete_msg :ds_complete
  @error_msg :ds_error

  @doc """
  Initializes stream-related assigns on the socket.

  Call this in your `mount/3` callback to set up the required assigns.

  ## Options

  - `:timeout` - Default long-poll timeout in milliseconds (default: 30_000)

  ## Example

      def mount(_params, _session, socket) do
        {:ok, DurableStreams.LiveView.init(socket)}
      end
  """
  @spec init(Phoenix.LiveView.Socket.t(), keyword()) :: Phoenix.LiveView.Socket.t()
  def init(socket, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    socket
    |> assign(:ds_stream_id, nil)
    |> assign(:ds_offset, "-1")
    |> assign(:ds_status, :idle)
    |> assign(:ds_listener_pid, nil)
    |> assign(:ds_listener_ref, nil)
    |> assign(:ds_timeout, timeout)
  end

  @doc """
  Starts listening to a durable stream.

  This spawns a background process that long-polls the stream and sends
  messages back to the LiveView. Messages are handled via `handle_message/2`.

  ## Options

  - `:offset` - Starting offset (default: current offset or "-1")
  - `:timeout` - Long-poll timeout in milliseconds (default: from init)

  ## Example

      def handle_event("subscribe", %{"stream_id" => id}, socket) do
        {:noreply, DurableStreams.LiveView.listen(socket, id)}
      end
  """
  @spec listen(Phoenix.LiveView.Socket.t(), String.t(), keyword()) :: Phoenix.LiveView.Socket.t()
  def listen(socket, stream_id, opts \\ []) do
    # Stop any existing listener first
    socket = stop(socket)

    offset = Keyword.get(opts, :offset, socket.assigns[:ds_offset] || "-1")
    timeout = Keyword.get(opts, :timeout, socket.assigns[:ds_timeout] || @default_timeout)

    parent = self()
    pid = spawn(fn -> poll_loop(parent, stream_id, offset, timeout) end)
    ref = Process.monitor(pid)

    socket
    |> assign(:ds_stream_id, stream_id)
    |> assign(:ds_offset, offset)
    |> assign(:ds_status, :connecting)
    |> assign(:ds_listener_pid, pid)
    |> assign(:ds_listener_ref, ref)
  end

  @doc """
  Stops listening to the current stream.

  This terminates the listener process and resets the connection status.
  The stream ID and offset are preserved for potential reconnection.

  ## Example

      def handle_event("disconnect", _, socket) do
        {:noreply, DurableStreams.LiveView.stop(socket)}
      end
  """
  @spec stop(Phoenix.LiveView.Socket.t()) :: Phoenix.LiveView.Socket.t()
  def stop(socket) do
    # Demonitor first to avoid receiving DOWN message
    if ref = socket.assigns[:ds_listener_ref] do
      Process.demonitor(ref, [:flush])
    end

    # Kill the listener process
    if pid = socket.assigns[:ds_listener_pid] do
      Process.exit(pid, :kill)
    end

    socket
    |> assign(:ds_listener_pid, nil)
    |> assign(:ds_listener_ref, nil)
    |> assign(:ds_status, :disconnected)
  end

  @doc """
  Resets all stream state, clearing the stream ID and offset.

  Use this when navigating away from a stream entirely.

  ## Example

      def handle_params(%{}, _uri, socket) do
        {:noreply, DurableStreams.LiveView.reset(socket)}
      end
  """
  @spec reset(Phoenix.LiveView.Socket.t()) :: Phoenix.LiveView.Socket.t()
  def reset(socket) do
    socket
    |> stop()
    |> assign(:ds_stream_id, nil)
    |> assign(:ds_offset, "-1")
    |> assign(:ds_status, :idle)
  end

  @doc """
  Checks if a message is from the stream listener.

  Use this as a guard in your `handle_info/2` to route stream messages.

  ## Example

      def handle_info(msg, socket) do
        if DurableStreams.LiveView.stream_message?(msg) do
          # Handle stream message
        else
          # Handle other messages
        end
      end
  """
  @spec stream_message?(term()) :: boolean()
  def stream_message?({@status_msg, _}), do: true
  def stream_message?({@data_msg, _, _}), do: true
  def stream_message?({@complete_msg}), do: true
  def stream_message?({@error_msg, _}), do: true
  def stream_message?({:DOWN, _, :process, _, _}), do: true
  def stream_message?(_), do: false

  @doc """
  Handles a stream message, returning the result and updated socket.

  Returns one of:
  - `{:data, messages, socket}` - New data received, messages is a list of `%{data: binary, offset: string}`
  - `{:status, status, socket}` - Status changed (`:connecting`, `:streaming`, `:disconnected`)
  - `{:complete, socket}` - Stream is closed, no more data
  - `{:error, reason, socket}` - An error occurred

  ## Example

      def handle_info(msg, socket) do
        case DurableStreams.LiveView.handle_message(socket, msg) do
          {:data, messages, socket} ->
            {:noreply, process_messages(socket, messages)}
          {:status, _status, socket} ->
            {:noreply, socket}
          {:complete, socket} ->
            {:noreply, socket}
          {:error, reason, socket} ->
            {:noreply, assign(socket, :error, reason)}
        end
      end
  """
  @spec handle_message(Phoenix.LiveView.Socket.t(), term()) ::
          {:data, list(map()), Phoenix.LiveView.Socket.t()}
          | {:status, atom(), Phoenix.LiveView.Socket.t()}
          | {:complete, Phoenix.LiveView.Socket.t()}
          | {:error, term(), Phoenix.LiveView.Socket.t()}
  def handle_message(socket, {@status_msg, status}) do
    {:status, status, assign(socket, :ds_status, status)}
  end

  def handle_message(socket, {@data_msg, messages, new_offset}) do
    socket =
      socket
      |> assign(:ds_offset, new_offset)
      |> assign(:ds_status, :streaming)

    {:data, messages, socket}
  end

  def handle_message(socket, {@complete_msg}) do
    {:complete, stop(socket)}
  end

  def handle_message(socket, {@error_msg, reason}) do
    {:error, reason, stop(socket)}
  end

  def handle_message(socket, {:DOWN, ref, :process, pid, reason}) do
    # Only handle if it's our listener
    if socket.assigns[:ds_listener_ref] == ref and socket.assigns[:ds_listener_pid] == pid do
      if reason in [:normal, :killed] do
        {:complete, stop(socket)}
      else
        Logger.warning("[DurableStreams.LiveView] Listener crashed: #{inspect(reason)}")
        {:error, {:listener_crashed, reason}, stop(socket)}
      end
    else
      # Not our message, return unchanged
      {:status, socket.assigns[:ds_status], socket}
    end
  end

  @doc """
  Returns the current stream status.

  Possible values: `:idle`, `:connecting`, `:streaming`, `:disconnected`
  """
  @spec status(Phoenix.LiveView.Socket.t()) :: atom()
  def status(socket), do: socket.assigns[:ds_status] || :idle

  @doc """
  Returns the current stream ID, or nil if not listening.
  """
  @spec stream_id(Phoenix.LiveView.Socket.t()) :: String.t() | nil
  def stream_id(socket), do: socket.assigns[:ds_stream_id]

  @doc """
  Returns the current offset in the stream.
  """
  @spec offset(Phoenix.LiveView.Socket.t()) :: String.t()
  def offset(socket), do: socket.assigns[:ds_offset] || "-1"

  @doc """
  Checks if currently listening to a stream.
  """
  @spec listening?(Phoenix.LiveView.Socket.t()) :: boolean()
  def listening?(socket) do
    socket.assigns[:ds_listener_pid] != nil
  end

  # --- Private: Long-polling loop ---

  defp poll_loop(parent, stream_id, offset, timeout) do
    send(parent, {@status_msg, :streaming})
    do_poll(parent, stream_id, offset, timeout)
  end

  defp do_poll(parent, stream_id, offset, timeout) do
    case DurableStreams.StreamManager.read_messages(stream_id, offset, live: true, timeout: timeout) do
      {:ok, %{messages: [], closed: true}} ->
        send(parent, {@complete_msg})

      {:ok, %{messages: [], offset: new_offset}} ->
        # Timeout with no data, keep polling
        do_poll(parent, stream_id, new_offset, timeout)

      {:ok, %{messages: messages, offset: new_offset, closed: closed}} ->
        send(parent, {@data_msg, messages, new_offset})

        if closed do
          send(parent, {@complete_msg})
        else
          do_poll(parent, stream_id, new_offset, timeout)
        end

      {:error, :not_found} ->
        send(parent, {@error_msg, :stream_not_found})

      {:error, reason} ->
        send(parent, {@error_msg, reason})
    end
  end
  end
end
