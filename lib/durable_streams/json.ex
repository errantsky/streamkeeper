defmodule DurableStreams.JSON do
  @moduledoc """
  JSON encoding/decoding using Erlang's stdlib :json module (OTP 27+).

  Provides a Jason-compatible API for easy migration.
  """

  @doc """
  Encodes a term to a JSON string.

  Raises on encoding errors.

  ## Examples

      iex> DurableStreams.JSON.encode!(%{key: "value"})
      "{\"key\":\"value\"}"

      iex> DurableStreams.JSON.encode!([1, 2, 3])
      "[1,2,3]"
  """
  @spec encode!(term()) :: String.t()
  def encode!(term) do
    :json.encode(term) |> IO.iodata_to_binary()
  end

  @doc """
  Decodes a JSON string to an Elixir term.

  Returns `{:ok, term}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> DurableStreams.JSON.decode("{\"key\":\"value\"}")
      {:ok, %{"key" => "value"}}

      iex> DurableStreams.JSON.decode("invalid")
      {:error, :invalid_json}
  """
  @spec decode(String.t()) :: {:ok, term()} | {:error, :invalid_json}
  def decode(string) when is_binary(string) do
    {:ok, :json.decode(string)}
  rescue
    _ -> {:error, :invalid_json}
  end

  @doc """
  Decodes a JSON string to an Elixir term.

  Raises on decoding errors.

  ## Examples

      iex> DurableStreams.JSON.decode!("{\"key\":\"value\"}")
      %{"key" => "value"}
  """
  @spec decode!(String.t()) :: term()
  def decode!(string) when is_binary(string) do
    :json.decode(string)
  end
end
