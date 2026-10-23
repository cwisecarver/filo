defmodule Filo.Json do
  @moduledoc false
  # Inbound JSON decode for the Hrana transports (expert review 2026-10-22 r8 #19).
  #
  # OTP's `:json.decode/3` is 25-33% faster than `Jason.decode/1` on request-sized frames and
  # yields identical terms (objects -> string-keyed maps, ints/floats the same, `null` -> nil via
  # the decoder override). Two behaviours are made to match Jason's explicitly:
  #
  #   * `:json.decode/3` is a prefix decoder: it returns the unparsed remainder instead of
  #     failing on trailing garbage, so a non-empty remainder is an error here (it already
  #     consumes trailing JSON whitespace).
  #   * it raises (error term / exception) on malformed input; that is mapped to `{:error, _}`.

  @spec decode(binary()) :: {:ok, term()} | {:error, term()}
  def decode(bin) when is_binary(bin) do
    case :json.decode(bin, :ok, %{null: nil}) do
      {value, :ok, ""} -> {:ok, value}
      {_value, :ok, rest} -> {:error, {:trailing, byte_size(rest)}}
    end
  catch
    :error, reason -> {:error, reason}
  end
end
