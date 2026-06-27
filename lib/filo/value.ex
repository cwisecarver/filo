defmodule Filo.Value do
  @moduledoc """
  Hrana value codec.

  Hrana represents SQLite values as tagged JSON maps:

      %{"type" => "null"}
      %{"type" => "integer", "value" => "42"}   # i64 carried as a string, for precision
      %{"type" => "float",   "value" => 3.14}
      %{"type" => "text",    "value" => "..."}
      %{"type" => "blob",    "base64" => "..."}  # standard base64, no padding

  `decode/1` turns a Hrana value into a native Elixir term suitable for binding
  into SQLite; `encode/1` turns a native term back into a Hrana value. A `text`
  value decodes to a binary and a `blob` to `{:blob, binary}`, so the two stay
  distinguishable when re-encoding.
  """

  @type native :: nil | integer() | float() | binary() | {:blob, binary()}
  @type t :: %{required(String.t()) => term()}

  @doc "Decodes a Hrana value map into a native Elixir term."
  @spec decode(t()) :: native()
  def decode(%{"type" => "null"}), do: nil
  def decode(%{"type" => "integer", "value" => v}), do: String.to_integer(v)
  def decode(%{"type" => "float", "value" => v}), do: v * 1.0
  def decode(%{"type" => "text", "value" => v}), do: v
  def decode(%{"type" => "blob", "base64" => v}), do: {:blob, decode_base64(v)}

  @doc "Encodes a native Elixir term into a Hrana value map."
  @spec encode(native()) :: t()
  def encode(nil), do: %{"type" => "null"}
  def encode(v) when is_integer(v), do: %{"type" => "integer", "value" => Integer.to_string(v)}
  def encode(v) when is_float(v), do: %{"type" => "float", "value" => v}
  def encode({:blob, v}) when is_binary(v), do: %{"type" => "blob", "base64" => encode_base64(v)}

  def encode(v) when is_binary(v) do
    if String.valid?(v) do
      %{"type" => "text", "value" => v}
    else
      %{"type" => "blob", "base64" => encode_base64(v)}
    end
  end

  # Hrana uses standard base64 without padding, and tolerates trailing padding
  # on input.
  defp encode_base64(bin), do: Base.encode64(bin, padding: false)

  defp decode_base64(str) do
    str
    |> String.trim_trailing("=")
    |> Base.decode64!(padding: false)
  end
end
