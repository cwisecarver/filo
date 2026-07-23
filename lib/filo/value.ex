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

  @doc """
  Encodes a native term straight to Hrana-value JSON **iodata** — the JSON transports'
  fast path.

  `encode/1`'s tagged map exists only to be consumed by a JSON encoder on those
  transports, costing a map + key/tag binaries per cell and a second full traversal by
  Jason; row encoding is the dominant per-request CPU of a SQL proxy, and the
  intermediate layer roughly doubled it. This emits the final wire bytes directly —
  five value shapes, all trivially concatenable. Floats and text delegate to
  `Jason.encode_to_iodata!/1` so number formatting and string escaping stay
  byte-identical to the map path (pinned by test); an invalid-UTF-8 binary falls back
  to blob exactly as `encode/1` does, via the encoder's own rejection — which also
  drops the `String.valid?/1` pre-scan (the map path walks every text cell twice:
  validity here, escaping in Jason).

  The protobuf transports keep consuming `encode/1`'s maps — this is JSON-only.
  """
  @spec encode_json(native()) :: iodata()
  def encode_json(nil), do: ~S({"type":"null"})

  def encode_json(v) when is_integer(v),
    do: [~S({"type":"integer","value":"), Integer.to_string(v), ~S("})]

  def encode_json(v) when is_float(v),
    do: [~S({"type":"float","value":), Jason.encode_to_iodata!(v), ?}]

  def encode_json({:blob, v}) when is_binary(v),
    do: [~S({"type":"blob","base64":"), encode_base64(v), ~S("})]

  def encode_json(v) when is_binary(v) do
    [~S({"type":"text","value":), Jason.encode_to_iodata!(v), ?}]
  rescue
    # Not valid UTF-8 (SQLite TEXT can legally hold it) — same blob fallback as encode/1.
    Jason.EncodeError -> [~S({"type":"blob","base64":"), encode_base64(v), ~S("})]
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
