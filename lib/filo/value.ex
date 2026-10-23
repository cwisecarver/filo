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
  five value shapes, all trivially concatenable. Text is escaped by `:json.encode_binary/1`
  and floats use `:erlang.float_to_binary/2`, both byte-identical to the Jason map path
  (pinned by test); an invalid-UTF-8 binary falls back to blob exactly as `encode/1` does.

  ## Text cells: `:json.encode_binary/1` with a catch, no pre-scan

  A text cell is encoded with OTP's `:json.encode_binary/1` (OTP 27+; fathom's floor is 28),
  which validates UTF-8 while it escapes, so the separate `String.valid?/1` walk is gone. An
  invalid binary makes it `error({:invalid_byte, _, _})`, which is caught and answered as a
  blob. Output is byte-identical to the former `Jason.encode_to_iodata!/1` path (pinned by a
  golden/property test over ASCII, multibyte, U+2028/2029, controls, quotes, backslashes and
  random binaries). Measured (expert review 2026-10-22 r8 #13): 4-7x faster per cell.

  ## Why catching is fine here (and Jason's rescue was not)

  A blob arrives as a bare binary - a SQLite driver hands back a plain binary for both TEXT
  and BLOB cells - and random bytes are essentially never valid UTF-8, so a blob cell takes
  the failure branch on every value. With Jason that meant raise/rescue of an exception
  struct (stacktrace and all): 32.84 us/value at 64 bytes, vs 0.225 us for a `String.valid?`
  pre-scan, which is why the pre-scan was added (146x on blobs). `:json` raises a plain
  `error` term with no exception struct, and catching it measured 69 ns, so the pre-scan can
  go: no cost on text, no cliff on blobs. `Jason.encode_to_iodata/2` (non-bang) is still not
  an option: it builds the error struct (22.5 us/value).

  The protobuf transports keep consuming `encode/1`'s maps — this is JSON-only.
  """
  @spec encode_json(native()) :: iodata()
  def encode_json(nil), do: ~S({"type":"null"})

  def encode_json(v) when is_integer(v),
    do: [~S({"type":"integer","value":"), Integer.to_string(v), ~S("})]

  def encode_json(v) when is_float(v),
    do: [~S({"type":"float","value":), :erlang.float_to_binary(v, [:short]), ?}]

  def encode_json({:blob, v}) when is_binary(v),
    do: [~S({"type":"blob","base64":"), encode_base64(v), ~S("})]

  def encode_json(v) when is_binary(v) do
    [~S({"type":"text","value":), :json.encode_binary(v), ?}]
  catch
    # Not valid UTF-8 (SQLite TEXT can legally hold it, and every BLOB arrives here as a bare
    # binary) - same blob fallback as encode/1. `:json` raises a plain error term, which is
    # cheap to catch (69 ns), unlike Jason's exception struct.
    :error, _ -> [~S({"type":"blob","base64":"), encode_base64(v), ~S("})]
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
