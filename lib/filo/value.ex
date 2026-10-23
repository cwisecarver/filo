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
  (pinned by test); an invalid-UTF-8 binary falls back to blob exactly as `encode/1` does,
  without raising.

  ## Text cells: a `:fast_ascii` validity check, then `:json.encode_binary/1`

  A text cell is escaped by OTP's `:json.encode_binary/1` (OTP 27+; fathom's floor is 28).
  Output is byte-identical to the former `Jason.encode_to_iodata!/1` path (pinned by a
  golden/property test over ASCII, multibyte, U+2028/2029, controls, quotes, backslashes and
  random binaries). Validity is checked first with `String.valid?(v, :fast_ascii)`, and an
  invalid binary is answered as a blob WITHOUT calling the encoder.

  ## Why there is a pre-check again (the blob cliff, third time)

  A blob arrives as a bare binary - a SQLite driver hands back a plain binary for both TEXT
  and BLOB cells - and random bytes are essentially never valid UTF-8, so a blob cell takes
  the failure branch on every value.

    * r4: Jason raise/rescue of an exception struct, 32.84 us/value at 64 bytes, vs 0.225 us
      for a `String.valid?/1` pre-scan (146x). Fixed by the pre-scan.
    * r8 #13 (fathom expert review 2026-10-22) dropped the pre-scan and let `:json`'s
      `error({:invalid_byte, _})` be caught, on a measured 69 ns catch. That number does not
      hold where rows are actually encoded: `error/3` with `error_info` plus the raise cost
      ~1.4 us per blob cell at the top of the stack and grows with stack depth (3.2 us at
      depth 1000, 8.8 us at 5000 - and `rows_iodata/1`'s body-recursive map runs one frame
      per row deep). On fathom's keystone result (1000 rows, ~1.7 blob cells per row) that
      cost more than the text speedup saved: `wire_server_rows_per_s` fell 9% (175k -> 159k,
      3/3 runs) where the review expected a rise.
    * Now: `String.valid?(v, :fast_ascii)` first. Per cell, keystone cells, min of 21
      (loaded host, relative only): text 145 ns (Jason path 194, catch-only 71); blob 143 ns
      (Jason path 97, catch-only 1,367); 1 KB ASCII 694 ns (Jason path 3,610, catch-only
      546; plain `String.valid?/1` would be 2,516). Break-even against catch-only is about
      one blob per 17 text cells, and the pre-check has no cliff.

  The `catch` stays as belt and braces: `String.valid?/2` already guarantees `:json` accepts
  the binary, so it should be unreachable, and costs nothing when nothing raises.
  `Jason.encode_to_iodata/2` (non-bang) is still not an option: it builds the error struct
  (22.5 us/value).

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
    # Not valid UTF-8 (SQLite TEXT can legally hold it, and every BLOB arrives here as a bare
    # binary): the same blob fallback as encode/1, reached WITHOUT raising - a caught `:json`
    # error costs ~1.4 us and up per blob cell (see the moduledoc above).
    if String.valid?(v, :fast_ascii) do
      [~S({"type":"text","value":), :json.encode_binary(v), ?}]
    else
      [~S({"type":"blob","base64":"), encode_base64(v), ~S("})]
    end
  catch
    # Belt and braces: unreachable while String.valid?/2 and :json agree on UTF-8.
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
