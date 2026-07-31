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
  to blob exactly as `encode/1` does.

  ## Why the `String.valid?/1` pre-scan is back

  This branch used to skip the pre-scan and let Jason's own rejection classify the value: try
  to encode as text, rescue `Jason.EncodeError`, fall back to blob. It saved a walk on every
  text cell (the map path validates here and escapes in Jason, walking twice), and on
  text-only workloads it measured slightly faster.

  It is catastrophic on blobs. A blob arrives here as a bare binary — a SQLite driver hands
  back a plain binary for both TEXT and BLOB cells, so the storage class is already gone — and
  random bytes are essentially never valid UTF-8. So **every blob cell raised and rescued an
  exception**, and building an exception in the BEAM builds a stacktrace. Measured at 64 bytes:

      raise/rescue   32.84  µs/value    <- blob
      pre-scan        0.225 µs/value    <- blob, 146x faster
      raise/rescue    0.162 µs/value    <- ASCII text
      pre-scan        0.232 µs/value    <- ASCII text, +0.07 µs

  So the pre-scan costs ~0.07 µs on a text cell and saves ~32.6 µs on a blob cell: break-even
  is around one blob per 460 text cells, and a blob-bearing result set is hundreds of times
  faster. It went unnoticed because every benchmark that drives this path — TPC-B, TPC-C, the
  wire benches — is INTEGER, REAL and TEXT only, and never put a blob on the wire.

  `Jason.encode_to_iodata/2` (non-bang) was measured too and is not a fix: it still builds the
  error struct, and came in at 22.5 µs/value.

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
    if String.valid?(v) do
      [~S({"type":"text","value":), Jason.encode_to_iodata!(v), ?}]
    else
      # Not valid UTF-8 (SQLite TEXT can legally hold it, and every BLOB arrives here as a
      # bare binary) — same blob fallback as encode/1, now reached WITHOUT raising.
      [~S({"type":"blob","base64":"), encode_base64(v), ~S("})]
    end
  rescue
    # Belt and braces. `String.valid?/1` already guarantees Jason accepts the binary, so this
    # should be unreachable; keeping it means a future encoder change degrades to a blob rather
    # than crashing a response mid-result-set. Costs nothing when nothing raises.
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
