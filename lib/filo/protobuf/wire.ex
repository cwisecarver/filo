defmodule Filo.Protobuf.Wire do
  @moduledoc """
  Protobuf wire-format primitives (proto3): varints, zigzag, `fixed64`, tags, and
  length-delimited fields — just enough of the encoding to (de)serialize the
  Hrana 3 messages. The message-level codecs live in `Filo.Protobuf`.

  Encoders return `t:iodata/0` (callers wrap a whole message in
  `IO.iodata_to_binary/1`). `decode_fields/1` returns the fields in wire order as
  `{field_number, value}` where `value` is one of `{:varint, n}`,
  `{:fixed64, <<8 bytes>>}`, `{:len, binary}`, or `{:fixed32, <<4 bytes>>}`;
  repeated fields appear once per occurrence.
  """
  import Bitwise

  @mask64 0xFFFFFFFFFFFFFFFF

  @wire_varint 0
  @wire_fixed64 1
  @wire_len 2
  @wire_fixed32 5

  ## --- encoding ---

  @doc "Encodes an unsigned integer as a base-128 varint."
  @spec varint(non_neg_integer()) :: iodata()
  def varint(n) when n >= 0 and n < 0x80, do: <<n>>
  def varint(n) when n >= 0, do: [<<1::1, band(n, 0x7F)::7>>, varint(bsr(n, 7))]

  @doc "Encodes a field tag (`field_number << 3 | wire_type`)."
  @spec tag(pos_integer(), 0..5) :: iodata()
  def tag(field, wire_type), do: varint(bor(bsl(field, 3), wire_type))

  @doc "A varint field for a non-negative integer (`uint32`/`uint64`/enum)."
  @spec field_varint(pos_integer(), non_neg_integer()) :: iodata()
  def field_varint(field, n) when n >= 0, do: [tag(field, @wire_varint), varint(n)]

  @doc "A varint field for a possibly-negative `int32`/`int64` (sign-extended to 64 bits)."
  @spec field_int(pos_integer(), integer()) :: iodata()
  def field_int(field, n), do: [tag(field, @wire_varint), varint(band(n, @mask64))]

  @doc "A zigzag varint field for a `sint64`."
  @spec field_sint64(pos_integer(), integer()) :: iodata()
  def field_sint64(field, n), do: [tag(field, @wire_varint), varint(zigzag_encode(n))]

  @doc "A varint field for a `bool`."
  @spec field_bool(pos_integer(), boolean()) :: iodata()
  def field_bool(field, true), do: [tag(field, @wire_varint), <<1>>]
  def field_bool(field, false), do: [tag(field, @wire_varint), <<0>>]

  @doc "A `fixed64` field for a `double` (little-endian IEEE-754)."
  @spec field_double(pos_integer(), float()) :: iodata()
  def field_double(field, f), do: [tag(field, @wire_fixed64), <<f::little-float-64>>]

  @doc "A length-delimited field (`string`, `bytes`, or a sub-message)."
  @spec field_len(pos_integer(), iodata()) :: iodata()
  def field_len(field, data) do
    bytes = IO.iodata_to_binary(data)
    [tag(field, @wire_len), varint(byte_size(bytes)), bytes]
  end

  @doc "Length-prefixes a message (no tag) for the cursor response stream."
  @spec delimit(iodata()) :: iodata()
  def delimit(data) do
    bytes = IO.iodata_to_binary(data)
    [varint(byte_size(bytes)), bytes]
  end

  @doc "Encodes a signed integer with zigzag (so small magnitudes stay small)."
  @spec zigzag_encode(integer()) :: non_neg_integer()
  def zigzag_encode(n), do: band(bxor(bsl(n, 1), bsr(n, 63)), @mask64)

  @doc "Inverse of `zigzag_encode/1`."
  @spec zigzag_decode(non_neg_integer()) :: integer()
  def zigzag_decode(z), do: bxor(bsr(z, 1), -band(z, 1))

  ## --- decoding ---

  @doc "Decodes a `fixed64` double payload."
  @spec decode_double(binary()) :: float()
  def decode_double(<<f::little-float-64>>), do: f

  @doc "Splits a message into its `{field_number, value}` fields, in wire order."
  @spec decode_fields(binary()) :: [{pos_integer(), term()}]
  def decode_fields(bin), do: decode_fields(bin, [])

  defp decode_fields(<<>>, acc), do: Enum.reverse(acc)

  defp decode_fields(bin, acc) do
    {tag, rest} = decode_varint(bin)
    {value, rest} = decode_value(band(tag, 0x7), rest)
    decode_fields(rest, [{bsr(tag, 3), value} | acc])
  end

  defp decode_value(@wire_varint, bin) do
    {n, rest} = decode_varint(bin)
    {{:varint, n}, rest}
  end

  defp decode_value(@wire_fixed64, <<v::binary-size(8), rest::binary>>), do: {{:fixed64, v}, rest}

  defp decode_value(@wire_len, bin) do
    {len, rest} = decode_varint(bin)
    <<data::binary-size(^len), rest::binary>> = rest
    {{:len, data}, rest}
  end

  defp decode_value(@wire_fixed32, <<v::binary-size(4), rest::binary>>), do: {{:fixed32, v}, rest}

  defp decode_varint(bin), do: decode_varint(bin, 0, 0)

  defp decode_varint(<<1::1, group::7, rest::binary>>, shift, acc),
    do: decode_varint(rest, shift + 7, bor(acc, bsl(group, shift)))

  defp decode_varint(<<0::1, group::7, rest::binary>>, shift, acc),
    do: {bor(acc, bsl(group, shift)), rest}
end
