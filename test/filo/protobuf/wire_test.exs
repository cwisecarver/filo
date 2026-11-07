defmodule Filo.Protobuf.WireTest do
  use ExUnit.Case, async: true

  alias Filo.Protobuf.Wire

  defp bin(iodata), do: IO.iodata_to_binary(iodata)

  describe "varint" do
    test "encodes canonical values" do
      assert bin(Wire.varint(0)) == <<0>>
      assert bin(Wire.varint(1)) == <<1>>
      assert bin(Wire.varint(127)) == <<127>>
      assert bin(Wire.varint(128)) == <<0x80, 0x01>>
      assert bin(Wire.varint(300)) == <<0xAC, 0x02>>
    end
  end

  describe "tag" do
    test "packs field number and wire type" do
      assert bin(Wire.tag(1, 2)) == <<0x0A>>
      assert bin(Wire.tag(1, 0)) == <<0x08>>
      assert bin(Wire.tag(2, 0)) == <<0x10>>
      assert bin(Wire.tag(3, 2)) == <<0x1A>>
    end
  end

  describe "zigzag" do
    test "matches the canonical mapping" do
      assert Wire.zigzag_encode(0) == 0
      assert Wire.zigzag_encode(-1) == 1
      assert Wire.zigzag_encode(1) == 2
      assert Wire.zigzag_encode(-2) == 3
      assert Wire.zigzag_encode(2) == 4
      assert Wire.zigzag_encode(2_147_483_647) == 4_294_967_294
    end

    test "round-trips across the i64 range" do
      for n <- [0, 1, -1, 42, -42, 9_223_372_036_854_775_807, -9_223_372_036_854_775_808] do
        assert Wire.zigzag_decode(Wire.zigzag_encode(n)) == n
      end
    end
  end

  describe "field encoders" do
    test "field_varint emits tag then value" do
      assert bin(Wire.field_varint(2, 150)) == <<0x10, 0x96, 0x01>>
    end

    test "field_len emits tag, length, then bytes" do
      assert bin(Wire.field_len(1, "abc")) == <<0x0A, 0x03, ?a, ?b, ?c>>
    end

    test "field_double round-trips through fixed64" do
      [{3, {:fixed64, bytes}}] = Wire.decode_fields(bin(Wire.field_double(3, 1.5)))
      assert Wire.decode_double(bytes) == 1.5
    end

    test "field_sint64 zigzag-encodes" do
      [{4, {:varint, z}}] = Wire.decode_fields(bin(Wire.field_sint64(4, -2)))
      assert z == 3
      assert Wire.zigzag_decode(z) == -2
    end
  end

  describe "decode_fields" do
    test "returns fields in wire order with their typed payloads" do
      bytes = bin([Wire.field_varint(1, 5), Wire.field_len(2, "hi")])
      assert Wire.decode_fields(bytes) == [{1, {:varint, 5}}, {2, {:len, "hi"}}]
    end

    test "repeated fields appear once per occurrence, in order" do
      bytes = bin([Wire.field_varint(1, 1), Wire.field_varint(1, 2), Wire.field_varint(1, 3)])

      assert Wire.decode_fields(bytes) == [
               {1, {:varint, 1}},
               {1, {:varint, 2}},
               {1, {:varint, 3}}
             ]
    end

    test "round-trips an empty message" do
      assert Wire.decode_fields(<<>>) == []
    end
  end

  # fathom expert review 2026-11-07: decode_varint shifted an unbounded accumulator, one bignum
  # OR per continuation byte, so a body of n continuation bytes cost O(n^2) - reached before
  # authorization. A varint is at most 10 bytes (the 10th may carry only bit 63).
  describe "varint length bound" do
    @max_u64 0xFFFFFFFFFFFFFFFF

    test "an 11-byte varint raises" do
      assert_raise ArgumentError, fn ->
        Wire.decode_fields(<<8>> <> :binary.copy(<<0xFF>>, 11) <> <<0>>)
      end
    end

    test "a 10th byte carrying more than bit 63 raises, in both terminator and continuation form" do
      nine = :binary.copy(<<0xFF>>, 9)
      assert_raise ArgumentError, fn -> Wire.decode_fields(<<8>> <> nine <> <<0x02>>) end
      assert_raise ArgumentError, fn -> Wire.decode_fields(<<8>> <> nine <> <<0x81, 0>>) end
    end

    test "a length prefix is bounded too" do
      assert_raise ArgumentError, fn ->
        Wire.decode_fields(<<10>> <> :binary.copy(<<0xFF>>, 11) <> <<0>>)
      end

      assert_raise ArgumentError, fn ->
        Wire.split_delimited(:binary.copy(<<0xFF>>, 11) <> <<0>>)
      end
    end

    test "200,000 continuation bytes fail fast (was quadratic: > 1.5 s)" do
      body = <<8>> <> :binary.copy(<<0xFF>>, 200_000) <> <<0>>

      {micros, result} =
        :timer.tc(fn ->
          try do
            Wire.decode_fields(body)
          rescue
            e in ArgumentError -> {:raised, e}
          end
        end)

      assert {:raised, %ArgumentError{}} = result
      assert micros < 50_000, "took #{div(micros, 1000)} ms"
    end

    test "int64 extremes round-trip, and negative int64 is a 10-byte varint" do
      for n <- [9_223_372_036_854_775_807, -9_223_372_036_854_775_808, -1, 0, 1] do
        [{1, {:varint, u}}] = Wire.decode_fields(bin(Wire.field_int(1, n)))
        assert u == Bitwise.band(n, @max_u64)
        assert if(u >= 0x8000000000000000, do: u - 0x10000000000000000, else: u) == n

        [{1, {:varint, z}}] = Wire.decode_fields(bin(Wire.field_sint64(1, n)))
        assert Wire.zigzag_decode(z) == n
      end

      # tag byte + 10 varint bytes
      assert byte_size(bin(Wire.field_int(1, -1))) == 11
      assert byte_size(bin(Wire.field_int(1, -9_223_372_036_854_775_808))) == 11
      assert byte_size(bin(Wire.field_sint64(1, -9_223_372_036_854_775_808))) == 11
    end

    test "the largest uint64 (a 10-byte varint ending in 0x01) decodes" do
      assert Wire.decode_fields(<<8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01>>) ==
               [{1, {:varint, @max_u64}}]
    end
  end

  describe "delimit" do
    test "length-prefixes a message" do
      assert bin(Wire.delimit("hi")) == <<0x02, ?h, ?i>>
    end
  end
end
