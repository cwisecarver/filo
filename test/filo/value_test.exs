defmodule Filo.ValueTest do
  use ExUnit.Case, async: true

  alias Filo.Value

  describe "decode/1" do
    test "null -> nil" do
      assert Value.decode(%{"type" => "null"}) == nil
    end

    test "integer is carried as a string, parsed to an Elixir integer" do
      assert Value.decode(%{"type" => "integer", "value" => "42"}) == 42
    end

    test "integer preserves the full signed 64-bit range" do
      assert Value.decode(%{"type" => "integer", "value" => "9223372036854775807"}) ==
               9_223_372_036_854_775_807

      assert Value.decode(%{"type" => "integer", "value" => "-9223372036854775808"}) ==
               -9_223_372_036_854_775_808
    end

    test "float -> float" do
      assert Value.decode(%{"type" => "float", "value" => 3.14}) == 3.14
    end

    test "text -> binary" do
      assert Value.decode(%{"type" => "text", "value" => "hello"}) == "hello"
    end

    test "blob is standard base64 without padding -> {:blob, binary}" do
      assert Value.decode(%{"type" => "blob", "base64" => "aGVsbG8"}) == {:blob, "hello"}
    end

    test "blob decoding tolerates trailing padding" do
      assert Value.decode(%{"type" => "blob", "base64" => "aGVsbG8="}) == {:blob, "hello"}
    end
  end

  describe "encode/1" do
    test "nil -> null" do
      assert Value.encode(nil) == %{"type" => "null"}
    end

    test "integer -> string-valued integer" do
      assert Value.encode(42) == %{"type" => "integer", "value" => "42"}
    end

    test "integer preserves the full signed 64-bit range as a string" do
      assert Value.encode(9_223_372_036_854_775_807) ==
               %{"type" => "integer", "value" => "9223372036854775807"}
    end

    test "float -> float" do
      assert Value.encode(3.14) == %{"type" => "float", "value" => 3.14}
    end

    test "utf-8 binary -> text" do
      assert Value.encode("hello") == %{"type" => "text", "value" => "hello"}
    end

    test "explicit {:blob, _} -> base64 without padding" do
      assert Value.encode({:blob, "hello"}) == %{"type" => "blob", "base64" => "aGVsbG8"}
    end

    test "non-utf-8 binary -> blob" do
      assert Value.encode(<<0xFF, 0xFE>>) ==
               %{"type" => "blob", "base64" => Base.encode64(<<0xFF, 0xFE>>, padding: false)}
    end
  end

  describe "round-trip" do
    test "native values survive encode |> decode" do
      values = [
        nil,
        0,
        42,
        -7,
        9_223_372_036_854_775_807,
        3.14,
        "",
        "héllo, 世界",
        {:blob, <<0, 1, 2, 255>>}
      ]

      for v <- values do
        assert Value.decode(Value.encode(v)) == v
      end
    end
  end
end
