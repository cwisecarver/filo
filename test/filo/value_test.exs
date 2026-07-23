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

  describe "encode_json/1" do
    # The direct-to-iodata encoder (the JSON transports' fast path): per cell it emits the
    # final wire bytes instead of an intermediate tagged map that Jason re-walks. The wire
    # form must be EXACTLY equivalent to Jason-encoding encode/1's map.
    test "wire-equivalent to Jason.encode(encode/1) across every value shape" do
      values = [
        nil,
        0,
        42,
        -7,
        9_223_372_036_854_775_807,
        -9_223_372_036_854_775_808,
        3.14,
        1.0,
        -2.5e300,
        "",
        "hello",
        "h\u00e9llo, \u4e16\u754c",
        ~s(quotes " and \\ backslashes\nnewlines\ttabs),
        <<3>>,
        {:blob, ""},
        {:blob, <<0, 1, 2, 255>>}
      ]

      for v <- values do
        via_fragment = v |> Value.encode_json() |> IO.iodata_to_binary() |> Jason.decode!()
        via_map = v |> Value.encode() |> Jason.encode!() |> Jason.decode!()
        assert via_fragment == via_map, "wire mismatch for #{inspect(v)}"
      end
    end

    test "a non-utf-8 binary falls back to blob, matching encode/1" do
      bin = <<0xFF, 0xFE, "tail">>
      decoded = bin |> Value.encode_json() |> IO.iodata_to_binary() |> Jason.decode!()
      assert decoded == %{"type" => "blob", "base64" => Base.encode64(bin, padding: false)}
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
