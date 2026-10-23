defmodule Filo.ValueJsonTextGoldenTest do
  use ExUnit.Case, async: true

  # expert review 2026-10-22 r8 #13: text cells moved from `String.valid?/1` + Jason to
  # `:json.encode_binary/1` with a catch-to-blob. The wire bytes must be IDENTICAL to the old
  # path for every input, valid or not. `reference/1` is the pre-change implementation.

  alias Filo.Value

  defp reference(v) when is_binary(v) do
    if String.valid?(v) do
      [~S({"type":"text","value":), Jason.encode_to_iodata!(v), ?}]
    else
      [~S({"type":"blob","base64":"), Base.encode64(v, padding: false), ~S("})]
    end
    |> IO.iodata_to_binary()
  end

  defp assert_same(v) do
    assert IO.iodata_to_binary(Value.encode_json(v)) == reference(v), "mismatch for #{inspect(v)}"
  end

  test "golden strings" do
    for v <- [
          "",
          "a",
          "hello \"quoted\" \\ back/slash",
          "tab\tnl\ncr\rbs\bff\f",
          <<0, 1, 31, 127>>,
          "line" <> <<0xE2, 0x80, 0xA8>> <> "sep" <> <<0xE2, 0x80, 0xA9>> <> "para",
          "héllo, 世界 \u{1F600}",
          "\u0000end",
          String.duplicate("x", 5_000),
          # invalid UTF-8: lone continuation, truncated sequences, overlong, surrogate, > U+10FFFF
          <<0xFF, 0xFE>>,
          <<0x80>>,
          <<0xC3>>,
          <<0xE4, 0xB8>>,
          <<0xC0, 0x80>>,
          <<0xED, 0xA0, 0x80>>,
          <<0xF4, 0x90, 0x80, 0x80>>,
          "ok" <> <<0xFF>>,
          <<0xFF>> <> "ok"
        ] do
      assert_same(v)
    end
  end

  test "every code point and random binaries" do
    for cp <- 0..0x10FFFF, cp not in 0xD800..0xDFFF, rem(cp, 7) == 0 or cp < 0x900 do
      assert_same(<<cp::utf8>>)
    end

    for _ <- 1..20_000, do: assert_same(:crypto.strong_rand_bytes(:rand.uniform(48)))

    # mostly-valid strings with a few random code points
    for _ <- 1..5_000 do
      s = for(_ <- 1..:rand.uniform(30), into: "", do: <<random_cp()::utf8>>)

      assert_same(s)
    end
  end

  defp random_cp do
    cp = :rand.uniform(0x10FFFF)
    if cp in 0xD800..0xDFFF, do: random_cp(), else: cp
  end
end
