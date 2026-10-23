defmodule Filo.JsonTest do
  use ExUnit.Case, async: false

  # expert review 2026-10-22 r8 #19: inbound decode moved from Jason to :json.decode/3. The
  # accept/reject decision and the decoded terms must match Jason for everything a client can
  # send. The trailing-garbage cases pin the trap: :json.decode/3 is a PREFIX decoder and returns
  # the remainder instead of failing, which would silently accept `{"a":1}x`.

  alias Filo.Json

  @corpus [
    ~S({"a":null}),
    ~S({"a":1} ),
    " {\"a\":1}\n\t",
    ~S({"a":1e2,"b":1.5,"c":12345678901234567890123,"d":-0,"e":true,"f":[null,{}]}),
    ~S({"a":"é😀"}),
    ~S({"a":1,"a":2}),
    ~S({"type":"hello","jwt":null}),
    ~S([1,2,3]),
    ~S("str"),
    "123",
    # rejected
    "",
    "  ",
    ~S({"a":1}x),
    ~S({"a":1}{}),
    "01",
    "1.",
    "[1,]",
    "[1,2",
    "nul",
    ~S({"a":NaN}),
    ~S({'a':1}),
    ~S({"a":"\ud800"}),
    "\"a\tb\"",
    <<"{\"a\":\"", 0xFF, "\"}">>,
    <<0xFF>>,
    "{\"a\":\"x\"}  ",
    <<0xEF, 0xBB, 0xBF, "{}">>
  ]

  test "agrees with Jason on accept/reject and on the decoded term" do
    for input <- @corpus do
      expected =
        case Jason.decode(input) do
          {:ok, term} -> {:ok, term}
          {:error, _} -> :error
        end

      actual =
        case Json.decode(input) do
          {:ok, term} -> {:ok, term}
          {:error, _} -> :error
        end

      assert actual == expected, "diverged on #{inspect(input)}"
    end
  end

  test "random byte strings never diverge from Jason's accept/reject" do
    for _ <- 1..5_000 do
      input = :crypto.strong_rand_bytes(:rand.uniform(24))
      assert match?({:ok, _}, Json.decode(input)) == match?({:ok, _}, Jason.decode(input))
    end
  end

  test "trailing garbage after a valid value is an error" do
    assert {:error, _} = Json.decode(~S({"a":1}x))
  end
end
