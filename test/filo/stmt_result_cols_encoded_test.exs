defmodule Filo.StmtResultColsEncodedTest do
  # fathom expert review 2026-10-19 #10: a host may precompute the wire form of a statement's
  # columns (StmtResult.encode_cols/1) and set it as :cols_encoded; encode/2 splices it in the
  # :json / :protobuf modes. The output must be byte-identical to the rebuild-per-call path.
  use ExUnit.Case, async: true

  alias Filo.{Protobuf, StmtResult}

  @col_sets [
    {"0 cols", []},
    {"1 col, decltype nil", [%{name: "a", decltype: nil}]},
    {"1 col, bare name", ["a"]},
    {"1 col, decltype set", [%{name: "a", decltype: "INTEGER"}]},
    {"8 cols mixed",
     for(i <- 1..8, do: %{name: "col_#{i}", decltype: if(rem(i, 2) == 0, do: "TEXT")})},
    {"nil name", [%{name: nil, decltype: nil}, %{name: "bé", decltype: "T\"X"}]}
  ]

  for {label, cols} <- @col_sets do
    test "byte-identical JSON and protobuf with and without cols_encoded: #{label}" do
      cols = unquote(Macro.escape(cols))
      n = length(cols)
      rows = if n == 0, do: [], else: [Enum.to_list(1..n), List.duplicate("x", n)]
      plain = %StmtResult{cols: cols, rows: rows, affected_row_count: 2, last_insert_rowid: 7}
      pre = %{plain | cols_encoded: StmtResult.encode_cols(cols)}

      assert Jason.encode!(StmtResult.encode(pre, rows: :json)) ==
               Jason.encode!(StmtResult.encode(plain, rows: :json))

      assert IO.iodata_to_binary(
               Protobuf.encode_stmt_result(StmtResult.encode(pre, rows: :protobuf))
             ) ==
               IO.iodata_to_binary(
                 Protobuf.encode_stmt_result(StmtResult.encode(plain, rows: :protobuf))
               )

      # :maps mode ignores the precomputed field and still returns plain maps.
      assert StmtResult.encode(pre) == StmtResult.encode(plain)
      assert is_list(StmtResult.encode(pre)["cols"])
    end
  end

  test "the precomputed JSON round-trips through the decoder" do
    cols = [%{name: "a", decltype: "INTEGER"}, "b"]
    r = %StmtResult{cols: cols, cols_encoded: StmtResult.encode_cols(cols)}
    decoded = r |> StmtResult.encode(rows: :json) |> Jason.encode!() |> Jason.decode!()

    assert decoded["cols"] == [
             %{"name" => "a", "decltype" => "INTEGER"},
             %{"name" => "b", "decltype" => nil}
           ]
  end

  test "the precomputed field is what gets spliced (json/protobuf modes), not rebuilt from cols" do
    r = %StmtResult{cols: ["real"], cols_encoded: StmtResult.encode_cols(["cached"])}
    assert %Jason.Fragment{} = StmtResult.encode(r, rows: :json)["cols"]
    assert Jason.encode!(StmtResult.encode(r, rows: :json)["cols"]) =~ "cached"
    assert {:protobuf_cols, _} = StmtResult.encode(r, rows: :protobuf)["cols"]
    assert [%{"name" => "real"}] = StmtResult.encode(r)["cols"]
  end

  test "a struct without cols_encoded (default nil) is unchanged" do
    assert %StmtResult{}.cols_encoded == nil
  end
end
