defmodule Filo.StmtResultTest do
  use ExUnit.Case, async: true

  alias Filo.StmtResult

  test "encodes bare column names and rows (native -> Hrana values)" do
    result = %StmtResult{
      cols: ["id", "name"],
      rows: [[1, "alpha"], [2, "beta"]],
      affected_row_count: 0,
      last_insert_rowid: nil,
      rows_read: 2,
      rows_written: 0
    }

    assert StmtResult.encode(result) == %{
             "cols" => [
               %{"name" => "id", "decltype" => nil},
               %{"name" => "name", "decltype" => nil}
             ],
             "rows" => [
               [%{"type" => "integer", "value" => "1"}, %{"type" => "text", "value" => "alpha"}],
               [%{"type" => "integer", "value" => "2"}, %{"type" => "text", "value" => "beta"}]
             ],
             "affected_row_count" => 0,
             "last_insert_rowid" => nil,
             "replication_index" => nil,
             "rows_read" => 2,
             "rows_written" => 0,
             "query_duration_ms" => 0.0
           }
  end

  test "encodes last_insert_rowid as a string (per Hrana i64-as-string)" do
    result = %StmtResult{affected_row_count: 1, last_insert_rowid: 42, rows_written: 1}
    encoded = StmtResult.encode(result)
    assert encoded["last_insert_rowid"] == "42"
    assert encoded["affected_row_count"] == 1
  end

  test "accepts {name, decltype} column maps" do
    result = %StmtResult{cols: [%{name: "id", decltype: "INTEGER"}]}
    assert StmtResult.encode(result)["cols"] == [%{"name" => "id", "decltype" => "INTEGER"}]
  end

  describe "encode/2 with rows: :json (the JSON transports' fast path)" do
    # Rows become a pre-encoded Jason.Fragment holding ONE flattened binary: built where
    # the result lives (the stream/socket process), so the HTTP reply crosses the process
    # boundary as a refc binary instead of a deep list-of-maps-of-maps copy, and Jason
    # never re-walks the row data. The wire JSON must be byte-for-byte-decodable to the
    # same document the :maps form produces.
    test "wire-equivalent to the :maps form" do
      result = %StmtResult{
        cols: ["id", "name", "bin"],
        rows: [
          [1, "alpha", {:blob, <<0, 255>>}],
          [2, "béta", nil],
          [3, 3.14, <<0xFF, 0xFE>>]
        ],
        affected_row_count: 0,
        last_insert_rowid: nil,
        rows_read: 3,
        rows_written: 0
      }

      json = result |> StmtResult.encode(rows: :json) |> Jason.encode!() |> Jason.decode!()
      maps = result |> StmtResult.encode() |> Jason.encode!() |> Jason.decode!()
      assert json == maps
    end

    test "rows are a Jason.Fragment (pre-encoded, spliced by Jason instead of re-walked)" do
      result = %StmtResult{cols: ["v"], rows: List.duplicate(["x"], 100), rows_read: 100}
      assert %Jason.Fragment{} = StmtResult.encode(result, rows: :json)["rows"]
    end

    test "empty rows encode as an empty JSON array" do
      result = %StmtResult{cols: ["v"], rows: []}
      json = result |> StmtResult.encode(rows: :json) |> Jason.encode!() |> Jason.decode!()
      assert json["rows"] == []
    end
  end
end
