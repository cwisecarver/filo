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
end
