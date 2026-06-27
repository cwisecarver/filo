defmodule Filo.CursorTest do
  use ExUnit.Case, async: true

  alias Filo.{BatchResult, Cursor, Error, StmtResult}

  test "an executed step encodes as step_begin, rows, step_end" do
    result = %BatchResult{
      step_results: [%StmtResult{cols: ["x"], rows: [[1], [2]]}],
      step_errors: [nil]
    }

    assert Cursor.entries(result) == [
             %{
               "type" => "step_begin",
               "step" => 0,
               "cols" => [%{"name" => "x", "decltype" => nil}]
             },
             %{"type" => "row", "row" => [%{"type" => "integer", "value" => "1"}]},
             %{"type" => "row", "row" => [%{"type" => "integer", "value" => "2"}]},
             %{"type" => "step_end", "affected_row_count" => 0, "last_insert_rowid" => nil}
           ]
  end

  test "step_end carries affected_row_count and the rowid as a string" do
    result = %BatchResult{
      step_results: [%StmtResult{affected_row_count: 1, last_insert_rowid: 5}],
      step_errors: [nil]
    }

    assert List.last(Cursor.entries(result)) == %{
             "type" => "step_end",
             "affected_row_count" => 1,
             "last_insert_rowid" => "5"
           }
  end

  test "a failed step encodes as step_error with its index" do
    result = %BatchResult{
      step_results: [nil],
      step_errors: [%Error{message: "boom", code: "SQLITE_ERROR"}]
    }

    assert Cursor.entries(result) == [
             %{
               "type" => "step_error",
               "step" => 0,
               "error" => %{"message" => "boom", "code" => "SQLITE_ERROR"}
             }
           ]
  end

  test "a skipped step (no result, no error) produces no entry" do
    result = %BatchResult{
      step_results: [nil, %StmtResult{cols: ["n"], rows: [[7]]}],
      step_errors: [nil, nil]
    }

    # step 0 skipped entirely; step 1 begins at index 1
    assert [%{"type" => "step_begin", "step" => 1} | _] = Cursor.entries(result)
    refute Enum.any?(Cursor.entries(result), &(&1["step"] == 0))
  end
end
