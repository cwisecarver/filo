defmodule Filo.StmtTest do
  use ExUnit.Case, async: true

  alias Filo.Stmt

  test "decodes sql with no args; want_rows defaults to true" do
    assert Stmt.decode(%{"sql" => "SELECT 1"}) == %Stmt{sql: "SELECT 1"}
  end

  test "decodes positional args, mapping Hrana values to native terms" do
    stmt = %{
      "sql" => "INSERT INTO t (a, b) VALUES (?, ?)",
      "args" => [%{"type" => "integer", "value" => "7"}, %{"type" => "text", "value" => "x"}],
      "want_rows" => false
    }

    assert Stmt.decode(stmt) == %Stmt{
             sql: "INSERT INTO t (a, b) VALUES (?, ?)",
             args: [7, "x"],
             want_rows: false
           }
  end

  test "decodes named args, preserving the client's parameter name" do
    stmt = %{
      "sql" => "SELECT * FROM t WHERE a = :a",
      "named_args" => [%{"name" => ":a", "value" => %{"type" => "integer", "value" => "5"}}]
    }

    decoded = Stmt.decode(stmt)
    assert decoded.named_args == [{":a", 5}]
    assert decoded.args == []
  end

  test "carries sql_id for stored statements" do
    assert Stmt.decode(%{"sql_id" => 3}).sql_id == 3
  end
end
