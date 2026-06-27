defmodule Filo.BatchTest do
  use ExUnit.Case, async: true

  alias Filo.{Batch, BatchResult, Error, Stmt, StmtResult}

  test "decode/1 builds steps with optional conditions and decoded statements" do
    json = %{
      "steps" => [
        %{"stmt" => %{"sql" => "BEGIN"}},
        %{"condition" => %{"type" => "ok", "step" => 0}, "stmt" => %{"sql" => "INSERT"}}
      ]
    }

    assert Batch.decode(json) == %Batch{
             steps: [
               %{condition: nil, stmt: %Stmt{sql: "BEGIN"}},
               %{condition: {:ok, 0}, stmt: %Stmt{sql: "INSERT"}}
             ]
           }
  end

  describe "run/3 — condition-gated execution" do
    test "runs every step when conditions hold; collects results, no errors" do
      batch = %Batch{
        steps: [
          %{condition: nil, stmt: %Stmt{sql: "BEGIN"}},
          %{condition: {:ok, 0}, stmt: %Stmt{sql: "INSERT"}},
          %{condition: {:ok, 1}, stmt: %Stmt{sql: "COMMIT"}}
        ]
      }

      exec = fn _stmt -> {:ok, %StmtResult{affected_row_count: 1}} end
      result = Batch.run(batch, exec, fn -> true end)

      assert length(result.step_results) == 3
      assert Enum.all?(result.step_results, &match?(%StmtResult{}, &1))
      assert result.step_errors == [nil, nil, nil]
    end

    test "a failing step skips later ok-conditioned steps and fires the rollback" do
      # The transactional pattern libsql-client emits:
      #   BEGIN | stmt(ok 0) | COMMIT(ok 1) | ROLLBACK(not ok 2)
      batch = %Batch{
        steps: [
          %{condition: nil, stmt: %Stmt{sql: "BEGIN"}},
          %{condition: {:ok, 0}, stmt: %Stmt{sql: "BOOM"}},
          %{condition: {:ok, 1}, stmt: %Stmt{sql: "COMMIT"}},
          %{condition: {:not, {:ok, 2}}, stmt: %Stmt{sql: "ROLLBACK"}}
        ]
      }

      exec = fn
        %Stmt{sql: "BOOM"} -> {:error, %Error{message: "boom", code: "SQLITE_ERROR"}}
        _ -> {:ok, %StmtResult{}}
      end

      result = Batch.run(batch, exec, fn -> true end)

      assert match?(%StmtResult{}, Enum.at(result.step_results, 0))
      assert Enum.at(result.step_results, 1) == nil
      assert %Error{message: "boom"} = Enum.at(result.step_errors, 1)
      # COMMIT (ok 1) is skipped because step 1 errored
      assert Enum.at(result.step_results, 2) == nil
      assert Enum.at(result.step_errors, 2) == nil
      # ROLLBACK (not ok 2) runs because COMMIT did not succeed
      assert match?(%StmtResult{}, Enum.at(result.step_results, 3))
    end
  end

  test "BatchResult.encode/1 produces the Hrana shape" do
    result = %BatchResult{
      step_results: [%StmtResult{affected_row_count: 1, rows_written: 1}, nil],
      step_errors: [nil, %Error{message: "boom", code: "SQLITE_ERROR"}]
    }

    encoded = BatchResult.encode(result)

    assert [%{"affected_row_count" => 1} | _] = encoded["step_results"]
    assert Enum.at(encoded["step_results"], 1) == nil
    assert Enum.at(encoded["step_errors"], 0) == nil
    assert Enum.at(encoded["step_errors"], 1) == %{"message" => "boom", "code" => "SQLITE_ERROR"}
    assert encoded["replication_index"] == nil
  end
end
