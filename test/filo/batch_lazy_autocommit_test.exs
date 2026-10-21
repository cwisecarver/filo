defmodule Filo.BatchLazyAutocommitTest do
  # Pins fathom expert review 2026-10-21 #17 (panel 2 #2): Batch.run read the connection's
  # autocommit flag once per conditioned step, before looking at the condition. Hosts whose
  # read is a real dispatch (fathom: a dirty-IO NIF call) paid it for every ok-chained step
  # (django-libsql executemany, SDK transactional batches) though no condition asked. The
  # read is now lazy: only a reached `is_autocommit` leaf calls it, at most once per step.
  use ExUnit.Case, async: true

  alias Filo.{Batch, BatchCond, Stmt, StmtResult}

  defp counting_ac(value) do
    {:ok, counter} = Agent.start_link(fn -> 0 end)
    {counter, fn -> Agent.update(counter, &(&1 + 1)) && value end}
  end

  defp reads(counter), do: Agent.get(counter, & &1)

  defp ok_exec, do: fn %Stmt{} -> {:ok, %StmtResult{}} end

  defp step(cond, sql \\ "x"), do: %{condition: cond, stmt: %Stmt{sql: sql}}

  test "an ok-chain of N steps reads autocommit zero times (was N-1)" do
    n = 50
    batch = %Batch{steps: [step(nil) | for(i <- 1..(n - 1), do: step({:ok, i - 1}))]}
    {counter, ac} = counting_ac(true)

    result = Batch.run(batch, ok_exec(), ac)

    assert length(result.step_results) == n
    assert Enum.all?(result.step_results, &match?(%StmtResult{}, &1))
    assert reads(counter) == 0
  end

  test "error / not / and / or conditions that never reference is_autocommit read nothing" do
    conds = [
      {:error, 0},
      {:not, {:ok, 0}},
      {:and, [{:ok, 0}, {:not, {:error, 0}}]},
      {:or, [{:error, 0}, {:ok, 0}]}
    ]

    batch = %Batch{steps: [step(nil) | Enum.map(conds, &step/1)]}
    {counter, ac} = counting_ac(true)
    Batch.run(batch, ok_exec(), ac)
    assert reads(counter) == 0
  end

  test "is_autocommit reads once per step, however many leaves reference it" do
    cond = {:and, [:is_autocommit, {:not, {:not, :is_autocommit}}, {:or, [:is_autocommit]}]}
    batch = %Batch{steps: [step(nil), step(cond), step(cond)]}
    {counter, ac} = counting_ac(true)

    result = Batch.run(batch, ok_exec(), ac)

    assert Enum.all?(result.step_results, &match?(%StmtResult{}, &1))
    assert reads(counter) == 2
  end

  test "short-circuit: a false leg before is_autocommit means no read" do
    batch = %Batch{steps: [step(nil), step({:and, [{:error, 0}, :is_autocommit]})]}
    {counter, ac} = counting_ac(true)
    result = Batch.run(batch, ok_exec(), ac)
    assert [%StmtResult{}, nil] = result.step_results
    assert reads(counter) == 0
  end

  describe "BatchCond.eval/3 semantics are unchanged for every condition type" do
    @outcomes %{0 => :ok, 1 => :error, 2 => :skipped}

    for {name, cond, expected_true, expected_false} <- [
          {"ok true", {:ok, 0}, true, true},
          {"ok on error", {:ok, 1}, false, false},
          {"ok on skipped", {:ok, 2}, false, false},
          {"error true", {:error, 1}, true, true},
          {"error on ok", {:error, 0}, false, false},
          {"not", {:not, {:ok, 1}}, true, true},
          {"and empty", {:and, []}, true, true},
          {"or empty", {:or, []}, false, false},
          {"and", {:and, [{:ok, 0}, {:error, 1}]}, true, true},
          {"or", {:or, [{:ok, 1}, {:error, 1}]}, true, true},
          {"is_autocommit", :is_autocommit, true, false},
          {"not is_autocommit", {:not, :is_autocommit}, false, true},
          {"and ok+ac", {:and, [{:ok, 0}, :is_autocommit]}, true, false},
          {"or err+ac", {:or, [{:error, 0}, :is_autocommit]}, true, false}
        ] do
      test "#{name}" do
        for {ac, expected} <- [{true, unquote(expected_true)}, {false, unquote(expected_false)}] do
          # boolean form (public API) and thunk form agree
          assert BatchCond.eval(unquote(Macro.escape(cond)), @outcomes, ac) == expected
          assert BatchCond.eval(unquote(Macro.escape(cond)), @outcomes, fn -> ac end) == expected
        end
      end
    end
  end
end
