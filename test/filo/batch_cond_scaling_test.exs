defmodule Filo.BatchCondScalingTest do
  # Pins fathom expert review 2026-10-19 #13: conditioned batch evaluation was O(N^2)
  # (Enum.reverse(outcomes) per step + Enum.at(outcomes, step)). The new map-indexed outcomes
  # must give byte-identical results and a linear cost.
  use ExUnit.Case, async: true

  alias Filo.{Batch, BatchCond, BatchResult, Error, Stmt, StmtResult}

  # The pre-fix implementation, verbatim, as the oracle.
  defp reference_run(%Batch{steps: steps}, exec_fun, autocommit_fun) do
    {results, errors, _} =
      Enum.reduce(steps, {[], [], []}, fn %{condition: condition, stmt: stmt},
                                          {results, errors, outcomes} ->
        run? =
          condition == nil or
            BatchCond.eval(condition, Enum.reverse(outcomes), autocommit_fun.())

        {result, error, outcome} =
          if run? do
            case exec_fun.(stmt) do
              {:ok, r} -> {r, nil, :ok}
              {:error, e} -> {nil, e, :error}
            end
          else
            {nil, nil, :skipped}
          end

        {[result | results], [error | errors], [outcome | outcomes]}
      end)

    %BatchResult{step_results: Enum.reverse(results), step_errors: Enum.reverse(errors)}
  end

  # Random condition over steps 0..(i+2): includes references to the current/future steps,
  # which have no outcome yet and must evaluate neither ok nor error.
  defp rand_cond(i, depth) do
    case :rand.uniform(if depth > 3, do: 3, else: 7) do
      1 -> {:ok, :rand.uniform(i + 3) - 1}
      2 -> {:error, :rand.uniform(i + 3) - 1}
      3 -> :is_autocommit
      4 -> {:not, rand_cond(i, depth + 1)}
      5 -> {:and, for(_ <- 1..:rand.uniform(3), do: rand_cond(i, depth + 1))}
      6 -> {:or, for(_ <- 1..:rand.uniform(3), do: rand_cond(i, depth + 1))}
      7 -> {:and, []}
    end
  end

  test "map-indexed outcomes give results identical to the old reversed-list evaluation" do
    :rand.seed(:exsss, {13, 2026, 1019})

    for _ <- 1..300 do
      n = :rand.uniform(40)

      steps =
        for i <- 0..(n - 1) do
          cond = if :rand.uniform(4) == 1, do: nil, else: rand_cond(i, 0)
          %{condition: cond, stmt: %Stmt{sql: "s#{i}"}}
        end

      batch = %Batch{steps: steps}
      # Deterministic per-statement success/failure keyed by its sql.
      fail = MapSet.new(for i <- 0..(n - 1), :rand.uniform(3) == 1, do: "s#{i}")

      exec = fn %Stmt{sql: sql} ->
        if MapSet.member?(fail, sql),
          do: {:error, %Error{message: sql, code: "E"}},
          else: {:ok, %StmtResult{affected_row_count: byte_size(sql)}}
      end

      ac? = :rand.uniform(2) == 1
      assert Batch.run(batch, exec, fn -> ac? end) == reference_run(batch, exec, fn -> ac? end)
    end
  end

  test "BatchCond.eval/3 still accepts a plain outcomes list" do
    assert BatchCond.eval({:ok, 1}, [:ok, :ok], true)
    assert BatchCond.eval({:error, 0}, [:error], true)
    refute BatchCond.eval({:ok, 5}, [:ok], true)
    refute BatchCond.eval({:error, 5}, %{0 => :ok}, true)
  end

  defp chain(n) do
    steps =
      for i <- 0..(n - 1),
          do: %{condition: if(i == 0, do: nil, else: {:ok, i - 1}), stmt: %Stmt{sql: "x"}}

    %Batch{steps: steps}
  end

  defp best_us(batch, runs) do
    exec = fn _ -> {:ok, %StmtResult{}} end

    1..runs
    |> Enum.map(fn _ ->
      {t, _} = :timer.tc(fn -> Batch.run(batch, exec, fn -> true end) end)
      t
    end)
    |> Enum.min()
  end

  test "conditioned-batch cost grows linearly: 4x the steps is nowhere near 16x the time" do
    small = best_us(chain(2_500), 5)
    large = best_us(chain(10_000), 5)
    # Linear => ~4x; the quadratic code measured ~16x (31 ms at 5k, 530 ms at 20k).
    assert large < small * 10,
           "4x steps took #{large / small}x as long (#{small} us -> #{large} us): quadratic again?"
  end
end
