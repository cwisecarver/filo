defmodule Filo.Batch do
  @moduledoc """
  A decoded Hrana batch: an ordered list of steps, each a statement with an
  optional condition. `run/3` executes the steps in order, gating each on its
  condition, and collects a `Filo.BatchResult`.
  """

  alias Filo.{BatchCond, BatchResult, Error, Stmt, StmtResult}

  @type step :: %{condition: BatchCond.t() | nil, stmt: Stmt.t()}
  @type t :: %__MODULE__{steps: [step()]}

  defstruct steps: []

  @typedoc "Runs one statement against the underlying connection."
  @type exec_fun :: (Stmt.t() -> {:ok, StmtResult.t()} | {:error, Error.t()})

  @typedoc "Reports the connection's current autocommit state."
  @type autocommit_fun :: (-> boolean())

  @doc "Decodes a Hrana `Batch` map into a `Filo.Batch` struct."
  @spec decode(map()) :: t()
  def decode(%{"steps" => steps}) do
    %__MODULE__{steps: Enum.map(steps, &decode_step/1)}
  end

  defp decode_step(%{"stmt" => stmt} = step) do
    %{condition: decode_condition(Map.get(step, "condition")), stmt: Stmt.decode(stmt)}
  end

  defp decode_condition(nil), do: nil
  defp decode_condition(cond), do: BatchCond.decode(cond)

  @doc """
  Runs each step in order, gated by its condition, returning a `Filo.BatchResult`.

  `exec_fun` runs one statement; `autocommit_fun` reports the connection's current
  autocommit state, consulted by any `is_autocommit` conditions. A step whose
  condition is false is skipped — `nil` in both result and error positions.
  """
  @spec run(t(), exec_fun(), autocommit_fun()) :: BatchResult.t()
  def run(%__MODULE__{steps: steps}, exec_fun, autocommit_fun) do
    {results, errors, _outcomes} =
      Enum.reduce(steps, {[], [], []}, fn %{condition: condition, stmt: stmt},
                                          {results, errors, outcomes} ->
        run? =
          condition == nil or
            BatchCond.eval(condition, Enum.reverse(outcomes), autocommit_fun.())

        {result, error, outcome} =
          if run? do
            case exec_fun.(stmt) do
              {:ok, stmt_result} -> {stmt_result, nil, :ok}
              {:error, err} -> {nil, err, :error}
            end
          else
            {nil, nil, :skipped}
          end

        {[result | results], [error | errors], [outcome | outcomes]}
      end)

    %BatchResult{step_results: Enum.reverse(results), step_errors: Enum.reverse(errors)}
  end
end
