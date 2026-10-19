defmodule Filo.BatchCond do
  @moduledoc """
  Hrana batch step conditions.

  Each batch step may carry a condition gating whether it runs, expressed over
  the outcomes of *earlier* steps and the connection's autocommit state. This is
  how libsql clients build transactional batches: `BEGIN`, then statements
  guarded by `ok` of the previous step, a `COMMIT` guarded by `ok` of the last
  statement, and a `ROLLBACK` guarded by `not ok` of the commit.

  Decoded form (tagged terms):

      {:ok, step} | {:error, step} | {:not, cond} | {:and, [cond]} |
      {:or, [cond]} | :is_autocommit
  """

  @type t ::
          {:ok, non_neg_integer()}
          | {:error, non_neg_integer()}
          | {:not, t()}
          | {:and, [t()]}
          | {:or, [t()]}
          | :is_autocommit

  @type outcome :: :ok | :error | :skipped

  @doc "Decodes a Hrana `BatchCond` map into a tagged condition term."
  @spec decode(map()) :: t()
  # `step` is validated here, not at `eval/3`: a non-integer index would otherwise raise in the
  # middle of a running batch (after earlier steps already executed).
  def decode(%{"type" => "ok", "step" => step}) when is_integer(step) and step >= 0,
    do: {:ok, step}

  def decode(%{"type" => "error", "step" => step}) when is_integer(step) and step >= 0,
    do: {:error, step}

  def decode(%{"type" => "not", "cond" => cond}), do: {:not, decode(cond)}
  def decode(%{"type" => "and", "conds" => conds}), do: {:and, Enum.map(conds, &decode/1)}
  def decode(%{"type" => "or", "conds" => conds}), do: {:or, Enum.map(conds, &decode/1)}
  def decode(%{"type" => "is_autocommit"}), do: :is_autocommit

  @typedoc """
  Earlier step outcomes: a list indexed by step number, or a map of step number to outcome.
  `Filo.Batch.run/3` passes the map, so a prior-step lookup is O(1) rather than the list's
  O(step) walk (fathom expert review 2026-10-19 #13: N conditioned steps were O(N^2)).
  """
  @type outcomes :: [outcome()] | %{optional(non_neg_integer()) => outcome()}

  @doc """
  Evaluates a condition. `outcomes` holds the earlier step outcomes (a list indexed by step
  number, or a step-number => outcome map); a step with no recorded outcome (one that has not
  run yet) is neither `ok` nor `error`. `autocommit?` is the connection's current autocommit
  state.
  """
  @spec eval(t(), outcomes(), boolean()) :: boolean()
  def eval({:ok, step}, outcomes, _autocommit?), do: outcome_at(outcomes, step) == :ok
  def eval({:error, step}, outcomes, _autocommit?), do: outcome_at(outcomes, step) == :error
  def eval({:not, cond}, outcomes, autocommit?), do: not eval(cond, outcomes, autocommit?)
  def eval({:and, conds}, outcomes, ac?), do: Enum.all?(conds, &eval(&1, outcomes, ac?))
  def eval({:or, conds}, outcomes, ac?), do: Enum.any?(conds, &eval(&1, outcomes, ac?))
  def eval(:is_autocommit, _outcomes, autocommit?), do: autocommit?

  defp outcome_at(outcomes, step) when is_map(outcomes), do: Map.get(outcomes, step)
  defp outcome_at(outcomes, step) when is_list(outcomes), do: Enum.at(outcomes, step)
end
