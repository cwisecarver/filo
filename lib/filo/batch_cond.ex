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
  run yet) is neither `ok` nor `error`.

  `autocommit?` is the connection's autocommit state: a boolean, or a 0-arity function that
  reads it. A function is called only when an `is_autocommit` leaf is actually reached, and at
  most once per `eval/3` call - the flag can only change when a statement runs, so a step's
  condition sees one value. Hosts whose read is a real dispatch (a NIF call) pay nothing for
  the ok/error chains that never ask.
  """
  @spec eval(t(), outcomes(), boolean() | (-> boolean())) :: boolean()
  def eval(cond, outcomes, autocommit?) do
    {result, _ac} = ev(cond, outcomes, autocommit?)
    result
  end

  # Threads the autocommit slot (a boolean once read, else the thunk) through the tree so a
  # lazily-read flag is memoized for the rest of this evaluation.
  defp ev({:ok, step}, outcomes, ac), do: {outcome_at(outcomes, step) == :ok, ac}
  defp ev({:error, step}, outcomes, ac), do: {outcome_at(outcomes, step) == :error, ac}

  defp ev({:not, cond}, outcomes, ac) do
    {result, ac} = ev(cond, outcomes, ac)
    {not result, ac}
  end

  defp ev({:and, conds}, outcomes, ac), do: ev_all(conds, outcomes, ac)

  defp ev({:or, conds}, outcomes, ac) do
    # any(c) == not all(not c); evaluate left to right, stop at the first true.
    Enum.reduce_while(conds, {false, ac}, fn cond, {_, ac} ->
      case ev(cond, outcomes, ac) do
        {true, ac} -> {:halt, {true, ac}}
        {false, ac} -> {:cont, {false, ac}}
      end
    end)
  end

  defp ev(:is_autocommit, _outcomes, ac) when is_boolean(ac), do: {ac, ac}

  defp ev(:is_autocommit, _outcomes, ac) when is_function(ac, 0) do
    value = ac.()
    {value, value}
  end

  defp ev_all(conds, outcomes, ac) do
    Enum.reduce_while(conds, {true, ac}, fn cond, {_, ac} ->
      case ev(cond, outcomes, ac) do
        {false, ac} -> {:halt, {false, ac}}
        {true, ac} -> {:cont, {true, ac}}
      end
    end)
  end

  defp outcome_at(outcomes, step) when is_map(outcomes), do: Map.get(outcomes, step)
  defp outcome_at(outcomes, step) when is_list(outcomes), do: Enum.at(outcomes, step)
end
