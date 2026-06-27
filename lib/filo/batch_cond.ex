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
  def decode(%{"type" => "ok", "step" => step}), do: {:ok, step}
  def decode(%{"type" => "error", "step" => step}), do: {:error, step}
  def decode(%{"type" => "not", "cond" => cond}), do: {:not, decode(cond)}
  def decode(%{"type" => "and", "conds" => conds}), do: {:and, Enum.map(conds, &decode/1)}
  def decode(%{"type" => "or", "conds" => conds}), do: {:or, Enum.map(conds, &decode/1)}
  def decode(%{"type" => "is_autocommit"}), do: :is_autocommit

  @doc """
  Evaluates a condition. `outcomes` is the list of earlier step outcomes indexed
  by step number; `autocommit?` is the connection's current autocommit state.
  """
  @spec eval(t(), [outcome()], boolean()) :: boolean()
  def eval({:ok, step}, outcomes, _autocommit?), do: Enum.at(outcomes, step) == :ok
  def eval({:error, step}, outcomes, _autocommit?), do: Enum.at(outcomes, step) == :error
  def eval({:not, cond}, outcomes, autocommit?), do: not eval(cond, outcomes, autocommit?)
  def eval({:and, conds}, outcomes, ac?), do: Enum.all?(conds, &eval(&1, outcomes, ac?))
  def eval({:or, conds}, outcomes, ac?), do: Enum.any?(conds, &eval(&1, outcomes, ac?))
  def eval(:is_autocommit, _outcomes, autocommit?), do: autocommit?
end
