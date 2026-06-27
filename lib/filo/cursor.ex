defmodule Filo.Cursor do
  @moduledoc """
  Turns a `Filo.BatchResult` into the sequence of Hrana cursor entries.

  A cursor encodes the same information as a `BatchResult`, but as a stream of
  per-step entries so a large result need not be held whole in memory:

    - an executed step → a `step_begin` (with its columns), one `row` per row,
      then a `step_end` (affected rows + last insert rowid);
    - a failed step → a `step_error`;
    - a skipped step (its condition was false) → no entry.

  `entries/1` materializes the full list from a completed batch; the connection
  executor returns whole `StmtResult`s, so true row-streaming would require a
  streaming executor callback. The entry *sequence* matches the spec exactly.
  """

  alias Filo.{BatchResult, Error, StmtResult, Value}

  @doc "Builds the ordered list of Hrana cursor entries for a batch result."
  @spec entries(BatchResult.t()) :: [map()]
  def entries(%BatchResult{step_results: results, step_errors: errors}) do
    results
    |> Enum.zip(errors)
    |> Enum.with_index()
    |> Enum.flat_map(fn {{result, error}, step} -> step_entries(step, result, error) end)
  end

  defp step_entries(step, %StmtResult{} = result, _error) do
    begin = %{
      "type" => "step_begin",
      "step" => step,
      "cols" => Enum.map(result.cols, &encode_col/1)
    }

    rows =
      Enum.map(result.rows, fn row ->
        %{"type" => "row", "row" => Enum.map(row, &Value.encode/1)}
      end)

    step_end = %{
      "type" => "step_end",
      "affected_row_count" => result.affected_row_count,
      "last_insert_rowid" => encode_rowid(result.last_insert_rowid)
    }

    [begin | rows] ++ [step_end]
  end

  defp step_entries(step, nil, %Error{} = error) do
    [%{"type" => "step_error", "step" => step, "error" => Error.encode(error)}]
  end

  # Skipped step: condition was false, neither result nor error.
  defp step_entries(_step, nil, nil), do: []

  defp encode_col(%{name: name, decltype: decltype}),
    do: %{"name" => name, "decltype" => decltype}

  defp encode_col(name) when is_binary(name), do: %{"name" => name, "decltype" => nil}

  defp encode_rowid(nil), do: nil
  defp encode_rowid(id) when is_integer(id), do: Integer.to_string(id)
end
