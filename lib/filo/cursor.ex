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

  @doc """
  Builds the ordered list of Hrana cursor entries for a batch result.

  With `rows: :json`, each `row` entry's payload is a pre-encoded `Jason.Fragment`
  (the JSON cursor endpoint's fast path — one line per entry, so per-row tagged maps
  plus a Jason re-walk dominated the cursor's CPU); the protobuf cursor keeps the
  default `:maps` form it can traverse.
  """
  @spec entries(BatchResult.t(), keyword()) :: [map()]
  def entries(%BatchResult{step_results: results, step_errors: errors}, opts \\ []) do
    row_mode = Keyword.get(opts, :rows, :maps)

    results
    |> Enum.zip(errors)
    |> Enum.with_index()
    |> Enum.flat_map(fn {{result, error}, step} ->
      step_entries(step, result, error, row_mode)
    end)
  end

  defp step_entries(step, %StmtResult{} = result, _error, row_mode) do
    begin = %{
      "type" => "step_begin",
      "step" => step,
      "cols" => Enum.map(result.cols, &encode_col/1)
    }

    rows =
      Enum.map(result.rows, fn row -> %{"type" => "row", "row" => encode_row(row, row_mode)} end)

    step_end = %{
      "type" => "step_end",
      "affected_row_count" => result.affected_row_count,
      "last_insert_rowid" => encode_rowid(result.last_insert_rowid)
    }

    [begin | rows] ++ [step_end]
  end

  defp step_entries(step, nil, %Error{} = error, _row_mode) do
    [%{"type" => "step_error", "step" => step, "error" => Error.encode(error)}]
  end

  # Skipped step: condition was false, neither result nor error.
  defp step_entries(_step, nil, nil, _row_mode), do: []

  defp encode_row(row, :maps), do: Enum.map(row, &Value.encode/1)

  defp encode_row(row, :json),
    do: Jason.Fragment.new(IO.iodata_to_binary(StmtResult.row_iodata(row)))

  defp encode_col(%{name: name, decltype: decltype}),
    do: %{"name" => name, "decltype" => decltype}

  defp encode_col(name) when is_binary(name), do: %{"name" => name, "decltype" => nil}

  defp encode_rowid(nil), do: nil
  defp encode_rowid(id) when is_integer(id), do: Integer.to_string(id)
end
