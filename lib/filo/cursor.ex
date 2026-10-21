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
    rows =
      Enum.map(result.rows, fn row -> %{"type" => "row", "row" => encode_row(row, row_mode)} end)

    [step_begin(step, result, row_mode) | rows] ++ [step_end(result)]
  end

  defp step_entries(step, nil, %Error{} = error, _row_mode), do: [step_error(step, error)]

  # Skipped step: condition was false, neither result nor error.
  defp step_entries(_step, nil, nil, _row_mode), do: []

  # The JSON modes splice the host's precomputed `cols_encoded` (see `StmtResult.encode_cols/1`)
  # instead of rebuilding a map per column on every execution, as `StmtResult.encode/2` does;
  # :maps keeps the maps, which the protobuf cursor encoder reads.
  defp step_begin(step, %StmtResult{cols_encoded: %{json: json}}, mode)
       when mode in [:json, :json_iodata],
       do: %{"type" => "step_begin", "step" => step, "cols" => json}

  defp step_begin(step, %StmtResult{cols: cols}, _mode),
    do: %{"type" => "step_begin", "step" => step, "cols" => Enum.map(cols, &encode_col/1)}

  defp step_end(%StmtResult{} = result) do
    %{
      "type" => "step_end",
      "affected_row_count" => result.affected_row_count,
      "last_insert_rowid" => encode_rowid(result.last_insert_rowid)
    }
  end

  defp step_error(step, %Error{} = error),
    do: %{"type" => "step_error", "step" => step, "error" => Error.encode(error)}

  defp encode_row(row, :maps), do: Enum.map(row, &Value.encode/1)

  defp encode_row(row, :json),
    do: Jason.Fragment.new(IO.iodata_to_binary(StmtResult.row_iodata(row)))

  defp encode_row(row, :json_iodata), do: Jason.Fragment.new(StmtResult.row_iodata(row))

  defp encode_col(%{name: name, decltype: decltype}),
    do: %{"name" => name, "decltype" => decltype}

  defp encode_col(name) when is_binary(name), do: %{"name" => name, "decltype" => nil}

  defp encode_rowid(nil), do: nil
  defp encode_rowid(id) when is_integer(id), do: Integer.to_string(id)

  @row_prefix ~s({"row":)
  @row_suffix ~s(,"type":"row"}\n)

  @doc """
  The JSON cursor body as newline-delimited lines, flattened into binaries of roughly
  `chunk_bytes` each - the same bytes `Jason.encode_to_iodata!/1` of each `entries/2` item plus
  `"\n"` produces, without the per-row map, `Jason.Fragment` and Jason pass.

  Built where the result lives (the stream process), so the reply crosses to the HTTP process as
  a few refc binaries instead of a structural copy of the native rows, and the plug writes each
  one straight to the transport. A row line is framed as `{"row":ROW,"type":"row"}`: Jason emits
  map keys in term order and `"row" < "type"`; a test pins the bytes against the map form.
  Chunk boundaries are invisible to the ndjson consumer; a line is never split.
  """
  @spec json_chunks(BatchResult.t(), pos_integer()) :: [binary()]
  def json_chunks(%BatchResult{step_results: results, step_errors: errors}, chunk_bytes) do
    acc =
      results
      |> Enum.zip(errors)
      |> Enum.with_index()
      |> Enum.reduce({[], [], 0}, fn {{result, error}, step}, acc ->
        json_step(step, result, error, chunk_bytes, acc)
      end)

    {chunks, buf, size} = acc
    chunks = if size == 0, do: chunks, else: [IO.iodata_to_binary(buf) | chunks]
    Enum.reverse(chunks)
  end

  defp json_step(step, %StmtResult{} = result, _error, limit, acc) do
    acc = json_line(step_begin(step, result, :json_iodata), limit, acc)

    acc =
      Enum.reduce(result.rows, acc, fn row, acc ->
        line = [@row_prefix, StmtResult.row_iodata(row), @row_suffix]
        push(line, IO.iodata_length(line), limit, acc)
      end)

    json_line(step_end(result), limit, acc)
  end

  defp json_step(step, nil, %Error{} = error, limit, acc),
    do: json_line(step_error(step, error), limit, acc)

  defp json_step(_step, nil, nil, _limit, acc), do: acc

  defp json_line(entry, limit, acc) do
    line = [Jason.encode_to_iodata!(entry), ?\n]
    push(line, IO.iodata_length(line), limit, acc)
  end

  # Close the chunk before a line that would push it to the limit (never splits a line).
  defp push(line, line_size, limit, {chunks, buf, size}) do
    if size > 0 and size + line_size >= limit,
      do: {[IO.iodata_to_binary(buf) | chunks], [line], line_size},
      else: {chunks, [buf, line], size + line_size}
  end
end
