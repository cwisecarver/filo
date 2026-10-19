defmodule Filo.StmtResult do
  @moduledoc """
  The result of executing a single statement, encoded to a Hrana `StmtResult`.

  A host executor fills this struct with native Elixir values; `encode/1`
  produces the Hrana wire form — rows as `Filo.Value` maps, `last_insert_rowid`
  as a string (Hrana carries i64 as a string), and the `replication_index` /
  `query_duration_ms` fields the protocol expects.

  `cols` may be bare column-name strings or `%{name: ..., decltype: ...}` maps.
  """

  alias Filo.Value

  @type col :: String.t() | %{name: String.t() | nil, decltype: String.t() | nil}

  @type t :: %__MODULE__{
          cols: [col()],
          rows: [[Value.native()]],
          affected_row_count: non_neg_integer(),
          last_insert_rowid: integer() | nil,
          rows_read: non_neg_integer(),
          rows_written: non_neg_integer(),
          cols_encoded: cols_encoded() | nil
        }

  @typedoc """
  Pre-built wire form of a column list, from `encode_cols/1`: the JSON `"cols"` array as a
  `Jason.Fragment` and the protobuf `StmtResult.cols` fields as a binary.
  """
  @type cols_encoded :: %{json: Jason.Fragment.t(), protobuf: binary()}

  defstruct cols: [],
            rows: [],
            affected_row_count: 0,
            last_insert_rowid: nil,
            rows_read: 0,
            rows_written: 0,
            cols_encoded: nil

  @doc """
  Encodes a `Filo.StmtResult` into a Hrana `StmtResult` map.

  With `rows: :json`, `"rows"` is a `Jason.Fragment` holding ONE pre-encoded, flattened
  binary instead of the list-of-maps-of-tagged-maps: built where the result lives (the
  stream/socket process), so an HTTP reply crosses the process boundary as a refc
  binary reference instead of a deep structural copy, and the outer `Jason.encode`
  splices it without re-walking the row data. JSON transports only.

  With `rows: :protobuf`, `"rows"` is `{:protobuf_rows, binary}`: the rows already
  encoded as protobuf `StmtResult.rows` fields, for the same reason. Only
  `Filo.Protobuf.encode_stmt_result/1` understands it. The default `:maps` gives the
  plain Hrana JSON-term maps.
  """
  @spec encode(t(), keyword()) :: map()
  def encode(%__MODULE__{} = result, opts \\ []) do
    rows_mode = Keyword.get(opts, :rows, :maps)

    %{
      "cols" => encode_cols_for(result, rows_mode),
      "rows" => encode_rows(result.rows, rows_mode),
      "affected_row_count" => result.affected_row_count,
      "last_insert_rowid" => encode_rowid(result.last_insert_rowid),
      "replication_index" => nil,
      "rows_read" => result.rows_read,
      "rows_written" => result.rows_written,
      "query_duration_ms" => 0.0
    }
  end

  # When the host supplied `cols_encoded` (see `encode_cols/1`) the JSON / protobuf modes splice
  # it instead of rebuilding a map per column on every execution (fathom expert review
  # 2026-10-19 #10). The default :maps mode always builds the maps: its consumers read them.
  defp encode_cols_for(%__MODULE__{cols_encoded: %{json: json}}, :json), do: json

  defp encode_cols_for(%__MODULE__{cols_encoded: %{protobuf: pb}}, :protobuf),
    do: {:protobuf_cols, pb}

  defp encode_cols_for(%__MODULE__{cols: cols}, _mode), do: Enum.map(cols, &encode_col/1)

  @doc """
  Pre-builds the wire form of a column list, for a host that executes the same statement
  repeatedly (a statement cache): store the result in `:cols_encoded` and `encode/2` splices it
  in `rows: :json` / `rows: :protobuf` mode instead of re-encoding every column per execution.

  The output is byte-identical to what `encode/2` produces from `cols` alone. The caller owns
  the cache's validity: `cols_encoded` must come from the same `cols` the struct carries
  (rebuild it whenever the statement's columns change).
  """
  @spec encode_cols([col()]) :: cols_encoded()
  def encode_cols(cols) when is_list(cols) do
    maps = Enum.map(cols, &encode_col/1)

    %{
      json: Jason.Fragment.new(IO.iodata_to_binary(Jason.encode_to_iodata!(maps))),
      protobuf: Filo.Protobuf.encode_cols(maps)
    }
  end

  defp encode_rows(rows, :maps),
    do: Enum.map(rows, fn row -> Enum.map(row, &Value.encode/1) end)

  defp encode_rows(rows, :json),
    do: Jason.Fragment.new(IO.iodata_to_binary(rows_iodata(rows)))

  defp encode_rows(rows, :protobuf), do: {:protobuf_rows, Filo.Protobuf.encode_native_rows(rows)}

  @doc false
  # The rows array as final JSON iodata (see encode/2's :json mode and Filo.Cursor).
  @spec rows_iodata([[Value.native()]]) :: iodata()
  def rows_iodata(rows), do: json_array(rows, &row_iodata/1)

  @doc false
  @spec row_iodata([Value.native()]) :: iodata()
  def row_iodata(row), do: json_array(row, &Value.encode_json/1)

  defp json_array([], _fun), do: "[]"
  defp json_array(items, fun), do: [?[, Enum.map_intersperse(items, ?,, fun), ?]]

  defp encode_col(%{name: name, decltype: decltype}),
    do: %{"name" => name, "decltype" => decltype}

  defp encode_col(name) when is_binary(name), do: %{"name" => name, "decltype" => nil}

  defp encode_rowid(nil), do: nil
  defp encode_rowid(id) when is_integer(id), do: Integer.to_string(id)

  @doc """
  Decodes a Hrana `StmtResult` map back into a `Filo.StmtResult` — the inverse of
  `encode/2`, for `Filo.Client`.

  Rows come back as native terms via `Filo.Value.decode/1`, `last_insert_rowid`
  parses from its wire string, and columns keep the `%{name:, decltype:}` shape.
  """
  @spec decode(map()) :: t()
  def decode(%{} = result) do
    %__MODULE__{
      cols: result |> Map.get("cols", []) |> Enum.map(&decode_col/1),
      rows:
        result |> Map.get("rows", []) |> Enum.map(fn row -> Enum.map(row, &Value.decode/1) end),
      affected_row_count: Map.get(result, "affected_row_count", 0),
      last_insert_rowid: decode_rowid(Map.get(result, "last_insert_rowid")),
      rows_read: Map.get(result, "rows_read", 0),
      rows_written: Map.get(result, "rows_written", 0)
    }
  end

  defp decode_col(%{"name" => name, "decltype" => decltype}),
    do: %{name: name, decltype: decltype}

  defp decode_col(%{"name" => name}), do: %{name: name, decltype: nil}

  defp decode_rowid(nil), do: nil
  defp decode_rowid(id) when is_binary(id), do: String.to_integer(id)
  defp decode_rowid(id) when is_integer(id), do: id
end
