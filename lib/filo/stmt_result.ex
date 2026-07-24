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
          rows_written: non_neg_integer()
        }

  defstruct cols: [],
            rows: [],
            affected_row_count: 0,
            last_insert_rowid: nil,
            rows_read: 0,
            rows_written: 0

  @doc """
  Encodes a `Filo.StmtResult` into a Hrana `StmtResult` map.

  With `rows: :json`, `"rows"` is a `Jason.Fragment` holding ONE pre-encoded, flattened
  binary instead of the list-of-maps-of-tagged-maps: built where the result lives (the
  stream/socket process), so an HTTP reply crosses the process boundary as a refc
  binary reference instead of a deep structural copy, and the outer `Jason.encode`
  splices it without re-walking the row data. JSON transports only — the protobuf
  paths keep the default `:maps` form they can traverse.
  """
  @spec encode(t(), keyword()) :: map()
  def encode(%__MODULE__{} = result, opts \\ []) do
    %{
      "cols" => Enum.map(result.cols, &encode_col/1),
      "rows" => encode_rows(result.rows, Keyword.get(opts, :rows, :maps)),
      "affected_row_count" => result.affected_row_count,
      "last_insert_rowid" => encode_rowid(result.last_insert_rowid),
      "replication_index" => nil,
      "rows_read" => result.rows_read,
      "rows_written" => result.rows_written,
      "query_duration_ms" => 0.0
    }
  end

  defp encode_rows(rows, :maps),
    do: Enum.map(rows, fn row -> Enum.map(row, &Value.encode/1) end)

  defp encode_rows(rows, :json),
    do: Jason.Fragment.new(IO.iodata_to_binary(rows_iodata(rows)))

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
