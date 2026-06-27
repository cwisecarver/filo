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

  @doc "Encodes a `Filo.StmtResult` into a Hrana `StmtResult` map."
  @spec encode(t()) :: map()
  def encode(%__MODULE__{} = result) do
    %{
      "cols" => Enum.map(result.cols, &encode_col/1),
      "rows" => Enum.map(result.rows, fn row -> Enum.map(row, &Value.encode/1) end),
      "affected_row_count" => result.affected_row_count,
      "last_insert_rowid" => encode_rowid(result.last_insert_rowid),
      "replication_index" => nil,
      "rows_read" => result.rows_read,
      "rows_written" => result.rows_written,
      "query_duration_ms" => 0.0
    }
  end

  defp encode_col(%{name: name, decltype: decltype}),
    do: %{"name" => name, "decltype" => decltype}

  defp encode_col(name) when is_binary(name), do: %{"name" => name, "decltype" => nil}

  defp encode_rowid(nil), do: nil
  defp encode_rowid(id) when is_integer(id), do: Integer.to_string(id)
end
