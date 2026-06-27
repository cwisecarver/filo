defmodule Filo.Stmt do
  @moduledoc """
  A decoded Hrana statement.

  Hrana statements carry SQL (or a `sql_id` referencing a previously stored
  statement), positional `args`, `named_args`, and a `want_rows` flag. Argument
  values are decoded from Hrana into native terms via `Filo.Value`; named
  arguments keep the client's parameter name verbatim (e.g. `":a"`) so the host
  executor can bind them however its engine expects.
  """

  alias Filo.Value

  @type t :: %__MODULE__{
          sql: String.t() | nil,
          sql_id: integer() | nil,
          args: [Value.native()],
          named_args: [{String.t(), Value.native()}],
          want_rows: boolean()
        }

  defstruct sql: nil, sql_id: nil, args: [], named_args: [], want_rows: true

  @doc "Decodes a Hrana `Stmt` map into a `Filo.Stmt` struct."
  @spec decode(map()) :: t()
  def decode(%{} = stmt) do
    %__MODULE__{
      sql: Map.get(stmt, "sql"),
      sql_id: Map.get(stmt, "sql_id"),
      args: stmt |> Map.get("args", []) |> Enum.map(&Value.decode/1),
      named_args: stmt |> Map.get("named_args", []) |> Enum.map(&decode_named_arg/1),
      want_rows: Map.get(stmt, "want_rows", true)
    }
  end

  defp decode_named_arg(%{"name" => name, "value" => value}), do: {name, Value.decode(value)}
end
