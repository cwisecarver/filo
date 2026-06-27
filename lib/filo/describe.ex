defmodule Filo.Describe do
  @moduledoc """
  The result of a Hrana `describe` request: the parameters and columns of a
  prepared statement, plus whether it is an `EXPLAIN` and whether it is
  read-only. The libSQL client uses this to learn a statement's shape (e.g. how
  many parameters to bind) before executing it.

  A host executor fills this struct; `encode/1` produces the Hrana
  `DescribeResult` map. `params` are parameter names (`nil` for an unnamed
  positional `?`). `cols` may be bare column-name strings or
  `%{name: ..., decltype: ...}` maps, matching `Filo.StmtResult`.
  """

  @type param :: String.t() | nil
  @type col :: String.t() | %{name: String.t(), decltype: String.t() | nil}

  @type t :: %__MODULE__{
          params: [param()],
          cols: [col()],
          is_explain: boolean(),
          is_readonly: boolean()
        }

  defstruct params: [], cols: [], is_explain: false, is_readonly: false

  @doc "Encodes a `Filo.Describe` into a Hrana `DescribeResult` map."
  @spec encode(t()) :: map()
  def encode(%__MODULE__{} = describe) do
    %{
      "params" => Enum.map(describe.params, &%{"name" => &1}),
      "cols" => Enum.map(describe.cols, &encode_col/1),
      "is_explain" => describe.is_explain,
      "is_readonly" => describe.is_readonly
    }
  end

  defp encode_col(%{name: name, decltype: decltype}),
    do: %{"name" => name, "decltype" => decltype}

  defp encode_col(name) when is_binary(name), do: %{"name" => name, "decltype" => nil}
end
