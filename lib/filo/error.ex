defmodule Filo.Error do
  @moduledoc """
  A Hrana error: a human-readable `message` and a SQLite error `code`
  (e.g. `"SQLITE_CONSTRAINT"`).
  """

  @type t :: %__MODULE__{message: String.t(), code: String.t() | nil}

  defstruct message: "", code: nil

  @doc "Encodes a `Filo.Error` into a Hrana `Error` map."
  @spec encode(t()) :: map()
  def encode(%__MODULE__{} = error) do
    %{"message" => error.message, "code" => error.code}
  end
end
