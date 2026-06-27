defmodule Filo.BatchResult do
  @moduledoc """
  The result of executing a batch, encoded to a Hrana `BatchResult`.

  Per step there is either a `Filo.StmtResult` (the step ran and succeeded), a
  `Filo.Error` (it ran and failed), or `nil` (its condition was false and it was
  skipped) — tracked positionally in `step_results` and `step_errors`.
  """

  alias Filo.{Error, StmtResult}

  @type t :: %__MODULE__{
          step_results: [StmtResult.t() | nil],
          step_errors: [Error.t() | nil]
        }

  defstruct step_results: [], step_errors: []

  @doc "Encodes a `Filo.BatchResult` into a Hrana `BatchResult` map."
  @spec encode(t()) :: map()
  def encode(%__MODULE__{} = result) do
    %{
      "step_results" => Enum.map(result.step_results, &encode_step_result/1),
      "step_errors" => Enum.map(result.step_errors, &encode_step_error/1),
      "replication_index" => nil
    }
  end

  defp encode_step_result(nil), do: nil
  defp encode_step_result(%StmtResult{} = result), do: StmtResult.encode(result)

  defp encode_step_error(nil), do: nil
  defp encode_step_error(%Error{} = error), do: Error.encode(error)
end
