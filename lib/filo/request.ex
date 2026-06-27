defmodule Filo.Request do
  @moduledoc """
  Dispatch of a single Hrana stream request against a host `Filo.Executor`.

  `handle/3` runs one decoded request map and returns `{status, stream_result}`,
  where `status` is `:open` (the stream continues) or `:closed` (a `close`
  request released the connection), and `stream_result` is the Hrana
  `StreamResult` map — `%{"type" => "ok", ...}` or `%{"type" => "error", ...}`.
  """

  alias Filo.{Batch, BatchResult, Describe, Error, Stmt, StmtResult}

  @type status :: :open | :closed

  @doc "Runs one decoded Hrana stream request through `executor`."
  @spec handle(module(), Filo.Executor.conn(), map()) :: {status(), map()}
  def handle(executor, conn, %{"type" => "execute", "stmt" => stmt}) do
    case executor.execute(conn, Stmt.decode(stmt)) do
      {:ok, result} -> {:open, ok(%{"type" => "execute", "result" => StmtResult.encode(result)})}
      {:error, %Error{} = error} -> {:open, stream_error(error)}
    end
  end

  def handle(executor, conn, %{"type" => "batch", "batch" => batch}) do
    result =
      batch
      |> Batch.decode()
      |> Batch.run(&executor.execute(conn, &1), fn -> executor.autocommit?(conn) end)

    {:open, ok(%{"type" => "batch", "result" => BatchResult.encode(result)})}
  end

  def handle(executor, conn, %{"type" => "describe", "sql" => sql}) when is_binary(sql) do
    if function_exported?(executor, :describe, 2) do
      case executor.describe(conn, sql) do
        {:ok, %Describe{} = describe} ->
          {:open, ok(%{"type" => "describe", "result" => Describe.encode(describe)})}

        {:error, %Error{} = error} ->
          {:open, stream_error(error)}
      end
    else
      {:open,
       stream_error(%Error{message: "describe is not supported", code: "FILO_UNSUPPORTED"})}
    end
  end

  def handle(executor, conn, %{"type" => "get_autocommit"}) do
    {:open, ok(%{"type" => "get_autocommit", "is_autocommit" => executor.autocommit?(conn)})}
  end

  def handle(executor, conn, %{"type" => "close"}) do
    :ok = executor.close(conn)
    {:closed, ok(%{"type" => "close"})}
  end

  def handle(_executor, _conn, %{"type" => type}) do
    {:open,
     stream_error(%Error{message: "unsupported request type: #{type}", code: "FILO_UNSUPPORTED"})}
  end

  defp ok(response), do: %{"type" => "ok", "response" => response}
  defp stream_error(%Error{} = error), do: %{"type" => "error", "error" => Error.encode(error)}
end
