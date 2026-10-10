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

  @doc """
  Runs one decoded Hrana stream request through `executor`.

  `opts` pass through to the result encoders — `rows: :json` makes execute/batch rows a
  pre-encoded `Jason.Fragment` (the JSON transports' fast path); the default `:maps`
  form is what the protobuf transports traverse.
  """
  @spec handle(module(), Filo.Executor.conn(), map(), keyword()) :: {status(), map()}
  def handle(executor, conn, request, opts \\ [])

  def handle(executor, conn, %{"type" => "execute", "stmt" => stmt}, opts) do
    with {:ok, decoded} <- Error.guard(fn -> Stmt.decode(stmt) end),
         {:ok, result} <- execute(executor, conn, decoded) do
      {:open, ok(%{"type" => "execute", "result" => StmtResult.encode(result, opts)})}
    else
      {:error, %Error{} = error} -> {:open, stream_error(error)}
    end
  end

  def handle(executor, conn, %{"type" => "batch", "batch" => batch}, opts) do
    case Error.guard(fn -> Batch.decode(batch) end) do
      {:ok, decoded} ->
        result =
          Batch.run(decoded, &execute(executor, conn, &1), fn -> executor.autocommit?(conn) end)

        {:open, ok(%{"type" => "batch", "result" => BatchResult.encode(result, opts)})}

      {:error, %Error{} = error} ->
        {:open, stream_error(error)}
    end
  end

  def handle(executor, conn, %{"type" => "describe", "sql" => sql}, _opts) when is_binary(sql) do
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

  def handle(executor, conn, %{"type" => "sequence", "sql" => sql}, _opts) when is_binary(sql) do
    if function_exported?(executor, :execute_sequence, 2) do
      case executor.execute_sequence(conn, sql) do
        :ok -> {:open, ok(%{"type" => "sequence"})}
        {:error, %Error{} = error} -> {:open, stream_error(error)}
      end
    else
      {:open,
       stream_error(%Error{message: "sequence is not supported", code: "FILO_UNSUPPORTED"})}
    end
  end

  def handle(executor, conn, %{"type" => "get_autocommit"}, _opts) do
    {:open, ok(%{"type" => "get_autocommit", "is_autocommit" => executor.autocommit?(conn)})}
  end

  def handle(executor, conn, %{"type" => "close"}, _opts) do
    :ok = executor.close(conn)
    {:closed, ok(%{"type" => "close"})}
  end

  def handle(_executor, _conn, %{"type" => type}, _opts) do
    name = if is_binary(type), do: type, else: inspect(type)

    {:open,
     stream_error(%Error{message: "unsupported request type: #{name}", code: "FILO_UNSUPPORTED"})}
  end

  # Not even a map with a type (e.g. a bare string in the `requests` list).
  def handle(_executor, _conn, _request, _opts) do
    {:open, stream_error(Error.bad_request("request must be an object with a type"))}
  end

  @doc false
  # Runs one statement, refusing one that has no SQL text. A `sql_id` only means something on the
  # WebSocket binding (it names a `store_sql` entry, which the socket substitutes before dispatch);
  # on HTTP there is no stored-SQL store, so such a statement used to reach the executor with
  # `sql: nil` and fail there in whatever way the host happened to (fathom panel2 2026-10-10 H9).
  # Every transport's statements go through here so the refusal is uniform.
  @spec execute(module(), Filo.Executor.conn(), Stmt.t()) ::
          {:ok, StmtResult.t()} | {:error, Error.t()}
  def execute(_executor, _conn, %Stmt{sql: nil, sql_id: sql_id}) when not is_nil(sql_id) do
    {:error,
     %Error{
       message:
         "stored SQL (sql_id) is only supported over WebSocket, and no stored statement matches this id",
       code: "FILO_UNSUPPORTED",
       status: 400
     }}
  end

  def execute(_executor, _conn, %Stmt{sql: nil}) do
    {:error, Error.bad_request("statement has no sql")}
  end

  def execute(executor, conn, %Stmt{} = stmt), do: executor.execute(conn, stmt)

  defp ok(response), do: %{"type" => "ok", "response" => response}
  defp stream_error(%Error{} = error), do: %{"type" => "error", "error" => Error.encode(error)}
end
