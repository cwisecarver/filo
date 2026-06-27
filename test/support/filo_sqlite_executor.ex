defmodule Filo.Test.SqliteExecutor do
  @moduledoc """
  A real `Filo.Executor` backed by SQLite (exqlite), for end-to-end tests that
  drive Filo with actual libSQL clients. `open_arg` is a database path; every
  stream opens its own connection to it, so a shared file behaves like a real
  shared database (writes committed by one connection are visible to others).

  Test-only — the library never depends on a SQLite engine.
  """
  @behaviour Filo.Executor

  alias Exqlite.Sqlite3
  alias Filo.{Describe, Error, Stmt, StmtResult}

  @impl true
  def open(path) do
    case Sqlite3.open(path) do
      {:ok, conn} -> {:ok, conn}
      {:error, reason} -> {:error, error(reason, "FILO_OPEN")}
    end
  end

  @impl true
  def execute(conn, %Stmt{sql: sql, args: args}) do
    with {:ok, stmt} <- Sqlite3.prepare(conn, sql),
         :ok <- Sqlite3.bind(stmt, Enum.map(args, &to_bind/1)) do
      result = collect(conn, stmt)
      Sqlite3.release(conn, stmt)
      result
    else
      {:error, reason} -> {:error, error(reason)}
    end
  end

  # exqlite exposes no autocommit query; libSQL's hrana2 clients never ask, and
  # hrana3 `is_autocommit` is only consulted by batch conditions the tests don't
  # use. Report true (the common case) rather than guess.
  @impl true
  def autocommit?(_conn), do: true

  @impl true
  def close(conn) do
    Sqlite3.close(conn)
    :ok
  end

  @impl true
  def describe(conn, sql) do
    case Sqlite3.prepare(conn, sql) do
      {:ok, stmt} ->
        {:ok, cols} = Sqlite3.columns(conn, stmt)
        params = List.duplicate(nil, Sqlite3.bind_parameter_count(stmt))
        Sqlite3.release(conn, stmt)
        {:ok, %Describe{params: params, cols: cols, is_readonly: readonly?(sql)}}

      {:error, reason} ->
        {:error, error(reason)}
    end
  end

  @impl true
  def execute_sequence(conn, sql) do
    # exqlite runs every statement in the script in one call.
    case Sqlite3.execute(conn, sql) do
      :ok -> :ok
      {:error, reason} -> {:error, error(reason)}
    end
  end

  defp collect(conn, stmt) do
    {:ok, cols} = Sqlite3.columns(conn, stmt)
    rows = step_all(conn, stmt, [])
    {:ok, changes} = Sqlite3.changes(conn)
    {:ok, rowid} = Sqlite3.last_insert_rowid(conn)

    # A statement that returns columns is a query: report no affected rows and no
    # rowid (otherwise SQLite leaks the previous DML's counters).
    {affected, last_rowid} =
      if cols == [], do: {changes, if(changes > 0, do: rowid, else: nil)}, else: {0, nil}

    {:ok,
     %StmtResult{
       cols: cols,
       rows: rows,
       affected_row_count: affected,
       last_insert_rowid: last_rowid,
       rows_read: length(rows),
       rows_written: affected
     }}
  end

  defp step_all(conn, stmt, acc) do
    case Sqlite3.step(conn, stmt) do
      {:row, row} -> step_all(conn, stmt, [row | acc])
      :done -> Enum.reverse(acc)
    end
  end

  defp to_bind({:blob, blob}), do: {:blob, blob}
  defp to_bind(value), do: value

  defp readonly?(sql),
    do: sql |> String.trim_leading() |> String.upcase() |> String.starts_with?("SELECT")

  defp error(reason, code \\ "SQLITE_ERROR"),
    do: %Error{message: to_string(reason), code: code}
end
