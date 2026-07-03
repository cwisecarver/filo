defmodule Filo.Executor do
  @moduledoc """
  Behaviour a host application implements to back a Hrana stream with a real
  database connection.

  Filo owns the *protocol*; the executor owns *SQL*. A stream opens one
  connection (`open/1`), runs statements on it (`execute/2`), reports its
  autocommit state (`autocommit?/1`), and releases it when the stream closes
  (`close/1`). The connection handle is opaque to Filo.
  """

  @typedoc "An opaque connection handle the host returns from `open/1`."
  @type conn :: term()

  @doc """
  Opens a connection for a new stream. `arg` is host-supplied context (for
  example the database name extracted from the request).
  """
  @callback open(arg :: term()) :: {:ok, conn()} | {:error, Filo.Error.t()}

  @doc "Runs one statement on the connection."
  @callback execute(conn(), Filo.Stmt.t()) ::
              {:ok, Filo.StmtResult.t()} | {:error, Filo.Error.t()}

  @doc """
  Reports whether the connection is currently in autocommit mode — i.e. not
  inside an explicit transaction.
  """
  @callback autocommit?(conn()) :: boolean()

  @doc "Releases the connection when its stream closes."
  @callback close(conn()) :: :ok

  @doc """
  Describes a statement without running it — its parameters, columns, and whether
  it is an `EXPLAIN` or read-only. Optional: an executor that does not implement
  it makes `describe` requests fail with an "unsupported" stream error.
  """
  @callback describe(conn(), sql :: String.t()) ::
              {:ok, Filo.Describe.t()} | {:error, Filo.Error.t()}

  @doc """
  Runs a SQL script — one or more statements — for its side effects, returning no
  rows. Used by the Hrana `sequence` request. Optional: an executor that does not
  implement it makes `sequence` requests fail with an "unsupported" error.
  """
  @callback execute_sequence(conn(), sql :: String.t()) :: :ok | {:error, Filo.Error.t()}

  @doc """
  The process whose death invalidates this connection — e.g. a per-database
  coordinator that owns the underlying file's lifecycle and whose successor may
  flush/replace/drop that file once it believes no connections remain.

  Optional. When implemented and non-nil, the stream holding the connection
  monitors the pid and tears itself down on `:DOWN` — closing the connection via
  `close/1` — so a connection never outlives its owner and keeps writing into a
  file the owner's successor can pull out from under it. The client sees the
  stream as gone (`STREAM_NOT_FOUND` on next use) and reopens, landing on the
  successor. Return `nil` (or don't implement) when no such process exists.
  """
  @callback owner(conn()) :: pid() | nil

  @optional_callbacks describe: 2, execute_sequence: 2, owner: 1

  @doc false
  # The owner pid to monitor for `conn`, or nil (callback not implemented, or no owner).
  # Shared by Filo.Stream (HTTP) and Filo.Socket (WebSocket).
  @spec owner_pid(module(), conn()) :: pid() | nil
  def owner_pid(executor, conn) do
    with true <- function_exported?(executor, :owner, 1),
         pid when is_pid(pid) <- executor.owner(conn) do
      pid
    else
      _ -> nil
    end
  end
end
