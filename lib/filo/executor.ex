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
end
