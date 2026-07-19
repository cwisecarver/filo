defmodule Filo.Executor do
  @moduledoc """
  Behaviour a host application implements to back a Hrana stream with a real
  database connection.

  Filo owns the *protocol*; the executor owns *SQL*. A stream opens one
  connection (`open/1`, or `open/2` to receive the `:authorize` context),
  runs statements on it (`execute/2`), reports its autocommit state
  (`autocommit?/1`), and releases it when the stream closes (`close/1`). The
  connection handle is opaque to Filo.
  """

  @typedoc "An opaque connection handle the host returns from `open/1`."
  @type conn :: term()

  @doc """
  Opens a connection for a new stream. `arg` is host-supplied context (for
  example the database name extracted from the request).
  """
  @callback open(arg :: term()) :: {:ok, conn()} | {:error, Filo.Error.t()}

  @doc """
  Opens a connection for a new stream, given the host `context` that the
  `:authorize` callback returned for this connection (see `Filo.Plug`).

  This is how a verified per-connection credential — the token's scope, a tenant
  id, anything `authorize` decoded — reaches `open` without a process-local
  side-channel: `authorize` returns `{:ok, context}` and Filo threads that
  `context` here, on **both** transports (the HTTP stream that opens in its own
  process, and every WebSocket `open_stream` after the `hello`). `context` is
  `nil` when no `:authorize` callback is configured or it returned a bare `:ok`.

  Optional. When implemented, Filo calls it in preference to `open/1`; an
  executor that only defines `open/1` keeps working unchanged (the context is
  simply dropped).
  """
  @callback open(arg :: term(), context :: term()) ::
              {:ok, conn()} | {:error, Filo.Error.t()}

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

  @optional_callbacks describe: 2, execute_sequence: 2, owner: 1, open: 2

  @doc false
  # Opens a connection for a new stream, threading the authorize `context`.
  # Prefers the context-aware `open/2`; falls back to `open/1` for executors that
  # don't implement it (the context is dropped). Shared by Filo.Stream (HTTP),
  # Filo.Plug (the stateless v1 path), and Filo.Socket (WebSocket), so all three
  # transports thread the context identically.
  @spec open(module(), term(), term()) :: {:ok, conn()} | {:error, Filo.Error.t()}
  def open(executor, arg, context) do
    if function_exported?(executor, :open, 2) do
      executor.open(arg, context)
    else
      executor.open(arg)
    end
  end

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
