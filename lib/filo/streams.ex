defmodule Filo.Streams do
  @moduledoc """
  Registry of live `Filo.Stream` processes — the find-or-start layer between the
  HTTP plug and the per-stream processes.

  A `Filo.Streams` supervisor owns a `Registry` (mapping `stream_id` to the
  stream process) and a `DynamicSupervisor` (the streams themselves). The plug:

    - calls `create/2` when a request arrives with no baton, getting back a fresh
      `stream_id` and initial `seq` to mint the first baton;
    - calls `lookup/2` with the `stream_id` decoded from a baton to find the
      stream for the next request.

  Stream ids and initial sequence numbers are drawn from
  `:crypto.strong_rand_bytes/1`, so they are unguessable across the full 64-bit
  range, matching libsql.
  """

  use Supervisor

  @u64_bytes 8

  @doc """
  Starts the streams supervisor.

  Required option `:name` is the supervisor's registered name; the `Registry` and
  `DynamicSupervisor` are registered under names derived from it.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, name, name: name)
  end

  @impl true
  def init(name) do
    children = [
      {Registry, keys: :unique, name: registry(name)},
      {DynamicSupervisor, name: dynsup(name), strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Starts a new stream and registers it under a fresh, random `stream_id`.

  `stream_opts` are forwarded to `Filo.Stream.start_link/1` (`:executor`,
  `:open_arg`, `:idle_timeout`); the `:seq` and registry `:name` are supplied
  here. Returns `{:ok, stream_id, seq, pid}`, or `{:error, reason}` if the
  executor cannot open a connection.
  """
  @spec create(Supervisor.supervisor(), keyword()) ::
          {:ok, non_neg_integer(), non_neg_integer(), pid()} | {:error, term()}
  def create(name, stream_opts), do: create(name, stream_opts, 10)

  # ONE `:crypto.strong_rand_bytes/1` for id and seq, and no Registry pre-lookup (fathom expert
  # review 2026-10-17 #9): registering the via-name is itself the uniqueness check, so a (vanishingly
  # unlikely) collision with a live stream surfaces as `{:already_started, _}` from `start_child` and
  # draws a new id, instead of every create paying a lookup first.
  defp create(_name, _stream_opts, 0),
    do: raise("Filo.Streams: could not generate a free stream id")

  defp create(name, stream_opts, attempts) do
    registry = registry(name)
    <<stream_id::unsigned-64, seq::unsigned-64>> = :crypto.strong_rand_bytes(2 * @u64_bytes)

    opts =
      Keyword.merge(stream_opts, seq: seq, name: via(registry, stream_id), deferred_open: true)

    # The executor open runs in the stream process, and THIS caller waits for it - not the shared
    # DynamicSupervisor, which used to block every other create on the node behind one slow open
    # (fathom expert review 2026-10-01 #2). The contract is unchanged: `create/2` returns once the
    # connection is open, or with `{:error, {:open_failed, error}}`.
    case DynamicSupervisor.start_child(dynsup(name), {Filo.Stream, opts}) do
      {:ok, pid} ->
        with :ok <- await_open(pid), do: {:ok, stream_id, seq, pid}

      {:error, {:already_started, _pid}} ->
        create(name, stream_opts, attempts - 1)

      {:error, _} = error ->
        error
    end
  end

  # An executor that raises (rather than returning an error) crashes the stream mid-open; report
  # that as the create's error, as start_child did when the open ran in init/1.
  defp await_open(pid) do
    Filo.Stream.await_open(pid)
  catch
    :exit, {reason, _call} -> {:error, reason}
  end

  @doc "Finds the stream process for `stream_id`, or `:error` if there is none."
  @spec lookup(Supervisor.supervisor(), non_neg_integer()) :: {:ok, pid()} | :error
  def lookup(name, stream_id) do
    case Registry.lookup(registry(name), stream_id) do
      [{pid, _value}] -> {:ok, pid}
      [] -> :error
    end
  end

  @doc """
  Drains every live stream: each one not inside a transaction closes (see `Filo.Stream.drain/1`).
  Streams inside a transaction stay until they finish, idle out, or the host stops them.
  """
  @spec drain(Supervisor.supervisor()) :: :ok
  def drain(name) do
    for pid <- Registry.select(registry(name), [{{:_, :"$1", :_}, [], [:"$1"]}]),
        do: Filo.Stream.drain(pid)

    :ok
  end

  defp registry(name), do: Module.concat(name, Registry)
  defp dynsup(name), do: Module.concat(name, DynamicSupervisor)
  defp via(registry, stream_id), do: {:via, Registry, {registry, stream_id}}
end
