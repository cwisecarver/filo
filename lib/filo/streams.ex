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
  def create(name, stream_opts) do
    registry = registry(name)
    stream_id = gen_stream_id(registry)
    seq = rand_u64()
    opts = Keyword.merge(stream_opts, seq: seq, name: via(registry, stream_id))

    case DynamicSupervisor.start_child(dynsup(name), {Filo.Stream, opts}) do
      {:ok, pid} -> {:ok, stream_id, seq, pid}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Finds the stream process for `stream_id`, or `:error` if there is none."
  @spec lookup(Supervisor.supervisor(), non_neg_integer()) :: {:ok, pid()} | :error
  def lookup(name, stream_id) do
    case Registry.lookup(registry(name), stream_id) do
      [{pid, _value}] -> {:ok, pid}
      [] -> :error
    end
  end

  defp registry(name), do: Module.concat(name, Registry)
  defp dynsup(name), do: Module.concat(name, DynamicSupervisor)
  defp via(registry, stream_id), do: {:via, Registry, {registry, stream_id}}

  # Random unguessable u64 id, rejection-sampled against the registry to avoid a
  # (vanishingly unlikely) collision with a live stream.
  defp gen_stream_id(registry) do
    Enum.find_value(1..10, fn _ ->
      id = rand_u64()

      case Registry.lookup(registry, id) do
        [] -> id
        _ -> nil
      end
    end) || raise "Filo.Streams: could not generate a free stream id"
  end

  defp rand_u64, do: :binary.decode_unsigned(:crypto.strong_rand_bytes(@u64_bytes))
end
