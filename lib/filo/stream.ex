defmodule Filo.Stream do
  @moduledoc """
  A single Hrana-over-HTTP stream: one `GenServer` owning one executor
  connection and the stream's baton sequence number.

  In libsql's server a stream is a passive struct guarded by a mutex; each HTTP
  request *acquires* it, runs its pipeline, then *releases* it with a fresh
  baton. Filo collapses that into a process: the connection lives in the
  `GenServer`, and the mailbox serializes requests for free.

  ## Sequence numbers

  Every request must present the `seq` the server last handed out. A successful
  run advances `seq` by one, so each baton is single-use and requests on a
  stream are strictly serial. A request that presents the wrong `seq` is
  rejected as `{:error, :baton_reused}` without touching the connection.

  ## Lifecycle

  - `start_link/1` opens the connection through the executor; if the executor
    cannot open one, the start fails with `{:open_failed, error}`.
  - A `close` request releases the connection (via `Filo.Request`) and the
    process stops normally.
  - After `:idle_timeout` of inactivity the stream expires: it closes its
    connection and stops. Every successful run resets the timer.

  The process is `:temporary` — a dead stream is not restarted; the client must
  open a new one.
  """

  use GenServer

  alias Filo.Request

  @default_idle_timeout 10_000

  defstruct [:executor, :conn, :seq, :idle_timeout, :timer]

  @doc """
  Starts a stream.

  Options:

    - `:executor` (required) — the `Filo.Executor` module.
    - `:open_arg` — host context passed to `executor.open/1` (default `nil`).
    - `:seq` (required) — the initial sequence number.
    - `:idle_timeout` — inactivity timeout in ms (default `#{@default_idle_timeout}`).
    - `:name` — a standard `GenServer` name (e.g. a `Registry` via-tuple).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_opts, init_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, init_opts, gen_opts)
  end

  @doc false
  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, restart: :temporary}
  end

  @doc """
  Runs a pipeline of decoded Hrana requests against the stream.

  `seq` must match the sequence the stream currently expects. On success returns
  `{:ok, status, results, next_seq}` where `status` is `:open` (the stream
  continues, `next_seq` is the rotated sequence) or `:closed` (a `close` request
  released the connection, `next_seq` is `nil`). A mismatched `seq` returns
  `{:error, :baton_reused}`.
  """
  @spec run(GenServer.server(), non_neg_integer(), [map()]) ::
          {:ok, Request.status(), [map()], non_neg_integer() | nil} | {:error, :baton_reused}
  def run(server, seq, requests) do
    GenServer.call(server, {:run, seq, requests})
  end

  @impl true
  def init(opts) do
    executor = Keyword.fetch!(opts, :executor)
    seq = Keyword.fetch!(opts, :seq)
    open_arg = Keyword.get(opts, :open_arg)
    idle_timeout = Keyword.get(opts, :idle_timeout, @default_idle_timeout)

    case executor.open(open_arg) do
      {:ok, conn} ->
        state = %__MODULE__{
          executor: executor,
          conn: conn,
          seq: seq,
          idle_timeout: idle_timeout
        }

        {:ok, arm_timer(state)}

      {:error, error} ->
        {:stop, {:open_failed, error}}
    end
  end

  @impl true
  def handle_call({:run, seq, _requests}, _from, %{seq: expected} = state) when seq != expected do
    {:reply, {:error, :baton_reused}, state}
  end

  def handle_call({:run, _seq, requests}, _from, state) do
    case run_pipeline(state, requests) do
      {:open, results} ->
        next = state.seq + 1
        {:reply, {:ok, :open, results, next}, arm_timer(%{state | seq: next})}

      {:closed, results} ->
        # `Filo.Request` already released the connection on the close request, so
        # there is nothing left for us to close — just stop.
        {:stop, :normal, {:ok, :closed, results, nil}, %{state | conn: nil}}
    end
  end

  @impl true
  def handle_info(:expire, state) do
    {:stop, :normal, close_conn(state)}
  end

  @impl true
  def terminate(_reason, state) do
    close_conn(state)
    :ok
  end

  defp run_pipeline(state, requests) do
    {status, acc} =
      Enum.reduce_while(requests, {:open, []}, fn request, {_status, acc} ->
        {status, result} = Request.handle(state.executor, state.conn, request)

        case status do
          :open -> {:cont, {:open, [result | acc]}}
          :closed -> {:halt, {:closed, [result | acc]}}
        end
      end)

    {status, Enum.reverse(acc)}
  end

  # Releases the connection if we still own one, returning the nulled state.
  # Idempotent: a stream closed via `close`/expiry won't be closed again on
  # `terminate`.
  defp close_conn(%{conn: nil} = state), do: state

  defp close_conn(state) do
    state.executor.close(state.conn)
    %{state | conn: nil}
  end

  defp arm_timer(state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    %{state | timer: Process.send_after(self(), :expire, state.idle_timeout)}
  end
end
