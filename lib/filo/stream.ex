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

  import Bitwise

  alias Filo.{Batch, Request}

  @default_idle_timeout 10_000
  @u64_mask 0xFFFFFFFFFFFFFFFF

  defstruct [:executor, :conn, :seq, :idle_timeout, :timer, :owner_ref]

  @doc """
  Starts a stream.

  Options:

    - `:executor` (required) — the `Filo.Executor` module.
    - `:open_arg` — host context passed to `executor.open` (default `nil`).
    - `:open_context` — the `:authorize` context threaded to `executor.open/2`
      (default `nil`). See `c:Filo.Executor.open/2`.
    - `:seq` (required) — the initial sequence number.
    - `:idle_timeout` — inactivity timeout in ms (default `#{@default_idle_timeout}`).
    - `:name` — a standard `GenServer` name (e.g. a `Registry` via-tuple).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    # `:hibernate_after` and `:spawn_opt` pass through to GenServer (fathom expert review
    # 2026-07-24 #22). A stream is idle-dominant by construction — a django-libsql WebSocket
    # stream lives for hours between requests — while holding the executor handle, its statement
    # cache, and a heap grown to the largest result set it ever materialized. With ERTS defaults
    # that heap is never given back, which is a large part of the measured per-served-shard cost.
    # The host decides the policy; filo just stops swallowing the options.
    {gen_opts, init_opts} = Keyword.split(opts, [:name, :hibernate_after, :spawn_opt])
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

  `opts` pass through to `Filo.Request.handle/4`'s result encoders (`rows: :json` for the
  JSON transports' pre-encoded row fragments; default `:maps` for protobuf).
  """
  @spec run(GenServer.server(), non_neg_integer(), [map()], keyword()) ::
          {:ok, Request.status(), [map()], non_neg_integer() | nil} | {:error, :baton_reused}
  def run(server, seq, requests, opts \\ []) do
    GenServer.call(server, {:run, seq, requests, opts})
  end

  @doc """
  Runs a decoded Hrana batch map against the stream for a cursor, returning the
  raw `Filo.BatchResult` (which the caller streams as cursor entries) and the
  rotated sequence. Like `run/3`, a mismatched `seq` is `{:error, :baton_reused}`.
  The stream stays open.
  """
  @spec run_cursor(GenServer.server(), non_neg_integer(), map()) ::
          {:ok, Filo.BatchResult.t(), non_neg_integer()} | {:error, :baton_reused}
  def run_cursor(server, seq, batch) do
    GenServer.call(server, {:run_cursor, seq, batch})
  end

  @impl true
  def init(opts) do
    executor = Keyword.fetch!(opts, :executor)
    seq = Keyword.fetch!(opts, :seq)
    open_arg = Keyword.get(opts, :open_arg)
    open_context = Keyword.get(opts, :open_context)
    idle_timeout = Keyword.get(opts, :idle_timeout, @default_idle_timeout)

    case Filo.Executor.open(executor, open_arg, open_context) do
      {:ok, conn} ->
        state = %__MODULE__{
          executor: executor,
          conn: conn,
          seq: seq,
          idle_timeout: idle_timeout,
          # Monitor the connection's owner (see Filo.Executor.owner/1): if it dies, this
          # stream must not keep the connection alive — the owner's successor may replace
          # the underlying file believing no connections remain.
          owner_ref: monitor_owner(executor, conn)
        }

        {:ok, arm_timer(state)}

      {:error, error} ->
        {:stop, {:open_failed, error}}
    end
  end

  @impl true
  def handle_call({:run, seq, _requests, _opts}, _from, %{seq: expected} = state)
      when seq != expected do
    {:reply, {:error, :baton_reused}, state}
  end

  def handle_call({:run, _seq, requests, opts}, _from, state) do
    case run_pipeline(state, requests, opts) do
      {:open, results} ->
        # Wrap at 2^64 so the seq always fits the baton's u64 field, matching
        # libsql's wrapping_add.
        next = state.seq + 1 &&& @u64_mask
        {:reply, {:ok, :open, results, next}, arm_timer(%{state | seq: next})}

      {:closed, results} ->
        # `Filo.Request` already released the connection on the close request, so
        # there is nothing left for us to close — just stop.
        {:stop, :normal, {:ok, :closed, results, nil}, %{state | conn: nil}}
    end
  end

  def handle_call({:run_cursor, seq, _batch}, _from, %{seq: expected} = state)
      when seq != expected do
    {:reply, {:error, :baton_reused}, state}
  end

  def handle_call({:run_cursor, _seq, batch}, _from, state) do
    result =
      batch
      |> Batch.decode()
      |> Batch.run(
        &state.executor.execute(state.conn, &1),
        fn -> state.executor.autocommit?(state.conn) end
      )

    next = state.seq + 1 &&& @u64_mask
    {:reply, {:ok, result, next}, arm_timer(%{state | seq: next})}
  end

  @impl true
  def handle_info(:expire, state) do
    {:stop, :normal, close_conn(state)}
  end

  # The connection's owner died: close the connection and stop, so an orphaned stream
  # never keeps writing into a file the owner's successor may flush/drop from under it.
  # The client's next request on this baton gets STREAM_NOT_FOUND and reopens.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{owner_ref: ref} = state) do
    {:stop, :normal, close_conn(state)}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state), do: {:noreply, state}

  defp monitor_owner(executor, conn) do
    case Filo.Executor.owner_pid(executor, conn) do
      nil -> nil
      pid -> Process.monitor(pid)
    end
  end

  @impl true
  def terminate(_reason, state) do
    close_conn(state)
    :ok
  end

  defp run_pipeline(state, requests, opts) do
    {status, acc} =
      Enum.reduce_while(requests, {:open, []}, fn request, {_status, acc} ->
        {status, result} = Request.handle(state.executor, state.conn, request, opts)

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
    # async: true — a synchronous Process.cancel_timer/1 SUSPENDS this process until the timer
    # service that owns the timer replies, and send_after/3 registers the timer on whichever
    # scheduler ran the call. A stream that has since migrated schedulers therefore pays a
    # cross-scheduler signal round-trip on EVERY request, not merely a timer-wheel delete
    # (fathom expert review 2026-07-24 #22).
    #
    # Semantics are unchanged: the cancel still happens, it is just not waited on. The only
    # observable difference is that an already-in-flight :expire may still arrive — and
    # handle_info(:expire, …) already handles that by stopping the stream, which is the documented
    # re-openable contract. info: false because the cancellation result is never inspected.
    if state.timer, do: Process.cancel_timer(state.timer, async: true, info: false)
    %{state | timer: Process.send_after(self(), :expire, state.idle_timeout)}
  end
end
