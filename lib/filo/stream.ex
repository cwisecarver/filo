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
    cannot open one, the start fails with `{:open_failed, error}`. With
    `deferred_open: true` the start returns at once and the open runs in the
    stream process; the starter then waits for it with `await_open/1`.
  - A `close` request releases the connection (via `Filo.Request`) and the
    process stops normally.
  - After `:idle_timeout` of inactivity the stream expires: it closes its
    connection and stops. Every successful run resets the timer.

  - When its supervisor stops it (`:shutdown`, e.g. on application stop) the
    stream closes its connection through `c:Filo.Executor.close/1` before it
    exits. The stream traps exits for this; a linked process that exits
    abnormally still takes the stream down (with the same reason), but the
    connection is closed first.

  The process is `:temporary` — a dead stream is not restarted; the client must
  open a new one.
  """

  use GenServer

  import Bitwise

  require Logger

  alias Filo.{Batch, Request}

  @default_idle_timeout 10_000
  @u64_mask 0xFFFFFFFFFFFFFFFF

  defstruct [:executor, :conn, :seq, :idle_timeout, :timer, :owner_ref, :open_error]

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
    - `:deferred_open` — when `true`, return before the executor open and run it in the stream
      process instead; the caller must then `await_open/1` (default `false`). `Filo.Streams`
      uses it so a slow open never runs inside the shared `DynamicSupervisor`.
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
    # shutdown: the time terminate/2 gets to run the executor's close/1 before the supervisor
    # escalates to :kill. Stated explicitly (it is the worker default) because the close is the
    # point of trapping exits: a host's close is a rollback plus a handle release, well inside it.
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      shutdown: 5_000
    }
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
  Waits for a `deferred_open: true` stream's executor open. Returns `:ok` once the connection is
  open, or `{:error, {:open_failed, error}}` (and the stream stops) if the executor refused it.

  It waits as long as the open takes, as `start_link/1` would: the executor bounds its own open.
  """
  @spec await_open(GenServer.server()) :: :ok | {:error, {:open_failed, term()}}
  def await_open(server), do: GenServer.call(server, :await_open, :infinity)

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
    state = %__MODULE__{executor: executor, seq: seq, idle_timeout: idle_timeout}

    # Trap exits so a supervisor shutdown runs terminate/2 and with it the executor's close/1
    # (fathom expert review 2026-10-08 #30). Without it the supervisor's :shutdown exit signal
    # killed the stream outright: the host never got to roll back and release the connection, so
    # on every application stop each stream with a live connection left its handle to be reclaimed
    # by GC. Set before the open so no exit signal can slip past an open connection.
    Process.flag(:trap_exit, true)

    if Keyword.get(opts, :deferred_open, false) do
      # The open can take seconds (a cold shard's storage pull, a failover hold) and a
      # DynamicSupervisor's start_child does not return until init/1 does, so opening here would
      # make every stream start on the node wait behind the slowest open (fathom expert review
      # 2026-10-01 #2). The continue runs before any call, so `await_open/1` sees the result.
      {:ok, state, {:continue, {:open, open_arg, open_context}}}
    else
      case open_conn(state, open_arg, open_context) do
        {:ok, state} -> {:ok, state}
        {:error, error} -> {:stop, {:open_failed, error}}
      end
    end
  end

  @impl true
  def handle_continue({:open, open_arg, open_context}, state) do
    case open_conn(state, open_arg, open_context) do
      {:ok, state} ->
        {:noreply, state}

      # Kept alive to hand the error to `await_open/1`, which then stops the stream normally.
      # The idle timer covers a starter that died before asking, so the stream cannot linger.
      {:error, error} ->
        {:noreply, arm_timer(%{state | open_error: error})}
    end
  end

  defp open_conn(state, open_arg, open_context) do
    case Filo.Executor.open(state.executor, open_arg, open_context) do
      {:ok, conn} ->
        # Monitor the connection's owner (see Filo.Executor.owner/1): if it dies, this
        # stream must not keep the connection alive — the owner's successor may replace
        # the underlying file believing no connections remain.
        state = %{state | conn: conn, owner_ref: monitor_owner(state.executor, conn)}
        {:ok, arm_timer(state)}

      {:error, error} ->
        {:error, error}
    end
  end

  @impl true
  def handle_call(:await_open, _from, %{open_error: nil} = state), do: {:reply, :ok, state}

  def handle_call(:await_open, _from, state) do
    {:stop, :normal, {:error, {:open_failed, state.open_error}}, state}
  end

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

  # Trapping exits turns exit signals from linked processes (other than the parent, which
  # GenServer handles itself by running terminate/2) into messages. Keep the untrapped semantics:
  # a normal exit of a linked process is ignored, any other reason stops the stream with that same
  # reason — terminate/2 then closes the connection, which the untrapped kill never did.
  def handle_info({:EXIT, _pid, :normal}, state), do: {:noreply, state}
  def handle_info({:EXIT, _pid, reason}, state), do: {:stop, reason, state}

  # Ignore anything else rather than crash on it (fathom expert review 2026-10-08 #27): a stray
  # message — fathom saw a late watchdog `{:timed_out, ref}` land here — used to kill the stream
  # with a FunctionClauseError, taking the client's open transaction with it.
  def handle_info(message, state) do
    Logger.debug("Filo.Stream ignoring unexpected message: #{inspect(message)}")
    {:noreply, state}
  end

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
  # Idempotent: a stream closed via `close`/expiry/owner-DOWN won't be closed again on
  # `terminate` — the conn is nil by then, so close/1 runs exactly once per connection.
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
