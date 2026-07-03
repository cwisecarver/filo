defmodule Filo.OwnerMonitorTest.Echo do
  @moduledoc false
  # An executor whose connections have an owner process (Filo.Executor.owner/1) —
  # e.g. a per-database coordinator. `open_arg` is `{owner_pid, test_pid}`; close/1
  # reports to the test so teardown is observable.
  @behaviour Filo.Executor

  @impl true
  def open({owner, test}), do: {:ok, {owner, test, make_ref()}}

  @impl true
  def execute({_owner, _test, _ref}, %Filo.Stmt{}),
    do: {:ok, %Filo.StmtResult{cols: ["n"], rows: [[1]]}}

  @impl true
  def autocommit?(_conn), do: true

  @impl true
  def close({_owner, test, ref}) do
    send(test, {:closed, ref})
    :ok
  end

  @impl true
  def owner({owner, _test, _ref}), do: owner
end

defmodule Filo.OwnerMonitorTest.EchoNoOwner do
  @moduledoc false
  # The pre-seam shape: no owner/1 — streams must behave exactly as before.
  @behaviour Filo.Executor

  @impl true
  def open(_arg), do: {:ok, :conn}

  @impl true
  def execute(:conn, %Filo.Stmt{}), do: {:ok, %Filo.StmtResult{cols: ["n"], rows: [[1]]}}

  @impl true
  def autocommit?(:conn), do: true

  @impl true
  def close(:conn), do: :ok
end

defmodule Filo.OwnerMonitorTest do
  # The owner seam: a stream monitors its connection's owner process and tears down
  # (closing the connection) when the owner dies. Symptom pinned: without this, a
  # stream whose owner (e.g. a per-database coordinator) crashed kept its connection
  # open and writing — an orphan writer the owner's successor could flush/drop the
  # underlying file under, losing the writes.
  use ExUnit.Case, async: true

  alias Filo.OwnerMonitorTest.Echo
  alias Filo.{Socket, Stream}

  defp spawn_owner, do: spawn(fn -> receive(do: (:never -> :ok)) end)

  # --- Filo.Stream (HTTP) ---

  test "a stream tears down and closes its connection when the owner dies" do
    owner = spawn_owner()

    stream =
      start_supervised!({Stream, executor: Echo, open_arg: {owner, self()}, seq: 0})

    stream_ref = Process.monitor(stream)
    Process.exit(owner, :kill)

    assert_receive {:DOWN, ^stream_ref, :process, ^stream, :normal}
    assert_receive {:closed, _conn_ref}, 100, "the orphaned connection must be closed"
  end

  test "a stream whose executor has no owner is unaffected (pre-seam behavior)" do
    stream =
      start_supervised!(
        {Stream, executor: Filo.OwnerMonitorTest.EchoNoOwner, open_arg: nil, seq: 0}
      )

    assert {:ok, :open, _results, 1} =
             Stream.run(stream, 0, [
               %{"type" => "execute", "stmt" => %{"sql" => "SELECT 1"}}
             ])
  end

  # --- Filo.Socket (WebSocket) ---
  #
  # Socket callbacks run in the caller here, so the monitors created by open_stream
  # belong to the test process: the owner's :DOWN lands in OUR mailbox and is handed
  # to Socket.handle_info exactly as the WebSock runtime would.

  defp socket(owner) do
    {:ok, state} = Socket.init(executor: Echo, open_arg: {owner, self()})

    {%{"type" => "hello_ok"}, state} =
      pushed(Socket.handle_in({Jason.encode!(%{"type" => "hello"}), [opcode: :text]}, state))

    state
  end

  defp send_msg(state, msg), do: Socket.handle_in({Jason.encode!(msg), [opcode: :text]}, state)
  defp pushed({:push, {:text, json}, state}), do: {Jason.decode!(json), state}
  defp req(id, request), do: %{"type" => "request", "request_id" => id, "request" => request}

  defp open_stream(state, sid) do
    {%{"type" => "response_ok"}, state} =
      pushed(send_msg(state, req(sid * 100, %{"type" => "open_stream", "stream_id" => sid})))

    state
  end

  defp execute(state, sid) do
    {resp, state} =
      pushed(
        send_msg(
          state,
          req(sid * 100 + 1, %{
            "type" => "execute",
            "stream_id" => sid,
            "stmt" => %{"sql" => "SELECT 1"}
          })
        )
      )

    {resp, state}
  end

  test "an owner death closes exactly that stream; others keep serving" do
    owner = spawn_owner()
    state = socket(owner) |> open_stream(1)

    # Second stream with a different (live) owner on the same socket.
    survivor = spawn_owner()
    state = %{state | open_arg: {survivor, self()}}
    state = open_stream(state, 2)

    Process.exit(owner, :kill)
    assert_receive {:DOWN, _ref, :process, ^owner, :killed} = down

    {:ok, state} = Socket.handle_info(down, state)
    assert_received {:closed, _conn_ref}

    # Stream 1 is gone (STREAM_NOT_FOUND); stream 2 still serves.
    assert {%{"type" => "response_error", "error" => %{"code" => "STREAM_NOT_FOUND"}}, state} =
             execute(state, 1)

    assert {%{"type" => "response_ok"}, _state} = execute(state, 2)
  end

  test "close_stream demonitors: a later owner death fires no stale teardown" do
    owner = spawn_owner()
    state = socket(owner) |> open_stream(1)

    {%{"type" => "response_ok"}, _state} =
      pushed(send_msg(state, req(9, %{"type" => "close_stream", "stream_id" => 1})))

    assert_received {:closed, _conn_ref}

    # The socket's monitor was flushed at close; killing the owner now must deliver
    # nothing to the socket process (only our own explicit monitor fires).
    ours = Process.monitor(owner)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^ours, :process, ^owner, :killed}
    refute_received {:DOWN, _other, :process, ^owner, _}
  end
end
