defmodule Filo.IdleBoundsTest.Txn do
  @moduledoc false
  # An executor whose connection is "in a transaction" between BEGIN and COMMIT. `open_arg` is
  # {test_pid, agent}; the agent holds the in-transaction flag.
  @behaviour Filo.Executor

  @impl true
  def open({test, agent}), do: {:ok, {test, make_ref(), agent}}

  @impl true
  def execute({_t, _r, agent}, %Filo.Stmt{sql: "BEGIN"}) do
    Agent.update(agent, fn _ -> true end)
    {:ok, %Filo.StmtResult{}}
  end

  def execute({_t, _r, agent}, %Filo.Stmt{sql: "COMMIT"}) do
    Agent.update(agent, fn _ -> false end)
    {:ok, %Filo.StmtResult{}}
  end

  def execute(_conn, %Filo.Stmt{}), do: {:ok, %Filo.StmtResult{cols: ["n"], rows: [[1]]}}

  @impl true
  def autocommit?({_t, _r, agent}), do: not Agent.get(agent, & &1)

  @impl true
  def close({test, ref, _agent}) do
    send(test, {:closed, ref})
    :ok
  end
end

defmodule Filo.IdleBoundsTest do
  use ExUnit.Case, async: true

  alias Filo.IdleBoundsTest.Txn
  alias Filo.Socket

  setup do
    agent = start_supervised!({Agent, fn -> false end})
    %{agent: agent}
  end

  defp socket(agent, opts \\ []) do
    {:ok, state} = Socket.init([executor: Txn, open_arg: {self(), agent}] ++ opts)
    state
  end

  defp send_msg(state, msg), do: Socket.handle_in({Jason.encode!(msg), [opcode: :text]}, state)
  defp req(id, request), do: %{"type" => "request", "request_id" => id, "request" => request}

  defp pushed({:push, {:text, json}, state}), do: {Jason.decode!(json), state}

  defp pushed({:stop, :normal, {1001, _}, [{:text, json}], state}),
    do: {Jason.decode!(json), state}

  defp hello(state) do
    {%{"type" => "hello_ok"}, state} = pushed(send_msg(state, %{"type" => "hello", "jwt" => nil}))
    state
  end

  defp run(state, id, sql) do
    request = %{"type" => "execute", "stream_id" => 1, "stmt" => %{"sql" => sql}}
    send_msg(state, req(id, request))
  end

  defp ready(agent, opts \\ []) do
    state = socket(agent, opts) |> hello()

    {_, state} =
      pushed(send_msg(state, req(1, %{"type" => "open_stream", "stream_id" => 1})))

    state
  end

  defp begin_txn(state) do
    {%{"type" => "response_ok"}, state} = pushed(run(state, 2, "BEGIN"))
    state
  end

  # The sweep timer is armed by the stream's FIRST request (open_stream), so the first sweep can
  # fire before the latest request is a full window old; the socket then correctly re-arms for the
  # remainder. Handle sweeps until the socket stops re-arming, so the test never races that gap.
  defp sweep_until_idle(state) do
    assert_receive :filo_txn_sweep, 1_000
    {:ok, state} = Socket.handle_info(:filo_txn_sweep, state)
    if state.sweep_ref, do: sweep_until_idle(state), else: state
  end

  describe "hello timeout" do
    test "init arms it and a socket with no hello is closed 1008", %{agent: agent} do
      state = socket(agent, hello_timeout: 5)
      assert_receive :filo_hello_timeout, 1_000

      assert {:stop, :normal, {1008, "hello timeout"}, _} =
               Socket.handle_info(:filo_hello_timeout, state)
    end

    test "a completed hello makes the timer a no-op", %{agent: agent} do
      state = socket(agent) |> hello()
      assert {:ok, ^state} = Socket.handle_info(:filo_hello_timeout, state)
    end
  end

  # Symptom (fathom panel2 2026-10-10 H5): a client that sent BEGIN and went silent held the
  # database's write lock until the socket died. Invariant: idle + in transaction => released.
  describe "idle in transaction" do
    test "an idle stream in a transaction is closed and its id is gone", %{agent: agent} do
      state = ready(agent, txn_idle_timeout: 5) |> begin_txn() |> sweep_until_idle()
      assert_received {:closed, _ref}

      {resp, _} = pushed(run(state, 3, "SELECT 1"))
      assert resp["type"] == "response_error"
      assert resp["error"]["code"] == "STREAM_NOT_FOUND"
    end

    test "an idle stream in autocommit mode is left open", %{agent: agent} do
      state = ready(agent, txn_idle_timeout: 5)
      {_, state} = pushed(run(state, 2, "SELECT 1"))
      state = sweep_until_idle(state)
      refute_received {:closed, _}
      {resp, _} = pushed(run(state, 3, "SELECT 1"))
      assert resp["type"] == "response_ok"
    end

    test "a stream used within the window is not swept", %{agent: agent} do
      state = ready(agent, txn_idle_timeout: 60_000) |> begin_txn()

      {:ok, state} = Socket.handle_info(:filo_txn_sweep, state)
      refute_received {:closed, _}
      {resp, _} = pushed(run(state, 3, "COMMIT"))
      assert resp["type"] == "response_ok"
    end
  end

  # H4: the embedding app can ask a socket to go away (fathom drain).
  describe "server-initiated close" do
    test "drain closes 1001 at once when no stream is in a transaction", %{agent: agent} do
      state = ready(agent)
      assert {:stop, :normal, {1001, _}, ^state} = Socket.handle_info(:filo_drain, state)
    end

    test "drain waits for the transaction to end, then closes 1001 after the response", %{
      agent: agent
    } do
      state = ready(agent) |> begin_txn()
      assert {:ok, state} = Socket.handle_info(:filo_drain, state)

      # still in the transaction: the BEGIN-era statement is answered and the socket stays up
      assert {:push, _, state} = run(state, 3, "SELECT 1")

      reply = run(state, 4, "COMMIT")
      assert {resp, _} = pushed(reply)
      assert resp["type"] == "response_ok"
      assert {:stop, :normal, {1001, _}, [_frame], _} = reply
    end

    test "close is immediate 1001 even inside a transaction and terminate rolls back", %{
      agent: agent
    } do
      state = ready(agent) |> begin_txn()
      assert {:stop, :normal, {1001, _}, state} = Socket.handle_info(:filo_close, state)
      Socket.terminate(:normal, state)
      assert_receive {:closed, _ref}
    end

    test "drain/1 and close/1 deliver the messages" do
      assert :ok = Socket.drain(self())
      assert_received :filo_drain
      assert :ok = Socket.close(self())
      assert_received :filo_close
    end
  end

  describe "Filo.Stream drain" do
    setup %{agent: agent} do
      start_supervised!({Filo.Streams, name: __MODULE__.Streams})
      %{opts: [executor: Txn, open_arg: {self(), agent}]}
    end

    test "closes idle-autocommit streams, keeps in-transaction ones", %{agent: agent, opts: opts} do
      {:ok, _id, _seq, idle} = Filo.Streams.create(__MODULE__.Streams, opts)
      ref = Process.monitor(idle)
      Filo.Streams.drain(__MODULE__.Streams)
      assert_receive {:DOWN, ^ref, :process, ^idle, :normal}
      assert_receive {:closed, _}

      {:ok, _id, seq, busy} = Filo.Streams.create(__MODULE__.Streams, opts)
      begin = %{"type" => "execute", "stmt" => %{"sql" => "BEGIN"}}
      {:ok, :open, _, _} = Filo.Stream.run(busy, seq, [begin])
      assert Agent.get(agent, & &1)
      busy_ref = Process.monitor(busy)
      Filo.Streams.drain(__MODULE__.Streams)
      _ = :sys.get_state(busy)
      refute_received {:DOWN, ^busy_ref, :process, ^busy, _}
    end
  end
end
