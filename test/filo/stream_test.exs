defmodule Filo.StreamTest.Echo do
  @moduledoc false
  @behaviour Filo.Executor

  # `open_arg` is the test pid, so `close/1` can notify the test that the
  # connection was released. The conn handle carries a unique ref to prove it.
  @impl true
  def open(test) when is_pid(test), do: {:ok, {test, make_ref()}}
  def open({:fail, _test}), do: {:error, %Filo.Error{message: "no db", code: "FILO_OPEN"}}

  @impl true
  def execute({_test, _ref}, %Filo.Stmt{sql: "BOOM"}),
    do: {:error, %Filo.Error{message: "boom", code: "SQLITE_ERROR"}}

  @impl true
  def execute({_test, _ref}, %Filo.Stmt{}), do: {:ok, %Filo.StmtResult{cols: ["n"], rows: [[1]]}}

  @impl true
  def autocommit?({_test, _ref}), do: true

  @impl true
  def close({test, ref}) do
    send(test, {:closed, ref})
    :ok
  end
end

defmodule Filo.StreamTest do
  use ExUnit.Case, async: true

  alias Filo.Stream
  alias Filo.StreamTest.Echo

  defp start_stream(opts) do
    start_supervised!({Stream, Keyword.merge([executor: Echo, open_arg: self(), seq: 0], opts)})
  end

  defp execute_req, do: %{"type" => "execute", "stmt" => %{"sql" => "SELECT 1"}}

  test "run dispatches each request against the connection and returns ok stream results" do
    pid = start_stream(seq: 7)

    assert {:ok, :open, [result], 8} = Stream.run(pid, 7, [execute_req()])
    assert result["type"] == "ok"
    assert result["response"]["type"] == "execute"
    assert result["response"]["result"]["rows"] == [[%{"type" => "integer", "value" => "1"}]]
  end

  test "run threads a multi-request pipeline in order" do
    pid = start_stream(seq: 0)

    reqs = [execute_req(), %{"type" => "get_autocommit"}]
    assert {:ok, :open, [first, second], 1} = Stream.run(pid, 0, reqs)
    assert first["response"]["type"] == "execute"
    assert second["response"] == %{"type" => "get_autocommit", "is_autocommit" => true}
  end

  test "a per-request SQL error is reported but keeps the stream open and rotates the baton" do
    pid = start_stream(seq: 0)

    boom = %{"type" => "execute", "stmt" => %{"sql" => "BOOM"}}
    assert {:ok, :open, [result], 1} = Stream.run(pid, 0, [boom])

    assert result == %{
             "type" => "error",
             "error" => %{"message" => "boom", "code" => "SQLITE_ERROR"}
           }

    # still usable on the next seq
    assert {:ok, :open, _results, 2} = Stream.run(pid, 1, [execute_req()])
  end

  test "the seq rotates by one on every successful run" do
    pid = start_stream(seq: 0)

    assert {:ok, :open, _, 1} = Stream.run(pid, 0, [execute_req()])
    assert {:ok, :open, _, 2} = Stream.run(pid, 1, [execute_req()])
    assert {:ok, :open, _, 3} = Stream.run(pid, 2, [execute_req()])
  end

  test "the seq wraps at the u64 boundary so it always encodes as a baton" do
    max_u64 = 0xFFFFFFFFFFFFFFFF
    pid = start_stream(seq: max_u64)

    assert {:ok, :open, _, 0} = Stream.run(pid, max_u64, [execute_req()])
  end

  test "a request presenting the wrong seq is rejected as baton reuse, without running" do
    pid = start_stream(seq: 7)

    assert {:error, :baton_reused} = Stream.run(pid, 999, [execute_req()])
    # the stream is untouched: the correct seq still works
    assert {:ok, :open, _, 8} = Stream.run(pid, 7, [execute_req()])
  end

  test "run_cursor executes a batch and returns the raw result with a rotated seq" do
    pid = start_stream(seq: 0)

    batch = %{"steps" => [%{"stmt" => %{"sql" => "SELECT 1"}}]}

    assert {:ok, %Filo.BatchResult{step_results: [result], step_errors: [nil]}, 1} =
             Stream.run_cursor(pid, 0, batch)

    assert %Filo.StmtResult{rows: [[1]]} = result
  end

  test "run_cursor rejects a mismatched seq" do
    pid = start_stream(seq: 5)

    assert {:error, :baton_reused} = Stream.run_cursor(pid, 99, %{"steps" => []})
  end

  test "a close request releases the connection and terminates the stream" do
    pid = start_stream(seq: 0)
    ref = Process.monitor(pid)

    assert {:ok, :closed, [result], nil} = Stream.run(pid, 0, [%{"type" => "close"}])
    assert result == %{"type" => "ok", "response" => %{"type" => "close"}}

    assert_receive {:closed, _conn_ref}
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
  end

  test "the stream idle-expires, closing the connection and stopping" do
    pid = start_stream(seq: 0, idle_timeout: 30)
    ref = Process.monitor(pid)

    assert_receive {:closed, _conn_ref}, 500
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 500
  end

  test "start_link fails with the executor error when the connection cannot open" do
    Process.flag(:trap_exit, true)

    assert {:error, {:open_failed, %Filo.Error{code: "FILO_OPEN"}}} =
             Stream.start_link(executor: Echo, open_arg: {:fail, self()}, seq: 0)
  end

  # fathom expert review 2026-07-24 #22: a stream is idle-dominant by construction (a WebSocket
  # stream lives for hours between requests) while holding the executor handle, its statement cache,
  # and a heap grown to the largest result set it ever materialized. start_link/1 used to swallow
  # every option except :name, so a host could not set a GC or hibernation policy at all.
  test "hibernate_after and spawn_opt are passed through to the GenServer" do
    pid = start_stream(hibernate_after: 50, spawn_opt: [fullsweep_after: 0])

    {:garbage_collection, gc} = Process.info(pid, :garbage_collection)
    assert Keyword.fetch!(gc, :fullsweep_after) == 0, "spawn_opt was swallowed"

    # The stream still serves normally with a hibernation policy set — hibernate preserves both
    # the GenServer state and the process dictionary, so the connection and seq survive.
    assert {:ok, :open, [_], 1} = Stream.run(pid, 0, [execute_req()])
  end

  test "the idle timer is re-armed per request without a synchronous cancel" do
    pid = start_stream(idle_timeout: 60_000)

    # Several requests in a row: each re-arms. The assertion is simply that the stream stays
    # healthy and the timer keeps a fresh deadline — an async cancel must not drop or duplicate it.
    for seq <- 0..2 do
      assert {:ok, :open, [_], _} = Stream.run(pid, seq, [execute_req()])
    end

    timer = :sys.get_state(pid).timer
    assert is_reference(timer)
    assert Process.read_timer(timer) > 0, "the re-armed idle timer must carry a live deadline"
  end

  describe "shutdown and exit signals (fathom expert review 2026-10-08 #30)" do
    # Symptom: the stream did not trap exits, so a supervisor shutdown (SIGTERM, application stop)
    # killed it without running terminate/2 — the executor's close/1 never ran, and the host lost
    # its rollback-and-release of the connection. Invariant: a supervisor stop closes the conn.
    test "a supervisor shutdown runs the executor's close/1, exactly once" do
      pid = start_stream(seq: 0)
      ref = Process.monitor(pid)

      :ok = stop_supervised(Stream)

      assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}
      # close/1 sends before terminate/2 returns, so it is already in the mailbox by the DOWN.
      assert_received {:closed, _conn_ref}, "a supervisor shutdown must close the connection"
      refute_received {:closed, _}, "the connection was closed twice"
    end

    test "shutting down the Filo.Streams supervisor closes every open stream's connection" do
      name = Module.concat(__MODULE__, "Streams#{System.unique_integer([:positive])}")
      start_supervised!({Filo.Streams, name: name})

      for _ <- 1..3 do
        {:ok, _id, _seq, _pid} =
          Filo.Streams.create(name, executor: Echo, open_arg: self(), seq: 0)
      end

      :ok = stop_supervised(Filo.Streams)

      for _ <- 1..3, do: assert_received({:closed, _conn_ref})
      refute_received {:closed, _}
    end

    test "a close request followed by the process exiting closes the connection only once" do
      pid = start_stream(seq: 0)
      ref = Process.monitor(pid)

      assert {:ok, :closed, _, nil} = Stream.run(pid, 0, [%{"type" => "close"}])

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
      assert_received {:closed, _conn_ref}
      refute_received {:closed, _}, "terminate/2 closed a connection the close request released"
    end

    # Trapping exits must not change what a linked process's exit does to the stream: an abnormal
    # exit still takes it down with the same reason (now closing the connection first), and a
    # normal exit is still ignored.
    @tag :capture_log
    test "a linked process exiting abnormally stops the stream with that reason, closing the conn" do
      pid = start_stream(seq: 0)
      ref = Process.monitor(pid)

      spawn(fn ->
        Process.link(pid)
        exit(:boom)
      end)

      assert_receive {:DOWN, ^ref, :process, ^pid, :boom}
      assert_received {:closed, _conn_ref}
      refute_received {:closed, _}
    end

    test "a linked process exiting normally leaves the stream running" do
      pid = start_stream(seq: 0)
      linked = spawn(fn -> Process.link(pid) end)
      lref = Process.monitor(linked)
      assert_receive {:DOWN, ^lref, :process, ^linked, _}

      _ = :sys.get_state(pid)
      assert {:ok, :open, [_], 1} = Stream.run(pid, 0, [execute_req()])
      refute_received {:closed, _}
    end
  end

  # fathom expert review 2026-10-08 #27. Symptom: handle_info/2 matched only :expire and :DOWN, so
  # any other message — fathom hit a late watchdog `{:timed_out, ref}` — crashed the stream with a
  # FunctionClauseError and lost the client's open transaction. Invariant: unknown messages are
  # ignored and the stream keeps serving on the same baton.
  test "an unexpected message does not kill the stream" do
    pid = start_stream(seq: 0)
    ref = Process.monitor(pid)

    send(pid, {:timed_out, make_ref()})
    send(pid, :some_stray_message)

    assert {:ok, :open, [_], 1} = Stream.run(pid, 0, [execute_req()])
    refute_received {:DOWN, ^ref, :process, ^pid, _}
    refute_received {:closed, _}
  end
end
