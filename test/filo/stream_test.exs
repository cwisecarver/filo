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
end
