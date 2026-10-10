defmodule Filo.StreamExpireTest.Slow do
  @moduledoc false
  @behaviour Filo.Executor

  @impl true
  def open(test), do: {:ok, {test, make_ref()}}

  @impl true
  def execute({_test, _ref}, %Filo.Stmt{sql: "SLEEP " <> ms}) do
    Process.sleep(String.to_integer(ms))
    {:ok, %Filo.StmtResult{cols: ["n"], rows: [[1]]}}
  end

  @impl true
  def autocommit?(_conn), do: true

  @impl true
  def close({test, ref}) do
    send(test, {:closed, ref})
    :ok
  end
end

defmodule Filo.StreamExpireTest do
  use ExUnit.Case, async: true

  alias Filo.Stream
  alias Filo.StreamExpireTest.Slow

  defp sleep_req(ms), do: %{"type" => "execute", "stmt" => %{"sql" => "SLEEP #{ms}"}}

  # Symptom (fathom panel2 2026-10-10 H3): the idle timer fires while a request is running, its
  # :expire message queues behind the request, the request re-arms the timer and acks - then the
  # stale :expire kills the stream, so the client's next baton is STREAM_NOT_FOUND. Invariant: an
  # expiry only counts if it belongs to the CURRENT timer.
  test "a stale expiry queued behind a running request does not kill the stream" do
    pid = start_supervised!({Stream, executor: Slow, open_arg: self(), seq: 0, idle_timeout: 60})

    # Longer than the idle timeout: the pre-run timer fires mid-request.
    assert {:ok, :open, _, 1} = Stream.run(pid, 0, [sleep_req(150)])

    # The stale expiry is already in the mailbox; the new timer (60 ms) is far from due.
    ref = Process.monitor(pid)
    _ = :sys.get_state(pid)
    refute_receive {:DOWN, ^ref, :process, ^pid, _}, 20
    assert {:ok, :open, _, 2} = Stream.run(pid, 1, [sleep_req(0)])
  end

  test "the current expiry still stops the stream and closes the connection" do
    pid = start_supervised!({Stream, executor: Slow, open_arg: self(), seq: 0, idle_timeout: 30})
    ref = Process.monitor(pid)

    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000
    assert_receive {:closed, _}
  end
end
