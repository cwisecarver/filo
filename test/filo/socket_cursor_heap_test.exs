defmodule Filo.SocketCursorHeapTest.Big do
  @moduledoc false
  @behaviour Filo.Executor

  @impl true
  def open(test) when is_pid(test), do: {:ok, {test, make_ref()}}

  @impl true
  def execute({_test, _ref}, %Filo.Stmt{}) do
    row = for i <- 1..8, do: "cell-#{i}-" <> String.duplicate("x", 20)
    {:ok, %Filo.StmtResult{cols: for(i <- 1..8, do: "c#{i}"), rows: List.duplicate(row, 2_000)}}
  end

  @impl true
  def autocommit?({_test, _ref}), do: true

  @impl true
  def close({_test, _ref}), do: :ok

  @impl true
  def execute_sequence({_test, _ref}, _sql), do: :ok
end

defmodule Filo.SocketCursorHeapTest do
  use ExUnit.Case, async: true

  # expert review 2026-10-22 r8 #6: a WebSocket open_cursor keeps its whole result in socket
  # state across fetch_cursor round trips. Stored as unflattened iodata (rows: :json_iodata) it
  # was ~6x the heap of per-row flattened binaries and was copied by every major GC of the
  # connection process. The held entries must be per-row binaries (off-heap refc/heap binaries,
  # not deep iolists).

  alias Filo.Socket
  alias Filo.SocketCursorHeapTest.Big

  defp send_msg(state, msg), do: Socket.handle_in({Jason.encode!(msg), [opcode: :text]}, state)
  defp pushed({:push, {:text, json}, state}), do: {Jason.decode!(json), state}

  defp req(id, request), do: %{"type" => "request", "request_id" => id, "request" => request}

  test "a held cursor's entries are flattened rows, not unflattened iodata" do
    {:ok, state} = Socket.init(executor: Big, open_arg: self())
    {_, state} = pushed(send_msg(state, %{"type" => "hello", "jwt" => nil}))

    {_, state} =
      pushed(send_msg(state, req(1, %{"type" => "open_stream", "stream_id" => 1})))

    open = %{
      "type" => "open_cursor",
      "stream_id" => 1,
      "cursor_id" => 7,
      "batch" => %{"steps" => [%{"stmt" => %{"sql" => "SELECT"}}]}
    }

    {%{"type" => "response_ok"}, state} = pushed(send_msg(state, req(2, open)))

    words = :erts_debug.size(state.cursors)
    # Flattened: ~2000 rows x (Fragment struct + a ~6 word proc-bin) ~ 40k words. The iodata
    # form measured ~180k+ words here.
    assert words < 80_000, "held cursor is #{words} words; rows were not flattened"

    # and it still serves every row
    fetch = %{"type" => "fetch_cursor", "cursor_id" => 7, "max_count" => 5_000}
    {f, _state} = pushed(send_msg(state, req(3, fetch)))
    assert length(f["response"]["entries"]) == 2_002
  end
end
