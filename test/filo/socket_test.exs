defmodule Filo.SocketTest.Echo do
  @moduledoc false
  @behaviour Filo.Executor

  @impl true
  def open(test) when is_pid(test), do: {:ok, {test, make_ref()}}

  @impl true
  def execute({_test, _ref}, %Filo.Stmt{sql: "BOOM"}),
    do: {:error, %Filo.Error{message: "boom", code: "SQLITE_ERROR"}}

  def execute({_test, _ref}, %Filo.Stmt{}), do: {:ok, %Filo.StmtResult{cols: ["n"], rows: [[1]]}}

  @impl true
  def autocommit?({_test, _ref}), do: true

  @impl true
  def close({test, ref}) do
    send(test, {:closed, ref})
    :ok
  end

  @impl true
  def execute_sequence({_test, _ref}, _sql), do: :ok
end

defmodule Filo.SocketTest do
  use ExUnit.Case, async: true

  alias Filo.Socket
  alias Filo.SocketTest.Echo

  defp open_socket do
    {:ok, state} = Socket.init(executor: Echo, open_arg: self())
    state
  end

  # A socket with small per-connection resource caps, so the DoS-cap tests are fast (#23).
  defp capped_socket(opts) do
    {:ok, state} = Socket.init([executor: Echo, open_arg: self()] ++ opts)
    state
  end

  defp send_msg(state, msg), do: Socket.handle_in({Jason.encode!(msg), [opcode: :text]}, state)
  defp pushed({:push, {:text, json}, state}), do: {Jason.decode!(json), state}
  defp req(id, request), do: %{"type" => "request", "request_id" => id, "request" => request}

  defp hello(state) do
    {%{"type" => "hello_ok"}, state} = pushed(send_msg(state, %{"type" => "hello", "jwt" => nil}))
    state
  end

  defp open_stream(state, sid) do
    {%{"type" => "response_ok", "response" => %{"type" => "open_stream"}}, state} =
      pushed(send_msg(state, req(sid * 100, %{"type" => "open_stream", "stream_id" => sid})))

    state
  end

  test "hello is answered with hello_ok" do
    assert {%{"type" => "hello_ok"}, _state} =
             pushed(send_msg(open_socket(), %{"type" => "hello", "jwt" => nil}))
  end

  test "open_stream, execute, then close_stream over one stream" do
    state = open_socket() |> hello() |> open_stream(1)

    exec = %{"type" => "execute", "stream_id" => 1, "stmt" => %{"sql" => "SELECT 1"}}
    {resp, state} = pushed(send_msg(state, req(2, exec)))
    assert resp["type"] == "response_ok"
    assert resp["request_id"] == 2
    assert resp["response"]["type"] == "execute"
    assert resp["response"]["result"]["rows"] == [[%{"type" => "integer", "value" => "1"}]]

    {close, _state} =
      pushed(send_msg(state, req(3, %{"type" => "close_stream", "stream_id" => 1})))

    assert close["response"] == %{"type" => "close_stream"}
    assert_receive {:closed, _ref}
  end

  test "execute on an unknown stream is a response_error" do
    state = open_socket() |> hello()

    exec = %{"type" => "execute", "stream_id" => 99, "stmt" => %{"sql" => "SELECT 1"}}
    {resp, _state} = pushed(send_msg(state, req(1, exec)))
    assert resp["type"] == "response_error"
    assert resp["request_id"] == 1
    assert resp["error"]["code"] == "STREAM_NOT_FOUND"
  end

  test "a statement error becomes a response_error" do
    state = open_socket() |> hello() |> open_stream(1)

    exec = %{"type" => "execute", "stream_id" => 1, "stmt" => %{"sql" => "BOOM"}}
    {resp, _state} = pushed(send_msg(state, req(2, exec)))
    assert resp["type"] == "response_error"
    assert resp["error"] == %{"message" => "boom", "code" => "SQLITE_ERROR"}
  end

  test "batch runs over a stream" do
    state = open_socket() |> hello() |> open_stream(1)

    batch = %{
      "type" => "batch",
      "stream_id" => 1,
      "batch" => %{"steps" => [%{"stmt" => %{"sql" => "SELECT 1"}}]}
    }

    {resp, _state} = pushed(send_msg(state, req(2, batch)))
    assert resp["response"]["type"] == "batch"
    assert [%{"affected_row_count" => _} | _] = resp["response"]["result"]["step_results"]
  end

  test "sequence runs a script over a stream" do
    state = open_socket() |> hello() |> open_stream(1)

    seq = %{"type" => "sequence", "stream_id" => 1, "sql" => "CREATE TABLE t(x); SELECT 1"}
    {resp, _state} = pushed(send_msg(state, req(2, seq)))
    assert resp["type"] == "response_ok"
    assert resp["response"] == %{"type" => "sequence"}
  end

  test "a stored SQL can be executed by sql_id" do
    state = open_socket() |> hello() |> open_stream(1)

    {store, state} =
      pushed(
        send_msg(state, req(2, %{"type" => "store_sql", "sql_id" => 10, "sql" => "SELECT 1"}))
      )

    assert store["response"] == %{"type" => "store_sql"}

    exec = %{"type" => "execute", "stream_id" => 1, "stmt" => %{"sql_id" => 10}}
    {resp, _state} = pushed(send_msg(state, req(3, exec)))
    assert resp["response"]["result"]["rows"] == [[%{"type" => "integer", "value" => "1"}]]
  end

  test "a cursor runs a batch and fetches its entries incrementally, then closes" do
    state = open_socket() |> hello() |> open_stream(1)

    open = %{
      "type" => "open_cursor",
      "stream_id" => 1,
      "cursor_id" => 7,
      "batch" => %{"steps" => [%{"stmt" => %{"sql" => "SELECT 1"}}]}
    }

    {opened, state} = pushed(send_msg(state, req(2, open)))
    assert opened["response"] == %{"type" => "open_cursor"}

    # first fetch: step_begin + row, more remain
    fetch = %{"type" => "fetch_cursor", "cursor_id" => 7, "max_count" => 2}
    {f1, state} = pushed(send_msg(state, req(3, fetch)))
    assert f1["response"]["type"] == "fetch_cursor"
    assert f1["response"]["done"] == false
    assert [%{"type" => "step_begin"}, %{"type" => "row"}] = f1["response"]["entries"]

    # second fetch: step_end, done
    {f2, state} = pushed(send_msg(state, req(4, %{fetch | "max_count" => 10})))
    assert [%{"type" => "step_end"}] = f2["response"]["entries"]
    assert f2["response"]["done"] == true

    {closed, _state} =
      pushed(send_msg(state, req(5, %{"type" => "close_cursor", "cursor_id" => 7})))

    assert closed["response"] == %{"type" => "close_cursor"}
  end

  test "open_cursor on an unknown stream is a response_error" do
    state = open_socket() |> hello()

    open = %{
      "type" => "open_cursor",
      "stream_id" => 99,
      "cursor_id" => 1,
      "batch" => %{"steps" => []}
    }

    {resp, _state} = pushed(send_msg(state, req(1, open)))
    assert resp["type"] == "response_error"
    assert resp["error"]["code"] == "STREAM_NOT_FOUND"
  end

  # Expert review 2026-09-05 #23: per-connection resource caps against an unbounded-growth DoS.
  test "store_sql past :max_sqls is refused with SQL_LIMIT" do
    state = capped_socket(max_sqls: 2) |> hello() |> open_stream(1)

    # store two (the cap), each accepted, keeping the accumulated state
    state =
      Enum.reduce(1..2, state, fn id, st ->
        {ok, st} =
          pushed(
            send_msg(
              st,
              req(id, %{"type" => "store_sql", "sql_id" => id, "sql" => "SELECT #{id}"})
            )
          )

        assert ok["response"] == %{"type" => "store_sql"}
        st
      end)

    {resp, _} =
      pushed(
        send_msg(state, req(3, %{"type" => "store_sql", "sql_id" => 3, "sql" => "SELECT 3"}))
      )

    assert resp["type"] == "response_error"
    assert resp["error"]["code"] == "SQL_LIMIT"
  end

  test "store_sql past :max_sql_bytes is refused with SQL_LIMIT" do
    state = capped_socket(max_sql_bytes: 10) |> hello() |> open_stream(1)

    {ok, state} =
      pushed(
        send_msg(state, req(1, %{"type" => "store_sql", "sql_id" => 1, "sql" => "SELECT 1"}))
      )

    assert ok["response"] == %{"type" => "store_sql"}

    # 8 stored + 9 more > 10 → refused on total bytes, not count
    {resp, _} =
      pushed(
        send_msg(state, req(2, %{"type" => "store_sql", "sql_id" => 2, "sql" => "SELECT 22"}))
      )

    assert resp["type"] == "response_error"
    assert resp["error"]["code"] == "SQL_LIMIT"
  end

  test "open_cursor past :max_cursors is refused with CURSOR_LIMIT" do
    state = capped_socket(max_cursors: 1) |> hello() |> open_stream(1)

    open = fn cid ->
      %{
        "type" => "open_cursor",
        "stream_id" => 1,
        "cursor_id" => cid,
        "batch" => %{"steps" => [%{"stmt" => %{"sql" => "SELECT 1"}}]}
      }
    end

    {o1, state} = pushed(send_msg(state, req(2, open.(1))))
    assert o1["response"] == %{"type" => "open_cursor"}

    {resp, _} = pushed(send_msg(state, req(3, open.(2))))
    assert resp["type"] == "response_error"
    assert resp["error"]["code"] == "CURSOR_LIMIT"
  end

  test "fetch_cursor on an unknown cursor is a response_error" do
    state = open_socket() |> hello()

    fetch = %{"type" => "fetch_cursor", "cursor_id" => 123, "max_count" => 5}
    {resp, _state} = pushed(send_msg(state, req(1, fetch)))
    assert resp["type"] == "response_error"
    assert resp["error"]["code"] == "CURSOR_NOT_FOUND"
  end

  test "opening a stream id that already exists is a response_error" do
    state = open_socket() |> hello() |> open_stream(1)

    {resp, _state} =
      pushed(send_msg(state, req(2, %{"type" => "open_stream", "stream_id" => 1})))

    assert resp["type"] == "response_error"
    assert resp["error"]["code"] == "STREAM_EXISTS"
  end

  test "a request before hello stops the connection" do
    assert {:stop, :normal, _state} =
             send_msg(open_socket(), req(1, %{"type" => "open_stream", "stream_id" => 1}))
  end

  # RFC 6455 §7.4.1: a text frame whose payload isn't valid UTF-8 (or otherwise isn't consistent
  # with the message type) closes with 1007, not 1000. Bandit's `validate_text_frames` used to emit
  # that close by pre-walking every inbound frame's bytes, immediately before Jason walks the same
  # bytes again. A host that disables the duplicate scan lands the failure HERE instead, so this is
  # what keeps the close code identical either way — i.e. what makes disabling it conformance-
  # neutral rather than a silent spec deviation.
  test "a malformed text frame closes with 1007, not a bare 1000" do
    assert {:stop, :normal, {1007, _}, _state} =
             Socket.handle_in({"{not json", [opcode: :text]}, open_socket())
  end

  test "an invalid-UTF-8 text frame also closes with 1007" do
    # The exact case Bandit's pre-scan was catching: a lone continuation byte.
    assert {:stop, :normal, {1007, _}, _state} =
             Socket.handle_in({<<0x22, 0x80, 0x22>>, [opcode: :text]}, open_socket())
  end

  test "terminate releases every open stream's connection" do
    state = open_socket() |> hello() |> open_stream(1) |> open_stream(2)

    assert :ok = Socket.terminate(:normal, state)
    assert_receive {:closed, _ref1}
    assert_receive {:closed, _ref2}
  end
end
