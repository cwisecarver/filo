defmodule Filo.PlugInlineOneshotTest.Spy do
  @moduledoc false
  # Reports which process opened/closed the connection, so a test can tell whether a baton-less
  # pipeline ran in the request process or in a Filo.Stream process. `open_arg` is the test pid.
  @behaviour Filo.Executor

  @impl true
  def open(test_pid) when is_pid(test_pid) do
    send(test_pid, {:opened, self()})
    {:ok, test_pid}
  end

  @impl true
  def execute(_conn, %Filo.Stmt{sql: "RAISE"}), do: raise("executor blew up")
  def execute(_conn, %Filo.Stmt{}), do: {:ok, %Filo.StmtResult{cols: ["n"], rows: [[1]]}}

  @impl true
  def autocommit?(_conn), do: true

  @impl true
  def close(test_pid) do
    send(test_pid, {:closed, self()})
    :ok
  end
end

defmodule Filo.PlugInlineOneshotTest do
  # fathom expert review 2026-10-17 #9: a baton-less pipeline containing `close` can never be
  # resumed, so it runs in the request process instead of starting a Filo.Stream. The wire result
  # must be identical; only the process that owns the connection changes.
  use ExUnit.Case, async: false

  import Plug.Test
  import Plug.Conn

  alias Filo.PlugInlineOneshotTest.Spy

  @streams __MODULE__.Streams
  @exec %{"type" => "execute", "stmt" => %{"sql" => "SELECT 1", "args" => []}}
  @close %{"type" => "close"}

  setup do
    start_supervised!({Filo.Streams, name: @streams})
    :ok
  end

  defp plug_opts(extra \\ []) do
    Filo.Plug.init(
      [
        executor: Spy,
        streams: @streams,
        key: Filo.Baton.new_key(),
        open_arg: self(),
        base_url: "http://localhost:8080"
      ] ++ extra
    )
  end

  defp post(opts, requests) do
    conn(:post, "/v3/pipeline", Jason.encode!(%{"requests" => requests}))
    |> put_req_header("content-type", "application/json")
    |> Filo.Plug.call(opts)
  end

  defp decoded(conn), do: Jason.decode!(conn.resp_body)

  defp live_streams, do: Registry.count(Module.concat(@streams, Registry))

  test "execute+close runs in the request process: no stream process, null baton" do
    conn = post(plug_opts(), [@exec, @close])

    assert conn.status == 200
    assert %{"baton" => nil, "results" => [%{"type" => "ok"}, %{"type" => "ok"}]} = decoded(conn)
    me = self()
    assert_received {:opened, ^me}, "opened outside the request process"
    assert_received {:closed, ^me}, "closed outside the request process"
    assert live_streams() == 0
  end

  test "with :inline_oneshot false the same request uses a stream process (the test discriminates)" do
    conn = post(plug_opts(inline_oneshot: false), [@exec, @close])

    assert conn.status == 200
    me = self()
    assert_received {:opened, opener}
    refute opener == me, "inline_oneshot: false still ran inline"
  end

  test "the response is byte-identical inline and via a stream process" do
    inline = post(plug_opts(), [@exec, @close])
    streamed = post(plug_opts(inline_oneshot: false), [@exec, @close])

    assert inline.status == streamed.status
    assert inline.resp_body == streamed.resp_body
  end

  test "a close mid-pipeline halts the rest, as the stream does" do
    conn = post(plug_opts(), [@exec, @close, @exec])

    assert %{"baton" => nil, "results" => results} = decoded(conn)
    assert length(results) == 2
  end

  test "a pipeline with NO close still opens a resumable stream and returns a baton" do
    conn = post(plug_opts(), [@exec])

    assert %{"baton" => baton} = decoded(conn)
    assert is_binary(baton)
    assert live_streams() == 1
  end

  test "a finite :call_timeout keeps the stream process (the inline run could not enforce it)" do
    conn = post(plug_opts(call_timeout: 5_000), [@exec, @close])

    assert conn.status == 200
    me = self()
    assert_received {:opened, opener}
    refute opener == me
  end

  test "an executor that raises mid-pipeline answers STREAM_NOT_FOUND and still closes the connection" do
    boom = %{"type" => "execute", "stmt" => %{"sql" => "RAISE", "args" => []}}
    conn = ExUnit.CaptureLog.with_log(fn -> post(plug_opts(), [boom, @close]) end) |> elem(0)

    assert conn.status == 400
    assert %{"code" => "STREAM_NOT_FOUND"} = decoded(conn)
    me = self()
    assert_received {:closed, ^me}, "the connection leaked after the executor raised"
  end

  test "an open that fails keeps its status and code" do
    defmodule FailOpen do
      @moduledoc false
      @behaviour Filo.Executor
      def open(_),
        do: {:error, %Filo.Error{message: "no shard", code: "FILO_NO_SHARD", status: 400}}

      def execute(_, _), do: {:ok, %Filo.StmtResult{}}
      def autocommit?(_), do: true
      def close(_), do: :ok
    end

    opts =
      Filo.Plug.init(executor: FailOpen, streams: @streams, key: Filo.Baton.new_key())

    conn = post(opts, [@exec, @close])
    assert conn.status == 400
    assert %{"code" => "FILO_NO_SHARD"} = decoded(conn)
  end
end
