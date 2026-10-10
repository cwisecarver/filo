defmodule Filo.PlugHardeningTest.Exec do
  @moduledoc false
  @behaviour Filo.Executor

  @impl true
  def open(test), do: {:ok, {test, make_ref()}}

  # "SLEEP <ms>" blocks the stream process for that long, like a slow statement.
  @impl true
  def execute({_test, _ref}, %Filo.Stmt{sql: "SLEEP " <> ms}) do
    Process.sleep(String.to_integer(ms))
    {:ok, %Filo.StmtResult{cols: ["n"], rows: [[1]]}}
  end

  def execute({_test, _ref}, %Filo.Stmt{}), do: {:ok, %Filo.StmtResult{cols: ["n"], rows: [[1]]}}

  @impl true
  def autocommit?({_test, _ref}), do: true

  @impl true
  def close({test, ref}) do
    send(test, {:closed, ref})
    :ok
  end
end

defmodule Filo.PlugHardeningTest do
  # Shares a globally-named Filo.Streams supervisor, so not concurrent.
  use ExUnit.Case, async: false

  import Plug.Test
  import Plug.Conn

  alias Filo.PlugHardeningTest.Exec

  @streams __MODULE__.Streams

  setup do
    start_supervised!({Filo.Streams, name: @streams})
    %{key: Filo.Baton.new_key()}
  end

  defp plug_opts(key, extra \\ []) do
    Filo.Plug.init([executor: Exec, streams: @streams, key: key, open_arg: self()] ++ extra)
  end

  defp post(opts, path, body, content_type \\ "application/json") do
    body = if is_binary(body), do: body, else: Jason.encode!(body)

    conn(:post, path, body)
    |> put_req_header("content-type", content_type)
    |> Filo.Plug.call(opts)
  end

  defp exec_req(sql), do: %{"type" => "execute", "stmt" => %{"sql" => sql}}
  defp decoded(conn), do: Jason.decode!(conn.resp_body)

  # H1 (fathom panel2 2026-10-10): Stream.run used GenServer.call's default 5 s, shorter than
  # fathom's 30 s statement deadline, so a slow-but-fine statement was answered 400
  # STREAM_NOT_FOUND while it could still commit.
  describe "call timeout" do
    test "a slow request is not cut off by a default timeout", %{key: key} do
      opts = plug_opts(key)
      # No :call_timeout configured => :infinity. The old fixed 5 s is too slow to wait out in a
      # test, so assert the configured default and that a slow statement still completes.
      assert opts.call_timeout == :infinity
      conn = post(opts, "/v3/pipeline", %{"requests" => [exec_req("SLEEP 300")]})
      assert conn.status == 200
    end

    test "an expired :call_timeout is a distinct 504 and shuts the stream down", %{key: key} do
      opts = plug_opts(key, call_timeout: 50)
      conn = post(opts, "/v3/pipeline", %{"requests" => [exec_req("SLEEP 400")]})

      assert conn.status == 504
      assert decoded(conn)["code"] == "FILO_STREAM_TIMEOUT"
      # The stream traps exits: the shutdown runs terminate/2 once the slow request returns.
      assert_receive {:closed, _ref}, 2_000
    end

    test "a stream that is simply gone is still 400 STREAM_NOT_FOUND", %{key: key} do
      opts = plug_opts(key, idle_timeout: 20)
      first = post(opts, "/v3/pipeline", %{"requests" => [exec_req("SELECT 1")]})
      baton = decoded(first)["baton"]
      assert_receive {:closed, _ref}, 2_000

      conn = post(opts, "/v3/pipeline", %{"baton" => baton, "requests" => [exec_req("SELECT 1")]})
      assert conn.status == 400
      assert decoded(conn)["code"] == "STREAM_NOT_FOUND"
    end
  end
end
