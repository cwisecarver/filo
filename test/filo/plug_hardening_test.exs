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

  # H8 (fathom panel2 2026-10-10): decode failures raised in the plug or inside the stream process
  # (killing the stream and its open transaction). Invariant: a malformed body is a 400
  # FILO_BAD_REQUEST and a baton'd stream survives it.
  describe "malformed requests" do
    @bad_value %{"type" => "integer", "value" => "not-a-number"}

    defp assert_bad_request(conn) do
      assert conn.status == 400, inspect({conn.status, conn.resp_body})
      assert decoded(conn)["code"] == "FILO_BAD_REQUEST"
    end

    test "v1 execute and batch with undecodable values", %{key: key} do
      opts = plug_opts(key)

      assert_bad_request(
        post(opts, "/v1/execute", %{"stmt" => %{"sql" => "SELECT ?", "args" => [@bad_value]}})
      )

      assert_bad_request(
        post(opts, "/v1/batch", %{
          "batch" => %{"steps" => [%{"stmt" => %{"sql" => "SELECT ?", "args" => [@bad_value]}}]}
        })
      )
    end

    test "a pipeline whose requests is not a list, or whose baton is not a string", %{key: key} do
      opts = plug_opts(key)
      assert_bad_request(post(opts, "/v3/pipeline", %{"requests" => "SELECT 1"}))
      assert_bad_request(post(opts, "/v3/pipeline", %{"baton" => 5, "requests" => []}))
      assert_bad_request(post(opts, "/v3/cursor", %{"baton" => 5, "batch" => %{"steps" => []}}))
    end

    test "a bad stmt inside a pipeline is a per-request error and the stream survives", %{
      key: key
    } do
      opts = plug_opts(key)

      bad = %{"type" => "execute", "stmt" => %{"sql" => "SELECT ?", "args" => [@bad_value]}}
      first = post(opts, "/v3/pipeline", %{"requests" => [bad, "junk", exec_req("SELECT 1")]})
      assert first.status == 200
      [r1, r2, r3] = decoded(first)["results"]
      assert r1["error"]["code"] == "FILO_BAD_REQUEST"
      assert r2["error"]["code"] == "FILO_BAD_REQUEST"
      assert r3["type"] == "ok"

      next =
        post(opts, "/v3/pipeline", %{
          "baton" => decoded(first)["baton"],
          "requests" => [exec_req("SELECT 1")]
        })

      assert next.status == 200
    end

    test "a cursor with an undecodable batch is a 400 and the stream survives", %{key: key} do
      opts = plug_opts(key)
      first = post(opts, "/v3/pipeline", %{"requests" => [exec_req("SELECT 1")]})
      baton = decoded(first)["baton"]

      bad = %{"steps" => [%{"stmt" => %{"sql" => "SELECT ?", "args" => [@bad_value]}}]}
      assert_bad_request(post(opts, "/v3/cursor", %{"baton" => baton, "batch" => bad}))

      # The same baton still works: the stream was not killed and its seq was not consumed.
      ok = post(opts, "/v3/pipeline", %{"baton" => baton, "requests" => [exec_req("SELECT 1")]})
      assert ok.status == 200
    end

    test "garbage protobuf bodies are 400, not a crash", %{key: key} do
      opts = plug_opts(key)

      for path <- ["/v3-protobuf/pipeline", "/v3-protobuf/cursor"],
          body <- [<<255, 255, 255>>, <<10, 200, 1>>, <<8>>] do
        conn = post(opts, path, body, "application/x-protobuf")
        assert conn.status == 400, "#{path} #{inspect(body)} -> #{conn.status}"
      end
    end
  end

  # H7 (fathom panel2 2026-10-10): the 8 MB constant was passed as read_body's per-call length, but
  # `{:more, ...}` was looped on with a growing accumulator, so it bounded each read, not the body.
  # Invariant: a body over the cap is a 413 with a Hrana-shaped error, before auth and the stream.
  describe "body cap" do
    test "an oversized JSON body is 413 and never reaches the executor", %{key: key} do
      opts =
        plug_opts(key, max_body: 1_000, authorize: fn _arg, _token -> flunk("authorized") end)

      big = Jason.encode!(%{"requests" => [exec_req(String.duplicate("x", 5_000))]})

      conn = post(opts, "/v3/pipeline", big)
      assert conn.status == 413
      assert %{"message" => _, "code" => "FILO_BODY_TOO_LARGE"} = decoded(conn)
      refute_received {:closed, _}
    end

    test "an oversized protobuf body is 413 too", %{key: key} do
      opts = plug_opts(key, max_body: 1_000)

      conn =
        post(
          opts,
          "/v3-protobuf/pipeline",
          :crypto.strong_rand_bytes(5_000),
          "application/x-protobuf"
        )

      assert conn.status == 413
    end

    test "a body at the cap is accepted", %{key: key} do
      opts = plug_opts(key, max_body: 1_000)
      body = Jason.encode!(%{"requests" => [exec_req("SELECT 1")]})
      assert byte_size(body) < 1_000
      assert post(opts, "/v3/pipeline", body).status == 200
    end
  end

  # H9 (fathom panel2 2026-10-10): HTTP has no stored-SQL store, so a statement carrying only a
  # sql_id reached the executor with sql: nil. Invariant: it is refused with FILO_UNSUPPORTED before
  # the executor, and (in a pipeline) the stream and baton survive.
  describe "sql_id over HTTP" do
    @by_id %{"type" => "execute", "stmt" => %{"sql_id" => 3}}

    test "a pipeline execute is a per-request FILO_UNSUPPORTED error", %{key: key} do
      opts = plug_opts(key)
      first = post(opts, "/v3/pipeline", %{"requests" => [@by_id, exec_req("SELECT 1")]})
      assert first.status == 200
      [bad, good] = decoded(first)["results"]
      assert bad["error"]["code"] == "FILO_UNSUPPORTED"
      assert bad["error"]["message"] =~ "WebSocket"
      assert good["type"] == "ok"
    end

    test "a batch step is a step error", %{key: key} do
      opts = plug_opts(key)
      batch = %{"steps" => [%{"stmt" => %{"sql_id" => 3}}]}
      conn = post(opts, "/v3/pipeline", %{"requests" => [%{"type" => "batch", "batch" => batch}]})
      [%{"response" => %{"result" => result}}] = decoded(conn)["results"]
      assert [%{"code" => "FILO_UNSUPPORTED"}] = result["step_errors"]
    end

    test "v1 execute is a 400 FILO_UNSUPPORTED", %{key: key} do
      conn = post(plug_opts(key), "/v1/execute", %{"stmt" => %{"sql_id" => 3}})
      assert conn.status == 400
      assert decoded(conn)["code"] == "FILO_UNSUPPORTED"
    end
  end
end
