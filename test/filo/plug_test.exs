defmodule Filo.PlugTest.Echo do
  @moduledoc false
  @behaviour Filo.Executor

  @impl true
  def open(_arg), do: {:ok, :conn}

  @impl true
  def execute(:conn, %Filo.Stmt{sql: "BOOM"}),
    do: {:error, %Filo.Error{message: "boom", code: "SQLITE_ERROR"}}

  @impl true
  def execute(:conn, %Filo.Stmt{}), do: {:ok, %Filo.StmtResult{cols: ["n"], rows: [[1]]}}

  @impl true
  def autocommit?(:conn), do: true

  @impl true
  def close(:conn), do: :ok
end

defmodule Filo.PlugTest.FailOpen do
  @moduledoc false
  # An executor whose open/1 refuses with a client-error Filo.Error (status 400) — e.g. a missing
  # or invalid shard. The pipeline path must surface that status, not a blanket 500.
  @behaviour Filo.Executor

  @impl true
  def open(_arg),
    do: {:error, %Filo.Error{message: "no shard specified", code: "FILO_NO_SHARD", status: 400}}

  @impl true
  def execute(_conn, _stmt), do: {:ok, %Filo.StmtResult{}}

  @impl true
  def autocommit?(_conn), do: true

  @impl true
  def close(_conn), do: :ok
end

defmodule Filo.PlugTest do
  # Shares a globally-named Filo.Streams supervisor, so not concurrent.
  use ExUnit.Case, async: false

  import Plug.Test
  import Plug.Conn

  alias Filo.PlugTest.Echo

  @streams __MODULE__.Streams
  @base_url "http://localhost:8080"

  setup do
    start_supervised!({Filo.Streams, name: @streams})

    opts =
      Filo.Plug.init(
        executor: Echo,
        streams: @streams,
        key: Filo.Baton.new_key(),
        base_url: @base_url
      )

    %{opts: opts}
  end

  defp post_pipeline(opts, body) do
    conn(:post, "/v3/pipeline", Jason.encode!(body))
    |> put_req_header("content-type", "application/json")
    |> Filo.Plug.call(opts)
  end

  defp decoded(conn), do: Jason.decode!(conn.resp_body)

  defp execute_req, do: %{"type" => "execute", "stmt" => %{"sql" => "SELECT 1"}}

  test "GET /v3 reports protocol support", %{opts: opts} do
    conn = Filo.Plug.call(conn(:get, "/v3"), opts)
    assert conn.status == 200
  end

  test "GET /v2 also reports protocol support", %{opts: opts} do
    conn = Filo.Plug.call(conn(:get, "/v2"), opts)
    assert conn.status == 200
  end

  test "POST /v2/pipeline runs the same pipeline as v3", %{opts: opts} do
    conn =
      conn(:post, "/v2/pipeline", Jason.encode!(%{"baton" => nil, "requests" => [execute_req()]}))
      |> put_req_header("content-type", "application/json")
      |> Filo.Plug.call(opts)

    assert conn.status == 200
    resp = decoded(conn)
    assert is_binary(resp["baton"])
    assert [%{"type" => "ok", "response" => %{"type" => "execute"}}] = resp["results"]
  end

  test "POST /v3/pipeline with no baton opens a stream and runs the pipeline", %{opts: opts} do
    conn = post_pipeline(opts, %{"baton" => nil, "requests" => [execute_req()]})

    assert conn.status == 200
    assert ["application/json" <> _] = get_resp_header(conn, "content-type")

    resp = decoded(conn)
    assert is_binary(resp["baton"])
    assert resp["base_url"] == @base_url

    assert [%{"type" => "ok", "response" => response}] = resp["results"]
    assert response["type"] == "execute"
    assert response["result"]["rows"] == [[%{"type" => "integer", "value" => "1"}]]
  end

  test "a stream open that fails with a Filo.Error status surfaces that status (400, not 500)" do
    opts =
      Filo.Plug.init(
        executor: Filo.PlugTest.FailOpen,
        streams: @streams,
        key: Filo.Baton.new_key(),
        base_url: @base_url
      )

    conn = post_pipeline(opts, %{"baton" => nil, "requests" => [execute_req()]})

    assert conn.status == 400, "the executor's client-error status must propagate, not become 500"
    assert %{"code" => "FILO_NO_SHARD", "message" => "no shard specified"} = decoded(conn)
  end

  test "POST /v1/execute runs a single statement statelessly", %{opts: opts} do
    conn =
      conn(:post, "/v1/execute", Jason.encode!(%{"stmt" => %{"sql" => "SELECT 1"}}))
      |> put_req_header("content-type", "application/json")
      |> Filo.Plug.call(opts)

    assert conn.status == 200
    assert decoded(conn)["result"]["rows"] == [[%{"type" => "integer", "value" => "1"}]]
  end

  test "POST /v1/batch runs a batch statelessly", %{opts: opts} do
    body = %{"batch" => %{"steps" => [%{"stmt" => %{"sql" => "SELECT 1"}}]}}

    conn =
      conn(:post, "/v1/batch", Jason.encode!(body))
      |> put_req_header("content-type", "application/json")
      |> Filo.Plug.call(opts)

    assert conn.status == 200
    assert [%{"affected_row_count" => _} | _] = decoded(conn)["result"]["step_results"]
  end

  test "POST /v1/execute surfaces a SQL error as a 400 Hrana error", %{opts: opts} do
    conn =
      conn(:post, "/v1/execute", Jason.encode!(%{"stmt" => %{"sql" => "BOOM"}}))
      |> put_req_header("content-type", "application/json")
      |> Filo.Plug.call(opts)

    assert conn.status == 400
    assert decoded(conn)["code"] == "SQLITE_ERROR"
  end

  test "the returned baton authorizes the next request on the same stream", %{opts: opts} do
    first = decoded(post_pipeline(opts, %{"baton" => nil, "requests" => [execute_req()]}))
    baton1 = first["baton"]

    second = decoded(post_pipeline(opts, %{"baton" => baton1, "requests" => [execute_req()]}))
    assert is_binary(second["baton"])
    assert second["baton"] != baton1
    assert [%{"type" => "ok"}] = second["results"]
  end

  test "replaying a spent baton is rejected", %{opts: opts} do
    first = decoded(post_pipeline(opts, %{"baton" => nil, "requests" => [execute_req()]}))
    baton1 = first["baton"]

    # consume baton1 on the next request, which rotates the stream's seq
    _ = post_pipeline(opts, %{"baton" => baton1, "requests" => [execute_req()]})

    # replaying baton1 now presents a stale seq
    replay = post_pipeline(opts, %{"baton" => baton1, "requests" => [execute_req()]})
    assert replay.status == 400
  end

  test "a close request returns a null baton and frees the stream", %{opts: opts} do
    first = decoded(post_pipeline(opts, %{"baton" => nil, "requests" => [execute_req()]}))
    baton1 = first["baton"]

    closed =
      decoded(post_pipeline(opts, %{"baton" => baton1, "requests" => [%{"type" => "close"}]}))

    assert closed["baton"] == nil
    assert [%{"type" => "ok", "response" => %{"type" => "close"}}] = closed["results"]

    # the stream is gone: a request for it now fails
    gone = post_pipeline(opts, %{"baton" => baton1, "requests" => [execute_req()]})
    assert gone.status == 400
  end

  test "a malformed baton is rejected", %{opts: opts} do
    conn = post_pipeline(opts, %{"baton" => "not-a-real-baton", "requests" => [execute_req()]})
    assert conn.status == 400
  end

  test "a baton signed with a different key is rejected", %{opts: opts} do
    forged = Filo.Baton.encode(1, 1, Filo.Baton.new_key())
    conn = post_pipeline(opts, %{"baton" => forged, "requests" => [execute_req()]})
    assert conn.status == 400
  end

  test "invalid JSON in the body is a 400", %{opts: opts} do
    conn =
      conn(:post, "/v3/pipeline", "{not json")
      |> put_req_header("content-type", "application/json")
      |> Filo.Plug.call(opts)

    assert conn.status == 400
  end

  test "POST /v3/cursor streams batch results as newline-delimited entries", %{opts: opts} do
    body = %{"baton" => nil, "batch" => %{"steps" => [%{"stmt" => %{"sql" => "SELECT 1"}}]}}

    conn =
      conn(:post, "/v3/cursor", Jason.encode!(body))
      |> put_req_header("content-type", "application/json")
      |> Filo.Plug.call(opts)

    assert conn.status == 200

    [head | entries] =
      conn.resp_body |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)

    assert is_binary(head["baton"])
    assert head["base_url"] == @base_url

    assert [
             %{"type" => "step_begin", "step" => 0, "cols" => [%{"name" => "n"}]},
             %{"type" => "row", "row" => [%{"type" => "integer", "value" => "1"}]},
             %{"type" => "step_end", "affected_row_count" => 0}
           ] = entries
  end

  test "unknown routes are 404", %{opts: opts} do
    assert Filo.Plug.call(conn(:get, "/nope"), opts).status == 404

    unknown =
      conn(:post, "/v9/pipeline", "{}")
      |> put_req_header("content-type", "application/json")
      |> Filo.Plug.call(opts)

    assert unknown.status == 404
  end
end
