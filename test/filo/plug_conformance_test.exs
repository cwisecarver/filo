defmodule Filo.PlugConformanceTest.Kv do
  @moduledoc """
  A tiny stateful executor for end-to-end tests: a per-connection key/value
  "table" held in an ETS table, with just enough SQL vocabulary to prove the
  whole stack composes — including cross-request persistence and transactions.

  The connection handle is the ETS table id, created in `open/1`. ETS tables are
  owned by the process that creates them — here the stream process — so the
  state survives across HTTP requests routed back by baton, and is dropped
  automatically when the stream dies. Rows live under `{:row, key}`; the
  autocommit flag under `:autocommit`.
  """
  @behaviour Filo.Executor

  alias Filo.{Error, Stmt, StmtResult}

  @impl true
  def open(_arg) do
    table = :ets.new(:filo_kv, [:set, :public])
    :ets.insert(table, {:autocommit, true})
    {:ok, table}
  end

  @impl true
  def execute(_conn, %Stmt{sql: nil}),
    do: {:error, %Error{message: "no sql", code: "SQLITE_MISUSE"}}

  def execute(conn, %Stmt{sql: sql, args: args}), do: run(conn, String.upcase(sql), sql, args)

  @impl true
  def autocommit?(conn), do: :ets.lookup_element(conn, :autocommit, 2)

  @impl true
  def close(conn) do
    :ets.delete(conn)
    :ok
  end

  defp run(_conn, "CREATE TABLE" <> _, _sql, _args), do: {:ok, %StmtResult{}}

  defp run(conn, "BEGIN" <> _, _sql, _args) do
    :ets.insert(conn, {:autocommit, false})
    {:ok, %StmtResult{}}
  end

  defp run(conn, "COMMIT" <> _, _sql, _args) do
    :ets.insert(conn, {:autocommit, true})
    {:ok, %StmtResult{}}
  end

  defp run(conn, "INSERT" <> _, _sql, [key, value]) when is_integer(key) do
    :ets.insert(conn, {{:row, key}, value})
    {:ok, %StmtResult{affected_row_count: 1, last_insert_rowid: key, rows_written: 1}}
  end

  defp run(conn, "SELECT" <> _, _sql, [key]) when is_integer(key) do
    rows =
      case :ets.lookup(conn, {:row, key}) do
        [{_key, value}] -> [[value]]
        [] -> []
      end

    {:ok, %StmtResult{cols: ["value"], rows: rows, rows_read: length(rows)}}
  end

  defp run(_conn, _upper, sql, _args),
    do: {:error, %Error{message: "unsupported sql: #{sql}", code: "SQLITE_ERROR"}}
end

defmodule Filo.PlugConformanceTest do
  # End-to-end through Filo.Plug against a stateful executor. Uses a
  # globally-named Filo.Streams, so not concurrent.
  use ExUnit.Case, async: false

  import Plug.Test
  import Plug.Conn

  alias Filo.PlugConformanceTest.Kv

  @streams __MODULE__.Streams

  setup do
    start_supervised!({Filo.Streams, name: @streams})
    opts = Filo.Plug.init(executor: Kv, streams: @streams, key: Filo.Baton.new_key())
    %{opts: opts}
  end

  # Runs one pipeline request, asserts a 200, and returns {next_baton, results}.
  defp step(opts, baton, requests) do
    conn =
      conn(:post, "/v3/pipeline", Jason.encode!(%{"baton" => baton, "requests" => requests}))
      |> put_req_header("content-type", "application/json")
      |> Filo.Plug.call(opts)

    assert conn.status == 200
    resp = Jason.decode!(conn.resp_body)
    {resp["baton"], resp["results"]}
  end

  defp execute(sql, args \\ []), do: %{"type" => "execute", "stmt" => stmt(sql, args)}
  defp stmt(sql, args), do: %{"sql" => sql, "args" => args}
  defp int(i), do: %{"type" => "integer", "value" => Integer.to_string(i)}
  defp text(s), do: %{"type" => "text", "value" => s}

  test "a stream's connection state persists across pipeline requests", %{opts: opts} do
    {baton, results} =
      step(opts, nil, [
        execute("CREATE TABLE kv (key, value)"),
        execute("INSERT INTO kv VALUES (?, ?)", [int(1), text("alice")])
      ])

    assert [
             %{"type" => "ok"},
             %{"type" => "ok", "response" => %{"result" => insert}}
           ] = results

    assert insert["affected_row_count"] == 1
    assert insert["last_insert_rowid"] == "1"

    # A separate HTTP request, routed back to the same stream by the baton, sees
    # the row the previous request wrote.
    {_baton, [select]} =
      step(opts, baton, [execute("SELECT value FROM kv WHERE key = ?", [int(1)])])

    assert select["type"] == "ok"
    assert select["response"]["result"]["rows"] == [[%{"type" => "text", "value" => "alice"}]]
  end

  test "a transactional batch commits and the write persists", %{opts: opts} do
    {baton, _} = step(opts, nil, [execute("CREATE TABLE kv (key, value)")])

    # The django-libsql transaction shape: BEGIN, then steps gated on the prior
    # step's success, COMMIT gated on the insert.
    batch = %{
      "type" => "batch",
      "batch" => %{
        "steps" => [
          %{"stmt" => stmt("BEGIN", [])},
          %{
            "stmt" => stmt("INSERT INTO kv VALUES (?, ?)", [int(2), text("bob")]),
            "condition" => %{"type" => "ok", "step" => 0}
          },
          %{"stmt" => stmt("COMMIT", []), "condition" => %{"type" => "ok", "step" => 1}}
        ]
      }
    }

    {baton, [batch_result]} = step(opts, baton, [batch])

    assert batch_result["type"] == "ok"
    assert batch_result["response"]["type"] == "batch"
    result = batch_result["response"]["result"]
    assert length(result["step_results"]) == 3
    assert Enum.all?(result["step_results"], &(&1 != nil))
    assert result["step_errors"] == [nil, nil, nil]

    {_baton, [select]} =
      step(opts, baton, [execute("SELECT value FROM kv WHERE key = ?", [int(2)])])

    assert select["response"]["result"]["rows"] == [[%{"type" => "text", "value" => "bob"}]]
  end

  test "an interactive transaction spans requests and toggles autocommit", %{opts: opts} do
    {baton, _} = step(opts, nil, [execute("CREATE TABLE kv (key, value)")])

    {baton, _} = step(opts, baton, [execute("BEGIN")])

    {baton, [ac_open]} = step(opts, baton, [%{"type" => "get_autocommit"}])
    assert ac_open["response"] == %{"type" => "get_autocommit", "is_autocommit" => false}

    {baton, _} =
      step(opts, baton, [
        execute("INSERT INTO kv VALUES (?, ?)", [int(3), text("carol")]),
        execute("COMMIT")
      ])

    {_baton, [ac_closed]} = step(opts, baton, [%{"type" => "get_autocommit"}])
    assert ac_closed["response"] == %{"type" => "get_autocommit", "is_autocommit" => true}
  end

  test "closing the stream ends the session", %{opts: opts} do
    {baton, _} = step(opts, nil, [execute("CREATE TABLE kv (key, value)")])

    {closed_baton, [close_result]} = step(opts, baton, [%{"type" => "close"}])
    assert closed_baton == nil
    assert close_result == %{"type" => "ok", "response" => %{"type" => "close"}}
  end
end
