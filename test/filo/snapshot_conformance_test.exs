defmodule Filo.SnapshotConformanceTest.Stats do
  @moduledoc """
  An executor that returns the exact statement statistics libsql's `stats` test
  recorded, so we can assert Filo's wire output against upstream's own snapshot.
  """
  @behaviour Filo.Executor

  alias Filo.{Stmt, StmtResult}

  @impl true
  def open(_arg), do: {:ok, :conn}

  @impl true
  def execute(:conn, %Stmt{sql: "CREATE TABLE foo (x INT)"}),
    do: {:ok, %StmtResult{rows_read: 1, rows_written: 2}}

  def execute(:conn, %Stmt{sql: "INSERT INTO foo VALUES (42)"}),
    do: {:ok, %StmtResult{affected_row_count: 1, last_insert_rowid: 1, rows_written: 1}}

  def execute(:conn, %Stmt{sql: "SELECT * FROM foo"}),
    do: {:ok, %StmtResult{cols: [%{name: "x", decltype: "INT"}], rows: [[42]], rows_read: 1}}

  @impl true
  def autocommit?(:conn), do: true

  @impl true
  def close(:conn), do: :ok
end

defmodule Filo.SnapshotConformanceTest do
  # Asserts Filo's /v3/pipeline output against a request/response vector recorded
  # by libsql's own integration suite (libsql-server/tests/hrana/batch.rs::stats).
  use ExUnit.Case, async: false

  import Plug.Test
  import Plug.Conn

  alias Filo.SnapshotConformanceTest.Stats

  @streams __MODULE__.Streams
  @snapshot Path.join([__DIR__, "..", "fixtures", "hrana", "stats.snap"])
  @external_resource @snapshot

  setup do
    start_supervised!({Filo.Streams, name: @streams})
    opts = Filo.Plug.init(executor: Stats, streams: @streams, key: Filo.Baton.new_key())
    %{opts: opts}
  end

  test "the /v3/pipeline response matches libsql's recorded `stats` snapshot", %{opts: opts} do
    request = %{
      "requests" => [
        %{"type" => "execute", "stmt" => %{"sql" => "CREATE TABLE foo (x INT)"}},
        %{"type" => "execute", "stmt" => %{"sql" => "INSERT INTO foo VALUES (42)"}},
        %{"type" => "execute", "stmt" => %{"sql" => "SELECT * FROM foo"}}
      ]
    }

    conn =
      conn(:post, "/v3/pipeline", Jason.encode!(request))
      |> put_req_header("content-type", "application/json")
      |> Filo.Plug.call(opts)

    assert conn.status == 200

    actual = conn.resp_body |> Jason.decode!() |> normalize_actual()
    expected = @snapshot |> File.read!() |> parse_snapshot() |> normalize_expected()

    assert actual == expected
  end

  # Filo's bytes diverge from libsql's recorded bytes only on fields that are
  # legitimately implementation-specific; normalize those out on both sides.
  #
  #   - baton           — server-signed, non-deterministic. The upstream test
  #                       deletes it before snapshotting; we drop it here too.
  #   - query_duration_ms — Filo always emits 0.0 (no timing yet); the upstream
  #                       test strips it before snapshotting; we drop it too.
  #   - replication_index — a replicating-server frame number ("1"/"2") upstream;
  #                       Filo has no replication and emits null, so we null it
  #                       on the expected side.
  #
  # Everything else — the envelope, value tagging, integer-as-string, rowid-as-
  # string, the cols decltype map — must match byte for byte.

  defp normalize_actual(%{"results" => results} = body) do
    body
    |> Map.delete("baton")
    |> Map.put("results", Enum.map(results, &delete_result_field(&1, "query_duration_ms")))
  end

  defp normalize_expected(%{"results" => results} = body) do
    Map.put(
      body,
      "results",
      Enum.map(results, &put_in(&1, ["response", "result", "replication_index"], nil))
    )
  end

  defp delete_result_field(entry, field) do
    update_in(entry, ["response", "result"], &Map.delete(&1, field))
  end

  # An insta `.snap` is `---\n<yaml>\n---\n<body>`; return the decoded body.
  defp parse_snapshot(raw) do
    [_before, _frontmatter, body] = String.split(raw, "---\n", parts: 3)
    Jason.decode!(body)
  end
end
