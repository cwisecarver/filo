defmodule Filo.IntegrationSqlalchemyTest do
  @moduledoc """
  End-to-end test of the SQLAlchemy ORM path: the real `sqlalchemy-libsql`
  dialect (`sqlite+libsql://…`, built on `libsql-experimental`) talking to Filo
  over HTTP — DDL compilation, parameter binding, and a row-returning query —
  backed by a real SQLite engine. This drives the dialect itself, above the raw
  driver covered by `Filo.IntegrationHttpTest`. Excluded by default; run with
  `mix test --include integration` and `FILO_INTEGRATION_PYTHON` pointing at a
  Python that has `sqlalchemy-libsql`.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Filo.Test.SqliteExecutor

  @script Path.join([__DIR__, "..", "integration", "sqlalchemy_smoke.py"])

  test "real sqlalchemy-libsql dialect over http round-trips DDL + CRUD" do
    case System.get_env("FILO_INTEGRATION_PYTHON") do
      nil ->
        IO.puts(
          "\n[integration] skipped — set FILO_INTEGRATION_PYTHON to a python with sqlalchemy-libsql"
        )

      python ->
        run(python)
    end
  end

  defp run(python) do
    db = Path.join(System.tmp_dir!(), "filo_sqlalchemy_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> File.rm(db) end)

    start_supervised!({Filo.Streams, name: __MODULE__.Streams})

    port = free_port()

    start_supervised!(
      {Bandit,
       scheme: :http,
       port: port,
       plug:
         {Filo.Plug,
          executor: SqliteExecutor,
          open_arg: db,
          streams: __MODULE__.Streams,
          key: Filo.Baton.new_key()}}
    )

    {output, status} =
      System.cmd(python, [@script, "127.0.0.1:#{port}"], stderr_to_stdout: true)

    assert status == 0, output
    assert output =~ "SQLALCHEMY_OK"
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end
end
