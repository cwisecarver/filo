defmodule Filo.IntegrationDjangoTest do
  @moduledoc """
  End-to-end test of the Django ORM path: the real `django-libsql` backend
  (`libsql.db.backends.sqlite3`, built on `libsql-client`) talking to Filo over a
  WebSocket — schema-editor DDL plus ORM create/get — backed by a real SQLite
  engine. This drives the Django backend itself, above the raw driver covered by
  `Filo.IntegrationWsTest`. Excluded by default; run with
  `mix test --include integration` and `FILO_INTEGRATION_PYTHON` pointing at a
  Python that has `django-libsql`.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Filo.Test.SqliteExecutor

  @script Path.join([__DIR__, "..", "integration", "django_smoke.py"])

  test "real django-libsql backend over ws round-trips schema-editor DDL + ORM CRUD" do
    case System.get_env("FILO_INTEGRATION_PYTHON") do
      nil ->
        IO.puts(
          "\n[integration] skipped — set FILO_INTEGRATION_PYTHON to a python with django-libsql"
        )

      python ->
        run(python)
    end
  end

  defp run(python) do
    db = Path.join(System.tmp_dir!(), "filo_django_#{System.unique_integer([:positive])}.db")
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
      System.cmd(python, [@script, "ws://127.0.0.1:#{port}"], stderr_to_stdout: true)

    assert status == 0, output
    assert output =~ "DJANGO_OK"
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end
end
