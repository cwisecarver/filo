defmodule Filo.IntegrationHttpTest do
  @moduledoc """
  End-to-end test of the SQLAlchemy path: the real `libsql-experimental` driver
  talking to Filo over HTTP — the Hrana 3 pipeline, `describe`, and the streaming
  `POST /v3/cursor` endpoint — backed by real SQLite. Excluded by default; run
  with `mix test --include integration` and `FILO_INTEGRATION_PYTHON` pointing at
  a Python that has `libsql-experimental`.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Filo.Test.SqliteExecutor

  @script Path.join([__DIR__, "..", "integration", "http_smoke.py"])

  test "real libsql-experimental over http round-trips CRUD via pipeline + cursor" do
    case System.get_env("FILO_INTEGRATION_PYTHON") do
      nil ->
        IO.puts(
          "\n[integration] skipped — set FILO_INTEGRATION_PYTHON to a python with libsql-experimental"
        )

      python ->
        run(python)
    end
  end

  defp run(python) do
    db = Path.join(System.tmp_dir!(), "filo_http_#{System.unique_integer([:positive])}.db")
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
      System.cmd(python, [@script, "http://127.0.0.1:#{port}"], stderr_to_stdout: true)

    assert status == 0, output
    assert output =~ "HTTP_OK"
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end
end
