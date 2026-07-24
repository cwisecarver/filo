defmodule Filo.ClientTest do
  @moduledoc """
  End-to-end test of `Filo.Client` against a real `Filo.Plug` server (Bandit +
  `Filo.Test.SqliteExecutor`), all in-process — no external SDK needed. Proves the
  client and server share one wire codec: what the server encodes, the client
  decodes back to the same native terms.
  """
  # Shares a globally-named Filo.Streams supervisor, so not concurrent.
  use ExUnit.Case, async: false

  alias Filo.Client

  @streams __MODULE__.Streams

  setup do
    db = Path.join(System.tmp_dir!(), "filo_client_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> File.rm(db) end)

    start_supervised!({Filo.Streams, name: @streams})
    port = free_port()

    start_supervised!(
      {Bandit,
       scheme: :http,
       port: port,
       plug:
         {Filo.Plug,
          executor: Filo.Test.SqliteExecutor,
          open_arg: db,
          streams: @streams,
          key: Filo.Baton.new_key()}}
    )

    {:ok, url: "http://127.0.0.1:#{port}"}
  end

  test "round-trips CRUD across a baton-threaded stream", %{url: url} do
    {:ok, c} = Client.connect(url)

    {:ok, _r, c} = Client.execute(c, "CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
    {:ok, ins, c} = Client.execute(c, "INSERT INTO t (a, b) VALUES (?, ?)", [1, "hello"])
    assert ins.affected_row_count == 1

    {:ok, sel, c} = Client.execute(c, "SELECT a, b FROM t WHERE a = ?", [1])
    assert sel.rows == [[1, "hello"]]
    assert Enum.map(sel.cols, & &1.name) == ["a", "b"]

    assert :ok = Client.close(c)
  end

  test "a multi-statement transaction commits on the held stream", %{url: url} do
    {:ok, c} = Client.connect(url)
    {:ok, _r, c} = Client.execute(c, "CREATE TABLE acct (id INTEGER PRIMARY KEY, bal INTEGER)")
    {:ok, _r, c} = Client.execute(c, "INSERT INTO acct VALUES (1, 100)")

    {:ok, _r, c} = Client.execute(c, "BEGIN IMMEDIATE")
    {:ok, _r, c} = Client.execute(c, "UPDATE acct SET bal = bal + ? WHERE id = ?", [50, 1])
    {:ok, _r, c} = Client.execute(c, "COMMIT")

    {:ok, sel, c} = Client.execute(c, "SELECT bal FROM acct WHERE id = 1")
    assert sel.rows == [[150]]
    Client.close(c)
  end

  test "encodes and decodes every value type through Filo.Value", %{url: url} do
    {:ok, c} = Client.connect(url)

    {:ok, r, c} = Client.execute(c, "SELECT ?, ?, ?, ?", [1, 3.5, "text", nil])
    assert r.rows == [[1, 3.5, "text", nil]]

    {:ok, b, c} = Client.execute(c, "SELECT ?", [{:blob, <<0xFF, 0xFE, 0xFD>>}])
    assert b.rows == [[{:blob, <<0xFF, 0xFE, 0xFD>>}]]

    Client.close(c)
  end

  test "a SQL error is returned but leaves the stream open", %{url: url} do
    {:ok, c} = Client.connect(url)

    {:error, %Filo.Error{} = err, c} = Client.execute(c, "SELECT * FROM does_not_exist")
    assert err.message =~ "does_not_exist"

    # Same client, same stream: the baton advanced, so the next statement still runs.
    {:ok, ok, c} = Client.execute(c, "SELECT 1")
    assert ok.rows == [[1]]
    Client.close(c)
  end

  test "reconnect resets the baton and keeps working", %{url: url} do
    {:ok, c} = Client.connect(url)
    {:ok, _r, c} = Client.execute(c, "CREATE TABLE k (v INTEGER)")

    {:ok, c} = Client.reconnect(c)
    assert c.baton == nil

    {:ok, _r, c} = Client.execute(c, "INSERT INTO k VALUES (7)")
    {:ok, sel, c} = Client.execute(c, "SELECT v FROM k")
    assert sel.rows == [[7]]
    Client.close(c)
  end

  test "sends a custom :authority (Host) without breaking the request", %{url: url} do
    {:ok, c} = Client.connect(url, authority: "acme.fathom.test")
    {:ok, r, c} = Client.execute(c, "SELECT 42")
    assert r.rows == [[42]]
    Client.close(c)
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end
end
