defmodule Filo.ClientTest.FlakyTransport do
  @moduledoc false
  # A stub `Filo.Client.Transport` that reports `:closed` for its first `fail_first` requests
  # (tracked in an Agent passed via transport_opts), then serves a canned 200. Used to prove the
  # client transparently reopens + resumes on a dropped connection.
  @behaviour Filo.Client.Transport

  @impl true
  def connect(_uri, opts) do
    agent = Keyword.fetch!(opts, :agent)
    Agent.update(agent, &%{&1 | connects: &1.connects + 1})
    {:ok, %{agent: agent}}
  end

  @impl true
  def request(%{agent: agent} = state, _method, _path, _headers, _body) do
    {n, fail_first} =
      Agent.get_and_update(agent, fn s ->
        {{s.requests, s.fail_first}, %{s | requests: s.requests + 1}}
      end)

    if n < fail_first do
      {:error, :closed, state}
    else
      ok = %{
        "type" => "ok",
        "response" => %{
          "type" => "execute",
          "result" => %{"cols" => [], "rows" => [[%{"type" => "integer", "value" => "1"}]]}
        }
      }

      body = Jason.encode!(%{"baton" => "resumed", "results" => [ok]})
      {:ok, %{status: 200, headers: [], body: body}, state}
    end
  end

  @impl true
  def close(_state), do: :ok
end

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

  test "transparently reopens + resumes the stream after a dropped connection" do
    {:ok, agent} = start_supervised({Agent, fn -> %{connects: 0, requests: 0, fail_first: 1} end})

    {:ok, c} =
      Client.connect("http://ignored",
        transport: Filo.ClientTest.FlakyTransport,
        transport_opts: [agent: agent]
      )

    # The transport drops the first request; the client should reopen and resume (baton preserved),
    # so the execute still succeeds without the caller ever seeing an error.
    {:ok, res, _c} = Client.execute(c, "SELECT 1")
    assert res.rows == [[1]]

    # One initial connect + one reopen after the drop; two request attempts (drop, then resume).
    assert Agent.get(agent, & &1.connects) == 2
    assert Agent.get(agent, & &1.requests) == 2
  end

  test "surfaces {:transport, :closed} when the connection keeps dropping" do
    {:ok, agent} =
      start_supervised({Agent, fn -> %{connects: 0, requests: 0, fail_first: 99} end})

    {:ok, c} =
      Client.connect("http://ignored",
        transport: Filo.ClientTest.FlakyTransport,
        transport_opts: [agent: agent]
      )

    # Retries once (2 attempts total); both drop, so the error is surfaced rather than looping.
    assert {:error, {:transport, :closed}, _c} = Client.execute(c, "SELECT 1")
    assert Agent.get(agent, & &1.requests) == 2
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end
end
