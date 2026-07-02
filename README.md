# Filo

A [Hrana](https://github.com/tursodatabase/libsql/blob/main/docs/HRANA_3_SPEC.md)
(libSQL) protocol **server** for Elixir — speak libSQL's wire protocol from any
Plug app, backed by the SQLite engine of your choice.

Hrana is the wire protocol spoken by libSQL clients: the official libSQL SDKs,
[`django-libsql`](https://github.com/aaronkazah/django-libsql),
[`sqlalchemy-libsql`](https://github.com/tursodatabase/sqlalchemy-libsql), and
friends. Filo implements the **server** side, so any SQLite-backed Elixir app can
accept those clients over the network — with no changes on the client.

Filo is **engine-agnostic**: it owns the *protocol* (the value codec, statement and
batch decoding, cursors, and baton-pinned streams) and calls back into a
`Filo.Executor` you provide to actually run SQL. Bring `exqlite`, a libSQL/Turso
connection, or anything that speaks SQLite.

> **Status:** early, built test-first.

## Features

- **Both transports** libSQL clients use, from one Plug:
  - **HTTP** — Hrana 1 (stateless `execute`/`batch`), Hrana 2/3 pipelines, and
    Hrana 3 cursors, in JSON **and** Protobuf.
  - **WebSocket** — Hrana 2/3 (and `hrana3-protobuf`) — the path `django-libsql`
    uses.
- **Ships a `Plug` and a `WebSock` handler, nothing else.** Mount them in any Plug
  or Phoenix app; you bring the HTTP server (Bandit/Cowboy).
- **Stateful streams.** A stream owns one connection for its life, so transactions
  and temp tables persist across requests. HTTP streams are pinned by signed
  **batons**; WebSocket streams by a client-allocated id.
- **Engine-agnostic** via a small `Filo.Executor` behaviour — `open`, `execute`,
  `autocommit?`, `close` (plus optional `describe` and `sequence`).
- **Minimal footprint.** Only `plug`, `jason`, and the `websock` behaviour are
  runtime deps; the server and SQLite engine are the host's choice.

## How it works

```
libSQL client ──Hrana──▶ Filo.Plug / Filo.Socket ──▶ Filo.Executor ──▶ your SQLite/libSQL
(SDK, django-libsql, …)   protocol: streams, batons,      your SQL
                          cursors, value codec
```

Filo decodes the wire, manages stream lifecycles and batons, and hands each
statement to your executor. Your executor only ever sees a `Filo.Stmt` going in and
returns a `Filo.StmtResult` or a `Filo.Error`.

## Usage

Keep a `Filo.Streams` supervisor in your tree, implement a `Filo.Executor`, and run
`Filo.Plug` behind an HTTP server (Bandit gives you the WebSocket upgrade for free).

**1. Implement an executor** (here with `exqlite`):

```elixir
defmodule MyApp.SqliteExecutor do
  @behaviour Filo.Executor

  @impl true
  def open(db_name) do
    case Exqlite.Sqlite3.open("#{db_name}.db") do
      {:ok, conn} -> {:ok, conn}
      {:error, reason} -> {:error, %Filo.Error{message: to_string(reason), code: "FILO_OPEN"}}
    end
  end

  @impl true
  def execute(conn, %Filo.Stmt{sql: sql, args: args}) do
    # run sql/args on conn, then map rows -> %Filo.StmtResult{} (or -> %Filo.Error{})
  end

  @impl true
  def autocommit?(_conn), do: true

  @impl true
  def close(conn), do: Exqlite.Sqlite3.close(conn)
end
```

**2. Add the streams registry and the listener to your supervision tree:**

```elixir
children = [
  {Filo.Streams, name: MyApp.Streams},
  {Bandit,
   scheme: :http,
   port: 8080,
   plug:
     {Filo.Plug,
      [
        executor: MyApp.SqliteExecutor,
        streams: MyApp.Streams,
        key: Filo.Baton.new_key(),          # keep stable for the server's life
        open_arg: fn conn -> conn.host end   # e.g. pick a database from the request
      ]}}
]
```

**3. Point any libSQL client at it:**

```python
# django-libsql (over WebSocket)
DATABASES = {"default": {"ENGINE": "django_libsql.libsql", "NAME": "ws://localhost:8080"}}
```

See the `Filo.Plug` (options) and `Filo.Executor` (callbacks) module docs for the
full reference.

## Supported clients

`django-libsql` (WebSocket) and `libsql-client` / `libsql-experimental` / the
official libSQL SDKs (HTTP) all work end to end. Filo is exercised against real
clients in its integration suite.

## Development

```bash
mix deps.get
mix test        # unit + protocol tests
mix precommit   # compile --warnings-as-errors, format check, unused-deps check, test
```

Integration tests drive real libSQL clients against Filo over a live server and
need external setup (a Python with `libsql-client`), so they are excluded by
default:

```bash
mix test --include integration
```

CI runs `mix precommit` on Elixir 1.20 across OTP 27, 28, and 29.

## License

MIT
