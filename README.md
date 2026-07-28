# Filo

A [Hrana](https://github.com/tursodatabase/libsql/blob/main/docs/HRANA_3_SPEC.md)
(libSQL) protocol **server and client** for Elixir — speak libSQL's wire protocol
from any Plug app, backed by the SQLite engine of your choice, and drive other
Hrana servers with a client built on the same codec.

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
- **An `:authorize` seam** gating every stream open, for both transports. It sees
  the WebSocket `hello` JWT and the HTTP `Authorization` header, so per-tenant auth
  works on the path where clients send no upgrade header. May return a host-opaque
  context that Filo threads into `Executor.open/2`.
- **A matching client** (`Filo.Client`) that reuses the server's own codec, so
  there is no second implementation of the wire format to drift. It transparently
  **resumes a dropped stream** with the same baton, so an idle connection recycled
  by a load balancer does not abandon a held transaction.
- **Minimal footprint.** Only `plug`, `jason`, and the `websock` behaviour are
  runtime deps; the server and SQLite engine are the host's choice.

## How it works

```
libSQL client ──Hrana──▶ Filo.Plug / Filo.Socket ──▶ Filo.Executor ──▶ your SQLite/libSQL
(SDK, django-libsql, …)   protocol: streams, batons,      your SQL
                          cursors, value codec
                                   ▲
                          same codec, other direction
                                   │
                            Filo.Client ──Hrana──▶ any Hrana server
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

### Gating stream opens (`:authorize`)

Pass an `:authorize` callback to gate every stream open, on both transports. This is
a Filo seam rather than a plug because WebSocket clients send **no upgrade header** —
the credential arrives in the Hrana `hello` frame, which only Filo sees:

```elixir
{Filo.Plug,
 [
   executor: MyApp.SqliteExecutor,
   streams: MyApp.Streams,
   key: Filo.Baton.new_key(),
   open_arg: fn conn -> conn.host end,
   authorize: fn token, open_arg ->
     case MyApp.Auth.verify(token, open_arg) do
       # a bare :ok admits the stream; {:ok, context} also threads a
       # host-opaque value into Executor.open/2 for this connection
       {:ok, claims} -> {:ok, claims}
       :error -> {:error, %Filo.Error{message: "unauthorized", code: "UNAUTHORIZED"}}
     end
   end
 ]}
```

`open/2` is optional — an executor that only implements `open/1` keeps working and
the context is dropped.

### Driving a Hrana server (`Filo.Client`)

One client is one Hrana **stream** over one owned connection: the first request opens
it, and each response carries a baton threaded into the next, so a transaction is a
burst of `execute/3` on a held connection.

```elixir
{:ok, c} = Filo.Client.connect("http://localhost:8080", authority: "acme.example")
{:ok, _res, c} = Filo.Client.execute(c, "INSERT INTO t VALUES (?, ?)", [1, "x"])
{:ok, res, c}  = Filo.Client.execute(c, "SELECT a, b FROM t WHERE a = ?", [1])
res.rows       #=> [[1, "x"]]
:ok = Filo.Client.close(c)
```

The struct is immutable — thread it through your own process and hold one per
concurrent stream (on the BEAM that is one cheap process each). If the connection
drops, the client reconnects and **resumes the same stream with its baton** rather
than abandoning a held transaction. HTTP transport is a `Filo.Client.Transport`
behaviour: the default `Filo.Client.Transport.Mint` needs the optional `:mint` dep,
and a host that prefers Finch/Req/`:gun` passes its own `:transport`.

Scope today is the Hrana 2/3 **HTTP JSON pipeline** — enough to drive a database end
to end. See the `Filo.Client` module docs for the rest.

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
