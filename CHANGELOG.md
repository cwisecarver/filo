# Changelog

All notable changes to Filo are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
Filo uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html). While the
version is below `1.0.0`, the **minor** number carries breaking changes as well as
features; patch releases stay backward compatible.

## [0.2.0] — 2026-07-28

Filo stops being server-only: it now ships a Hrana **client** built on the same
codec, plus an authorization seam that works on both transports. Everything here is
additive — a host on 0.1.0 upgrades without code changes.

### Added

- **`Filo.Client`** — a Hrana client mirroring the server's codec, so there is no
  second implementation of the wire format to drift from. One client is one Hrana
  stream over one owned connection, with baton threading, so a transaction is a
  burst of `execute/3` on a held connection. Scope is the Hrana 2/3 HTTP JSON
  pipeline. The HTTP round-trip is a `Filo.Client.Transport` behaviour; the default
  `Filo.Client.Transport.Mint` uses the **optional** `:mint` dependency, so
  server-only hosts never pull it in.
- **Transparent stream resume.** A dropped connection is retried with the *same*
  baton, resuming the Hrana stream instead of abandoning it. This matters behind a
  load balancer that recycles an idle client keep-alive after N requests — that used
  to kill a held transaction mid-flight.
- **An optional `:authorize` seam** gating every stream open, on both transports.
  It is a Filo callback rather than a plug because WebSocket clients send no upgrade
  header: the credential arrives in the Hrana `hello` frame, which only Filo sees.
- **Authorize context threaded to `Executor.open/2`.** `:authorize` may return
  `{:ok, context}`; Filo threads that host-opaque value into `open/2` for every
  stream the connection opens — the HTTP stream that opens in its own process, the
  stateless v1 open, and every WebSocket `open_stream` after the hello. `open/2` is
  in `@optional_callbacks`, so **an executor that only implements `open/1` keeps
  working** and the context is dropped. Not a breaking change.
- **Streams monitor their connection owner** and tear down when it dies, so an
  orphaned stream cannot outlive the process that opened it.

### Changed

- **Cursor entries batch into ~32 KiB transport chunks** instead of one write per
  ndjson entry.
- **Direct-to-iodata JSON row encoding** (`Filo.Value.encode_json`, `rows: :json`)
  and pre-encoded row fragments on the JSON paths, with iodata carried through to
  the send sites — fewer intermediate binaries per result.
- **Idle-timer cancellation is async**, and the host can set a process policy for
  stream processes.

### Fixed

- A malformed WebSocket **text frame now closes with 1007** (invalid frame payload
  data) rather than a bare 1000, so a client can tell a protocol error from a normal
  close.

## [0.1.0] — 2026-07-02

Initial release: the Hrana protocol server.

### Added

- **Both transports libSQL clients use, from one Plug.** HTTP — Hrana 1 (stateless
  `execute`/`batch`), Hrana 2/3 pipelines, and Hrana 3 cursors, in JSON and
  Protobuf. WebSocket — Hrana 2/3 and `hrana3-protobuf`, the path `django-libsql`
  uses.
- **Stateful streams.** A stream owns one connection for its life, so transactions
  and temp tables persist across requests. HTTP streams are pinned by signed batons,
  WebSocket streams by a client-allocated id.
- **`Filo.Executor` behaviour** — `open`, `execute`, `autocommit?`, `close`, plus
  optional `describe` and `sequence`. Bring `exqlite`, a libSQL/Turso connection, or
  anything that speaks SQLite.
- **Ships a `Plug` and a `WebSock` handler and nothing else** — the host brings the
  HTTP server. Runtime deps are only `plug`, `jason`, and the `websock` behaviour.
- MIT license.

[0.2.0]: https://github.com/cwisecarver/filo/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/cwisecarver/filo/releases/tag/v0.1.0
