# Contributing to Filo

Thanks for helping build Filo. This is the practical "get set up and land a change"
guide. Filo is a Hrana (libSQL) protocol server **and** client for Elixir; the
architecture notes live in [`AGENTS.md`](https://github.com/cwisecarver/filo/blob/main/AGENTS.md).

## Prerequisites

- **Elixir ~> 1.20** on OTP 27, 28, or 29 (what CI runs).
- **A C toolchain** — `exqlite` is a test-only dependency and compiles its bundled
  SQLite from source (`build-essential` on Linux, Xcode Command Line Tools on macOS).
- Nothing else. Filo has no database, no service dependencies, and no config to set
  up: it ships a `Plug` and a `WebSock` handler, and the host brings the server.

```bash
git clone https://github.com/cwisecarver/filo && cd filo
mix deps.get
mix test
```

## The gate

`mix precommit` is what has to pass before a commit lands. It runs
`compile --warnings-as-errors`, `deps.unlock --unused`, `format --check-formatted`,
and the test suite:

```bash
mix precommit
```

CI runs exactly this across OTP 27/28/29. If `precommit` is green locally it should
be green on CI; if it isn't, that difference is itself worth reporting.

### Integration tests

The default suite is self-contained. Integration tests drive **real libSQL clients**
against a live Filo server and need external setup (a Python with `libsql-client`),
so they are excluded by default:

```bash
mix test --include integration
```

Run these when you touch the wire format, the transports, or stream lifecycle —
they are what catch a change that is valid Elixir but invalid Hrana.

## What a good change looks like

- **Test-first.** Filo was built that way and the protocol rewards it: the wire
  format has exact expectations, and a test is usually shorter than the prose
  describing the case.
- **Protocol changes ship with a test at the wire level**, not just at the function
  boundary. A codec that round-trips in Elixir but emits the wrong JSON shape is
  still broken.
- **Keep the dependency footprint small.** Runtime deps are `plug`, `jason`, and the
  `websock` behaviour — plus `mint`, which is **optional** and only for the client's
  default transport. A server-only host should never pull in a transport. If a change
  needs a new runtime dep, that is a design discussion first.
- **The executor stays engine-agnostic.** Filo owns the protocol; the host owns SQL
  execution. New `Filo.Executor` callbacks should be in `@optional_callbacks` unless
  there is a strong reason, so existing implementations keep working.
- **Update [`CHANGELOG.md`](CHANGELOG.md)** under an `## [Unreleased]` heading for
  anything user-visible.

## Versioning

Filo follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html). While the
version is below `1.0.0`, the **minor** number carries breaking changes as well as
features; patch releases stay backward compatible.

Releases are git tags of the form `vX.Y.Z` matching `@version` in `mix.exs`. That
match matters beyond tidiness: `ex_doc` builds source links from
`source_ref: "v#{@version}"`, so a tag that doesn't exist at the documented code
produces documentation whose "source" links point at a tree without it.

## Reporting a protocol bug

Filo implements someone else's wire protocol, so the most useful bug report includes
**what the client sent and what it expected back**. A failing request/response pair —
or the client library and version plus the operation that failed — is worth more than
a stack trace, because the question is almost always "what does Hrana actually
require here."
