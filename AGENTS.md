# AGENTS.md — Filo

## Project

Filo is a **Hrana (libSQL) protocol server library** for Elixir. It implements the
server side of Hrana-over-HTTP so any SQLite-backed Elixir app can accept libSQL
clients — the libSQL SDKs, `django-libsql`, `sqlalchemy-libsql`, and friends —
over the network with no changes on the client.

Filo is **engine-agnostic**: it owns the *protocol* — the value codec, statement
and batch decoding, and baton-pinned streams — and delegates SQL execution to a
host-provided executor. It must never depend on a specific SQLite engine or app.

Reference: the Hrana 3 spec and the libsql server source in `tursodatabase/libsql`
(`libsql-hrana/src/proto.rs`, `libsql-server/src/hrana/…`, and the
`tests/hrana/snapshots/*.snap` request/response vectors). Open source, MIT.

## Execution style

- **Sequenced directives** ("do X then Y") → execute directly; don't re-confirm.
  If genuinely ambiguous, name your default and proceed.
- **"Go ahead" / "continue"** = continue the *most recently scoped* task.
- **Locating things:** one targeted Read/Grep/Glob, not speculative `find`/`ls`.
- **Don't narrate intentions** ("let me check…"). State results.

## Build

```bash
mix deps.get
mix compile        # precommit uses --warnings-as-errors
mix test           # the suite
mix test test/filo/value_test.exs:12   # a single test
mix test --failed  # rerun last failures
mix format
mix docs           # ex_doc (dev)
mix precommit      # the gate: compile --warnings-as-errors, deps.unlock --unused, format check, test
```

No database, no assets — it's a pure library. **Shell is zsh**: backticks and
`$(...)` run even inside double quotes, so never put them in a `git commit -m`
body — use `git commit -F <file>`.

## Workflow — TDD

Filo is built **test-first**. Every change is red → green → refactor:

1. Write a failing test that pins the protocol behaviour (a vector from the spec,
   a `.snap`, or the reference `proto.rs`).
2. Run it; see it fail for the right reason.
3. Implement the minimum to pass.
4. Refactor; keep it green.
5. `mix precommit`, then commit, then **push**.

- **Always `git push` immediately after every commit.** An unpushed commit is
  unbacked-up work; never batch local commits.
- **Never commit** with compiler warnings, build errors, or failing tests.
- **Never use `sed`/`awk`/`head`/`tail`/`echo` to read or edit files** — use Read
  (offset/limit), Grep, Edit/Write. Piping command output is fine.
- **Stop-after-2-failures:** if a command fails twice with a similar error, stop,
  print the exact command + error + a one-paragraph root-cause hypothesis, and
  wait. Don't loop on infra failures.

## Testing

- **TDD by default** (above). Cover the happy path, error cases, and edge cases.
- **Conformance is the point.** Translate the libsql `.snap` snapshots into Elixir
  tests; the real `libsql-client` is the ultimate conformance check. When in doubt,
  match the reference exactly (integer-as-string, blob base64 *no-pad*, …) rather
  than what seems convenient.
- **Processes:** start with `start_supervised!/1`. **Avoid** `Process.sleep/1` and
  `Process.alive?/1` — use `Process.monitor/1` + `assert_receive {:DOWN, …}`, and
  `_ = :sys.get_state(pid)` to synchronize.
- **Every bug fix ships with a regression test in the same commit** — it must fail
  pre-fix, pass post-fix, and pin the violated invariant.

## Gates

- **`mix precommit` is the commit gate** (compile `--warnings-as-errors`,
  `deps.unlock --unused`, format check, test). Never commit if it fails.

## Principles

- **Simplicity first.** Minimal code; root causes, not workarounds.
- **Stay engine-agnostic.** The protocol layer is pure; SQL execution is delegated
  through the executor boundary. Don't reach into a concrete engine from the
  protocol code.
- **Protocol fidelity over convenience.** The wire format is defined by the spec
  and the reference; conform to it.

## Elixir guidelines

- Lists have **no index access** (`list[i]`); use `Enum.at/2`, pattern matching,
  or `List`.
- Variables are immutable but rebindable; bind the result of `if`/`case`/`cond` to
  a variable — don't rebind inside the block.
- **Never** nest multiple modules in one file.
- **Never** use map-access syntax (`struct[:field]`) on structs — access fields
  directly.
- Don't `String.to_atom/1` on input from the wire (atom-table exhaustion).
- Predicate names end in `?` and don't start with `is_` (reserve `is_` for guards).
- `DynamicSupervisor`/`Registry` need a `:name` in the child spec.
- Use `Task.async_stream/3` (usually `timeout: :infinity`) for concurrent
  enumeration with back-pressure.

## Plug

Filo ships a `Plug` (no Phoenix dependency). Standard Plug conventions; the host
app mounts it (e.g. a Phoenix router `forward`, or a plain Plug pipeline).
