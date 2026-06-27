# Filo

A [Hrana](https://github.com/tursodatabase/libsql/blob/main/docs/HRANA_3_SPEC.md)
(libSQL) protocol **server** for Elixir.

Hrana is the wire protocol spoken by libSQL clients — the official libSQL SDKs,
`django-libsql`, `sqlalchemy-libsql`, and friends. Filo implements the **server**
side of Hrana-over-HTTP, so any SQLite-backed Elixir app can accept those clients
over the network with no changes on the client.

Filo is engine-agnostic: it owns the *protocol* (the value codec, statement and
batch decoding, and baton-pinned streams) and calls back into an executor you
provide to actually run SQL against your database.

> Status: early, built test-first.

## License

MIT
