"""End-to-end smoke test: the real sqlalchemy-libsql dialect — SQLAlchemy's
`sqlite+libsql://` engine, built on libsql-experimental — talking to Filo over
HTTP. Exercises the dialect's DDL compilation, parameter binding, and result
handling, not just the raw driver. Invoked by Filo.IntegrationSqlalchemyTest
with the server's host:port as argv[1]. Prints SQLALCHEMY_OK on success.

Requires: sqlalchemy>=2 and sqlalchemy-libsql (e.g. `pip install sqlalchemy-libsql`).
"""
import sys

from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    insert,
    select,
)

hostport = sys.argv[1]  # 127.0.0.1:PORT

# secure=false -> plain http:// against our local test server (no TLS).
# Over Hrana-HTTP each libsql-experimental request is its own stream, so writes
# must autocommit to be visible to a later read (SQLAlchemy transactions would
# straddle streams and never persist). isolation_level=None on the underlying
# connect() is the driver's autocommit switch — the same one the raw
# libsql-experimental smoke uses.
engine = create_engine(
    f"sqlite+libsql://{hostport}?secure=false",
    connect_args={"isolation_level": None},
)

metadata = MetaData()
kv = Table(
    "kv",
    metadata,
    Column("k", Integer, primary_key=True),
    Column("v", String),
)

with engine.connect() as conn:
    # DDL + a write, both compiled and bound by the dialect.
    metadata.create_all(conn)
    conn.execute(insert(kv).values(k=1, v="alice"))

    # A row-returning query compiled and bound by the dialect.
    row = conn.execute(select(kv.c.v).where(kv.c.k == 1)).fetchone()
    assert row is not None and row[0] == "alice", row

print("SQLALCHEMY_OK")
