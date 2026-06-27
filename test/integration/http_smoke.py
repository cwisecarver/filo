"""End-to-end smoke test: the real libsql-experimental driver (what
sqlalchemy-libsql uses) talking to Filo over HTTP — exercising the Hrana 3
pipeline, describe, and the streaming cursor. Invoked by Filo.IntegrationHttpTest
with the server's http:// URL as argv[1]. Prints HTTP_OK on success.

Requires: libsql-experimental (e.g. `pip install libsql-experimental`).
"""
import sys

import libsql_experimental as libsql

url = sys.argv[1]  # http://127.0.0.1:PORT

# isolation_level=None -> autocommit: each statement commits immediately, so a
# later read on a fresh stream sees it.
conn = libsql.connect(url, isolation_level=None)
cur = conn.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS kv (k INTEGER PRIMARY KEY, v TEXT)")
cur.execute("INSERT INTO kv VALUES (?, ?)", (1, "alice"))

# A row-returning query goes through the cursor endpoint (describe reports
# columns, so the driver streams via POST /v3/cursor).
cur.execute("SELECT v FROM kv WHERE k = ?", (1,))
row = cur.fetchone()
assert row is not None and row[0] == "alice", row

print("HTTP_OK")
