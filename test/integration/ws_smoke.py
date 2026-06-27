"""End-to-end smoke test: the real libsql-client (the driver django-libsql uses)
talking to Filo over a WebSocket. Invoked by Filo.IntegrationWsTest with the
server's ws:// URL as argv[1]. Prints WS_OK on success; any failure raises and
exits non-zero.

Requires: libsql-client (e.g. `pip install libsql-client==0.3.1`).
"""
import sys

import libsql_client

url = sys.argv[1]  # ws://127.0.0.1:PORT

with libsql_client.create_client_sync(url) as client:
    client.execute("CREATE TABLE IF NOT EXISTS kv (k INTEGER PRIMARY KEY, v TEXT)")

    # autocommit write, then a separate read sees it (each is its own stream)
    client.execute("INSERT INTO kv VALUES (?, ?)", [1, "alice"])
    rs = client.execute("SELECT v FROM kv WHERE k = ?", [1])
    assert rs.rows[0][0] == "alice", rs.rows

    # a batch (non-interactive transaction) over a single stream
    rs2 = client.batch(
        [
            ("INSERT INTO kv VALUES (?, ?)", [2, "bob"]),
            ("SELECT v FROM kv WHERE k = ?", [2]),
        ]
    )
    assert rs2[1].rows[0][0] == "bob", rs2[1].rows

    print("WS_OK")
