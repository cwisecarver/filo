# Integration tests drive real libsql clients against Filo over a real server and
# need external setup (a Python with libsql-client, network-installed deps), so
# they are excluded by default. Run them with `mix test --include integration`.
ExUnit.start(exclude: [:integration])
