defmodule Filo.Plug do
  @moduledoc """
  A `Plug` that speaks Hrana over HTTP (versions 1, 2, and 3) and upgrades
  WebSocket requests to `Filo.Socket` (Hrana over WebSocket). Mount it in any Plug
  or Phoenix app to accept libSQL clients (`django-libsql` over ws, `libsql-client`,
  `libsql-experimental`, the libSQL SDKs).

  ## Routes

    - `GET /v1` · `/v2` · `/v3` · `/v3-protobuf` — protocol-support checks; reply `200`.
    - `POST /v2/pipeline` · `/v3/pipeline` — run a pipeline of stream requests (JSON).
    - `POST /v3-protobuf/pipeline` — same, Protobuf-encoded (`application/x-protobuf`).
    - `POST /v3/cursor` — run a batch, streaming the result (Hrana 3, JSON).
    - `POST /v3-protobuf/cursor` — same, length-delimited Protobuf frames.
    - `POST /v1/execute` · `/v1/batch` — stateless Hrana 1 legacy API.
    - A WebSocket upgrade (any path) is handed to `Filo.Socket`.

  ## Configuration (`init/1`)

    - `:executor` (required) — the `Filo.Executor` module backing each stream.
    - `:streams` (required) — the registered name of a `Filo.Streams` supervisor
      the host keeps in its supervision tree.
    - `:key` (required) — the baton signing key (`Filo.Baton.new_key/0`). Keep it
      stable for the life of the server; rotating it invalidates live batons.
    - `:open_arg` — host context for `executor.open/1`. Either a term used as-is
      or a 1-arity function of the `Plug.Conn` (e.g. to pick a database from the
      request). Default `nil`.
    - `:authorize` — optional host auth callback, `fun(open_arg, token)` →
      `:ok | {:ok, context} | {:error, %Filo.Error{}}`. `token` is the request's
      `Authorization: Bearer` credential (HTTP), or — on the WebSocket binding —
      the `jwt` the client sent in its Hrana `hello` (libSQL's `authToken`
      travels there, not in an upgrade header), falling back to the upgrade
      request's bearer token; `nil` when the client sent neither. Checked once
      per stream open (HTTP pipeline/cursor without a baton, each stateless v1
      request, the WS hello) — a baton resumes a stream that was already
      authorized when opened. Returning `{:ok, context}` threads `context`
      (any host-opaque term — the verified scope, a tenant id) to
      `executor.open/2` for every stream this connection opens; a bare `:ok`
      threads `nil`. A refusal is surfaced with the error's `status`
      (default 401), or as a fatal `hello_error` on WebSocket. Default `nil`
      (no auth — the host trusts the network).
    - `:base_url` — value returned in each response's `base_url`. Default `nil`.
    - `:idle_timeout` — per-stream inactivity timeout in ms. Default uses
      `Filo.Stream`'s.

  ## Pipeline flow

  A request with no `baton` opens a new stream; a request with a `baton` resumes
  the stream it names. Each response carries a fresh `baton` for the next request
  (or `null` once the stream is closed). A reused, forged, or malformed baton, or
  a baton for a stream that has been closed or expired, is rejected with `400`.
  """

  @behaviour Plug

  import Plug.Conn

  alias Filo.{Baton, Batch, BatchResult, Cursor, Error, Stmt, StmtResult, Stream, Streams}
  alias Filo.Protobuf
  alias Filo.Protobuf.Wire

  @max_body 8_000_000

  @impl true
  def init(opts) do
    %{
      executor: Keyword.fetch!(opts, :executor),
      streams: Keyword.fetch!(opts, :streams),
      key: Keyword.fetch!(opts, :key),
      open_arg: Keyword.get(opts, :open_arg),
      authorize: Keyword.get(opts, :authorize),
      base_url: Keyword.get(opts, :base_url),
      idle_timeout: Keyword.get(opts, :idle_timeout),
      # Per-stream process policy, forwarded verbatim to Filo.Stream.start_link/1 (fathom expert
      # review 2026-07-24 #22). A stream is idle-dominant and long-lived, so the host may want a
      # hibernation / GC policy; filo has no opinion, it just stops dropping the options.
      stream_spawn: Keyword.take(opts, [:hibernate_after, :spawn_opt])
    }
  end

  @impl true
  def call(conn, opts) do
    if websocket_upgrade?(conn) do
      upgrade(conn, opts)
    else
      route(conn, opts)
    end
  end

  # Version-support checks. Hrana 1 (legacy /v1/execute + /v1/batch), 2, and 3 are
  # all served; v2/v3 share the pipeline shape, v3 adds the cursor endpoint.
  defp route(%Plug.Conn{method: "GET", path_info: [version]} = conn, _opts)
       when version in ~w(v1 v2 v3 v3-protobuf) do
    send_resp(conn, 200, "Filo: Hrana over HTTP (#{version})")
  end

  defp route(%Plug.Conn{method: "POST", path_info: [version, "pipeline"]} = conn, opts)
       when version in ~w(v2 v3) do
    handle_pipeline(conn, opts, :json)
  end

  defp route(%Plug.Conn{method: "POST", path_info: ["v3-protobuf", "pipeline"]} = conn, opts) do
    handle_pipeline(conn, opts, :protobuf)
  end

  # Cursor is a Hrana 3 addition: it runs a batch and streams the result.
  defp route(%Plug.Conn{method: "POST", path_info: ["v3", "cursor"]} = conn, opts) do
    handle_cursor(conn, opts, :json)
  end

  defp route(%Plug.Conn{method: "POST", path_info: ["v3-protobuf", "cursor"]} = conn, opts) do
    handle_cursor(conn, opts, :protobuf)
  end

  # Hrana 1 (legacy HTTP API): stateless execute/batch, no baton — each runs on a
  # fresh connection.
  defp route(%Plug.Conn{method: "POST", path_info: ["v1", "execute"]} = conn, opts) do
    handle_v1_execute(conn, opts)
  end

  defp route(%Plug.Conn{method: "POST", path_info: ["v1", "batch"]} = conn, opts) do
    handle_v1_batch(conn, opts)
  end

  defp route(conn, _opts), do: send_resp(conn, 404, "not found")

  # A WebSocket upgrade on any path is handed to Filo.Socket (Hrana over WS),
  # so one mount serves both the HTTP pipeline and the WebSocket binding.
  defp upgrade(conn, opts) do
    {conn, encoding} = negotiate_subprotocol(conn)

    socket_opts = [
      executor: opts.executor,
      open_arg: open_arg(opts, conn),
      encoding: encoding,
      # Auth on the WS binding happens at the Hrana hello (the token rides the hello's
      # `jwt` field, which no plug can see); hand the socket the callback plus any
      # bearer token the upgrade request did carry, as the hello-jwt fallback.
      authorize: opts.authorize,
      header_token: bearer_token(conn)
    ]

    conn
    |> Plug.Conn.upgrade_adapter(:websocket, {Filo.Socket, socket_opts, []})
  end

  # JSON variants are listed first, so a client offering both JSON and Protobuf
  # gets JSON; a Protobuf-only client (`hrana3-protobuf`) gets the binary encoding.
  @subprotocols ~w(hrana3 hrana2 hrana1 hrana3-protobuf)

  defp negotiate_subprotocol(conn) do
    offered = conn |> get_req_header("sec-websocket-protocol") |> Enum.flat_map(&tokens/1)

    case Enum.find(@subprotocols, &(&1 in offered)) do
      nil -> {conn, :json}
      chosen -> {put_resp_header(conn, "sec-websocket-protocol", chosen), encoding_for(chosen)}
    end
  end

  defp encoding_for("hrana3-protobuf"), do: :protobuf
  defp encoding_for(_), do: :json

  defp websocket_upgrade?(conn) do
    conn.method == "GET" and header_token?(conn, "upgrade", "websocket") and
      header_token?(conn, "connection", "upgrade")
  end

  defp header_token?(conn, name, token) do
    conn
    |> get_req_header(name)
    |> Enum.flat_map(&tokens/1)
    |> Enum.any?(&(String.downcase(&1) == token))
  end

  defp tokens(value), do: value |> String.split(",") |> Enum.map(&String.trim/1)

  defp handle_pipeline(conn, opts, :json) do
    case read_request(conn) do
      {:ok, body, conn} ->
        baton = Map.get(body, "baton")
        requests = Map.get(body, "requests", [])

        case dispatch(opts, conn, baton, requests, rows: :json) do
          {:ok, new_baton, results} ->
            send_json(conn, 200, %{
              "baton" => new_baton,
              "base_url" => opts.base_url,
              "results" => results
            })

          {:error, status, %Error{} = error} ->
            send_json(conn, status, Error.encode(error))
        end

      {:error, :bad_json, conn} ->
        send_json(
          conn,
          400,
          Error.encode(%Error{message: "invalid JSON body", code: "FILO_BAD_REQUEST"})
        )
    end
  end

  defp handle_pipeline(conn, opts, :protobuf) do
    case read_all(conn, "") do
      {:ok, body, conn} ->
        decoded = Protobuf.Http.decode_pipeline_req(body)

        case dispatch(opts, conn, Map.get(decoded, "baton"), Map.get(decoded, "requests", []),
               rows: :maps
             ) do
          {:ok, new_baton, results} ->
            send_protobuf(
              conn,
              200,
              Protobuf.Http.encode_pipeline_resp(%{
                "baton" => new_baton,
                "base_url" => opts.base_url,
                "results" => results
              })
            )

          {:error, status, %Error{} = error} ->
            send_protobuf(conn, status, Protobuf.encode_error(Error.encode(error)))
        end

      {:error, conn} ->
        send_protobuf(conn, 400, bad_request_pb())
    end
  end

  # No baton: authorize, open a fresh stream, then run the pipeline against it. A baton
  # request (below) skips re-auth: the baton is a signed capability minted only after an
  # authorized open, and it names the stream it was minted for.
  defp dispatch(opts, conn, nil, requests, enc_opts) do
    arg = open_arg(opts, conn)

    with {:ok, context} <- authorize(opts, arg, bearer_token(conn)) do
      case Streams.create(opts.streams, new_stream_opts(opts, arg, context)) do
        {:ok, stream_id, seq, pid} ->
          run(opts, stream_id, pid, seq, requests, enc_opts)

        {:error, reason} ->
          open_failed_response(reason)
      end
    end
  end

  # Baton present: resolve it to a live stream and run the pipeline.
  defp dispatch(opts, _conn, baton, requests, enc_opts) when is_binary(baton) do
    with {:ok, {stream_id, seq}} <- decode_baton(baton, opts.key),
         {:ok, pid} <- find_stream(opts.streams, stream_id) do
      run(opts, stream_id, pid, seq, requests, enc_opts)
    end
  end

  defp run(opts, stream_id, pid, seq, requests, enc_opts) do
    case Stream.run(pid, seq, requests, enc_opts) do
      {:ok, :open, results, next_seq} ->
        {:ok, Baton.encode(stream_id, next_seq, opts.key), results}

      {:ok, :closed, results, nil} ->
        {:ok, nil, results}

      {:error, :baton_reused} ->
        {:error, 400, %Error{message: "baton reused", code: "BATON_REUSED"}}
    end
  catch
    # The stream was looked up but died (idle-expired) before the call landed.
    :exit, _ ->
      {:error, 400, %Error{message: "stream not found", code: "STREAM_NOT_FOUND"}}
  end

  defp handle_cursor(conn, opts, :json) do
    case read_request(conn) do
      {:ok, %{"batch" => batch} = body, conn} ->
        case dispatch_cursor(opts, conn, Map.get(body, "baton"), batch, rows: :json) do
          {:ok, new_baton, entries} ->
            head = %{"baton" => new_baton, "base_url" => opts.base_url}

            conn
            |> put_resp_content_type("text/plain")
            |> send_chunked(200)
            |> stream_lines([head | entries])

          {:error, status, %Error{} = error} ->
            send_json(conn, status, Error.encode(error))
        end

      {:ok, _no_batch, conn} ->
        send_json(
          conn,
          400,
          Error.encode(%Error{message: "missing batch", code: "FILO_BAD_REQUEST"})
        )

      {:error, :bad_json, conn} ->
        send_json(
          conn,
          400,
          Error.encode(%Error{message: "invalid JSON body", code: "FILO_BAD_REQUEST"})
        )
    end
  end

  defp handle_cursor(conn, opts, :protobuf) do
    case read_all(conn, "") do
      {:ok, body, conn} ->
        decoded = Protobuf.Http.decode_cursor_req(body)

        case Map.get(decoded, "batch") do
          nil ->
            send_protobuf(conn, 400, bad_request_pb("missing batch"))

          batch ->
            case dispatch_cursor(opts, conn, Map.get(decoded, "baton"), batch, rows: :maps) do
              {:ok, new_baton, entries} ->
                # Response is a stream of length-delimited protobufs: the
                # CursorRespBody first, then one CursorEntry per entry.
                head =
                  Protobuf.Http.encode_cursor_resp(%{
                    "baton" => new_baton,
                    "base_url" => opts.base_url
                  })

                frames = [
                  Wire.delimit(head)
                  | Enum.map(entries, &Wire.delimit(Protobuf.encode_cursor_entry(&1)))
                ]

                send_protobuf(conn, 200, frames)

              {:error, status, %Error{} = error} ->
                send_protobuf(conn, status, Protobuf.encode_error(Error.encode(error)))
            end
        end

      {:error, conn} ->
        send_protobuf(conn, 400, bad_request_pb())
    end
  end

  # Cursor responses are newline-delimited JSON: the CursorRespBody, then entries.
  #
  # Entries are batched into ~@cursor_chunk_bytes iodata chunks before hitting the
  # transport (fathom perf review 2026-07-23 #20): one `chunk/2` per ~40-byte row line
  # meant one adapter write + HTTP/1.1 chunk framing per row - a 100k-row cursor paid
  # 100k transport writes with framing overhead dominating payload, on the endpoint that
  # exists specifically for large results. The wire bytes are identical (chunked
  # framing boundaries are transport-invisible to the ndjson consumer).
  @cursor_chunk_bytes 32 * 1024

  defp stream_lines(conn, items) do
    {conn, buf, size} =
      Enum.reduce(items, {conn, [], 0}, fn item, {conn, buf, size} ->
        line = [Jason.encode_to_iodata!(item), "\n"]
        line_size = IO.iodata_length(line)

        if size + line_size >= @cursor_chunk_bytes do
          {:ok, conn} = chunk(conn, [buf, line])
          {conn, [], 0}
        else
          {conn, [buf, line], size + line_size}
        end
      end)

    case size do
      0 ->
        conn

      _ ->
        {:ok, conn} = chunk(conn, buf)
        conn
    end
  end

  defp dispatch_cursor(opts, conn, nil, batch, enc_opts) do
    arg = open_arg(opts, conn)

    with {:ok, context} <- authorize(opts, arg, bearer_token(conn)) do
      case Streams.create(opts.streams, new_stream_opts(opts, arg, context)) do
        {:ok, stream_id, seq, pid} ->
          run_cursor(opts, stream_id, pid, seq, batch, enc_opts)

        {:error, reason} ->
          open_failed_response(reason)
      end
    end
  end

  defp dispatch_cursor(opts, _conn, baton, batch, enc_opts) when is_binary(baton) do
    with {:ok, {stream_id, seq}} <- decode_baton(baton, opts.key),
         {:ok, pid} <- find_stream(opts.streams, stream_id) do
      run_cursor(opts, stream_id, pid, seq, batch, enc_opts)
    end
  end

  # A stream open that failed carrying the executor's own Filo.Error propagates that error and its
  # HTTP status hint (a client error like a missing/invalid shard → 400, at-capacity → 503) instead
  # of a blanket 500, so the client sees why the open was refused. Any other failure stays a 500.
  defp open_failed_response({:open_failed, %Error{} = error}) do
    {:error, error.status || 500, error}
  end

  defp open_failed_response(_reason) do
    {:error, 500, %Error{message: "could not open stream", code: "FILO_OPEN_FAILED"}}
  end

  defp run_cursor(opts, stream_id, pid, seq, batch, enc_opts) do
    case Stream.run_cursor(pid, seq, batch) do
      {:ok, %BatchResult{} = result, next_seq} ->
        {:ok, Baton.encode(stream_id, next_seq, opts.key), Cursor.entries(result, enc_opts)}

      {:error, :baton_reused} ->
        {:error, 400, %Error{message: "baton reused", code: "BATON_REUSED"}}
    end
  catch
    :exit, _ ->
      {:error, 400, %Error{message: "stream not found", code: "STREAM_NOT_FOUND"}}
  end

  defp new_stream_opts(opts, arg, context),
    do:
      [executor: opts.executor, open_arg: arg, open_context: context] ++
        idle_opt(opts) ++ Map.get(opts, :stream_spawn, [])

  # No `:authorize` configured ⇒ every request is accepted (the host trusts the network),
  # with a nil open context. A host callback may return `{:ok, context}` to thread a verified
  # per-connection term to `executor.open/2`; a bare `:ok` threads `nil`. A refusal propagates
  # the host error's HTTP status hint, defaulting to 401.
  defp authorize(%{authorize: nil}, _arg, _token), do: {:ok, nil}

  defp authorize(%{authorize: fun}, arg, token) when is_function(fun, 2) do
    case fun.(arg, token) do
      :ok -> {:ok, nil}
      {:ok, context} -> {:ok, context}
      {:error, %Error{} = error} -> {:error, error.status || 401, error}
    end
  end

  # The `Authorization: Bearer <token>` credential, or nil when absent/not-bearer.
  # Scheme match is case-insensitive per RFC 7235.
  defp bearer_token(conn) do
    with [value | _] <- get_req_header(conn, "authorization"),
         [scheme, token] <- String.split(value, " ", parts: 2),
         "bearer" <- String.downcase(scheme) do
      String.trim(token)
    else
      _ -> nil
    end
  end

  # --- Hrana 1 legacy HTTP API: stateless, one fresh connection per request ---

  defp handle_v1_execute(conn, opts) do
    case read_request(conn) do
      {:ok, %{"stmt" => stmt}, conn} ->
        result =
          with_conn(opts, conn, fn db ->
            case opts.executor.execute(db, Stmt.decode(stmt)) do
              {:ok, stmt_result} -> {:ok, %{"result" => StmtResult.encode(stmt_result)}}
              {:error, %Error{} = error} -> {:error, error}
            end
          end)

        respond_v1(conn, result)

      {:ok, _no_stmt, conn} ->
        send_json(
          conn,
          400,
          Error.encode(%Error{message: "missing stmt", code: "FILO_BAD_REQUEST"})
        )

      {:error, :bad_json, conn} ->
        send_json(
          conn,
          400,
          Error.encode(%Error{message: "invalid JSON body", code: "FILO_BAD_REQUEST"})
        )
    end
  end

  defp handle_v1_batch(conn, opts) do
    case read_request(conn) do
      {:ok, %{"batch" => batch}, conn} ->
        result =
          with_conn(opts, conn, fn db ->
            batch_result =
              batch
              |> Batch.decode()
              |> Batch.run(
                &opts.executor.execute(db, &1),
                fn -> opts.executor.autocommit?(db) end
              )

            {:ok, %{"result" => BatchResult.encode(batch_result)}}
          end)

        respond_v1(conn, result)

      {:ok, _no_batch, conn} ->
        send_json(
          conn,
          400,
          Error.encode(%Error{message: "missing batch", code: "FILO_BAD_REQUEST"})
        )

      {:error, :bad_json, conn} ->
        send_json(
          conn,
          400,
          Error.encode(%Error{message: "invalid JSON body", code: "FILO_BAD_REQUEST"})
        )
    end
  end

  # Authorizes (each v1 request is stateless — every one opens fresh), opens a
  # connection, runs `fun`, and always closes it.
  defp with_conn(opts, conn, fun) do
    arg = open_arg(opts, conn)

    case authorize(opts, arg, bearer_token(conn)) do
      {:ok, context} ->
        case Filo.Executor.open(opts.executor, arg, context) do
          {:ok, db} ->
            try do
              fun.(db)
            after
              opts.executor.close(db)
            end

          {:error, %Error{} = error} ->
            {:error, error}
        end

      {:error, _status, %Error{} = error} ->
        {:error, error}
    end
  end

  defp respond_v1(conn, {:ok, body}), do: send_json(conn, 200, body)

  defp respond_v1(conn, {:error, %Error{} = error}),
    do: send_json(conn, error.status || 400, Error.encode(error))

  defp decode_baton(baton, key) do
    case Baton.decode(baton, key) do
      {:ok, decoded} -> {:ok, decoded}
      {:error, :invalid} -> {:error, 400, %Error{message: "invalid baton", code: "BATON_INVALID"}}
    end
  end

  defp find_stream(streams, stream_id) do
    case Streams.lookup(streams, stream_id) do
      {:ok, pid} -> {:ok, pid}
      :error -> {:error, 400, %Error{message: "stream not found", code: "STREAM_NOT_FOUND"}}
    end
  end

  defp open_arg(%{open_arg: fun}, conn) when is_function(fun, 1), do: fun.(conn)
  defp open_arg(%{open_arg: arg}, _conn), do: arg

  defp idle_opt(%{idle_timeout: nil}), do: []
  defp idle_opt(%{idle_timeout: timeout}), do: [idle_timeout: timeout]

  defp read_request(conn) do
    case conn.body_params do
      %Plug.Conn.Unfetched{} ->
        case read_all(conn, "") do
          {:ok, raw, conn} ->
            case Jason.decode(raw) do
              {:ok, params} when is_map(params) -> {:ok, params, conn}
              _ -> {:error, :bad_json, conn}
            end

          {:error, conn} ->
            {:error, :bad_json, conn}
        end

      params when is_map(params) ->
        {:ok, params, conn}
    end
  end

  defp read_all(conn, acc) do
    case read_body(conn, length: @max_body) do
      {:ok, chunk, conn} -> {:ok, acc <> chunk, conn}
      {:more, chunk, conn} -> read_all(conn, acc <> chunk)
      {:error, _reason} -> {:error, conn}
    end
  end

  defp send_json(conn, status, body) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode_to_iodata!(body))
  end

  defp send_protobuf(conn, status, body) do
    conn
    # Binary body — no charset (would be wrong for protobuf).
    |> put_resp_content_type("application/x-protobuf", nil)
    |> send_resp(status, IO.iodata_to_binary(body))
  end

  defp bad_request_pb(message \\ "invalid protobuf body") do
    Protobuf.encode_error(%{"message" => message, "code" => "FILO_BAD_REQUEST"})
  end
end
