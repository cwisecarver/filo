defmodule Filo.Plug do
  @moduledoc """
  A `Plug` that speaks Hrana over HTTP (versions 1, 2, and 3) and upgrades
  WebSocket requests to `Filo.Socket` (Hrana over WebSocket). Mount it in any Plug
  or Phoenix app to accept libSQL clients (`django-libsql` over ws, `libsql-client`,
  `libsql-experimental`, the libSQL SDKs).

  ## Routes

    - `GET /v1` · `/v2` · `/v3` — protocol-support checks; reply `200`.
    - `POST /v2/pipeline` · `/v3/pipeline` — run a pipeline of stream requests.
    - `POST /v3/cursor` — run a batch, streaming the result (Hrana 3).
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

  @max_body 8_000_000

  @impl true
  def init(opts) do
    %{
      executor: Keyword.fetch!(opts, :executor),
      streams: Keyword.fetch!(opts, :streams),
      key: Keyword.fetch!(opts, :key),
      open_arg: Keyword.get(opts, :open_arg),
      base_url: Keyword.get(opts, :base_url),
      idle_timeout: Keyword.get(opts, :idle_timeout)
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
       when version in ~w(v1 v2 v3) do
    send_resp(conn, 200, "Filo: Hrana over HTTP (#{version})")
  end

  defp route(%Plug.Conn{method: "POST", path_info: [version, "pipeline"]} = conn, opts)
       when version in ~w(v2 v3) do
    handle_pipeline(conn, opts)
  end

  # Cursor is a Hrana 3 addition: it runs a batch and streams the result.
  defp route(%Plug.Conn{method: "POST", path_info: ["v3", "cursor"]} = conn, opts) do
    handle_cursor(conn, opts)
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
    socket_opts = [executor: opts.executor, open_arg: open_arg(opts, conn)]

    conn
    |> negotiate_subprotocol()
    |> Plug.Conn.upgrade_adapter(:websocket, {Filo.Socket, socket_opts, []})
  end

  @subprotocols ~w(hrana3 hrana2 hrana1)

  defp negotiate_subprotocol(conn) do
    offered = conn |> get_req_header("sec-websocket-protocol") |> Enum.flat_map(&tokens/1)

    case Enum.find(@subprotocols, &(&1 in offered)) do
      nil -> conn
      chosen -> put_resp_header(conn, "sec-websocket-protocol", chosen)
    end
  end

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

  defp handle_pipeline(conn, opts) do
    case read_request(conn) do
      {:ok, body, conn} ->
        baton = Map.get(body, "baton")
        requests = Map.get(body, "requests", [])

        case dispatch(opts, conn, baton, requests) do
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

  # No baton: open a fresh stream, then run the pipeline against it.
  defp dispatch(opts, conn, nil, requests) do
    case Streams.create(opts.streams, new_stream_opts(opts, conn)) do
      {:ok, stream_id, seq, pid} ->
        run(opts, stream_id, pid, seq, requests)

      {:error, _reason} ->
        {:error, 500, %Error{message: "could not open stream", code: "FILO_OPEN_FAILED"}}
    end
  end

  # Baton present: resolve it to a live stream and run the pipeline.
  defp dispatch(opts, _conn, baton, requests) when is_binary(baton) do
    with {:ok, {stream_id, seq}} <- decode_baton(baton, opts.key),
         {:ok, pid} <- find_stream(opts.streams, stream_id) do
      run(opts, stream_id, pid, seq, requests)
    end
  end

  defp run(opts, stream_id, pid, seq, requests) do
    case Stream.run(pid, seq, requests) do
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

  defp handle_cursor(conn, opts) do
    case read_request(conn) do
      {:ok, %{"batch" => batch} = body, conn} ->
        case dispatch_cursor(opts, conn, Map.get(body, "baton"), batch) do
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

  # Cursor responses are newline-delimited JSON: the CursorRespBody, then entries.
  defp stream_lines(conn, items) do
    Enum.reduce(items, conn, fn item, conn ->
      {:ok, conn} = chunk(conn, [Jason.encode!(item), "\n"])
      conn
    end)
  end

  defp dispatch_cursor(opts, conn, nil, batch) do
    case Streams.create(opts.streams, new_stream_opts(opts, conn)) do
      {:ok, stream_id, seq, pid} ->
        run_cursor(opts, stream_id, pid, seq, batch)

      {:error, _reason} ->
        {:error, 500, %Error{message: "could not open stream", code: "FILO_OPEN_FAILED"}}
    end
  end

  defp dispatch_cursor(opts, _conn, baton, batch) when is_binary(baton) do
    with {:ok, {stream_id, seq}} <- decode_baton(baton, opts.key),
         {:ok, pid} <- find_stream(opts.streams, stream_id) do
      run_cursor(opts, stream_id, pid, seq, batch)
    end
  end

  defp run_cursor(opts, stream_id, pid, seq, batch) do
    case Stream.run_cursor(pid, seq, batch) do
      {:ok, %BatchResult{} = result, next_seq} ->
        {:ok, Baton.encode(stream_id, next_seq, opts.key), Cursor.entries(result)}

      {:error, :baton_reused} ->
        {:error, 400, %Error{message: "baton reused", code: "BATON_REUSED"}}
    end
  catch
    :exit, _ ->
      {:error, 400, %Error{message: "stream not found", code: "STREAM_NOT_FOUND"}}
  end

  defp new_stream_opts(opts, conn),
    do: [executor: opts.executor, open_arg: open_arg(opts, conn)] ++ idle_opt(opts)

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

  # Opens a fresh connection, runs `fun`, and always closes it.
  defp with_conn(opts, conn, fun) do
    case opts.executor.open(open_arg(opts, conn)) do
      {:ok, db} ->
        try do
          fun.(db)
        after
          opts.executor.close(db)
        end

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end

  defp respond_v1(conn, {:ok, body}), do: send_json(conn, 200, body)
  defp respond_v1(conn, {:error, %Error{} = error}), do: send_json(conn, 400, Error.encode(error))

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
    |> send_resp(status, Jason.encode!(body))
  end
end
