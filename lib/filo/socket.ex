defmodule Filo.Socket do
  @moduledoc """
  A `WebSock` handler that speaks Hrana over WebSocket. This is the binding the
  libSQL clients use over `ws://`/`wss://`/`libsql://` — notably `django-libsql`
  (via `libsql-client`), which has no HTTP-pipeline path.

  Mount it from the host's server by upgrading a WebSocket request to this
  handler (see `Filo.Plug`, which performs the upgrade). The handler is server
  agnostic: it only implements the `WebSock` behaviour.

  ## Protocol

  After the WebSocket opens (subprotocol `hrana2`/`hrana3`, or `hrana3-protobuf`
  for the binary Protobuf encoding), the client sends a
  `hello` and the server replies `hello_ok`. Thereafter every message is a
  `request` carrying a `request_id`, answered by a `response_ok` or
  `response_error` with the same id. Requests:

    - `open_stream` / `close_stream` — open and release a connection; the client
      allocates the `stream_id`. A connection lives for the life of its stream,
      so state (transactions, temp tables) persists across requests.
    - `execute` / `batch` / `sequence` / `describe` / `get_autocommit` — run
      against a stream.
    - `store_sql` / `close_sql` — cache SQL text under a `sql_id` for the
      connection, referenced by later statements.
    - `open_cursor` / `fetch_cursor` / `close_cursor` (Hrana 3) — run a batch and
      read its results incrementally as cursor entries.

  The per-request payloads reuse the shared protocol core (`Filo.Stmt`,
  `Filo.Batch`, `Filo.Request`, …) — only the framing and stream bookkeeping are
  WebSocket specific. Unlike the HTTP binding there are no batons: the persistent
  socket plus the client-allocated `stream_id` identify a stream.
  """

  @behaviour WebSock

  alias Filo.{Batch, Cursor, Error, Request}

  # Per-connection resource caps (defence against an unbounded-growth DoS: a single socket that keeps
  # `store_sql`-ing distinct ids, or opening cursors without closing them, grows the handler's state
  # without limit). Deliberately GENEROUS — no legitimate client stores hundreds of distinct prepared
  # statements or opens dozens of concurrent cursors on ONE connection — so they bound abuse without
  # tripping any real workload. Each is overridable via `init/1` opts for a host with unusual needs.
  @default_max_sqls 512
  @default_max_sql_bytes 16_000_000
  @default_max_cursors 128

  defstruct [
    :executor,
    :open_arg,
    :authorize,
    :header_token,
    # The `:authorize` context captured at the `hello` handshake (see Filo.Executor.open/2).
    # Held for the life of the connection and threaded to EVERY stream's open — so a per-connection
    # credential (e.g. a read-only scope) applies to the 2nd stream, not just the first.
    :open_context,
    encoding: :json,
    hello?: false,
    streams: %{},
    sqls: %{},
    cursors: %{},
    # Monitor ref => stream_id, for connections whose executor reports an owner
    # process (Filo.Executor.owner/1): the owner's death tears that stream down.
    owners: %{},
    # Per-connection resource caps (see the @default_* attributes above).
    max_sqls: @default_max_sqls,
    max_sql_bytes: @default_max_sql_bytes,
    max_cursors: @default_max_cursors
  ]

  @doc """
  Initializes a connection's handler state.

  Options: `:executor` (required, the `Filo.Executor` module), `:open_arg`
  (host context passed to `executor.open/1` for each stream), `:encoding`
  (`:json` (default) or `:protobuf`, set from the negotiated subprotocol),
  `:authorize` (optional 2-arity host callback `fun(open_arg, token)` — see
  `Filo.Plug`; checked at the `hello` handshake against the hello's `jwt`
  field), and `:header_token` (a bearer token the host extracted from the
  upgrade request, the fallback when the hello carries no `jwt`).

  Per-connection resource caps (all optional, generous defaults; override only for
  unusual workloads): `:max_sqls` (stored-statement count, default #{@default_max_sqls}),
  `:max_sql_bytes` (total stored-SQL bytes, default #{@default_max_sql_bytes}), and
  `:max_cursors` (concurrently open cursors, default #{@default_max_cursors}). Past a cap the
  offending `store_sql`/`open_cursor` is refused with a `SQL_LIMIT`/`CURSOR_LIMIT` error.
  """
  @impl true
  def init(opts) do
    state = %__MODULE__{
      executor: Keyword.fetch!(opts, :executor),
      open_arg: Keyword.get(opts, :open_arg),
      authorize: Keyword.get(opts, :authorize),
      header_token: Keyword.get(opts, :header_token),
      encoding: Keyword.get(opts, :encoding, :json),
      max_sqls: Keyword.get(opts, :max_sqls, @default_max_sqls),
      max_sql_bytes: Keyword.get(opts, :max_sql_bytes, @default_max_sql_bytes),
      max_cursors: Keyword.get(opts, :max_cursors, @default_max_cursors)
    }

    {:ok, state}
  end

  @impl true
  def handle_in({text, [opcode: :text]}, %{encoding: :json} = state) do
    case Jason.decode(text) do
      {:ok, message} ->
        handle_message(message, state)

      # Close 1007, not a bare 1000. RFC 6455 §5.6 requires a text frame's payload to be valid
      # UTF-8 and §7.4.1 assigns 1007 ("data inconsistent with the type of the message") to that
      # failure. Bandit's `validate_text_frames` produced that close itself, but it does so by
      # walking every inbound frame's bytes — immediately before `Jason.decode/1` walks the same
      # bytes again. A host that turns the option off to remove the duplicate scan (fathom does)
      # would otherwise silently downgrade every malformed-frame close to 1000, because Jason's
      # failure lands here. Emitting 1007 from the decode failure keeps the close code identical
      # either way, so disabling the pre-scan is conformance-neutral rather than a spec deviation.
      #
      # Invalid UTF-8 is still rejected exactly as before — Jason cannot parse it — so this
      # changes only which code accompanies the close.
      {:error, _} ->
        {:stop, :normal, {1007, "invalid frame payload data"}, state}
    end
  end

  def handle_in({data, [opcode: :binary]}, %{encoding: :protobuf} = state) do
    case safe_decode_client_msg(data) do
      {:ok, message} -> handle_message(message, state)
      :error -> {:stop, :normal, state}
    end
  end

  # The frame type must match the negotiated encoding (Hrana 3 spec): a binary
  # frame under JSON, or a text frame under Protobuf, is a protocol violation.
  def handle_in({_data, _opts}, state), do: {:stop, :normal, state}

  defp safe_decode_client_msg(data) do
    case Filo.Protobuf.Ws.decode_client_msg(data) do
      nil -> :error
      message -> {:ok, message}
    end
  rescue
    _ -> :error
  end

  # A connection's owner died (see Filo.Executor.owner/1): close that stream's
  # connection now, so it never keeps writing into a file the owner's successor may
  # flush/drop from under it. The client's next request on the stream_id gets
  # STREAM_NOT_FOUND and reopens (landing on the successor).
  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.owners, ref) do
      {nil, _owners} ->
        {:ok, state}

      {sid, owners} ->
        {conn, streams} = Map.pop(state.streams, sid)
        if conn, do: state.executor.close(conn)
        {:ok, %{state | streams: streams, owners: owners}}
    end
  end

  # Filo never sends itself other process messages — it only replies to client frames.
  def handle_info(_message, state), do: {:ok, state}

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.streams, fn {_id, conn} -> state.executor.close(conn) end)
    :ok
  end

  # The hello handshake is where Hrana-over-WebSocket authenticates: the client's token
  # arrives as the hello's `jwt` field (libSQL clients put `authToken` there, NOT in an
  # upgrade header — so a plug in front of the upgrade never sees it). Fall back to the
  # upgrade request's bearer token for clients that do send a header. A refused hello
  # gets the spec's fatal `hello_error`, pushed before the close frame (1008, policy
  # violation), so no stream can ever open on an unauthorized socket.
  defp handle_message(%{"type" => "hello"} = msg, %{hello?: false} = state) do
    case authorize(state, Map.get(msg, "jwt") || state.header_token) do
      {:ok, context} ->
        # Capture the verified context once, at hello, and hold it for the whole connection —
        # every later open_stream threads it, so a per-connection scope can't be escalated away
        # after the first stream.
        push(%{"type" => "hello_ok"}, %{state | hello?: true, open_context: context})

      {:error, %Error{} = error} ->
        hello_error = %{"type" => "hello_error", "error" => encode(error)}
        {:stop, :normal, {1008, error.message}, [frame(hello_error, state)], state}
    end
  end

  defp handle_message(%{"type" => "hello"}, state) do
    push(%{"type" => "hello_error", "error" => encode(error("hello already received"))}, state)
  end

  # Any request before the hello handshake is a protocol violation.
  defp handle_message(%{"type" => "request"}, %{hello?: false} = state) do
    {:stop, :normal, state}
  end

  defp handle_message(%{"type" => "request", "request_id" => id, "request" => request}, state) do
    {outcome, state} = handle_request(request, state)

    case outcome do
      {:ok, response} ->
        push(%{"type" => "response_ok", "request_id" => id, "response" => response}, state)

      {:error, error_map} ->
        push(%{"type" => "response_error", "request_id" => id, "error" => error_map}, state)
    end
  end

  defp handle_message(_unknown, state), do: {:stop, :normal, state}

  defp handle_request(%{"type" => "open_stream", "stream_id" => sid}, state) do
    if Map.has_key?(state.streams, sid) do
      {{:error, encode(error("stream #{sid} already exists", "STREAM_EXISTS"))}, state}
    else
      case Filo.Executor.open(state.executor, state.open_arg, state.open_context) do
        {:ok, conn} ->
          owners =
            case Filo.Executor.owner_pid(state.executor, conn) do
              nil -> state.owners
              pid -> Map.put(state.owners, Process.monitor(pid), sid)
            end

          {{:ok, %{"type" => "open_stream"}},
           %{state | streams: Map.put(state.streams, sid, conn), owners: owners}}

        {:error, %Error{} = error} ->
          {{:error, encode(error)}, state}
      end
    end
  end

  defp handle_request(%{"type" => "close_stream", "stream_id" => sid}, state) do
    case Map.pop(state.streams, sid) do
      {nil, _streams} ->
        # Closing an unknown/already-closed stream is not an error.
        {{:ok, %{"type" => "close_stream"}}, state}

      {conn, streams} ->
        state.executor.close(conn)

        {{:ok, %{"type" => "close_stream"}},
         %{state | streams: streams, owners: forget(state.owners, sid)}}
    end
  end

  defp handle_request(%{"type" => "store_sql", "sql_id" => sql_id, "sql" => sql}, state) do
    cond do
      Map.has_key?(state.sqls, sql_id) ->
        {{:error, encode(error("sql_id #{sql_id} is already stored", "SQL_EXISTS"))}, state}

      # Cap the COUNT of stored statements (expert review 2026-09-05 #23). Unbounded `store_sql` on a
      # single socket is a memory-growth DoS; a real client caches a handful of prepared statements.
      map_size(state.sqls) >= state.max_sqls ->
        {{:error,
          encode(
            error("too many stored SQLs on this connection (max #{state.max_sqls})", "SQL_LIMIT")
          )}, state}

      # Cap the total BYTES too: the count cap alone still admits `max_sqls` huge statements. Summed
      # lazily over the (already count-bounded) map, so this is O(max_sqls) on a cold `store_sql`.
      stored_sql_bytes(state.sqls) + byte_size(sql) > state.max_sql_bytes ->
        {{:error,
          encode(
            error(
              "stored SQL byte budget exceeded on this connection (max #{state.max_sql_bytes})",
              "SQL_LIMIT"
            )
          )}, state}

      true ->
        {{:ok, %{"type" => "store_sql"}}, %{state | sqls: Map.put(state.sqls, sql_id, sql)}}
    end
  end

  defp handle_request(%{"type" => "close_sql", "sql_id" => sql_id}, state) do
    {{:ok, %{"type" => "close_sql"}}, %{state | sqls: Map.delete(state.sqls, sql_id)}}
  end

  defp handle_request(
         %{"type" => "open_cursor", "stream_id" => sid, "cursor_id" => cid, "batch" => batch},
         state
       ) do
    cond do
      Map.has_key?(state.cursors, cid) ->
        {{:error, encode(error("cursor #{cid} already open", "CURSOR_EXISTS"))}, state}

      # Cap the number of concurrently OPEN cursors per connection (expert review 2026-09-05 #23).
      # A cursor holds its batch's materialized entries in state until `close_cursor`/`fetch_cursor`
      # drains it, so opening cursors without closing them grows memory without bound. Checked before
      # STREAM_NOT_FOUND so a client cannot probe stream existence past the cap.
      map_size(state.cursors) >= state.max_cursors ->
        {{:error,
          encode(
            error(
              "too many open cursors on this connection (max #{state.max_cursors})",
              "CURSOR_LIMIT"
            )
          )}, state}

      not Map.has_key?(state.streams, sid) ->
        {{:error, encode(error("stream #{sid} not found", "STREAM_NOT_FOUND"))}, state}

      true ->
        conn = Map.fetch!(state.streams, sid)
        %{"batch" => batch} = resolve_sql(%{"batch" => batch}, state.sqls)

        result =
          batch
          |> Batch.decode()
          |> Batch.run(
            &state.executor.execute(conn, &1),
            fn -> state.executor.autocommit?(conn) end
          )

        entries = Cursor.entries(result, rows_opt(state))

        {{:ok, %{"type" => "open_cursor"}},
         %{state | cursors: Map.put(state.cursors, cid, entries)}}
    end
  end

  defp handle_request(%{"type" => "fetch_cursor", "cursor_id" => cid} = request, state) do
    case Map.fetch(state.cursors, cid) do
      {:ok, entries} ->
        {taken, rest} = Enum.split(entries, Map.get(request, "max_count", 0))
        response = %{"type" => "fetch_cursor", "entries" => taken, "done" => rest == []}
        {{:ok, response}, %{state | cursors: Map.put(state.cursors, cid, rest)}}

      :error ->
        {{:error, encode(error("cursor #{cid} not found", "CURSOR_NOT_FOUND"))}, state}
    end
  end

  defp handle_request(%{"type" => "close_cursor", "cursor_id" => cid}, state) do
    {{:ok, %{"type" => "close_cursor"}}, %{state | cursors: Map.delete(state.cursors, cid)}}
  end

  defp handle_request(%{"type" => type, "stream_id" => sid} = request, state)
       when type in ~w(execute batch sequence describe get_autocommit) do
    case Map.fetch(state.streams, sid) do
      {:ok, conn} ->
        inner = request |> Map.delete("stream_id") |> resolve_sql(state.sqls)
        {_status, stream_result} = Request.handle(state.executor, conn, inner, rows_opt(state))
        {from_stream_result(stream_result), state}

      :error ->
        {{:error, encode(error("stream #{sid} not found", "STREAM_NOT_FOUND"))}, state}
    end
  end

  defp handle_request(%{"type" => type}, state) do
    {{:error, encode(error("unsupported request: #{type}", "FILO_UNSUPPORTED"))}, state}
  end

  # Drop (and flush) the owner monitor for a stream that closed normally, so a later
  # owner death can't fire a stale :DOWN for it.
  defp forget(owners, sid) do
    case Enum.find(owners, fn {_ref, s} -> s == sid end) do
      nil ->
        owners

      {ref, _sid} ->
        Process.demonitor(ref, [:flush])
        Map.delete(owners, ref)
    end
  end

  # Maps `Filo.Request`'s in-band stream result to a WebSocket frame outcome.
  defp from_stream_result(%{"type" => "ok", "response" => response}), do: {:ok, response}
  defp from_stream_result(%{"type" => "error", "error" => error_map}), do: {:error, error_map}

  # Substitutes a stored SQL text for any `sql_id` reference before dispatch.
  defp resolve_sql(%{"stmt" => stmt} = request, sqls),
    do: %{request | "stmt" => resolve_stmt(stmt, sqls)}

  defp resolve_sql(%{"batch" => %{"steps" => steps} = batch} = request, sqls) do
    steps = Enum.map(steps, &resolve_step(&1, sqls))
    %{request | "batch" => %{batch | "steps" => steps}}
  end

  defp resolve_sql(%{"type" => "sequence", "sql_id" => sql_id} = request, sqls)
       when not is_map_key(request, "sql") do
    case Map.fetch(sqls, sql_id) do
      {:ok, sql} -> request |> Map.delete("sql_id") |> Map.put("sql", sql)
      :error -> request
    end
  end

  defp resolve_sql(request, _sqls), do: request

  defp resolve_step(%{"stmt" => stmt} = step, sqls),
    do: %{step | "stmt" => resolve_stmt(stmt, sqls)}

  defp resolve_step(step, _sqls), do: step

  defp resolve_stmt(%{"sql_id" => sql_id} = stmt, sqls) when not is_map_key(stmt, "sql") do
    case Map.fetch(sqls, sql_id) do
      {:ok, sql} -> stmt |> Map.delete("sql_id") |> Map.put("sql", sql)
      :error -> stmt
    end
  end

  defp resolve_stmt(stmt, _sqls), do: stmt

  # No host callback configured ⇒ every hello is accepted (the pre-auth behavior), with a nil
  # open context. A host callback may return `{:ok, context}` to thread a verified per-connection
  # term (e.g. the token's scope) to `executor.open/2`; a bare `:ok` threads `nil`.
  defp authorize(%{authorize: nil}, _token), do: {:ok, nil}

  defp authorize(%{authorize: fun, open_arg: arg}, token) when is_function(fun, 2) do
    case fun.(arg, token) do
      :ok -> {:ok, nil}
      {:ok, context} -> {:ok, context}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp push(message, state), do: {:push, frame(message, state), state}

  defp frame(message, %{encoding: :protobuf}),
    do: {:binary, IO.iodata_to_binary(Filo.Protobuf.Ws.encode_server_msg(message))}

  # WebSock frames accept iodata (its message type), so skip Jason's final
  # IO.iodata_to_binary flatten — one less O(frame) copy per message (review #9).
  defp frame(message, _state), do: {:text, Jason.encode_to_iodata!(message)}

  # Result-encoder opts by negotiated encoding: the JSON transport takes pre-encoded row
  # fragments; the protobuf transport traverses the :maps form.
  defp rows_opt(%{encoding: :json}), do: [rows: :json]
  defp rows_opt(_state), do: []

  defp stored_sql_bytes(sqls),
    do: Enum.reduce(sqls, 0, fn {_id, sql}, acc -> acc + byte_size(sql) end)

  defp error(message, code \\ "FILO_PROTO_ERROR"), do: %Error{message: message, code: code}
  defp encode(%Error{} = error), do: Error.encode(error)
end
