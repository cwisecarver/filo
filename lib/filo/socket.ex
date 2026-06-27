defmodule Filo.Socket do
  @moduledoc """
  A `WebSock` handler that speaks Hrana over WebSocket. This is the binding the
  libSQL clients use over `ws://`/`wss://`/`libsql://` — notably `django-libsql`
  (via `libsql-client`), which has no HTTP-pipeline path.

  Mount it from the host's server by upgrading a WebSocket request to this
  handler (see `Filo.Plug`, which performs the upgrade). The handler is server
  agnostic: it only implements the `WebSock` behaviour.

  ## Protocol

  After the WebSocket opens (subprotocol `hrana2`/`hrana3`), the client sends a
  `hello` and the server replies `hello_ok`. Thereafter every message is a
  `request` carrying a `request_id`, answered by a `response_ok` or
  `response_error` with the same id. Requests:

    - `open_stream` / `close_stream` — open and release a connection; the client
      allocates the `stream_id`. A connection lives for the life of its stream,
      so state (transactions, temp tables) persists across requests.
    - `execute` / `batch` / `describe` / `get_autocommit` — run against a stream.
    - `store_sql` / `close_sql` — cache SQL text under a `sql_id` for the
      connection, referenced by later statements.

  The per-request payloads reuse the shared protocol core (`Filo.Stmt`,
  `Filo.Batch`, `Filo.Request`, …) — only the framing and stream bookkeeping are
  WebSocket specific. Unlike the HTTP binding there are no batons: the persistent
  socket plus the client-allocated `stream_id` identify a stream.
  """

  @behaviour WebSock

  alias Filo.{Error, Request}

  defstruct [:executor, :open_arg, hello?: false, streams: %{}, sqls: %{}]

  @doc """
  Initializes a connection's handler state.

  Options: `:executor` (required, the `Filo.Executor` module) and `:open_arg`
  (host context passed to `executor.open/1` for each stream).
  """
  @impl true
  def init(opts) do
    state = %__MODULE__{
      executor: Keyword.fetch!(opts, :executor),
      open_arg: Keyword.get(opts, :open_arg)
    }

    {:ok, state}
  end

  @impl true
  def handle_in({text, [opcode: :text]}, state) do
    case Jason.decode(text) do
      {:ok, message} -> handle_message(message, state)
      {:error, _} -> {:stop, :normal, state}
    end
  end

  # Hrana is a text protocol; reject binary frames.
  def handle_in({_data, [opcode: :binary]}, state), do: {:stop, :normal, state}

  # Filo never sends itself process messages — it only replies to client frames.
  @impl true
  def handle_info(_message, state), do: {:ok, state}

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.streams, fn {_id, conn} -> state.executor.close(conn) end)
    :ok
  end

  defp handle_message(%{"type" => "hello"}, %{hello?: false} = state) do
    push(%{"type" => "hello_ok"}, %{state | hello?: true})
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
      case state.executor.open(state.open_arg) do
        {:ok, conn} ->
          {{:ok, %{"type" => "open_stream"}},
           %{state | streams: Map.put(state.streams, sid, conn)}}

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
        {{:ok, %{"type" => "close_stream"}}, %{state | streams: streams}}
    end
  end

  defp handle_request(%{"type" => "store_sql", "sql_id" => sql_id, "sql" => sql}, state) do
    if Map.has_key?(state.sqls, sql_id) do
      {{:error, encode(error("sql_id #{sql_id} is already stored", "SQL_EXISTS"))}, state}
    else
      {{:ok, %{"type" => "store_sql"}}, %{state | sqls: Map.put(state.sqls, sql_id, sql)}}
    end
  end

  defp handle_request(%{"type" => "close_sql", "sql_id" => sql_id}, state) do
    {{:ok, %{"type" => "close_sql"}}, %{state | sqls: Map.delete(state.sqls, sql_id)}}
  end

  defp handle_request(%{"type" => type, "stream_id" => sid} = request, state)
       when type in ~w(execute batch sequence describe get_autocommit) do
    case Map.fetch(state.streams, sid) do
      {:ok, conn} ->
        inner = request |> Map.delete("stream_id") |> resolve_sql(state.sqls)
        {_status, stream_result} = Request.handle(state.executor, conn, inner)
        {from_stream_result(stream_result), state}

      :error ->
        {{:error, encode(error("stream #{sid} not found", "STREAM_NOT_FOUND"))}, state}
    end
  end

  defp handle_request(%{"type" => type}, state) do
    {{:error, encode(error("unsupported request: #{type}", "FILO_UNSUPPORTED"))}, state}
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

  defp push(message, state), do: {:push, {:text, Jason.encode!(message)}, state}

  defp error(message, code \\ "FILO_PROTO_ERROR"), do: %Error{message: message, code: code}
  defp encode(%Error{} = error), do: Error.encode(error)
end
