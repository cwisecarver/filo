defmodule Filo.Plug do
  @moduledoc """
  A `Plug` that speaks Hrana 3 over HTTP. Mount it in any Plug or Phoenix app to
  accept libSQL clients (`django-libsql`, `libsql-client`, the libSQL SDKs).

  ## Routes

    - `GET /v3` — protocol-support check; replies `200`.
    - `POST /v3/pipeline` — runs a pipeline of stream requests.

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

  alias Filo.{Baton, Error, Stream, Streams}

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
  def call(%Plug.Conn{method: "GET", path_info: ["v3"]} = conn, _opts) do
    send_resp(conn, 200, "Filo: Hrana over HTTP (v3)")
  end

  def call(%Plug.Conn{method: "POST", path_info: ["v3", "pipeline"]} = conn, opts) do
    handle_pipeline(conn, opts)
  end

  def call(conn, _opts), do: send_resp(conn, 404, "not found")

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
    stream_opts =
      [executor: opts.executor, open_arg: open_arg(opts, conn)] ++ idle_opt(opts)

    case Streams.create(opts.streams, stream_opts) do
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
