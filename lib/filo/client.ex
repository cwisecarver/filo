defmodule Filo.Client do
  @moduledoc """
  A Hrana **client** — the mirror of `Filo.Plug`.

  Where the server decodes client requests and encodes results, a client encodes
  requests and decodes results, so it reuses the same protocol codec the server is
  built on: `Filo.Value` for cell values and `Filo.StmtResult` for the shape of a
  result. There is no second implementation of the wire format to drift from the
  server's.

  ## Model

  One `Filo.Client` is one Hrana **stream** over one owned transport connection. The
  first request opens the stream; each response carries a `baton` that threads into
  the next request, so a transaction is a burst of `execute/3`s on the held
  connection — the same per-statement round-trip model libSQL SDKs use over
  `POST /v2/pipeline`. The client is an immutable struct you thread through your own
  process; hold one per concurrent stream (on the BEAM, that is one cheap process
  each).

      {:ok, c} = Filo.Client.connect("http://localhost:8080", authority: "acme.example")
      {:ok, _res, c} = Filo.Client.execute(c, "CREATE TABLE t (a INTEGER, b TEXT)")
      {:ok, _res, c} = Filo.Client.execute(c, "INSERT INTO t VALUES (?, ?)", [1, "x"])
      {:ok, res, c}  = Filo.Client.execute(c, "SELECT a, b FROM t WHERE a = ?", [1])
      res.rows       #=> [[1, "x"]]
      :ok = Filo.Client.close(c)

  ## Transport

  The HTTP round-trip is a `Filo.Client.Transport` behaviour so Filo keeps its
  no-concrete-transport posture: the default `Filo.Client.Transport.Mint` needs the
  optional `:mint` dependency, and a host that would rather drive Finch/Req/`:gun`
  passes its own `:transport`.

  ## Scope

  This is the Hrana 2/3 **HTTP JSON pipeline** (`execute`, `close`, baton threading) —
  enough to drive a shard end to end. Protobuf, the WebSocket binding, cursors, and
  batches are natural follow-ups that reuse the same codec.
  """

  alias Filo.{Error, StmtResult, Value}

  @type t :: %__MODULE__{
          transport: module(),
          transport_state: term(),
          transport_opts: keyword(),
          uri: URI.t(),
          authority: String.t() | nil,
          path: String.t(),
          headers: [{String.t(), String.t()}],
          timeout: timeout(),
          baton: String.t() | nil
        }

  defstruct [
    :transport,
    :transport_state,
    :transport_opts,
    :uri,
    :authority,
    :path,
    :headers,
    :timeout,
    baton: nil
  ]

  @doc """
  Opens a Hrana stream to `url` (e.g. `"http://host:8080"`).

  ## Options

    - `:transport` — a `Filo.Client.Transport` module. Default
      `Filo.Client.Transport.Mint` (requires the optional `:mint` dep).
    - `:authority` — the `Host` header value. Set this to route by subdomain through
      a load balancer (`"acme.fathom.example"`), exactly as a libSQL SDK does.
      Default: the URL's host.
    - `:auth_token` — a bearer token sent as `Authorization: Bearer <token>`
      (libSQL's `authToken` over HTTP). Default: none.
    - `:version` — `"v2"` or `"v3"` (the pipeline path). Default `"v2"`.
    - `:headers` — extra request headers. Default `[]`.
    - `:timeout` — per-request timeout in ms. Default `15_000`.
    - `:transport_opts` — extra options passed to the transport's `connect/2`.
  """
  @spec connect(String.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def connect(url, opts \\ []) when is_binary(url) do
    uri = URI.parse(url)
    transport = Keyword.get(opts, :transport, Filo.Client.Transport.Mint)
    version = Keyword.get(opts, :version, "v2")
    timeout = Keyword.get(opts, :timeout, 15_000)

    headers =
      [{"content-type", "application/json"}]
      |> maybe_auth(Keyword.get(opts, :auth_token))
      |> Kernel.++(Keyword.get(opts, :headers, []))

    transport_opts = Keyword.merge([timeout: timeout], Keyword.get(opts, :transport_opts, []))

    case transport.connect(uri, transport_opts) do
      {:ok, state} ->
        {:ok,
         %__MODULE__{
           transport: transport,
           transport_state: state,
           transport_opts: transport_opts,
           uri: uri,
           authority: Keyword.get(opts, :authority) || uri.host,
           path: "/#{version}/pipeline",
           headers: headers,
           timeout: timeout
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Runs one SQL statement on the stream, binding `args` positionally.

  Returns the decoded `Filo.StmtResult` and the advanced client (its baton threaded
  for the next call). A SQL-level failure is `{:error, %Filo.Error{}, client}` with
  the stream **still open** (its baton advanced), matching the server. A transport
  failure is `{:error, {:transport, reason}, client}`; recover with `reconnect/1`.

  Values are encoded via `Filo.Value`, so integers, floats, text, `nil`, and
  `{:blob, binary}` all bind correctly.
  """
  @spec execute(t(), String.t(), [Value.native()]) ::
          {:ok, StmtResult.t(), t()} | {:error, Error.t() | {:transport, term()}, t()}
  def execute(%__MODULE__{} = client, sql, args \\ []) when is_binary(sql) do
    stmt = %{"sql" => sql, "args" => Enum.map(args, &Value.encode/1)}

    case pipeline(client, [%{"type" => "execute", "stmt" => stmt}]) do
      {:ok, [result], client} -> decode_execute(result, client)
      {:ok, _unexpected, client} -> {:error, client_error("unexpected pipeline result"), client}
      {:error, reason, client} -> {:error, reason, client}
    end
  end

  @doc """
  Sends a raw list of Hrana stream-request maps as one pipeline and returns the raw
  `results` list (and the advanced client). The low-level primitive `execute/3` is
  built on; use it to send request types this module has no sugar for yet
  (`batch`, `describe`, `get_autocommit`).
  """
  @spec pipeline(t(), [map()]) ::
          {:ok, [map()], t()} | {:error, Error.t() | {:transport, term()}, t()}
  def pipeline(%__MODULE__{} = client, requests) when is_list(requests) do
    body = Jason.encode_to_iodata!(%{"baton" => client.baton, "requests" => requests})
    headers = [{"host", client.authority} | client.headers]
    do_pipeline(client, headers, body, 1)
  end

  defp do_pipeline(client, headers, body, retries) do
    case client.transport.request(client.transport_state, "POST", client.path, headers, body) do
      {:ok, %{status: 200, body: raw}, state} ->
        doc = Jason.decode!(raw)
        client = %{client | transport_state: state, baton: Map.get(doc, "baton")}
        {:ok, Map.get(doc, "results", []), client}

      {:ok, %{status: status, body: raw}, state} ->
        client = %{client | transport_state: state}
        {:error, http_error(status, raw), client}

      {:error, :closed, state} when retries > 0 ->
        # The connection dropped — commonly a load balancer recycling an idle keep-alive after N
        # requests, or the peer's idle close. A Hrana stream SURVIVES a connection close (that is what
        # the baton is for), so transparently reopen the transport and retry the SAME request with the
        # SAME baton to resume the stream — the way a real SDK (and Python's http.client `auto_open`)
        # does, instead of surfacing an error that would make the caller abandon the stream (and any
        # open transaction it holds). The baton's sequence number keeps this exactly-once: if the
        # request had in fact been processed before the drop, the resend is rejected (`BATON_REUSED`),
        # never double-applied.
        client = %{client | transport_state: state}

        case reopen(client) do
          {:ok, client} -> do_pipeline(client, headers, body, retries - 1)
          {:error, reason} -> {:error, {:transport, reason}, client}
        end

      {:error, reason, state} ->
        {:error, {:transport, reason}, %{client | transport_state: state}}
    end
  end

  # Reopen the transport PRESERVING the baton (to resume the stream) — the difference from the public
  # reconnect/1, which resets the baton to start a fresh stream.
  defp reopen(%__MODULE__{} = client) do
    _ = client.transport.close(client.transport_state)

    case client.transport.connect(client.uri, client.transport_opts) do
      {:ok, state} -> {:ok, %{client | transport_state: state}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Drops the transport connection and opens a fresh one, resetting the baton.

  The server rolls back any uncommitted transaction on the abandoned stream. Use it
  to recover from a transport error (a stale keepalive the server closed, or a `502`
  while a load balancer moves the shard) without losing the client.
  """
  @spec reconnect(t()) :: {:ok, t()} | {:error, term()}
  def reconnect(%__MODULE__{} = client) do
    _ = client.transport.close(client.transport_state)

    case client.transport.connect(client.uri, client.transport_opts) do
      {:ok, state} -> {:ok, %{client | transport_state: state, baton: nil}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Closes the stream (a best-effort `close` request that lets the server release the
  connection) and the underlying transport.
  """
  @spec close(t()) :: :ok
  def close(%__MODULE__{} = client) do
    _ = pipeline(client, [%{"type" => "close"}])
    client.transport.close(client.transport_state)
  end

  # --- internals ---

  defp decode_execute(%{"type" => "ok", "response" => %{"result" => result}}, client),
    do: {:ok, StmtResult.decode(result), client}

  defp decode_execute(%{"type" => "error", "error" => error}, client),
    do: {:error, %Error{message: error["message"], code: error["code"]}, client}

  defp decode_execute(_other, client), do: {:error, client_error("malformed result"), client}

  defp maybe_auth(headers, nil), do: headers
  defp maybe_auth(headers, token), do: [{"authorization", "Bearer " <> token} | headers]

  defp http_error(status, raw) do
    %Error{
      message: "HTTP #{status}: #{String.slice(raw, 0, 200)}",
      code: "FILO_HTTP_#{status}",
      status: status
    }
  end

  defp client_error(message), do: %Error{message: message, code: "FILO_CLIENT"}
end
