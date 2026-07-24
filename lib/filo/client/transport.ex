defmodule Filo.Client.Transport do
  @moduledoc """
  The HTTP transport a `Filo.Client` drives — a behaviour, so Filo ships the Hrana
  protocol without pinning a concrete HTTP client on hosts (the same posture the
  server keeps with `Plug`/`WebSock`).

  Filo provides `Filo.Client.Transport.Mint` as the default, behind the optional
  `:mint` dependency. A host that already runs Finch, Req, or `:gun` can implement
  this behaviour over it and pass it as `Filo.Client.connect(url, transport: MyTransport)`.

  An implementation owns **one** connection and issues requests on it serially (a
  Hrana stream is sequential — one baton-threaded round-trip at a time), so it does
  not need its own pool or concurrency.
  """

  @typedoc "Opaque per-connection state carried between calls by the client."
  @type state :: term()

  @typedoc "A decoded HTTP response."
  @type response :: %{
          status: non_neg_integer(),
          headers: [{String.t(), String.t()}],
          body: binary()
        }

  @doc "Opens a connection to `uri`. `opts` carries at least `:timeout` (ms)."
  @callback connect(URI.t(), keyword()) :: {:ok, state()} | {:error, term()}

  @doc """
  Sends one request on the held connection and reads the full response.

  Returns the advanced state either way so a transport error still hands the client
  a connection to `close/1`.

  Report a **dropped connection** (the peer or a load balancer closed the socket) as the
  reason `:closed` — that is the sentinel `Filo.Client` treats as retryable, reconnecting and
  resuming the stream by baton. Any other reason is returned to the caller unretried.
  """
  @callback request(
              state(),
              method :: String.t(),
              path :: String.t(),
              headers :: [{String.t(), String.t()}],
              body :: iodata()
            ) :: {:ok, response(), state()} | {:error, term(), state()}

  @doc "Closes the connection. Always returns `:ok`."
  @callback close(state()) :: :ok
end
