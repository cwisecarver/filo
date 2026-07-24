defmodule Filo.Client.Transport.Mint do
  @moduledoc """
  The default `Filo.Client.Transport`, over [Mint](https://hex.pm/packages/mint).

  Mint is an **optional** dependency: it is not pulled in by depending on Filo, so a
  host that only serves Hrana never pays for it. To use the client with this
  transport, add `{:mint, "~> 1.6"}` to your deps (or pass your own `:transport`).

  One owned HTTP/1 connection in passive mode, so the request/response round-trip is
  synchronous — the natural fit for a single sequential Hrana stream, with one such
  connection held per client process.
  """

  @behaviour Filo.Client.Transport

  # Mint is optional; a host without it that compiles this module (e.g. under
  # --warnings-as-errors) must not fail on the unresolved reference. `connect/2`
  # raises a clear error at runtime if it is genuinely missing.
  @compile {:no_warn_undefined, [Mint.HTTP, Mint.Types]}

  @enforce_keys [:conn, :timeout]
  defstruct [:conn, :timeout]

  @impl true
  def connect(%URI{} = uri, opts) do
    ensure_mint!()
    scheme = scheme(uri.scheme)
    host = uri.host || "localhost"
    port = uri.port || default_port(scheme)
    timeout = Keyword.get(opts, :timeout, 15_000)

    case Mint.HTTP.connect(scheme, host, port, mode: :passive, protocols: [:http1]) do
      {:ok, conn} -> {:ok, %__MODULE__{conn: conn, timeout: timeout}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def request(%__MODULE__{} = state, method, path, headers, body) do
    body = IO.iodata_to_binary(body)

    case Mint.HTTP.request(state.conn, method, path, headers, body) do
      {:ok, conn, ref} ->
        case recv(conn, ref, %{status: nil, headers: [], data: []}, state.timeout) do
          {:ok, conn, response} -> {:ok, response, %{state | conn: conn}}
          {:error, conn, reason} -> {:error, reason, %{state | conn: conn}}
        end

      {:error, conn, reason} ->
        {:error, reason, %{state | conn: conn}}
    end
  end

  @impl true
  def close(%__MODULE__{conn: conn}) do
    _ = Mint.HTTP.close(conn)
    :ok
  end

  # Blocking read of one full response over the passive socket.
  defp recv(conn, ref, acc, timeout) do
    case Mint.HTTP.recv(conn, 0, timeout) do
      {:ok, conn, responses} ->
        case collect(responses, ref, acc) do
          {:done, acc} ->
            {:ok, conn,
             %{status: acc.status, headers: acc.headers, body: IO.iodata_to_binary(acc.data)}}

          {:error, reason} ->
            {:error, conn, reason}

          acc ->
            recv(conn, ref, acc, timeout)
        end

      {:error, conn, reason, _responses} ->
        {:error, conn, reason}
    end
  end

  defp collect(responses, ref, acc) do
    Enum.reduce_while(responses, acc, fn
      {:status, ^ref, status}, acc -> {:cont, %{acc | status: status}}
      {:headers, ^ref, headers}, acc -> {:cont, %{acc | headers: acc.headers ++ headers}}
      {:data, ^ref, data}, acc -> {:cont, %{acc | data: [acc.data, data]}}
      {:done, ^ref}, acc -> {:halt, {:done, acc}}
      {:error, ^ref, reason}, _acc -> {:halt, {:error, reason}}
      _other, acc -> {:cont, acc}
    end)
  end

  defp ensure_mint! do
    unless Code.ensure_loaded?(Mint.HTTP) do
      raise """
      Filo.Client.Transport.Mint requires the optional :mint dependency.
      Add {:mint, "~> 1.6"} to your deps, or pass a custom :transport to Filo.Client.connect/2.
      """
    end
  end

  defp scheme("https"), do: :https
  defp scheme(_), do: :http

  defp default_port(:https), do: 443
  defp default_port(_), do: 80
end
