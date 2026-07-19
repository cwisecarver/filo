defmodule Filo.AuthorizeTest.Echo do
  @moduledoc false
  @behaviour Filo.Executor

  @impl true
  def open(_arg), do: {:ok, :conn}

  @impl true
  def execute(:conn, %Filo.Stmt{}), do: {:ok, %Filo.StmtResult{cols: ["n"], rows: [[1]]}}

  @impl true
  def autocommit?(:conn), do: true

  @impl true
  def close(:conn), do: :ok
end

defmodule Filo.AuthorizeTest.ContextEcho do
  @moduledoc false
  # Proves the `:authorize` context reaches `open`: the handle carries whatever context
  # `open/2` received, and `execute` reflects it back as a text row. `open/1` (the
  # back-compat path, no context) is still defined so the required callback is satisfied;
  # Filo prefers `open/2` when present, so a threaded context lands there.
  @behaviour Filo.Executor

  @impl true
  def open(_arg), do: {:ok, {:conn, :no_context}}

  @impl true
  def open(_arg, context), do: {:ok, {:conn, context}}

  @impl true
  def execute({:conn, context}, %Filo.Stmt{}),
    do: {:ok, %Filo.StmtResult{cols: ["ctx"], rows: [[inspect(context)]]}}

  @impl true
  def autocommit?({:conn, _context}), do: true

  @impl true
  def close({:conn, _context}), do: :ok
end

defmodule Filo.AuthorizeTest do
  # The `:authorize` seam: a host callback `fun(open_arg, token)` gating every stream
  # open. Symptom pinned: the Hrana data path used to carry NO credential check at all —
  # any request that reached the port ran SQL. With `:authorize` configured, an
  # unauthorized HTTP request is refused with the error's status (default 401) before
  # `executor.open/1` runs, and an unauthorized WebSocket hello gets a fatal
  # `hello_error` so no stream can open on that socket.
  #
  # Shares a globally-named Filo.Streams supervisor, so not concurrent.
  use ExUnit.Case, async: false

  import Plug.Test
  import Plug.Conn

  alias Filo.AuthorizeTest.{ContextEcho, Echo}
  alias Filo.Socket

  @streams __MODULE__.Streams

  # Grants access iff the bearer token is "sekret-<open_arg>", so tests can prove the
  # callback sees the resolved open_arg (the host's shard id) and the request token.
  defp authorize(arg, token) do
    if token == "sekret-#{arg}" do
      :ok
    else
      {:error, %Filo.Error{message: "unauthorized", code: "AUTH_REQUIRED", status: 401}}
    end
  end

  # Returns `{:ok, context}` so Filo threads `context` to `executor.open/2` — the scope
  # decoded from the token (`:ro`/`:rw`), the shape a host like fathom uses to carry a
  # read-only credential to the connection without a process-dict side-channel.
  defp authorize_ctx(arg, token) do
    cond do
      token == "ro-#{arg}" -> {:ok, :ro}
      token == "rw-#{arg}" -> {:ok, :rw}
      true -> {:error, %Filo.Error{message: "unauthorized", code: "AUTH_REQUIRED", status: 401}}
    end
  end

  setup do
    start_supervised!({Filo.Streams, name: @streams})

    opts =
      Filo.Plug.init(
        executor: Echo,
        streams: @streams,
        key: Filo.Baton.new_key(),
        open_arg: fn _conn -> "db1" end,
        authorize: &authorize/2
      )

    %{opts: opts}
  end

  defp post(opts, path, body, headers) do
    Enum.reduce(
      headers,
      conn(:post, path, Jason.encode!(body))
      |> put_req_header("content-type", "application/json"),
      fn {k, v}, conn -> put_req_header(conn, k, v) end
    )
    |> Filo.Plug.call(opts)
  end

  defp execute_req, do: %{"type" => "execute", "stmt" => %{"sql" => "SELECT 1"}}
  defp pipeline_body, do: %{"baton" => nil, "requests" => [execute_req()]}

  # --- HTTP ---

  test "pipeline without a token is refused 401 and no stream opens", %{opts: opts} do
    conn = post(opts, "/v3/pipeline", pipeline_body(), [])

    assert conn.status == 401
    assert %{"code" => "AUTH_REQUIRED"} = Jason.decode!(conn.resp_body)
  end

  test "pipeline with a wrong-database token is refused 401", %{opts: opts} do
    conn =
      post(opts, "/v3/pipeline", pipeline_body(), [{"authorization", "Bearer sekret-db2"}])

    assert conn.status == 401
  end

  test "pipeline with the right token runs", %{opts: opts} do
    conn =
      post(opts, "/v3/pipeline", pipeline_body(), [{"authorization", "Bearer sekret-db1"}])

    assert conn.status == 200
    assert [%{"type" => "ok"}] = Jason.decode!(conn.resp_body)["results"]
  end

  test "bearer scheme match is case-insensitive", %{opts: opts} do
    conn =
      post(opts, "/v3/pipeline", pipeline_body(), [{"authorization", "bearer sekret-db1"}])

    assert conn.status == 200
  end

  test "a baton resumes an authorized stream without re-presenting the token", %{opts: opts} do
    conn =
      post(opts, "/v3/pipeline", pipeline_body(), [{"authorization", "Bearer sekret-db1"}])

    assert conn.status == 200
    baton = Jason.decode!(conn.resp_body)["baton"]

    conn = post(opts, "/v3/pipeline", %{"baton" => baton, "requests" => [execute_req()]}, [])
    assert conn.status == 200
  end

  test "cursor without a token is refused 401", %{opts: opts} do
    body = %{"baton" => nil, "batch" => %{"steps" => [%{"stmt" => %{"sql" => "SELECT 1"}}]}}
    conn = post(opts, "/v3/cursor", body, [])

    assert conn.status == 401
  end

  test "v1 execute is authorized per stateless request", %{opts: opts} do
    body = %{"stmt" => %{"sql" => "SELECT 1"}}

    assert post(opts, "/v1/execute", body, []).status == 401

    assert post(opts, "/v1/execute", body, [{"authorization", "Bearer sekret-db1"}]).status ==
             200
  end

  test "no :authorize configured keeps the pre-auth behavior (everything accepted)" do
    opts =
      Filo.Plug.init(
        executor: Echo,
        streams: @streams,
        key: Filo.Baton.new_key(),
        open_arg: "db1"
      )

    assert post(opts, "/v3/pipeline", pipeline_body(), []).status == 200
  end

  # --- WebSocket (Hrana hello) ---

  defp socket(opts) do
    {:ok, state} =
      Socket.init([executor: Echo, open_arg: "db1", authorize: &authorize/2] ++ opts)

    state
  end

  defp send_msg(state, msg), do: Socket.handle_in({Jason.encode!(msg), [opcode: :text]}, state)

  test "hello with a valid jwt is answered hello_ok" do
    assert {:push, {:text, json}, _state} =
             send_msg(socket([]), %{"type" => "hello", "jwt" => "sekret-db1"})

    assert %{"type" => "hello_ok"} = Jason.decode!(json)
  end

  test "hello with a bad jwt pushes a fatal hello_error and closes the socket" do
    assert {:stop, :normal, {1008, "unauthorized"}, [{:text, json}], _state} =
             send_msg(socket([]), %{"type" => "hello", "jwt" => "sekret-db2"})

    assert %{"type" => "hello_error", "error" => %{"code" => "AUTH_REQUIRED"}} =
             Jason.decode!(json)
  end

  test "hello without a jwt falls back to the upgrade request's bearer token" do
    assert {:push, {:text, json}, _state} =
             send_msg(socket(header_token: "sekret-db1"), %{"type" => "hello"})

    assert %{"type" => "hello_ok"} = Jason.decode!(json)
  end

  test "hello with neither jwt nor header token is refused" do
    assert {:stop, :normal, {1008, _}, [_hello_error], _state} =
             send_msg(socket([]), %{"type" => "hello"})
  end

  test "an unauthorized protobuf hello gets a protobuf-encoded hello_error" do
    {:ok, state} =
      Socket.init(
        executor: Echo,
        open_arg: "db1",
        authorize: &authorize/2,
        encoding: :protobuf
      )

    frame = IO.iodata_to_binary(Filo.Protobuf.Ws.encode_client_msg(%{"type" => "hello"}))

    assert {:stop, :normal, {1008, _}, [{:binary, bin}], _state} =
             Socket.handle_in({frame, [opcode: :binary]}, state)

    assert %{"type" => "hello_error", "error" => %{"code" => "AUTH_REQUIRED"}} =
             Filo.Protobuf.Ws.decode_server_msg(bin)
  end

  # --- authorize context threading to executor.open/2 (both transports) ---

  @ctx_streams __MODULE__.CtxStreams

  defp ctx_opts do
    Filo.Plug.init(
      executor: ContextEcho,
      streams: @ctx_streams,
      key: Filo.Baton.new_key(),
      open_arg: fn _conn -> "db1" end,
      authorize: &authorize_ctx/2
    )
  end

  test "authorize context threads to open/2 over the HTTP pipeline (stream opens in its own process)" do
    start_supervised!({Filo.Streams, name: @ctx_streams}, id: @ctx_streams)

    conn = post(ctx_opts(), "/v3/pipeline", pipeline_body(), [{"authorization", "Bearer ro-db1"}])

    assert conn.status == 200

    # The stream GenServer opened the connection in a DIFFERENT process than the plug's
    # authorize; the :ro scope still reached open/2 there (not via a process-dict side-channel
    # that a cross-process open would find empty). execute reflects the threaded context.
    assert [%{"type" => "ok", "response" => %{"result" => %{"rows" => [[cell]]}}}] =
             Jason.decode!(conn.resp_body)["results"]

    assert cell == %{"type" => "text", "value" => ":ro"}
  end

  test "authorize context threads to open/2 on the stateless v1 path" do
    start_supervised!({Filo.Streams, name: @ctx_streams}, id: @ctx_streams)

    conn =
      post(ctx_opts(), "/v1/execute", %{"stmt" => %{"sql" => "SELECT 1"}}, [
        {"authorization", "Bearer ro-db1"}
      ])

    assert conn.status == 200

    assert %{"result" => %{"rows" => [[%{"type" => "text", "value" => ":ro"}]]}} =
             Jason.decode!(conn.resp_body)
  end

  test "an executor without open/2 still works — the context is dropped (back-compat)" do
    start_supervised!({Filo.Streams, name: @ctx_streams}, id: @ctx_streams)

    # Echo implements only open/1; authorize returns {:ok, :ro}, so Filo falls back to open/1
    # and drops the context — a pre-open/2 executor keeps working unchanged.
    opts =
      Filo.Plug.init(
        executor: Echo,
        streams: @ctx_streams,
        key: Filo.Baton.new_key(),
        open_arg: fn _conn -> "db1" end,
        authorize: &authorize_ctx/2
      )

    conn = post(opts, "/v3/pipeline", pipeline_body(), [{"authorization", "Bearer ro-db1"}])

    assert conn.status == 200
    assert [%{"type" => "ok"}] = Jason.decode!(conn.resp_body)["results"]
  end

  test "the hello context threads to EVERY WebSocket stream, not just the first" do
    {:ok, state} =
      Socket.init(executor: ContextEcho, open_arg: "db1", authorize: &authorize_ctx/2)

    # hello carries the ro token; the socket captures :ro for the whole connection.
    {%{"type" => "hello_ok"}, state} =
      ws_pushed(send_msg(state, %{"type" => "hello", "jwt" => "ro-db1"}))

    assert state.open_context == :ro

    # First stream: opened with the :ro context.
    {%{"response" => %{"type" => "open_stream"}}, state} =
      ws_pushed(send_msg(state, req_msg(1, %{"type" => "open_stream", "stream_id" => 1})))

    {r1, state} =
      ws_pushed(
        send_msg(
          state,
          req_msg(2, %{"type" => "execute", "stream_id" => 1, "stmt" => %{"sql" => "SELECT 1"}})
        )
      )

    assert r1["response"]["result"]["rows"] == [[%{"type" => "text", "value" => ":ro"}]]

    # Second stream on the SAME hello-authorized connection. The OLD consuming take_scope left
    # this one defaulting to :rw (the ro→rw escalation). It must now open with :ro too.
    {%{"response" => %{"type" => "open_stream"}}, state} =
      ws_pushed(send_msg(state, req_msg(3, %{"type" => "open_stream", "stream_id" => 2})))

    {r2, _state} =
      ws_pushed(
        send_msg(
          state,
          req_msg(4, %{"type" => "execute", "stream_id" => 2, "stmt" => %{"sql" => "SELECT 1"}})
        )
      )

    assert r2["response"]["result"]["rows"] == [[%{"type" => "text", "value" => ":ro"}]]
  end

  defp req_msg(id, request), do: %{"type" => "request", "request_id" => id, "request" => request}
  defp ws_pushed({:push, {:text, json}, state}), do: {Jason.decode!(json), state}
end
