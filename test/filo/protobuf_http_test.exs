defmodule Filo.ProtobufHttpEcho do
  @moduledoc false
  @behaviour Filo.Executor

  @impl true
  def open(_arg), do: {:ok, :conn}

  @impl true
  def execute(:conn, %Filo.Stmt{sql: "BOOM"}),
    do: {:error, %Filo.Error{message: "boom", code: "SQLITE_ERROR"}}

  @impl true
  def execute(:conn, %Filo.Stmt{args: args}),
    do: {:ok, %Filo.StmtResult{cols: ["n"], rows: [args], affected_row_count: 1}}

  @impl true
  def autocommit?(:conn), do: true

  @impl true
  def close(:conn), do: :ok
end

defmodule Filo.ProtobufHttpTest do
  use ExUnit.Case, async: true

  alias Filo.Protobuf
  alias Filo.Protobuf.{Http, Wire}

  defp enc(iodata), do: IO.iodata_to_binary(iodata)

  describe "envelope round-trips" do
    test "pipeline request and response" do
      req = %{
        "baton" => "abc",
        "requests" => [
          %{
            "type" => "execute",
            "stmt" => %{"sql" => "SELECT 1", "args" => [], "named_args" => []}
          },
          %{"type" => "close"}
        ]
      }

      assert Http.decode_pipeline_req(enc(Http.encode_pipeline_req(req))) == req

      resp = %{
        "baton" => "def",
        "base_url" => "http://x",
        "results" => [
          %{"type" => "ok", "response" => %{"type" => "get_autocommit", "is_autocommit" => true}},
          %{"type" => "error", "error" => %{"message" => "boom", "code" => "X"}}
        ]
      }

      assert Http.decode_pipeline_resp(enc(Http.encode_pipeline_resp(resp))) == resp
    end

    test "every stream-request variant round-trips" do
      requests = [
        %{"type" => "close"},
        %{
          "type" => "execute",
          "stmt" => %{"sql" => "SELECT ?", "args" => [], "named_args" => []}
        },
        %{"type" => "batch", "batch" => %{"steps" => []}},
        %{"type" => "sequence", "sql" => "SELECT 1"},
        %{"type" => "describe", "sql_id" => 3},
        %{"type" => "store_sql", "sql_id" => 1, "sql" => "SELECT 1"},
        %{"type" => "close_sql", "sql_id" => 1},
        %{"type" => "get_autocommit"}
      ]

      for req <- requests do
        assert Http.decode_stream_request(enc(Http.encode_stream_request(req))) == req
      end
    end

    test "cursor request and response bodies" do
      req = %{"baton" => "b", "batch" => %{"steps" => []}}
      assert Http.decode_cursor_req(enc(Http.encode_cursor_req(req))) == req

      resp = %{"baton" => "b2", "base_url" => "http://x"}
      assert Http.decode_cursor_resp(enc(Http.encode_cursor_resp(resp))) == resp
    end

    test "split_delimited inverts repeated delimit" do
      framed = enc([Wire.delimit("aa"), Wire.delimit("bbb")])
      assert Wire.split_delimited(framed) == ["aa", "bbb"]
    end
  end

  describe "plug routes" do
    @streams __MODULE__.Streams

    setup do
      start_supervised!({Filo.Streams, name: @streams})

      opts =
        Filo.Plug.init(
          executor: Filo.ProtobufHttpEcho,
          streams: @streams,
          key: Filo.Baton.new_key(),
          base_url: "http://localhost"
        )

      %{opts: opts}
    end

    defp post_pb(opts, path, body) do
      Plug.Test.conn(:post, path, enc(body))
      |> Plug.Conn.put_req_header("content-type", "application/x-protobuf")
      |> Filo.Plug.call(opts)
    end

    test "GET /v3-protobuf reports support", %{opts: opts} do
      conn = Filo.Plug.call(Plug.Test.conn(:get, "/v3-protobuf"), opts)
      assert conn.status == 200
    end

    test "POST /v3-protobuf/pipeline executes and returns a protobuf response", %{opts: opts} do
      body =
        Http.encode_pipeline_req(%{
          "baton" => nil,
          "requests" => [
            %{
              "type" => "execute",
              "stmt" => %{
                "sql" => "SELECT ?",
                "args" => [Filo.Value.encode(42)],
                "named_args" => []
              }
            }
          ]
        })

      conn = post_pb(opts, "/v3-protobuf/pipeline", body)

      assert conn.status == 200
      assert ["application/x-protobuf"] = Plug.Conn.get_resp_header(conn, "content-type")

      resp = Http.decode_pipeline_resp(conn.resp_body)
      assert is_binary(resp["baton"])

      assert [%{"type" => "ok", "response" => %{"type" => "execute", "result" => result}}] =
               resp["results"]

      assert result["rows"] == [[%{"type" => "integer", "value" => "42"}]]
    end

    test "a baton resumes the stream across two protobuf pipelines", %{opts: opts} do
      first =
        post_pb(
          opts,
          "/v3-protobuf/pipeline",
          Http.encode_pipeline_req(%{"requests" => [exec()]})
        )

      baton = Http.decode_pipeline_resp(first.resp_body)["baton"]
      assert is_binary(baton)

      second =
        post_pb(
          opts,
          "/v3-protobuf/pipeline",
          Http.encode_pipeline_req(%{"baton" => baton, "requests" => [exec()]})
        )

      assert second.status == 200
      assert [%{"type" => "ok"}] = Http.decode_pipeline_resp(second.resp_body)["results"]
    end

    test "POST /v3-protobuf/cursor streams length-delimited frames", %{opts: opts} do
      body =
        Http.encode_cursor_req(%{
          "baton" => nil,
          "batch" => %{
            "steps" => [%{"stmt" => %{"sql" => "SELECT 1", "args" => [], "named_args" => []}}]
          }
        })

      conn = post_pb(opts, "/v3-protobuf/cursor", body)
      assert conn.status == 200

      [head | entries] = Wire.split_delimited(conn.resp_body)
      assert is_binary(Http.decode_cursor_resp(head)["baton"])

      kinds = Enum.map(entries, fn e -> Protobuf.decode_cursor_entry(e)["type"] end)
      assert "step_begin" in kinds
      assert "step_end" in kinds
    end

    test "an invalid baton is rejected with a protobuf error", %{opts: opts} do
      body = Http.encode_pipeline_req(%{"baton" => "not-a-real-baton", "requests" => [exec()]})
      conn = post_pb(opts, "/v3-protobuf/pipeline", body)

      assert conn.status == 400
      assert Protobuf.decode_error(conn.resp_body)["code"] == "BATON_INVALID"
    end

    defp exec,
      do: %{
        "type" => "execute",
        "stmt" => %{"sql" => "SELECT 1", "args" => [], "named_args" => []}
      }
  end
end
