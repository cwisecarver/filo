defmodule Filo.ProtobufWsEcho do
  @moduledoc false
  @behaviour Filo.Executor

  @impl true
  def open(_arg), do: {:ok, :conn}

  @impl true
  def execute(:conn, %Filo.Stmt{}),
    do: {:ok, %Filo.StmtResult{cols: ["n"], rows: [[1]], affected_row_count: 0}}

  @impl true
  def autocommit?(:conn), do: true

  @impl true
  def close(:conn), do: :ok
end

defmodule Filo.ProtobufWsTest do
  use ExUnit.Case, async: true

  alias Filo.Socket
  alias Filo.Protobuf.Ws
  alias Filo.ProtobufWsEcho, as: Echo

  defp enc(iodata), do: IO.iodata_to_binary(iodata)
  defp req(id, request), do: %{"type" => "request", "request_id" => id, "request" => request}

  describe "ClientMsg round-trips" do
    test "every request variant" do
      requests = [
        %{"type" => "open_stream", "stream_id" => 1},
        %{"type" => "close_stream", "stream_id" => 1},
        %{
          "type" => "execute",
          "stream_id" => 1,
          "stmt" => %{"sql" => "SELECT 1", "args" => [], "named_args" => []}
        },
        %{"type" => "batch", "stream_id" => 1, "batch" => %{"steps" => []}},
        %{
          "type" => "open_cursor",
          "stream_id" => 1,
          "cursor_id" => 2,
          "batch" => %{"steps" => []}
        },
        %{"type" => "close_cursor", "cursor_id" => 2},
        %{"type" => "fetch_cursor", "cursor_id" => 2, "max_count" => 10},
        %{"type" => "sequence", "stream_id" => 1, "sql" => "SELECT 1"},
        %{"type" => "describe", "stream_id" => 1, "sql_id" => 3},
        %{"type" => "store_sql", "sql_id" => 3, "sql" => "SELECT 1"},
        %{"type" => "close_sql", "sql_id" => 3},
        %{"type" => "get_autocommit", "stream_id" => 1}
      ]

      for request <- requests do
        msg = req(7, request)
        assert Ws.decode_client_msg(enc(Ws.encode_client_msg(msg))) == msg
      end
    end

    test "hello" do
      assert Ws.decode_client_msg(enc(Ws.encode_client_msg(%{"type" => "hello"}))) == %{
               "type" => "hello"
             }
    end
  end

  describe "ServerMsg round-trips" do
    test "hello_ok, hello_error, response_error" do
      for msg <- [
            %{"type" => "hello_ok"},
            %{"type" => "hello_error", "error" => %{"message" => "x", "code" => nil}},
            %{
              "type" => "response_error",
              "request_id" => 7,
              "error" => %{"message" => "x", "code" => "C"}
            }
          ] do
        assert Ws.decode_server_msg(enc(Ws.encode_server_msg(msg))) == msg
      end
    end

    test "response_ok for the simple responses" do
      responses = [
        %{"type" => "open_stream"},
        %{"type" => "close_stream"},
        %{"type" => "open_cursor"},
        %{"type" => "close_cursor"},
        %{"type" => "sequence"},
        %{"type" => "store_sql"},
        %{"type" => "close_sql"},
        %{"type" => "get_autocommit", "is_autocommit" => true},
        %{"type" => "get_autocommit", "is_autocommit" => false},
        %{
          "type" => "fetch_cursor",
          "entries" => [%{"type" => "row", "row" => [Filo.Value.encode(1)]}],
          "done" => true
        }
      ]

      for response <- responses do
        msg = %{"type" => "response_ok", "request_id" => 7, "response" => response}
        assert Ws.decode_server_msg(enc(Ws.encode_server_msg(msg))) == msg
      end
    end
  end

  describe "socket over binary frames" do
    defp open_pb do
      {:ok, state} = Socket.init(executor: Echo, open_arg: nil, encoding: :protobuf)
      state
    end

    defp pb_send(state, msg),
      do: Socket.handle_in({enc(Ws.encode_client_msg(msg)), [opcode: :binary]}, state)

    defp pb_pushed({:push, {:binary, data}, state}), do: {Ws.decode_server_msg(data), state}

    defp hello(state) do
      {%{"type" => "hello_ok"}, state} = pb_pushed(pb_send(state, %{"type" => "hello"}))
      state
    end

    test "hello is answered with a binary hello_ok" do
      assert {%{"type" => "hello_ok"}, _} = pb_pushed(pb_send(open_pb(), %{"type" => "hello"}))
    end

    test "open_stream then execute returns a protobuf response_ok" do
      state = open_pb() |> hello()

      {%{"type" => "response_ok", "response" => %{"type" => "open_stream"}}, state} =
        pb_pushed(pb_send(state, req(1, %{"type" => "open_stream", "stream_id" => 1})))

      {response, _state} =
        pb_pushed(
          pb_send(
            state,
            req(2, %{
              "type" => "execute",
              "stream_id" => 1,
              "stmt" => %{"sql" => "SELECT 1", "args" => [], "named_args" => []}
            })
          )
        )

      assert %{
               "type" => "response_ok",
               "request_id" => 2,
               "response" => %{"type" => "execute", "result" => result}
             } = response

      assert result["rows"] == [[%{"type" => "integer", "value" => "1"}]]
    end

    test "a text frame under the protobuf encoding is a protocol violation" do
      assert {:stop, :normal, _} =
               Socket.handle_in({Jason.encode!(%{"type" => "hello"}), [opcode: :text]}, open_pb())
    end
  end
end
