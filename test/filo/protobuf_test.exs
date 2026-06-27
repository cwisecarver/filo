defmodule Filo.ProtobufTest do
  use ExUnit.Case, async: true

  alias Filo.Protobuf
  alias Filo.{Batch, BatchResult, Describe, Error, Stmt, StmtResult, Value}

  defp enc(iodata), do: IO.iodata_to_binary(iodata)

  describe "Value" do
    test "round-trips every value type through the canonical map" do
      for native <- [nil, 42, -7, 3.5, "hello", {:blob, <<1, 2, 3>>}] do
        map = Value.encode(native)
        assert Protobuf.decode_value(enc(Protobuf.encode_value(map))) == map
        assert Value.decode(Protobuf.decode_value(enc(Protobuf.encode_value(map)))) == native
      end
    end

    test "golden wire bytes pin the encoding" do
      assert enc(Protobuf.encode_value(%{"type" => "null"})) == <<0x0A, 0x00>>
      assert enc(Protobuf.encode_value(%{"type" => "integer", "value" => "1"})) == <<0x10, 0x02>>

      assert enc(Protobuf.encode_value(%{"type" => "text", "value" => "hi"})) ==
               <<0x22, 0x02, ?h, ?i>>
    end

    test "integer is carried as a string, blob as no-pad base64" do
      assert Protobuf.decode_value(enc(Protobuf.encode_value(Value.encode(42)))) ==
               %{"type" => "integer", "value" => "42"}

      assert Protobuf.decode_value(enc(Protobuf.encode_value(Value.encode({:blob, "hi"})))) ==
               %{"type" => "blob", "base64" => "aGk"}
    end
  end

  describe "Error" do
    test "round-trips with and without a code" do
      for map <- [
            %{"message" => "boom", "code" => "SQLITE_X"},
            %{"message" => "boom", "code" => nil}
          ] do
        assert Protobuf.decode_error(enc(Protobuf.encode_error(map))) == map
      end
    end
  end

  describe "Stmt" do
    test "round-trips and stays compatible with Filo.Stmt.decode" do
      map = %{
        "sql" => "SELECT ?, :a",
        "args" => [Value.encode(1)],
        "named_args" => [%{"name" => ":a", "value" => Value.encode("x")}],
        "want_rows" => true
      }

      decoded = Protobuf.decode_stmt(enc(Protobuf.encode_stmt(map)))
      assert decoded == map

      stmt = Stmt.decode(decoded)
      assert stmt.sql == "SELECT ?, :a"
      assert stmt.args == [1]
      assert stmt.named_args == [{":a", "x"}]
      assert stmt.want_rows == true
    end

    test "carries sql_id and omits absent optionals" do
      map = %{"sql_id" => 7, "args" => [], "named_args" => []}
      assert Protobuf.decode_stmt(enc(Protobuf.encode_stmt(map))) == map
    end
  end

  describe "StmtResult" do
    test "round-trips the proto-representable fields" do
      map =
        StmtResult.encode(%StmtResult{
          cols: ["v"],
          rows: [[1], ["x"]],
          affected_row_count: 2,
          last_insert_rowid: 7
        })

      decoded = Protobuf.decode_stmt_result(enc(Protobuf.encode_stmt_result(map)))

      assert decoded["cols"] == [%{"name" => "v", "decltype" => nil}]
      assert decoded["affected_row_count"] == 2
      assert decoded["last_insert_rowid"] == "7"

      assert decoded["rows"] == [
               [%{"type" => "integer", "value" => "1"}],
               [%{"type" => "text", "value" => "x"}]
             ]
    end
  end

  describe "Batch" do
    test "round-trips steps with a recursive condition, compatible with Filo.Batch.decode" do
      map = %{
        "steps" => [
          %{"stmt" => %{"sql" => "BEGIN", "args" => [], "named_args" => []}},
          %{
            "condition" => %{
              "type" => "and",
              "conds" => [
                %{"type" => "ok", "step" => 0},
                %{"type" => "not", "cond" => %{"type" => "is_autocommit"}}
              ]
            },
            "stmt" => %{"sql" => "COMMIT", "args" => [], "named_args" => []}
          }
        ]
      }

      decoded = Protobuf.decode_batch(enc(Protobuf.encode_batch(map)))
      assert decoded == map

      batch = Batch.decode(decoded)
      assert length(batch.steps) == 2
      assert Enum.at(batch.steps, 1).condition == {:and, [{:ok, 0}, {:not, :is_autocommit}]}
    end
  end

  describe "BatchResult" do
    test "round-trips positional results/errors (skipped steps stay nil)" do
      map =
        BatchResult.encode(%BatchResult{
          step_results: [%StmtResult{affected_row_count: 1}, nil],
          step_errors: [nil, %Error{message: "boom", code: "X"}]
        })

      decoded = Protobuf.decode_batch_result(enc(Protobuf.encode_batch_result(map)))

      assert [%{"affected_row_count" => 1}, nil] = decoded["step_results"]
      assert [nil, %{"message" => "boom", "code" => "X"}] = decoded["step_errors"]
    end
  end

  describe "DescribeResult" do
    test "round-trips params, cols, and flags" do
      map =
        Describe.encode(%Describe{
          params: ["?", nil],
          cols: [%{name: "a", decltype: "INT"}],
          is_explain: true,
          is_readonly: false
        })

      decoded = Protobuf.decode_describe_result(enc(Protobuf.encode_describe_result(map)))
      assert decoded["params"] == [%{"name" => "?"}, %{"name" => nil}]
      assert decoded["cols"] == [%{"name" => "a", "decltype" => "INT"}]
      assert decoded["is_explain"] == true
      assert decoded["is_readonly"] == false
    end
  end

  describe "CursorEntry" do
    test "round-trips each entry kind" do
      entries = [
        %{"type" => "step_begin", "step" => 0, "cols" => [%{"name" => "v", "decltype" => nil}]},
        %{"type" => "row", "row" => [Value.encode(1)]},
        %{"type" => "step_end", "affected_row_count" => 3, "last_insert_rowid" => "9"},
        %{"type" => "step_error", "step" => 1, "error" => %{"message" => "boom", "code" => nil}}
      ]

      for entry <- entries do
        assert Protobuf.decode_cursor_entry(enc(Protobuf.encode_cursor_entry(entry))) == entry
      end
    end
  end
end
