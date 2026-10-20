defmodule Filo.JsonEncodePerfTest do
  use ExUnit.Case, async: true
  import Bitwise

  # fathom expert review 2026-10-20 #31: encode_json's float cells call
  # :erlang.float_to_binary(f, [:short]) directly (what Jason itself does on OTP >= 25) and
  # the same-process transports skip the rows flatten (rows: :json_iodata). Both must stay
  # BYTE-IDENTICAL to the Jason path / the flattened :json mode.

  alias Filo.{Cursor, BatchResult, StmtResult, Value}

  @floats [
    0.0,
    -0.0,
    1.0,
    -1.0,
    0.1,
    1.0e20,
    1.0e21,
    -1.0e22,
    1.0e-7,
    123_456_789.123456789,
    5.0e-324,
    2.2250738585072014e-308,
    1.7976931348623157e308,
    -1.7976931348623157e308,
    3.141592653589793,
    1.0e15,
    1.0e16,
    9_007_199_254_740_993.0
  ]

  test "float cells are byte-identical to Jason's encoding" do
    # NaN and +/-inf are not representable as BEAM floats, so there is no case for them.
    random = for _ <- 1..2_000, do: :rand.uniform() * :math.pow(10, :rand.uniform(600) - 300)
    # Random bit patterns cover subnormals and every exponent.
    bits =
      Enum.flat_map(1..2_000, fn _ ->
        # Exponent 0x7FF patterns are NaN/inf, which a BEAM float cannot hold: skip them.
        case <<:rand.uniform(1 <<< 63) - 1::64>> do
          <<f::float-64>> -> [f]
          _ -> []
        end
      end)

    for f <- @floats ++ random ++ bits do
      assert IO.iodata_to_binary(Value.encode_json(f)) ==
               ~s({"type":"float","value":#{Jason.encode!(f)}})
    end
  end

  defp result do
    %StmtResult{
      cols: ["a", "b", "c", "d", "e"],
      rows: [[1, 2.5, "t\"x\n", {:blob, <<0, 255>>}, nil], [2, -0.0, <<255, 254>>, "é", 1.0e20]],
      affected_row_count: 0,
      last_insert_rowid: nil,
      rows_read: 2,
      rows_written: 0
    }
  end

  test ":json_iodata produces the same wire bytes as :json" do
    a = result() |> StmtResult.encode(rows: :json) |> Jason.encode_to_iodata!()
    b = result() |> StmtResult.encode(rows: :json_iodata) |> Jason.encode_to_iodata!()
    assert IO.iodata_to_binary(a) == IO.iodata_to_binary(b)

    empty = %{result() | rows: []}

    assert IO.iodata_to_binary(Jason.encode_to_iodata!(StmtResult.encode(empty, rows: :json))) ==
             IO.iodata_to_binary(
               Jason.encode_to_iodata!(StmtResult.encode(empty, rows: :json_iodata))
             )
  end

  test ":json_iodata splices cols_encoded like :json, and cursor rows match" do
    r = %{result() | cols_encoded: StmtResult.encode_cols(result().cols)}

    assert IO.iodata_to_binary(Jason.encode_to_iodata!(StmtResult.encode(r, rows: :json))) ==
             IO.iodata_to_binary(
               Jason.encode_to_iodata!(StmtResult.encode(r, rows: :json_iodata))
             )

    br = %BatchResult{step_results: [result()], step_errors: [nil]}

    enc = fn mode ->
      br |> Cursor.entries(rows: mode) |> Jason.encode_to_iodata!() |> IO.iodata_to_binary()
    end

    assert enc.(:json) == enc.(:json_iodata)
  end
end
