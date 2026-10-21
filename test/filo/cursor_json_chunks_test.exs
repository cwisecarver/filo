defmodule Filo.CursorJsonChunksTest do
  # Pins fathom expert review 2026-10-21 #20 (panel 2 #3): the JSON cursor shipped the native
  # BatchResult across processes and encoded each row through its own map + Jason pass, ignoring
  # StmtResult.cols_encoded. It now encodes in the stream process via Cursor.json_chunks/2.
  # The wire format must not move: the golden below is the previous encoding, verbatim.
  use ExUnit.Case, async: true

  alias Filo.{BatchResult, Cursor, Error, StmtResult}

  # The pre-change body: Cursor.entries(rows: :json), each item Jason-encoded plus "\n".
  defp golden(result) do
    result
    |> Cursor.entries(rows: :json)
    |> Enum.map(fn item -> [Jason.encode_to_iodata!(item), "\n"] end)
    |> IO.iodata_to_binary()
  end

  @cols [
    %{name: "i", decltype: "INTEGER"},
    %{name: "f", decltype: "REAL"},
    %{name: "t", decltype: "TEXT"},
    %{name: "b", decltype: "BLOB"},
    %{name: "n", decltype: nil}
  ]

  defp mixed_rows do
    [
      [1, 1.5, "plain", <<0, 1, 2, 255>>, nil],
      [
        9_223_372_036_854_775_807,
        -0.0,
        "quote\" slash\\ nl\n tab\t ünïcode 日本 \u2028",
        <<>>,
        nil
      ],
      [-9_223_372_036_854_775_808, 1.0e300, "", :binary.copy(<<7>>, 300), nil],
      [0, 123_456_789.123456789, "emoji 🙂", <<"abc">>, nil],
      [4_503_599_627_370_497, 3.0e-10, "x", <<255, 254>>, nil]
    ]
  end

  defp mixed_result(extra \\ []) do
    struct!(
      %StmtResult{
        cols: @cols,
        rows: mixed_rows(),
        affected_row_count: 5,
        last_insert_rowid: 9_223_372_036_854_775_807
      },
      extra
    )
  end

  defp batch(results, errors), do: %BatchResult{step_results: results, step_errors: errors}

  defp chunks_bin(result, limit), do: result |> Cursor.json_chunks(limit) |> IO.iodata_to_binary()

  test "mixed types (blob, float, null, i64 extremes, escapes) are byte-identical to the old body" do
    br = batch([mixed_result()], [nil])
    assert chunks_bin(br, 32 * 1024) == golden(br)
  end

  test "multi-step: executed, failed, skipped, empty and bare-name-column steps" do
    br =
      batch(
        [
          mixed_result(),
          nil,
          nil,
          %StmtResult{cols: ["only"], rows: [], affected_row_count: 3, last_insert_rowid: nil},
          %StmtResult{cols: [], rows: [[]]}
        ],
        [nil, %Error{message: "boom", code: "SQLITE_ERROR"}, nil, nil, nil]
      )

    assert chunks_bin(br, 32 * 1024) == golden(br)
    assert chunks_bin(batch([], []), 32 * 1024) == ""
  end

  test "any chunk size gives the same bytes, and a line is never split" do
    br = batch([mixed_result(), mixed_result()], [nil, nil])
    expected = golden(br)

    for limit <- [1, 17, 64, 200, 1_000, 1_000_000] do
      chunks = Cursor.json_chunks(br, limit)
      assert IO.iodata_to_binary(chunks) == expected
      assert Enum.all?(chunks, &is_binary/1)
      # chunks end on a line boundary
      assert Enum.all?(chunks, &String.ends_with?(&1, "\n"))
    end

    assert length(Cursor.json_chunks(br, 1)) > 10
  end

  # Symptom: step_begin re-encoded cols per execution and ignored the host's precomputed
  # cols_encoded. A sentinel fragment (valid JSON, different from cols) proves it is spliced.
  test "step_begin splices cols_encoded when the host supplied it" do
    sentinel = %{
      json: Jason.Fragment.new(~s([{"name":"SENTINEL","decltype":null}])),
      protobuf: ""
    }

    br = batch([mixed_result(cols_encoded: sentinel)], [nil])

    [begin | _] = chunks_bin(br, 32 * 1024) |> String.split("\n", trim: true)
    assert Jason.decode!(begin)["cols"] == [%{"name" => "SENTINEL", "decltype" => nil}]

    [entry | _] = Cursor.entries(br, rows: :json)
    assert Jason.encode!(entry["cols"]) =~ "SENTINEL"
  end

  test "real cols_encoded is byte-identical to the unencoded columns" do
    plain = batch([mixed_result()], [nil])
    cached = batch([mixed_result(cols_encoded: StmtResult.encode_cols(@cols))], [nil])
    assert chunks_bin(cached, 32 * 1024) == golden(plain)

    assert IO.iodata_to_binary(Enum.map(Cursor.entries(cached, rows: :json), &Jason.encode!/1)) ==
             IO.iodata_to_binary(Enum.map(Cursor.entries(plain, rows: :json), &Jason.encode!/1))
  end

  test ":maps mode (protobuf cursor) still builds plain column maps even with cols_encoded" do
    br = batch([mixed_result(cols_encoded: StmtResult.encode_cols(@cols))], [nil])
    [%{"cols" => cols} | _] = Cursor.entries(br)
    assert [%{"name" => "i", "decltype" => "INTEGER"} | _] = cols
  end
end
