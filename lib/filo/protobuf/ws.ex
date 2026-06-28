defmodule Filo.Protobuf.Ws do
  @moduledoc """
  Protobuf codec for the Hrana 3 *over WebSocket* messages: `ClientMsg`
  (hello / request) and `ServerMsg` (hello_ok / hello_error / response_ok /
  response_error), plus the `RequestMsg` / `ResponseOkMsg` request- and
  response-oneofs. Maps to and from the same message maps `Filo.Socket` already
  dispatches on, so the socket only swaps text-JSON framing for binary-protobuf
  framing. Shared structures come from `Filo.Protobuf`.
  """
  alias Filo.Protobuf
  alias Filo.Protobuf.Wire

  ## --- ClientMsg (server decodes; encode is for tests/clients) ---

  @spec decode_client_msg(binary()) :: map() | nil
  def decode_client_msg(bin) do
    bin
    |> Wire.decode_fields()
    |> Enum.find_value(fn
      {1, {:len, b}} -> decode_hello(b)
      {2, {:len, b}} -> decode_request_msg(b)
      _ -> nil
    end)
  end

  @spec encode_client_msg(map()) :: iodata()
  def encode_client_msg(%{"type" => "hello"} = m),
    do: Wire.field_len(1, Protobuf.string_field(1, Map.get(m, "jwt")))

  def encode_client_msg(%{"type" => "request", "request_id" => id, "request" => request}),
    do: Wire.field_len(2, [Wire.field_int(1, id), encode_request(request)])

  defp decode_hello(b) do
    Protobuf.put_if(%{"type" => "hello"}, "jwt", str(b, 1))
  end

  defp decode_request_msg(b) do
    fields = Wire.decode_fields(b)
    request = Enum.find_value(fields, fn {f, v} -> decode_request_field(f, v) end)

    %{
      "type" => "request",
      "request_id" => Protobuf.take_varint(fields, 1, 0),
      "request" => request
    }
  end

  defp decode_request_field(2, {:len, b}),
    do: %{"type" => "open_stream", "stream_id" => req_int(b, 1)}

  defp decode_request_field(3, {:len, b}),
    do: %{"type" => "close_stream", "stream_id" => req_int(b, 1)}

  defp decode_request_field(4, {:len, b}),
    do: %{
      "type" => "execute",
      "stream_id" => req_int(b, 1),
      "stmt" => Protobuf.decode_stmt(sub(b, 2))
    }

  defp decode_request_field(5, {:len, b}),
    do: %{
      "type" => "batch",
      "stream_id" => req_int(b, 1),
      "batch" => Protobuf.decode_batch(sub(b, 2))
    }

  defp decode_request_field(6, {:len, b}) do
    %{
      "type" => "open_cursor",
      "stream_id" => req_int(b, 1),
      "cursor_id" => req_int(b, 2),
      "batch" => Protobuf.decode_batch(sub(b, 3))
    }
  end

  defp decode_request_field(7, {:len, b}),
    do: %{"type" => "close_cursor", "cursor_id" => req_int(b, 1)}

  defp decode_request_field(8, {:len, b}),
    do: %{"type" => "fetch_cursor", "cursor_id" => req_int(b, 1), "max_count" => req_int(b, 2)}

  defp decode_request_field(9, {:len, b}),
    do: sql_ref(%{"type" => "sequence", "stream_id" => req_int(b, 1)}, b)

  defp decode_request_field(10, {:len, b}),
    do: sql_ref(%{"type" => "describe", "stream_id" => req_int(b, 1)}, b)

  defp decode_request_field(11, {:len, b}),
    do: %{"type" => "store_sql", "sql_id" => req_int(b, 1), "sql" => str(b, 2) || ""}

  defp decode_request_field(12, {:len, b}),
    do: %{"type" => "close_sql", "sql_id" => req_int(b, 1)}

  defp decode_request_field(13, {:len, b}),
    do: %{"type" => "get_autocommit", "stream_id" => req_int(b, 1)}

  defp decode_request_field(_f, _v), do: nil

  # sequence/describe carry stream_id (1) plus optional sql (2) / sql_id (3).
  defp sql_ref(base, b) do
    base
    |> Protobuf.put_if("sql", str(b, 2))
    |> Protobuf.put_if("sql_id", opt_int(b, 3))
  end

  defp encode_request(%{"type" => "open_stream"} = r),
    do: Wire.field_len(2, Wire.field_int(1, r["stream_id"]))

  defp encode_request(%{"type" => "close_stream"} = r),
    do: Wire.field_len(3, Wire.field_int(1, r["stream_id"]))

  defp encode_request(%{"type" => "execute"} = r),
    do:
      Wire.field_len(4, [
        Wire.field_int(1, r["stream_id"]),
        Wire.field_len(2, Protobuf.encode_stmt(r["stmt"]))
      ])

  defp encode_request(%{"type" => "batch"} = r),
    do:
      Wire.field_len(5, [
        Wire.field_int(1, r["stream_id"]),
        Wire.field_len(2, Protobuf.encode_batch(r["batch"]))
      ])

  defp encode_request(%{"type" => "open_cursor"} = r) do
    Wire.field_len(6, [
      Wire.field_int(1, r["stream_id"]),
      Wire.field_int(2, r["cursor_id"]),
      Wire.field_len(3, Protobuf.encode_batch(r["batch"]))
    ])
  end

  defp encode_request(%{"type" => "close_cursor"} = r),
    do: Wire.field_len(7, Wire.field_int(1, r["cursor_id"]))

  defp encode_request(%{"type" => "fetch_cursor"} = r),
    do:
      Wire.field_len(8, [
        Wire.field_int(1, r["cursor_id"]),
        Wire.field_varint(2, Map.get(r, "max_count", 0))
      ])

  defp encode_request(%{"type" => "sequence"} = r),
    do: Wire.field_len(9, sql_ref_body(r["stream_id"], r))

  defp encode_request(%{"type" => "describe"} = r),
    do: Wire.field_len(10, sql_ref_body(r["stream_id"], r))

  defp encode_request(%{"type" => "store_sql"} = r),
    do: Wire.field_len(11, [Wire.field_int(1, r["sql_id"]), Protobuf.string_field(2, r["sql"])])

  defp encode_request(%{"type" => "close_sql"} = r),
    do: Wire.field_len(12, Wire.field_int(1, r["sql_id"]))

  defp encode_request(%{"type" => "get_autocommit"} = r),
    do: Wire.field_len(13, Wire.field_int(1, r["stream_id"]))

  defp sql_ref_body(stream_id, r) do
    [
      Wire.field_int(1, stream_id),
      Protobuf.string_field(2, Map.get(r, "sql")),
      opt_int_field(3, Map.get(r, "sql_id"))
    ]
  end

  ## --- ServerMsg (server encodes; decode is for tests/clients) ---

  @spec encode_server_msg(map()) :: iodata()
  def encode_server_msg(%{"type" => "hello_ok"}), do: Wire.field_len(1, "")

  def encode_server_msg(%{"type" => "hello_error", "error" => error}),
    do: Wire.field_len(2, Wire.field_len(1, Protobuf.encode_error(error)))

  def encode_server_msg(%{"type" => "response_ok", "request_id" => id, "response" => response}),
    do: Wire.field_len(3, [Wire.field_int(1, id), encode_response(response)])

  def encode_server_msg(%{"type" => "response_error", "request_id" => id, "error" => error}),
    do:
      Wire.field_len(4, [Wire.field_int(1, id), Wire.field_len(2, Protobuf.encode_error(error))])

  @spec decode_server_msg(binary()) :: map() | nil
  def decode_server_msg(bin) do
    bin
    |> Wire.decode_fields()
    |> Enum.find_value(fn
      {1, {:len, _}} ->
        %{"type" => "hello_ok"}

      {2, {:len, b}} ->
        %{"type" => "hello_error", "error" => Protobuf.decode_error(sub(b, 1))}

      {3, {:len, b}} ->
        decode_response_ok(b)

      {4, {:len, b}} ->
        %{
          "type" => "response_error",
          "request_id" => req_int(b, 1),
          "error" => Protobuf.decode_error(sub(b, 2))
        }

      _ ->
        nil
    end)
  end

  defp decode_response_ok(b) do
    fields = Wire.decode_fields(b)
    response = Enum.find_value(fields, fn {f, v} -> decode_response_field(f, v) end)

    %{
      "type" => "response_ok",
      "request_id" => Protobuf.take_varint(fields, 1, 0),
      "response" => response
    }
  end

  defp encode_response(%{"type" => "open_stream"}), do: Wire.field_len(2, "")
  defp encode_response(%{"type" => "close_stream"}), do: Wire.field_len(3, "")

  defp encode_response(%{"type" => "execute", "result" => r}),
    do: Wire.field_len(4, Wire.field_len(1, Protobuf.encode_stmt_result(r)))

  defp encode_response(%{"type" => "batch", "result" => r}),
    do: Wire.field_len(5, Wire.field_len(1, Protobuf.encode_batch_result(r)))

  defp encode_response(%{"type" => "open_cursor"}), do: Wire.field_len(6, "")
  defp encode_response(%{"type" => "close_cursor"}), do: Wire.field_len(7, "")

  defp encode_response(%{"type" => "fetch_cursor", "entries" => entries} = r) do
    body = [
      for(e <- entries, do: Wire.field_len(1, Protobuf.encode_cursor_entry(e))),
      bool_opt(2, Map.get(r, "done"))
    ]

    Wire.field_len(8, body)
  end

  defp encode_response(%{"type" => "sequence"}), do: Wire.field_len(9, "")

  defp encode_response(%{"type" => "describe", "result" => r}),
    do: Wire.field_len(10, Wire.field_len(1, Protobuf.encode_describe_result(r)))

  defp encode_response(%{"type" => "store_sql"}), do: Wire.field_len(11, "")
  defp encode_response(%{"type" => "close_sql"}), do: Wire.field_len(12, "")

  defp encode_response(%{"type" => "get_autocommit"} = r),
    do: Wire.field_len(13, bool_opt(1, Map.get(r, "is_autocommit")))

  defp decode_response_field(2, {:len, _}), do: %{"type" => "open_stream"}
  defp decode_response_field(3, {:len, _}), do: %{"type" => "close_stream"}

  defp decode_response_field(4, {:len, b}),
    do: %{"type" => "execute", "result" => Protobuf.decode_stmt_result(sub(b, 1))}

  defp decode_response_field(5, {:len, b}),
    do: %{"type" => "batch", "result" => Protobuf.decode_batch_result(sub(b, 1))}

  defp decode_response_field(6, {:len, _}), do: %{"type" => "open_cursor"}
  defp decode_response_field(7, {:len, _}), do: %{"type" => "close_cursor"}

  defp decode_response_field(8, {:len, b}) do
    fields = Wire.decode_fields(b)

    %{
      "type" => "fetch_cursor",
      "entries" => for(e <- Protobuf.collect_len(fields, 1), do: Protobuf.decode_cursor_entry(e)),
      "done" => Protobuf.take_varint(fields, 2, 0) != 0
    }
  end

  defp decode_response_field(9, {:len, _}), do: %{"type" => "sequence"}

  defp decode_response_field(10, {:len, b}),
    do: %{"type" => "describe", "result" => Protobuf.decode_describe_result(sub(b, 1))}

  defp decode_response_field(11, {:len, _}), do: %{"type" => "store_sql"}
  defp decode_response_field(12, {:len, _}), do: %{"type" => "close_sql"}

  defp decode_response_field(13, {:len, b}),
    do: %{"type" => "get_autocommit", "is_autocommit" => bool(b, 1)}

  defp decode_response_field(_f, _v), do: nil

  ## --- helpers ---

  defp req_int(b, field), do: Protobuf.take_varint(Wire.decode_fields(b), field, 0)
  defp opt_int(b, field), do: Protobuf.take_varint(Wire.decode_fields(b), field, nil)
  defp str(b, field), do: Protobuf.take_string(Wire.decode_fields(b), field, nil)
  defp sub(b, field), do: Protobuf.take_len(Wire.decode_fields(b), field) || ""
  defp bool(b, field), do: Protobuf.take_varint(Wire.decode_fields(b), field, 0) != 0

  defp opt_int_field(_field, nil), do: []
  defp opt_int_field(field, v) when is_integer(v), do: Wire.field_int(field, v)

  defp bool_opt(_field, value) when value in [nil, false], do: []
  defp bool_opt(field, true), do: Wire.field_bool(field, true)
end
