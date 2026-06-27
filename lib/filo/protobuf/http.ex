defmodule Filo.Protobuf.Http do
  @moduledoc """
  Protobuf codec for the Hrana 3 *over HTTP* message envelopes: the pipeline and
  cursor request/response bodies and the per-request `StreamRequest` /
  `StreamResponse` / `StreamResult` types. Maps to and from the same Hrana maps
  the JSON pipeline uses (`%{"baton" => ..., "requests" => [...]}`, request maps
  tagged by `"type"`), so `Filo.Plug` can serve the `v3-protobuf` routes by
  swapping only the body codec. Shared structures come from `Filo.Protobuf`.
  """
  alias Filo.Protobuf
  alias Filo.Protobuf.Wire

  ## --- PipelineReqBody / PipelineRespBody ---

  @spec decode_pipeline_req(binary()) :: map()
  def decode_pipeline_req(bin) do
    fields = Wire.decode_fields(bin)

    %{
      "baton" => Protobuf.take_string(fields, 1, nil),
      "requests" => for(b <- Protobuf.collect_len(fields, 2), do: decode_stream_request(b))
    }
  end

  @spec encode_pipeline_req(map()) :: iodata()
  def encode_pipeline_req(%{} = body) do
    [
      Protobuf.string_field(1, Map.get(body, "baton")),
      for(r <- Map.get(body, "requests", []), do: Wire.field_len(2, encode_stream_request(r)))
    ]
  end

  @spec encode_pipeline_resp(map()) :: iodata()
  def encode_pipeline_resp(%{} = body) do
    [
      Protobuf.string_field(1, Map.get(body, "baton")),
      Protobuf.string_field(2, Map.get(body, "base_url")),
      for(r <- Map.get(body, "results", []), do: Wire.field_len(3, encode_stream_result(r)))
    ]
  end

  @spec decode_pipeline_resp(binary()) :: map()
  def decode_pipeline_resp(bin) do
    fields = Wire.decode_fields(bin)

    %{
      "baton" => Protobuf.take_string(fields, 1, nil),
      "base_url" => Protobuf.take_string(fields, 2, nil),
      "results" => for(b <- Protobuf.collect_len(fields, 3), do: decode_stream_result(b))
    }
  end

  ## --- CursorReqBody / CursorRespBody ---

  @spec decode_cursor_req(binary()) :: map()
  def decode_cursor_req(bin) do
    fields = Wire.decode_fields(bin)

    %{
      "baton" => Protobuf.take_string(fields, 1, nil),
      "batch" => Protobuf.decode_batch(Protobuf.take_len(fields, 2) || "")
    }
  end

  @spec encode_cursor_req(map()) :: iodata()
  def encode_cursor_req(%{"batch" => batch} = body) do
    [
      Protobuf.string_field(1, Map.get(body, "baton")),
      Wire.field_len(2, Protobuf.encode_batch(batch))
    ]
  end

  @spec encode_cursor_resp(map()) :: iodata()
  def encode_cursor_resp(%{} = body) do
    [
      Protobuf.string_field(1, Map.get(body, "baton")),
      Protobuf.string_field(2, Map.get(body, "base_url"))
    ]
  end

  @spec decode_cursor_resp(binary()) :: map()
  def decode_cursor_resp(bin) do
    fields = Wire.decode_fields(bin)

    %{
      "baton" => Protobuf.take_string(fields, 1, nil),
      "base_url" => Protobuf.take_string(fields, 2, nil)
    }
  end

  ## --- StreamRequest (oneof) ---

  @spec encode_stream_request(map()) :: iodata()
  def encode_stream_request(%{"type" => "close"}), do: Wire.field_len(1, "")

  def encode_stream_request(%{"type" => "execute", "stmt" => stmt}),
    do: Wire.field_len(2, Wire.field_len(1, Protobuf.encode_stmt(stmt)))

  def encode_stream_request(%{"type" => "batch", "batch" => batch}),
    do: Wire.field_len(3, Wire.field_len(1, Protobuf.encode_batch(batch)))

  def encode_stream_request(%{"type" => "sequence"} = r),
    do: Wire.field_len(4, sql_ref(r))

  def encode_stream_request(%{"type" => "describe"} = r),
    do: Wire.field_len(5, sql_ref(r))

  def encode_stream_request(%{"type" => "store_sql"} = r),
    do: Wire.field_len(6, [Wire.field_int(1, r["sql_id"]), Protobuf.string_field(2, r["sql"])])

  def encode_stream_request(%{"type" => "close_sql"} = r),
    do: Wire.field_len(7, Wire.field_int(1, r["sql_id"]))

  def encode_stream_request(%{"type" => "get_autocommit"}), do: Wire.field_len(8, "")

  @spec decode_stream_request(binary()) :: map()
  def decode_stream_request(bin) do
    bin
    |> Wire.decode_fields()
    |> Enum.find_value(fn
      {1, {:len, _}} ->
        %{"type" => "close"}

      {2, {:len, b}} ->
        %{"type" => "execute", "stmt" => Protobuf.decode_stmt(inner(b, 1))}

      {3, {:len, b}} ->
        %{"type" => "batch", "batch" => Protobuf.decode_batch(inner(b, 1))}

      {4, {:len, b}} ->
        sql_ref_map("sequence", b)

      {5, {:len, b}} ->
        sql_ref_map("describe", b)

      {6, {:len, b}} ->
        decode_store_sql(b)

      {7, {:len, b}} ->
        %{"type" => "close_sql", "sql_id" => Protobuf.take_varint(Wire.decode_fields(b), 1, nil)}

      {8, {:len, _}} ->
        %{"type" => "get_autocommit"}

      _ ->
        nil
    end)
  end

  # SequenceStreamReq / DescribeStreamReq: optional sql (1) or sql_id (2).
  defp sql_ref(r) do
    [Protobuf.string_field(1, Map.get(r, "sql")), int_opt(2, Map.get(r, "sql_id"))]
  end

  defp sql_ref_map(type, b) do
    fields = Wire.decode_fields(b)

    %{"type" => type}
    |> Protobuf.put_if("sql", Protobuf.take_string(fields, 1, nil))
    |> Protobuf.put_if("sql_id", Protobuf.take_varint(fields, 2, nil))
  end

  defp decode_store_sql(b) do
    fields = Wire.decode_fields(b)

    %{
      "type" => "store_sql",
      "sql_id" => Protobuf.take_varint(fields, 1, nil),
      "sql" => Protobuf.take_string(fields, 2, "")
    }
  end

  ## --- StreamResult (oneof ok | error) ---

  @spec encode_stream_result(map()) :: iodata()
  def encode_stream_result(%{"type" => "ok", "response" => response}),
    do: Wire.field_len(1, encode_stream_response(response))

  def encode_stream_result(%{"type" => "error", "error" => error}),
    do: Wire.field_len(2, Protobuf.encode_error(error))

  @spec decode_stream_result(binary()) :: map()
  def decode_stream_result(bin) do
    bin
    |> Wire.decode_fields()
    |> Enum.find_value(fn
      {1, {:len, b}} -> %{"type" => "ok", "response" => decode_stream_response(b)}
      {2, {:len, b}} -> %{"type" => "error", "error" => Protobuf.decode_error(b)}
      _ -> nil
    end)
  end

  ## --- StreamResponse (oneof) ---

  @spec encode_stream_response(map()) :: iodata()
  def encode_stream_response(%{"type" => "close"}), do: Wire.field_len(1, "")

  def encode_stream_response(%{"type" => "execute", "result" => result}),
    do: Wire.field_len(2, Wire.field_len(1, Protobuf.encode_stmt_result(result)))

  def encode_stream_response(%{"type" => "batch", "result" => result}),
    do: Wire.field_len(3, Wire.field_len(1, Protobuf.encode_batch_result(result)))

  def encode_stream_response(%{"type" => "sequence"}), do: Wire.field_len(4, "")

  def encode_stream_response(%{"type" => "describe", "result" => result}),
    do: Wire.field_len(5, Wire.field_len(1, Protobuf.encode_describe_result(result)))

  def encode_stream_response(%{"type" => "store_sql"}), do: Wire.field_len(6, "")
  def encode_stream_response(%{"type" => "close_sql"}), do: Wire.field_len(7, "")

  def encode_stream_response(%{"type" => "get_autocommit"} = r),
    do: Wire.field_len(8, bool_opt(1, Map.get(r, "is_autocommit")))

  @spec decode_stream_response(binary()) :: map()
  def decode_stream_response(bin) do
    bin
    |> Wire.decode_fields()
    |> Enum.find_value(fn
      {1, {:len, _}} ->
        %{"type" => "close"}

      {2, {:len, b}} ->
        %{"type" => "execute", "result" => Protobuf.decode_stmt_result(inner(b, 1))}

      {3, {:len, b}} ->
        %{"type" => "batch", "result" => Protobuf.decode_batch_result(inner(b, 1))}

      {4, {:len, _}} ->
        %{"type" => "sequence"}

      {5, {:len, b}} ->
        %{"type" => "describe", "result" => Protobuf.decode_describe_result(inner(b, 1))}

      {6, {:len, _}} ->
        %{"type" => "store_sql"}

      {7, {:len, _}} ->
        %{"type" => "close_sql"}

      {8, {:len, b}} ->
        %{"type" => "get_autocommit", "is_autocommit" => decode_bool(b, 1)}

      _ ->
        nil
    end)
  end

  ## --- helpers ---

  defp inner(b, field), do: Protobuf.take_len(Wire.decode_fields(b), field) || ""

  defp int_opt(_field, nil), do: []
  defp int_opt(field, v) when is_integer(v), do: Wire.field_int(field, v)

  defp bool_opt(_field, value) when value in [nil, false], do: []
  defp bool_opt(field, true), do: Wire.field_bool(field, true)

  defp decode_bool(b, field), do: Protobuf.take_varint(Wire.decode_fields(b), field, 0) != 0
end
