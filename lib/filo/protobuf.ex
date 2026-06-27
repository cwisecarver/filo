defmodule Filo.Protobuf do
  @moduledoc """
  Protobuf codec for the Hrana 3 shared structures.

  Filo's execution core works on the canonical Hrana **JSON-term maps** (the same
  maps `Jason` produces/consumes — integers carried as strings, blobs as no-pad
  base64, values tagged by `"type"`). Protobuf is therefore a pure *edge* codec:
  these functions convert between protobuf wire bytes and those same maps, so the
  streams/dispatch/execution layers never change. The HTTP and WebSocket message
  envelopes are layered on top in `Filo.Protobuf.Http` and `Filo.Protobuf.Ws`.

  Each `encode_*/1` returns the message *body* as `t:iodata/0` (a container embeds
  it with `Filo.Protobuf.Wire.field_len/2`); each `decode_*/1` takes the message
  bytes and returns the Hrana map. Field numbers follow the proto3 schema in the
  Hrana 3 spec.
  """
  alias Filo.Protobuf.Wire

  ## --- Value (oneof) ---

  @doc "Encodes a Hrana value map into a `Value` message body."
  @spec encode_value(map()) :: iodata()
  def encode_value(%{"type" => "null"}), do: Wire.field_len(1, "")
  def encode_value(%{"type" => "integer", "value" => v}), do: Wire.field_sint64(2, to_int(v))
  def encode_value(%{"type" => "float", "value" => v}), do: Wire.field_double(3, v * 1.0)
  def encode_value(%{"type" => "text", "value" => v}), do: Wire.field_len(4, v)
  def encode_value(%{"type" => "blob", "base64" => v}), do: Wire.field_len(5, decode_base64(v))

  @doc "Decodes a `Value` message into a Hrana value map."
  @spec decode_value(binary()) :: map()
  def decode_value(bin) do
    bin
    |> Wire.decode_fields()
    |> Enum.find_value(%{"type" => "null"}, fn
      {1, _} ->
        %{"type" => "null"}

      {2, {:varint, z}} ->
        %{"type" => "integer", "value" => Integer.to_string(Wire.zigzag_decode(z))}

      {3, {:fixed64, b}} ->
        %{"type" => "float", "value" => Wire.decode_double(b)}

      {4, {:len, s}} ->
        %{"type" => "text", "value" => s}

      {5, {:len, b}} ->
        %{"type" => "blob", "base64" => encode_base64(b)}

      _ ->
        nil
    end)
  end

  ## --- Error ---

  @spec encode_error(map()) :: iodata()
  def encode_error(%{} = error) do
    [
      string_field(1, Map.get(error, "message")),
      string_field(2, Map.get(error, "code"))
    ]
  end

  @spec decode_error(binary()) :: map()
  def decode_error(bin) do
    fields = Wire.decode_fields(bin)
    %{"message" => take_string(fields, 1, ""), "code" => take_string(fields, 2, nil)}
  end

  ## --- Stmt / NamedArg ---

  @spec encode_stmt(map()) :: iodata()
  def encode_stmt(%{} = stmt) do
    [
      string_field(1, Map.get(stmt, "sql")),
      int_field(2, Map.get(stmt, "sql_id")),
      for(arg <- Map.get(stmt, "args", []), do: Wire.field_len(3, encode_value(arg))),
      for(na <- Map.get(stmt, "named_args", []), do: Wire.field_len(4, encode_named_arg(na))),
      bool_field(5, Map.get(stmt, "want_rows"))
    ]
  end

  @spec decode_stmt(binary()) :: map()
  def decode_stmt(bin) do
    fields = Wire.decode_fields(bin)

    %{
      "args" => for(b <- collect_len(fields, 3), do: decode_value(b)),
      "named_args" => for(b <- collect_len(fields, 4), do: decode_named_arg(b))
    }
    |> put_if("sql", take_string(fields, 1, nil))
    |> put_if("sql_id", take_varint(fields, 2, nil))
    |> put_if("want_rows", take_bool(fields, 5, nil))
  end

  defp encode_named_arg(%{"name" => name, "value" => value}) do
    [Wire.field_len(1, name), Wire.field_len(2, encode_value(value))]
  end

  defp decode_named_arg(bin) do
    fields = Wire.decode_fields(bin)
    %{"name" => take_string(fields, 1, ""), "value" => decode_value(take_len(fields, 2) || "")}
  end

  ## --- StmtResult / Col / Row ---

  @spec encode_stmt_result(map()) :: iodata()
  def encode_stmt_result(%{} = result) do
    [
      for(col <- Map.get(result, "cols", []), do: Wire.field_len(1, encode_col(col))),
      for(row <- Map.get(result, "rows", []), do: Wire.field_len(2, encode_row(row))),
      uint_field(3, Map.get(result, "affected_row_count", 0)),
      int_field(4, to_int_or_nil(Map.get(result, "last_insert_rowid")), :sint)
    ]
  end

  @spec decode_stmt_result(binary()) :: map()
  def decode_stmt_result(bin) do
    fields = Wire.decode_fields(bin)
    rowid = take_sint(fields, 4, nil)

    %{
      "cols" => for(b <- collect_len(fields, 1), do: decode_col(b)),
      "rows" => for(b <- collect_len(fields, 2), do: decode_row(b)),
      "affected_row_count" => take_varint(fields, 3, 0),
      "last_insert_rowid" => if(rowid, do: Integer.to_string(rowid))
    }
  end

  defp encode_col(%{"name" => name, "decltype" => decltype}) do
    [string_field(1, name), string_field(2, decltype)]
  end

  defp encode_col(name) when is_binary(name), do: string_field(1, name)

  defp decode_col(bin) do
    fields = Wire.decode_fields(bin)
    %{"name" => take_string(fields, 1, nil), "decltype" => take_string(fields, 2, nil)}
  end

  defp encode_row(values), do: for(v <- values, do: Wire.field_len(1, encode_value(v)))

  defp decode_row(bin) do
    bin
    |> Wire.decode_fields()
    |> Enum.flat_map(fn
      {1, {:len, b}} -> [decode_value(b)]
      _ -> []
    end)
  end

  ## --- Batch / BatchStep / BatchCond ---

  @spec encode_batch(map()) :: iodata()
  def encode_batch(%{} = batch) do
    for step <- Map.get(batch, "steps", []), do: Wire.field_len(1, encode_step(step))
  end

  @spec decode_batch(binary()) :: map()
  def decode_batch(bin) do
    fields = Wire.decode_fields(bin)
    %{"steps" => for(b <- collect_len(fields, 1), do: decode_step(b))}
  end

  defp encode_step(%{"stmt" => stmt} = step) do
    [
      case Map.get(step, "condition") do
        nil -> []
        cond -> Wire.field_len(1, encode_cond(cond))
      end,
      Wire.field_len(2, encode_stmt(stmt))
    ]
  end

  defp decode_step(bin) do
    fields = Wire.decode_fields(bin)

    %{"stmt" => decode_stmt(take_len(fields, 2) || "")}
    |> put_if("condition", decode_cond_opt(take_len(fields, 1)))
  end

  @doc "Encodes a Hrana `BatchCond` map into a `BatchCond` message body."
  @spec encode_cond(map()) :: iodata()
  def encode_cond(%{"type" => "ok", "step" => s}), do: Wire.field_varint(1, s)
  def encode_cond(%{"type" => "error", "step" => s}), do: Wire.field_varint(2, s)
  def encode_cond(%{"type" => "not", "cond" => c}), do: Wire.field_len(3, encode_cond(c))
  def encode_cond(%{"type" => "and", "conds" => cs}), do: Wire.field_len(4, encode_cond_list(cs))
  def encode_cond(%{"type" => "or", "conds" => cs}), do: Wire.field_len(5, encode_cond_list(cs))
  def encode_cond(%{"type" => "is_autocommit"}), do: Wire.field_len(6, "")

  @doc "Decodes a `BatchCond` message into a Hrana condition map."
  @spec decode_cond(binary()) :: map()
  def decode_cond(bin) do
    bin
    |> Wire.decode_fields()
    |> Enum.find_value(fn
      {1, {:varint, s}} -> %{"type" => "ok", "step" => s}
      {2, {:varint, s}} -> %{"type" => "error", "step" => s}
      {3, {:len, b}} -> %{"type" => "not", "cond" => decode_cond(b)}
      {4, {:len, b}} -> %{"type" => "and", "conds" => decode_cond_list(b)}
      {5, {:len, b}} -> %{"type" => "or", "conds" => decode_cond_list(b)}
      {6, _} -> %{"type" => "is_autocommit"}
      _ -> nil
    end)
  end

  defp decode_cond_opt(nil), do: nil
  defp decode_cond_opt(bin), do: decode_cond(bin)

  defp encode_cond_list(conds), do: for(c <- conds, do: Wire.field_len(1, encode_cond(c)))

  defp decode_cond_list(bin),
    do: for(b <- collect_len(Wire.decode_fields(bin), 1), do: decode_cond(b))

  ## --- BatchResult (proto maps keyed by step index) ---

  @spec encode_batch_result(map()) :: iodata()
  def encode_batch_result(%{} = result) do
    [
      encode_indexed(1, Map.get(result, "step_results", []), &encode_stmt_result/1),
      encode_indexed(2, Map.get(result, "step_errors", []), &encode_error/1)
    ]
  end

  @spec decode_batch_result(binary()) :: map()
  def decode_batch_result(bin) do
    fields = Wire.decode_fields(bin)
    results = for(b <- collect_len(fields, 1), do: decode_map_entry(b, &decode_stmt_result/1))
    errors = for(b <- collect_len(fields, 2), do: decode_map_entry(b, &decode_error/1))

    # step_results and step_errors are parallel positional arrays, so they share a
    # length: the step count is the max key across both proto maps.
    count = step_count(results ++ errors)
    %{"step_results" => positional(results, count), "step_errors" => positional(errors, count)}
  end

  # A proto map field: one length-delimited entry message {key=1, value=2} per
  # non-nil element, keyed by its position in the Hrana positional array.
  defp encode_indexed(field, list, encode_value_fun) do
    list
    |> Enum.with_index()
    |> Enum.flat_map(fn
      {nil, _index} ->
        []

      {value, index} ->
        entry = [Wire.field_varint(1, index), Wire.field_len(2, encode_value_fun.(value))]
        [Wire.field_len(field, entry)]
    end)
  end

  defp decode_map_entry(bin, decoder) do
    fields = Wire.decode_fields(bin)

    value =
      case take_len(fields, 2) do
        nil -> nil
        b -> decoder.(b)
      end

    {take_varint(fields, 1, 0), value}
  end

  defp step_count([]), do: 0
  defp step_count(entries), do: 1 + (entries |> Enum.map(fn {k, _} -> k end) |> Enum.max())

  defp positional(_entries, 0), do: []

  defp positional(entries, count) do
    map = Map.new(entries)
    for i <- 0..(count - 1), do: Map.get(map, i)
  end

  ## --- DescribeResult ---

  @spec encode_describe_result(map()) :: iodata()
  def encode_describe_result(%{} = describe) do
    [
      for(p <- Map.get(describe, "params", []), do: Wire.field_len(1, encode_describe_param(p))),
      for(c <- Map.get(describe, "cols", []), do: Wire.field_len(2, encode_describe_col(c))),
      bool_field(3, Map.get(describe, "is_explain", false) || nil),
      bool_field(4, Map.get(describe, "is_readonly", false) || nil)
    ]
  end

  @spec decode_describe_result(binary()) :: map()
  def decode_describe_result(bin) do
    fields = Wire.decode_fields(bin)

    %{
      "params" =>
        for(
          b <- collect_len(fields, 1),
          do: %{"name" => take_string(Wire.decode_fields(b), 1, nil)}
        ),
      "cols" => for(b <- collect_len(fields, 2), do: decode_col(b)),
      "is_explain" => take_bool(fields, 3, false),
      "is_readonly" => take_bool(fields, 4, false)
    }
  end

  defp encode_describe_param(%{"name" => name}), do: string_field(1, name)
  defp encode_describe_col(col), do: encode_col(col)

  ## --- CursorEntry (oneof) ---

  @spec encode_cursor_entry(map()) :: iodata()
  def encode_cursor_entry(%{"type" => "step_begin"} = e) do
    body = [
      Wire.field_varint(1, Map.get(e, "step", 0)),
      for(col <- Map.get(e, "cols", []), do: Wire.field_len(2, encode_col(col)))
    ]

    Wire.field_len(1, body)
  end

  def encode_cursor_entry(%{"type" => "step_end"} = e) do
    body = [
      uint_field(1, Map.get(e, "affected_row_count", 0)),
      int_field(2, to_int_or_nil(Map.get(e, "last_insert_rowid")), :sint)
    ]

    Wire.field_len(2, body)
  end

  def encode_cursor_entry(%{"type" => "step_error"} = e) do
    body = [
      Wire.field_varint(1, Map.get(e, "step", 0)),
      Wire.field_len(2, encode_error(e["error"]))
    ]

    Wire.field_len(3, body)
  end

  def encode_cursor_entry(%{"type" => "row", "row" => row}),
    do: Wire.field_len(4, encode_row(row))

  def encode_cursor_entry(%{"type" => "error", "error" => err}),
    do: Wire.field_len(5, encode_error(err))

  @spec decode_cursor_entry(binary()) :: map()
  def decode_cursor_entry(bin) do
    bin
    |> Wire.decode_fields()
    |> Enum.find_value(fn
      {1, {:len, b}} -> decode_step_begin(b)
      {2, {:len, b}} -> decode_step_end(b)
      {3, {:len, b}} -> decode_step_error(b)
      {4, {:len, b}} -> %{"type" => "row", "row" => decode_row(b)}
      {5, {:len, b}} -> %{"type" => "error", "error" => decode_error(b)}
      _ -> nil
    end)
  end

  defp decode_step_begin(bin) do
    fields = Wire.decode_fields(bin)

    %{
      "type" => "step_begin",
      "step" => take_varint(fields, 1, 0),
      "cols" => for(b <- collect_len(fields, 2), do: decode_col(b))
    }
  end

  defp decode_step_end(bin) do
    fields = Wire.decode_fields(bin)
    rowid = take_sint(fields, 2, nil)

    %{
      "type" => "step_end",
      "affected_row_count" => take_varint(fields, 1, 0),
      "last_insert_rowid" => if(rowid, do: Integer.to_string(rowid))
    }
  end

  defp decode_step_error(bin) do
    fields = Wire.decode_fields(bin)

    %{
      "type" => "step_error",
      "step" => take_varint(fields, 1, 0),
      "error" => decode_error(take_len(fields, 2) || "")
    }
  end

  ## --- field helpers (presence-aware, proto3) ---

  @doc false
  def string_field(_field, nil), do: []
  def string_field(_field, ""), do: []
  def string_field(field, v) when is_binary(v), do: Wire.field_len(field, v)

  defp int_field(_field, nil), do: []
  defp int_field(field, v) when is_integer(v), do: Wire.field_int(field, v)

  defp int_field(_field, nil, _kind), do: []
  defp int_field(field, v, :sint) when is_integer(v), do: Wire.field_sint64(field, v)

  defp uint_field(_field, 0), do: []
  defp uint_field(field, v) when is_integer(v) and v > 0, do: Wire.field_varint(field, v)

  defp bool_field(_field, nil), do: []
  defp bool_field(_field, false), do: []
  defp bool_field(field, true), do: Wire.field_bool(field, true)

  ## --- field accessors over decoded fields ---

  defp field_value(fields, num) do
    case fields |> Enum.filter(fn {f, _} -> f == num end) |> List.last() do
      nil -> nil
      {_f, v} -> v
    end
  end

  @doc false
  def take_string(fields, num, default) do
    case field_value(fields, num) do
      {:len, v} -> v
      _ -> default
    end
  end

  @doc false
  def take_varint(fields, num, default) do
    case field_value(fields, num) do
      {:varint, v} -> v
      _ -> default
    end
  end

  defp take_sint(fields, num, default) do
    case field_value(fields, num) do
      {:varint, v} -> Wire.zigzag_decode(v)
      _ -> default
    end
  end

  defp take_bool(fields, num, default) do
    case field_value(fields, num) do
      {:varint, v} -> v != 0
      _ -> default
    end
  end

  @doc false
  def take_len(fields, num) do
    case field_value(fields, num) do
      {:len, v} -> v
      _ -> nil
    end
  end

  @doc false
  def collect_len(fields, num) do
    for {f, {:len, b}} <- fields, f == num, do: b
  end

  ## --- value helpers ---

  @doc false
  def put_if(map, _key, nil), do: map
  def put_if(map, key, value), do: Map.put(map, key, value)

  defp to_int(v) when is_integer(v), do: v
  defp to_int(v) when is_binary(v), do: String.to_integer(v)

  defp to_int_or_nil(nil), do: nil
  defp to_int_or_nil(v), do: to_int(v)

  defp encode_base64(bin), do: Base.encode64(bin, padding: false)

  defp decode_base64(str) do
    str |> String.trim_trailing("=") |> Base.decode64!(padding: false)
  end
end
