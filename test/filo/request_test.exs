defmodule Filo.RequestTest.FakeExecutor do
  @moduledoc false
  @behaviour Filo.Executor

  @impl true
  def open(_arg), do: {:ok, :conn}

  @impl true
  def execute(:conn, %Filo.Stmt{sql: "BOOM"}),
    do: {:error, %Filo.Error{message: "boom", code: "SQLITE_ERROR"}}

  @impl true
  def execute(:conn, %Filo.Stmt{}), do: {:ok, %Filo.StmtResult{cols: ["n"], rows: [[1]]}}

  @impl true
  def autocommit?(:conn), do: true

  @impl true
  def close(:conn), do: :ok

  @impl true
  def describe(:conn, "BOOM"), do: {:error, %Filo.Error{message: "boom", code: "SQLITE_ERROR"}}
  def describe(:conn, _sql), do: {:ok, %Filo.Describe{cols: ["n"], is_readonly: true}}
end

defmodule Filo.RequestTest do
  use ExUnit.Case, async: true

  alias Filo.Request
  alias Filo.RequestTest.FakeExecutor

  test "execute -> open + ok/execute response with encoded rows" do
    req = %{"type" => "execute", "stmt" => %{"sql" => "SELECT 1"}}
    assert {:open, result} = Request.handle(FakeExecutor, :conn, req)
    assert result["type"] == "ok"
    assert result["response"]["type"] == "execute"
    assert result["response"]["result"]["rows"] == [[%{"type" => "integer", "value" => "1"}]]
  end

  test "execute failure -> error stream result" do
    req = %{"type" => "execute", "stmt" => %{"sql" => "BOOM"}}

    assert {:open,
            %{"type" => "error", "error" => %{"message" => "boom", "code" => "SQLITE_ERROR"}}} =
             Request.handle(FakeExecutor, :conn, req)
  end

  test "batch -> open + ok/batch response" do
    req = %{"type" => "batch", "batch" => %{"steps" => [%{"stmt" => %{"sql" => "SELECT 1"}}]}}
    assert {:open, result} = Request.handle(FakeExecutor, :conn, req)
    assert result["response"]["type"] == "batch"
    assert [%{"affected_row_count" => _} | _] = result["response"]["result"]["step_results"]
  end

  test "get_autocommit -> open + is_autocommit" do
    assert {:open, result} = Request.handle(FakeExecutor, :conn, %{"type" => "get_autocommit"})
    assert result["response"] == %{"type" => "get_autocommit", "is_autocommit" => true}
  end

  test "close -> closed + close response, after releasing the connection" do
    assert {:closed, result} = Request.handle(FakeExecutor, :conn, %{"type" => "close"})
    assert result == %{"type" => "ok", "response" => %{"type" => "close"}}
  end

  test "describe -> open + ok/describe response with the statement shape" do
    req = %{"type" => "describe", "sql" => "SELECT 1"}
    assert {:open, result} = Request.handle(FakeExecutor, :conn, req)
    assert result["response"]["type"] == "describe"
    assert result["response"]["result"]["cols"] == [%{"name" => "n", "decltype" => nil}]
    assert result["response"]["result"]["is_readonly"] == true
  end

  test "describe failure -> error stream result" do
    req = %{"type" => "describe", "sql" => "BOOM"}

    assert {:open,
            %{"type" => "error", "error" => %{"message" => "boom", "code" => "SQLITE_ERROR"}}} =
             Request.handle(FakeExecutor, :conn, req)
  end

  test "unsupported request type -> error stream result, stream stays open" do
    assert {:open, %{"type" => "error", "error" => %{"message" => message}}} =
             Request.handle(FakeExecutor, :conn, %{"type" => "sequence", "sql" => "SELECT 1"})

    assert message =~ "sequence"
  end
end
