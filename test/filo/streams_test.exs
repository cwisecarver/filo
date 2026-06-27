defmodule Filo.StreamsTest.Echo do
  @moduledoc false
  @behaviour Filo.Executor

  @impl true
  def open(:fail), do: {:error, %Filo.Error{message: "no db", code: "FILO_OPEN"}}
  def open(_arg), do: {:ok, :conn}

  @impl true
  def execute(:conn, %Filo.Stmt{}), do: {:ok, %Filo.StmtResult{cols: ["n"], rows: [[1]]}}

  @impl true
  def autocommit?(:conn), do: true

  @impl true
  def close(:conn), do: :ok
end

defmodule Filo.StreamsTest do
  # Filo.Streams registers a Supervisor, Registry and DynamicSupervisor under
  # globally-named atoms, so these tests can't run concurrently with each other.
  use ExUnit.Case, async: false

  alias Filo.{Stream, Streams}
  alias Filo.StreamsTest.Echo

  @name __MODULE__.Streams

  setup do
    start_supervised!({Streams, name: @name})
    :ok
  end

  test "create starts a registered, running stream and returns its id, seq, and pid" do
    assert {:ok, stream_id, seq, pid} = Streams.create(@name, executor: Echo, open_arg: self())
    assert is_integer(stream_id)
    assert is_integer(seq)
    assert Process.alive?(pid)
    assert {:ok, ^pid} = Streams.lookup(@name, stream_id)
  end

  test "lookup is :error for an unknown stream id" do
    assert :error = Streams.lookup(@name, 123_456_789)
  end

  test "a created stream dispatches requests through the registry" do
    {:ok, stream_id, seq, _pid} = Streams.create(@name, executor: Echo, open_arg: self())
    {:ok, pid} = Streams.lookup(@name, stream_id)

    assert {:ok, :open, [result], next} = Stream.run(pid, seq, [%{"type" => "get_autocommit"}])
    assert is_integer(next)
    assert result["response"] == %{"type" => "get_autocommit", "is_autocommit" => true}
  end

  test "each create gets a distinct stream id" do
    ids =
      for _ <- 1..20 do
        {:ok, id, _seq, _pid} = Streams.create(@name, executor: Echo, open_arg: self())
        id
      end

    assert length(Enum.uniq(ids)) == 20
  end

  @tag :capture_log
  test "create surfaces the executor error when the connection cannot open" do
    assert {:error, {:open_failed, %Filo.Error{code: "FILO_OPEN"}}} =
             Streams.create(@name, executor: Echo, open_arg: :fail)
  end
end
