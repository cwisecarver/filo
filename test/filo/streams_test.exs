defmodule Filo.StreamsTest.Echo do
  @moduledoc false
  @behaviour Filo.Executor

  @impl true
  def open(:fail), do: {:error, %Filo.Error{message: "no db", code: "FILO_OPEN"}}

  # A slow open (a cold shard's S3 pull, a held retry): tell the test it started, then block until
  # the test says :go, and open or fail as told.
  def open({:gate, test, result}) do
    send(test, {:opening, self()})

    receive do
      :go -> if result == :ok, do: {:ok, :conn}, else: open(:fail)
    end
  end

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

  # Fathom expert review 2026-10-01 #2. The executor open used to run inside `Filo.Stream.init/1`,
  # and `DynamicSupervisor.start_child/2` does not return until `init/1` does — so ONE slow open (a
  # cold shard pulling from S3, a failover held-retry sleeping for seconds) blocked every other
  # baton-less stream open on the node behind the single supervisor. The open must happen outside
  # the supervisor: a second create completes while the first open is still stuck.
  test "a slow open does not block other streams from being created" do
    test = self()

    slow =
      Task.async(fn -> Streams.create(@name, executor: Echo, open_arg: {:gate, test, :ok}) end)

    assert_receive {:opening, opener}

    fast = Task.async(fn -> Streams.create(@name, executor: Echo, open_arg: self()) end)

    try do
      assert {:ok, {:ok, _id, _seq, _pid}} = Task.yield(fast, 2_000),
             "a second create waited behind the first stream's slow open"
    after
      # Release the slow open even on failure: pre-fix it holds the supervisor, which would then
      # never shut down and fail every later test's setup instead of just this one.
      send(opener, :go)
    end

    assert {:ok, _id, _seq, _pid} = Task.await(slow)
  end

  # The caller still waits for ITS OWN open, so `create/2`'s contract is unchanged: it returns only
  # once the connection is open, and a failed open comes back as {:error, {:open_failed, _}}.
  @tag :capture_log
  test "a slow open that fails still surfaces the executor error, and the stream is gone" do
    test = self()

    slow =
      Task.async(fn -> Streams.create(@name, executor: Echo, open_arg: {:gate, test, :fail}) end)

    assert_receive {:opening, opener}
    ref = Process.monitor(opener)
    send(opener, :go)

    assert {:error, {:open_failed, %Filo.Error{code: "FILO_OPEN"}}} = Task.await(slow)
    assert_receive {:DOWN, ^ref, :process, ^opener, _}
  end
end
