defmodule Filo.BatchCondTest do
  use ExUnit.Case, async: true

  alias Filo.BatchCond

  describe "decode/1" do
    test "ok / error reference a step index" do
      assert BatchCond.decode(%{"type" => "ok", "step" => 0}) == {:ok, 0}
      assert BatchCond.decode(%{"type" => "error", "step" => 2}) == {:error, 2}
    end

    test "not wraps a nested condition" do
      assert BatchCond.decode(%{"type" => "not", "cond" => %{"type" => "ok", "step" => 1}}) ==
               {:not, {:ok, 1}}
    end

    test "and / or carry a list of conditions" do
      json = %{
        "type" => "and",
        "conds" => [%{"type" => "ok", "step" => 0}, %{"type" => "ok", "step" => 1}]
      }

      assert BatchCond.decode(json) == {:and, [{:ok, 0}, {:ok, 1}]}

      assert BatchCond.decode(%{"type" => "or", "conds" => [%{"type" => "error", "step" => 0}]}) ==
               {:or, [{:error, 0}]}
    end

    test "is_autocommit" do
      assert BatchCond.decode(%{"type" => "is_autocommit"}) == :is_autocommit
    end
  end

  # `outcomes` is a list of :ok | :error | :skipped, indexed by step number.
  describe "eval/3" do
    test "ok is true only when that step succeeded" do
      assert BatchCond.eval({:ok, 0}, [:ok], false)
      refute BatchCond.eval({:ok, 0}, [:error], false)
      refute BatchCond.eval({:ok, 0}, [:skipped], false)
    end

    test "error is true only when that step errored" do
      assert BatchCond.eval({:error, 1}, [:ok, :error], false)
      refute BatchCond.eval({:error, 1}, [:ok, :ok], false)
    end

    test "a not-yet-reached step is neither ok nor error" do
      refute BatchCond.eval({:ok, 5}, [:ok], false)
      refute BatchCond.eval({:error, 5}, [:ok], false)
    end

    test "not negates" do
      refute BatchCond.eval({:not, {:ok, 0}}, [:ok], false)
      assert BatchCond.eval({:not, {:ok, 0}}, [:error], false)
    end

    test "and / or combine nested conditions" do
      assert BatchCond.eval({:and, [{:ok, 0}, {:ok, 1}]}, [:ok, :ok], false)
      refute BatchCond.eval({:and, [{:ok, 0}, {:ok, 1}]}, [:ok, :error], false)
      assert BatchCond.eval({:or, [{:ok, 0}, {:error, 1}]}, [:ok, :ok], false)
      refute BatchCond.eval({:or, [{:error, 0}, {:error, 1}]}, [:ok, :ok], false)
    end

    test "is_autocommit reflects the connection state" do
      assert BatchCond.eval(:is_autocommit, [], true)
      refute BatchCond.eval(:is_autocommit, [], false)
    end
  end
end
