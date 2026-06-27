defmodule Filo.DescribeTest do
  use ExUnit.Case, async: true

  alias Filo.Describe

  test "encodes params, cols, and flags into a Hrana DescribeResult" do
    describe = %Describe{
      params: [nil, ":name"],
      cols: ["x", %{name: "y", decltype: "TEXT"}],
      is_explain: false,
      is_readonly: true
    }

    assert Describe.encode(describe) == %{
             "params" => [%{"name" => nil}, %{"name" => ":name"}],
             "cols" => [
               %{"name" => "x", "decltype" => nil},
               %{"name" => "y", "decltype" => "TEXT"}
             ],
             "is_explain" => false,
             "is_readonly" => true
           }
  end

  test "defaults to empty params/cols and false flags" do
    assert Describe.encode(%Describe{}) == %{
             "params" => [],
             "cols" => [],
             "is_explain" => false,
             "is_readonly" => false
           }
  end
end
