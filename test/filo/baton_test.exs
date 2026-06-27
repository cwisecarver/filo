defmodule Filo.BatonTest do
  use ExUnit.Case, async: true

  alias Filo.Baton

  setup do
    %{key: Baton.new_key()}
  end

  test "round-trips stream_id and seq", %{key: key} do
    assert {:ok, {123, 456}} = Baton.decode(Baton.encode(123, 456, key), key)
  end

  test "round-trips the full unsigned 64-bit range", %{key: key} do
    max = 18_446_744_073_709_551_615
    assert {:ok, {^max, ^max}} = Baton.decode(Baton.encode(max, max, key), key)
  end

  test "a baton signed with a different key is rejected", %{key: key} do
    other = Baton.new_key()
    assert Baton.decode(Baton.encode(1, 1, other), key) == {:error, :invalid}
  end

  test "a tampered payload fails MAC verification", %{key: key} do
    {:ok, <<first, rest::binary>>} = Base.decode64(Baton.encode(7, 7, key), padding: false)
    tampered = Base.encode64(<<:erlang.bxor(first, 1)>> <> rest, padding: false)
    assert Baton.decode(tampered, key) == {:error, :invalid}
  end

  test "non-base64 garbage is rejected", %{key: key} do
    assert Baton.decode("not a baton!!", key) == {:error, :invalid}
    assert Baton.decode("", key) == {:error, :invalid}
  end

  test "a well-formed base64 string of the wrong length is rejected", %{key: key} do
    short = Base.encode64("too short", padding: false)
    assert Baton.decode(short, key) == {:error, :invalid}
  end

  test "new_key/0 returns 32 random bytes" do
    key = Baton.new_key()
    assert byte_size(key) == 32
    assert key != Baton.new_key()
  end
end
