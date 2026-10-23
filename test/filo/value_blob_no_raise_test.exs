defmodule Filo.ValueBlobNoRaiseTest do
  # async: false - :erlang.trace_pattern/3 is node-global state.
  use ExUnit.Case, async: false

  alias Filo.Value

  # fathom expert review 2026-10-22 r8 #13, follow-up. r8 #13 dropped encode_json/1's validity
  # pre-scan and let `:json.encode_binary/1` RAISE on an invalid-UTF-8 binary, caught and turned
  # into a blob. Every blob cell arrives as a bare binary and takes that branch, and the raise
  # costs ~1.4 us at the top of the stack, growing with depth (3.2 us at 1000 frames) - so on
  # fathom's keystone result `wire_server_rows_per_s` fell 9% instead of rising. The invariant
  # pinned here: a blob cell is classified WITHOUT entering the encoder, so it cannot take the
  # raise path. (Timing is not asserted: it is load-sensitive; whether the encoder is called is
  # not.)

  setup do
    # trace_pattern only matches LOADED code: without this it matches 0 functions and every
    # call count reads 0.
    {:module, :json} = Code.ensure_loaded(:json)
    assert :erlang.trace_pattern({:json, :encode_binary, 1}, true, [:global]) == 1
    on_exit(fn -> :erlang.trace_pattern({:json, :encode_binary, 1}, false, [:global]) end)
    :ok
  end

  # Runs `fun` in a fresh process traced by this one; returns how many times it called
  # :json.encode_binary/1.
  defp encoder_calls(fun) do
    test = self()

    {pid, mref} =
      spawn_monitor(fn ->
        receive do
          :go -> send(test, {:result, fun.()})
        end
      end)

    :erlang.trace(pid, true, [:call])
    send(pid, :go)
    assert_receive {:result, :ok}, 5_000
    assert_receive {:DOWN, ^mref, :process, ^pid, :normal}, 5_000
    # Trace messages are delivered asynchronously; wait until every one sent so far is in.
    ref = :erlang.trace_delivered(pid)
    assert_receive {:trace_delivered, ^pid, ^ref}, 5_000
    count_calls(pid)
  end

  defp count_calls(pid) do
    receive do
      {:trace, ^pid, :call, {:json, :encode_binary, _}} -> 1 + count_calls(pid)
    after
      0 -> 0
    end
  end

  test "an invalid-UTF-8 binary becomes a blob without calling :json.encode_binary/1" do
    blobs = [
      <<0xFF>>,
      <<0xFF, 0xFE, 0xFD>>,
      <<"abc", 0xC3>>,
      <<0x80, "tail">>,
      <<0xED, 0xA0, 0x80>>
    ]

    calls =
      encoder_calls(fn ->
        for b <- blobs do
          expected = ~s({"type":"blob","base64":"#{Base.encode64(b, padding: false)}"})
          ^expected = IO.iodata_to_binary(Value.encode_json(b))
        end

        :ok
      end)

    assert calls == 0, "a blob cell went through the raising encoder path"
  end

  test "a valid text cell still uses :json.encode_binary/1 (the trace is live)" do
    # Guards the test above against passing because tracing silently did nothing.
    calls =
      encoder_calls(fn ->
        ~s({"type":"text","value":"héllo \\"q\\""}) =
          IO.iodata_to_binary(Value.encode_json("héllo \"q\""))

        :ok
      end)

    assert calls == 1
  end
end
