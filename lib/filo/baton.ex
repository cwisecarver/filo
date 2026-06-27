defmodule Filo.Baton do
  @moduledoc """
  Hrana-over-HTTP baton — an opaque, server-signed token that identifies a stream
  and enforces single-use, serial access.

  Wire format (matching libsql): base64 (no padding) of a 48-byte string:

    - payload (16 bytes): `stream_id` (u64 big-endian) followed by `seq`
      (u64 big-endian)
    - MAC (32 bytes): `HMAC-SHA256(payload)` keyed by a per-server secret

  The MAC proves the baton was issued by this server. The `seq` rotates on every
  request, so a baton is single-use and requests on a stream are serialized: the
  next request must present the `seq` the server last handed back.
  """

  @payload_size 16
  @mac_size 32

  @doc "Generates a fresh 32-byte signing key for a server."
  @spec new_key() :: binary()
  def new_key, do: :crypto.strong_rand_bytes(32)

  @doc "Encodes a baton for `stream_id`/`seq`, signed with `key`."
  @spec encode(non_neg_integer(), non_neg_integer(), binary()) :: String.t()
  def encode(stream_id, seq, key) do
    payload = <<stream_id::unsigned-big-64, seq::unsigned-big-64>>
    Base.encode64(payload <> mac(payload, key), padding: false)
  end

  @doc """
  Decodes and verifies a baton, returning `{:ok, {stream_id, seq}}` or
  `{:error, :invalid}` for any malformed, wrong-length, or unauthentic baton.
  """
  @spec decode(String.t(), binary()) ::
          {:ok, {non_neg_integer(), non_neg_integer()}} | {:error, :invalid}
  def decode(baton, key) when is_binary(baton) do
    with {:ok, data} <- Base.decode64(baton, padding: false),
         <<payload::binary-size(@payload_size), received_mac::binary-size(@mac_size)>> <- data,
         true <- Plug.Crypto.secure_compare(received_mac, mac(payload, key)) do
      <<stream_id::unsigned-big-64, seq::unsigned-big-64>> = payload
      {:ok, {stream_id, seq}}
    else
      _ -> {:error, :invalid}
    end
  end

  defp mac(payload, key), do: :crypto.mac(:hmac, :sha256, key, payload)
end
