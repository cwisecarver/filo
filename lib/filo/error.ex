defmodule Filo.Error do
  @moduledoc """
  A Hrana error: a human-readable `message` and a SQLite error `code`
  (e.g. `"SQLITE_CONSTRAINT"`).

  `status` is an optional HTTP status hint for the HTTP transports: an executor's `open/1` can set
  it (e.g. `400` for a bad/missing shard, `503` at capacity) so a failed stream-open surfaces the
  right status instead of a blanket `500`. It is purely a transport concern — `encode/1` (the Hrana
  wire form) never includes it, and `nil` means "let the caller pick a default".
  """

  @type t :: %__MODULE__{
          message: String.t(),
          code: String.t() | nil,
          status: pos_integer() | nil
        }

  defstruct message: "", code: nil, status: nil

  @doc false
  # The error for a request the server could not decode (fathom expert review 2026-10-10 panel2 H8).
  @spec bad_request(String.t()) :: t()
  def bad_request(message \\ "malformed request"),
    do: %__MODULE__{message: message, code: "FILO_BAD_REQUEST", status: 400}

  @doc false
  # Runs a pure decoder, turning ANY raise into `{:error, bad_request}`. Client-supplied JSON/protobuf
  # reaches decoders that pattern-match and `String.to_integer`; a malformed value must be answered
  # with an error, not crash the socket or stream process that owns the client's open transaction.
  # Only decoding goes through here - an executor's own failures are never swallowed.
  @spec guard((-> a)) :: {:ok, a} | {:error, t()} when a: term()
  def guard(decoder) when is_function(decoder, 0) do
    {:ok, decoder.()}
  rescue
    _ -> {:error, bad_request()}
  end

  @doc "Encodes a `Filo.Error` into a Hrana `Error` map (the `status` hint is HTTP-only, not wire)."
  @spec encode(t()) :: map()
  def encode(%__MODULE__{} = error) do
    %{"message" => error.message, "code" => error.code}
  end
end
