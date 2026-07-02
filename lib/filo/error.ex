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

  @doc "Encodes a `Filo.Error` into a Hrana `Error` map (the `status` hint is HTTP-only, not wire)."
  @spec encode(t()) :: map()
  def encode(%__MODULE__{} = error) do
    %{"message" => error.message, "code" => error.code}
  end
end
