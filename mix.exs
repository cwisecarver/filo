defmodule Filo.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/cwisecarver/filo"

  def project do
    [
      app: :filo,
      version: @version,
      elixir: "~> 1.20",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      description: description(),
      package: package(),
      source_url: @source_url,
      name: "Filo",
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  def cli do
    [preferred_envs: [precommit: :test]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:plug, "~> 1.16"},
      {:jason, "~> 1.4"},
      # The WebSock behaviour for the Hrana-over-WebSocket handler. Behaviour
      # only (like Plug) — the host brings the actual server (Bandit/Cowboy).
      {:websock, "~> 0.5"},
      # Test-only: a real HTTP/WebSocket server, plus a real SQLite engine for
      # the end-to-end executor, to run libsql clients against Filo. The library
      # itself ships a Plug and a WebSock handler and never depends on either.
      {:bandit, "~> 1.0", only: :test},
      {:exqlite, "~> 0.27", only: :test},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      precommit: [
        "compile --warning-as-errors",
        "deps.unlock --unused",
        "format --check-formatted",
        "test"
      ]
    ]
  end

  defp description do
    "A Hrana (libSQL) protocol server for Elixir — speak libSQL's wire " <>
      "protocol from any Plug app, backed by the SQLite engine of your choice."
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs do
    [
      main: "Filo",
      source_ref: "v#{@version}",
      extras: ["README.md"]
    ]
  end
end
