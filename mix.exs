defmodule Bani.MixProject do
  use Mix.Project

  def project do
    [
      app: :bani,
      version: "0.1.0",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env())
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      included_applications: [:mnesia],
      mod: {Bani.Application, []}
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:lake, "~> 0.1.2"},
      {:mox, ">= 0.0.0", only: :test},
      {:uniq, "~> 0.4"}
    ]
  end
end
