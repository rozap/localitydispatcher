defmodule Localitydispatcher.Mixfile do
  use Mix.Project

  def project do
    [app: :localitydispatcher,
     version: "0.1.1",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     description: description,
     package: package(),
     deps: deps()]
  end

  defp description do
    """
      A GenStage dispatcher to route events based on
      some notion of locality
    """
  end

  defp package do
    [
      licenses: ["MIT"],
      maintainers: ["Chris Duranti"],
      links: %{github: "https://github.com/rozap/localitydispatcher"}
    ]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [applications: [:logger]]
  end

  defp deps do
    [
      {:gen_stage, "~> 0.9.0"},
      {:ex_doc, ">= 0.0.0", only: :dev}
    ]
  end
end
