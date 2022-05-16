defmodule Bani.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Bani.Registry},
      {Bani.ConnectionDynamicSupervisor, []},
      {Bani.TenantDynamicSupervisor, []}
    ]

    # Configure database and top level store
    :ok = Bani.Store.init_database()
    :ok = Bani.Store.TenantStore.init_store()

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Bani.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
