defmodule Bani.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    # Configure database and top level store
    :ok = Bani.Store.init_database()
    :ok = Bani.Store.TenantStore.init_store()

    children = [
      {Registry, keys: :unique, name: Bani.Registry},
      {Bani.ConnectionDynamicSupervisor, []},
      {Bani.TenantDynamicSupervisor, []}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Bani.Supervisor]
    res = Supervisor.start_link(children, opts)

    # TODO: cleanup
    Enum.each(Bani.Store.TenantStore.list_tenant_ids(), fn tenant ->
      Bani.TenantDynamicSupervisor.add_tenant(tenant)
    end)

    res
  end
end
