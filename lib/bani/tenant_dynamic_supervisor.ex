defmodule Bani.TenantDynamicSupervisor do
  use DynamicSupervisor

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  def add_tenant(tenant) do
    opts = [tenant: tenant]

    DynamicSupervisor.start_child(
      __MODULE__,
      {Bani.Tenant, opts}
    )
  end

  def remove_tenant(pid) do
    DynamicSupervisor.terminate_child(__MODULE__, pid)
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
