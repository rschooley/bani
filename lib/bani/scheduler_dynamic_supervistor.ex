defmodule Bani.SchedulerDynamicSupervisor do
  use DynamicSupervisor

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  def add_scheduler(tenant, conn_opts) do
    opts = [tenant: tenant, conn_opts: conn_opts]

    DynamicSupervisor.start_child(
      __MODULE__,
      {Bani.Scheduler, opts}
    )
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
