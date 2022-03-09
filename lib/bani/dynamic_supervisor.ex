defmodule Bani.ConnectionDynamicSupervisor do
  use DynamicSupervisor

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  def add_connection_supervisor(conn_opts) do
    DynamicSupervisor.start_child(
      __MODULE__,
      {Bani.ConnectionSupervisor, conn_opts}
    )
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
