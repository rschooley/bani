defmodule Bani.ConnectionDynamicSupervisor do
  use DynamicSupervisor

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  def add_connection_supervisor(conn_opts, connection_id) do
    opts = Keyword.put(conn_opts, :connection_id, connection_id)

    DynamicSupervisor.start_child(
      __MODULE__,
      {Bani.ConnectionSupervisor, opts}
    )
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
