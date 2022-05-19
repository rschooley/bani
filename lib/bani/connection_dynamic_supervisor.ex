defmodule Bani.ConnectionDynamicSupervisor do
  use DynamicSupervisor

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  def add_connection_supervisor(tenant, conn_opts, connection_id) do
    opts =
      conn_opts
      |> Keyword.put(:connection_id, connection_id)
      |> Keyword.put(:tenant, tenant)

    DynamicSupervisor.start_child(
      __MODULE__,
      {Bani.ConnectionSupervisor, opts}
    )
  end

  def remove_connection_supervisor(pids) when is_list(pids) do
    Enum.each(pids, fn pid ->
      :ok = remove_connection_supervisor(pid)
    end)
  end

  def remove_connection_supervisor(pid) when is_pid(pid) do
    DynamicSupervisor.terminate_child(__MODULE__, pid)
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
