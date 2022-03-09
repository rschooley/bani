defmodule Bani.Scheduling do
  def available_conn(tenant, conn_opts) do
    key(tenant)
    |> available_connection_manager(conn_opts, :publisher)
    |> (fn ({pid, _value}) -> pid end).()
    |> Bani.ConnectionManager.conn()
  end

  def create_publisher(tenant, conn_opts, stream_name) do
    tenant_key = key(tenant)
    {pid, available_ids} = available_connection_manager(tenant_key, conn_opts, :publisher)

    Bani.ConnectionManager.add_publisher(pid, tenant_key, stream_name, available_ids)
  end

  def create_subscriber(tenant, conn_opts, stream_name, handler) do
    tenant_key = key(tenant)
    {pid, available_ids} = available_connection_manager(tenant_key, conn_opts, :subscriber)

    Bani.ConnectionManager.add_subscriber(pid, tenant_key, stream_name, available_ids, handler)
  end

  defp key(tenant) do
    "#{tenant}/available_connection_manager"
  end

  defp available_connection_manager(tenant_key, conn_opts, pubsub_type) do
    Bani.Storage.lookup(tenant_key)
    |> available_connection_manager(tenant_key, conn_opts, pubsub_type)
  end

  defp available_connection_manager([], tenant_key, conn_opts, _pubsub_type) do
    available_pubsub_ids = %{
      publisher: Enum.to_list(0..255),
      subscriber: Enum.to_list(0..255)
    }

    with {:ok, supervisor_pid} <- Bani.ConnectionDynamicSupervisor.add_connection_supervisor(conn_opts),
         connection_manager_pid <- Bani.ConnectionSupervisor.child_connection_manager(supervisor_pid),
         :ok <- Bani.ConnectionManager.register(connection_manager_pid, tenant_key, available_pubsub_ids)
    do
      {connection_manager_pid, available_pubsub_ids}
    end
  end

  defp available_connection_manager(connection_managers, _tenant, _conn_opts, _pubsub_type) do
    # look through conn managers for value of pubsub_type list has any
    # if not, create a new one (just call use_or_register_connection_manager([])?)

    # there should be at most 2 available connection_managers here per tenant
    # TODO
    hd(connection_managers)
  end
end
