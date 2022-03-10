defmodule Bani.Scheduling do
  def available_conn(tenant, conn_opts) do
    key(tenant)
    |> available_connection_manager(conn_opts, :publisher)
    |> (fn ({pid, _value}) -> pid end).()
    |> Bani.ConnectionManager.conn()
  end

  def create_publisher(tenant, conn_opts, stream_name) do
    tenant_key = key(tenant)

    with {pid, available_ids} <- available_connection_manager(tenant_key, conn_opts, :publisher),
         {supervisor, conn, id} <- Bani.ConnectionManager.lease(pid, tenant_key, available_ids, :publisher),
         {:ok, _} <- Bani.ConnectionSupervisor.add_publisher(supervisor, stream_name, id, conn)
    do
      :ok
    end
  end

  def create_subscriber(tenant, conn_opts, stream_name, handler) do
    tenant_key = key(tenant)

    with {pid, available_ids} <- available_connection_manager(tenant_key, conn_opts, :subscriber),
         {supervisor, conn, id} <- Bani.ConnectionManager.lease(pid, tenant_key, available_ids, :subscriber),
         {:ok, _} <- Bani.ConnectionSupervisor.add_subscriber(supervisor, stream_name, id, conn, handler)
    do
      :ok
    end
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

    with {:ok, supervisor} <- Bani.ConnectionDynamicSupervisor.add_connection_supervisor(conn_opts),
         connection_manager <- Bani.ConnectionSupervisor.child_connection_manager(supervisor),
         :ok <- Bani.ConnectionManager.register(connection_manager, tenant_key, available_pubsub_ids)
    do
      {connection_manager, available_pubsub_ids}
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
