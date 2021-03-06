defmodule Bani.Scheduling do
  @behaviour Bani.SchedulingBehaviour

  import Logger

  @impl Bani.SchedulingBehaviour
  def create_stream(conn_opts, stream_name) do
    {:ok, conn} = tmp_conn(conn_opts)
    :ok = Bani.Broker.create_stream(conn, stream_name)

    Bani.Broker.disconnect(conn)
  end

  @impl Bani.SchedulingBehaviour
  def delete_stream(conn_opts, stream_name) do
    {:ok, conn} = tmp_conn(conn_opts)
    :ok = Bani.Broker.delete_stream(conn, stream_name)

    Bani.Broker.disconnect(conn)
  end

  defp tmp_conn(conn_opts) do
    # we don't track / enforce a max connection limit for the server
    #  this might get bounced by rabbit / the provider

    host = Keyword.fetch!(conn_opts, :host)
    port = Keyword.fetch!(conn_opts, :port)
    username = Keyword.fetch!(conn_opts, :username)
    password = Keyword.fetch!(conn_opts, :password)
    vhost = Keyword.fetch!(conn_opts, :vhost)

    Bani.Broker.connect(host, port, username, password, vhost)
  end

  @doc """
  Creates a publisher for the tenant.  Will find an existing/create a new connection manager
  under a supervision tree.

  This should be called by a GenServer (Bani.Tenant) to avoid race conditions of
  the available ids for a connection.
  """
  @impl Bani.SchedulingBehaviour
  def create_publisher(tenant, conn_opts, stream_name) do
    {connection_id, publisher_id} = next_available_pubsub_opts(tenant, conn_opts, :publisher)

    {:ok, _} = Bani.ConnectionSupervisor.add_publisher(connection_id, tenant, stream_name, publisher_id)
    :ok
  end

  @impl Bani.SchedulingBehaviour
  def delete_publisher(tenant, stream_name) do
    {pid, connection_id, publisher_id} = Bani.Publisher.lookup(tenant, stream_name)

    :ok = Bani.ConnectionSupervisor.remove_publisher(connection_id, pid)
    :ok = Bani.Store.release_available_pubsub_id(tenant, connection_id, :publisher, publisher_id)
    :ok
  end

  @impl Bani.SchedulingBehaviour
  def create_subscriber(tenant, conn_opts, stream_name, subscription_name, handler, acc, offset, poisoned) do
    {connection_id, subscription_id} = next_available_pubsub_opts(tenant, conn_opts, :subscriber)

    storage_opts = [
      acc: acc,
      offset: offset,
      poisoned: poisoned,
      stream_name: stream_name,
      subscription_name: subscription_name,
      tenant: tenant
    ]

    # TODO: move this to ETS
    {:ok, _} = Bani.SubscriberStorageDynamicSupervisor.add_storage(storage_opts)
    {:ok, _} = Bani.ConnectionSupervisor.add_subscriber(connection_id, tenant, stream_name, subscription_id, subscription_name, handler)
    :ok
  end

  @impl Bani.SchedulingBehaviour
  def delete_subscriber(tenant, stream_name, subscription_name) do
    {pid, connection_id, subscription_id, subscription_name} = Bani.Subscriber.lookup(tenant, stream_name, subscription_name)

    :ok = Bani.ConnectionSupervisor.remove_subscriber(connection_id, pid)
    :ok = Bani.SubscriberStorageDynamicSupervisor.remove_storage(tenant, stream_name, subscription_name)
    :ok = Bani.Store.release_available_pubsub_id(tenant, connection_id, :subscriber, subscription_id)
    :ok
  end

  defp next_available_pubsub_opts(tenant, conn_opts, pubsub_type) do
    {connection_id, pubsub_id} = Bani.Store.next_available_pubsub_opts(tenant, pubsub_type)

    if (Bani.ConnectionSupervisor.exists?(connection_id)) do
      Logger.info("Bani Scheduling - existing connection id found (#{connection_id}) for tenant #{tenant}")
    else
      Logger.info("Bani Scheduling - existing connection id not found (#{connection_id}) for tenant #{tenant}")
      {:ok, _} = Bani.ConnectionDynamicSupervisor.add_connection_supervisor(conn_opts, connection_id)
    end

    {connection_id, pubsub_id}
  end
end
