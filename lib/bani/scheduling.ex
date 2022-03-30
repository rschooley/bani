defmodule Bani.Scheduling do
  @behaviour Bani.SchedulingBehaviour

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

    Bani.ConnectionSupervisor.add_publisher(connection_id, tenant, stream_name, publisher_id)
  end

  @impl Bani.SchedulingBehaviour
  def delete_publisher(_tenant, _stream_name) do
    # Bani.ConnectionSupervisor.remove_publisher(supervisor, conn, tenant, stream_name, id)

    # TODO: unlease id from connection manager
    # delete publisher in rabbit
    # stop publisher genserver
    # remove publisher genserver from connection_supervisor
    # cleanup empty connection supervisor?

    # publisher_name = Bani.KeyRing.publisher_name(tenant, stream_name)

    # Bani.ConnectionSupervisor.remove_publisher(supervisor, tenant, stream_name, id)

    # :ok = Bani.Publisher.delete_publisher(tenant, stream_name)

    # with {connection_manager, available_ids} <- Bani.ConnectionManagement.available_connection_manager(available_key, conn_opts, :publisher),
    #      {supervisor, conn, id} <- Bani.ConnectionManager.lease(connection_manager, available_key, available_ids, :publisher),
    #      {:ok, _} <- Bani.ConnectionSupervisor.add_publisher(supervisor, conn, tenant, stream_name, id)
    # do
    #   Bani.Publisher.create_publisher(tenant, stream_name)
    # end
  end

  @impl Bani.SchedulingBehaviour
  def create_subscriber(tenant, conn_opts, stream_name, subscription_name, handler) do
    {connection_id, subscription_id} = next_available_pubsub_opts(tenant, conn_opts, :subscriber)

    Bani.ConnectionSupervisor.add_subscriber(connection_id, tenant, stream_name, subscription_id, subscription_name, handler)
  end

  @impl Bani.SchedulingBehaviour
  def delete_subscriber(_tenant, _stream_name) do
  end

  defp next_available_pubsub_opts(tenant, conn_opts, pubsub_type) do
    Bani.Store.next_available_pubsub_opts(tenant, pubsub_type)
    |> next_available_pubsub_opts(conn_opts)
  end

  defp next_available_pubsub_opts({:new, connection_id, pubsub_id}, conn_opts) do
    {:ok, _} = Bani.ConnectionDynamicSupervisor.add_connection_supervisor(conn_opts, connection_id)

    {connection_id, pubsub_id}
  end

  defp next_available_pubsub_opts({:existing, connection_id, pubsub_id}, _conn_opts) do
    {connection_id, pubsub_id}
  end
end
