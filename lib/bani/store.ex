defmodule Bani.Store do
  # lookups
  # may add a connections table
  # or may move this to mnesia with indexes
  @available_publisher_connections :available_publisher_connections
  @available_subscriber_connections :available_subscriber_connections

  def init_connections_store() do
    # TODO: all tenants are being stored in one table
    #  table names are atoms -> atom limit in erlang
    #  tenant data is not segmented
    # TODO: ETS data will be lost on app restart, deploy, infrastructure upgrade, etc
    :ets.new(@available_publisher_connections, [:named_table, :set])
    :ets.new(@available_subscriber_connections, [:named_table, :set])

    :ok
  end

  def insert_connection(tenant) do
    connection_id = Uniq.UUID.uuid1()
    available_pubsub_ids = Enum.to_list(0..255)

    key = tenant_connection_key(tenant, connection_id)

    true = :ets.insert(@available_publisher_connections, {key, available_pubsub_ids})
    true = :ets.insert(@available_subscriber_connections, {key, available_pubsub_ids})

    {key, available_pubsub_ids}
  end

  # caller must ensure atomic access
  def next_available_pubsub_opts(tenant, :publisher) do
    table = @available_publisher_connections

    # from the first item in the table, grab the first id in the available list
    :ets.first(table) |> next_available_pubsub_opts(table, tenant)
  end

  defp next_available_pubsub_opts(:"$end_of_table", table, tenant) do
    {:existing, connection_id, pubsub_id} =
      tenant
      |> insert_connection()
      |> next_available_pubsub_opts(table, tenant)

    {:new, connection_id, pubsub_id}
  end

  defp next_available_pubsub_opts({key, available_pubsub_ids}, table, _tenant) do
    {pubsub_id, new_list} = List.pop_at(available_pubsub_ids, 0)

    :ets.update_element(table, key, {2, new_list})

    {:existing, connection_id_from_key(key), pubsub_id}
  end

  def release_available_publisher_id(_tenant, _connection_id, _publisher_id) do
    :ok
  end

  defp tenant_connection_key(tenant, connection_id) do
    "tenants/#{tenant}/connections/#{connection_id}"
  end

  defp connection_id_from_key(key) do
    key |> String.split("/") |> List.last()
  end
end
