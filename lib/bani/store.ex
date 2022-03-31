defmodule Bani.Store do
  # the store uses 2 atoms per tenant for the table names
  #  if you have 500_000 tenants you will run into max atom issues
  def init_store(tenant) do
    :ets.new(publisher_table(tenant), [:ordered_set, :named_table])
    :ets.new(subscriber_table(tenant), [:ordered_set, :named_table])

    :ok
  end

  defp publisher_table(tenant) do
    :"tenants/#{tenant}/available_publisher_connections"
  end

  defp subscriber_table(tenant) do
    :"tenants/#{tenant}/available_subscriber_connections"
  end

  # next publisher/subscriber opts

  # caller must ensure atomic access
  def next_available_pubsub_opts(tenant, :publisher) do
    table = publisher_table(tenant)

    # 1) grab the first key in the table
    :ets.first(table)
    |> next_available_pubsub_opts(table, tenant)
  end

  # caller must ensure atomic access
  def next_available_pubsub_opts(tenant, :subscriber) do
    table = subscriber_table(tenant)

    # 1) grab the first key in the table
    :ets.first(table)
    |> next_available_pubsub_opts(table, tenant)
  end

  defp next_available_pubsub_opts(:"$end_of_table", table, tenant) do
    # 2a) the table is empty
    # insert a new connection, pass that key on to 2b
    tenant
    |> insert_connection()
    |> next_available_pubsub_opts(table, tenant)
  end

  defp next_available_pubsub_opts([{connection_id, []}], table, tenant) do
    # 3a) the list of ids for the record is empty (all used)
    #  find the next record in the table
    #  and remove the current record
    next_connection_id = :ets.next(table, connection_id)

    :ets.delete(table, connection_id)

    # if there isn't another record call 2a
    # if there is another record call 3b
    next_available_pubsub_opts(next_connection_id, table, tenant)
  end

  defp next_available_pubsub_opts([{connection_id, available_pubsub_ids}], table, _tenant) do
    # 3b) grab the next pubsub_id and update the list
    {pubsub_id, new_list} = List.pop_at(available_pubsub_ids, 0)

    :ets.update_element(table, connection_id, {2, new_list})

    {connection_id, pubsub_id}
  end

  defp next_available_pubsub_opts(connection_id, table, tenant) do
    # 2b) the table has at least one record
    # get the next pubsub_id from its list
    # calling 3a or 3b
    :ets.lookup(table, connection_id)
    |> next_available_pubsub_opts(table, tenant)
  end

  defp insert_connection(tenant) do
    # time based and sortable
    connection_id = Uniq.UUID.uuid6()
    available_pubsub_ids = Enum.to_list(0..255)

    # oldest connection_ids will be at the front
    #  taking new ids removes holes in existing connections
    #  instead of the newest (freshest) connection
    # always add to both the publisher and tenant
    #  over time these will probably skew towards more subscribers
    #  but there is little overhead (extra list in the table here, an extra PublisherSupervisor process)
    #  and makes ensuring one connection_id refers to both publishers and subscribers easier
    true = :ets.insert(publisher_table(tenant), {connection_id, available_pubsub_ids})
    true = :ets.insert(subscriber_table(tenant), {connection_id, available_pubsub_ids})

    connection_id
  end

  # release available publisher/subscriber id

   def release_available_pubsub_id(tenant, connection_id, :publisher, pubsub_id) do
    table = publisher_table(tenant)

    :ets.lookup(table, connection_id)
    |> release_available_pubsub_id(table, tenant, connection_id, pubsub_id)
  end

  def release_available_pubsub_id(tenant, connection_id, :subscriber, pubsub_id) do
    table = subscriber_table(tenant)

    :ets.lookup(table, connection_id)
    |> release_available_pubsub_id(table, tenant, connection_id, pubsub_id)
  end

  def release_available_pubsub_id([], table, _tenant, connection_id, pubsub_id) do
    # will be added to front based on timestamp (uuid6)
    true = :ets.insert(table, {connection_id, [pubsub_id]})

    :ok
  end

  def release_available_pubsub_id([{connection_id, available_pubsub_ids}], table, _tenant, _connection_id, pubsub_id) do
    # add to the front of the existing list
    new_list = List.insert_at(available_pubsub_ids, 0, pubsub_id)

    true = :ets.update_element(table, connection_id, {2, new_list})

    :ok
  end
end
