defmodule Bani.Store.SchedulingStore do
  @behaviour Bani.Store.SchedulingStoreBehaviour

  @table_attrs [:connection_id, :available_pubsub_ids]

  @impl Bani.Store.SchedulingStoreBehaviour
  def init_store(tenant) do
    # TODO: merge into one table with index
    # the store uses 2 atoms per tenant for the table names
    #  if you have 500_000 tenants you will run into max atom issues
    :ok = table_name(tenant, :publisher) |> init_table()
    :ok = table_name(tenant, :subscriber) |> init_table()

    :ok
  end

  @impl Bani.Store.SchedulingStoreBehaviour
  def delete_store(tenant) do
    {:atomic, :ok} = table_name(tenant, :publisher) |> :mnesia.delete_table()
    {:atomic, :ok} = table_name(tenant, :subscriber) |> :mnesia.delete_table()

    :ok
  end

  defp table_name(tenant, :publisher) do
    :"tenants_#{tenant}_available_publisher_connections"
  end

  defp table_name(tenant, :subscriber) do
    :"tenants_#{tenant}_available_subscriber_connections"
  end

  defp init_table(table_name) do
    opts = [
      attributes: @table_attrs,
      disc_copies: [node()],
      type: :ordered_set
    ]

    case :mnesia.create_table(table_name, opts) do
      {:atomic, :ok} ->
        :ok

      {:aborted, {:already_exists, table_name}} ->
        :mnesia.wait_for_tables([table_name], 5000)
    end
  end

  # next publisher/subscriber opts

  @impl Bani.Store.SchedulingStoreBehaviour
  def next_available_pubsub_opts(tenant, pub_sub_type) do
    table_name = table_name(tenant, pub_sub_type)

    result =
      :mnesia.transaction(fn ->
        # 1) grab the first key in the table
        :mnesia.first(table_name)
        |> do_next_available_pubsub_opts(table_name, tenant)
      end)

    case result do
      {:atomic, record} -> {:ok, record}
      {:aborted, reason} -> {:error, reason}
    end
  end

  defp do_next_available_pubsub_opts(:"$end_of_table", table, tenant) do
    # 2a) the table is empty
    # insert a new connection, pass that key on to 2b
    tenant
    |> insert_connection()
    |> do_next_available_pubsub_opts(table, tenant)
  end

  defp do_next_available_pubsub_opts(connection_id, table, tenant) do
    # 2b) the table has at least one record
    # get the next pubsub_id from its list
    # calling 3a or 3b
    :mnesia.read(table, connection_id)
    |> do_next_available_pubsub_opts(tenant)
  end

  defp do_next_available_pubsub_opts([{table, connection_id, []}], tenant) do
    # 3a) the list of ids for the record is empty (all used)
    #  find the next record in the table
    #  and remove the current record
    next_connection_id = :mnesia.next(table, connection_id)

    :ok = :mnesia.delete({table, connection_id})

    # if there isn't another record call 2a
    # if there is another record call 3b
    do_next_available_pubsub_opts(next_connection_id, table, tenant)
  end

  defp do_next_available_pubsub_opts([{table, connection_id, available_pubsub_ids}], _tenant) do
    # 3b) grab the next pubsub_id and update the list
    {pubsub_id, new_list} = List.pop_at(available_pubsub_ids, 0)

    :ok = :mnesia.write({table, connection_id, new_list})

    {connection_id, pubsub_id}
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
    :ok = :mnesia.write({table_name(tenant, :publisher), connection_id, available_pubsub_ids})
    :ok = :mnesia.write({table_name(tenant, :subscriber), connection_id, available_pubsub_ids})

    connection_id
  end

  # release available publisher/subscriber id

  @impl Bani.Store.SchedulingStoreBehaviour
  def release_available_pubsub_id(tenant, connection_id, pub_sub_type, pubsub_id) do
    table_name = table_name(tenant, pub_sub_type)

    result =
      :mnesia.transaction(fn ->
        :mnesia.read(table_name, connection_id)
        |> do_release_available_pubsub_id(table_name, connection_id, pubsub_id)
      end)

    case result do
      {:atomic, _record} -> :ok
      {:aborted, reason} -> {:error, reason}
    end
  end

  defp do_release_available_pubsub_id([], table, connection_id, pubsub_id) do
    # will be added to front based on timestamp (uuid6)
    :ok = :mnesia.write({table, connection_id, [pubsub_id]})

    :ok
  end

  defp do_release_available_pubsub_id(
         [{_table, connection_id, available_pubsub_ids}],
         table,
         _connection_id,
         pubsub_id
       ) do
    # add to the front of the existing list
    new_list = List.insert_at(available_pubsub_ids, 0, pubsub_id)

    :ok = :mnesia.write({table, connection_id, new_list})
    :ok
  end
end
