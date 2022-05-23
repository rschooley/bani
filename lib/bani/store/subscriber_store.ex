defmodule Bani.Store.SubscriberStore do
  @behaviour Bani.Store.SubscriberStoreBehaviour

  alias Bani.Store.{PublisherState, SubscriberState}

  @table_attrs [:key, :value]

  @impl Bani.Store.SubscriberStoreBehaviour
  def init_store(tenant) do
    opts = [
      attributes: @table_attrs,
      disc_copies: [node()],
      type: :set
    ]

    # warning: this will grow atoms over time, 1 for each tenant
    #  the tradeoff is data isolation with separate tables for each tenant
    table_name = table_name(tenant)

    case :mnesia.create_table(table_name, opts) do
      {:atomic, :ok} ->
        :ok

      {:aborted, {:already_exists, table_name}} ->
        :mnesia.wait_for_tables([table_name], 5000)
    end
  end

  @impl Bani.Store.SubscriberStoreBehaviour
  def delete_store(tenant) do
    table_name = table_name(tenant)

    {:atomic, :ok} = :mnesia.delete_table(table_name)
    :ok
  end

  @impl Bani.Store.SubscriberStoreBehaviour
  def list_keys(tenant) do
    table_name = table_name(tenant)

    {:atomic, list} =
      :mnesia.transaction(fn ->
        :mnesia.all_keys(table_name)
      end)

    list
  end

  @impl Bani.Store.SubscriberStoreBehaviour
  def add_publisher(tenant, publisher_key) do
    table_name = table_name(tenant)
    state = %PublisherState{}

    {:atomic, :ok} =
      :mnesia.transaction(fn ->
        :mnesia.write({table_name, {:pub, publisher_key}, state})
      end)

    :ok
  end

  @impl Bani.Store.SubscriberStoreBehaviour
  def remove_publisher(tenant, publisher_key) do
    table_name = table_name(tenant)

    result =
      :mnesia.transaction(fn ->
        :mnesia.delete({table_name, {:pub, publisher_key}})
      end)

    case result do
      {:atomic, :ok} -> :ok
      {:aborted, reason} -> {:error, reason}
    end
  end

  @impl Bani.Store.SubscriberStoreBehaviour
  def add_subscriber(tenant, subscriber_key, state) do
    table_name = table_name(tenant)

    {:atomic, :ok} =
      :mnesia.transaction(fn ->
        :mnesia.write({table_name, {:sub, subscriber_key}, state})
      end)

    :ok
  end

  @impl Bani.Store.SubscriberStoreBehaviour
  def remove_subscriber(tenant, subscriber_key) do
    table_name = table_name(tenant)

    result =
      :mnesia.transaction(fn ->
        :mnesia.delete({table_name, {:sub, subscriber_key}})
      end)

    case result do
      {:atomic, :ok} -> :ok
      {:aborted, reason} -> {:error, reason}
    end
  end

  @impl Bani.Store.SubscriberStoreBehaviour
  def get_subscriber(tenant, subscriber_key) do
    table_name = table_name(tenant)

    result =
      :mnesia.transaction(fn ->
        :mnesia.read({table_name, {:sub, subscriber_key}})
      end)

    case result do
      {:atomic, [record]} -> {:ok, record_to_struct(record)}
      {:atomic, []} -> {:error, "not found"}
      {:aborted, reason} -> {:error, reason}
    end
  end

  @impl Bani.Store.SubscriberStoreBehaviour
  def lock_subscriber(tenant, subscriber_key) do
    table_name = table_name(tenant)
    key = {:sub, subscriber_key}

    result =
      :mnesia.transaction(fn ->
        [{_, _, state}] = :mnesia.read({table_name, key})

        case state.locked do
          true ->
            :already_locked

          false ->
            state = %{state | locked: true}
            tuple = {table_name, key, state}

            :ok = :mnesia.write(tuple)
            tuple
        end
      end)

    case result do
      {:atomic, :already_locked} -> {:error, :already_locked}
      {:atomic, record} -> {:ok, record_to_struct(record)}
      {:aborted, reason} -> {:error, reason}
    end
  end

  @impl Bani.Store.SubscriberStoreBehaviour
  def unlock_subscriber(tenant, subscriber_key, new_acc, inc_offset) do
    table_name = table_name(tenant)
    key = {:sub, subscriber_key}

    result =
      :mnesia.transaction(fn ->
        [{_, _, state}] = :mnesia.read({table_name, key})

        case state.locked do
          true ->
            state = %{state | acc: new_acc, offset: state.offset + inc_offset, locked: false}
            tuple = {table_name, key, state}

            :ok = :mnesia.write(tuple)
            tuple

          false ->
            :already_unlocked
        end
      end)

    case result do
      {:atomic, :already_unlocked} -> {:error, :already_unlocked}
      {:atomic, record} -> {:ok, record_to_struct(record)}
      {:aborted, reason} -> {:error, reason}
    end
  end

  @impl Bani.Store.SubscriberStoreBehaviour
  def update_subscriber(tenant, subscriber_key, new_acc, inc_offset) do
    table_name = table_name(tenant)
    key = {:sub, subscriber_key}

    result =
      :mnesia.transaction(fn ->
        [{_, _, state}] = :mnesia.read({table_name, key})

        state = %{state | acc: new_acc, offset: state.offset + inc_offset}
        tuple = {table_name, key, state}

        :ok = :mnesia.write(tuple)
        tuple
      end)

    case result do
      {:atomic, record} -> {:ok, record_to_struct(record)}
      {:aborted, reason} -> {:error, reason}
    end
  end

  defp table_name(tenant) do
    :"tenants_#{tenant}_subscribers"
  end

  defp record_to_struct(record) when is_tuple(record) and tuple_size(record) == 3 do
    {_table, {type, key}, struct} = record

    case type do
      :pub -> Map.put(struct, :publisher_key, key)
      :sub -> Map.put(struct, :subscriber_key, key)
    end
  end
end
