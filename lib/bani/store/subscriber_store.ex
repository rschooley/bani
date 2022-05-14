defmodule Bani.Store.SubscriberStore do
  @behaviour Bani.Store.SubscriberStoreBehaviour

  alias Bani.Store.SubscriberState

  @table_attrs [:subscriber_key, :acc, :offset, :locked]

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
  def add_subscriber(tenant, subscriber_key, acc, offset) do
    table_name = table_name(tenant)

    {:atomic, :ok} =
      :mnesia.transaction(fn ->
        :mnesia.write({table_name, subscriber_key, acc, offset, _locked = false})
      end)

    :ok
  end

  @impl Bani.Store.SubscriberStoreBehaviour
  def get_subscriber(tenant, subscriber_key) do
    table_name = table_name(tenant)

    result =
      :mnesia.transaction(fn ->
        :mnesia.read({table_name, subscriber_key})
      end)

    case result do
      {:atomic, [record]} -> {:ok, subscriber_tuple_to_struct(record)}
      {:aborted, reason} -> {:error, reason}
    end
  end

  @impl Bani.Store.SubscriberStoreBehaviour
  def lock_subscriber(tenant, subscriber_key) do
    table_name = table_name(tenant)

    result =
      :mnesia.transaction(fn ->
        [{_, _, acc, offset, locked}] = :mnesia.read({table_name, subscriber_key})

        case locked do
          true ->
            :already_locked

          false ->
            tuple = {table_name, subscriber_key, acc, offset, _locked = true}

            :ok = :mnesia.write(tuple)
            tuple
        end
      end)

    case result do
      {:atomic, :already_locked} -> {:error, :already_locked}
      {:atomic, record} -> {:ok, subscriber_tuple_to_struct(record)}
      {:aborted, reason} -> {:error, reason}
    end
  end

  @impl Bani.Store.SubscriberStoreBehaviour
  def unlock_subscriber(tenant, subscriber_key, new_acc, inc_offset) do
    table_name = table_name(tenant)

    result =
      :mnesia.transaction(fn ->
        [{_, _, _, offset, locked}] = :mnesia.read({table_name, subscriber_key})

        tuple = {table_name, subscriber_key, new_acc, offset + inc_offset, _locked = false}

        case locked do
          true ->
            :ok = :mnesia.write(tuple)
            tuple

          false ->
            :already_unlocked
        end
      end)

    case result do
      {:atomic, :already_unlocked} -> {:error, :already_unlocked}
      {:atomic, record} -> {:ok, subscriber_tuple_to_struct(record)}
      {:aborted, reason} -> {:error, reason}
    end
  end

  @impl Bani.Store.SubscriberStoreBehaviour
  def update_subscriber(tenant, subscriber_key, new_acc, inc_offset) do
    table_name = table_name(tenant)

    result =
      :mnesia.transaction(fn ->
        [{_, _, _, offset, locked}] = :mnesia.read({table_name, subscriber_key})

        tuple = {table_name, subscriber_key, new_acc, offset + inc_offset, locked}

        :ok = :mnesia.write(tuple)
        tuple
      end)

    case result do
      {:atomic, record} -> {:ok, subscriber_tuple_to_struct(record)}
      {:aborted, reason} -> {:error, reason}
    end
  end

  defp table_name(tenant) do
    :"tenants_#{tenant}_subscribers"
  end

  defp subscriber_tuple_to_struct(tuple) when is_tuple(tuple) do
    # don't know if this is going to be expensive
    #  but working with the tuples outside of this module gets messy
    list =
      tuple
      # trim the table name
      |> Tuple.delete_at(0)
      |> Tuple.to_list()

    zipped = Enum.zip(@table_attrs, list)

    Kernel.struct!(SubscriberState, zipped)
  end
end
