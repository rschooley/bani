defmodule Bani.Store.TenantStore do
  @behaviour Bani.Store.TenantStoreBehaviour

  alias Bani.Store.TenantState

  @table_name :tenants
  @table_attrs [:id, :conn_opts]

  @impl Bani.Store.TenantStoreBehaviour
  def init_store() do
    opts = [
      attributes: @table_attrs,
      disc_copies: [node()],
      load_order: 1,
      type: :set
    ]

    case :mnesia.create_table(@table_name, opts) do
      {:atomic, :ok} ->
        :ok

      {:aborted, {:already_exists, @table_name}} ->
        :mnesia.wait_for_tables([@table_name], 5000)
    end
  end

  @impl Bani.Store.TenantStoreBehaviour
  def add_tenant(tenant, conn_opts) do
    {:atomic, :ok} =
      :mnesia.transaction(fn ->
        :mnesia.write({@table_name, tenant, conn_opts})
      end)

    :ok
  end

  @impl Bani.Store.TenantStoreBehaviour
  def remove_tenant(tenant) do
    {:atomic, :ok} =
      :mnesia.transaction(fn ->
        :mnesia.delete({@table_name, tenant})
      end)

    :ok
  end

  @impl Bani.Store.TenantStoreBehaviour
  def list_tenant_ids() do
    {:atomic, list} =
      :mnesia.transaction(fn ->
        :mnesia.all_keys(@table_name)
      end)

    list
  end

  def get_tenant(tenant) do
    result =
      :mnesia.transaction(fn ->
        :mnesia.read({@table_name, tenant})
      end)

    case result do
      {:atomic, [record]} -> {:ok, tenant_tuple_to_struct(record)}
      {:aborted, reason} -> {:error, reason}
    end
  end

  defp tenant_tuple_to_struct(tuple) when is_tuple(tuple) do
    # don't know if this is going to be expensive
    #  but working with the tuples outside of this module gets messy
    list =
      tuple
      # trim the table name
      |> Tuple.delete_at(0)
      |> Tuple.to_list()

    zipped = Enum.zip(@table_attrs, list)

    Kernel.struct!(TenantState, zipped)
  end
end
