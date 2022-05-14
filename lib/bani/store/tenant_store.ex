defmodule Bani.Store.TenantStore do
  @behaviour Bani.Store.TenantStoreBehaviour

  @table_name :tenants
  @table_attrs [:id, :ts]

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
  def add_tenant(tenant) do
    {:atomic, :ok} =
      :mnesia.transaction(fn ->
        :mnesia.write({@table_name, tenant, "ts"})
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
end
