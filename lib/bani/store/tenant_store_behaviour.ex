defmodule Bani.Store.TenantStoreBehaviour do
  @callback init_store() :: :ok

  @callback add_tenant(tenant :: String.t()) :: :ok

  @callback remove_tenant(tenant :: String.t()) :: :ok

  @callback list_tenant_ids() :: [id :: String.t()]
end
