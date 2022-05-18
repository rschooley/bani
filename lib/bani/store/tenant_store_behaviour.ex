defmodule Bani.Store.TenantStoreBehaviour do
  @callback init_store() :: :ok

  @callback add_tenant(tenant :: String.t(), conn_opts :: keyword()) :: :ok

  @callback remove_tenant(tenant :: String.t()) :: :ok

  @callback get_tenant(tenant :: String.t()) :: {:ok, term()} | {:error, term()}

  @callback list_tenant_ids() :: [id :: String.t()]
end
