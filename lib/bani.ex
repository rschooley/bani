defmodule Bani do
  @moduledoc """
  Documentation for `Bani`.
  """

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      def start_link(opts \\ []) do
        Bani.Application.start(__MODULE__, opts)
      end

      def add_tenant(tenant, conn_opts) do
        # add tenant to store
        # add tenant to supervisor, bind to store
        # add scheduling and subscriber stores for tenant
        :ok = Bani.Store.TenantStore.add_tenant(tenant, conn_opts)
        {:ok, _} = Bani.TenantDynamicSupervisor.add_tenant(tenant)
        :ok = Bani.Tenant.init_stores(tenant)

        :ok
      end

      def remove_tenant(tenant) do
        # TODO: shutdown connection_supervisors
        :ok = Bani.Tenant.delete_stores(tenant)
        # {:ok, _} = Bani.TenantDynamicSupervisor.remove_tenant(tenant)
        :ok = Bani.Store.TenantStore.remove_tenant(tenant)
      end

      def create_stream(tenant, stream_name) do
        Bani.Tenant.create_stream(tenant, stream_name)
      end

      def delete_stream(tenant, stream_name) do
        Bani.Tenant.delete_stream(tenant, stream_name)
      end

      def create_publisher(tenant, stream_name) do
        Bani.Tenant.create_publisher(tenant, stream_name)
      end

      def delete_publisher(tenant, stream_name) do
        Bani.Tenant.delete_publisher(tenant, stream_name)
      end

      def create_subscriber(tenant, stream_name, subscription_name, handler, acc, offset \\ 0) do
        Bani.Tenant.create_subscriber(tenant, stream_name, subscription_name, handler, acc, offset)
      end

      def delete_subscriber(tenant, stream_name, subscription_name) do
        Bani.Tenant.delete_subscriber(tenant, stream_name, subscription_name)
      end

      def publish(tenant, stream_name, messages) when is_binary(tenant) and is_binary(stream_name) and is_list(messages) do
        Bani.Publisher.publish_sync(tenant, stream_name, messages)
      end

      def publish(tenant, stream_name, message) when is_binary(tenant) and is_binary(stream_name) and is_binary(message) do
        Bani.Publisher.publish_sync(tenant, stream_name, message)
      end

      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :supervisor
        }
      end
    end
  end
end
