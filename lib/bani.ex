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

      def bootstrap() do
        # genservers will pick up state from their stores
        Bani.Store.TenantStore.list_tenant_ids()
        |> Enum.each(fn tenant ->
          :ok = Bani.TenantDynamicSupervisor.add_tenant(tenant)

          # TODO:
          # this can be in parallel
          # don't do this, read each record until $end
          Bani.Store.SubscriberStore.list_keys(tenant)
          |> Enum.each(fn key ->
            {:ok, _} = bootstrap_pubsub(tenant, key)
          end)
        end)
      end

      defp bootstrap_pubsub(tenant, {:pub, publisher_key}) do
        state = Bani.Store.SubscriberStore.get_publisher(tenant, publisher_key)

        Bani.ConnectionSupervisor.add_publisher(
          state.connection_id,
          state.tenant,
          state.stream_name,
          state.publisher_id
        )
      end

      defp bootstrap_pubsub(tenant, {:sub, subscriber_key}) do
        state = Bani.Store.SubscriberStore.get_subscriber(tenant, subscriber_key)

        Bani.ConnectionSupervisor.add_subscriber(
          state.connection_id,
          state.tenant,
          state.stream_name,
          state.subscription_id,
          state.subscription_name,
          state.handler,
          state.strategy
        )
      end

      def add_tenant(tenant, conn_opts) do
        :ok = Bani.Store.TenantStore.add_tenant(tenant, conn_opts)
        {:ok, _} = Bani.TenantDynamicSupervisor.add_tenant(tenant)
        :ok = Bani.Tenant.init_stores(tenant)

        :ok
      end

      @doc """
      Does not delete streams, vhost, or server.
      """
      def remove_tenant(tenant) do
        :ok =
          tenant
          |> Bani.ConnectionSupervisor.select_connection_pids_by_tenant()
          |> Bani.ConnectionDynamicSupervisor.remove_connection_supervisor()

        :ok = Bani.Tenant.delete_stores(tenant)

        :ok =
          tenant
          |> Bani.Tenant.whereis()
          |> Bani.TenantDynamicSupervisor.remove_tenant()

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

      def create_subscriber(tenant, stream_name, subscription_name, handler, acc, offset, strategy) do
        Bani.Tenant.create_subscriber(tenant, stream_name, subscription_name, handler, acc, offset, strategy)
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
