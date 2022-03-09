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
        {:ok, _} = Bani.SchedulerDynamicSupervisor.add_scheduler(tenant, conn_opts)

        :ok
      end

      def create_stream(tenant, stream_name) do
        Bani.Scheduler.create_stream(tenant, stream_name)
      end

      def delete_stream(tenant, stream_name) do
        Bani.Scheduler.delete_stream(tenant, stream_name)
      end

      def create_publisher(tenant, stream_name) do
        Bani.Scheduler.create_publisher(tenant, stream_name)
      end

      def create_subscriber(tenant, stream_name, handler) do
        Bani.Scheduler.create_subscriber(tenant, stream_name, handler)
      end

      def publish(tenant, stream_name, message) do
        Bani.Publisher.publish_sync(stream_name, message)
      end
    end
  end
end
