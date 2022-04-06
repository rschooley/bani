defmodule Bani.SubscriberStorageDynamicSupervisor do
  use DynamicSupervisor

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  def add_storage(opts) do
    DynamicSupervisor.start_child(
      __MODULE__,
      {Bani.SubscriberStorage, opts}
    )
  end

  def remove_storage(tenant, stream_name, subscription_name) do
    Bani.SubscriberStorage.stop(tenant, stream_name, subscription_name)
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
