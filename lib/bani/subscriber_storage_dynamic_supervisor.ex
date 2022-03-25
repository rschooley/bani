defmodule Bani.SubscriberStorageDynamicSupervisor do
  use DynamicSupervisor

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  def add_storage(offset, acc \\ nil) do
    opts = [acc: acc, offset: offset]

    DynamicSupervisor.start_child(
      __MODULE__,
      {Bani.SubscriberStorage, opts}
    )
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
