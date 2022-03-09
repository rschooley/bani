defmodule Bani.SubscriberSupervisor do
  use DynamicSupervisor

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg)
  end

  def add_subscriber(conn, stream_name, subscription_id, handler) do
    DynamicSupervisor.start_child(
      __MODULE__, 
      {Bani.Subscriber, conn, stream_name, subscription_id, handler}
    )
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
