defmodule Bani.PublisherSupervisor do
  use DynamicSupervisor

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg)
  end

  def add_publisher(conn, stream_name, publisher_id) do
    DynamicSupervisor.start_child(
      __MODULE__, 
      {Bani.Publisher, conn, stream_name, publisher_id}
    )
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
