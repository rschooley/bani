defmodule Bani.StoreManager do
  use GenServer

  # Client

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Server (callbacks)

  @impl true
  def init(state) do
    :ok = Bani.Store.init_connections_store()

    {:ok, state}
  end
end
