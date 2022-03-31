defmodule Bani.Tenant do
  use GenServer

  # Client

  def start_link(opts) do
    init_state = %{
      conn_opts: Keyword.fetch!(opts, :conn_opts),
      scheduling: Keyword.get(opts, :scheduling, Bani.Scheduling),
      tenant: Keyword.fetch!(opts, :tenant)
    }

    GenServer.start_link(__MODULE__, init_state, name: via_tuple(init_state.tenant))
  end

  def create_stream(tenant, stream_name) do
    GenServer.call(via_tuple(tenant), {:create_stream, stream_name})
  end

  def delete_stream(tenant, stream_name) do
    GenServer.call(via_tuple(tenant), {:delete_stream, stream_name})
  end

  def create_publisher(tenant, stream_name) do
    GenServer.call(via_tuple(tenant), {:create_publisher, stream_name})
  end

  def delete_publisher(tenant, stream_name) do
    GenServer.call(via_tuple(tenant), {:delete_publisher, stream_name})
  end

  def create_subscriber(tenant, stream_name, subscription_name, handler, acc, offset, poisoned) do
    GenServer.call(via_tuple(tenant), {:create_subscriber, stream_name, subscription_name, handler, acc, offset, poisoned})
  end

  def delete_subscriber(tenant, stream_name, subscription_name) do
    GenServer.call(via_tuple(tenant), {:delete_subscriber, stream_name, subscription_name})
  end

  defp via_tuple(tenant) do
    name = Bani.KeyRing.tenant_name(tenant)

    {:via, Registry, {Bani.Registry, name}}
  end

  # Server (callbacks)

  @impl true
  def init(state) do
    # store data will be lost on app restart, deploy, infrastructure upgrade, etc
    :ok = Bani.Store.init_store(state.tenant)

    {:ok, state}
  end

  @impl true
  def handle_call({:create_stream, stream_name}, _from, state) do
    :ok = state.scheduling.create_stream(state.conn_opts, stream_name)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:delete_stream, stream_name}, _from, state) do
    :ok = state.scheduling.delete_stream(state.conn_opts, stream_name)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:create_publisher, stream_name}, _from, state) do
    :ok = state.scheduling.create_publisher(state.tenant, state.conn_opts, stream_name)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:delete_publisher, stream_name}, _from, state) do
    :ok = state.scheduling.delete_publisher(state.tenant, stream_name)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:create_subscriber, stream_name, subscription_name, handler, acc, offset, poisoned}, _from, state) do
    :ok = state.scheduling.create_subscriber(state.tenant, state.conn_opts, stream_name, subscription_name, handler, acc, offset, poisoned)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:delete_subscriber, stream_name, subscription_name}, _from, state) do
    :ok = state.scheduling.delete_subscriber(state.tenant, stream_name, subscription_name)

    {:reply, :ok, state}
  end
end
