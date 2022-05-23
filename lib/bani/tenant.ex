defmodule Bani.Tenant do
  use GenServer

  # Client

  def start_link(opts) do
    init_state = %{
      scheduling: Keyword.get(opts, :scheduling, Bani.Scheduling),
      store: Keyword.get(opts, :store, Bani.Store.TenantStore),
      tenant: Keyword.fetch!(opts, :tenant)
    }

    GenServer.start_link(__MODULE__, init_state, name: via_tuple(init_state.tenant))
  end

  def init_stores(tenant) do
    GenServer.call(via_tuple(tenant), :init_stores)
  end

  def delete_stores(tenant) do
    GenServer.call(via_tuple(tenant), {:delete_stores})
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

  def create_subscriber(tenant, stream_name, subscription_name, handler, acc, offset, strategy) do
    GenServer.call(via_tuple(tenant), {:create_subscriber, stream_name, subscription_name, handler, acc, offset, strategy})
  end

  def delete_subscriber(tenant, stream_name, subscription_name) do
    GenServer.call(via_tuple(tenant), {:delete_subscriber, stream_name, subscription_name})
  end

  def whereis(tenant) when is_binary(tenant) do
    GenServer.whereis(via_tuple(tenant))
  end

  defp via_tuple(tenant) do
    name = Bani.KeyRing.tenant_name(tenant)

    {:via, Registry, {Bani.Registry, name}}
  end

  # Server (callbacks)

  @impl true
  def init(state) do
    {:ok, %Bani.Store.TenantState{conn_opts: conn_opts}} = state.store.get_tenant(state.tenant)

    new_state = Map.put(state, :conn_opts, conn_opts)

    {:ok, new_state}
  end

  @impl true
  def handle_call(:init_stores, _from, state) do
    :ok = Bani.Store.SchedulingStore.init_store(state.tenant)
    :ok = Bani.Store.SubscriberStore.init_store(state.tenant)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:delete_stores}, _from, state) do
    :ok = Bani.Store.SchedulingStore.delete_store(state.tenant)
    :ok = Bani.Store.SubscriberStore.delete_store(state.tenant)

    {:reply, :ok, state}
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
  def handle_call({:create_subscriber, stream_name, subscription_name, handler, acc, offset, strategy}, _from, state) do
    :ok = state.scheduling.create_subscriber(state.tenant, state.conn_opts, stream_name, subscription_name, handler, acc, offset, strategy)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:delete_subscriber, stream_name, subscription_name}, _from, state) do
    :ok = state.scheduling.delete_subscriber(state.tenant, stream_name, subscription_name)

    {:reply, :ok, state}
  end
end
