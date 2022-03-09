defmodule Bani.Scheduler do
  use GenServer

  # Client

  def start_link(opts) do
    init_state = %{
      conn_opts: Keyword.fetch!(opts, :conn_opts),
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

  def create_subscriber(tenant, stream_name, handler) do
    GenServer.call(via_tuple(tenant), {:create_subscriber, stream_name, handler})
  end

  defp via_tuple(tenant) do
    name = "#{tenant}-scheduler"

    {:via, Registry, {Bani.Registry, name}}
  end

  # Server (callbacks)

  @impl true
  def init(init_state) do
    {:ok, init_state}
  end

  @impl true
  def handle_call({:create_stream, stream_name}, _from, state) do
    :ok =
      Bani.Scheduling.available_conn(state.tenant, state.conn_opts)
      |> Bani.Broker.create_stream(stream_name)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:delete_stream, stream_name}, _from, state) do
    :ok =
      Bani.Scheduling.available_conn(state.tenant, state.conn_opts)
      |> Bani.Broker.delete_stream(stream_name)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:create_publisher, stream_name}, _from, state) do
    :ok = Bani.Scheduling.create_publisher(state.tenant, state.conn_opts, stream_name)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:create_subscriber, stream_name, handler}, _from, state) do
    :ok = Bani.Scheduling.create_subscriber(state.tenant, state.conn_opts, stream_name, handler)

    {:reply, :ok, state}
  end
end
