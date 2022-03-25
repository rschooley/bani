defmodule Bani.Publisher do
  use GenServer

  # Client

  def start_link(opts) do
    state = %{
      broker: Keyword.get(opts, :broker, Bani.Broker),
      conn: Keyword.fetch!(opts, :conn),
      publisher_id: Keyword.fetch!(opts, :publisher_id),
      stream_name: Keyword.fetch!(opts, :stream_name),
      tenant: Keyword.fetch!(opts, :tenant)
    }

    GenServer.start_link(__MODULE__, state, name: via_tuple(state.tenant, state.stream_name))
  end

  def add_publisher(tenant, stream_name) do
    GenServer.call(via_tuple(tenant, stream_name), :add_publisher)
  end

  def create_publisher(tenant, stream_name) do
    GenServer.call(via_tuple(tenant, stream_name), :create_publisher)
  end

  def publish_sync(tenant, stream_name, messages) when is_binary(tenant) and is_binary(stream_name) and is_list(messages) do
    GenServer.call(via_tuple(tenant, stream_name), {:publish_sync, messages})
  end

  def publish_sync(tenant, stream_name, message) when is_binary(tenant) and is_binary(stream_name) and is_binary(message) do
    publish_sync(tenant, stream_name, [message])
  end

  def delete_publisher(tenant, stream_name) do
    GenServer.call(via_tuple(tenant, stream_name), :delete_publisher)
  end

  defp via_tuple(tenant, stream_name) do
    name = "#{tenant}/#{stream_name}-publisher"

    {:via, Registry, {Bani.Registry, name}}
  end

  # Server (callbacks)

  @impl true
  def init(state) do
    publisher_name = Bani.KeyRing.publisher_name(state.tenant, state.stream_name)
    new_state = Map.put(state, :publisher_name, publisher_name)

    {:ok, new_state}
  end

  @impl true
  def handle_call(:add_publisher, _from, state) do
    {:ok, publishing_id} = state.broker.query_publisher_sequence(
      state.conn,
      state.publisher_name,
      state.stream_name
    )

    new_state = Map.put(state, :publishing_id, publishing_id)

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:create_publisher, from, state) do
    :ok = state.broker.create_publisher(
      state.conn,
      state.stream_name,
      state.publisher_id,
      state.publisher_name
    )

    handle_call(:add_publisher, from, state)
  end

  @impl true
  def handle_call({:publish_sync, messages}, _from, state) do
    message_count = Enum.count(messages)
    payload = Enum.zip(state.publishing_id..message_count, messages)

    :ok = state.broker.publish(
      state.conn,
      state.publisher_id,
      payload
    )

    new_state = Map.put(state, :publishing_id, state.publishing_id + message_count)

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:delete_publisher, _from, state) do
    :ok = state.broker.delete_publisher(state.conn, state.publisher_id)

    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:metadata_update, _, _stream_name}, state) do
    # TODO: handle these messages
    {:noreply, state}
  end
end
