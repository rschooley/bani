defmodule Bani.Publisher do
  use GenServer

  # Client

  def start_link(opts) do
    init_state = %{
      broker: Keyword.get(opts, :broker, Bani.Broker),
      conn: Keyword.fetch!(opts, :conn),
      publisher_id: Keyword.fetch!(opts, :publisher_id),
      publishing_id: Keyword.get(opts, :publishing_id, 0),
      stream_name: Keyword.fetch!(opts, :stream_name)
    }

    GenServer.start_link(__MODULE__, init_state, name: via_tuple(init_state.stream_name))
  end

  def publish_sync(stream_name, message) do
    GenServer.call(via_tuple(stream_name), {:publish_sync, message})
  end

  defp via_tuple(stream_name) do
    name = "#{stream_name}-publisher"

    {:via, Registry, {Bani.Registry, name}}
  end

  # Server (callbacks)

  @impl true
  def init(init_state) do
    publisher_name = "#{init_state.stream_name}-publisher-#{init_state.publisher_id}"
    state = Map.merge(init_state, %{publisher_name: publisher_name})

    {:ok, state, {:continue, :create_publisher}}
  end

  @impl true
  def handle_continue(:create_publisher, state) do
    :ok = state.broker.create_publisher(
      state.conn,
      state.stream_name,
      state.publisher_id,
      state.publisher_name
    )

    pid = self() 
    ref = make_ref()
    spawn_link(fn -> monitor_for_cleanup(pid, ref, {state.broker, state.conn, state.publisher_id}) end)

    receive do
      {^ref, :ready} -> :ok
    end

    {:noreply, state}
  end

  @impl true
  def handle_call({:publish_sync, message}, _from, state) do
    :ok = state.broker.publish(state.conn, state.publisher_id, message, state.publishing_id)

    state = %{state | publishing_id: state.publishing_id + 1}

    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:metadata_update, _, _stream_name}, state) do
    # TODO: handle these messages
    {:noreply, state}
  end

  defp monitor_for_cleanup(pid, ref, {broker, conn, publisher_id}) do
    Process.flag(:trap_exit, true)
    send(pid, {ref, :ready})

    receive do
      {:EXIT, ^pid, _reason} ->
        :ok = broker.delete_publisher(conn, publisher_id)
    end
  end
end
