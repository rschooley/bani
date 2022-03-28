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

  def publish_sync(tenant, stream_name, messages) when is_binary(tenant) and is_binary(stream_name) and is_list(messages) do
    GenServer.call(via_tuple(tenant, stream_name), {:publish_sync, messages})
  end

  def publish_sync(tenant, stream_name, message) when is_binary(tenant) and is_binary(stream_name) and is_binary(message) do
    publish_sync(tenant, stream_name, [message])
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

    {:ok, new_state, {:continue, :create_publisher}}
  end

  @impl true
  def handle_continue(:create_publisher, state) do
    # can create and then query sequence
    #  for a new publisher it will be new
    #  for an existing/recreated publisher it will be the existing
    :ok = state.broker.create_publisher(
      state.conn,
      state.stream_name,
      state.publisher_id,
      state.publisher_name
    )

    {:ok, publishing_id} = state.broker.query_publisher_sequence(
      state.conn,
      state.publisher_name,
      state.stream_name
    )

    new_state = Map.put(state, :publishing_id, publishing_id)

    pid = self()
    ref = make_ref()

    spawn_link(fn -> monitor_for_cleanup(pid, ref, {state.broker, state.conn, state.publisher_id})
    end)

    receive do
      {^ref, :ready} -> :ok
    end

    {:noreply, new_state}
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
  def handle_info({:metadata_update, _, _stream_name}, state) do
    # TODO: handle these messages
    {:noreply, state}
  end

  defp monitor_for_cleanup(pid, ref, {broker, conn, publisher_id}) do
    Process.flag(:trap_exit, true)
    send(pid, {ref, :ready})

    receive do
      {:EXIT, ^pid, _reason} ->
        # two most probable reasons for an exit
        # 1) the conn dropped and the ConnectionSupervisor subtree is restarting
        #    in that case the conn is dead, but that will remove the sub in rabbit
        # 2) this genserver alone is crashing
        #    in that case the conn is alive, we'll remove the existing sub and recreate on init
        if (Process.alive?(conn)) do
          # if this fails the init will fail and cascade crash the supervisor resetting the conn
          :ok = broker.delete_publisher(conn, publisher_id)
        end
    end
  end
end
