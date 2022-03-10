defmodule Bani.ConnectionManager do
  use GenServer

  # Client

  def start_link(opts) do
    init_state = %{
      broker: Keyword.get(opts, :broker, Bani.Broker),
      supervisor: Keyword.fetch!(opts, :supervisor),
      host: Keyword.fetch!(opts, :host),
      port: Keyword.fetch!(opts, :port),
      username: Keyword.fetch!(opts, :username),
      password: Keyword.fetch!(opts, :password),
      vhost: Keyword.fetch!(opts, :vhost)
    }

    GenServer.start_link(__MODULE__, init_state)
  end

  def conn(pid) do
    GenServer.call(pid, :conn)
  end

  def register(pid, key, available_ids) do
    GenServer.call(pid, {:register, key, available_ids})
  end

  def lease(pid, key, available_ids, pubsub_type) do
    GenServer.call(pid, {:lease, key, available_ids, pubsub_type})
  end

  # Callbacks

  @impl true
  def init(init_state) do
    {:ok, init_state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    {:ok, conn} = state.broker.connect(
      state.host,
      state.port,
      state.username,
      state.password,
      state.vhost
    )

    new_state = Map.put(state, :conn, conn)

    pid = self()
    ref = make_ref()
    spawn_link(fn -> monitor_for_cleanup(pid, ref, {new_state.broker, new_state.conn}) end)

    receive do
      {^ref, :ready} -> :ok
    end

    {:noreply, new_state}
  end

  @impl true
  def handle_call(:conn, _from, state) do
    {:reply, state.conn, state}
  end

  @impl true
  def handle_call({:register, key, available_ids}, _from, state) do
    :ok = Bani.Storage.register(key, available_ids)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:lease, key, available_ids, pubsub_type}, _from, state) do
    {id, new_available_ids} = next_pubsub_id(available_ids, pubsub_type)
    :ok = Bani.Storage.update_value(key, new_available_ids)

    {:reply, {state.supervisor, state.conn, id}, state}
  end

  defp next_pubsub_id(list, key) do
    Map.get_and_update(list, key, fn curr -> List.pop_at(curr, 0) end)
  end

  defp monitor_for_cleanup(pid, ref, {broker, conn}) do
    Process.flag(:trap_exit, true)
    send(pid, {ref, :ready})

    receive do
      {:EXIT, ^pid, _reason} ->
        if Process.alive?(conn) do
          :ok = broker.disconnect(conn)
        end
    end
  end
end
